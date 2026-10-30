"""Live migration verification, second engine pair: MySQL -> Debezium (binlog) -> Kafka -> PostgreSQL.

    python bench/live_mysql_pg.py load --warehouses 4
    python bench/live_mysql_pg.py baseline --rate 100 --seconds 300
    python bench/live_mysql_pg.py live --rate 100 --seconds 300 --lag 2

The verifier is bench/live_cluster.py's, unchanged: one single-partition topic, so the Kafka offset is
a version in commit order (MySQL writes a transaction to the binlog when it commits); the target stores
each row's version in src_v and each delete's version in dp_tombstones; checks compare sketches at the
watermark and confirm candidates against the target only. What differs is the engines: MySQL's types
(a single-precision FLOAT, TIMESTAMP as UTC), PostgreSQL as the target, and MySQL scans for ground truth.

Addresses come from the environment (local Docker, see bench/mysql_pg_local.sh) or ~/cluster.env
(MYSQL_IP, PGT_IP, KAFKA_IP on the cloud cluster).
"""

from __future__ import annotations

import argparse
import os
import random
import subprocess
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import live_cluster  # noqa: E402
from live_cluster import ClusterLive, benchbase_summary, debezium_row, sketches_equal  # noqa: E402
from live_e2e import scan_table  # noqa: E402

from deltaproof import canon  # noqa: E402
from deltaproof.parallel import scan  # noqa: E402
from deltaproof.relational import TPCC_MYSQL, TPCC_MYSQL_BY_NAME, TPCC_MYSQL_BY_TAG  # noqa: E402
from deltaproof.sizing import RatelessSketch  # noqa: E402
from deltaproof.sketch import Sketch  # noqa: E402
from deltaproof.store import Table, exact_diff  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
HOME = Path.home()
ENV = live_cluster.ENV
_get = lambda name, default: os.environ.get(name) or ENV.get(name) or default
MYSQL_HOST = _get("MYSQL_IP", "127.0.0.1")
MYSQL_PORT = int(_get("MYSQL_PORT", "3306"))
# Debezium Connect and BenchBase run in containers, which may reach MySQL under another name.
MYSQL_CONNECT_HOST = _get("MYSQL_CONNECT_HOST", MYSQL_HOST)
MYSQL_CONNECT_PORT = int(_get("MYSQL_CONNECT_PORT", str(MYSQL_PORT)))
PGT_HOST = _get("PGT_IP", "127.0.0.1")
PGT_PORT = int(_get("PGT_PORT", "5432"))
PGT_URL = f"postgresql://postgres:deltaproof@{PGT_HOST}:{PGT_PORT}/tpcc_target"
KAFKA = _get("KAFKA", f"{_get('KAFKA_IP', '127.0.0.1')}:9092")
CONNECT = _get("CONNECT", f"http://{_get('KAFKA_IP', '127.0.0.1')}:8083")
BB_NET = _get("BB_NET", "host")
BB_DIR = Path(_get("BB_DIR", str(HOME / "benchbase-mysql")))
TABLES = [s.name for s in TPCC_MYSQL] + ["history"]  # history has no key: copied, not verified


# ---------------------------------------------------------------- MySQL source


def connect_mysql(db: str | None = "tpcc"):
    import pymysql

    return pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user="root", password="deltaproof", database=db,
                           autocommit=True,
                           # streaming scans read slowly; MySQL drops a client after net_write_timeout (60 s)
                           init_command="SET time_zone = '+00:00', net_write_timeout = 3600, net_read_timeout = 3600")


def benchbase(tag: str, warehouses: int, rate, seconds: int, terminals: int, phase: str) -> subprocess.Popen:
    cfg = (ROOT / "bench" / "tpcc" / "config-mysql.xml.tmpl").read_text()
    cfg = cfg.replace("{host}", f"{MYSQL_CONNECT_HOST}:{MYSQL_CONNECT_PORT}")
    cfg = cfg.replace("{warehouses}", str(warehouses)).replace("{terminals}", str(terminals))
    cfg = cfg.replace("{seconds}", str(seconds)).replace("{rate}", str(rate))
    bb = HOME / "bb"
    (bb / "results" / tag).mkdir(parents=True, exist_ok=True)
    for old in (bb / "results" / tag).glob("*"):
        old.unlink()
    (bb / f"{tag}.xml").write_text(cfg)
    flags = ["--create=true", "--load=true"] if phase == "load" else ["--execute=true"]
    log = open(bb / "results" / tag / "benchbase.log", "w")
    docker = ["docker"] if BB_NET != "host" else ["sudo", "docker"]
    return subprocess.Popen(
        [*docker, "run", "--rm", "--network", BB_NET, "-v", f"{bb}:/ext", "-v", f"{BB_DIR}:/bb",
         "-w", "/bb", "eclipse-temurin:23-jre", "java", "-jar", "benchbase.jar", "-b", "tpcc", "-c", f"/ext/{tag}.xml",
         *flags, "-d", f"/ext/results/{tag}"], stdout=log, stderr=subprocess.STDOUT)


def copy_db(src: str, dst: str) -> None:
    with connect_mysql(None) as c, c.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {dst}")
        cur.execute(f"CREATE DATABASE {dst}")
        cur.execute("SET FOREIGN_KEY_CHECKS = 0")
        for t in TABLES:
            cur.execute(f"CREATE TABLE {dst}.{t} LIKE {src}.{t}")
            cur.execute(f"INSERT INTO {dst}.{t} SELECT * FROM {src}.{t}")


def run_load(args) -> None:
    with connect_mysql(None) as c, c.cursor() as cur:
        cur.execute("DROP DATABASE IF EXISTS tpcc")
        cur.execute("CREATE DATABASE tpcc")
    t0 = time.time()
    bb = benchbase("mysql_load", args.warehouses, "unlimited", 60, args.terminals, "load")
    if bb.wait() != 0:
        raise SystemExit("BenchBase load failed; see ~/bb/results/mysql_load/benchbase.log")
    with connect_mysql() as c, c.cursor() as cur:
        rows = 0
        for s in TPCC_MYSQL:
            cur.execute(f"SELECT count(*) FROM {s.name}")
            rows += cur.fetchone()[0]
    print(f"loaded {args.warehouses} warehouses, {rows:,} keyed rows in {time.time() - t0:.0f} s")
    copy_db("tpcc", "tpcc_base")  # every run starts from the same data


def restore() -> None:
    copy_db("tpcc_base", "tpcc")


def _mysql_rows(name: str, cols: list[str]):
    import pymysql.cursors

    c = connect_mysql()
    with c.cursor(pymysql.cursors.SSDictCursor) as cur:
        cur.execute(f"SELECT {', '.join(cols)} FROM {name}")
        yield from cur
    c.close()


def _scan_one(name: str) -> Table:
    # MySQL's text protocol prints FLOAT with 6 significant digits (10110.35 reads as 10110.3), so a plain
    # SELECT is lossy; widening to DOUBLE on the server is exact, and canonical rounding to float32 undoes it.
    spec = TPCC_MYSQL_BY_NAME[name]
    f32 = {c.name for c in spec.columns if isinstance(c.type, canon.Float) and c.type.bits == 32}
    cols = [f"CAST({c} AS DOUBLE) AS {c}" if c in f32 else c for c in spec.column_names]
    return scan_table([(spec, _mysql_rows(spec.name, cols))])


def scan_mysql() -> Table:
    """Every verified table, one process per table (REPEATABLE READ is per connection, so callers scan a
    quiesced source, as the final ground truth does)."""
    with ProcessPoolExecutor(len(TPCC_MYSQL)) as pool:
        parts = list(pool.map(_scan_one, [s.name for s in TPCC_MYSQL]))
    t = parts[0]
    for p in parts[1:]:
        t = t.concat(p)
    return t


def register_connector(name: str, topic: str) -> None:
    requests.delete(f"{CONNECT}/connectors/{name}", timeout=30)
    cfg = {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "database.hostname": MYSQL_CONNECT_HOST, "database.port": str(MYSQL_CONNECT_PORT),
        "database.user": "root", "database.password": "deltaproof",
        "database.server.id": str(random.randint(10_000, 2**31 - 1)), "topic.prefix": "dpm",
        "database.include.list": "tpcc", "table.include.list": ",".join(f"tpcc.{s.name}" for s in TPCC_MYSQL),
        "schema.history.internal.kafka.bootstrap.servers": _get("CONNECT_KAFKA", KAFKA),
        "schema.history.internal.kafka.topic": f"{name}_schema_history",
        "snapshot.mode": "initial", "decimal.handling.mode": "string",
        "time.precision.mode": "adaptive_time_microseconds", "tombstones.on.delete": "false",
        "max.batch.size": "8192", "max.queue.size": "65536",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter", "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter", "value.converter.schemas.enable": "false",
        "transforms": "route", "transforms.route.type": "io.debezium.transforms.ByLogicalTableRouter",
        "transforms.route.topic.regex": "dpm\\.tpcc\\.(.*)", "transforms.route.topic.replacement": topic,
        "transforms.route.key.enforce.uniqueness": "false",
    }
    r = requests.post(f"{CONNECT}/connectors", json={"name": name, "config": cfg}, timeout=60)
    r.raise_for_status()


def drop_connector(name: str) -> None:
    requests.delete(f"{CONNECT}/connectors/{name}", timeout=30)


# ---------------------------------------------------------------- PostgreSQL target


class PGTarget:
    """PostgreSQL target. As on Cassandra, rows carry src_v and deletes leave a versioned tombstone.
    Writes and confirmation reads use separate connections: they run on different threads."""

    def __init__(self, fresh: bool = True):
        import psycopg

        if fresh:  # a new, empty target database; otherwise attach to the existing one
            with psycopg.connect(f"host={PGT_HOST} port={PGT_PORT} user=postgres password=deltaproof dbname=postgres",
                                 autocommit=True) as admin:
                admin.execute("DROP DATABASE IF EXISTS tpcc_target WITH (FORCE)")
                admin.execute("CREATE DATABASE tpcc_target")
        self.w = psycopg.connect(PGT_URL)
        self.r = psycopg.connect(PGT_URL, autocommit=True)

    def create(self) -> None:
        with self.w.transaction():
            for spec in TPCC_MYSQL:
                self.w.execute(spec.pg_ddl("src_v bigint NOT NULL"))
            self.w.execute("CREATE TABLE dp_tombstones (khi bigint, klo bigint, src_v bigint NOT NULL, "
                           "PRIMARY KEY (khi, klo))")

    @staticmethod
    def _upsert(spec) -> str:
        cols = spec.column_names + ["src_v"]
        sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in spec.key)
        return (f"INSERT INTO {spec.name} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))}) "
                f"ON CONFLICT ({', '.join(spec.key)}) DO UPDATE SET {sets}")

    def write(self, ops: list) -> None:
        """ops: (spec, row or None, key parts, version), distinct keys; one transaction per batch."""
        ups, dels, tombs = {}, {}, []
        for spec, row, parts, v in ops:
            if row is None:
                dels.setdefault(spec, []).append(tuple(parts))
                tombs.append((*live_cluster._signed(canon.pack_key(spec.tag, tuple(parts))), v))
            else:
                ups.setdefault(spec, []).append([row.get(c) for c in spec.column_names] + [v])
        with self.w.transaction(), self.w.cursor() as cur:
            for spec, rows in ups.items():
                cur.executemany(self._upsert(spec), rows)
            for spec, keys in dels.items():
                where = " AND ".join(f"{k} = %s" for k in spec.key)
                cur.executemany(f"DELETE FROM {spec.name} WHERE {where}", keys)
            if tombs:
                cur.executemany("INSERT INTO dp_tombstones (khi, klo, src_v) VALUES (%s, %s, %s) "
                                "ON CONFLICT (khi, klo) DO UPDATE SET src_v = EXCLUDED.src_v", tombs)

    def read_many(self, spec, parts_list: list) -> dict:
        """key parts -> (src_v, row) for the keys present, in one query."""
        k = len(spec.key)
        arrays = [[p[i] for p in parts_list] for i in range(k)]
        un = ", ".join(f"%s::int[]" for _ in range(k))
        on = " AND ".join(f"t.{c} = u.k{i}" for i, c in enumerate(spec.key))
        cols = ", ".join(f"t.{c}" for c in spec.column_names + ["src_v"])
        q = f"SELECT {cols} FROM {spec.name} t JOIN unnest({un}) AS u({', '.join(f'k{i}' for i in range(k))}) ON {on}"
        out = {}
        for r in self.r.execute(q, arrays):
            row = dict(zip(spec.column_names + ["src_v"], r))
            out[tuple(row[c] for c in spec.key)] = (row.pop("src_v"), row)
        return out

    def tombstones(self, keys: list) -> dict:
        signed = [live_cluster._signed(k) for k in keys]
        q = ("SELECT t.khi, t.klo, t.src_v FROM dp_tombstones t JOIN unnest(%s::bigint[], %s::bigint[]) AS u(h, l) "
             "ON t.khi = u.h AND t.klo = u.l")
        found = {(h, l): v for h, l, v in self.r.execute(q, [[s[0] for s in signed], [s[1] for s in signed]])}
        return {k: found.get(s, 0) for k, s in zip(keys, signed)}

    def close(self) -> None:
        self.w.close()
        self.r.close()


# ---------------------------------------------------------------- live run


class MySQLPGLive(ClusterLive):
    specs_by_name = TPCC_MYSQL_BY_NAME
    decode = staticmethod(debezium_row)  # MySQL TIMESTAMP arrives as an ISO-8601 UTC string; normalize parses it
    bootstrap = KAFKA

    def confirm_many(self, target: PGTarget, keys: list) -> list:
        """Version-aware confirmation with versioned deletes, as on Cassandra: an absent row is
        (tombstone version, absent), or (0, absent) if never deleted."""
        by_spec: dict = {}
        for key in keys:
            tag, parts = canon.unpack_key(*key, 4)
            spec = TPCC_MYSQL_BY_TAG[tag]
            by_spec.setdefault(spec, []).append((key, tuple(parts[: len(spec.key)])))
        state, absent = {}, []
        for spec, items in by_spec.items():
            rows = target.read_many(spec, [p for _, p in items])
            for key, parts in items:
                hit = rows.get(parts)
                if hit is None:
                    absent.append(key)
                else:
                    state[key] = (hit[0], spec.element(hit[1])[2])
        if absent:
            for key, v in target.tombstones(absent).items():
                state[key] = (v, None)
        self.read_states.update(state)
        return [key for key in keys if not self.state_valid(key, state[key])]

    def full_rescan(self, pg, target):
        cache = {}

        def rescan(partitions: int, p: int):
            if "t" not in cache:
                print("  rescan: reading both stores in full", flush=True)
                cache["s"], cache["t"] = scan_mysql(), scan(PGT_URL, list(TPCC_MYSQL), 8)
            from deltaproof.hashing import key_hash

            s, t = cache["s"], cache["t"]
            if partitions == 1:
                return s, t
            P, pp = np.uint64(partitions), np.uint64(p)
            return s.take(key_hash(s.khi, s.klo) % P == pp), t.take(key_hash(t.khi, t.klo) % P == pp)

        return rescan


def run_baseline(args) -> None:
    restore()
    tag = f"baseline_r{args.rate}" + args.suffix
    bb = benchbase(tag, args.warehouses, args.rate, args.seconds, args.terminals, "execute")
    bb.wait()
    out = {"pipeline": "mysql->postgresql", "mode": "baseline", "rate": args.rate, "seconds": args.seconds,
           "terminals": args.terminals, "warehouses": args.warehouses, **benchbase_summary(tag)}
    live_cluster.write(tag, out, [], [], sub="mysql_pg")


def run_live(args) -> None:
    restore()
    tag = f"live_r{args.rate}_l{args.lag:g}" + (f"_o{args.reorder:g}" if args.reorder else "") + args.suffix
    name = f"dpm_{int(time.time())}"
    topic = f"{name}_changes"
    target = PGTarget()
    target.create()
    live = MySQLPGLive(args, topic)
    out = {"pipeline": "mysql->postgresql", "mode": "live", "no_tombstones": args.no_tombstones,
           "rate": args.rate, "seconds": args.seconds,
           "terminals": args.terminals, "warehouses": args.warehouses, "lag_s": args.lag, "reorder": args.reorder,
           "drop": args.drop, "corrupt": args.corrupt, "interval_s": args.interval, "rateless_symbols": args.symbols}
    register_connector(name, topic)
    live.t_start = time.monotonic()
    threads = [threading.Thread(target=live.poller, daemon=True),
               threading.Thread(target=live.applier, args=(target,), daemon=True)]
    for t in threads:
        t.start()
    print("Debezium snapshot (backfill) running", flush=True)
    t0 = time.monotonic()
    live.snapshot_done.wait()
    while live.queue or live.deferred:  # backfill fully applied
        time.sleep(1)
    out["backfill_events"] = live.snapshot_events
    out["backfill_s"] = round(time.monotonic() - t0, 1)
    r = live.verify_now(None, target, "after_backfill")["rec"]
    print(f"  backfill done: {live.snapshot_events:,} rows in {out['backfill_s']} s; "
          f"reported={r['reported']} false={r['reported_false']}", flush=True)

    live.stats.lag_samples.clear()
    bb = benchbase(tag, args.warehouses, args.rate, args.seconds, args.terminals, "execute")
    print(f"TPC-C running ({args.seconds}s)", flush=True)
    next_verify = time.monotonic() + args.interval
    while bb.poll() is None:
        if time.monotonic() >= next_verify:
            r = live.verify_now(None, target, "live")["rec"]
            print(f"  t={r['t']}s cand={r['candidates']} reported={r['reported']} false={r['reported_false']} "
                  f"missed={r['missed']} ladder={r['bytes']}B riblt={r['riblt_bytes']}B "
                  f"backlog={len(live.queue) + len(live.deferred)}", flush=True)
            next_verify = time.monotonic() + args.interval
        time.sleep(0.2)
    print("TPC-C finished; draining the change stream", flush=True)
    out.update(benchbase_summary(tag))
    time.sleep(10)
    live.stop_polling.set()
    for t in threads:
        t.join()
    final = live.verify_now(None, target, "final")

    print("final full scans for ground truth", flush=True)
    s_scan, t_scan = scan_mysql(), scan(PGT_URL, list(TPCC_MYSQL), 8)
    a, b = exact_diff(s_scan, t_scan)
    truth = {(h, l) for h, l, _ in a} | {(h, l) for h, l, _ in b}
    rebuilt_s, rebuilt_t = Sketch(live.cfg), Sketch(live.cfg)
    rebuilt_s.update(s_scan.khi, s_scan.klo, s_scan.dig)
    rebuilt_t.update(t_scan.khi, t_scan.klo, t_scan.dig)
    rr_s, rr_t = RatelessSketch(args.symbols), RatelessSketch(args.symbols)
    rr_s.update(s_scan.khi, s_scan.klo, s_scan.dig)
    rr_t.update(t_scan.khi, t_scan.klo, t_scan.dig)
    same = lambda x, y: all(np.array_equal(p, q) for p, q in zip(x.arrays, y.arrays))
    out.update(live_cluster.summarize(live, final, s_scan, t_scan, truth))
    out["ladder_sketches_match_rescan"] = sketches_equal(live.src, rebuilt_s) and sketches_equal(live.tgt, rebuilt_t)
    out["riblt_sketches_match_rescan"] = same(live.src_r, rr_s) and same(live.tgt_r, rr_t)
    drop_connector(name)
    target.close()
    live_cluster.write(tag, out, live.verifications, live.reports, live.misses, sub="mysql_pg",
                       followups=live.followups)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["load", "baseline", "live"])
    p.add_argument("--warehouses", type=int, default=4)
    p.add_argument("--rate", default="100")
    p.add_argument("--seconds", type=int, default=300)
    p.add_argument("--terminals", type=int, default=16)
    p.add_argument("--lag", type=float, default=2.0)
    p.add_argument("--reorder", type=float, default=0.0)
    p.add_argument("--drop", type=float, default=0.0005)
    p.add_argument("--corrupt", type=float, default=0.0005)
    p.add_argument("--interval", type=float, default=10.0)
    p.add_argument("--symbols", type=int, default=1 << 18)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--suffix", default="")
    p.add_argument("--ladder-every", type=int, default=1)
    p.add_argument("--no-tombstones", action="store_true", help="ablation: ignore delete versions in confirmation")
    p.add_argument("--split", action="store_true",
                   help="run the verifier and the replicator in separate processes (bench/live_split.py)")
    args = p.parse_args()
    if args.mode == "live" and args.split:
        import live_split

        live_split.run(args, "mysql_pg")
        return
    {"load": run_load, "baseline": run_baseline, "live": run_live}[args.mode](args)


if __name__ == "__main__":
    main()
