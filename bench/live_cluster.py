"""Live migration verification on a multi-machine cluster (infra/gcp/cluster.sh + services.sh).

Runs on the app VM:
    ~/venv/bin/python ~/deltaproof/bench/live_cluster.py load --warehouses 16
    ~/venv/bin/python ~/deltaproof/bench/live_cluster.py baseline --rate 1000 --seconds 300
    ~/venv/bin/python ~/deltaproof/bench/live_cluster.py live --rate 1000 --seconds 300 --lag 2

Pipeline under test, as in a production migration:
  BenchBase TPC-C -> PostgreSQL -> Debezium (pgoutput) -> Kafka -> replicator -> Cassandra (RF 3)
Debezium's initial snapshot is the backfill, followed by the change stream. Every table is routed
into one single-partition topic, so the Kafka offset is a version in commit order (a change's own
LSN is not: a transaction that writes first can commit later). The replicator writes each row
with that version (src_v) at QUORUM and injects drops, corruptions and, with --reorder, deferred
changes. Verification reuses bench/live_e2e.py's protocol: watermark, sketch comparison, and
version-aware confirmation reading the target only. Both a sketch ladder and a rateless IBLT are
maintained on each side and compared at every check.
"""

from __future__ import annotations

import argparse
import copy
import heapq
import json
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import numpy as np
import requests

try:  # several times faster than json for Debezium's change events
    from orjson import loads as _loads
except ImportError:  # pragma: no cover
    _loads = json.loads

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import live_e2e  # noqa: E402
from live_e2e import FAULTY, Live, Target, sketches_equal  # noqa: E402

from deltaproof import canon  # noqa: E402
from deltaproof.parallel import scan  # noqa: E402
from deltaproof.relational import TPCC, TPCC_BY_NAME  # noqa: E402
from deltaproof.sizing import RatelessSketch  # noqa: E402
from deltaproof.sketch import Sketch  # noqa: E402
from deltaproof.store import exact_diff  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
HOME = Path.home()
_env_file = HOME / "cluster.env"  # written by infra/gcp/services.sh; absent when imported elsewhere
ENV = dict(kv.split("=") for kv in _env_file.read_text().split()) if _env_file.exists() else {}
PG_IP, KAFKA_IP = ENV.get("PG_IP", "127.0.0.1"), ENV.get("KAFKA_IP", "127.0.0.1")
CASS_IPS = [x for x in ENV.get("CASS_IPS", "127.0.0.1").split(",") if x]
PG_URL = f"postgresql://postgres:deltaproof@{PG_IP}:5432/tpcc"
CASS_URL = f"cassandra://{CASS_IPS[0]}:9042/tpcc?consistency=QUORUM"
CONNECT = f"http://{KAFKA_IP}:8083"
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------- environment


def benchbase(tag: str, warehouses: int, rate, seconds: int, terminals: int, phase: str) -> subprocess.Popen:
    cfg = (ROOT / "bench" / "tpcc" / "config.xml.tmpl").read_text().replace("postgres:5432", f"{PG_IP}:5432")
    cfg = cfg.replace("{warehouses}", str(warehouses)).replace("{terminals}", str(terminals))
    cfg = cfg.replace("{seconds}", str(seconds)).replace("{rate}", str(rate))
    bb = HOME / "bb"
    (bb / "results" / tag).mkdir(parents=True, exist_ok=True)
    for old in (bb / "results" / tag).glob("*"):
        old.unlink()
    (bb / f"{tag}.xml").write_text(cfg)
    flags = ["--create=true", "--load=true"] if phase == "load" else ["--execute=true"]
    log = open(bb / "results" / tag / "benchbase.log", "w")
    return subprocess.Popen(
        ["sudo", "docker", "run", "--rm", "--network", "host", "-v", f"{bb}:/ext", "-v", f"{HOME}/benchbase-postgres:/bb",
         "-w", "/bb", "eclipse-temurin:23-jre", "java", "-jar", "benchbase.jar", "-b", "tpcc", "-c", f"/ext/{tag}.xml",
         *flags, "-d", f"/ext/results/{tag}"], stdout=log, stderr=subprocess.STDOUT)


def benchbase_summary(tag: str) -> dict:
    files = sorted((HOME / "bb" / "results" / tag).glob("*.summary.json"))
    s = json.loads(files[-1].read_text())
    lat = s.get("Latency Distribution", {})
    pick = lambda name: next((v for k, v in lat.items() if name in k), None)
    return {"tpcc_throughput_tps": round(s.get("Throughput (requests/second)", 0), 2),
            "tpcc_goodput_tps": round(s.get("Goodput (requests/second)", 0), 2),
            "tpcc_latency_p50_ms": round(pick("Median") / 1000, 2) if pick("Median") else None,
            "tpcc_latency_p95_ms": round(pick("95th") / 1000, 2) if pick("95th") else None,
            "tpcc_latency_p99_ms": round(pick("99th") / 1000, 2) if pick("99th") else None}


def pg_admin():
    import psycopg

    return psycopg.connect(f"host={PG_IP} user=postgres password=deltaproof dbname=postgres", autocommit=True)


def connect_pg():
    import psycopg

    return psycopg.connect(f"host={PG_IP} user=postgres password=deltaproof dbname=tpcc", autocommit=True)


def connect_cassandra():
    from cassandra import ConsistencyLevel
    from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile

    profile = ExecutionProfile(consistency_level=ConsistencyLevel.QUORUM, request_timeout=120)
    cluster = Cluster(CASS_IPS, execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    return cluster, cluster.connect()


def _signed(key) -> tuple[int, int]:
    """A 128-bit packed key as two signed 64-bit integers, for Cassandra bigint columns."""
    return tuple(x - (1 << 64) if x >= 1 << 63 else x for x in key)


class ClusterTarget(Target):
    """Cassandra target. Deletes leave a tombstone row carrying the delete's version, so an absent row
    has a version too and confirmation can tell a dropped insert from a delete applied early."""

    def create(self) -> None:
        ks = live_e2e.KEYSPACE
        self.s.execute(f"DROP KEYSPACE IF EXISTS {ks}", timeout=300)
        self.s.execute(f"CREATE KEYSPACE {ks} WITH replication = "
                       f"{{'class': 'SimpleStrategy', 'replication_factor': {min(3, len(CASS_IPS))}}}")
        for spec in TPCC:
            self.s.execute(spec.cql_ddl(ks), timeout=120)
            self.s.execute(f"ALTER TABLE {ks}.{spec.name} ADD src_v bigint", timeout=120)
        self.s.execute(f"CREATE TABLE {ks}.dp_tombstones (khi bigint, klo bigint, src_v bigint, PRIMARY KEY ((khi, klo)))",
                       timeout=120)
        self.prepare()

    def prepare(self) -> None:
        super().prepare()
        ks = live_e2e.KEYSPACE
        self.tomb_ins = self.s.prepare(f"INSERT INTO {ks}.dp_tombstones (khi, klo, src_v) VALUES (?, ?, ?)")
        self.tomb_sel = self.s.prepare(f"SELECT src_v FROM {ks}.dp_tombstones WHERE khi = ? AND klo = ?")

    def write(self, ops: list) -> None:
        super().write(ops)
        tombs = [(spec, parts, v) for spec, row, parts, v in ops if row is None]
        if tombs:
            from cassandra.concurrent import execute_concurrent

            stmts = [(self.tomb_ins, [*_signed(canon.pack_key(spec.tag, tuple(parts))), v]) for spec, parts, v in tombs]
            for ok, res in execute_concurrent(self.s, stmts, concurrency=128, raise_on_first_error=False):
                if not ok:
                    raise res


def create_topic(topic: str, partitions: int, bootstrap: str | None = None) -> None:
    """Create the change topic up front, so Debezium writes to `partitions` partitions keyed by row key."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap or f"{KAFKA_IP}:9092"})
    for f in admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)]).values():
        f.result(timeout=60)


def register_connector(name: str, topic: str) -> None:
    requests.delete(f"{CONNECT}/connectors/{name}", timeout=30)
    tables = ",".join(f"public.{s.name}" for s in TPCC)
    cfg = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector", "plugin.name": "pgoutput",
        "database.hostname": PG_IP, "database.port": "5432", "database.user": "postgres",
        "database.password": "deltaproof", "database.dbname": "tpcc", "topic.prefix": "dp",
        "slot.name": name, "publication.name": f"{name}_pub", "publication.autocreate.mode": "filtered",
        "table.include.list": tables, "snapshot.mode": "initial", "decimal.handling.mode": "string",
        "time.precision.mode": "adaptive_time_microseconds", "tombstones.on.delete": "false",
        "max.batch.size": "8192", "max.queue.size": "65536",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter", "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter", "value.converter.schemas.enable": "false",
        "transforms": "route", "transforms.route.type": "io.debezium.transforms.ByLogicalTableRouter",
        "transforms.route.topic.regex": "dp\\.public\\.(.*)", "transforms.route.topic.replacement": topic,
        "transforms.route.key.enforce.uniqueness": "false",
    }
    r = requests.post(f"{CONNECT}/connectors", json={"name": name, "config": cfg}, timeout=60)
    r.raise_for_status()


def drop_connector(name: str) -> None:
    requests.delete(f"{CONNECT}/connectors/{name}", timeout=30)
    time.sleep(5)
    with connect_pg() as pg:
        pg.execute("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = %s", (name,))
        pg.execute(f"DROP PUBLICATION IF EXISTS {name}_pub")


def debezium_row(spec, d: dict | None) -> dict | None:
    """Debezium JSON values -> the logical values the canonical types expect."""
    if d is None:
        return None
    row = dict(d)
    for c in spec.columns:
        v = row.get(c.name)
        if v is None:
            continue
        if isinstance(c.type, canon.Timestamp) and isinstance(v, int):  # MicroTimestamp
            row[c.name] = _EPOCH + timedelta(microseconds=v)
        elif isinstance(c.type, canon.Date) and isinstance(v, int):  # days since epoch
            row[c.name] = (_EPOCH + timedelta(days=v)).date()
        elif isinstance(c.type, canon.Dec) and isinstance(v, str):
            row[c.name] = Decimal(v)
    return row


# ---------------------------------------------------------------- live run on the cluster


class ClusterLive(Live):
    # What a subclass for another source changes: its table specs, how Debezium's JSON values become
    # the logical values those specs expect, and where Kafka is.
    specs_by_name = TPCC_BY_NAME
    decode = staticmethod(debezium_row)
    bootstrap = f"{KAFKA_IP}:9092"

    def __init__(self, args, topic: str):
        super().__init__(args)
        self.topic = topic
        self.src_r, self.tgt_r = RatelessSketch(args.symbols), RatelessSketch(args.symbols)
        self.snapshot_done = threading.Event()
        self.snapshot_events = 0
        self.ts_lag_ms: list = []  # commit -> applied, from Debezium's source.ts_ms
        # What this process does with the stream. One process does both; with the verifier in its own process
        # (bench/live_split.py) the replicator only feeds its queue and the verifier only maintains the source side.
        self.do_source, self.do_queue, self.role = True, True, "both"
        self.max_polled = 0  # highest version read from the stream
        # The Kafka partition this process reads. With one partition, offsets follow the source's commit order; with
        # several (bench/live_sharded.py), Debezium routes each key to one partition, and a partition's offsets
        # follow commit order for its keys, so each partition is verified as an independent instance.
        self.partition = 0

    def src_update(self, khi, klo, dig, sign):
        super().src_update(khi, klo, dig, sign)
        self.src_r.update(khi, klo, dig, sign)

    def tgt_update(self, khi, klo, dig, sign):
        super().tgt_update(khi, klo, dig, sign)
        self.tgt_r.update(khi, klo, dig, sign)

    def poller(self) -> None:
        from confluent_kafka import Consumer, TopicPartition

        c = Consumer({"bootstrap.servers": self.bootstrap, "group.id": f"{self.topic}_{self.role}",
                      "auto.offset.reset": "earliest",
                      "enable.auto.commit": False, "fetch.max.bytes": 64 << 20})
        c.assign([TopicPartition(self.topic, self.partition, 0)])
        idle = 0
        while True:
            t0 = time.thread_time()
            msgs = c.consume(num_messages=5000, timeout=0.5)
            arrived = time.monotonic()
            batch, src, hist, top = [], [], [], 0
            for m in msgs:
                if m.error() or m.value() is None:
                    continue
                ev = _loads(m.value())
                s = ev.get("source", {})
                spec = self.specs_by_name.get(s.get("table"))
                if spec is None:
                    self.stats.skipped_history += 1
                    continue
                v = m.offset() + 1  # version in commit order; 0 is reserved for "no change seen"
                old, new = self.decode(spec, ev.get("before")), self.decode(spec, ev.get("after"))
                if ev.get("op") in ("r", "c"):
                    old = None
                parts = tuple(int((new or old)[k]) for k in spec.key)
                key = canon.pack_key(spec.tag, parts)
                if self.do_source:
                    t1 = time.perf_counter()
                    new_e = spec.element(new) if new is not None else None
                    if old is not None:
                        src.append((v, *spec.element(old), -1))
                    if new_e is not None:
                        src.append((v, *new_e, +1))
                    self.stats.source_sketch_s += time.perf_counter() - t1
                    hist.append((key, v, new_e[2] if new_e else None))
                if self.do_queue:
                    batch.append((arrived, v, spec, spec.normalize(new) if new is not None else None, parts, key))
                top = v
                if ev.get("op") == "r":  # snapshot read: part of the backfill
                    self.snapshot_events += 1
                if s.get("snapshot") == "last":
                    self.snapshot_done.set()
            self.stats.changes += len(batch) if self.do_queue else len(hist)
            with self.slock:
                self.src_pending.extend(src)
                for key, v, d in hist:
                    self.src_later.setdefault(key, []).append((v, d))
                    heapq.heappush(self.src_due, (v, key))
            with self.qlock:
                self.queue.extend(batch)
                self.stats.max_backlog = max(self.stats.max_backlog, len(self.queue) + len(self.deferred))
            self.max_polled = max(self.max_polled, top)  # after the batch is visible to the verifier
            self.stats.poll_cpu_s += time.thread_time() - t0
            if not msgs:
                if self.stop_polling.is_set():
                    idle += 1
                    if idle >= 6:
                        break
            else:
                idle = 0
        c.close()
        self.poll_done.set()

    def snapshot_extra(self):
        return copy.deepcopy(self.tgt_r)  # target side, same frozen instant as the ladder's target copy

    def compare_extra(self, state, candidates: set) -> dict:
        # Called after the source has been advanced to the same watermark; only the verifier changes it.
        t0 = time.perf_counter()
        try:
            r = self.src_r.compare(state)
        except RuntimeError:  # more differences than the symbols decode: the check falls back to the ladder
            return {"riblt_bytes": None, "riblt_symbols": None, "riblt_decode_ms": None,
                    "riblt_candidates_equal_ladder": None}
        return {"riblt_bytes": r.bytes, "riblt_symbols": r.cells,
                "riblt_decode_ms": round((time.perf_counter() - t0) * 1000, 1),
                "riblt_candidates_equal_ladder": (r.keys == candidates) if candidates is not None else None,
                "riblt_keys": r.keys}

    def state_valid(self, key, state: tuple) -> bool:
        """Is the target's (version, digest) a state the source held at or after w_T? With --no-tombstones
        (ablation), an absent row matches any deleted source state whatever its version, as it did before
        deletes carried versions: a dropped insert whose delete is still in flight is then accepted."""
        valid = self.valid_states(key)
        if state[1] is None and getattr(self.a, "no_tombstones", False):
            return any(d is None for _, d in valid)
        return state in valid

    def confirm_many(self, target, keys: list) -> list:
        """Version-aware confirmation with versioned deletes: an absent row is (tombstone version, absent),
        or (0, absent) if the key was never deleted, and must be one of the source's states at or after w_T."""
        from cassandra.concurrent import execute_concurrent

        specs, stmts = [], []
        for key in keys:
            tag, parts = canon.unpack_key(*key, 4)
            spec = live_e2e.TPCC_BY_TAG[tag]
            specs.append(spec)
            stmts.append((target.sel[spec.name], list(parts[: len(spec.key)])))
        rows = [res.one() for ok, res in execute_concurrent(target.s, stmts, concurrency=128, raise_on_first_error=True)]
        absent = [k for k, r in zip(keys, rows) if r is None]
        tomb = {}
        if absent:
            res = execute_concurrent(target.s, [(target.tomb_sel, list(_signed(k))) for k in absent],
                                     concurrency=128, raise_on_first_error=True)
            for k, (ok, r) in zip(absent, res):
                one = r.one()
                tomb[k] = one[0] if one else 0
        reported = []
        for key, spec, r in zip(keys, specs, rows):
            if r is None:
                state = (tomb[key], None)
            else:
                row = dict(zip(spec.column_names + ["src_v"], r))
                v_t = row.pop("src_v")
                state = (v_t, spec.element(row)[2])
            self.read_states[key] = state
            if not self.state_valid(key, state):
                reported.append(key)
        return reported

    def full_rescan(self, pg, target):
        cache = {}

        def rescan(partitions: int, p: int):
            if "t" not in cache:
                print("  rescan: reading both stores in full", flush=True)
                cache["s"], cache["t"] = scan(PG_URL, list(TPCC), 8), scan(CASS_URL, list(TPCC), 8)
            from deltaproof.hashing import key_hash

            s, t = cache["s"], cache["t"]
            if partitions == 1:
                return s, t
            P, pp = np.uint64(partitions), np.uint64(p)
            return s.take(key_hash(s.khi, s.klo) % P == pp), t.take(key_hash(t.khi, t.klo) % P == pp)

        return rescan


def run_load(args) -> None:
    with pg_admin() as admin:
        admin.execute("DROP DATABASE IF EXISTS tpcc WITH (FORCE)")
        admin.execute("CREATE DATABASE tpcc")
    t0 = time.time()
    bb = benchbase("load", args.warehouses, "unlimited", 60, args.terminals, "load")
    if bb.wait() != 0:
        raise SystemExit("BenchBase load failed; see ~/bb/results/load/benchbase.log")
    with connect_pg() as pg:
        for spec in TPCC:
            pg.execute(f"ALTER TABLE {spec.name} REPLICA IDENTITY FULL")
        pg.execute("VACUUM ANALYZE")
        rows = sum(pg.execute(f"SELECT count(*) FROM {s.name}").fetchone()[0] for s in TPCC)
    print(f"loaded {args.warehouses} warehouses, {rows:,} keyed rows in {time.time() - t0:.0f} s")
    with pg_admin() as admin:  # keep a pristine copy so every run starts from the same data
        admin.execute("DROP DATABASE IF EXISTS tpcc_base WITH (FORCE)")
        admin.execute("CREATE DATABASE tpcc_base TEMPLATE tpcc")


def restore() -> None:
    with pg_admin() as admin:
        admin.execute("DROP DATABASE IF EXISTS tpcc WITH (FORCE)")
        admin.execute("CREATE DATABASE tpcc TEMPLATE tpcc_base")


def run_baseline(args) -> None:
    restore()
    tag = f"baseline_r{args.rate}" + args.suffix
    bb = benchbase(tag, args.warehouses, args.rate, args.seconds, args.terminals, "execute")
    bb.wait()
    out = {"mode": "baseline", "rate": args.rate, "seconds": args.seconds, "terminals": args.terminals,
           "warehouses": args.warehouses, **benchbase_summary(tag)}
    write(tag, out, [], [])


def run_live(args) -> None:
    restore()
    tag = f"live_r{args.rate}_l{args.lag:g}" + (f"_o{args.reorder:g}" if args.reorder else "") + args.suffix
    name = f"dp_{int(time.time())}"
    topic = f"{name}_changes"
    cluster, session = connect_cassandra()
    target = ClusterTarget(session)
    target.create()
    live = ClusterLive(args, topic)
    out = {"mode": "live", "rate": args.rate, "seconds": args.seconds, "terminals": args.terminals,
           "warehouses": args.warehouses, "lag_s": args.lag, "reorder": args.reorder, "drop": args.drop,
           "corrupt": args.corrupt, "interval_s": args.interval, "rateless_symbols": args.symbols,
           "cassandra_nodes": len(CASS_IPS), "no_tombstones": args.no_tombstones}
    register_connector(name, topic)
    pg = connect_pg()
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
    r = live.verify_now(pg, target, "after_backfill")["rec"]
    print(f"  backfill done: {live.snapshot_events:,} rows in {out['backfill_s']} s; "
          f"reported={r['reported']} false={r['reported_false']}", flush=True)

    live.stats.lag_samples.clear()  # report apply delay for the live phase, not the backfill
    bb = benchbase(tag, args.warehouses, args.rate, args.seconds, args.terminals, "execute")
    print(f"TPC-C running ({args.seconds}s)", flush=True)
    next_verify = time.monotonic() + args.interval
    while bb.poll() is None:
        if time.monotonic() >= next_verify:
            r = live.verify_now(pg, target, "live")["rec"]
            print(f"  t={r['t']}s cand={r['candidates']} reported={r['reported']} false={r['reported_false']} "
                  f"missed={r['missed']} ladder={r['bytes']}B riblt={r['riblt_bytes']}B "
                  f"backlog={len(live.queue) + len(live.deferred)}", flush=True)
            next_verify = time.monotonic() + args.interval
        time.sleep(0.2)
    print("TPC-C finished; draining the change stream", flush=True)
    out.update(benchbase_summary(tag))
    time.sleep(10)  # let Debezium publish the tail of the stream
    live.stop_polling.set()
    for t in threads:
        t.join()
    final = live.verify_now(pg, target, "final")

    print("final full scans for ground truth", flush=True)
    s_scan, t_scan = scan(PG_URL, list(TPCC), 8), scan(CASS_URL, list(TPCC), 8)
    a, b = exact_diff(s_scan, t_scan)
    truth = {(h, l) for h, l, _ in a} | {(h, l) for h, l, _ in b}
    rebuilt_s, rebuilt_t = Sketch(live.cfg), Sketch(live.cfg)
    rebuilt_s.update(s_scan.khi, s_scan.klo, s_scan.dig)
    rebuilt_t.update(t_scan.khi, t_scan.klo, t_scan.dig)
    rr_s, rr_t = RatelessSketch(args.symbols), RatelessSketch(args.symbols)
    rr_s.update(s_scan.khi, s_scan.klo, s_scan.dig)
    rr_t.update(t_scan.khi, t_scan.klo, t_scan.dig)
    same = lambda x, y: all(np.array_equal(p, q) for p, q in zip(x.arrays, y.arrays))
    out.update(summarize(live, final, s_scan, t_scan, truth))
    out.update({
        "ladder_sketches_match_rescan": sketches_equal(live.src, rebuilt_s) and sketches_equal(live.tgt, rebuilt_t),
        "riblt_sketches_match_rescan": same(live.src_r, rr_s) and same(live.tgt_r, rr_t),
    })
    drop_connector(name)
    cluster.shutdown()
    write(tag, out, live.verifications, live.reports, live.misses, followups=live.followups)


def _median(xs: list):
    xs = [x for x in xs if x is not None]
    return float(np.median(xs)) if xs else None


def summarize(live, final: dict, s_scan, t_scan, truth: set) -> dict:
    """Run metrics shared by every pipeline: faults, lag, checks, bytes, and the final answer vs. truth."""
    st = live.stats
    lag = np.array(st.lag_samples) if st.lag_samples else np.zeros(1)
    lv = [v for v in live.verifications if v["label"] == "live"]
    found, reported = set(final["candidates"]), set(final["reported"])
    return {
        "changes_captured": st.changes, "dropped": st.dropped, "corrupted": st.corrupted, "deferred": st.deferred,
        "applied_ops": st.applied_ops, "max_backlog": st.max_backlog,
        "apply_delay_p50_s": round(float(np.percentile(lag, 50)), 2),
        "apply_delay_p99_s": round(float(np.percentile(lag, 99)), 2),
        "source_sketches_us_per_change": round(st.source_sketch_s / max(1, st.changes) * 1e6, 1),
        "live_verifications": len(lv),
        "live_candidates_total": sum(v["candidates"] for v in lv),
        "live_reported_total": sum(v["reported"] for v in lv),
        "live_reported_false_total": sum(v["reported_false"] for v in lv),
        "live_reported_while_rewritten_total": sum(v.get("reported_while_rewritten", 0) for v in lv),
        "live_missed_total": sum(v["missed"] for v in lv),
        "live_faulty_being_rewritten_total": sum(v["faulty_being_rewritten"] for v in lv),
        "live_faulty_content_agrees_max": max((v.get("faulty_content_agrees", 0) for v in lv), default=0),
        "live_check_ms_median": float(np.median([v["total_ms"] for v in lv])) if lv else None,
        "live_replicator_paused_ms_median": float(np.median([v["replicator_paused_ms"] for v in lv])) if lv else None,
        "live_target_point_reads_total": sum(v["target_point_reads"] for v in lv),
        "live_cached_verdicts_total": sum(v["cached_verdicts"] for v in lv),
        "live_ladder_checks": sum(1 for v in lv if v.get("ladder_decoded", True)),
        "live_ladder_bytes_median": _median([v["bytes"] for v in lv]),
        "live_riblt_bytes_median": _median([v["riblt_bytes"] for v in lv]),
        "live_riblt_agrees_with_ladder": all(v["riblt_candidates_equal_ladder"] is not False for v in live.verifications),
        "live_riblt_agreement_checks": sum(1 for v in live.verifications if v["riblt_candidates_equal_ladder"] is not None),
        "final_rows_source": len(s_scan), "final_rows_target": len(t_scan), "final_truth_keys": len(truth),
        "final_reported_equals_truth": reported == truth, "final_candidates_equal_truth": found == truth,
    }


def write(tag: str, out: dict, verifications: list, reports: list, misses: list = (), sub: str = "cluster",
          followups: list = (), ever_faulty: set = ()) -> None:
    import csv

    d = ROOT / "results" / sub
    d.mkdir(parents=True, exist_ok=True)
    with (d / f"{tag}.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["metric", "value"])
        w.writerows(out.items())
    for name, rows in (("verifications", verifications), ("unexplained_reports", reports), ("missed", list(misses)),
                       ("report_followups", list(followups)),
                       ("ever_faulty", [{"khi": k[0], "klo": k[1]} for k in sorted(ever_faulty)])):
        if rows:
            keys = list(dict.fromkeys(k for r in rows for k in r))
            with (d / f"{tag}_{name}.csv").open("w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=keys)
                w.writeheader()
                w.writerows(rows)
    for k, v in out.items():
        print(f"{k:38} {v}")
    print(f"wrote {d / tag}.csv")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["load", "baseline", "live"])
    p.add_argument("--warehouses", type=int, default=16)
    p.add_argument("--rate", default="1000")
    p.add_argument("--seconds", type=int, default=300)
    p.add_argument("--terminals", type=int, default=32)
    p.add_argument("--lag", type=float, default=2.0)
    p.add_argument("--reorder", type=float, default=0.0)
    p.add_argument("--drop", type=float, default=0.0005)
    p.add_argument("--corrupt", type=float, default=0.0005)
    p.add_argument("--interval", type=float, default=10.0)
    p.add_argument("--symbols", type=int, default=1 << 18)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--suffix", default="", help="appended to result file names, e.g. _long")
    p.add_argument("--no-tombstones", action="store_true", help="ablation: ignore delete versions in confirmation")
    p.add_argument("--ladder-every", type=int, default=1,
                   help="decode the ladder baseline every N live checks; others use the rateless sketch")
    p.add_argument("--shards", type=int, default=1,
                   help="Kafka partitions, each verified by its own replicator and verifier processes (bench/live_sharded.py)")
    p.add_argument("--split", action="store_true",
                   help="run the verifier and the replicator in separate processes (bench/live_split.py)")
    args = p.parse_args()
    if args.mode == "live" and args.shards > 1:
        import live_sharded

        live_sharded.run(args, "cluster")
        return
    if args.mode == "live" and args.split:
        import live_split

        live_split.run(args, "cluster")
        return
    {"load": run_load, "baseline": run_baseline, "live": run_live}[args.mode](args)


if __name__ == "__main__":
    main()
