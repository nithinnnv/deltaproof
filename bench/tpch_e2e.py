"""End-to-end experiment on TPC-H lineitem: PostgreSQL (source) vs Cassandra (target).

    docker compose -p deltaproof up -d
    python bench/tpch_e2e.py --sf 1 [--per-fault 20]

Data is generated with TPC-H dbgen through DuckDB's tpch extension. These are not TPC-H
benchmark results. Faults are injected with SQL/CQL statements; ground truth is the exact
difference of full scans of both stores.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import sys
import time
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from deltaproof.adapters import postgres_rows  # noqa: E402
from deltaproof.relational import TableSpec, _cols  # noqa: E402
from deltaproof.sketch import LadderConfig, Sketch  # noqa: E402
from deltaproof.store import Table, exact_diff  # noqa: E402
from deltaproof.verify import table_rescan, verify  # noqa: E402

LINEITEM = TableSpec("lineitem", 9, ("l_orderkey", "l_linenumber"), _cols(
    "l_partkey:i l_suppkey:i l_quantity:d2 l_extendedprice:d2 l_discount:d2 l_tax:d2 l_returnflag:c "
    "l_linestatus:c l_shipdate:dt l_commitdate:dt l_receiptdate:dt l_shipinstruct:c l_shipmode:c l_comment:t"))
COLS = LINEITEM.column_names
PG_DDL = """CREATE TABLE lineitem (l_orderkey int NOT NULL, l_linenumber int NOT NULL, l_partkey int NOT NULL,
  l_suppkey int NOT NULL, l_quantity decimal(15,2) NOT NULL, l_extendedprice decimal(15,2) NOT NULL,
  l_discount decimal(15,2) NOT NULL, l_tax decimal(15,2) NOT NULL, l_returnflag char(1) NOT NULL,
  l_linestatus char(1) NOT NULL, l_shipdate date NOT NULL, l_commitdate date NOT NULL, l_receiptdate date NOT NULL,
  l_shipinstruct char(25) NOT NULL, l_shipmode char(10) NOT NULL, l_comment varchar(44) NOT NULL,
  PRIMARY KEY (l_orderkey, l_linenumber))"""


def connect_pg(db: str):
    import psycopg

    return psycopg.connect(f"host=localhost port=55432 user=postgres password=deltaproof dbname={db}", autocommit=True)


def connect_cassandra():
    from cassandra.cluster import Cluster

    t0 = time.time()
    while True:
        try:
            c = Cluster(["127.0.0.1"], port=59042, connect_timeout=20)
            return c, c.connect()
        except Exception:
            if time.time() - t0 > 180:
                raise
            time.sleep(5)


def load(sf: float, pg, cs) -> dict:
    import duckdb
    from cassandra.concurrent import execute_concurrent_with_args

    t0 = time.perf_counter()
    con = duckdb.connect()
    con.execute("INSTALL tpch; LOAD tpch")
    con.execute(f"CALL dbgen(sf={sf})")
    gen_s = time.perf_counter() - t0

    pg.execute("DROP TABLE IF EXISTS lineitem")
    pg.execute(PG_DDL)
    cs.execute("CREATE KEYSPACE IF NOT EXISTS tpch WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    cs.execute("DROP TABLE IF EXISTS tpch.lineitem", timeout=120)
    cs.execute(LINEITEM.cql_ddl("tpch"), timeout=60)
    ins = cs.prepare(f"INSERT INTO tpch.lineitem ({', '.join(COLS)}) VALUES ({', '.join('?' * len(COLS))})")

    cur = con.execute(f"SELECT {', '.join(COLS)} FROM lineitem")
    n, pg_s, cs_s = 0, 0.0, 0.0
    while True:
        chunk = cur.fetchmany(50_000)
        if not chunk:
            break
        t1 = time.perf_counter()
        with pg.cursor() as c, c.copy(f"COPY lineitem ({', '.join(COLS)}) FROM STDIN") as cp:
            for r in chunk:
                cp.write_row(r)
        pg_s += time.perf_counter() - t1
        t1 = time.perf_counter()
        execute_concurrent_with_args(cs, ins, chunk, concurrency=256, raise_on_first_error=True)
        cs_s += time.perf_counter() - t1
        n += len(chunk)
        if n % 1_000_000 < 50_000:
            print(f"  loaded {n} rows", flush=True)
    return {"rows": n, "dbgen_s": round(gen_s, 1), "load_postgres_s": round(pg_s, 1), "load_cassandra_s": round(cs_s, 1)}


def inject(pg, cs, per: int, rng) -> dict[str, set]:
    max_key = pg.execute("SELECT max(l_orderkey) FROM lineitem").fetchone()[0]
    keys = pg.execute(f"SELECT l_orderkey, l_linenumber FROM lineitem TABLESAMPLE SYSTEM (1) LIMIT {6 * per}").fetchall()
    groups = {c: keys[i * per:(i + 1) * per] for i, c in enumerate(["lost_write", "precision", "date_shift", "corruption", "lost_delete"])}
    where = "WHERE l_orderkey = %s AND l_linenumber = %s"
    for k in groups["lost_write"]:
        cs.execute(f"DELETE FROM tpch.lineitem {where}", k)
    for k in groups["precision"]:
        v = pg.execute(f"SELECT l_extendedprice FROM lineitem {where}", k).fetchone()[0]
        cs.execute(f"UPDATE tpch.lineitem SET l_extendedprice = %s {where}", (v + Decimal("0.01"), *k))
    for k in groups["date_shift"]:
        v = pg.execute(f"SELECT l_shipdate FROM lineitem {where}", k).fetchone()[0]
        cs.execute(f"UPDATE tpch.lineitem SET l_shipdate = %s {where}", (v + timedelta(days=1), *k))
    for k in groups["corruption"]:
        cs.execute(f"UPDATE tpch.lineitem SET l_comment = %s {where}", ("corrupted comment", *k))
    for k in groups["lost_delete"]:
        pg.execute(f"DELETE FROM lineitem {where}", k)
    phantom = [(max_key + 1 + i, 1) for i in range(per)]
    day = pg.execute("SELECT DATE '1998-08-02'").fetchone()[0]
    for k in phantom:
        cs.execute(f"INSERT INTO tpch.lineitem ({', '.join(COLS)}) VALUES ({', '.join(['%s'] * len(COLS))})",
                   (*k, 1, 1, Decimal("1.00"), Decimal("1.00"), Decimal("0.00"), Decimal("0.00"), "N", "O",
                    day, day, day, "NONE", "MAIL", "phantom row"))
    groups["phantom"] = phantom
    return {c: {LINEITEM.key_of({"l_orderkey": a, "l_linenumber": b}) for a, b in ks} for c, ks in groups.items()}


def scan(rows) -> tuple[Table, list]:
    kh, kl, dg, raw = [], [], [], []
    for r in rows:
        h, l, d = LINEITEM.element(r)
        kh.append(h)
        kl.append(l)
        dg.append(d)
        raw.append(r)
    u = lambda x: np.array(x, dtype=np.uint64)
    return Table(u(kh), u(kl), u(dg), np.zeros(len(kl), np.int64)), raw


def cassandra_scan(cs):
    from cassandra.query import SimpleStatement

    stmt = SimpleStatement(f"SELECT {', '.join(COLS)} FROM tpch.lineitem", fetch_size=5000)
    for r in cs.execute(stmt, timeout=600):
        yield dict(zip(COLS, r))


def naive(row: dict) -> str:
    return hashlib.blake2b("|".join(str(row[c]) for c in COLS[2:]).encode(), digest_size=8).hexdigest()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--sf", type=float, default=1.0)
    p.add_argument("--per-fault", type=int, default=20)
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()
    admin = connect_pg("postgres")
    admin.execute("DROP DATABASE IF EXISTS tpch WITH (FORCE)")
    admin.execute("CREATE DATABASE tpch")
    pg = connect_pg("tpch")
    cluster, cs = connect_cassandra()
    out = {"sf": args.sf, **load(args.sf, pg, cs)}
    truth_by_class = inject(pg, cs, args.per_fault, np.random.default_rng(args.seed))
    truth = set().union(*truth_by_class.values())

    t0 = time.perf_counter()
    src, src_raw = scan(postgres_rows(pg, "lineitem", COLS))
    out["scan_canon_postgres_s"] = round(time.perf_counter() - t0, 1)
    t0 = time.perf_counter()
    tgt, tgt_raw = scan(cassandra_scan(cs))
    out["scan_canon_cassandra_s"] = round(time.perf_counter() - t0, 1)
    cfg = LadderConfig(max_capacity=max(64, len(src) // 8))
    t0 = time.perf_counter()
    a, b = Sketch(cfg), Sketch(cfg)
    a.update(src.khi, src.klo, src.dig)
    b.update(tgt.khi, tgt.klo, tgt.dig)
    out["sketch_build_s"] = round(time.perf_counter() - t0, 1)
    t0 = time.perf_counter()
    rep = verify(a, b, table_rescan(src, tgt))
    out["verify_s"] = round(time.perf_counter() - t0, 3)
    x, y = exact_diff(src, tgt)
    scan_truth = {(h, l) for h, l, _ in x} | {(h, l) for h, l, _ in y}
    out.update({
        "rows_source": len(src), "rows_target": len(tgt),
        "injected_keys": len(truth), "scan_difference_keys": len(scan_truth),
        "scan_difference_equals_injected": scan_truth == truth,
        "deltaproof_found": len(rep.keys), "deltaproof_recall": round(len(rep.keys & truth) / len(truth), 4),
        "deltaproof_false_positives": len(rep.keys - truth), "deltaproof_bytes": rep.cost.bytes,
        "deltaproof_round_trips": rep.cost.round_trips, "deltaproof_level": rep.level,
    })
    for cls, keys in truth_by_class.items():
        out[f"recall_{cls}"] = len(rep.keys & keys) / len(keys)
    ns = {(r["l_orderkey"], r["l_linenumber"]): naive(r) for r in src_raw}
    nt = {(r["l_orderkey"], r["l_linenumber"]): naive(r) for r in tgt_raw}
    flagged = (ns.keys() ^ nt.keys()) | {k for k in ns.keys() & nt.keys() if ns[k] != nt[k]}
    out["naive_flagged"] = len(flagged)
    out["naive_flagged_fraction"] = round(len(flagged) / len(src), 4)
    for k, v in out.items():
        print(f"{k:34} {v}")
    path = Path(__file__).resolve().parents[1] / "results" / f"tpch_sf{args.sf:g}.csv"
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["metric", "value"])
        w.writerows(out.items())
    print(f"wrote {path}")
    cluster.shutdown()


if __name__ == "__main__":
    main()
