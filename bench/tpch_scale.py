"""Large-scale run on TPC-H lineitem: PostgreSQL (source) vs Cassandra (target), parallel throughout.

    docker compose -p deltaproof up -d
    python bench/tpch_scale.py --sf 10 [--workers 8] [--max-differences 100000]

Unlike bench/tpch_e2e.py, nothing holds raw rows: data is generated into an on-disk DuckDB file,
Cassandra is loaded by parallel processes, both stores are scanned by deltaproof's parallel
scanner, and the sketch is sized for at most --max-differences differences. The ground truth is
the exact difference of the two full scans. These are not TPC-H benchmark results.
"""

from __future__ import annotations

import argparse
import csv
import resource
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from deltaproof.parallel import scan  # noqa: E402
from deltaproof.sketch import LadderConfig, Sketch  # noqa: E402
from deltaproof.store import exact_diff  # noqa: E402
from deltaproof.verify import table_rescan, verify  # noqa: E402
from tpch_e2e import COLS, LINEITEM, PG_DDL, connect_cassandra, inject  # noqa: E402

PG_ADMIN = "host=localhost port=55432 user=postgres password=deltaproof dbname=postgres"


def pg_url(db: str) -> str:
    return f"postgresql://postgres:deltaproof@localhost:55432/{db}"


def generate(sf: float, path: Path) -> int:
    import duckdb

    path.unlink(missing_ok=True)
    con = duckdb.connect(str(path))
    con.execute("INSTALL tpch; LOAD tpch")
    con.execute(f"CALL dbgen(sf={sf})")
    n = con.execute("SELECT count(*) FROM lineitem").fetchone()[0]
    con.close()
    return n


def _load_slice(args) -> int:
    """Insert one l_orderkey range from the DuckDB file into Cassandra."""
    path, keyspace, lo, hi = args
    import duckdb
    from cassandra.cluster import Cluster
    from cassandra.concurrent import execute_concurrent_with_args

    con = duckdb.connect(str(path), read_only=True)
    cluster = Cluster(["127.0.0.1"], port=59042, connect_timeout=30)
    session = cluster.connect()
    ins = session.prepare(f"INSERT INTO {keyspace}.lineitem ({', '.join(COLS)}) VALUES ({', '.join('?' * len(COLS))})")
    cur = con.execute(f"SELECT {', '.join(COLS)} FROM lineitem WHERE l_orderkey >= {lo} AND l_orderkey < {hi}")
    n = 0
    while chunk := cur.fetchmany(20_000):
        execute_concurrent_with_args(session, ins, chunk, concurrency=128, raise_on_first_error=True)
        n += len(chunk)
    cluster.shutdown()
    con.close()
    return n


def load(path: Path, db: str, keyspace: str, processes: int) -> dict:
    import duckdb
    import psycopg

    out = {}
    con = duckdb.connect(str(path), read_only=True)
    max_key = con.execute("SELECT max(l_orderkey) FROM lineitem").fetchone()[0]
    with psycopg.connect(pg_url(db), autocommit=True) as pg:
        pg.execute("DROP TABLE IF EXISTS lineitem")
        pg.execute(PG_DDL)
        t0 = time.perf_counter()
        cur = con.execute(f"SELECT {', '.join(COLS)} FROM lineitem")
        with pg.cursor() as c, c.copy(f"COPY lineitem ({', '.join(COLS)}) FROM STDIN") as cp:
            while chunk := cur.fetchmany(100_000):
                for r in chunk:
                    cp.write_row(r)
        out["load_postgres_s"] = round(time.perf_counter() - t0, 1)
    con.close()

    cluster, cs = connect_cassandra()
    cs.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = "
               "{'class': 'SimpleStrategy', 'replication_factor': 1}")
    cs.execute(f"DROP TABLE IF EXISTS {keyspace}.lineitem", timeout=300)
    cs.execute(LINEITEM.cql_ddl(keyspace), timeout=120)
    cluster.shutdown()
    t0 = time.perf_counter()
    edges = np.linspace(0, max_key + 1, 4 * processes + 1).astype(np.int64).tolist()
    tasks = [(path, keyspace, lo, hi) for lo, hi in zip(edges[:-1], edges[1:])]
    with ProcessPoolExecutor(processes) as pool:
        loaded = sum(pool.map(_load_slice, tasks))
    out["load_cassandra_s"] = round(time.perf_counter() - t0, 1)
    out["load_cassandra_rows"] = loaded
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--sf", type=float, default=10)
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--load-processes", type=int, default=8)
    p.add_argument("--max-differences", type=int, default=100_000)
    p.add_argument("--per-fault", type=int, default=20)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--duckdb", default=str(Path(__file__).resolve().parents[1] / "results" / "raw" / "tpch.duckdb"))
    p.add_argument("--skip-load", action="store_true", help="reuse data already loaded (faults are not re-injected)")
    args = p.parse_args()
    tag = f"{args.sf:g}".replace(".", "_")
    db = keyspace = f"tpch_sf{tag}"
    path = Path(args.duckdb)
    path.parent.mkdir(parents=True, exist_ok=True)
    out = {"sf": args.sf, "workers": args.workers, "max_differences": args.max_differences}

    if not args.skip_load:
        import psycopg

        t0 = time.perf_counter()
        out["rows"] = generate(args.sf, path)
        out["dbgen_s"] = round(time.perf_counter() - t0, 1)
        print(f"generated {out['rows']:,} rows in {out['dbgen_s']} s", flush=True)
        with psycopg.connect(PG_ADMIN, autocommit=True) as admin:
            admin.execute(f"DROP DATABASE IF EXISTS {db} WITH (FORCE)")
            admin.execute(f"CREATE DATABASE {db}")
        out.update(load(path, db, keyspace, args.load_processes))
        print(f"loaded: {out}", flush=True)
        pg = psycopg.connect(pg_url(db), autocommit=True)
        pg.execute("ANALYZE lineitem")
        cluster, cs = connect_cassandra()
        truth_by_class = inject_into(pg, cs, keyspace, args.per_fault, args.seed)
        cluster.shutdown()
        pg.close()
        truth = set().union(*truth_by_class.values())
        out["injected_keys"] = len(truth)
        print(f"injected {len(truth)} faults", flush=True)
    else:
        truth = None

    t0 = time.perf_counter()
    src = scan(pg_url(db), [LINEITEM], args.workers)
    out["scan_postgres_s"] = round(time.perf_counter() - t0, 1)
    t0 = time.perf_counter()
    tgt = scan(f"cassandra://127.0.0.1:59042/{keyspace}", [LINEITEM], args.workers)
    out["scan_cassandra_s"] = round(time.perf_counter() - t0, 1)
    out["rows_source"], out["rows_target"] = len(src), len(tgt)
    print(f"scanned: {out}", flush=True)

    cfg = LadderConfig(max_capacity=min(max(64, len(src) // 8), max(64, int(1.25 * args.max_differences))))
    t0 = time.perf_counter()
    a, b = Sketch(cfg), Sketch(cfg)
    step = 5_000_000
    for sk, t in ((a, src), (b, tgt)):
        for i in range(0, len(t), step):
            sk.update(t.khi[i:i + step], t.klo[i:i + step], t.dig[i:i + step])
    out["sketch_build_s"] = round(time.perf_counter() - t0, 1)
    out["sketch_memory_bytes_per_store"] = a.memory_bytes
    t0 = time.perf_counter()
    rep = verify(a, b, table_rescan(src, tgt))
    out["verify_s"] = round(time.perf_counter() - t0, 3)
    out.update({"found": len(rep.keys), "bytes": rep.cost.bytes, "round_trips": rep.cost.round_trips,
                "rows_rescanned": rep.cost.rows_scanned, "level": rep.level})

    t0 = time.perf_counter()
    x, y = exact_diff(src, tgt)
    scan_truth = {(h, l) for h, l, _ in x} | {(h, l) for h, l, _ in y}
    out["exact_diff_s"] = round(time.perf_counter() - t0, 1)
    out["scan_difference_keys"] = len(scan_truth)
    out["found_equals_scan_difference"] = rep.keys == scan_truth
    if truth is not None:
        out["scan_difference_equals_injected"] = scan_truth == truth
        out["false_positives"] = len(rep.keys - truth)
        out["recall"] = len(rep.keys & truth) / len(truth)
    out["coordinator_peak_rss_gb"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 2**30, 2)
    for k, v in out.items():
        print(f"{k:34} {v}")
    path_out = Path(__file__).resolve().parents[1] / "results" / f"tpch_scale_sf{tag}.csv"
    with path_out.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["metric", "value"])
        w.writerows(out.items())
    print(f"wrote {path_out}")


def inject_into(pg, cs, keyspace: str, per: int, seed: int) -> dict:
    """tpch_e2e.inject against this run's keyspace (its CQL names the tpch keyspace)."""

    class Session:
        def __init__(self, s):
            self.s = s

        def execute(self, q, *a, **kw):
            return self.s.execute(q.replace("tpch.lineitem", f"{keyspace}.lineitem"), *a, **kw)

    return inject(pg, Session(cs), per, np.random.default_rng(seed))


if __name__ == "__main__":
    main()
