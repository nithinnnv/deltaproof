"""End-to-end experiment on real stores: PostgreSQL 16 (source) and Cassandra 4.1 (target).

    docker compose -p deltaproof up -d
    python bench/e2e.py [--rows 200000] [--per-fault 20]

Loads one synthetic ledger into both stores, injects faults with real SQL/CQL statements,
scans both stores through their drivers, and verifies with DeltaProof. It also checks a naive
digest (hash of the driver's raw values, no canonical encoding) to show why encoding matters.
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

from deltaproof import canon  # noqa: E402
from deltaproof.adapters import cassandra_rows, postgres_rows, to_table  # noqa: E402
from deltaproof.sketch import LadderConfig, Sketch  # noqa: E402
from deltaproof.verify import table_rescan, verify  # noqa: E402
from deltaproof.workload import generate  # noqa: E402

COLUMNS = ["entry_id", "account_id", "txn_id", "amount", "currency", "posted_at", "memo"]
# Cassandra's timestamp keeps milliseconds, so the canonical form is declared at ms.
SCHEMA = canon.Schema("entry_id", (
    canon.Column("account_id", canon.Int()),
    canon.Column("txn_id", canon.Int()),
    canon.Column("amount", canon.Dec(2)),
    canon.Column("currency", canon.Text()),
    canon.Column("posted_at", canon.Timestamp(unit="ms")),
    canon.Column("memo", canon.Text()),
))


def connect_postgres():
    import psycopg

    return psycopg.connect("host=localhost port=55432 user=postgres password=deltaproof dbname=ledger", autocommit=True)


def connect_cassandra(timeout_s: int = 240):
    from cassandra.cluster import Cluster

    t0 = time.time()
    while True:
        try:
            cluster = Cluster(["127.0.0.1"], port=59042)
            return cluster, cluster.connect()
        except Exception:
            if time.time() - t0 > timeout_s:
                raise
            time.sleep(5)


def load(pg, cs, ledger) -> dict:
    from cassandra.concurrent import execute_concurrent_with_args

    rows = [ledger.row(i) for i in range(len(ledger))]
    t0 = time.perf_counter()
    pg.execute("DROP TABLE IF EXISTS ledger")
    pg.execute("""CREATE TABLE ledger (entry_id bigint PRIMARY KEY, account_id bigint, txn_id bigint,
                  amount numeric(18,2), currency text, posted_at timestamptz, memo text)""")
    with pg.cursor() as cur, cur.copy(f"COPY ledger ({', '.join(COLUMNS)}) FROM STDIN") as cp:
        for r in rows:
            cp.write_row([r[c] for c in COLUMNS])
    pg_s = time.perf_counter() - t0

    t0 = time.perf_counter()
    cs.execute("CREATE KEYSPACE IF NOT EXISTS ledger WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    cs.execute("DROP TABLE IF EXISTS ledger.journal")
    cs.execute("""CREATE TABLE ledger.journal (entry_id bigint PRIMARY KEY, account_id bigint, txn_id bigint,
                  amount decimal, currency text, posted_at timestamp, memo text)""")
    ins = cs.prepare(f"INSERT INTO ledger.journal ({', '.join(COLUMNS)}) VALUES ({', '.join('?' * len(COLUMNS))})")
    execute_concurrent_with_args(cs, ins, [[r[c] for c in COLUMNS] for r in rows], concurrency=256, raise_on_first_error=True)
    return {"load_postgres_s": round(pg_s, 2), "load_cassandra_s": round(time.perf_counter() - t0, 2)}


def inject(pg, cs, ledger, per: int, rng) -> dict[str, set]:
    n = len(ledger)
    ids = (rng.permutation(n)[: 5 * per] + 1).tolist()
    groups = {c: ids[i * per:(i + 1) * per] for i, c in enumerate(["lost_write", "precision", "timezone", "corruption", "lost_delete"])}
    for k in groups["lost_write"]:
        cs.execute("DELETE FROM ledger.journal WHERE entry_id = %s", (k,))
    for k in groups["precision"]:
        r = ledger.row(k - 1)
        cs.execute("UPDATE ledger.journal SET amount = %s WHERE entry_id = %s", (r["amount"] + Decimal("0.01"), k))
    for k in groups["timezone"]:
        r = ledger.row(k - 1)
        cs.execute("UPDATE ledger.journal SET posted_at = %s WHERE entry_id = %s", (r["posted_at"] + timedelta(hours=5), k))
    for k in groups["corruption"]:
        cs.execute("UPDATE ledger.journal SET memo = %s WHERE entry_id = %s", ("corrupted memo", k))
    for k in groups["lost_delete"]:
        pg.execute("DELETE FROM ledger WHERE entry_id = %s", (k,))
    phantom = generate(per, rng, start_id=n + 1)
    for i in range(per):
        r = phantom.row(i)
        cs.execute(f"INSERT INTO ledger.journal ({', '.join(COLUMNS)}) VALUES ({', '.join(['%s'] * len(COLUMNS))})", [r[c] for c in COLUMNS])
    groups["phantom"] = [int(x) for x in phantom.entry_id]
    return {c: {(0, k) for k in ks} for c, ks in groups.items()}


def naive_digest(row: dict) -> str:
    return hashlib.blake2b("|".join(str(row[c]) for c in COLUMNS[1:]).encode(), digest_size=8).hexdigest()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=200_000)
    p.add_argument("--per-fault", type=int, default=20)
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()
    rng = np.random.default_rng(args.seed)
    ledger = generate(args.rows, rng)

    pg = connect_postgres()
    cluster, cs = connect_cassandra()
    out = {"rows": args.rows, **load(pg, cs, ledger)}
    truth_by_class = inject(pg, cs, ledger, args.per_fault, rng)
    truth = set().union(*truth_by_class.values())

    t0 = time.perf_counter()
    pg_rows = list(postgres_rows(pg, "ledger", COLUMNS))
    out["scan_postgres_s"] = round(time.perf_counter() - t0, 2)
    t0 = time.perf_counter()
    cs_rows = list(cassandra_rows(cs, "ledger.journal", COLUMNS))
    out["scan_cassandra_s"] = round(time.perf_counter() - t0, 2)

    t0 = time.perf_counter()
    src, tgt = to_table(SCHEMA, pg_rows, "amount"), to_table(SCHEMA, cs_rows, "amount")
    out["canonicalize_s"] = round(time.perf_counter() - t0, 2)
    cfg = LadderConfig(max_capacity=max(64, args.rows // 8))
    t0 = time.perf_counter()
    a, b = Sketch(cfg), Sketch(cfg)
    a.update(src.khi, src.klo, src.dig)
    b.update(tgt.khi, tgt.klo, tgt.dig)
    out["sketch_build_s"] = round(time.perf_counter() - t0, 2)
    t0 = time.perf_counter()
    rep = verify(a, b, table_rescan(src, tgt))
    out["verify_s"] = round(time.perf_counter() - t0, 4)
    out.update({
        "injected_keys": len(truth),
        "deltaproof_found": len(rep.keys),
        "deltaproof_recall": len(rep.keys & truth) / len(truth),
        "deltaproof_false_positives": len(rep.keys - truth),
        "deltaproof_bytes": rep.cost.bytes,
        "deltaproof_round_trips": rep.cost.round_trips,
    })
    for cls, keys in truth_by_class.items():
        out[f"recall_{cls}"] = len(rep.keys & keys) / len(keys)

    ns = {r["entry_id"]: naive_digest(r) for r in pg_rows}
    nt = {r["entry_id"]: naive_digest(r) for r in cs_rows}
    naive = {(0, k) for k in ns.keys() ^ nt.keys()} | {(0, k) for k in ns.keys() & nt.keys() if ns[k] != nt[k]}
    out["naive_found"] = len(naive)
    out["naive_false_positives"] = len(naive - truth)

    for k, v in out.items():
        print(f"{k:32} {v}")
    path = Path(__file__).resolve().parents[1] / "results" / "e2e.csv"
    path.parent.mkdir(exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["metric", "value"])
        w.writerows(out.items())
    print(f"wrote {path}")
    cluster.shutdown()


if __name__ == "__main__":
    main()
