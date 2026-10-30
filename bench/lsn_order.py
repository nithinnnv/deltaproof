"""Ablation (a): are a change's own LSNs a valid version order for a live-verification watermark?

Runs TPC-C on the local PostgreSQL (docker compose stack of bench/live_e2e.py) and reads the change stream from a
logical replication slot. Logical decoding delivers transactions in commit order; each change also carries the
LSN of its own WAL record, which follows when it was written, not when its transaction committed. A watermark
expressed in LSNs ("every change with LSN <= w is processed") is then wrong whenever a change arrives after
another change with a higher LSN. For each simulated check (every --interval seconds) this measures w = the
highest LSN delivered so far and counts the changes with LSN <= w that had not arrived yet: changes an LSN
watermark would claim as processed. Per key, LSNs still increase (row locks order writers), which is checked too.

    python bench/lsn_order.py --rate 100 --seconds 300
"""

from __future__ import annotations

import argparse
import csv
import json
import time
from pathlib import Path

import live_e2e
from live_e2e import benchbase, benchbase_summary, connect_pg, parse_lsn

ROOT = Path(__file__).resolve().parents[1]
SLOT = "dp_lsn_order"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--rate", default="100")
    p.add_argument("--seconds", type=int, default=300)
    p.add_argument("--terminals", type=int, default=16)
    p.add_argument("--interval", type=float, default=10.0)
    p.add_argument("--restore", action="store_true", help="reload tpcc from the 4-warehouse dump first")
    args = p.parse_args()
    if args.restore:
        live_e2e.restore_postgres()
    pg = connect_pg()
    pg.execute("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = %s", (SLOT,))
    pg.execute("SELECT pg_create_logical_replication_slot(%s, 'wal2json')", (SLOT,))

    changes = []  # (arrival s, lsn, xid, table, key) in delivery order
    t0 = time.monotonic()
    bb = benchbase("lsn_order", args.rate, args.seconds, args.terminals)
    try:
        idle = 0
        while True:
            rows = pg.execute("SELECT lsn, xid, data FROM pg_logical_slot_get_changes(%s, NULL, 20000, "
                              "'format-version', '2', 'include-pk', '1')", (SLOT,)).fetchall()
            now = time.monotonic() - t0
            for lsn, xid, data in rows:
                d = json.loads(data)
                if d.get("action") not in ("I", "U", "D"):
                    continue
                cols = d.get("identity") or d.get("columns") or []
                pk = [c["name"] for c in d.get("pk", [])]
                key = (d["table"], tuple(c["value"] for c in cols if c["name"] in pk))
                changes.append((now, parse_lsn(str(lsn)), int(xid), d["table"], key))
            if bb.poll() is not None and not rows:
                idle += 1
                if idle >= 6:
                    break
            else:
                idle = 0
            time.sleep(0.5)
    finally:
        pg.execute("SELECT pg_drop_replication_slot(%s)", (SLOT,))

    # Out of LSN order: delivered after a change with a higher LSN.
    out_of_order, high = 0, -1
    for _, lsn, *_ in changes:
        if lsn < high:
            out_of_order += 1
        high = max(high, lsn)
    # Per key, LSNs must increase in delivery order.
    last, key_violations = {}, 0
    for _, lsn, _, _, key in changes:
        if key in last and lsn < last[key]:
            key_violations += 1
        last[key] = lsn
    # Simulated checks: an LSN watermark w = highest LSN delivered by time t claims every change with LSN <= w.
    checks, i, high = [], 0, -1
    t = args.interval
    end = changes[-1][0] if changes else 0
    while t <= end:
        while i < len(changes) and changes[i][0] <= t:
            high = max(high, changes[i][1])
            i += 1
        later = [c for c in changes[i:] if c[1] <= high]
        checks.append({"t": round(t, 1), "lsn_watermark": high, "delivered": i,
                       "claimed_not_arrived": len(later),
                       "max_arrival_after_check_s": round(max((c[0] - t for c in later), default=0.0), 2)})
        t += args.interval
    # Every position in the stream: a replicator that has processed exactly the first i changes. An LSN watermark
    # there is the highest LSN among them; count the later changes at or below it (claimed but not processed).
    # Checks at poll instants rarely split an out-of-order pair, since both usually arrive in one batch.
    lsns = [c[1] for c in changes]
    rank = {v: r + 1 for r, v in enumerate(sorted(set(lsns)))}
    tree = [0] * (len(rank) + 1)

    def add(r):
        while r < len(tree):
            tree[r] += 1
            r += r & -r

    def count_le(r):
        s = 0
        while r > 0:
            s += tree[r]
            r -= r & -r
        return s

    prefix_max, high = [], -1
    for v in lsns:
        high = max(high, v)
        prefix_max.append(high)
    import bisect
    sorted_lsns = sorted(rank)
    wrong_positions, wrong_max, wrong_sum = 0, 0, 0
    for i in range(len(lsns) - 1, -1, -1):  # tree holds the changes after position i
        r = bisect.bisect_right(sorted_lsns, prefix_max[i])
        n = count_le(r)
        if n:
            wrong_positions += 1
            wrong_max = max(wrong_max, n)
            wrong_sum += n
        add(rank[lsns[i]])
    out = {"rate": args.rate, "seconds": args.seconds, "changes": len(changes),
           "positions_lsn_watermark_wrong": wrong_positions,
           "fraction_positions_lsn_watermark_wrong": round(wrong_positions / max(1, len(lsns)), 4),
           "claimed_not_processed_max": wrong_max,
           "claimed_not_processed_mean_when_wrong": round(wrong_sum / max(1, wrong_positions), 2),
           "transactions": len({c[2] for c in changes}),
           "changes_out_of_lsn_order": out_of_order,
           "fraction_out_of_lsn_order": round(out_of_order / max(1, len(changes)), 4),
           "per_key_lsn_decreases": key_violations,
           "checks": len(checks),
           "checks_with_claimed_not_arrived": sum(1 for c in checks if c["claimed_not_arrived"]),
           "claimed_not_arrived_max": max((c["claimed_not_arrived"] for c in checks), default=0),
           "claimed_not_arrived_total": sum(c["claimed_not_arrived"] for c in checks),
           "max_arrival_after_check_s": max((c["max_arrival_after_check_s"] for c in checks), default=0.0),
           **benchbase_summary("lsn_order")}
    d = ROOT / "results" / "ablation"
    d.mkdir(parents=True, exist_ok=True)
    with (d / "lsn_order.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["metric", "value"])
        w.writerows(out.items())
    with (d / "lsn_order_checks.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(checks[0]) if checks else ["t"])
        w.writeheader()
        w.writerows(checks)
    for k, v in out.items():
        print(f"{k:34} {v}")


if __name__ == "__main__":
    main()
