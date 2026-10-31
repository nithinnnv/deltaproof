"""Existing approaches, run against a live MySQL -> PostgreSQL migration (bench/live_mysql_pg.py), for comparison.

    round datadiff   one pass of data-diff (Datafold's open-source range-checksum differ, HashDiffer) over every
                     verified table; run with an interpreter that has data-diff installed (it needs pydantic<2)
    round full       one row-by-row comparison: scan both stores in full and take the exact difference
    analyze          classify every key each round reported, using the run's ever_faulty file (every key the
                     replicator ever mishandled): a key never mishandled can only have differed because a change
                     was in flight, so reporting it is a false difference

Each round appends one JSON line: start time, duration, rows read or downloaded, and the keys reported per table.
data-diff cannot verify Cassandra (it bisects ranges of keys in key order; Cassandra stores rows by token).
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from deltaproof.relational import TPCC_MYSQL  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]


def _urls():
    import os
    get = lambda n, d: os.environ.get(n, d)
    mysql = f"mysql://root:deltaproof@{get('MYSQL_IP', '127.0.0.1')}:{get('MYSQL_PORT', '3306')}/tpcc"
    pg = f"postgresql://postgres:deltaproof@{get('PGT_IP', '127.0.0.1')}:{get('PGT_PORT', '5432')}/tpcc_target"
    return mysql, pg


def round_datadiff() -> dict:
    from data_diff import HashDiffer, connect_to_table

    mysql, pg = _urls()
    out = {"tool": "data-diff", "t_wall": time.time(), "tables": {}}
    t0 = time.perf_counter()
    for spec in TPCC_MYSQL:
        cols = tuple(c.name for c in spec.columns)
        a = connect_to_table(mysql, spec.name, spec.key, extra_columns=cols)
        b = connect_to_table(pg, spec.name, spec.key, extra_columns=cols)
        t1 = time.perf_counter()
        res = HashDiffer().diff_tables(a, b)
        keys = sorted({tuple(int(x) for x in row[:len(spec.key)]) for _, row in res})
        stats = res.get_stats_dict()
        out["tables"][spec.name] = {"keys": keys, "duration_s": round(time.perf_counter() - t1, 3),
                                    "rows_downloaded": stats["stats"].get("rows_downloaded", 0),
                                    "rows_source": stats["rows_A"], "rows_target": stats["rows_B"]}
    out["duration_s"] = round(time.perf_counter() - t0, 3)
    out["rows_moved"] = sum(t["rows_downloaded"] for t in out["tables"].values())
    return out


def round_full() -> dict:
    import live_mysql_pg as m
    from deltaproof import canon
    from deltaproof.parallel import scan
    from deltaproof.store import exact_diff

    out = {"tool": "full-compare", "t_wall": time.time(), "tables": {}}
    t0 = time.perf_counter()
    s, t = m.scan_mysql(), scan(m.PGT_URL, list(TPCC_MYSQL), 8)
    a, b = exact_diff(s, t)
    by = {spec.tag: spec for spec in TPCC_MYSQL}
    for h, l in {(h, l) for h, l, _ in a} | {(h, l) for h, l, _ in b}:
        tag, parts = canon.unpack_key(h, l, 4)
        spec = by[tag]
        out["tables"].setdefault(spec.name, {"keys": []})["keys"].append(tuple(parts[:len(spec.key)]))
    out["duration_s"] = round(time.perf_counter() - t0, 3)
    out["rows_moved"] = len(s) + len(t)  # every row of both stores is read and transferred
    return out


def analyze(rounds_file: Path, ever_faulty_file: Path, verifications_file: Path) -> list[dict]:
    from deltaproof import canon

    ever = {(int(r["khi"]), int(r["klo"])) for r in csv.DictReader(ever_faulty_file.open())}
    by = {spec.name: spec for spec in TPCC_MYSQL}
    rows = []
    for line in rounds_file.open():
        r = json.loads(line)
        keys = [canon.pack_key(by[name].tag, tuple(k)) for name, t in r["tables"].items() for k in t["keys"]]
        false = sum(1 for k in keys if k not in ever)
        rows.append({"tool": r["tool"], "t_wall": round(r["t_wall"], 1), "duration_s": r["duration_s"],
                     "rows_moved": r["rows_moved"], "reported": len(keys), "false_in_flight": false,
                     "false_fraction": round(false / max(1, len(keys)), 4)})
    live = [v for v in csv.DictReader(verifications_file.open()) if v["label"] == "live"]
    if live:
        med = lambda k: sorted(float(v[k]) for v in live if v[k] not in ("", "None"))[len(live) // 2]
        rows.append({"tool": "kestrel (median of live checks)", "t_wall": "", "duration_s": round(med("total_ms") / 1000, 3),
                     "rows_moved": 0, "reported": int(med("reported")),
                     "false_in_flight": sum(int(v["reported_false"]) for v in live), "false_fraction": 0.0,
                     "bytes_per_check": int(med("riblt_bytes")), "target_point_reads": int(med("target_point_reads"))})
    return rows


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("cmd", choices=["round", "analyze"])
    p.add_argument("what", nargs="?", choices=["datadiff", "full"])
    p.add_argument("--out", default=str(ROOT / "results" / "baselines" / "rounds.jsonl"))
    p.add_argument("--ever-faulty", default="")
    p.add_argument("--verifications", default="")
    a = p.parse_args()
    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    if a.cmd == "round":
        r = round_datadiff() if a.what == "datadiff" else round_full()
        with out.open("a") as f:
            f.write(json.dumps(r) + "\n")
        n = sum(len(t["keys"]) for t in r["tables"].values())
        print(f"{r['tool']}: {n} keys reported, {r['rows_moved']:,} rows moved, {r['duration_s']} s", flush=True)
    else:
        rows = analyze(out, Path(a.ever_faulty), Path(a.verifications))
        keys = list(dict.fromkeys(k for r in rows for k in r))
        with (out.parent / "summary.csv").open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            w.writerows(rows)
        for r in rows:
            print(r)


if __name__ == "__main__":
    main()
