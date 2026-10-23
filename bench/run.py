"""Run the paper's experiments (Section VI) and write CSVs to results/.

    python bench/run.py rq1 rq2 rq3 rq4 rq5 [--scale small|full] [--seed N]

Every number in the paper comes from these CSVs. Times are wall-clock on the machine that
ran them; record its CPU, memory, Python and numpy versions alongside (see results/env.txt).
"""

from __future__ import annotations

import argparse
import csv
import platform
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from deltaproof import baselines  # noqa: E402
from deltaproof.faults import FAULT_CLASSES, inject  # noqa: E402
from deltaproof.live import simulate  # noqa: E402
from deltaproof.sketch import LadderConfig, Sketch, SketchTable, Strata  # noqa: E402
from deltaproof.verify import table_rescan, verify  # noqa: E402
from deltaproof.workload import generate  # noqa: E402

RESULTS = Path(__file__).resolve().parents[1] / "results"


def write(name: str, rows: list[dict]) -> None:
    RESULTS.mkdir(exist_ok=True)
    path = RESULTS / f"{name}.csv"
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {path} ({len(rows)} rows)")


def pinsketch_enabled() -> bool:
    try:
        from deltaproof.pinsketch import lib
        lib()
        return True
    except OSError:
        print("minisketch library not found; skipping the PinSketch baseline (see native/build_minisketch.sh)")
        return False


def build_sketches(inj, n):
    cfg = LadderConfig(max_capacity=max(64, n // 8))
    t0 = time.perf_counter()
    a = Sketch(cfg)
    a.update(inj.source.khi, inj.source.klo, inj.source.dig)
    b = Sketch(cfg)
    b.update(inj.target.khi, inj.target.klo, inj.target.dig)
    return a, b, time.perf_counter() - t0


def run_deltaproof(inj, n):
    a, b, build_s = build_sketches(inj, n)
    t0 = time.perf_counter()
    rep = verify(a, b, table_rescan(inj.source, inj.target))
    return rep, time.perf_counter() - t0, build_s, a.memory_bytes


def rq1(scale: str, seed: int) -> None:
    """Detection per fault class, each class injected alone so methods cannot borrow signal."""
    n, per = (200_000, 50) if scale == "small" else (1_000_000, 100)
    rows = []
    for cls in FAULT_CLASSES:
        rng = np.random.default_rng(seed)
        inj = inject(generate(n, rng), {cls: per}, rng)
        truth = inj.truth[cls]
        rep, *_ = run_deltaproof(inj, n)
        results = {"deltaproof": rep.keys}
        for m in (baselines.full_scan, baselines.bisection, baselines.merkle):
            results[m.__name__] = m(inj.source, inj.target).keys
        agg = baselines.aggregates(inj.source, inj.target)
        for name, found in results.items():
            rows.append({"fault": cls, "method": name, "injected_keys": len(truth),
                         "recall": len(found & truth) / len(truth), "false_positives": len(found - truth)})
        from deltaproof.hashing import key_hash_int
        hit = sum(1 for k in truth if key_hash_int(*k) % agg.partitions in agg.flagged_partitions)
        rows.append({"fault": cls, "method": "aggregates", "injected_keys": len(truth),
                     "recall": hit / len(truth), "false_positives": ""})
        print(f"rq1 {cls}: done")
    write("rq1_detection", rows)


def rq2(scale: str, seed: int) -> None:
    """Cost as table size n and difference count d grow."""
    ns = [100_000, 1_000_000] if scale == "small" else [100_000, 1_000_000, 10_000_000]
    ds = [0, 10, 1_000, 10_000, 100_000]
    rows = []
    for n in ns:
        rng = np.random.default_rng(seed)
        led = generate(n, rng)
        for d in ds:
            if d > n // 10:
                continue
            per = {c: d // len(FAULT_CLASSES) for c in FAULT_CLASSES}
            per["precision"] += d - sum(per.values())
            inj = inject(led, per, np.random.default_rng(seed + d))
            truth = inj.all_keys
            rep, verify_s, build_s, mem = run_deltaproof(inj, n)
            rows.append({"n": n, "d": len(truth), "method": "deltaproof", "bytes": rep.cost.bytes,
                         "round_trips": rep.cost.round_trips, "rows_scanned": rep.cost.rows_scanned,
                         "seconds": round(verify_s, 4), "correct": rep.keys == truth,
                         "level": rep.level, "estimate": rep.estimate, "failed_partitions": rep.failed_partitions,
                         "sketch_build_seconds": round(build_s, 3), "sketch_memory_bytes": mem})
            if pinsketch_enabled():
                from deltaproof.pinsketch import pinsketch
                t0 = time.perf_counter()
                keys, cost, tm = pinsketch(inj.source, inj.target, rep.estimate, Strata().nbytes)
                rows.append({"n": n, "d": len(truth), "method": "pinsketch", "bytes": cost.bytes,
                             "round_trips": cost.round_trips, "rows_scanned": cost.rows_scanned,
                             "seconds": round(tm["decode_s"], 4), "correct": keys == truth,
                             "level": "", "estimate": rep.estimate, "failed_partitions": tm["failed_partitions"],
                             "sketch_build_seconds": round(tm["encode_source_s"] + tm["encode_target_s"], 3),
                             "sketch_memory_bytes": ""})
            for m in (baselines.full_scan, baselines.bisection, baselines.merkle):
                t0 = time.perf_counter()
                r = m(inj.source, inj.target)
                rows.append({"n": n, "d": len(truth), "method": r.name, "bytes": r.cost.bytes,
                             "round_trips": r.cost.round_trips, "rows_scanned": r.cost.rows_scanned,
                             "seconds": round(time.perf_counter() - t0, 4), "correct": r.keys == truth,
                             "level": "", "estimate": "", "failed_partitions": "",
                             "sketch_build_seconds": "", "sketch_memory_bytes": ""})
            print(f"rq2 n={n} d={len(truth)}: done")
    write("rq2_scaling", rows)


def rq3(scale: str, seed: int) -> None:
    """False candidates from in-flight writes, and whether watermark + confirmation removes them."""
    n = 200_000 if scale == "small" else 1_000_000
    rows = []
    for lag in (100, 1_000, 10_000):
        for early in (0.0, 0.1, 0.5):
            for r in simulate(n, writes=50_000, lag=lag, early_fraction=early, faults=50,
                              rng=np.random.default_rng(seed)):
                rows.append({"lag_writes": lag, "early_fraction": early, **r.__dict__})
            print(f"rq3 lag={lag} early={early}: done")
    write("rq3_live_writes", rows)


def rq4(scale: str, seed: int) -> None:
    """Sketch maintenance cost per write and memory per row (in-process; no database)."""
    rng = np.random.default_rng(seed)
    rows = []
    for n in ([100_000, 1_000_000] if scale == "small" else [100_000, 1_000_000, 10_000_000]):
        t = generate(n, rng).table()
        cfg = LadderConfig(max_capacity=max(64, n // 8))
        s = Sketch(cfg)
        t0 = time.perf_counter()
        s.update(t.khi, t.klo, t.dig)
        bulk = time.perf_counter() - t0
        k = 2_000
        idx = rng.choice(n, k, replace=False)
        new = rng.integers(0, 2**63, k, dtype=np.uint64)
        t0 = time.perf_counter()
        for i, nd in zip(idx.tolist(), new.tolist()):
            s.apply_change(t.khi[i:i + 1], t.klo[i:i + 1], t.dig[i:i + 1], np.array([nd], np.uint64))
        single = (time.perf_counter() - t0) / k
        t0 = time.perf_counter()
        s.apply_change(t.khi[idx], t.klo[idx], t.dig[idx], new)
        batched = (time.perf_counter() - t0) / k
        rows.append({"n": n, "levels": len(cfg.levels()), "memory_bytes": s.memory_bytes,
                     "memory_bytes_per_row": round(s.memory_bytes / n, 2),
                     "bulk_insert_us_per_row": round(bulk / n * 1e6, 3),
                     "update_us_single": round(single * 1e6, 1),
                     "update_us_batched_2000": round(batched * 1e6, 3)})
        print(f"rq4 n={n}: done")
    write("rq4_overhead", rows)


def rq5(scale: str, seed: int) -> None:
    """Decode failure vs. load, and strata-estimate accuracy."""
    rng = np.random.default_rng(seed)
    trials = 20 if scale == "small" else 100
    rows = []
    P, m = 64, 513  # one ladder level: capacity 256 per partition at cell_factor 2
    for load in (0.5, 1.0, 1.25, 1.5, 2.0, 2.5, 3.0):
        d = int(load * 256 * P)
        failed = []
        for _ in range(max(3, trials // 10)):
            a = SketchTable(P, m)
            a.update(np.zeros(d, np.uint64), rng.integers(0, 2**63, d, dtype=np.uint64),
                     rng.integers(0, 2**63, d, dtype=np.uint64))
            failed.append(a.subtract(SketchTable(P, m)).decode().failed.size / P)
        rows.append({"kind": "decode", "diffs_per_partition": load * 256, "cells_per_partition": m,
                     "load_vs_capacity": load, "failed_partition_fraction": round(float(np.mean(failed)), 4),
                     "d": "", "estimate_ratio_median": "", "estimate_ratio_p5": "", "estimate_ratio_p95": ""})
    for d in (100, 1_000, 10_000, 100_000):
        ratios = []
        for _ in range(trials):
            x, y = Strata(), Strata()
            x.update(np.zeros(d, np.uint64), rng.integers(0, 2**63, d, dtype=np.uint64),
                     rng.integers(0, 2**63, d, dtype=np.uint64))
            est, _ = x.estimate(y)
            ratios.append(est / d)
        rows.append({"kind": "strata", "diffs_per_partition": "", "cells_per_partition": "",
                     "load_vs_capacity": "", "failed_partition_fraction": "", "d": d,
                     "estimate_ratio_median": round(float(np.median(ratios)), 3),
                     "estimate_ratio_p5": round(float(np.percentile(ratios, 5)), 3),
                     "estimate_ratio_p95": round(float(np.percentile(ratios, 95)), 3)})
        print(f"rq5 strata d={d}: done")
    write("rq5_robustness", rows)


def rq6(scale: str, seed: int) -> None:
    """Sensitivity to a wrong difference estimate: cost of the fallback, and correctness."""
    n = 1_000_000
    rows = []
    led = generate(n, np.random.default_rng(seed))
    for d in (1_000, 10_000):
        per = {c: d // len(FAULT_CLASSES) for c in FAULT_CLASSES}
        per["precision"] += d - sum(per.values())
        inj = inject(led, per, np.random.default_rng(seed + d))
        truth = inj.all_keys
        a, b, _ = build_sketches(inj, n)
        rescan = table_rescan(inj.source, inj.target)
        true_est = verify(a, b, rescan).estimate
        for factor in (1 / 16, 1 / 4, 1 / 2, 1, 2, 4, 16):
            est = max(1, round(true_est * factor))
            t0 = time.perf_counter()
            rep = verify(a, b, rescan, estimate=est)
            rows.append({"n": n, "d": len(truth), "strata_estimate": true_est, "factor": factor, "estimate_used": est,
                         "level": rep.level, "failed_partitions": rep.failed_partitions, "bytes": rep.cost.bytes,
                         "rows_rescanned": rep.cost.rows_scanned, "round_trips": rep.cost.round_trips,
                         "seconds": round(time.perf_counter() - t0, 3), "correct": rep.keys == truth})
            print(f"rq6 d={d} factor={factor}: done", flush=True)
    write("rq6_estimate_sensitivity", rows)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("experiments", nargs="+", choices=["rq1", "rq2", "rq3", "rq4", "rq5", "rq6"])
    p.add_argument("--scale", choices=["small", "full"], default="small")
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()
    RESULTS.mkdir(exist_ok=True)
    (RESULTS / "env.txt").write_text(
        f"python {platform.python_version()}\nnumpy {np.__version__}\n"
        f"machine {platform.machine()} {platform.processor()}\nplatform {platform.platform()}\n"
        f"scale {args.scale}\nseed {args.seed}\n")
    for e in args.experiments:
        globals()[e](args.scale, args.seed)


if __name__ == "__main__":
    main()
