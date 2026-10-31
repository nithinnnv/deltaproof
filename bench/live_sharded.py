"""Live verification sharded across Kafka partitions: P shards, each a replicator process and a verifier process.

Debezium writes every table into one topic with P partitions, keyed by row key, so each key's changes all go to
one partition in commit order. A partition's offsets then follow the source's commit order restricted to its keys,
which is all the protocol needs: each shard is an independent instance (its own versions, watermark, sketches and
confirmation), and a key is verified by the one shard that owns it. The coordinator (this process) starts the
shards, triggers checks on all of them together, merges their records, and computes the ground truth once.

    python bench/live_cluster.py live --shards 8 ...      (or bench/live_mysql_pg.py live --shards 8 ...)
"""

from __future__ import annotations

import csv
import multiprocessing as mp
import sys
import threading
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import live_split  # noqa: E402
from live_split import VerifierSide, pipeline, replicator_main  # noqa: E402

from deltaproof.sizing import RatelessSketch  # noqa: E402
from deltaproof.sketch import LadderConfig, Sketch  # noqa: E402
from deltaproof.store import Table, exact_diff  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]


def _members(t: Table, keys: set) -> Table:
    """The rows of t whose key this shard owns."""
    if not len(t):
        return t
    mask = np.fromiter(((h, l) in keys for h, l in zip(t.khi.tolist(), t.klo.tolist())), bool, len(t))
    return t.take(mask)


class ShardVerifier(VerifierSide):
    """A shard's verifier: a full rescan (the ladder's fallback) keeps only the keys this shard owns, which are
    exactly the keys it has read from its partition (every row reaches the target through the stream)."""

    def owned(self) -> set:
        with self.slock:
            return set(self.src_base) | set(self.src_later)

    def full_rescan(self, pg, target):
        inner = super().full_rescan(pg, target)

        def rescan(partitions: int, p: int):
            s, t = inner(partitions, p)
            keys = self.owned()
            return _members(s, keys), _members(t, keys)

        return rescan


def verifier_main(coord, rep, pipe_name: str, args, topic: str, t_start: float, shard: int) -> None:
    P = pipeline(pipe_name)
    target, close = P.target(False)
    live = type("ShardVerifier", (ShardVerifier, P.live_cls), {})(args, topic)
    live.conn, live.role, live.do_queue, live.t_start, live.partition = rep, "verifier", False, t_start, shard
    poller = threading.Thread(target=live.poller, daemon=True)
    poller.start()
    while True:
        cmd, arg = coord.recv()
        if cmd == "progress":
            pr = live._call("progress")
            coord.send({**pr, "verifier_max_polled": live.max_polled})
        elif cmd == "check":
            rec = live.verify_now(None, target, arg)["rec"]
            coord.send({**rec, "shard": shard})
        elif cmd == "clear_lag":
            coord.send(live._call("clear_lag"))
        elif cmd == "stop":
            last = live._call("stop")
            while live.max_polled < last:
                time.sleep(0.05)
            live.stop_polling.set()
            poller.join()
            coord.send(last)
        elif cmd == "final":
            final = live.verify_now(None, target, "final")
            fin = live._call("final")
            coord.send({"final_rec": {**final["rec"], "shard": shard}, "final_reported": set(final["reported"]),
                        "final_candidates": set(final["candidates"]), "owned": live.owned(),
                        "verifications": [{**v, "shard": shard} for v in live.verifications],
                        "reports": [{**r, "shard": shard} for r in live.reports],
                        "misses": [{**m, "shard": shard} for m in live.misses],
                        "followups": [{**f, "shard": shard} for f in fin["followups"]],
                        "ever_faulty": fin["ever_faulty"], "stats": fin["stats"],
                        "source_sketch_s": live.stats.source_sketch_s,
                        "src": live.src, "src_r": live.src_r, "tgt": fin["tgt"], "tgt_r": fin["tgt_r"]})
        elif cmd == "exit":
            live._call("exit")
            close()
            coord.send(True)
            return


def _broadcast(conns: list, cmd: str, arg=None) -> list:
    for c in conns:
        c.send((cmd, arg))
    return [c.recv() for c in conns]


def _round(recs: list) -> dict:
    """One check across all shards: counts add up; time and pauses are the slowest shard's."""
    s = lambda k: sum(r[k] or 0 for r in recs)
    m = lambda k: max(r[k] or 0 for r in recs)
    return {"label": recs[0]["label"], "t": m("t"), "shards": len(recs), "candidates": s("candidates"),
            "reported": s("reported"), "reported_false": s("reported_false"),
            "reported_while_rewritten": s("reported_while_rewritten"), "missed": s("missed"),
            "faulty_at_or_below_w": s("faulty_at_or_below_w"), "target_point_reads": s("target_point_reads"),
            "riblt_bytes": s("riblt_bytes"), "ladder_bytes": s("bytes") if any(r["bytes"] for r in recs) else None,
            "total_ms_max": m("total_ms"), "replicator_paused_ms_max": m("replicator_paused_ms"),
            "riblt_candidates_equal_ladder": (None if all(r["riblt_candidates_equal_ladder"] is None for r in recs)
                                              else all(r["riblt_candidates_equal_ladder"] is not False for r in recs))}


def run(args, pipe_name: str) -> None:
    import live_cluster

    P = pipeline(pipe_name)
    n = args.shards
    P.restore()
    tag = f"live_r{args.rate}_l{args.lag:g}_s{n}" + (f"_o{args.reorder:g}" if args.reorder else "") + args.suffix
    name = f"dpk_{int(time.time())}"
    topic = f"{name}_changes"
    target, close = P.target(True)
    source_rows = P.source_rows()
    out = {**P.out_extra, "mode": "live", "shards": n, "rate": args.rate, "seconds": args.seconds,
           "terminals": args.terminals, "warehouses": args.warehouses, "lag_s": args.lag, "reorder": args.reorder,
           "drop": args.drop, "corrupt": args.corrupt, "interval_s": args.interval,
           "rateless_symbols": args.symbols, "no_tombstones": getattr(args, "no_tombstones", False)}
    P.create_topic(topic, n)
    P.register(name, topic)
    t_start = time.monotonic()
    ctx = mp.get_context("spawn")
    coords, procs = [], []
    for shard in range(n):
        v_rep, r_conn = ctx.Pipe()
        coord, v_coord = ctx.Pipe()
        procs.append(ctx.Process(target=replicator_main, args=(r_conn, pipe_name, args, topic, t_start, shard),
                                 daemon=True))
        procs.append(ctx.Process(target=verifier_main,
                                 args=(v_coord, v_rep, pipe_name, args, topic, t_start, shard), daemon=True))
        coords.append(coord)
    for p in procs:
        p.start()

    print(f"Debezium snapshot (backfill) of {source_rows:,} rows into {n} partitions", flush=True)
    t0 = time.monotonic()
    while True:  # only one partition sees Debezium's "last" snapshot marker: count snapshot rows instead
        prs = _broadcast(coords, "progress")
        done = sum(p["snapshot_events"] for p in prs)
        if done >= source_rows and all(p["backlog"] == 0 for p in prs):
            break
        time.sleep(1)
    out["backfill_events"] = done
    out["backfill_s"] = round(time.monotonic() - t0, 1)
    rounds = [_round(_broadcast(coords, "check", "after_backfill"))]
    print(f"  backfill done: {done:,} rows in {out['backfill_s']} s; reported={rounds[0]['reported']} "
          f"false={rounds[0]['reported_false']}", flush=True)

    _broadcast(coords, "clear_lag")
    bb = P.benchbase(tag, args)
    print(f"TPC-C running ({args.seconds}s)", flush=True)
    next_check = time.monotonic() + args.interval
    while bb.poll() is None:
        if time.monotonic() >= next_check:
            r = _round(_broadcast(coords, "check", "live"))
            rounds.append(r)
            backlog = sum(p["backlog"] for p in _broadcast(coords, "progress"))
            print(f"  t={r['t']}s cand={r['candidates']} reported={r['reported']} false={r['reported_false']} "
                  f"missed={r['missed']} riblt={r['riblt_bytes']}B slowest={r['total_ms_max']}ms backlog={backlog}",
                  flush=True)
            next_check = time.monotonic() + args.interval
        time.sleep(0.2)
    print("TPC-C finished; draining the change stream", flush=True)
    out.update(live_cluster.benchbase_summary(tag))
    time.sleep(10)
    _broadcast(coords, "stop")
    fins = _broadcast(coords, "final")
    rounds.append(_round([f["final_rec"] for f in fins]))
    _broadcast(coords, "exit")
    for p in procs:
        p.join(timeout=60)

    print("final full scans for ground truth", flush=True)
    s_scan, t_scan = P.scans()
    a, b = exact_diff(s_scan, t_scan)
    truth = {(h, l) for h, l, _ in a} | {(h, l) for h, l, _ in b}
    reported = set().union(*(f["final_reported"] for f in fins))
    candidates = set().union(*(f["final_candidates"] for f in fins))
    owned = [f["owned"] for f in fins]
    same_r = lambda x, y: all(np.array_equal(p, q) for p, q in zip(x.arrays, y.arrays))
    sketches_ok, riblt_ok = True, True
    for f, keys in zip(fins, owned):  # each shard's sketches against sketches rebuilt from its keys' rows
        s_own, t_own = _members(s_scan, keys), _members(t_scan, keys)
        cfg = LadderConfig(max_capacity=262_144)  # as Live.__init__
        rs, rt = Sketch(cfg), Sketch(cfg)
        rs.update(s_own.khi, s_own.klo, s_own.dig)
        rt.update(t_own.khi, t_own.klo, t_own.dig)
        sketches_ok &= live_cluster.sketches_equal(f["src"], rs) and live_cluster.sketches_equal(f["tgt"], rt)
        qs, qt = RatelessSketch(args.symbols), RatelessSketch(args.symbols)
        qs.update(s_own.khi, s_own.klo, s_own.dig)
        qt.update(t_own.khi, t_own.klo, t_own.dig)
        riblt_ok &= same_r(f["src_r"], qs) and same_r(f["tgt_r"], qt)
    union = set().union(*owned)
    rows_keys = {(h, l) for h, l in zip(s_scan.khi.tolist(), s_scan.klo.tolist())} | \
                {(h, l) for h, l in zip(t_scan.khi.tolist(), t_scan.klo.tolist())}
    live_rounds = [r for r in rounds if r["label"] == "live"]
    lag = np.concatenate([np.array(f["stats"].lag_samples) for f in fins if f["stats"].lag_samples] or [np.zeros(1)])
    med = lambda xs: float(np.median(xs)) if len(xs) else None
    out.update({
        "changes_captured": sum(f["stats"].changes for f in fins),
        "dropped": sum(f["stats"].dropped for f in fins), "corrupted": sum(f["stats"].corrupted for f in fins),
        "deferred": sum(f["stats"].deferred for f in fins),
        "apply_delay_p50_s": round(float(np.percentile(lag, 50)), 2),
        "apply_delay_p99_s": round(float(np.percentile(lag, 99)), 2),
        "live_rounds": len(live_rounds),
        "live_reported_false_total": sum(r["reported_false"] for r in live_rounds),
        "live_reported_while_rewritten_total": sum(r["reported_while_rewritten"] for r in live_rounds),
        "live_missed_total": sum(r["missed"] for r in live_rounds),
        "live_round_ms_median": med([r["total_ms_max"] for r in live_rounds]),
        "live_replicator_paused_ms_median": med([r["replicator_paused_ms_max"] for r in live_rounds]),
        "live_riblt_bytes_median": med([r["riblt_bytes"] for r in live_rounds]),
        "live_riblt_agrees_with_ladder": all(r["riblt_candidates_equal_ladder"] is not False for r in rounds),
        "final_rows_source": len(s_scan), "final_rows_target": len(t_scan), "final_truth_keys": len(truth),
        "final_reported_equals_truth": reported == truth, "final_candidates_equal_truth": candidates == truth,
        "shards_own_every_key_once": len(union) == sum(len(k) for k in owned) and rows_keys <= union,
        "ladder_sketches_match_rescan": sketches_ok, "riblt_sketches_match_rescan": riblt_ok,
    })
    close()
    P.drop(name)
    d = ROOT / "results" / P.sub
    live_cluster.write(tag, out, rounds, [r for f in fins for r in f["reports"]],
                       [m for f in fins for m in f["misses"]], sub=P.sub,
                       followups=[x for f in fins for x in f["followups"]],
                       ever_faulty=set().union(*(f["ever_faulty"] for f in fins)))
    with (d / f"{tag}_shard_verifications.csv").open("w", newline="") as fh:
        rows = [v for f in fins for v in f["verifications"]]
        keys = list(dict.fromkeys(k for r in rows for k in r))
        w = csv.DictWriter(fh, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)
