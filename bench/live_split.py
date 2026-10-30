"""Live verification with the verifier and the replicator in separate processes.

In one process (live_cluster.py, live_mysql_pg.py) the change-stream reader, the replicator and the verifier
share one Python interpreter, and so one core: as unrepaired differences accumulate, checks take a growing share
of it and the replicator falls behind. A deployed verifier runs apart from the pipeline it checks, so here:

  replicator process  reads the change stream, applies it to the target (with injected faults), maintains the
                      target-side sketches and the harness's ground truth (each key's last status)
  verifier process    reads the same stream with its own consumer to maintain the source-side sketches and
                      history, and reads the target for confirmation with its own connection

The protocol's two short pauses become messages to the replicator: "phase1" (watermark and copies of the
target-side sketches, under its lock) and "freeze" ... "release" (status updates paused while confirmation reads
run). Everything else is Live.verify_now, unchanged.

    python bench/live_cluster.py live --split ...     (or bench/live_mysql_pg.py live --split ...)
"""

from __future__ import annotations

import copy
import multiprocessing as mp
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from deltaproof.parallel import scan  # noqa: E402
from deltaproof.sizing import RatelessSketch  # noqa: E402
from deltaproof.sketch import Sketch  # noqa: E402
from deltaproof.store import exact_diff  # noqa: E402


def pipeline(name: str) -> SimpleNamespace:
    """What differs between the engine pairs; importable by name in the replicator process."""
    import live_cluster as lc

    if name == "cluster":
        from deltaproof.relational import TPCC

        def target(create: bool):
            cluster, session = lc.connect_cassandra()
            t = lc.ClusterTarget(session)
            t.create() if create else t.prepare()
            return t, cluster.shutdown

        return SimpleNamespace(
            name=name, live_cls=lc.ClusterLive, restore=lc.restore, target=target, sub="cluster",
            register=lc.register_connector, drop=lc.drop_connector,
            scans=lambda: (scan(lc.PG_URL, list(TPCC), 8), scan(lc.CASS_URL, list(TPCC), 8)),
            benchbase=lambda tag, a: lc.benchbase(tag, a.warehouses, a.rate, a.seconds, a.terminals, "execute"),
            out_extra={"pipeline": "postgresql->cassandra", "cassandra_nodes": len(lc.CASS_IPS)})
    if name == "mysql_pg":
        import live_mysql_pg as mp_

        def target(create: bool):
            t = mp_.PGTarget(fresh=create)
            if create:
                t.create()
            return t, t.close

        return SimpleNamespace(
            name=name, live_cls=mp_.MySQLPGLive, restore=mp_.restore, target=target, sub="mysql_pg",
            register=mp_.register_connector, drop=mp_.drop_connector,
            scans=lambda: (mp_.scan_mysql(), scan(mp_.PGT_URL, list(mp_.TPCC_MYSQL), 8)),
            benchbase=lambda tag, a: mp_.benchbase(tag, a.warehouses, a.rate, a.seconds, a.terminals, "execute"),
            out_extra={"pipeline": "mysql->postgresql"})
    raise ValueError(name)


# ---------------------------------------------------------------- replicator process


def replicator_main(conn, pipe_name: str, args, topic: str, t_start: float) -> None:
    P = pipeline(pipe_name)
    target, close = P.target(False)
    live = P.live_cls(args, topic)
    live.role, live.do_source, live.t_start = "replicator", False, t_start
    threads = [threading.Thread(target=live.poller, daemon=True),
               threading.Thread(target=live.applier, args=(target,), daemon=True)]
    for t in threads:
        t.start()
    while True:
        cmd, arg = conn.recv()
        if cmd == "phase1":
            t0 = time.perf_counter()
            with live.tlock:
                w = live.w_t
                tgt = copy.deepcopy(live.tgt) if arg else None
                extra = live.snapshot_extra()
            conn.send((w, tgt, extra, (time.perf_counter() - t0) * 1000))
        elif cmd == "freeze":  # held until "release": the verifier's confirmation reads run meanwhile
            live.tlock.acquire()
            conn.send({"writing": set(live.in_flight),
                       "version": {k: live.last_status[k][1] for k in arg if k in live.last_status}})
        elif cmd == "state":
            faulty = dict(live.faulty)
            conn.send({"faulty": faulty, "status": {k: live.last_status.get(k) for k in arg},
                       "last_t": {k: live.last_t.get(k) for k in faulty}})
        elif cmd == "release":
            live.watch.update(arg)
            live.tlock.release()
            conn.send(True)
        elif cmd == "progress":
            with live.qlock:
                backlog = len(live.queue) + len(live.deferred)
            conn.send({"snapshot_done": live.snapshot_done.is_set(), "backlog": backlog,
                       "snapshot_events": live.snapshot_events, "max_processed": live.max_processed})
        elif cmd == "clear_lag":
            live.stats.lag_samples.clear()
            conn.send(True)
        elif cmd == "stop":  # the stream has ended: drain, then stop
            live.stop_polling.set()
            for t in threads:
                t.join()
            conn.send(live.max_processed)
        elif cmd == "final":
            conn.send({"stats": live.stats, "tgt": live.tgt, "tgt_r": live.tgt_r, "followups": live.followups})
        elif cmd == "exit":
            close()
            conn.send(True)
            return


# ---------------------------------------------------------------- verifier process (the main one)


class VerifierSide:
    """Live with the replicator's state reached through messages; mixed in before the pipeline's Live class."""

    conn = None

    def _call(self, cmd: str, arg=None):
        self.conn.send((cmd, arg))
        return self.conn.recv()

    def _phase1(self, want_ladder: bool):
        return self._call("phase1", want_ladder)

    def _freeze(self, keys: list) -> dict:
        return self._call("freeze", keys)

    def _truth_state(self, reported: list) -> dict:
        return self._call("state", reported)

    def _release(self, watch: list) -> None:
        self._call("release", watch)

    def advance_source(self, w: int) -> None:
        # This process reads the stream itself: wait until it has read every change at or below w.
        while self.max_polled < w and not self.poll_done.is_set():
            time.sleep(0.005)
        super().advance_source(w)


def run(args, pipe_name: str) -> None:
    import live_cluster

    P = pipeline(pipe_name)
    P.restore()
    tag = f"live_r{args.rate}_l{args.lag:g}" + (f"_o{args.reorder:g}" if args.reorder else "") + args.suffix
    name = f"dps_{int(time.time())}"
    topic = f"{name}_changes"
    target, close = P.target(True)
    out = {**P.out_extra, "mode": "live", "split_processes": True, "rate": args.rate, "seconds": args.seconds,
           "terminals": args.terminals, "warehouses": args.warehouses, "lag_s": args.lag, "reorder": args.reorder,
           "drop": args.drop, "corrupt": args.corrupt, "interval_s": args.interval,
           "rateless_symbols": args.symbols, "no_tombstones": getattr(args, "no_tombstones", False)}
    P.register(name, topic)
    t_start = time.monotonic()
    ctx = mp.get_context("spawn")
    here, there = ctx.Pipe()
    proc = ctx.Process(target=replicator_main, args=(there, pipe_name, args, topic, t_start), daemon=True)
    proc.start()
    live = type("SplitVerifier", (VerifierSide, P.live_cls), {})(args, topic)
    live.conn, live.role, live.do_queue, live.t_start = here, "verifier", False, t_start
    poller = threading.Thread(target=live.poller, daemon=True)
    poller.start()

    print("Debezium snapshot (backfill) running", flush=True)
    t0 = time.monotonic()
    while True:
        pr = live._call("progress")
        if pr["snapshot_done"] and pr["backlog"] == 0:
            break
        time.sleep(1)
    out["backfill_events"] = pr["snapshot_events"]
    out["backfill_s"] = round(time.monotonic() - t0, 1)
    r = live.verify_now(None, target, "after_backfill")["rec"]
    print(f"  backfill done: {pr['snapshot_events']:,} rows in {out['backfill_s']} s; "
          f"reported={r['reported']} false={r['reported_false']}", flush=True)

    live._call("clear_lag")  # report apply delay for the live phase, not the backfill
    bb = P.benchbase(tag, args)
    print(f"TPC-C running ({args.seconds}s)", flush=True)
    next_verify = time.monotonic() + args.interval
    while bb.poll() is None:
        if time.monotonic() >= next_verify:
            r = live.verify_now(None, target, "live")["rec"]
            print(f"  t={r['t']}s cand={r['candidates']} reported={r['reported']} false={r['reported_false']} "
                  f"missed={r['missed']} riblt={r['riblt_bytes']}B backlog={live._call('progress')['backlog']}",
                  flush=True)
            next_verify = time.monotonic() + args.interval
        time.sleep(0.2)
    print("TPC-C finished; draining the change stream", flush=True)
    out.update(live_cluster.benchbase_summary(tag))
    time.sleep(10)  # let Debezium publish the tail of the stream
    last = live._call("stop")
    while live.max_polled < last:  # the verifier's own reader catches up to the end of the stream
        time.sleep(0.1)
    live.stop_polling.set()
    poller.join()
    final = live.verify_now(None, target, "final")

    fin = live._call("final")
    v_source_s = live.stats.source_sketch_s
    live.stats, live.tgt, live.tgt_r, live.followups = fin["stats"], fin["tgt"], fin["tgt_r"], fin["followups"]
    live.stats.source_sketch_s = v_source_s
    print("final full scans for ground truth", flush=True)
    s_scan, t_scan = P.scans()
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
    out["ladder_sketches_match_rescan"] = (live_cluster.sketches_equal(live.src, rebuilt_s)
                                           and live_cluster.sketches_equal(live.tgt, rebuilt_t))
    out["riblt_sketches_match_rescan"] = same(live.src_r, rr_s) and same(live.tgt_r, rr_t)
    live._call("exit")
    proc.join(timeout=60)
    P.drop(name)
    close()
    live_cluster.write(tag, out, live.verifications, live.reports, live.misses, sub=P.sub, followups=live.followups)
