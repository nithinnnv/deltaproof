"""Live end-to-end experiment: TPC-C on PostgreSQL, replicated to Cassandra, verified while running.

    docker compose -p deltaproof up -d
    python bench/live_e2e.py baseline --rate 200 --seconds 120              # TPC-C alone
    python bench/live_e2e.py live --rate 200 --seconds 120 --lag 2          # in-order replication
    python bench/live_e2e.py live --rate 200 --seconds 120 --lag 2 --reorder 0.05   # out of order

Pipeline under test (the "migration"):
  BenchBase TPC-C -> PostgreSQL -> wal2json logical slot -> replicator -> Cassandra
The replicator applies each change after `lag` seconds, drops or corrupts a small fraction of
changes, and with --reorder defers a fraction of changes so later changes to other keys land
first (per-key order is kept). Every target row carries `_v`, the source LSN that wrote it.

Verification (every `interval` seconds, with the replicator paused between batches):
  1. Sketch comparison of the source at the target watermark w_T against the target.
  2. Version-aware confirmation of each candidate (paper, Theorem 3): report k iff the target's
     (version, digest) is not a state the source held for k at or after w_T. This reads only the
     target, never the source.
Each report is checked against what the replicator actually did to k's last processed change.
At the end, both sketches are compared with full-scan rebuilds, and the final answer with the
exact difference of full scans of both stores.
"""

from __future__ import annotations

import argparse
import copy
import heapq
import csv
import json
import os
import subprocess
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from deltaproof import canon  # noqa: E402
from deltaproof.adapters import postgres_rows  # noqa: E402
from deltaproof.hashing import key_hash  # noqa: E402
from deltaproof.relational import TPCC, TPCC_BY_NAME, TPCC_BY_TAG, TableSpec  # noqa: E402
from deltaproof.sketch import LadderConfig, Sketch  # noqa: E402
from deltaproof.store import Table, exact_diff  # noqa: E402
from deltaproof.verify import verify  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
EXT = Path(os.environ.get("DELTAPROOF_EXT", Path.home() / "workspace" / "deltaproof-ext"))
PG = "deltaproof-postgres-1"
SLOT = "deltaproof"
KEYSPACE = "tpcc"
Key = tuple[int, int]
FAULTY = ("drop", "corrupt")


def sh(*cmd: str, check: bool = True) -> str:
    return subprocess.run(cmd, check=check, capture_output=True, text=True).stdout


def parse_lsn(s: str) -> int:
    hi, lo = s.split("/")
    return (int(hi, 16) << 32) | int(lo, 16)


# ---------------------------------------------------------------- environment


def restore_postgres() -> None:
    sh("docker", "exec", PG, "psql", "-U", "postgres", "-c", "DROP DATABASE IF EXISTS tpcc WITH (FORCE)", "-c", "CREATE DATABASE tpcc")
    sh("docker", "cp", str(EXT / "tpcc_w4.dump"), f"{PG}:/tmp/tpcc_w4.dump")
    sh("docker", "exec", PG, "pg_restore", "-U", "postgres", "-d", "tpcc", "-j", "4", "/tmp/tpcc_w4.dump")
    sh("docker", "exec", PG, "psql", "-U", "postgres", "-d", "tpcc", "-c", "VACUUM ANALYZE")


def benchbase(tag: str, rate, seconds: int, terminals: int) -> subprocess.Popen:
    cfg = (ROOT / "bench" / "tpcc" / "config.xml.tmpl").read_text()
    cfg = cfg.replace("{warehouses}", "4").replace("{terminals}", str(terminals))
    cfg = cfg.replace("{seconds}", str(seconds)).replace("{rate}", str(rate))
    (EXT / f"tpcc_{tag}.xml").write_text(cfg)
    out = EXT / "results" / tag
    out.mkdir(parents=True, exist_ok=True)
    for old in out.glob("*"):
        old.unlink()
    log = open(out / "benchbase.log", "w")
    return subprocess.Popen(
        ["docker", "run", "--rm", "--network", "deltaproof_default", "-v", f"{EXT}:/ext", "-w", "/ext/benchbase-postgres",
         "eclipse-temurin:23-jre", "java", "-jar", "benchbase.jar", "-b", "tpcc", "-c", f"/ext/tpcc_{tag}.xml",
         "--execute=true", "-d", f"/ext/results/{tag}"], stdout=log, stderr=subprocess.STDOUT)


def benchbase_summary(tag: str) -> dict:
    files = sorted((EXT / "results" / tag).glob("*.summary.json"))
    s = json.loads(files[-1].read_text())
    lat = s.get("Latency Distribution", {})
    pick = lambda name: next((v for k, v in lat.items() if name in k), None)
    return {
        "tpcc_throughput_tps": round(s.get("Throughput (requests/second)", 0), 2),
        "tpcc_goodput_tps": round(s.get("Goodput (requests/second)", 0), 2),
        "tpcc_latency_p50_ms": round(pick("Median") / 1000, 2) if pick("Median") else None,
        "tpcc_latency_p95_ms": round(pick("95th") / 1000, 2) if pick("95th") else None,
        "tpcc_latency_p99_ms": round(pick("99th") / 1000, 2) if pick("99th") else None,
    }


def connect_pg():
    import psycopg

    return psycopg.connect("host=localhost port=55432 user=postgres password=deltaproof dbname=tpcc", autocommit=True)


def connect_cassandra():
    from cassandra.cluster import Cluster

    t0 = time.time()
    while True:
        try:
            cluster = Cluster(["127.0.0.1"], port=59042, connect_timeout=20)
            return cluster, cluster.connect()
        except Exception:
            if time.time() - t0 > 180:
                raise
            time.sleep(5)


# ---------------------------------------------------------------- the target store


class Target:
    """Cassandra with prepared statements for every TPC-C table. Rows carry `_v`, the source LSN."""

    def __init__(self, session):
        self.s = session
        self.ins, self.dele, self.sel = {}, {}, {}

    def create(self) -> None:
        self.s.execute(f"DROP KEYSPACE IF EXISTS {KEYSPACE}", timeout=120)
        self.s.execute(f"CREATE KEYSPACE {KEYSPACE} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
        for spec in TPCC:
            self.s.execute(spec.cql_ddl(KEYSPACE), timeout=60)
            self.s.execute(f"ALTER TABLE {KEYSPACE}.{spec.name} ADD src_v bigint", timeout=60)
        self.prepare()

    def prepare(self) -> None:
        for spec in TPCC:
            cols = spec.column_names + ["src_v"]
            where = " AND ".join(f"{k} = ?" for k in spec.key)
            self.ins[spec.name] = self.s.prepare(
                f"INSERT INTO {KEYSPACE}.{spec.name} ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})")
            self.dele[spec.name] = self.s.prepare(f"DELETE FROM {KEYSPACE}.{spec.name} WHERE {where}")
            self.sel[spec.name] = self.s.prepare(f"SELECT {', '.join(cols)} FROM {KEYSPACE}.{spec.name} WHERE {where}")

    def write(self, ops: list) -> None:
        """ops: (spec, row or None, key parts, version). Distinct keys, so they may run concurrently."""
        from cassandra.concurrent import execute_concurrent

        stmts = []
        for spec, row, parts, v in ops:
            if row is None:
                stmts.append((self.dele[spec.name], list(parts)))
            else:
                stmts.append((self.ins[spec.name], [row.get(c) for c in spec.column_names] + [v]))
        for ok, res in execute_concurrent(self.s, stmts, concurrency=128, raise_on_first_error=False):
            if not ok:
                raise res

    def read(self, spec: TableSpec, parts) -> tuple[int | None, dict | None]:
        r = self.s.execute(self.sel[spec.name], list(parts)).one()
        if r is None:
            return None, None
        row = dict(zip(spec.column_names + ["src_v"], r))
        return row.pop("src_v"), row

    def scan(self, spec: TableSpec):
        from cassandra.query import SimpleStatement

        stmt = SimpleStatement(f"SELECT {', '.join(spec.column_names)} FROM {KEYSPACE}.{spec.name}", fetch_size=5000)
        for r in self.s.execute(stmt, timeout=300):
            yield dict(zip(spec.column_names, r))


def scan_table(rows_by_spec) -> Table:
    khi, klo, dig = [], [], []
    for spec, rows in rows_by_spec:
        for raw in rows:
            h, l, d = spec.element(raw)
            khi.append(h)
            klo.append(l)
            dig.append(d)
    u = lambda x: np.array(x, dtype=np.uint64)
    return Table(u(khi), u(klo), u(dig), np.zeros(len(klo), np.int64))


def corrupt(spec: TableSpec, row: dict) -> dict:
    """Change the first non-key column the way a mapping bug would."""
    c = spec.columns[0]
    v = row.get(c.name)
    t = c.type
    if isinstance(t, canon.Dec):
        v = (v or Decimal(0)) + Decimal("0.01")
    elif isinstance(t, canon.Float):
        v = float(v or 0) + 1.0
    elif isinstance(t, canon.Int):
        v = (v or 0) + 1
    elif isinstance(t, canon.Timestamp):
        v = v + timedelta(hours=1)
    else:
        v = (v or "") + "x"
    return {**row, c.name: v}


# ---------------------------------------------------------------- live run


@dataclass
class Stats:
    changes: int = 0
    skipped_history: int = 0
    dropped: int = 0
    corrupted: int = 0
    deferred: int = 0
    applied_ops: int = 0
    poll_cpu_s: float = 0.0
    apply_cpu_s: float = 0.0
    source_sketch_s: float = 0.0
    target_sketch_s: float = 0.0
    max_backlog: int = 0
    lag_samples: list = field(default_factory=list)


class Live:
    def __init__(self, args):
        self.a = args
        self.rng = np.random.default_rng(args.seed)
        self.cfg = LadderConfig(max_capacity=262_144)
        self.src, self.tgt = Sketch(self.cfg), Sketch(self.cfg)
        self.last_t: dict[Key, int] = {}  # target-side digest per key (Lemma 2's side table)
        # Source states per key for confirmation: the state at the watermark, plus later changes.
        self.src_base: dict[Key, tuple[int, int | None]] = {}
        self.src_later: dict[Key, list] = {}
        # (version, key) of every entry in src_later, smallest version first, so advancing to w touches only the
        # changes at or below w; scanning all of src_later made each check cost grow with the replicator's backlog.
        self.src_due: list = []
        # What the replicator did to each key's last processed change: (status, lsn). Ground truth.
        self.last_status: dict[Key, tuple[str, int]] = {}
        self.faulty: dict[Key, tuple[str, int]] = {}  # keys whose last processed change was faulty
        self.reported_cache: dict[Key, int] = {}  # key -> version of its last processed change when reported
        self.in_flight: set = set()  # keys of the batch being written to the target right now
        self.misses: list[dict] = []  # every fault at or below the watermark that a check did not report
        self.queue: deque = deque()
        self.deferred: list = []  # (release_time, lsn, item)
        self.blocked: dict[Key, float] = {}  # key -> release time of its latest deferred change
        self.src_pending: deque = deque()  # (lsn, khi, klo, dig, sign)
        self.qlock, self.tlock, self.slock = threading.Lock(), threading.Lock(), threading.Lock()
        self.w_t = 0
        self.next_w = 0
        self.max_processed = 0
        self.stop_polling = threading.Event()
        self.poll_done = threading.Event()
        self.stats = Stats()
        self.verifications: list[dict] = []
        self.reports: list[dict] = []
        # Audit trail for reports that the replicator's recorded status does not explain: the state the check
        # read, and every later change the replicator processed for those keys.
        self.read_states: dict[Key, tuple] = {}  # key -> (version, digest) last read from the target
        self.watch: set = set()
        self.followups: list[dict] = []
        self.n_live = 0  # live checks so far, for --ladder-every
        self.ever_faulty: set = set()  # every key the replicator ever mishandled, to classify other tools' reports

    # -- setup

    def setup(self, pg, target: Target) -> dict:
        t0 = time.perf_counter()
        for spec in TPCC:
            pg.execute(f"ALTER TABLE {spec.name} REPLICA IDENTITY FULL")
        pg.execute(f"SELECT pg_create_logical_replication_slot('{SLOT}', 'wal2json')")
        target.create()
        rows_total = 0
        for spec in TPCC:
            ops, kh, kl, dg = [], [], [], []
            for raw in postgres_rows(pg, spec.name, spec.column_names):
                row = spec.normalize(raw)
                h, l = spec.key_of(row)
                d = spec.digest(row)
                kh.append(h)
                kl.append(l)
                dg.append(d)
                self.last_t[(h, l)] = d
                self.src_base[(h, l)] = (0, d)
                ops.append((spec, row, None, 0))
                if len(ops) >= 5000:
                    target.write(ops)
                    ops = []
            if ops:
                target.write(ops)
            u = lambda x: np.array(x, dtype=np.uint64)
            self.src.update(u(kh), u(kl), u(dg))
            self.tgt.update(u(kh), u(kl), u(dg))
            rows_total += len(kh)
            print(f"  backfilled {spec.name}: {len(kh)} rows", flush=True)
        return {"backfill_rows": rows_total, "backfill_s": round(time.perf_counter() - t0, 1)}

    # -- change stream

    def poller(self) -> None:
        conn = connect_pg()
        empty = 0
        while True:
            t0 = time.thread_time()
            rows = conn.execute(
                f"SELECT lsn::text, data FROM pg_logical_slot_get_changes('{SLOT}', NULL, %s, "
                "'format-version', '2', 'include-transaction', 'false')", (5000,)).fetchall()
            arrived = time.monotonic()
            batch, src, hist = [], [], []
            for lsn_s, data in rows:
                ev = json.loads(data, parse_float=Decimal)
                spec = TPCC_BY_NAME.get(ev.get("table"))
                if spec is None:
                    self.stats.skipped_history += 1
                    continue
                lsn = parse_lsn(lsn_s)
                new = {c["name"]: c["value"] for c in ev.get("columns", [])} or None
                old = {c["name"]: c["value"] for c in ev.get("identity", [])} or None
                t1 = time.perf_counter()
                new_e = spec.element(new) if new is not None else None
                if old is not None:
                    src.append((lsn, *spec.element(old), -1))
                if new_e is not None:
                    src.append((lsn, *new_e, +1))
                self.stats.source_sketch_s += time.perf_counter() - t1
                row = spec.normalize(new) if new is not None else None
                parts = tuple(int((new or old)[k]) for k in spec.key)
                key = canon.pack_key(spec.tag, parts)
                hist.append((key, lsn, new_e[2] if new_e else None))
                batch.append((arrived, lsn, spec, row, parts, key))
            self.stats.changes += len(batch)
            with self.slock:
                self.src_pending.extend(src)
                for key, lsn, d in hist:
                    self.src_later.setdefault(key, []).append((lsn, d))
                    heapq.heappush(self.src_due, (lsn, key))
            with self.qlock:
                self.queue.extend(batch)
                self.stats.max_backlog = max(self.stats.max_backlog, len(self.queue) + len(self.deferred))
            self.stats.poll_cpu_s += time.thread_time() - t0
            if not rows:
                if self.stop_polling.is_set():
                    empty += 1
                    if empty >= 3:
                        break
                time.sleep(0.05)
            else:
                empty = 0
        self.poll_done.set()

    # -- replicator (the pipeline under test)

    def _take_ready(self, now: float) -> list:
        """Changes due now, in LSN order. With --reorder, defer some (keeping per-key order)."""
        ready = []
        with self.qlock:
            due, keep = [], []
            for d in self.deferred:
                (due if d[0] <= now else keep).append(d)
            self.deferred = keep
            for _, _, item in due:
                ready.append(item)
            for _, _, item in due:
                if self.blocked.get(item[5], 0) <= now:
                    self.blocked.pop(item[5], None)
            n = 0
            while self.queue and self.queue[0][0] + self.a.lag <= now and n < 4000:
                item = self.queue.popleft()
                n += 1
                key = item[5]
                if key in self.blocked:
                    release = self.blocked[key]
                elif self.a.reorder and self.rng.random() < self.a.reorder:
                    release = now + self.rng.uniform(0.5, 1.5)
                    self.stats.deferred += 1
                else:
                    ready.append(item)
                    continue
                self.blocked[key] = release
                self.deferred.append((release, item[1], item))
            outstanding = [d[1] for d in self.deferred]
            if self.queue:
                outstanding.append(self.queue[0][1])
            ready.sort(key=lambda x: x[1])
            if ready:
                self.max_processed = max(self.max_processed, ready[-1][1])
            self.next_w = (min(outstanding) - 1) if outstanding else self.max_processed
            drained = not self.queue and not self.deferred
        return ready, drained

    def applier(self, target: Target) -> None:
        while True:
            now = time.monotonic()
            ready, drained = self._take_ready(now)
            if not ready:
                if self.poll_done.is_set() and drained:
                    break
                with self.tlock:
                    self.w_t = max(self.w_t, min(self.next_w, self.max_processed))
                time.sleep(0.02)
                continue
            t0 = time.thread_time()
            final: dict[Key, tuple] = {}
            status = []
            for arrived, lsn, spec, row, parts, key in ready:
                r = self.rng.random()
                if r < self.a.drop:
                    self.stats.dropped += 1
                    status.append((key, "drop", lsn))
                    continue
                if row is not None and spec.columns and r < self.a.drop + self.a.corrupt:
                    row = corrupt(spec, row)
                    self.stats.corrupted += 1
                    status.append((key, "corrupt", lsn))
                else:
                    status.append((key, "ok", lsn))
                final[key] = (spec, row, parts, lsn)
                self.stats.lag_samples.append(now - arrived)
            # The store write happens outside the lock, so checks never wait behind network writes.
            # The sketch, statuses and watermark are updated together under the lock afterwards, so a check
            # always sees a target sketch that reflects exactly the changes up to the watermark; a check that
            # reads the target meanwhile may see newer rows, which confirmation allows.
            batch_keys = list(final)
            with self.tlock:
                self.in_flight.update(batch_keys)
            target.write(list(final.values()))
            self.stats.applied_ops += len(final)
            t1 = time.perf_counter()
            old_k, old_d, new_k, new_d = [], [], [], []
            for key, (spec, row, parts, lsn) in final.items():  # last_t is touched only by this thread
                prev = self.last_t.get(key)
                if prev is not None:
                    old_k.append(key)
                    old_d.append(prev)
                if row is None:
                    self.last_t.pop(key, None)
                else:
                    d = spec.digest(row)
                    self.last_t[key] = d
                    new_k.append(key)
                    new_d.append(d)
            u = lambda x: np.array(x, dtype=np.uint64)
            with self.tlock:
                if old_k:
                    self.tgt_update(u([k[0] for k in old_k]), u([k[1] for k in old_k]), u(old_d), -1)
                if new_k:
                    self.tgt_update(u([k[0] for k in new_k]), u([k[1] for k in new_k]), u(new_d), +1)
                self.in_flight.difference_update(batch_keys)
                for key, st, lsn in status:
                    if key in self.watch:
                        self.followups.append({"t": round(time.monotonic() - self.t_start, 1), "key": key,
                                               "status": st, "version": lsn})
                    self.last_status[key] = (st, lsn)
                    if st in FAULTY:  # small map of currently faulty keys, cheap to copy at checks
                        self.faulty[key] = (st, lsn)
                        self.ever_faulty.add(key)
                    else:
                        self.faulty.pop(key, None)
                self.w_t = max(self.w_t, self.next_w)
            self.stats.target_sketch_s += time.perf_counter() - t1
            self.stats.apply_cpu_s += time.thread_time() - t0

    # -- verification

    def advance_source(self, w: int) -> None:
        take = []
        with self.slock:
            while self.src_pending and self.src_pending[0][0] <= w:
                take.append(self.src_pending.popleft())
            keys = set()
            while self.src_due and self.src_due[0][0] <= w:
                keys.add(heapq.heappop(self.src_due)[1])
            for key in keys:
                hist = self.src_later.get(key)
                if not hist:
                    continue
                i = 0
                while i < len(hist) and hist[i][0] <= w:
                    i += 1
                if i:
                    self.src_base[key] = hist[i - 1]
                    if i == len(hist):
                        del self.src_later[key]
                    else:
                        self.src_later[key] = hist[i:]
        if take:
            arr = np.array([t[1:4] for t in take], dtype=np.uint64)
            sign = np.array([t[4] for t in take])
            for s in (-1, 1):
                m = sign == s
                if m.any():
                    self.src_update(arr[m, 0], arr[m, 1], arr[m, 2], s)

    # Every sketch change goes through these, so subclasses can maintain more than one sketch.
    def src_update(self, khi, klo, dig, sign: int) -> None:
        self.src.update(khi, klo, dig, sign)

    def tgt_update(self, khi, klo, dig, sign: int) -> None:
        self.tgt.update(khi, klo, dig, sign)

    def snapshot_extra(self):
        """Hook: copy the target side of any other sketches while verify_now holds the lock."""
        return None

    def compare_extra(self, state, candidates: set) -> dict:
        """Hook: compare other source sketches (already advanced to the watermark) with the target copies
        from snapshot_extra, unlocked. Returns fields for the record."""
        return {}

    def valid_states(self, key: Key) -> list:
        with self.slock:
            return [self.src_base.get(key, (0, None))] + list(self.src_later.get(key, ()))

    def confirm_many(self, target: Target, keys: list) -> list:
        """Theorem 3: report k iff the target holds no state the source had for k at or after w_T.

        Target rows are read concurrently; the source is never read."""
        from cassandra.concurrent import execute_concurrent

        specs, stmts = [], []
        for key in keys:
            tag, parts = canon.unpack_key(*key, 4)
            spec = TPCC_BY_TAG[tag]
            specs.append(spec)
            stmts.append((target.sel[spec.name], list(parts[: len(spec.key)])))
        reported = []
        results = execute_concurrent(target.s, stmts, concurrency=128, raise_on_first_error=True)
        for key, spec, (ok, res) in zip(keys, specs, results):
            r = res.one()
            valid = self.valid_states(key)
            if r is None:
                self.read_states[key] = (None, None)
                bad = not any(d is None for _, d in valid)
            else:
                row = dict(zip(spec.column_names + ["src_v"], r))
                v_t = row.pop("src_v")
                self.read_states[key] = (v_t, spec.element(row)[2])
                bad = (v_t, spec.element(row)[2]) not in valid
            if bad:
                reported.append(key)
        return reported

    def verify_now(self, pg, target: Target, label: str) -> dict:
        """Two short pauses of the replicator instead of one long one.

        Phase 1 freezes the state at w_T by copying the sketches; they are decoded unlocked.
        Phase 2 confirms candidates against the target. A key reported at an earlier check whose
        last processed change has not changed since keeps its target row, and every source state it
        could still match has a newer version, so its verdict is reused without a read. Reading the
        target after w_T is sound because valid states include every delivered change after w_T; a
        fault repaired by a later change before the read is no longer a divergence and is not
        counted as missed."""
        t0 = time.perf_counter()
        # Candidates come from the primary sketch a subclass provides (rateless), with the ladder decoded only
        # every --ladder-every live checks as the comparison baseline, and whenever the primary cannot decode.
        # Both are maintained on every change, so either one decoded at w gives the same keys.
        self.n_live += label == "live"
        every = getattr(self.a, "ladder_every", 1)
        use_ladder = label != "live" or every <= 1 or self.n_live % every == 0
        # phase 1: the watermark and a copy of what the replicator modifies (target side)
        w, tgt, extra_state, lock_ms = self._phase1(use_ladder)
        # Only the verifier changes the source sketch, so it advances to w without pausing the replicator.
        self.advance_source(w)
        rep = verify(self.src, tgt, self.full_rescan(pg, target)) if use_ladder else None
        extra = self.compare_extra(extra_state, set(rep.keys) if rep else None)  # same frozen instant
        primary = extra.pop("riblt_keys", None)
        if rep is None and primary is None:  # no primary sketch, or it could not decode: use the ladder
            if tgt is None:  # (split processes) the ladder was not copied: freeze again, at a new watermark
                w, tgt, extra_state, more_ms = self._phase1(True)
                lock_ms += more_ms
                self.advance_source(w)
            rep = verify(self.src, tgt, self.full_rescan(pg, target))
        t_sketch = time.perf_counter() - t0
        cands = sorted(rep.keys if rep else primary)
        t1 = time.perf_counter()
        reported, status = [], {}
        frozen = self._freeze(cands)  # phase 2: confirm, re-reading only keys that may have changed
        try:
            version = lambda k: frozen["version"].get(k)
            writing = frozen["writing"]
            # The cache is sound because a version names one state exactly; without delete versions (the
            # --no-tombstones ablation) an absent row cannot be matched to a delete, so every candidate is re-read.
            reuse = not getattr(self.a, "no_tombstones", False)
            cached = [k for k in cands if reuse and k in self.reported_cache and self.reported_cache[k] == version(k)
                      and k not in writing]  # a key being written now may have a new row: read it
            cached_set = set(cached)
            fresh = [k for k in cands if k not in cached_set]
            reported = cached + (self.confirm_many(target, fresh) if fresh else [])
            self.reported_cache = {k: version(k) for k in reported}
            state = self._truth_state(reported)
            faulty, status, held = state["faulty"], state["status"], state["last_t"]
            # A fault can leave the content agreeing at w, e.g. a dropped delete of a row whose insert was dropped
            # too: both stores lack the row, so there is no divergence to report. For a faulty key whose last
            # change is at or below w and not being written, last_t is the target's content at w.
            with self.slock:
                agree = {k for k, (st, lsn) in faulty.items() if lsn <= w and k not in writing
                         and held.get(k) == self.src_base.get(k, (0, None))[1]}
        finally:  # keys whose report the recorded status does not explain are watched from here on
            self._release([k for k in reported if (status.get(k) or ("ok", 0))[0] not in FAULTY])
        lock_ms += (time.perf_counter() - t1) * 1000
        # Ground truth at the instant of confirmation. A key whose new change is being written right now has
        # a status about to change (its row may already hold a newer, valid state), so it is left out and counted.
        truth_all = {k for k, (st, lsn) in faulty.items() if lsn <= w} - agree
        truth_now = truth_all - writing
        rep_set = set(reported)
        for k in sorted(truth_now - rep_set):  # every miss is logged with its context, for audit
            self.misses.append({"t": round(time.monotonic() - self.t_start, 1), "key": k, "watermark": w,
                                "status": faulty.get(k), "last_status": faulty.get(k),
                                "candidate": k in set(cands), "valid_states": self.valid_states(k)})
        # A report whose key's recorded status is not a fault is false, unless the key was being written when the
        # check read it: its status is recorded only after the write, so the row read may come from a newer change
        # (possibly itself a fault). Those are counted apart, as misses are, and resolved from the follow-up log.
        unexplained = [k for k in reported if (status.get(k) or ("ok", 0))[0] not in FAULTY]
        false_rep = [k for k in unexplained if k not in writing]
        rewritten_rep = [k for k in unexplained if k in writing]
        rec = {"label": label, "t": round(time.monotonic() - self.t_start, 1), "watermark": w,
               "ladder_decoded": rep is not None,
               "estimate": rep.estimate if rep else None, "level": rep.level if rep else None,
               "bytes": rep.cost.bytes if rep else None, "round_trips": rep.cost.round_trips if rep else None,
               "rows_rescanned": rep.cost.rows_scanned if rep else 0,
               "failed_partitions": rep.failed_partitions if rep else 0, "candidates": len(cands),
               "reported": len(reported), "reported_false": len(false_rep),
               "reported_while_rewritten": len(rewritten_rep),
               "faulty_at_or_below_w": len(truth_now), "missed": len(truth_now - rep_set),
               "faulty_being_rewritten": len(truth_all & writing), "faulty_content_agrees": len(agree),
               "target_point_reads": len(fresh), "cached_verdicts": len(cached),
               "sketch_ms": round(t_sketch * 1000, 1), "replicator_paused_ms": round(lock_ms, 1),
               "total_ms": round((time.perf_counter() - t0) * 1000, 1), **extra}
        self.verifications.append(rec)
        for k in unexplained:
            self.reports.append({"t": rec["t"], "key": k, "watermark": w, "status": status.get(k),
                                 "being_written": k in writing, "cached_verdict": k in cached_set,
                                 "target_state_read": self.read_states.get(k), "valid_states": self.valid_states(k)})
        return {"rec": rec, "candidates": cands, "reported": reported}

    # -- state shared with the replicator. In one process these use the lock; bench/live_split.py runs the
    # replicator in another process and implements them as messages to it.

    def _phase1(self, want_ladder: bool):
        """Watermark and copies of the target-side sketches, at one instant. Returns (w, ladder copy or None if
        not wanted, other sketches' copies, ms the replicator was paused)."""
        t0 = time.perf_counter()
        with self.tlock:
            w = self.w_t
            tgt = copy.deepcopy(self.tgt)
            extra_state = self.snapshot_extra()
        return w, tgt, extra_state, (time.perf_counter() - t0) * 1000

    def _freeze(self, keys: list) -> dict:
        """Pause status updates until _release; return keys being written and each key's last processed version."""
        self.tlock.acquire()
        return {"writing": set(self.in_flight),
                "version": {k: self.last_status[k][1] for k in keys if k in self.last_status}}

    def _truth_state(self, reported: list) -> dict:
        """(While frozen) the harness's ground truth: faulty keys, the status of reported keys, and the target
        content last applied for each faulty key."""
        faulty = dict(self.faulty)
        return {"faulty": faulty, "status": {k: self.last_status.get(k) for k in reported},
                "last_t": {k: self.last_t.get(k) for k in faulty}}

    def _release(self, watch: list) -> None:
        self.watch.update(watch)
        self.tlock.release()

    def full_rescan(self, pg, target: Target):
        cache = {}

        def rescan(partitions: int, p: int):
            if "t" not in cache:
                print("  rescan: reading both stores in full", flush=True)
                cache["s"] = scan_table((s, postgres_rows(pg, s.name, s.column_names)) for s in TPCC)
                cache["t"] = scan_table((s, target.scan(s)) for s in TPCC)
            s, t = cache["s"], cache["t"]
            if partitions == 1:
                return s, t
            P, pp = np.uint64(partitions), np.uint64(p)
            return s.take(key_hash(s.khi, s.klo) % P == pp), t.take(key_hash(t.khi, t.klo) % P == pp)

        return rescan


def sketches_equal(x: Sketch, y: Sketch) -> bool:
    return all(np.array_equal(getattr(p, f), getattr(q, f))
               for p, q in zip([x.strata.table, *x.levels], [y.strata.table, *y.levels])
               for f in ("count", "khi", "klo", "dig", "chk"))


def run_live(args) -> None:
    print("restoring PostgreSQL", flush=True)
    restore_postgres()
    pg = connect_pg()
    cluster, session = connect_cassandra()
    target = Target(session)
    live = Live(args)
    out = {"mode": "live", "rate": args.rate, "seconds": args.seconds, "terminals": args.terminals,
           "lag_s": args.lag, "reorder": args.reorder, "drop": args.drop, "corrupt": args.corrupt,
           "interval_s": args.interval}
    print("backfilling Cassandra", flush=True)
    out.update(live.setup(pg, target))

    threads = [threading.Thread(target=live.poller, daemon=True),
               threading.Thread(target=live.applier, args=(target,), daemon=True)]
    live.t_start = time.monotonic()
    for t in threads:
        t.start()
    tag = f"live_r{args.rate}_l{args.lag:g}" + (f"_o{args.reorder:g}" if args.reorder else "")
    bb = benchbase(tag, args.rate, args.seconds, args.terminals)
    print(f"TPC-C running ({args.seconds}s)", flush=True)
    next_verify = time.monotonic() + args.interval
    while bb.poll() is None:
        if time.monotonic() >= next_verify:
            r = live.verify_now(pg, target, "live")["rec"]
            print(f"  t={r['t']}s cand={r['candidates']} reported={r['reported']} false={r['reported_false']} "
                  f"missed={r['missed']} bytes={r['bytes']} backlog={len(live.queue) + len(live.deferred)}", flush=True)
            next_verify = time.monotonic() + args.interval
        time.sleep(0.2)
    print("TPC-C finished; draining the change stream", flush=True)
    out.update(benchbase_summary(tag))
    live.stop_polling.set()
    for t in threads:
        t.join()

    final = live.verify_now(pg, target, "final")
    print("final full scans for ground truth", flush=True)
    s_scan = scan_table((s, postgres_rows(pg, s.name, s.column_names)) for s in TPCC)
    t_scan = scan_table((s, target.scan(s)) for s in TPCC)
    a, b = exact_diff(s_scan, t_scan)
    truth = {(h, l) for h, l, _ in a} | {(h, l) for h, l, _ in b}
    rebuilt_s, rebuilt_t = Sketch(live.cfg), Sketch(live.cfg)
    rebuilt_s.update(s_scan.khi, s_scan.klo, s_scan.dig)
    rebuilt_t.update(t_scan.khi, t_scan.klo, t_scan.dig)
    lag = np.array(live.stats.lag_samples) if live.stats.lag_samples else np.zeros(1)
    st = live.stats
    lv = [v for v in live.verifications if v["label"] == "live"]
    found, reported = set(final["candidates"]), set(final["reported"])
    out.update({
        "changes_captured": st.changes, "history_changes_skipped": st.skipped_history,
        "dropped": st.dropped, "corrupted": st.corrupted, "deferred": st.deferred,
        "applied_ops": st.applied_ops, "max_backlog": st.max_backlog,
        "apply_delay_p50_s": round(float(np.percentile(lag, 50)), 2), "apply_delay_p99_s": round(float(np.percentile(lag, 99)), 2),
        "source_sketch_us_per_change": round(st.source_sketch_s / max(1, st.changes) * 1e6, 1),
        "target_sketch_us_per_op": round(st.target_sketch_s / max(1, st.applied_ops) * 1e6, 1),
        "live_verifications": len(lv),
        "live_candidates_total": sum(v["candidates"] for v in lv),
        "live_reported_total": sum(v["reported"] for v in lv),
        "live_reported_false_total": sum(v["reported_false"] for v in lv),
        "live_missed_total": sum(v["missed"] for v in lv),
        "live_candidates_minus_reported_total": sum(v["candidates"] - v["reported"] for v in lv),
        "live_target_point_reads": sum(v["target_point_reads"] for v in lv),
        "live_check_ms_median": float(np.median([v["total_ms"] for v in lv])) if lv else None,
        "final_rows_source": len(s_scan), "final_rows_target": len(t_scan),
        "final_truth_keys": len(truth), "final_candidates": len(found), "final_reported": len(reported),
        "final_reported_equals_truth": reported == truth, "final_candidates_equal_truth": found == truth,
        "final_bytes": final["rec"]["bytes"], "final_level": final["rec"]["level"],
        "source_sketch_matches_rescan": sketches_equal(live.src, rebuilt_s),
        "target_sketch_matches_rescan": sketches_equal(live.tgt, rebuilt_t),
    })
    pg.execute(f"SELECT pg_drop_replication_slot('{SLOT}')")
    cluster.shutdown()
    write_results(tag, out, live.verifications, live.reports, live.misses)


def run_baseline(args) -> None:
    print("restoring PostgreSQL", flush=True)
    restore_postgres()
    tag = f"baseline_r{args.rate}"
    bb = benchbase(tag, args.rate, args.seconds, args.terminals)
    bb.wait()
    out = {"mode": "baseline", "rate": args.rate, "seconds": args.seconds, "terminals": args.terminals, **benchbase_summary(tag)}
    write_results(tag, out, [], [])


def write_results(tag: str, out: dict, verifications: list, reports: list, misses: list = ()) -> None:
    d = ROOT / "results" / "live"
    d.mkdir(parents=True, exist_ok=True)
    with (d / f"{tag}.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["metric", "value"])
        w.writerows(out.items())
    for name, rows in (("verifications", verifications), ("unexplained_reports", reports), ("missed", list(misses))):
        if rows:
            with (d / f"{tag}_{name}.csv").open("w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=list(rows[0]))
                w.writeheader()
                w.writerows(rows)
    for k, v in out.items():
        print(f"{k:38} {v}")
    print(f"wrote {d / tag}.csv")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["baseline", "live"])
    p.add_argument("--rate", default="200", help="TPC-C transactions per second (BenchBase rate), or 'unlimited'")
    p.add_argument("--seconds", type=int, default=120)
    p.add_argument("--terminals", type=int, default=8)
    p.add_argument("--lag", type=float, default=2.0, help="replication delay in seconds")
    p.add_argument("--reorder", type=float, default=0.0, help="fraction of changes deferred 0.5-1.5 s (out-of-order)")
    p.add_argument("--drop", type=float, default=0.0005)
    p.add_argument("--corrupt", type=float, default=0.0005)
    p.add_argument("--interval", type=float, default=10.0, help="seconds between live verifications")
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()
    (run_live if args.mode == "live" else run_baseline)(args)


if __name__ == "__main__":
    main()
