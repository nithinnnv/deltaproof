"""Fault injection on the dual-write path (paper Sections II-B, VI-A).

Starting from one ledger, build a source and a target that differ by exactly the injected
faults, and return the ground-truth set of differing keys per fault class.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .hashing import key_hash
from .store import Table
from .workload import MEMOS, Ledger, generate

FAULT_CLASSES = (
    "lost_write",  # key missing in target
    "phantom",  # extra key in target (retry without idempotency)
    "stale_value",  # backfill overwrote a newer write with an older value
    "corruption",  # wrong value from a type-mapping bug
    "precision",  # amount off by one cent (float rounding)
    "timezone",  # timestamp shifted by whole hours
    "lost_delete",  # deleted in source, still present in target
    "offsetting",  # one lost write plus one phantom of equal amount in the same partition
)

Key = tuple[int, int]


@dataclass
class Injected:
    source: Table
    target: Table
    truth: dict[str, set[Key]]

    @property
    def all_keys(self) -> set[Key]:
        return set().union(*self.truth.values())


def _keys(ids) -> set[Key]:
    return {(0, int(i)) for i in ids}


def _same_partition_keys(targets: np.ndarray, start: int, partitions: int) -> np.ndarray:
    """For each target key, a fresh key >= start that lands in the same aggregate partition."""
    zeros = lambda a: np.zeros(len(a), np.uint64)
    want = key_hash(zeros(targets), targets.astype(np.uint64)) % np.uint64(partitions)
    out = np.empty(len(targets), np.uint64)
    pool, nxt = {}, start
    for i, w in enumerate(want.tolist()):
        while not pool.get(w):
            block = np.arange(nxt, nxt + 4 * partitions, dtype=np.uint64)
            nxt += 4 * partitions
            for k, p in zip(block.tolist(), (key_hash(zeros(block), block) % np.uint64(partitions)).tolist()):
                pool.setdefault(p, []).append(k)
        out[i] = pool[w].pop()
    return out


def inject(ledger: Ledger, counts: dict[str, int], rng: np.random.Generator, agg_partitions: int = 256) -> Injected:
    n = len(ledger)
    perm = rng.permutation(n)
    picks, at = {}, 0
    for cls in FAULT_CLASSES:
        c = counts.get(cls, 0) if cls != "phantom" else 0
        picks[cls] = perm[at:at + c]
        at += c
    if at > n:
        raise ValueError("more faults than rows")

    src, tgt = ledger.copy(), ledger.copy()
    truth = {cls: set() for cls in FAULT_CLASSES}
    for cls, col, change in (
        ("stale_value", "amount", lambda i: rng.integers(1, 10_000, len(i))),
        ("precision", "amount", lambda i: rng.choice([-1, 1], len(i))),
        ("timezone", "posted_at", lambda i: rng.choice([-12, -8, -5, -1, 1, 3, 5, 9], len(i)) * 3_600_000_000),
        ("corruption", "memo", lambda i: rng.integers(1, len(MEMOS), len(i))),
    ):
        i = picks[cls]
        arr = getattr(tgt, col)
        arr[i] = (arr[i] + change(i)) % len(MEMOS) if col == "memo" else arr[i] + change(i)
        truth[cls] = _keys(ledger.entry_id[i])

    next_id = int(ledger.entry_id.max()) + 1
    extra = []
    if counts.get("phantom", 0):
        ph = generate(counts["phantom"], rng, start_id=next_id)
        next_id += counts["phantom"]
        extra.append(ph)
        truth["phantom"] = _keys(ph.entry_id)
    if len(picks["offsetting"]):
        lost = picks["offsetting"]
        twin = ledger.take(lost).copy()
        twin.entry_id = _same_partition_keys(ledger.entry_id[lost], next_id, agg_partitions)
        twin.txn_id = twin.txn_id + 10**12
        extra.append(twin)
        truth["offsetting"] = _keys(ledger.entry_id[lost]) | _keys(twin.entry_id)

    drop_tgt = np.zeros(n, bool)
    drop_tgt[picks["lost_write"]] = True
    drop_tgt[picks["offsetting"]] = True
    truth["lost_write"] = _keys(ledger.entry_id[picks["lost_write"]])
    drop_src = np.zeros(n, bool)
    drop_src[picks["lost_delete"]] = True
    truth["lost_delete"] = _keys(ledger.entry_id[picks["lost_delete"]])

    src = src.take(~drop_src)
    tgt = tgt.take(~drop_tgt)
    for e in extra:
        tgt = tgt.concat(e)
    return Injected(src.table(), tgt.table(), truth)
