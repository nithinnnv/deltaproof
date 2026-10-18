"""Baselines (paper Section VI-B), costed with the same model as DeltaProof.

Cost model, shared by every method:
  bytes        - data sent from the stores to the verifier
  round_trips  - sequential request waves (requests within a wave run in parallel)
  rows_scanned - rows the stores read in bulk (scans, range checksums, downloads)
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

import numpy as np

from .hashing import key_hash, mix
from .sketch import ELEMENT_BYTES
from .store import Table, exact_diff
from .verify import Cost, Key, classify

CHECKSUM_BYTES = 16  # count + 64-bit checksum
HASH_BYTES = 8


@dataclass
class Result:
    name: str
    keys: set[Key] | None  # differing keys found; None if the method cannot localize
    cost: Cost
    flagged_partitions: set[int] = field(default_factory=set)
    partitions: int = 0


def _keys(only_s, only_t) -> set[Key]:
    missing, extra, mismatch = classify(only_s, only_t)
    return missing | extra | mismatch


def full_scan(s: Table, t: Table) -> Result:
    cost = Cost(bytes=(len(s) + len(t)) * ELEMENT_BYTES, round_trips=1, rows_scanned=len(s) + len(t))
    return Result("full_scan", _keys(*exact_diff(s, t)), cost)


def aggregates(s: Table, t: Table, partitions: int = 256) -> Result:
    """Row count and amount sum per partition. Cheap, but cannot localize keys."""

    def agg(x: Table):
        p = (key_hash(x.khi, x.klo) % np.uint64(partitions)).astype(np.int64)
        cnt = np.bincount(p, minlength=partitions)
        tot = np.zeros(partitions, np.int64)
        np.add.at(tot, p, x.amount)
        return cnt, tot

    (cs, ts), (ct, tt) = agg(s), agg(t)
    flagged = set(np.flatnonzero((cs != ct) | (ts != tt)).tolist())
    cost = Cost(bytes=2 * partitions * CHECKSUM_BYTES, round_trips=1, rows_scanned=len(s) + len(t))
    return Result("aggregates", None, cost, flagged, partitions)


def bisection(s: Table, t: Table, factor: int = 32, threshold: int = 16384) -> Result:
    """Range-checksum bisection as in open-source data-diff. Integer keys, both stores key-ordered.

    This is the baseline's best case: a hash-partitioned target (e.g. Cassandra) cannot
    serve key-range checksums without scanning, which would raise its cost further.
    """
    if np.any(s.khi) or np.any(t.khi):
        raise ValueError("bisection baseline supports 64-bit integer keys only")
    cost = Cost(round_trips=1, bytes=2 * 2 * HASH_BYTES)  # min/max key query
    os_, ot = np.argsort(s.klo, kind="stable"), np.argsort(t.klo, kind="stable")
    ks, kt = s.klo[os_], t.klo[ot]
    es, et = s.element_hashes()[os_], t.element_hashes()[ot]
    with np.errstate(over="ignore"):
        ps = np.concatenate([[np.uint64(0)], np.cumsum(es, dtype=np.uint64)])
        pt = np.concatenate([[np.uint64(0)], np.cumsum(et, dtype=np.uint64)])
    lo_key = min(ks[:1].tolist() + kt[:1].tolist() or [0])
    hi_key = max(ks[-1:].tolist() + kt[-1:].tolist() or [0]) + 1

    def split(lo: np.ndarray, hi: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        k = np.arange(factor + 1, dtype=np.uint64)
        width = hi - lo
        bounds = lo[:, None] + (width[:, None] * k[None, :]) // np.uint64(factor)
        nlo, nhi = bounds[:, :-1].ravel(), bounds[:, 1:].ravel()
        keep = nhi > nlo
        return nlo[keep], nhi[keep]

    lo, hi = split(np.array([lo_key], np.uint64), np.array([hi_key], np.uint64))
    only_s, only_t = [], []
    while lo.size:
        cost.round_trips += 1
        i0, i1 = np.searchsorted(ks, lo), np.searchsorted(ks, hi)
        j0, j1 = np.searchsorted(kt, lo), np.searchsorted(kt, hi)
        cs, ct = i1 - i0, j1 - j0
        with np.errstate(over="ignore"):
            hs, ht = ps[i1] - ps[i0], pt[j1] - pt[j0]
        cost.bytes += lo.size * 2 * CHECKSUM_BYTES
        cost.rows_scanned += int(cs.sum() + ct.sum())
        bad = (cs != ct) | (hs != ht)
        small = bad & (np.maximum(cs, ct) <= threshold)
        for a0, a1, b0, b1 in zip(i0[small], i1[small], j0[small], j1[small]):
            a, b = s.take(os_[a0:a1]), t.take(ot[b0:b1])
            cost.rows_scanned += len(a) + len(b)
            cost.bytes += (len(a) + len(b)) * ELEMENT_BYTES
            x, y = exact_diff(a, b)
            only_s += x
            only_t += y
        big = bad & ~small
        lo, hi = split(lo[big], hi[big]) if big.any() else (lo[:0], hi[:0])
    return Result("bisection", _keys(only_s, only_t), cost)


def merkle(s: Table, t: Table, leaf_rows: int = 1024) -> Result:
    """Binary hash tree over key-hash order, compared top-down; mismatched leaves are streamed."""
    n = max(len(s), len(t), 1)
    depth = max(0, math.ceil(math.log2(n / leaf_rows))) if n > leaf_rows else 0
    leaves = 1 << depth

    def leaf_of(x: Table) -> np.ndarray:
        h = key_hash(x.khi, x.klo)
        return (h >> np.uint64(64 - depth)).astype(np.int64) if depth else np.zeros(len(x), np.int64)

    def tree(x: Table, leaf: np.ndarray) -> list[np.ndarray]:
        sums = np.zeros(leaves, np.uint64)
        with np.errstate(over="ignore"):
            np.add.at(sums, leaf, x.element_hashes())
        counts = np.bincount(leaf, minlength=leaves).astype(np.uint64)
        levels = [mix(sums ^ mix(counts))]  # leaf hash covers the leaf's rows and their number
        while len(levels[-1]) > 1:
            c = levels[-1]
            levels.append(mix(c[0::2] ^ mix(c[1::2])))
        return levels[::-1]  # root first

    ls, lt = leaf_of(s), leaf_of(t)
    ts_, tt_ = tree(s, ls), tree(t, lt)
    cost = Cost(rows_scanned=len(s) + len(t), round_trips=1, bytes=2 * HASH_BYTES)
    nodes = np.array([0]) if ts_[0][0] != tt_[0][0] else np.array([], np.int64)
    for d in range(1, depth + 1):
        if nodes.size == 0:
            break
        children = np.concatenate([2 * nodes, 2 * nodes + 1])
        cost.round_trips += 1
        cost.bytes += children.size * 2 * HASH_BYTES
        nodes = children[ts_[d][children] != tt_[d][children]]
    only_s, only_t = [], []
    if nodes.size:
        cost.round_trips += 1
        ms, mt = np.isin(ls, nodes), np.isin(lt, nodes)
        a, b = s.take(ms), t.take(mt)
        cost.rows_scanned += len(a) + len(b)
        cost.bytes += (len(a) + len(b)) * ELEMENT_BYTES
        only_s, only_t = exact_diff(a, b)
    return Result("merkle", _keys(only_s, only_t), cost)
