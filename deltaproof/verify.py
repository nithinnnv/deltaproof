"""The verification protocol (paper Sections III-D and IV)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import numpy as np

from .hashing import key_hash, key_hash_int
from .sketch import ELEMENT_BYTES, Element, Sketch
from .store import Table, exact_diff

Key = tuple[int, int]
Rescan = Callable[[int, int], tuple[Table, Table]]  # (partitions, p) -> source rows, target rows


@dataclass
class Cost:
    bytes: int = 0
    round_trips: int = 0
    rows_scanned: int = 0  # bulk rows read from the stores
    point_reads: int = 0  # single-key reads (confirmation)

    def add(self, other: Cost) -> None:
        self.bytes += other.bytes
        self.round_trips += other.round_trips
        self.rows_scanned += other.rows_scanned
        self.point_reads += other.point_reads


@dataclass
class Report:
    missing_in_target: set[Key]
    extra_in_target: set[Key]
    value_mismatch: set[Key]
    cost: Cost = field(default_factory=Cost)
    estimate: int | None = None
    level: int | None = None  # -1 = resolved by the strata estimator alone, None = full rescan
    failed_partitions: int = 0

    @property
    def keys(self) -> set[Key]:
        return self.missing_in_target | self.extra_in_target | self.value_mismatch


def classify(only_source: list[Element], only_target: list[Element]) -> tuple[set, set, set]:
    ks = {(h, l) for h, l, _ in only_source}
    kt = {(h, l) for h, l, _ in only_target}
    return ks - kt, kt - ks, ks & kt


def table_rescan(source: Table, target: Table) -> Rescan:
    """Rescan by partition over in-memory tables, caching key hashes."""
    hs, ht = key_hash(source.khi, source.klo), key_hash(target.khi, target.klo)

    def rescan(partitions: int, p: int) -> tuple[Table, Table]:
        if partitions == 1:
            return source, target
        P, pp = np.uint64(partitions), np.uint64(p)
        return source.take(hs % P == pp), target.take(ht % P == pp)

    return rescan


def _rescan_cost(cost: Cost, s: Table, t: Table) -> None:
    cost.rows_scanned += len(s) + len(t)
    cost.bytes += (len(s) + len(t)) * ELEMENT_BYTES


def verify(source: Sketch, target: Sketch, rescan: Rescan, headroom: float = 1.25,
           estimate: int | None = None) -> Report:
    """`estimate` replaces the strata estimate (for sensitivity experiments); the result stays exact."""
    cost = Cost(bytes=source.strata.nbytes + target.strata.nbytes, round_trips=1)
    est, exact = source.strata.estimate(target.strata)
    if estimate is not None:
        est, exact = estimate, None
    if exact is not None:
        return Report(*classify(exact.only_a, exact.only_b), cost=cost, estimate=est, level=-1)

    levels = source.config.levels()
    choice = next((i for i, l in enumerate(levels) if l.capacity >= headroom * est), None)
    if choice is None:  # more differences than any level holds: a full scan is cheaper
        s, t = rescan(1, 0)
        _rescan_cost(cost, s, t)
        cost.round_trips += 1
        return Report(*classify(*exact_diff(s, t)), cost=cost, estimate=est, level=None)

    level = levels[choice]
    res = source.levels[choice].subtract(target.levels[choice]).decode()
    cost.bytes += 2 * source.levels[choice].nbytes
    cost.round_trips += 1
    only_s, only_t = res.only_a, res.only_b
    failed = set(res.failed.tolist())
    if failed:
        # Partial peels inside a failed partition are discarded; its rows are compared exactly.
        inside = lambda e: key_hash_int(e[0], e[1]) % level.partitions in failed
        only_s = [e for e in only_s if not inside(e)]
        only_t = [e for e in only_t if not inside(e)]
        for p in sorted(failed):
            s, t = rescan(level.partitions, p)
            _rescan_cost(cost, s, t)
            a, b = exact_diff(s, t)
            only_s += a
            only_t += b
        cost.round_trips += 1
    return Report(
        *classify(only_s, only_t), cost=cost, estimate=est, level=choice, failed_partitions=len(failed)
    )
