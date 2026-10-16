"""Partitioned IBLTs, the strata estimator and the sketch ladder (paper Sections II-C, III).

An element is (khi, klo, dig): a 128-bit primary key and a 64-bit value digest. A changed
value is a different element with the same key, so updates are remove-old + add-new.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

import numpy as np

from .hashing import (
    SEED_STRATA,
    cell_seed,
    check_hash,
    check_hash_int,
    element_hash,
    element_hash_int,
    key_hash,
    key_hash_int,
    mix,
    mix_int,
)

CELL_BYTES = 36  # wire size: 4 count + 16 key + 8 digest + 8 check
ELEMENT_BYTES = 24  # wire size of one (key, digest) pair

Element = tuple[int, int, int]


def _u64(a) -> np.ndarray:
    return np.asarray(a, dtype=np.uint64)


def strata_of(eh: np.ndarray, levels: int) -> np.ndarray:
    """Trailing zeros of a re-mixed element hash, capped at levels - 1."""
    x = mix(eh ^ np.uint64(SEED_STRATA))
    with np.errstate(over="ignore"):
        low = x & (~x + np.uint64(1))
    tz = np.where(low == 0, 64, np.log2(np.maximum(low, 1).astype(np.float64)).astype(np.int64))
    return np.minimum(tz, levels - 1)


def strata_of_int(eh: int, levels: int) -> int:
    x = mix_int(eh ^ SEED_STRATA)
    tz = 64 if x == 0 else (x & -x).bit_length() - 1
    return min(tz, levels - 1)


@dataclass
class DecodeResult:
    only_a: list[Element]  # in the minuend only (count +1)
    only_b: list[Element]  # in the subtrahend only (count -1)
    failed: np.ndarray  # partition ids that did not fully peel


class SketchTable:
    """P independent IBLTs of m cells each, stored flat. Partition by key hash or by stratum."""

    def __init__(self, partitions: int, cells: int, q: int = 3, mode: str = "key"):
        if cells % q:
            raise ValueError("cells must be a multiple of q")
        self.P, self.m, self.q, self.mode = partitions, cells, q, mode
        self.sub = cells // q
        n = partitions * cells
        self.count = np.zeros(n, dtype=np.int64)
        self.khi = np.zeros(n, dtype=np.uint64)
        self.klo = np.zeros(n, dtype=np.uint64)
        self.dig = np.zeros(n, dtype=np.uint64)
        self.chk = np.zeros(n, dtype=np.uint64)

    @property
    def nbytes(self) -> int:
        return self.P * self.m * CELL_BYTES

    def _partition(self, khi, klo, eh) -> np.ndarray:
        if self.mode == "strata":
            return strata_of(eh, self.P).astype(np.int64)
        return (key_hash(khi, klo) % np.uint64(self.P)).astype(np.int64)

    def _partition_int(self, khi: int, klo: int, eh: int) -> int:
        if self.mode == "strata":
            return strata_of_int(eh, self.P)
        return key_hash_int(khi, klo) % self.P

    def _cell_int(self, part: int, eh: int, j: int) -> int:
        return part * self.m + j * self.sub + mix_int(eh ^ cell_seed(j)) % self.sub

    def update(self, khi, klo, dig, sign: int = 1) -> None:
        khi, klo, dig = _u64(khi), _u64(klo), _u64(dig)
        if khi.size == 0:
            return
        eh = element_hash(khi, klo, dig)
        chk = check_hash(eh)
        base = self._partition(khi, klo, eh) * self.m
        for j in range(self.q):
            idx = base + j * self.sub + (mix(eh ^ np.uint64(cell_seed(j))) % np.uint64(self.sub)).astype(np.int64)
            np.add.at(self.count, idx, sign)
            np.bitwise_xor.at(self.khi, idx, khi)
            np.bitwise_xor.at(self.klo, idx, klo)
            np.bitwise_xor.at(self.dig, idx, dig)
            np.bitwise_xor.at(self.chk, idx, chk)

    def subtract(self, other: SketchTable) -> SketchTable:
        if (self.P, self.m, self.q, self.mode) != (other.P, other.m, other.q, other.mode):
            raise ValueError("sketch shapes differ")
        out = SketchTable(self.P, self.m, self.q, self.mode)
        out.count = self.count - other.count
        out.khi = self.khi ^ other.khi
        out.klo = self.klo ^ other.klo
        out.dig = self.dig ^ other.dig
        out.chk = self.chk ^ other.chk
        return out

    def decode(self) -> DecodeResult:
        """Peel pure cells until none remain. Cells in failed partitions stay non-empty."""
        count = self.count.tolist()
        khi, klo, dig, chk = self.khi.tolist(), self.klo.tolist(), self.dig.tolist(), self.chk.tolist()
        pure = np.flatnonzero((self.count == 1) | (self.count == -1))
        if pure.size:
            ok = check_hash(element_hash(self.khi[pure], self.klo[pure], self.dig[pure])) == self.chk[pure]
            pure = pure[ok]
        queue = pure.tolist()
        only_a: list[Element] = []
        only_b: list[Element] = []
        while queue:
            i = queue.pop()
            s = count[i]
            if s != 1 and s != -1:
                continue
            kh, kl, dg = khi[i], klo[i], dig[i]
            eh = element_hash_int(kh, kl, dg)
            if check_hash_int(eh) != chk[i]:
                continue
            (only_a if s == 1 else only_b).append((kh, kl, dg))
            ck = check_hash_int(eh)
            part = self._partition_int(kh, kl, eh)
            for j in range(self.q):
                c = self._cell_int(part, eh, j)
                count[c] -= s
                khi[c] ^= kh
                klo[c] ^= kl
                dig[c] ^= dg
                chk[c] ^= ck
                if count[c] == 1 or count[c] == -1:
                    queue.append(c)
        left = (np.asarray(count) != 0) | (np.asarray(khi, dtype=np.uint64) != 0) | (np.asarray(chk, dtype=np.uint64) != 0)
        failed = np.unique(np.flatnonzero(left) // self.m)
        return DecodeResult(only_a, only_b, failed)


class Strata:
    """Strata estimator (Eppstein et al. 2011): one small IBLT per trailing-zero stratum."""

    def __init__(self, levels: int = 32, cells: int = 81, q: int = 3):
        self.table = SketchTable(levels, cells, q, mode="strata")

    @property
    def nbytes(self) -> int:
        return self.table.nbytes

    def update(self, khi, klo, dig, sign: int = 1) -> None:
        self.table.update(khi, klo, dig, sign)

    def estimate(self, other: Strata) -> tuple[int, DecodeResult | None]:
        """Return (estimated difference size, exact result if every stratum decoded)."""
        res = self.table.subtract(other.table).decode()
        if res.failed.size == 0:
            return len(res.only_a) + len(res.only_b), res
        top = int(res.failed.max())
        n = sum(1 for e in res.only_a + res.only_b if strata_of_int(element_hash_int(*e), self.table.P) > top)
        return (2 ** (top + 1)) * max(n, 1), None


@dataclass(frozen=True)
class Level:
    capacity: int  # differences this level is sized to decode
    partitions: int
    cells: int


@dataclass(frozen=True)
class LadderConfig:
    """Sketch sizes kept up to date at once; verification picks the smallest that fits."""

    max_capacity: int
    base_capacity: int = 64
    growth: int = 8
    per_partition: int = 256
    cell_factor: float = 2.0
    q: int = 3

    def levels(self) -> list[Level]:
        out, cap = [], self.base_capacity
        while True:
            p = max(1, cap // self.per_partition)
            m = math.ceil(self.cell_factor * cap / p / self.q) * self.q
            out.append(Level(cap, p, m))
            if cap >= self.max_capacity:
                return out
            cap *= self.growth


@dataclass
class Sketch:
    """Everything one store keeps: the strata estimator plus every ladder level."""

    config: LadderConfig
    strata: Strata = field(default_factory=Strata)
    levels: list[SketchTable] = field(init=False)

    def __post_init__(self):
        self.levels = [SketchTable(l.partitions, l.cells, self.config.q) for l in self.config.levels()]

    def update(self, khi, klo, dig, sign: int = 1) -> None:
        self.strata.update(khi, klo, dig, sign)
        for t in self.levels:
            t.update(khi, klo, dig, sign)

    def apply_change(self, khi, klo, old_dig, new_dig) -> None:
        """An in-place value change: remove the old element, add the new one."""
        self.update(khi, klo, old_dig, -1)
        self.update(khi, klo, new_dig, +1)

    @property
    def memory_bytes(self) -> int:
        return self.strata.nbytes + sum(t.nbytes for t in self.levels)
