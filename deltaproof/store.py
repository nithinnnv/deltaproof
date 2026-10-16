"""Columnar in-memory table of elements, used by the benchmark and the baselines."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .hashing import element_hash, key_hash


@dataclass
class Table:
    khi: np.ndarray
    klo: np.ndarray
    dig: np.ndarray
    amount: np.ndarray  # signed scaled amount, used only by the aggregate baseline

    def __len__(self) -> int:
        return len(self.klo)

    @classmethod
    def empty(cls) -> Table:
        z = np.zeros(0, dtype=np.uint64)
        return cls(z, z.copy(), z.copy(), np.zeros(0, dtype=np.int64))

    def take(self, idx) -> Table:
        return Table(self.khi[idx], self.klo[idx], self.dig[idx], self.amount[idx])

    def concat(self, other: Table) -> Table:
        return Table(
            np.concatenate([self.khi, other.khi]),
            np.concatenate([self.klo, other.klo]),
            np.concatenate([self.dig, other.dig]),
            np.concatenate([self.amount, other.amount]),
        )

    def element_hashes(self) -> np.ndarray:
        return element_hash(self.khi, self.klo, self.dig)

    def partition_mask(self, partitions: int, p: int) -> np.ndarray:
        return key_hash(self.khi, self.klo) % np.uint64(partitions) == np.uint64(p)


def exact_diff(a: Table, b: Table) -> tuple[list, list]:
    """Elements only in a and only in b, by element hash (collisions are ~2^-64)."""
    ea, eb = a.element_hashes(), b.element_hashes()
    ia = np.flatnonzero(~np.isin(ea, eb))
    ib = np.flatnonzero(~np.isin(eb, ea))
    to_list = lambda t, idx: list(zip(t.khi[idx].tolist(), t.klo[idx].tolist(), t.dig[idx].tolist()))
    return to_list(a, ia), to_list(b, ib)
