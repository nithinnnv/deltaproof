"""Verification while writes are in flight (paper Section IV), as a deterministic simulation.

A write stream updates rows of the source in version order. The target applies every write
up to its watermark w_T, plus a fraction of later writes early (out of order, last writer
wins by source version). Separately, some target rows carry true faults that writes never
touch. We compare four ways of verifying at the same instant.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .sketch import LadderConfig, Sketch
from .store import Table
from .verify import table_rescan, verify
from .workload import generate


@dataclass
class LiveResult:
    mode: str
    candidates: int
    false_candidates: int
    reported: int
    true_reported: int
    false_reported: int
    point_reads: int


def _state(base: np.ndarray, w_row: np.ndarray, w_dig: np.ndarray, applied: np.ndarray):
    """Digest and version of every row after applying the given writes, last writer wins."""
    dig, ver = base.copy(), np.zeros(len(base), np.int64)
    idx = np.flatnonzero(applied)
    if idx.size:
        rows = w_row[idx][::-1]
        uniq, first = np.unique(rows, return_index=True)
        last = idx[::-1][first]
        dig[uniq] = w_dig[last]
        ver[uniq] = last + 1
    return dig, ver


def simulate(n: int, writes: int, lag: int, early_fraction: float, faults: int,
             rng: np.random.Generator, config: LadderConfig | None = None) -> list[LiveResult]:
    base = generate(n, rng).table()
    zeros = np.zeros(n, np.uint64)
    fault_rows = rng.choice(n, faults, replace=False)
    fault_keys = {(0, int(base.klo[r])) for r in fault_rows}
    eligible = np.setdiff1d(np.arange(n), fault_rows)
    w_row = rng.choice(eligible, writes)
    w_dig = rng.integers(0, 2**63, writes, dtype=np.uint64)
    version = np.arange(writes)
    w_t = writes - lag

    s_now, s_ver = _state(base.dig, w_row, w_dig, version < writes)
    s_mark, _ = _state(base.dig, w_row, w_dig, version < w_t)
    t_base = base.dig.copy()
    t_base[fault_rows] ^= np.uint64(1)
    applied = (version < w_t) | ((version >= w_t) & (rng.random(writes) < early_fraction))
    t_now, t_ver = _state(t_base, w_row, w_dig, applied)
    t_later, _ = _state(t_base, w_row, w_dig, version < writes)  # after the grace period

    config = config or LadderConfig(max_capacity=max(64, n // 8))
    table = lambda d: Table(zeros, base.klo, d, base.amount)

    def sketch(d):
        s = Sketch(config)
        s.update(zeros, base.klo, d)
        return s

    sk_now, sk_mark, sk_t = sketch(s_now), sketch(s_mark), sketch(t_now)
    row_of = {int(k): i for i, k in enumerate(base.klo.tolist())}

    def confirm(keys) -> tuple[set, int]:
        divergent, reads = set(), 0
        for k in keys:
            r = row_of[k[1]]
            reads += 2
            if t_now[r] == s_now[r]:
                continue  # converged: it was in flight
            if t_ver[r] < s_ver[r]:  # target behind: re-check after the grace period
                reads += 2
                if t_later[r] == s_now[r]:
                    continue
            divergent.add(k)
        return divergent, reads

    out = []
    for name, src, src_table in (("naive", sk_now, s_now), ("watermark", sk_mark, s_mark)):
        cand = verify(src, sk_t, table_rescan(table(src_table), table(t_now))).keys
        false_c = len(cand - fault_keys)
        out.append(LiveResult(name, len(cand), false_c, len(cand), len(cand & fault_keys), false_c, 0))
        div, reads = confirm(cand)
        out.append(LiveResult(name + "+confirm", len(cand), false_c, len(div),
                              len(div & fault_keys), len(div - fault_keys), reads))
    return out
