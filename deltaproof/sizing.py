"""Sizing policies compared with the ladder, on the same elements and the same 36-byte cells.

wu_two_round: Wu et al. (arXiv:2608.26537), as deployed: a first IBLT of M1 = 512 cells; if it
does not decode, the difference size is estimated from the subtracted sketch's count array,
d_hat = (sum C_i^2 - (sum C_i)^2 / M) / (k (1 - k / M)), which is unbiased; the second round
uses M2 = ceil(alpha * d_hat) cells (alpha = 1.8); if that fails too, both sides exchange all
elements. With k cells per element spread over k subtables the estimator keeps its expectation,
E[T] = d k (1 - k/M), because two elements share k^2/M cells on average either way.

riblt: rateless IBLT (Yang, Gilad, Alizadeh, SIGCOMM 2024). Each element maps to coded symbol 0
and then to a pseudo-random, increasingly sparse sequence of later symbols; the sender streams
symbols and the receiver peels as they arrive, stopping when symbol 0 is empty, which happens
exactly when every element of the difference is recovered.
"""

from __future__ import annotations

import heapq
import math
import time
from dataclasses import dataclass, field

import numpy as np

from .hashing import MASK, check_hash, check_hash_int, element_hash, element_hash_int, mix_int
from .sketch import CELL_BYTES, ELEMENT_BYTES, SketchTable
from .store import Table, exact_diff
from .verify import classify


@dataclass
class SizingResult:
    method: str
    keys: set
    bytes: int
    round_trips: int
    cells: int  # cells or coded symbols sent per side
    build_s: float
    decode_s: float
    fallback: bool = False
    detail: dict = field(default_factory=dict)


def _table(cells: int, t: Table, q: int = 3) -> SketchTable:
    m = max(q, math.ceil(cells / q) * q)
    st = SketchTable(1, m, q)
    st.update(t.khi, t.klo, t.dig)
    return st


def count_estimate(diff: SketchTable) -> float:
    """Wu et al.'s unbiased difference-size estimate from a subtracted sketch's count array."""
    c = diff.count.astype(np.float64)
    m, k = len(c), diff.q
    t = float(np.dot(c, c) - c.sum() ** 2 / m)
    return t / (k * (1 - k / m))


def wu_two_round(s: Table, t: Table, m1: int = 512, alpha: float = 1.8) -> SizingResult:
    build = decode = 0.0
    t0 = time.perf_counter()
    a, b = _table(m1, s), _table(m1, t)
    build += time.perf_counter() - t0
    t0 = time.perf_counter()
    diff = a.subtract(b)
    res = diff.decode()
    decode += time.perf_counter() - t0
    sent, rounds, detail = a.P * a.m, 1, {"m1": a.m}
    if res.failed.size == 0:
        keys = set().union(*classify(res.only_a, res.only_b))
        return SizingResult("wu_two_round", keys, 2 * sent * CELL_BYTES, rounds, sent, build, decode, detail=detail)
    d_hat = count_estimate(diff)
    m2 = max(3, math.ceil(alpha * max(d_hat, 1.0)))
    detail.update({"d_hat": round(d_hat, 1), "m2": m2})
    t0 = time.perf_counter()
    a2, b2 = _table(m2, s), _table(m2, t)
    build += time.perf_counter() - t0
    t0 = time.perf_counter()
    res2 = a2.subtract(b2).decode()
    decode += time.perf_counter() - t0
    sent += a2.m
    rounds = 2
    if res2.failed.size == 0:
        keys = set().union(*classify(res2.only_a, res2.only_b))
        return SizingResult("wu_two_round", keys, 2 * sent * CELL_BYTES, rounds, sent, build, decode, detail=detail)
    # FALLBACK: exchange every element
    x, y = exact_diff(s, t)
    keys = set().union(*classify(x, y))
    nbytes = 2 * sent * CELL_BYTES + (len(s) + len(t)) * ELEMENT_BYTES
    return SizingResult("wu_two_round", keys, nbytes, 3, sent, build, decode, fallback=True, detail=detail)


# ---- rateless IBLT -------------------------------------------------------------------------

_MUL = 0xDA942042E4DD58B5
_TWO32 = float(1 << 32)


def _next_index(last: int, prng: int) -> tuple[int, int]:
    prng = (prng * _MUL) & MASK
    # Same float arithmetic as _encode (float(prng) + 1.0, not prng + 1), so both sides agree on indices.
    last += max(1, math.ceil((float(last) + 1.5) * (_TWO32 / math.sqrt(float(prng) + 1.0) - 1)))  # never repeat
    return last, prng


def _encode(t: Table, n_symbols: int) -> tuple[np.ndarray, ...]:
    """First n_symbols coded symbols of a set, vectorized over elements."""
    arrays = (np.zeros(n_symbols, np.int64), *(np.zeros(n_symbols, np.uint64) for _ in range(4)))
    _accumulate(t, n_symbols, *arrays)
    return arrays


def _accumulate(t: Table, n_symbols: int, count, khi, klo, dig, chk, sign: int = 1) -> None:
    """Add (sign=+1) or remove (sign=-1) a set's elements in the first n_symbols symbols, in place."""
    eh = element_hash(t.khi, t.klo, t.dig)
    ch = check_hash(eh)
    last = np.zeros(len(t), np.float64)  # exact for indices < 2^53
    prng = eh.copy()
    live = np.ones(len(t), bool)
    while live.any():
        idx = np.flatnonzero(live)
        pos = last[idx].astype(np.int64)
        np.add.at(count, pos, sign)
        np.bitwise_xor.at(khi, pos, t.khi[idx])
        np.bitwise_xor.at(klo, pos, t.klo[idx])
        np.bitwise_xor.at(dig, pos, t.dig[idx])
        np.bitwise_xor.at(chk, pos, ch[idx])
        with np.errstate(over="ignore"):
            prng[idx] = prng[idx] * np.uint64(_MUL)
        step = np.ceil((last[idx] + 1.5) * (_TWO32 / np.sqrt(prng[idx].astype(np.float64) + 1) - 1))
        last[idx] += np.maximum(step, 1.0)
        live[idx] = last[idx] < n_symbols


class RatelessSketch:
    """The first n_symbols coded symbols of a set, kept current under inserts and deletes.

    Rateless symbols are linear like IBLT cells, so a change stream maintains them: an element
    touches about 2 ln(n_symbols) symbols. Comparing two such sketches streams the difference of
    their symbols until it decodes, so the bytes sent follow the difference, not n_symbols.
    """

    def __init__(self, n_symbols: int = 1 << 18):
        self.n = n_symbols
        self.count = np.zeros(n_symbols, np.int64)
        self.khi = np.zeros(n_symbols, np.uint64)
        self.klo = np.zeros(n_symbols, np.uint64)
        self.dig = np.zeros(n_symbols, np.uint64)
        self.chk = np.zeros(n_symbols, np.uint64)

    def update(self, khi, klo, dig, sign: int = 1) -> None:
        t = Table(np.asarray(khi, np.uint64), np.asarray(klo, np.uint64), np.asarray(dig, np.uint64),
                  np.zeros(len(np.atleast_1d(klo)), np.int64))
        if len(t) == 0:
            return
        _accumulate(t, self.n, *self.arrays, sign=sign)

    @property
    def arrays(self):
        return self.count, self.khi, self.klo, self.dig, self.chk

    def compare(self, other: RatelessSketch) -> SizingResult:
        """Stream this sketch's symbols against other's; exact difference, or RuntimeError if n is too small."""
        t0 = time.perf_counter()
        out = _stream_decode(self.arrays, other.arrays)
        if out is None:
            raise RuntimeError(f"difference larger than {self.n} rateless symbols can decode")
        used, only_a, only_b = out
        keys = set().union(*classify(only_a, only_b))
        return SizingResult("riblt", keys, 2 * used * CELL_BYTES, 1, used, 0.0, time.perf_counter() - t0,
                            detail={"symbols": used, "only_a": only_a, "only_b": only_b})


def _indices_from(eh: int, start: int, stop: int):
    """Symbol indices of an element in [start, stop), and the first index >= stop."""
    last, prng, out = 0, eh, []
    while last < stop:
        if last >= start:
            out.append(last)
        last, prng = _next_index(last, prng)
    return out, last, prng


def riblt(s: Table, t: Table, max_symbols: int | None = None) -> SizingResult:
    """Stream coded symbols until the receiver has decoded the whole difference."""
    n_sym = max_symbols or 64
    while True:
        t0 = time.perf_counter()
        sa, sb = _encode(s, n_sym), _encode(t, n_sym)
        build = time.perf_counter() - t0
        t0 = time.perf_counter()
        out = _stream_decode(sa, sb)
        decode = time.perf_counter() - t0
        if out is not None or max_symbols:
            break
        n_sym *= 2  # not enough symbols prepared to finish; prepare more (not a protocol round)
    if out is None:
        raise RuntimeError("riblt did not finish within max_symbols")
    used, only_a, only_b = out
    keys = set().union(*classify(only_a, only_b))
    return SizingResult("riblt", keys, 2 * used * CELL_BYTES, 1, used, build, decode,
                        detail={"symbols": used})


def _stream_decode(sa, sb):
    """Receive difference symbols one at a time; return (symbols used, only_a, only_b) or None."""
    count = (sa[0] - sb[0]).tolist()
    khi = (sa[1] ^ sb[1]).tolist()
    klo = (sa[2] ^ sb[2]).tolist()
    dig = (sa[3] ^ sb[3]).tolist()
    chk = (sa[4] ^ sb[4]).tolist()
    n = len(count)
    only_a, only_b = [], []
    pending: list = []  # heap of (next index, element id) for recovered elements
    rec: list = []  # recovered elements: (sign, khi, klo, dig, eh, ck, last, prng)

    def apply(j: int, r) -> None:
        sign, kh, kl, dg, _, ck = r[:6]
        count[j] -= sign
        khi[j] ^= kh
        klo[j] ^= kl
        dig[j] ^= dg
        chk[j] ^= ck

    def pure(j: int) -> bool:
        c = count[j]
        if c != 1 and c != -1:
            return False
        return check_hash_int(element_hash_int(khi[j], klo[j], dig[j])) == chk[j]

    for j in range(n):
        # bring symbol j up to date with every element already recovered
        while pending and pending[0][0] == j:
            _, eid = heapq.heappop(pending)
            r = rec[eid]
            apply(j, r)
            last, prng = _next_index(j, r[7])
            rec[eid] = r[:6] + (last, prng)
            heapq.heappush(pending, (last, eid))
        queue = [j]
        while queue:
            i = queue.pop()
            if not pure(i):
                continue
            sign, kh, kl, dg = count[i], khi[i], klo[i], dig[i]
            eh = element_hash_int(kh, kl, dg)
            r = (sign, kh, kl, dg, eh, check_hash_int(eh))
            (only_a if sign == 1 else only_b).append((kh, kl, dg))
            idxs, last, prng = _indices_from(eh, 0, j + 1)
            for x in idxs:
                apply(x, r)
                if x != i and (count[x] == 1 or count[x] == -1):
                    queue.append(x)
            rec.append(r + (last, prng))
            heapq.heappush(pending, (last, len(rec) - 1))
        if count[0] == 0 and khi[0] == 0 and klo[0] == 0 and dig[0] == 0 and chk[0] == 0:
            return j + 1, only_a, only_b
    return None
