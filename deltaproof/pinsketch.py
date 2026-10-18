"""PinSketch baseline: BCH-syndrome set reconciliation (Minsky et al.; Dodis et al.) via minisketch.

PinSketch needs the fewest bytes possible for a given capacity (8 bytes per element of capacity
in a 64-bit field), but adding an element costs O(capacity) and decoding O(capacity^2), so it is
partitioned the same way as DeltaProof. It gets the same strata estimate DeltaProof uses.

Build the library first: native/build_minisketch.sh <minisketch checkout>
"""

from __future__ import annotations

import ctypes
import math
import os
import time
from pathlib import Path

import numpy as np

from .hashing import key_hash
from .sketch import ELEMENT_BYTES
from .store import Table, exact_diff
from .verify import Cost, classify

BITS = 64
_DEFAULT = Path.home() / "workspace" / "deltaproof-ext" / "minisketch" / "libminisketch.dylib"
_lib = None


def lib():
    global _lib
    if _lib is None:
        L = ctypes.CDLL(os.environ.get("DELTAPROOF_MINISKETCH_LIB", str(_DEFAULT)))
        vp, u32, sz = ctypes.c_void_p, ctypes.c_uint32, ctypes.c_size_t
        L.minisketch_create.restype, L.minisketch_create.argtypes = vp, [u32, u32, sz]
        L.minisketch_destroy.argtypes = [vp]
        L.minisketch_merge.restype, L.minisketch_merge.argtypes = sz, [vp, vp]
        L.minisketch_decode.restype, L.minisketch_decode.argtypes = ctypes.c_ssize_t, [vp, sz, ctypes.POINTER(ctypes.c_uint64)]
        L.minisketch_serialized_size.restype, L.minisketch_serialized_size.argtypes = sz, [vp]
        L.minisketch_implementation_max.restype = u32
        L.ms_add_partitioned.argtypes = [ctypes.POINTER(vp), ctypes.POINTER(ctypes.c_uint64), ctypes.POINTER(ctypes.c_int64), sz]
        _lib = L
    return _lib


def _sketches(partitions: int, capacity: int):
    L = lib()
    arr = (ctypes.c_void_p * partitions)()
    for p in range(partitions):
        arr[p] = L.minisketch_create(BITS, 0, capacity)
        if not arr[p]:
            raise RuntimeError("minisketch_create failed")
    return arr


def _fingerprints(t: Table) -> np.ndarray:
    fp = t.element_hashes()
    fp[fp == 0] = 1  # minisketch elements must be non-zero; a 2^-64 event
    return np.ascontiguousarray(fp, dtype=np.uint64)


def _partitions(t: Table, P: int) -> np.ndarray:
    return np.ascontiguousarray((key_hash(t.khi, t.klo) % np.uint64(P)).astype(np.int64))


def layout(estimate: int, headroom: float = 1.25, per_partition: int = 256) -> tuple[int, int]:
    """(partitions, capacity per partition): mean load per_partition, plus 4 standard deviations."""
    total = max(16, math.ceil(headroom * estimate))
    P = max(1, math.ceil(total / per_partition))
    mean = total / P
    return P, math.ceil(mean + 4 * math.sqrt(mean)) + 8


def pinsketch(s: Table, t: Table, estimate: int, strata_bytes: int) -> tuple[set, Cost, dict]:
    L = lib()
    P, cap = layout(estimate)
    timings = {}
    sides = []
    for name, tab in (("source", s), ("target", t)):
        fp, part = _fingerprints(tab), _partitions(tab, P)
        sk = _sketches(P, cap)
        t0 = time.perf_counter()
        L.ms_add_partitioned(sk, fp.ctypes.data_as(ctypes.POINTER(ctypes.c_uint64)),
                             part.ctypes.data_as(ctypes.POINTER(ctypes.c_int64)), len(fp))
        timings[f"encode_{name}_s"] = time.perf_counter() - t0
        sides.append((sk, fp, part))
    (sa, fa, pa), (sb, fb, pb) = sides
    sketch_bytes = L.minisketch_serialized_size(sa[0])
    cost = Cost(bytes=2 * strata_bytes + 2 * P * sketch_bytes, round_trips=2)
    out = (ctypes.c_uint64 * cap)()
    found, failed = [], []
    t0 = time.perf_counter()
    for p in range(P):
        L.minisketch_merge(sa[p], sb[p])
        n = L.minisketch_decode(sa[p], cap, out)
        if n < 0:
            failed.append(p)
        else:
            found.extend(out[i] for i in range(n))
    timings["decode_s"] = time.perf_counter() - t0
    for arr in (sa, sb):
        for p in range(P):
            L.minisketch_destroy(arr[p])
    found = np.array(found, dtype=np.uint64)
    in_s = np.isin(fa, found)
    in_t = np.isin(fb, found)
    only_s = list(zip(s.khi[in_s].tolist(), s.klo[in_s].tolist(), s.dig[in_s].tolist()))
    only_t = list(zip(t.khi[in_t].tolist(), t.klo[in_t].tolist(), t.dig[in_t].tolist()))
    if failed:  # rescan the partitions that overflowed, as DeltaProof does
        cost.round_trips += 1
        bad = np.array(failed)
        ms, mt = np.isin(pa, bad), np.isin(pb, bad)
        only_s = [e for e, keep in zip(only_s, ~ms[in_s]) if keep]
        only_t = [e for e, keep in zip(only_t, ~mt[in_t]) if keep]
        a, b = exact_diff(s.take(ms), t.take(mt))
        cost.rows_scanned += int(ms.sum() + mt.sum())
        cost.bytes += int(ms.sum() + mt.sum()) * ELEMENT_BYTES
        only_s += a
        only_t += b
    missing, extra, mismatch = classify(only_s, only_t)
    timings.update({"partitions": P, "capacity": cap, "failed_partitions": len(failed)})
    return missing | extra | mismatch, cost, timings
