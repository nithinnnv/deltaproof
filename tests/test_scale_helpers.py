import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bench"))
import tpch_scale  # noqa: E402

from deltaproof.store import Table, exact_diff  # noqa: E402


def table(rng, n):
    u = lambda: rng.integers(0, 2**63, n, dtype=np.uint64)
    return Table(u(), u(), u(), np.broadcast_to(np.int64(0), (n,)))


def test_lowmem_exact_diff_matches_exact_diff(tmp_path):
    rng = np.random.default_rng(5)
    s = table(rng, 50_000)
    keep = rng.random(len(s)) > 0.001
    t = s.take(keep)  # some rows missing in t (works on the broadcast amount column)
    extra = table(rng, 40)
    t = t.concat(extra)  # some rows only in t
    t.dig[:25] ^= np.uint64(1)  # some values changed
    for a, b in ((s, t), (s, s), (s, table(rng, 0)), (table(rng, 0), t)):
        x1, y1 = exact_diff(a, b)
        x2, y2 = tpch_scale.exact_diff_lowmem(a, b)
        assert sorted(x1) == sorted(x2) and sorted(y1) == sorted(y2)
    tpch_scale.save_table(tmp_path / "s", s)
    r = tpch_scale.load_table(tmp_path / "s")
    assert all(np.array_equal(getattr(r, f), getattr(s, f)) for f in ("khi", "klo", "dig"))
