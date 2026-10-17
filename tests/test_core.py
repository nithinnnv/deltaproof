import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import numpy as np
import pytest

from deltaproof import canon
from deltaproof.hashing import element_hash, element_hash_int, key_hash, key_hash_int, mix, mix_int
from deltaproof.sketch import LadderConfig, Sketch, SketchTable, Strata
from deltaproof.store import Table
from deltaproof.verify import table_rescan, verify


def rand_elements(rng, n, start=0):
    klo = np.arange(start, start + n, dtype=np.uint64)
    return np.zeros(n, np.uint64), klo, rng.integers(0, 2**63, n, dtype=np.uint64)


def test_vector_and_scalar_hashes_agree():
    rng = np.random.default_rng(1)
    x = rng.integers(0, 2**63, 1000, dtype=np.uint64)
    assert [mix_int(v) for v in x.tolist()] == mix(x).tolist()
    h, l, d = rand_elements(rng, 500)
    scalar = [element_hash_int(a, b, c) for a, b, c in zip(h.tolist(), l.tolist(), d.tolist())]
    assert scalar == element_hash(h, l, d).tolist()
    assert [key_hash_int(a, b) for a, b in zip(h.tolist(), l.tolist())] == key_hash(h, l).tolist()


def test_iblt_recovers_symmetric_difference():
    rng = np.random.default_rng(2)
    h, l, d = rand_elements(rng, 20_000)
    a, b = SketchTable(4, 300), SketchTable(4, 300)
    a.update(h, l, d)
    b.update(h[100:], l[100:], d[100:])  # b lacks 100
    eh, el, ed = rand_elements(rng, 50, start=10**6)
    b.update(eh, el, ed)  # b has 50 extra
    res = a.subtract(b).decode()
    assert res.failed.size == 0
    assert {e[1] for e in res.only_a} == set(range(100))
    assert {e[1] for e in res.only_b} == set(el.tolist())


def test_iblt_is_linear():
    rng = np.random.default_rng(3)
    h, l, d = rand_elements(rng, 5000)
    batch, inc = SketchTable(2, 99), SketchTable(2, 99)
    batch.update(h, l, d)
    for i in range(0, 5000, 7):
        inc.update(h[i:i + 7], l[i:i + 7], d[i:i + 7])
    inc.update(h[:10], l[:10], d[:10], +1)
    inc.update(h[:10], l[:10], d[:10], -1)
    for f in ("count", "khi", "klo", "dig", "chk"):
        assert np.array_equal(getattr(batch, f), getattr(inc, f))


def test_overloaded_partition_reports_failure():
    rng = np.random.default_rng(4)
    h, l, d = rand_elements(rng, 400)
    a = SketchTable(1, 99)
    a.update(h, l, d)
    assert a.subtract(SketchTable(1, 99)).decode().failed.tolist() == [0]


@pytest.mark.parametrize("d", [0, 10, 300, 5000, 40000])
def test_strata_estimate(d):
    rng = np.random.default_rng(5)
    h, l, dg = rand_elements(rng, 100_000)
    a, b = Strata(), Strata()
    a.update(h, l, dg)
    b.update(h[d:], l[d:], dg[d:])
    est, exact = a.estimate(b)
    if exact is not None:
        assert est == d
    else:
        assert d / 4 <= est <= d * 4


@pytest.mark.parametrize("d", [0, 30, 2_000, 30_000])
def test_verify_end_to_end(d):
    rng = np.random.default_rng(6)
    n = 200_000
    h, l, dg = rand_elements(rng, n)
    amt = np.zeros(n, np.int64)
    src = Table(h, l, dg, amt)
    tdig = dg.copy()
    tdig[:d] ^= np.uint64(1)  # d value mismatches
    tgt = Table(h, l, tdig, amt)
    cfg = LadderConfig(max_capacity=n // 4)
    ss, st = Sketch(cfg), Sketch(cfg)
    ss.update(src.khi, src.klo, src.dig)
    st.update(tgt.khi, tgt.klo, tgt.dig)
    rep = verify(ss, st, table_rescan(src, tgt))
    assert rep.value_mismatch == {(0, i) for i in range(d)}
    assert not rep.missing_in_target and not rep.extra_in_target


def test_canonical_encoding_matches_across_representations():
    s = canon.Schema("id", (
        canon.Column("amount", canon.Dec(2)),
        canon.Column("at", canon.Timestamp()),
        canon.Column("memo", canon.Text(), nullable=True),
    ))
    t0 = datetime(2026, 1, 2, 3, 4, 5, 678901, tzinfo=timezone.utc)
    pg = {"id": 7, "amount": Decimal("12.30"), "at": t0, "memo": "café"}
    cass = {"id": 7, "amount": "12.3", "at": t0.astimezone(timezone(timedelta(hours=-5))), "memo": "café"}
    assert s.element(pg) == s.element(cass)
    assert s.digest({**pg, "amount": 12.3}) == s.digest(pg)  # float via shortest repr
    assert s.digest({**pg, "amount": Decimal("12.31")}) != s.digest(pg)
    assert s.digest({**pg, "at": t0 + timedelta(hours=1)}) != s.digest(pg)
    assert s.digest({**pg, "memo": None}) != s.digest({**pg, "memo": ""})
    with pytest.raises(canon.CanonError):
        s.digest({**pg, "at": t0.replace(tzinfo=None)})
    with pytest.raises(canon.CanonError):
        s.digest({**pg, "amount": Decimal("12.305")})
    u = uuid.uuid4()
    assert canon.canon_key(u) == (u.int >> 64, u.int & canon.MASK)


def test_vector_digest_matches_row_digest():
    s = canon.Schema("id", (canon.Column("n", canon.Int()), canon.Column("memo", canon.Text())))
    vals = np.array([5, 2**40, 0], dtype=np.uint64)
    memos = ["a", "bé", ""]
    words = [
        canon.word(canon.TAG_INT, vals),
        canon.word(canon.TAG_TEXT, np.array([canon.text_word_value(m) for m in memos], dtype=np.uint64)),
    ]
    rows = [s.digest({"id": i, "n": int(v), "memo": m}) for i, (v, m) in enumerate(zip(vals.tolist(), memos))]
    assert canon.fold_words(words).tolist() == rows
