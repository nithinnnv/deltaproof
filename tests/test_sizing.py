import numpy as np

from deltaproof import sizing
from deltaproof.faults import FAULT_CLASSES, inject
from deltaproof.hashing import element_hash
from deltaproof.sketch import SketchTable
from deltaproof.workload import generate


def injected(n, d, seed=7):
    per = {c: d // len(FAULT_CLASSES) for c in FAULT_CLASSES}
    per["precision"] += d - sum(per.values())
    return inject(generate(n, np.random.default_rng(seed)), per, np.random.default_rng(seed + d))


def test_vector_and_scalar_index_sequences_agree():
    inj = injected(2_000, 0)
    t = inj.source
    n_sym = 5_000
    count = sizing._encode(t, n_sym)[0]
    want = np.zeros(n_sym, np.int64)
    for eh in element_hash(t.khi, t.klo, t.dig).tolist():
        idx, _, _ = sizing._indices_from(eh, 0, n_sym)
        assert len(idx) == len(set(idx)) and idx[0] == 0
        want[idx] += 1
    assert np.array_equal(count, want)


def test_count_estimate_is_close_to_the_true_difference():
    rng = np.random.default_rng(3)
    est = []
    for _ in range(200):
        a = SketchTable(1, 1536)
        x = rng.integers(0, 2**63, size=(600, 3), dtype=np.uint64)
        a.update(x[:, 0], x[:, 1], x[:, 2], sign=1)
        a.update(x[:400, 0], x[:400, 1], x[:400, 2] ^ np.uint64(1), sign=-1)  # 600 + 400 elements differ
        est.append(sizing.count_estimate(a))
    assert abs(np.mean(est) / 1000 - 1) < 0.03  # unbiased


def test_wu_and_riblt_recover_exactly_the_injected_keys():
    for d in (0, 10, 1_000, 5_000):
        inj = injected(50_000, d)
        for fn in (sizing.wu_two_round, sizing.riblt):
            r = fn(inj.source, inj.target)
            assert r.keys == inj.all_keys, (fn.__name__, d)
