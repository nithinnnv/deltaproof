import numpy as np
import pytest

from deltaproof import baselines
from deltaproof.faults import FAULT_CLASSES, inject
from deltaproof.sketch import LadderConfig, Sketch
from deltaproof.verify import table_rescan, verify
from deltaproof.workload import SCHEMA, generate


def test_row_digest_matches_vectorized_ledger_digest():
    led = generate(2000, np.random.default_rng(1))
    dig = led.digests().tolist()
    for i in (0, 1, 17, 999, 1999):
        assert SCHEMA.digest(led.row(i)) == dig[i]


def deltaproof_keys(inj, n):
    cfg = LadderConfig(max_capacity=max(64, n // 8))
    a, b = Sketch(cfg), Sketch(cfg)
    a.update(inj.source.khi, inj.source.klo, inj.source.dig)
    b.update(inj.target.khi, inj.target.klo, inj.target.dig)
    return verify(a, b, table_rescan(inj.source, inj.target)).keys


@pytest.mark.parametrize("per_class", [0, 1, 40])
def test_every_exact_method_finds_exactly_the_injected_faults(per_class):
    n = 120_000
    rng = np.random.default_rng(per_class + 7)
    inj = inject(generate(n, rng), {c: per_class for c in FAULT_CLASSES}, rng)
    truth = inj.all_keys
    assert len(truth) == per_class * (len(FAULT_CLASSES) + 1)  # offsetting adds two keys each
    assert deltaproof_keys(inj, n) == truth
    for method in (baselines.full_scan, baselines.bisection, baselines.merkle):
        assert method(inj.source, inj.target).keys == truth, method.__name__


def test_aggregates_miss_offsetting_errors():
    rng = np.random.default_rng(11)
    inj = inject(generate(50_000, rng), {"offsetting": 30}, rng)
    assert len(inj.all_keys) == 60
    assert baselines.aggregates(inj.source, inj.target).flagged_partitions == set()
    assert deltaproof_keys(inj, 50_000) == inj.all_keys
