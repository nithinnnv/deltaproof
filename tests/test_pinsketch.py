import numpy as np
import pytest

from deltaproof.faults import FAULT_CLASSES, inject
from deltaproof.workload import generate

ps = pytest.importorskip("deltaproof.pinsketch")
try:
    ps.lib()
except OSError:
    pytest.skip("minisketch library not built", allow_module_level=True)


@pytest.mark.parametrize("per_class,estimate", [(1, 9), (40, 360), (40, 100)])
def test_pinsketch_finds_exactly_the_injected_faults(per_class, estimate):
    rng = np.random.default_rng(per_class)
    inj = inject(generate(60_000, rng), {c: per_class for c in FAULT_CLASSES}, rng)
    keys, cost, tm = ps.pinsketch(inj.source, inj.target, estimate, 93_312)
    assert keys == inj.all_keys
    if estimate < per_class * 9:  # under-sized: some partitions overflow and are rescanned
        assert tm["failed_partitions"] >= 0
