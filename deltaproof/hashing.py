"""64-bit mixing used everywhere in DeltaProof.

Every function exists in two bit-identical forms: a vectorized numpy form (uint64 arrays)
for bulk work and a scalar form (Python int) for the peeling loop and row-level encoding.
"""

import numpy as np

MASK = (1 << 64) - 1
_GOLDEN = 0x9E3779B97F4A7C15
_M1 = 0xBF58476D1CE4E5B9
_M2 = 0x94D049BB133111EB

SEED_ELEMENT = 0x243F6A8885A308D3
SEED_PARTITION = 0x13198A2E03707344
SEED_CHECK = 0xA4093822299F31D0
SEED_CELL = 0x082EFA98EC4E6C89
SEED_STRATA = 0x452821E638D01377
SEED_DIGEST = 0xBE5466CF34E90C6C


def mix(x):
    """SplitMix64 finalizer over a uint64 array."""
    x = np.asarray(x, dtype=np.uint64)
    with np.errstate(over="ignore"):
        z = x + np.uint64(_GOLDEN)
        z = (z ^ (z >> np.uint64(30))) * np.uint64(_M1)
        z = (z ^ (z >> np.uint64(27))) * np.uint64(_M2)
        return z ^ (z >> np.uint64(31))


def mix_int(x: int) -> int:
    z = (x + _GOLDEN) & MASK
    z = ((z ^ (z >> 30)) * _M1) & MASK
    z = ((z ^ (z >> 27)) * _M2) & MASK
    return z ^ (z >> 31)


def key_hash(khi, klo):
    """Hash of the primary key only; decides the partition, so value changes stay put."""
    return mix(mix(np.asarray(klo, dtype=np.uint64) ^ np.uint64(SEED_PARTITION)) ^ khi)


def key_hash_int(khi: int, klo: int) -> int:
    return mix_int(mix_int(klo ^ SEED_PARTITION) ^ khi)


def element_hash(khi, klo, dig):
    """Hash of the element (key, value digest)."""
    klo = np.asarray(klo, dtype=np.uint64)
    return mix(mix(mix(klo ^ np.uint64(SEED_ELEMENT)) ^ khi) ^ dig)


def element_hash_int(khi: int, klo: int, dig: int) -> int:
    return mix_int(mix_int(mix_int(klo ^ SEED_ELEMENT) ^ khi) ^ dig)


def check_hash(eh):
    return mix(np.asarray(eh, dtype=np.uint64) ^ np.uint64(SEED_CHECK))


def check_hash_int(eh: int) -> int:
    return mix_int(eh ^ SEED_CHECK)


def cell_seed(j: int) -> int:
    return (SEED_CELL + j * _GOLDEN) & MASK
