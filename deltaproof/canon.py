"""Canonical record encoding (paper Section III-A).

A record's value digest must be identical in every store, whatever its physical encoding.
Each column has a logical type; adapters convert a store's raw values to the logical Python
value, and the column type turns that into one canonical 64-bit word. The digest folds the
words in declared column order.

The row-level path (`Schema.digest`) is the reference. The vectorized path (`fold_words`
over `word` arrays) is bit-identical and used for bulk work.
"""

from __future__ import annotations

import hashlib
import struct
import unicodedata
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import ROUND_HALF_EVEN, Decimal

import numpy as np

from .hashing import MASK, SEED_DIGEST, mix, mix_int

TAG_INT, TAG_DECIMAL, TAG_TIMESTAMP, TAG_TEXT, TAG_BOOL, TAG_FLOAT, TAG_DATE = 1, 2, 3, 4, 5, 6, 7
KEY_PART_BITS = 30
_NULL_SALT = 0x6A09E667F3BCC909
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_EPOCH_DATE = date(1970, 1, 1)


class CanonError(ValueError):
    pass


@dataclass(frozen=True)
class Int:
    tag = TAG_INT

    def canon(self, v) -> int:
        if isinstance(v, bool) or not isinstance(v, int):
            raise CanonError(f"expected int, got {type(v).__name__}")
        return v & MASK


@dataclass(frozen=True)
class Dec:
    """Exact decimal with a declared scale, stored as the scaled integer."""

    scale: int
    strict: bool = True
    tag = TAG_DECIMAL

    def canon(self, v) -> int:
        if isinstance(v, float):
            v = Decimal(repr(v))  # shortest round-trip form, never the binary expansion
        elif not isinstance(v, Decimal):
            v = Decimal(v)
        q = v.quantize(Decimal(1).scaleb(-self.scale), rounding=ROUND_HALF_EVEN)
        if self.strict and q != v:
            raise CanonError(f"{v} does not fit scale {self.scale}")
        return int(q.scaleb(self.scale)) & MASK


@dataclass(frozen=True)
class Timestamp:
    """UTC microseconds since the epoch, truncated to a declared unit. Naive datetimes are rejected.

    Declare the coarsest precision any store keeps: e.g. unit="ms" when the target is
    Cassandra, whose timestamp type stores milliseconds.
    """

    unit: str = "us"
    tag = TAG_TIMESTAMP

    def canon(self, v) -> int:
        if not isinstance(v, datetime):
            raise CanonError(f"expected datetime, got {type(v).__name__}")
        if v.tzinfo is None:
            raise CanonError("naive datetime: time zone unknown")
        delta = v - _EPOCH
        micros = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
        step = {"us": 1, "ms": 1_000, "s": 1_000_000}[self.unit]
        return (micros - micros % step) & MASK


@dataclass(frozen=True)
class Date:
    """Calendar date as days since 1970-01-01."""

    tag = TAG_DATE

    def canon(self, v) -> int:
        if isinstance(v, datetime) or not isinstance(v, date):
            raise CanonError(f"expected date, got {type(v).__name__}")
        return (v - _EPOCH_DATE).days & MASK


@dataclass(frozen=True)
class Text:
    tag = TAG_TEXT

    def canon(self, v) -> int:
        if not isinstance(v, str):
            raise CanonError(f"expected str, got {type(v).__name__}")
        return text_word_value(v)


@dataclass(frozen=True)
class Char(Text):
    """SQL CHAR(n): trailing spaces are padding, not data, so they are dropped."""

    def canon(self, v) -> int:
        if not isinstance(v, str):
            raise CanonError(f"expected str, got {type(v).__name__}")
        return text_word_value(v.rstrip(" "))


@dataclass(frozen=True)
class Float:
    """IEEE-754 double, by its bits. -0.0 folds to 0.0 and every NaN to one NaN."""

    tag = TAG_FLOAT

    def canon(self, v) -> int:
        if isinstance(v, bool) or not isinstance(v, (int, float, Decimal)):
            raise CanonError(f"expected a number, got {type(v).__name__}")
        f = float(v)
        if f != f:
            f = float("nan")
        elif f == 0.0:
            f = 0.0
        return struct.unpack("<Q", struct.pack("<d", f))[0]


@dataclass(frozen=True)
class Bool:
    tag = TAG_BOOL

    def canon(self, v) -> int:
        if not isinstance(v, bool):
            raise CanonError(f"expected bool, got {type(v).__name__}")
        return int(v)


def text_word_value(s: str) -> int:
    data = unicodedata.normalize("NFC", s).encode("utf-8")
    return int.from_bytes(hashlib.blake2b(data, digest_size=8).digest(), "little")


def word_int(tag: int, value: int | None) -> int:
    if value is None:
        return mix_int(_NULL_SALT ^ mix_int(tag))
    return mix_int(value ^ mix_int(tag))


def word(tag: int, values: np.ndarray, null_mask: np.ndarray | None = None) -> np.ndarray:
    w = mix(np.asarray(values, dtype=np.uint64) ^ np.uint64(mix_int(tag)))
    if null_mask is not None:
        w = np.where(null_mask, np.uint64(mix_int(_NULL_SALT ^ mix_int(tag))), w)
    return w


def fold_words(words: list[np.ndarray]) -> np.ndarray:
    h = np.full(len(words[0]), SEED_DIGEST, dtype=np.uint64)
    for w in words:
        h = mix(h ^ w)
    return h


def fold_words_int(words: list[int]) -> int:
    h = SEED_DIGEST
    for w in words:
        h = mix_int(h ^ w)
    return h


def canon_key(k) -> tuple[int, int]:
    """Primary key -> (hi, lo) 64-bit words. Integers and UUIDs are supported."""
    if isinstance(k, uuid.UUID):
        return k.int >> 64, k.int & MASK
    if isinstance(k, int) and not isinstance(k, bool):
        return 0, k & MASK
    raise CanonError(f"unsupported key type {type(k).__name__}")


def pack_key(tag: int, parts: tuple[int, ...]) -> tuple[int, int]:
    """Composite key of up to 4 non-negative ints below 2^30, plus an 8-bit table tag, in 128 bits.

    The tag lets one sketch cover many tables; unpack_key recovers the parts for point reads.
    """
    if not 0 <= tag < 256 or not 1 <= len(parts) <= 4:
        raise CanonError("tag must fit 8 bits and a key must have 1-4 parts")
    v = tag
    for p in parts:
        if not 0 <= p < 1 << KEY_PART_BITS:
            raise CanonError(f"key part {p} out of range")
        v = (v << KEY_PART_BITS) | p
    v <<= KEY_PART_BITS * (4 - len(parts))
    return v >> 64, v & MASK


def unpack_key(khi: int, klo: int, arity: int) -> tuple[int, tuple[int, ...]]:
    v = (khi << 64) | klo
    m = (1 << KEY_PART_BITS) - 1
    parts = [(v >> (KEY_PART_BITS * (3 - i))) & m for i in range(4)]
    return v >> (4 * KEY_PART_BITS), tuple(parts[:arity])


@dataclass(frozen=True)
class Column:
    name: str
    type: Int | Dec | Timestamp | Text | Bool
    nullable: bool = False


@dataclass(frozen=True)
class Schema:
    key: str
    columns: tuple[Column, ...]

    def digest(self, row: dict) -> int:
        words = []
        for col in self.columns:
            v = row.get(col.name)
            if v is None:
                if not col.nullable:
                    raise CanonError(f"null in non-nullable column {col.name}")
                words.append(word_int(col.type.tag, None))
            else:
                words.append(word_int(col.type.tag, col.type.canon(v)))
        return fold_words_int(words)

    def element(self, row: dict) -> tuple[int, int, int]:
        khi, klo = canon_key(row[self.key])
        return khi, klo, self.digest(row)
