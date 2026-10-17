"""Synthetic double-entry ledger (paper Section VI-A).

Columns are held in canonical form (scaled integers, UTC microseconds, text hashes), so the
digest can be computed vectorized. `row()` materializes a logical row for loading into a real
database; its row-level digest equals the vectorized one.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import numpy as np

from . import canon
from .store import Table

CURRENCIES = ("USD", "EUR", "INR", "GBP", "JPY")
MEMOS = tuple(f"{kind} {k}" for kind in ("invoice", "refund", "transfer", "fee") for k in range(250))
_CUR_WORDS = np.array([canon.text_word_value(c) for c in CURRENCIES], dtype=np.uint64)
_MEMO_WORDS = np.array([canon.text_word_value(m) for m in MEMOS], dtype=np.uint64)
_START_US = 1_767_225_600_000_000  # 2026-01-01T00:00:00Z
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

SCHEMA = canon.Schema(
    "entry_id",
    (
        canon.Column("account_id", canon.Int()),
        canon.Column("txn_id", canon.Int()),
        canon.Column("amount", canon.Dec(2)),
        canon.Column("currency", canon.Text()),
        canon.Column("posted_at", canon.Timestamp()),
        canon.Column("memo", canon.Text()),
    ),
)


@dataclass
class Ledger:
    entry_id: np.ndarray  # uint64 primary key
    account_id: np.ndarray  # int64
    txn_id: np.ndarray  # int64
    amount: np.ndarray  # int64, cents
    currency: np.ndarray  # int64 index into CURRENCIES
    posted_at: np.ndarray  # int64, UTC microseconds
    memo: np.ndarray  # int64 index into MEMOS

    def __len__(self) -> int:
        return len(self.entry_id)

    def take(self, idx) -> Ledger:
        return Ledger(**{f.name: getattr(self, f.name)[idx] for f in fields(self)})

    def copy(self) -> Ledger:
        return Ledger(**{f.name: getattr(self, f.name).copy() for f in fields(self)})

    def concat(self, other: Ledger) -> Ledger:
        return Ledger(**{f.name: np.concatenate([getattr(self, f.name), getattr(other, f.name)]) for f in fields(self)})

    def digests(self) -> np.ndarray:
        return canon.fold_words([
            canon.word(canon.TAG_INT, self.account_id.view(np.uint64)),
            canon.word(canon.TAG_INT, self.txn_id.view(np.uint64)),
            canon.word(canon.TAG_DECIMAL, self.amount.view(np.uint64)),
            canon.word(canon.TAG_TEXT, _CUR_WORDS[self.currency]),
            canon.word(canon.TAG_TIMESTAMP, self.posted_at.view(np.uint64)),
            canon.word(canon.TAG_TEXT, _MEMO_WORDS[self.memo]),
        ])

    def table(self) -> Table:
        return Table(np.zeros(len(self), np.uint64), self.entry_id.astype(np.uint64), self.digests(), self.amount.copy())

    def row(self, i: int) -> dict:
        return {
            "entry_id": int(self.entry_id[i]),
            "account_id": int(self.account_id[i]),
            "txn_id": int(self.txn_id[i]),
            "amount": Decimal(int(self.amount[i])).scaleb(-2),
            "currency": CURRENCIES[int(self.currency[i])],
            "posted_at": _EPOCH + timedelta(microseconds=int(self.posted_at[i])),
            "memo": MEMOS[int(self.memo[i])],
        }


def generate(n: int, rng: np.random.Generator, accounts: int = 100_000, theta: float = 0.99, start_id: int = 1) -> Ledger:
    """n entries in debit/credit pairs; accounts drawn from a Zipf(theta) distribution."""
    w = 1.0 / np.arange(1, accounts + 1) ** theta
    account = rng.choice(accounts, size=n, p=w / w.sum()).astype(np.int64)
    ids = np.arange(start_id, start_id + n, dtype=np.int64)
    txn = (ids + 1) // 2
    x = np.maximum(1, rng.lognormal(mean=8.0, sigma=1.5, size=(n + 1) // 2)).astype(np.int64)
    amount = np.repeat(x, 2)[:n] * np.where(ids % 2 == 1, 1, -1)
    currency = np.repeat(rng.integers(0, len(CURRENCIES), (n + 1) // 2), 2)[:n]
    posted = _START_US + ids * 50_000 + rng.integers(0, 50_000, n)
    memo = rng.integers(0, len(MEMOS), n)
    return Ledger(ids.astype(np.uint64), account, txn, amount, currency, posted, memo)
