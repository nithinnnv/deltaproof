"""Store adapters: scan a real store and turn its rows into DeltaProof elements.

An adapter's only job is to convert a driver's raw values into the logical Python values the
schema expects (Decimal, aware datetime, str, int). The schema does the rest, so two adapters
over different stores produce identical elements for the same logical record.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, Iterator

import numpy as np

from .canon import Schema
from .store import Table


def to_table(schema: Schema, rows: Iterable[dict], amount_column: str | None = None) -> Table:
    khi, klo, dig, amt = [], [], [], []
    for r in rows:
        h, l, d = schema.element(r)
        khi.append(h)
        klo.append(l)
        dig.append(d)
        if amount_column:
            amt.append(int(r[amount_column].scaleb(2)))
    u = lambda x: np.array(x, dtype=np.uint64)
    return Table(u(khi), u(klo), u(dig), np.array(amt or [0] * len(klo), dtype=np.int64))


def postgres_rows(conn, table: str, columns: list[str], batch: int = 10_000) -> Iterator[dict]:
    """Server-side cursor scan. psycopg returns Decimal and aware datetimes already."""
    with conn.transaction(), conn.cursor(name="deltaproof_scan") as cur:
        cur.itersize = batch
        cur.execute(f"SELECT {', '.join(columns)} FROM {table}")
        for row in cur:
            yield dict(zip(columns, row))


def cassandra_rows(session, table: str, columns: list[str], fetch_size: int = 5_000) -> Iterator[dict]:
    """Paged full scan (token order). The driver returns naive datetimes that are UTC."""
    from cassandra.query import SimpleStatement

    stmt = SimpleStatement(f"SELECT {', '.join(columns)} FROM {table}", fetch_size=fetch_size)
    for row in session.execute(stmt):
        d = dict(zip(columns, row))
        for k, v in d.items():
            if isinstance(v, datetime) and v.tzinfo is None:
                d[k] = v.replace(tzinfo=timezone.utc)
        yield d
