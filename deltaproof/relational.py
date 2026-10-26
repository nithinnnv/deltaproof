"""Multi-table relational schemas: one sketch covers many tables via a per-table key tag.

A TableSpec names a table's primary key and its non-key columns with their canonical types.
`normalize` turns any store's raw row (driver values or wal2json values) into the logical
values the canonical types expect; `element` then gives the (khi, klo, digest) element.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone

from . import canon
from .canon import Char, Column, Date, Dec, Float, Int, Text, Timestamp

_CQL = {Int: "int", Dec: "decimal", Float: "double", Text: "text", Char: "text", Timestamp: "timestamp", Date: "date"}
_PG = {Int: "integer", Dec: "numeric", Float: "double precision", Text: "text", Char: "text", Timestamp: "timestamptz",
       Date: "date"}


@dataclass(frozen=True)
class TableSpec:
    name: str
    tag: int
    key: tuple[str, ...]
    columns: tuple[Column, ...]
    key_types: tuple = ()  # canonical type per key column; empty means all Int
    hash_key: bool = False  # force hashed keys, e.g. for integers wider than 30 bits

    @property
    def column_names(self) -> list[str]:
        return list(self.key) + [c.name for c in self.columns]

    @property
    def hashed(self) -> bool:
        """Packed keys hold up to four small integers; anything else is hashed into the same 128 bits."""
        return self.hash_key or len(self.key) > 4 or any(not isinstance(t, Int) for t in self.key_types)

    @property
    def key_only(self) -> TableSpec:
        """The same table with no value columns, for key-only scans."""
        return TableSpec(self.name, self.tag, self.key, (), self.key_types, self.hash_key)

    def normalize(self, raw: dict) -> dict:
        row = dict(raw)
        for c in self.columns:
            v = row.get(c.name)
            if isinstance(c.type, Timestamp) and v is not None:
                if isinstance(v, str):
                    v = datetime.fromisoformat(v)
                if v.tzinfo is None:  # stored without zone; the stores run in UTC
                    v = v.replace(tzinfo=timezone.utc)
                row[c.name] = v
            elif isinstance(c.type, Date) and v is not None:
                if isinstance(v, str):
                    v = date.fromisoformat(v)
                elif not isinstance(v, date) and hasattr(v, "date"):  # cassandra.util.Date
                    v = v.date()
                row[c.name] = v
        return row

    def key_of(self, row: dict) -> tuple[int, int]:
        if not self.hashed:
            return canon.pack_key(self.tag, tuple(int(row[k]) for k in self.key))
        types = self.key_types or (Int(),) * len(self.key)
        return canon.hash_key(self.tag, [(t, row[k]) for k, t in zip(self.key, types)])

    def words(self, row: dict) -> list[int]:
        """One canonical word per value column; two rows agree on a column iff its words match."""
        words = []
        for c in self.columns:
            v = row.get(c.name)
            if v is None:
                if not c.nullable:
                    raise canon.CanonError(f"null in non-nullable column {self.name}.{c.name}")
                words.append(canon.word_int(c.type.tag, None))
            else:
                words.append(canon.word_int(c.type.tag, c.type.canon(v)))
        return words

    def digest(self, row: dict) -> int:
        return canon.fold_words_int(self.words(row))

    def element(self, raw: dict) -> tuple[int, int, int]:
        row = self.normalize(raw)
        khi, klo = self.key_of(row)
        return khi, klo, self.digest(row)

    def cql_ddl(self, keyspace: str) -> str:
        types = self.key_types or (Int(),) * len(self.key)
        cols = [f"{k} {_CQL[type(t)]}" for k, t in zip(self.key, types)] + \
               [f"{c.name} {_CQL[type(c.type)]}" for c in self.columns]
        return f"CREATE TABLE {keyspace}.{self.name} ({', '.join(cols)}, PRIMARY KEY (({', '.join(self.key)})))"

    def pg_ddl(self, extra: str = "") -> str:
        """PostgreSQL table for this spec, as a migration target; `extra` adds columns such as a version."""
        pg = lambda t: "real" if isinstance(t, Float) and t.bits == 32 else _PG[type(t)]
        types = self.key_types or (Int(),) * len(self.key)
        cols = [f"{k} {pg(t)} NOT NULL" for k, t in zip(self.key, types)] + \
               [f"{c.name} {pg(c.type)}{'' if c.nullable else ' NOT NULL'}" for c in self.columns]
        if extra:
            cols.append(extra)
        return f"CREATE TABLE {self.name} ({', '.join(cols)}, PRIMARY KEY ({', '.join(self.key)}))"


def _cols(spec: str) -> tuple[Column, ...]:
    """Compact column list: 'name:type[?]' with types i, d2, d4, f, t, c, ts; '?' = nullable."""
    types = {"i": Int(), "d2": Dec(2), "d4": Dec(4), "f": Float(), "f4": Float(bits=32), "t": Text(), "c": Char(),
             "ts": Timestamp(unit="ms"), "dt": Date()}
    out = []
    for item in spec.split():
        name, ty = item.split(":")
        out.append(Column(name, types[ty.rstrip("?")], nullable=ty.endswith("?")))
    return tuple(out)


# BenchBase TPC-C (ddl-postgres.sql). `history` has no primary key, so it cannot be verified
# key-by-key and is left out. Timestamps are compared at milliseconds, Cassandra's precision.
TPCC = (
    TableSpec("warehouse", 1, ("w_id",), _cols(
        "w_ytd:d2 w_tax:d4 w_name:t w_street_1:t w_street_2:t w_city:t w_state:c w_zip:c")),
    TableSpec("district", 2, ("d_w_id", "d_id"), _cols(
        "d_ytd:d2 d_tax:d4 d_next_o_id:i d_name:t d_street_1:t d_street_2:t d_city:t d_state:c d_zip:c")),
    TableSpec("customer", 3, ("c_w_id", "c_d_id", "c_id"), _cols(
        "c_discount:d4 c_credit:c c_last:t c_first:t c_credit_lim:d2 c_balance:d2 c_ytd_payment:f "
        "c_payment_cnt:i c_delivery_cnt:i c_street_1:t c_street_2:t c_city:t c_state:c c_zip:c c_phone:c "
        "c_since:ts c_middle:c c_data:t")),
    TableSpec("item", 4, ("i_id",), _cols("i_name:t i_price:d2 i_data:t i_im_id:i")),
    TableSpec("stock", 5, ("s_w_id", "s_i_id"), _cols(
        "s_quantity:i s_ytd:d2 s_order_cnt:i s_remote_cnt:i s_data:t " +
        " ".join(f"s_dist_{i:02d}:c" for i in range(1, 11)))),
    TableSpec("oorder", 6, ("o_w_id", "o_d_id", "o_id"), _cols(
        "o_c_id:i o_carrier_id:i? o_ol_cnt:i o_all_local:i o_entry_d:ts")),
    TableSpec("new_order", 7, ("no_w_id", "no_d_id", "no_o_id"), ()),
    TableSpec("order_line", 8, ("ol_w_id", "ol_d_id", "ol_o_id", "ol_number"), _cols(
        "ol_i_id:i ol_delivery_d:ts? ol_amount:d2 ol_supply_w_id:i ol_quantity:d2 ol_dist_info:c")),
)
TPCC_BY_NAME = {t.name: t for t in TPCC}
TPCC_BY_TAG = {t.tag: t for t in TPCC}

# BenchBase TPC-C on MySQL (ddl-mysql.sql): the same tables, but c_ytd_payment is a single-precision FLOAT.
TPCC_MYSQL = tuple(
    TableSpec(t.name, t.tag, t.key, tuple(
        Column(c.name, Float(bits=32), c.nullable) if c.name == "c_ytd_payment" else c for c in t.columns))
    for t in TPCC)
TPCC_MYSQL_BY_NAME = {t.name: t for t in TPCC_MYSQL}
TPCC_MYSQL_BY_TAG = {t.tag: t for t in TPCC_MYSQL}
