"""Command-line tool.

    deltaproof sketch --spec tables.toml --store URL -o side.npz [--max-differences N]  # next to one store
    deltaproof diff   --spec tables.toml a.npz b.npz [--source URL --target URL]
    deltaproof verify --spec tables.toml --source URL --target URL  # both steps at once

Store URLs: postgresql://user:password@host:port/database and cassandra://host:port/keyspace.
The spec lists each table's primary key and its non-key columns with their canonical types:

    [[table]]
    name = "lineitem"
    key = ["l_orderkey", "l_linenumber"]
    columns = "l_partkey:i l_quantity:d2 l_shipdate:dt l_comment:t"

Types: i integer, d<scale> exact decimal, f float, t text, c fixed-width char, ts timestamp (ms),
dt date; a trailing '?' marks a nullable column.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import tomllib
from pathlib import Path
from urllib.parse import urlparse

import numpy as np

from . import canon
from .canon import Column, Dec
from .relational import TableSpec, _cols
from .sketch import LadderConfig, Sketch, SketchTable
from .store import Table
from .verify import Report, table_rescan, verify

FORMAT = 1


def load_spec(path: str) -> list[TableSpec]:
    doc = tomllib.loads(Path(path).read_text())
    specs = []
    for tag, t in enumerate(doc.get("table", []), start=1):
        cols = []
        for item in t.get("columns", "").split():
            name, ty = item.split(":")
            base = ty.rstrip("?")
            if base.startswith("d") and base[1:].isdigit():  # any decimal scale, not just d2/d4
                cols.append(Column(name, Dec(int(base[1:])), nullable=ty.endswith("?")))
            else:
                cols += _cols(item)
        specs.append(TableSpec(t["name"], tag, tuple(t["key"]), tuple(cols)))
    if not specs:
        raise SystemExit(f"{path}: no [[table]] entries")
    return specs


class Store:
    """One database, opened from a URL; yields raw rows for a table."""

    def __init__(self, url: str):
        u = urlparse(url)
        self.url, self.kind = url, u.scheme
        if u.scheme in ("postgres", "postgresql"):
            import psycopg

            self.conn = psycopg.connect(url, autocommit=True)
        elif u.scheme == "cassandra":
            from cassandra.cluster import Cluster

            self.cluster = Cluster([u.hostname or "127.0.0.1"], port=u.port or 9042)
            self.session = self.cluster.connect()
            self.keyspace = u.path.strip("/")
        else:
            raise SystemExit(f"unsupported store URL {url!r}: use postgresql:// or cassandra://")

    def rows(self, spec: TableSpec):
        from .adapters import cassandra_rows, postgres_rows

        if self.kind == "cassandra":
            return cassandra_rows(self.session, f"{self.keyspace}.{spec.name}", spec.column_names)
        return postgres_rows(self.conn, spec.name, spec.column_names)

    def scan(self, specs: list[TableSpec]) -> Table:
        kh, kl, dg = [], [], []
        for spec in specs:
            for r in self.rows(spec):
                h, l, d = spec.element(r)
                kh.append(h)
                kl.append(l)
                dg.append(d)
        u = lambda x: np.array(x, dtype=np.uint64)
        return Table(u(kh), u(kl), u(dg), np.zeros(len(kl), np.int64))

    def close(self) -> None:
        if self.kind == "cassandra":
            self.cluster.shutdown()
        else:
            self.conn.close()


def build(table: Table, max_differences: int | None = None) -> Sketch:
    cap = max(64, len(table) // 8)
    if max_differences is not None:  # a file for shipping: keep only the levels that many differences need
        cap = min(cap, max(64, int(1.25 * max_differences)))
    sk = Sketch(LadderConfig(max_capacity=cap))
    sk.update(table.khi, table.klo, table.dig)
    return sk


def save(sk: Sketch, rows: int, path: str) -> None:
    arrays = {"meta": np.array([FORMAT, sk.config.max_capacity, rows], np.int64)}
    for i, t in enumerate([sk.strata.table] + sk.levels):
        arrays[f"t{i}_count"] = t.count.astype(np.int32)  # per-cell counts stay far below 2^31
        for f in ("khi", "klo", "dig", "chk"):
            arrays[f"t{i}_{f}"] = getattr(t, f)
    np.savez_compressed(path, **arrays)


def load(path: str) -> tuple[Sketch, int]:
    z = np.load(path)
    fmt, max_capacity, rows = z["meta"].tolist()
    if fmt != FORMAT:
        raise SystemExit(f"{path}: sketch format {fmt}, expected {FORMAT}")
    sk = Sketch(LadderConfig(max_capacity=max_capacity))
    for i, t in enumerate([sk.strata.table] + sk.levels):
        t.count = z[f"t{i}_count"].astype(np.int64)
        for f in ("khi", "klo", "dig", "chk"):
            setattr(t, f, z[f"t{i}_{f}"])
    return sk, rows


def common(a: Sketch, b: Sketch) -> tuple[Sketch, Sketch]:
    """Each side sizes its ladder from its own row count; the smaller ladder is a prefix of the larger."""
    cfg = min(a.config, b.config, key=lambda c: c.max_capacity)
    n = len(cfg.levels())
    out = []
    for sk in (a, b):
        t = Sketch.__new__(Sketch)
        t.config, t.strata, t.levels = cfg, sk.strata, sk.levels[:n]
        out.append(t)
    return out[0], out[1]


def describe(rep: Report, specs: list[TableSpec], limit: int) -> dict:
    by_tag = {s.tag: s for s in specs}
    out = {"differences": len(rep.keys), "estimate": rep.estimate, "level": rep.level,
           "failed_partitions": rep.failed_partitions,
           "cost": {"bytes": rep.cost.bytes, "round_trips": rep.cost.round_trips, "rows_rescanned": rep.cost.rows_scanned}}
    for name, keys in (("missing_in_target", rep.missing_in_target), ("extra_in_target", rep.extra_in_target),
                       ("value_mismatch", rep.value_mismatch)):
        rows = []
        for khi, klo in sorted(keys):
            tag, _ = canon.unpack_key(khi, klo, 1)
            spec = by_tag[tag]
            _, parts = canon.unpack_key(khi, klo, len(spec.key))
            rows.append({"table": spec.name, **dict(zip(spec.key, parts))})
        out[name] = {"count": len(rows), "keys": rows if limit < 0 else rows[:limit]}
    return out


def show(result: dict, as_json: bool, limit: int) -> None:
    if as_json:
        print(json.dumps(result, indent=2))
        return
    n = result["differences"]
    print("source and target are identical" if n == 0 else f"{n} differing keys")
    for name in ("missing_in_target", "extra_in_target", "value_mismatch"):
        r = result[name]
        if r["count"]:
            print(f"  {name}: {r['count']}")
            for k in r["keys"]:
                print("    " + " ".join(f"{a}={b}" for a, b in k.items()))
            if 0 <= limit < r["count"]:
                print(f"    ... {r['count'] - limit} more (use --limit -1 or --json)")
    c = result["cost"]
    print(f"compared {c['bytes']:,} bytes of sketch in {c['round_trips']} round trips; "
          f"rescanned {c['rows_rescanned']:,} rows")


def cmd_sketch(args) -> None:
    specs = load_spec(args.spec)
    store = Store(args.store)
    t0 = time.perf_counter()
    table = store.scan(specs)
    store.close()
    sk = build(table, args.max_differences)
    save(sk, len(table), args.output)
    print(f"{len(table):,} rows -> {args.output} ({Path(args.output).stat().st_size:,} bytes, "
          f"{time.perf_counter() - t0:.1f} s)", file=sys.stderr)


def _store_rescan(args, specs):
    """Fallback for partitions that did not decode: compare their rows exactly."""
    cache = {}

    def rescan(partitions: int, p: int):
        if not (args.source and args.target):
            raise SystemExit("a partition did not decode; pass --source and --target to compare it exactly")
        if not cache:
            stores = [Store(args.source), Store(args.target)]
            cache["r"] = table_rescan(stores[0].scan(specs), stores[1].scan(specs))
            for s in stores:
                s.close()
        return cache["r"](partitions, p)

    return rescan


def cmd_diff(args) -> None:
    specs = load_spec(args.spec)
    (a, _), (b, _) = load(args.source_sketch), load(args.target_sketch)
    a, b = common(a, b)
    rep = verify(a, b, _store_rescan(args, specs))
    show(describe(rep, specs, args.limit), args.json, args.limit)
    sys.exit(1 if rep.keys else 0)


def cmd_verify(args) -> None:
    specs = load_spec(args.spec)
    tables = []
    for url in (args.source, args.target):
        store = Store(url)
        tables.append(store.scan(specs))
        store.close()
    a, b = common(build(tables[0]), build(tables[1]))
    rep = verify(a, b, table_rescan(*tables))
    show(describe(rep, specs, args.limit), args.json, args.limit)
    sys.exit(1 if rep.keys else 0)


def main(argv=None) -> None:
    p = argparse.ArgumentParser(prog="deltaproof", description="Find the exact rows that differ between two databases.")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("sketch", help="scan one store and write its sketch")
    s.add_argument("--spec", required=True)
    s.add_argument("--store", required=True)
    s.add_argument("-o", "--output", required=True)
    s.add_argument("--max-differences", type=int,
                   help="size the file for at most this many differences (smaller file; more falls back to "
                        "an exact comparison of the stores)")
    s.set_defaults(fn=cmd_sketch)
    for name, fn, hlp in (("diff", cmd_diff, "compare two sketch files"),
                          ("verify", cmd_verify, "scan both stores and compare")):
        c = sub.add_parser(name, help=hlp)
        c.add_argument("--spec", required=True)
        if name == "diff":
            c.add_argument("source_sketch")
            c.add_argument("target_sketch")
        c.add_argument("--source", required=name == "verify")
        c.add_argument("--target", required=name == "verify")
        c.add_argument("--json", action="store_true")
        c.add_argument("--limit", type=int, default=20, help="keys to print per class (-1 = all)")
        c.set_defaults(fn=fn)
    args = p.parse_args(argv)
    args.fn(args)


if __name__ == "__main__":
    main()
