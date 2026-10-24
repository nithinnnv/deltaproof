"""Parallel scans: each worker reads one slice of a table and encodes it, returning only elements.

PostgreSQL tables are sliced by physical block range (TID range scans, PostgreSQL 14+), so the
slices do not depend on key types or distribution. Cassandra tables are sliced by token range
over the table's partition key, the same way the server distributes data.
"""

from __future__ import annotations

import os
from concurrent.futures import ProcessPoolExecutor
from urllib.parse import urlparse

import numpy as np

from .relational import TableSpec
from .store import Table

_MIN_TOKEN, _MAX_TOKEN = -(2**63), 2**63 - 1
_conn = None  # one connection per worker process


def _connect(url: str):
    global _conn
    u = urlparse(url)
    if u.scheme == "cassandra":
        from urllib.parse import parse_qs

        from cassandra import ConsistencyLevel
        from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile

        # ?consistency=QUORUM for replicated keyspaces, so reads see every acknowledged write
        level = parse_qs(u.query).get("consistency", ["ONE"])[0].upper()
        profile = ExecutionProfile(consistency_level=getattr(ConsistencyLevel, level), request_timeout=600)
        cluster = Cluster([u.hostname or "127.0.0.1"], port=u.port or 9042,
                          execution_profiles={EXEC_PROFILE_DEFAULT: profile})
        _conn = ("cassandra", cluster.connect(), u.path.strip("/"))
    else:
        import psycopg

        _conn = ("postgres", psycopg.connect(url, autocommit=True), None)


def _encode(spec: TableSpec, columns: list[str], rows) -> tuple[list, list, list]:
    kh, kl, dg = [], [], []
    for r in rows:
        h, l, d = spec.element(dict(zip(columns, r)))
        kh.append(h)
        kl.append(l)
        dg.append(d)
    return kh, kl, dg


def _scan_slice(spec: TableSpec, lo: int, hi: int, pk: tuple[str, ...],
                snapshot: str | None) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    kind, conn, keyspace = _conn
    cols = spec.column_names
    if kind == "postgres":
        q = (f"SELECT {', '.join(cols)} FROM {spec.name} "
             f"WHERE ctid >= '({lo},0)'::tid AND ctid < '({hi},0)'::tid")
        with conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            conn.execute(f"SET TRANSACTION SNAPSHOT '{snapshot}'")  # every slice sees one snapshot
            with conn.cursor(name="deltaproof_slice") as cur:  # server-side: memory bounded per batch
                cur.itersize = 10_000
                cur.execute(q)
                kh, kl, dg = _encode(spec, cols, cur)
    else:
        from cassandra.query import SimpleStatement

        token = f"token({', '.join(pk)})"
        lo_op = ">=" if lo == _MIN_TOKEN else ">"
        stmt = SimpleStatement(f"SELECT {', '.join(cols)} FROM {keyspace}.{spec.name} "
                               f"WHERE {token} {lo_op} {lo} AND {token} <= {hi}", fetch_size=5000)
        kh, kl, dg = _encode(spec, cols, conn.execute(stmt, timeout=600))
    u = lambda x: np.array(x, dtype=np.uint64)
    return u(kh), u(kl), u(dg)


def _postgres_slices(url: str, spec: TableSpec, n: int) -> list[tuple[int, int]]:
    import psycopg

    with psycopg.connect(url, autocommit=True) as c:
        blocks = c.execute("SELECT pg_relation_size(%s::regclass) / current_setting('block_size')::int",
                           (spec.name,)).fetchone()[0]
    blocks = int(blocks) + 1  # rows appended during the scan land in the last slice's range
    step = max(1, -(-blocks // n))
    bounds = list(range(0, blocks, step)) + [2**31 - 1]
    return list(zip(bounds[:-1], bounds[1:]))


def _cassandra_slices(url: str, spec: TableSpec, n: int) -> tuple[list[tuple[int, int]], tuple[str, ...]]:
    from cassandra.cluster import Cluster

    u = urlparse(url)
    cluster = Cluster([u.hostname or "127.0.0.1"], port=u.port or 9042)
    cluster.connect()
    meta = cluster.metadata.keyspaces[u.path.strip("/")].tables[spec.name]
    pk = tuple(c.name for c in meta.partition_key)
    cluster.shutdown()
    edges = [_MIN_TOKEN + (2**64 * i) // n for i in range(n)] + [_MAX_TOKEN]
    return list(zip(edges[:-1], edges[1:])), pk


def scan(url: str, specs: list[TableSpec], workers: int | None = None) -> Table:
    """Scan every table in `specs` with `workers` processes; returns all elements."""
    workers = workers or min(8, os.cpu_count() or 1)
    cassandra = urlparse(url).scheme == "cassandra"
    leader = None
    snapshot = None
    if not cassandra:
        import psycopg

        # Hold a repeatable-read transaction open and export its snapshot for the workers.
        leader = psycopg.connect(url)
        leader.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        snapshot = leader.execute("SELECT pg_export_snapshot()").fetchone()[0]
    try:
        tasks = []
        for spec in specs:
            if cassandra:
                slices, pk = _cassandra_slices(url, spec, 4 * workers)
            else:
                slices, pk = _postgres_slices(url, spec, 4 * workers), ()
            tasks += [(spec, lo, hi, pk, snapshot) for lo, hi in slices]
        with ProcessPoolExecutor(workers, initializer=_connect, initargs=(url,)) as pool:
            parts = list(pool.map(_scan_slice, *zip(*tasks)))
    finally:
        if leader is not None:
            leader.rollback()
            leader.close()
    cat = lambda i: np.concatenate([p[i] for p in parts]) if parts else np.zeros(0, np.uint64)
    khi, klo, dig = cat(0), cat(1), cat(2)
    return Table(khi, klo, dig, np.zeros(len(klo), np.int64))
