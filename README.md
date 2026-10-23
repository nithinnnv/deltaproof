# DeltaProof

Difference-proportional verification of live heterogeneous database migrations.

When a system migrates from one database to another (for example PostgreSQL to Cassandra),
DeltaProof proves the two stores hold the same data. Each store keeps small, incrementally
updated sketches (invertible Bloom lookup tables) over a canonical encoding of its records.
Subtracting the two sketches and decoding the result yields the exact keys that differ, at a
cost that grows with the number of differences, not the size of the tables, and without
needing both stores to scan in the same key order.

This repository is the reference implementation and benchmark for the paper
*Difference-Proportional Verification of Live Heterogeneous Database Migrations with
Invertible Bloom Lookup Tables*.

## Command-line tool

Describe the tables once (see `examples/tpch/lineitem.toml`), then compare two databases:

```sh
deltaproof verify --spec lineitem.toml \
    --source postgresql://user:pw@pg:5432/tpch --target cassandra://cass:9042/tpch
```

It prints every key that is missing in the target, extra in the target, or holds a different
value, and exits with status 1 if any differ (`--json` for a machine-readable report). When the
stores sit in different networks, sketch each one where it lives and compare the files anywhere:

```sh
deltaproof sketch --spec lineitem.toml --store postgresql://... --max-differences 1000 -o pg.npz
deltaproof sketch --spec lineitem.toml --store cassandra://...  --max-differences 1000 -o cs.npz
deltaproof diff   --spec lineitem.toml pg.npz cs.npz
```

`--max-differences` keeps the files small (232 KB for 600,000 rows at 1,000); if more differences
turn up, `diff` compares the stores exactly when given `--source` and `--target`.

## Layout

| Path | What it is |
| --- | --- |
| `deltaproof/canon.py` | Canonical record encoding: decimals, timestamps, text, nulls |
| `deltaproof/sketch.py` | Partitioned IBLTs, strata estimator, sketch ladder |
| `deltaproof/verify.py` | The verification protocol and its cost accounting |
| `deltaproof/live.py` | Verification while writes are in flight (watermark + confirmation) |
| `deltaproof/baselines.py` | Full scan, range-checksum bisection, Merkle tree, aggregates |
| `deltaproof/faults.py` | Fault injection: 8 divergence classes with ground truth |
| `deltaproof/workload.py` | Synthetic double-entry ledger |
| `deltaproof/adapters.py` | PostgreSQL and Cassandra scan adapters |
| `deltaproof/cli.py` | The `deltaproof` command: `verify`, `sketch`, `diff` |
| `paper/main.tex` | Full research paper |
| `paper/saner/main.tex` | Tool paper (SANER 2027 tool demo track) |
| `bench/run.py` | In-process experiments RQ1–RQ5 |
| `bench/e2e.py` | End-to-end experiment on real PostgreSQL and Cassandra |
| `results/` | CSV output of the experiments |

## Quick start

```sh
python3 -m venv .venv && .venv/bin/pip install -e ".[dev,db]"
.venv/bin/python -m pytest -q
.venv/bin/python bench/run.py rq1 rq2 rq3 rq4 rq5 --scale small   # minutes
.venv/bin/python bench/run.py rq1 rq2 rq3 rq4 rq5 --scale full    # up to 10M rows

docker compose -p deltaproof up -d                                  # PostgreSQL + Cassandra
.venv/bin/python bench/e2e.py --rows 200000
```

## Status and limitations

- `bench/live_e2e.py` feeds the source sketch from PostgreSQL logical decoding (wal2json) under
  a BenchBase TPC-C load; the target sketch follows the values the replicator writes. Changes
  made to the target outside that path would need the target's own change stream.
- `bench/tpch_e2e.py` runs on TPC-H `lineitem` generated with dbgen (via DuckDB); these are not
  TPC-H benchmark results. The PinSketch baseline needs `native/build_minisketch.sh`.
- The bisection baseline assumes both stores can serve key-range checksums (its best case);
  a hash-partitioned target such as Cassandra cannot do this without scanning.
- Single-write sketch updates in Python cost hundreds of microseconds of interpreter
  overhead; batched updates cost a few microseconds per write. A production implementation
  would batch change events.
- Keys must be 64-bit integers or UUIDs.

## License

Apache License 2.0.
