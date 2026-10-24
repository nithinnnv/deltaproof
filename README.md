# DeltaProof

Find the exact rows that differ between two databases after a migration.

Before cutting over from one database to another (for example PostgreSQL to Cassandra),
DeltaProof checks a quiesced snapshot of both and reports every key that is missing, extra, or
holds a different value, and then which columns differ. A canonical encoder makes logically
equal records compare equal across stores; each store is summarized by a small sketch
(invertible Bloom lookup tables), so the comparison exchanges bytes proportional to the number
of differences, not the size of the tables, and works whatever order each store keeps its rows
in. On TPC-H `lineitem` with up to 60 million rows it found every injected fault from 260 KB of
sketch.

**Demo video (under 4 minutes):** https://www.youtube.com/watch?v=jJpkxPjW1ME

The library also maintains sketches from a change stream for verification while a migration
is live (`bench/live_e2e.py`); that mode is a research prototype.

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

Scans run in parallel (`--workers N`, before the subcommand; default up to 8): PostgreSQL tables
are split by block range and read from one exported snapshot, Cassandra tables by token range.

### Diagnosing differences

```sh
deltaproof verify --spec lineitem.toml --source ... --target ... --json --limit -1 > report.json
deltaproof inspect --spec lineitem.toml --source ... --target ... report.json
```

`inspect` reads each reported key from both stores and prints the columns whose canonical values
differ, with both values (`--json` for machine-readable output). Formatting-only differences
such as `CHAR` padding or decimal scale are not differences and are not shown.

The `--json` report of `verify` and `diff`:

```json
{
  "differences": 120, "estimate": 168, "level": 1, "failed_partitions": 0,
  "cost": {"bytes": 260496, "round_trips": 2, "rows_rescanned": 0},
  "missing_in_target": {"count": 20, "keys": [{"table": "lineitem", "l_orderkey": 11655, "l_linenumber": 3}]},
  "extra_in_target":   {"count": 40, "keys": ["..."]},
  "value_mismatch":    {"count": 60, "keys": ["..."]}
}
```

Exit status: 0 when the stores match, 1 when they differ, 2 when verification could not complete
(for example, an unreachable store), so `verify` can gate a cutover script or CI job.

### Keys

Key columns are integers unless typed: `key = ["region:c", "account:t"]`. Up to four integer key
columns below 2^30 are packed into the 128-bit key directly. Any other key (text, more columns,
or `hash_key = true` for wide integers) is hashed into it with BLAKE2b, and reported keys are
mapped back to their values with a key-only scan of the stores.

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
