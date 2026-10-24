# Command-line runs on TPC-H lineitem, SF 0.1

Data loaded and faults injected by `bench/tpch_e2e.py --sf 0.1` (see `results/tpch_sf0.1.csv`),
then compared with the `deltaproof` command (spec: `examples/tpch/lineitem.toml`).

| Run | Sketch file per store | Scan + encode + sketch |
| --- | --- | --- |
| `verify` (both stores, one command) | none | — |
| `sketch` + `diff`, full ladder | 12,304,973 B (PostgreSQL), 12,305,034 B (Cassandra) | 18.5 s, 28.6 s |
| `sketch --max-differences 1000` + `diff` | 232,241 B, 232,262 B | 17.7 s, 27.8 s |

All three reported the same 120 keys (20 missing, 40 extra, 60 value mismatches), comparing
260,496 bytes of sketch in 2 round trips with no rows rescanned.

## Parallel scans and diagnosis, TPC-H lineitem SF 1

`scan_workers_sf1.json`: scan + encode time by worker count (final code: PostgreSQL slices read
through server-side cursors from one exported snapshot). Every run produced exactly the same
elements as one worker.

| Workers | 1 | 2 | 4 | 8 |
| --- | --- | --- | --- | --- |
| PostgreSQL (s) | 176.4 | 92.8 | 49.1 | 26.6 |
| Cassandra (s) | 292.5 | 138.6 | 87.4 | 37.6 |

`scan_workers_sf0.1.json` was measured at SF 0.1 before PostgreSQL workers switched to
server-side cursors.

`tpch_sf1_verify.json` is the full `verify --json` report at SF 1 (120 keys);
`tpch_sf1_inspect_all.txt` is `inspect` on it: each of the 60 value mismatches names exactly one
column (20 each of l_extendedprice, l_shipdate, l_comment, the injected value faults).
