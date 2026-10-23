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
