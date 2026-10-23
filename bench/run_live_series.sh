#!/bin/sh
# The live TPC-C experiment series reported in the paper. Needs: docker compose -p deltaproof up -d
set -e
cd "$(dirname "$0")/.."
PY=${PY:-python}
$PY bench/live_e2e.py baseline --rate 200 --seconds 120
$PY bench/live_e2e.py live --rate 200 --seconds 120 --lag 2
$PY bench/live_e2e.py live --rate 200 --seconds 120 --lag 5
$PY bench/live_e2e.py live --rate 200 --seconds 120 --lag 2 --reorder 0.05
$PY bench/live_e2e.py baseline --rate unlimited --seconds 60
$PY bench/live_e2e.py live --rate unlimited --seconds 60 --lag 2
