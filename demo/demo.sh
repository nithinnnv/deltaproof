#!/bin/sh
# Paced terminal demo for the screencast (about 3.5 minutes). Prerequisites, run once before recording:
#   docker compose -p deltaproof up -d
#   python bench/tpch_e2e.py --sf 0.1     # loads TPC-H lineitem into both stores and injects 120 faults
# Then start the screen recording and run:  ./demo/demo.sh     (PAUSE=1 for a quick rehearsal)
cd "$(dirname "$0")/.."
# Use the project's virtual environment if deltaproof is not already on PATH.
for venv in .venv "$HOME/workspace/diffsketch/.venv"; do
  command -v deltaproof >/dev/null 2>&1 || PATH="$venv/bin:$PATH"
done
if ! command -v deltaproof >/dev/null 2>&1; then
  echo "deltaproof not found: run 'pip install -e .' in a virtual environment first" >&2; exit 2
fi
export PATH
export PG=postgresql://postgres:deltaproof@localhost:55432/tpch
export CS=cassandra://127.0.0.1:59042/tpch
export SPEC=examples/tpch/lineitem.toml
PAUSE=${PAUSE:-3}
ERR=$(mktemp)
trap 'rm -f "$ERR" report.json pg.npz cs.npz' EXIT

python_bin=$(dirname "$(command -v deltaproof)")/python
if ! "$python_bin" - <<EOF 2>/dev/null
import psycopg
n = psycopg.connect("$PG").execute("SELECT count(*) FROM lineitem").fetchone()[0]
raise SystemExit(0 if n else 1)
EOF
then
  echo "No TPC-H data in PostgreSQL: run 'docker compose -p deltaproof up -d' and" >&2
  echo "'python bench/tpch_e2e.py --sf 0.1' first (about 2 minutes)." >&2; exit 2
fi

say() { printf '\n\033[1;34m# %s\033[0m\n' "$1"; sleep "$PAUSE"; }
# Show a command as written (with $PG, $CS, $SPEC), run it, and hide only the database drivers' warnings.
run() {
  printf '\033[1m$ %s\033[0m\n' "$1"; sleep 1
  sh -c "$1" 2>"$ERR"; status=$?
  grep -v -i -E "warn|deprecat" "$ERR" >&2
  sleep "$PAUSE"; return $status
}

clear
say "DeltaProof: find the exact rows that differ between two databases."
say "A migration copied TPC-H lineitem (600,000 rows) from PostgreSQL to Cassandra. Did it copy everything correctly?"
run 'echo "PG=$PG"; echo "CS=$CS"'
say "Step 1. Describe the table once: its key and each column's logical type."
run 'cat $SPEC'
say "d2 = exact decimal, c = fixed-width text (padding ignored), dt = date. The types decide what counts as equal."
say "Step 2. Compare the two databases. Each side is scanned in parallel and summarized as a small sketch."
run 'deltaproof verify --spec $SPEC --source $PG --target $CS --limit 1'
say "Exit status $? : 0 = identical, 1 = differences, 2 = could not verify. A cutover script can gate on it."
say "Only 260 KB of sketch were compared for 600,000 rows, and the answer is exact: 20 missing, 40 extra, 60 changed."
say "Step 3. Save the full report as JSON, for automation."
run 'deltaproof verify --spec $SPEC --source $PG --target $CS --json --limit -1 > report.json; head -n 16 report.json'
say "Step 4. Diagnose: which columns differ, with both values?"
run 'deltaproof inspect --spec $SPEC --source $PG --target $CS report.json --limit 2'
say "Each mismatch names the column that differs. Across all 60 mismatches:"
run "deltaproof inspect --spec \$SPEC --source \$PG --target \$CS report.json --limit -1 | grep '^    ' | awk '{print \$1}' | sort | uniq -c"
say "Exactly the three injected faults: prices off by 0.01, shifted dates, corrupted text. Padding and decimal scale are never reported."
say "Step 5. Stores in different networks? Sketch each where it lives and compare the files anywhere."
run 'deltaproof sketch --spec $SPEC --store $PG --max-differences 1000 -o pg.npz'
run 'deltaproof sketch --spec $SPEC --store $CS --max-differences 1000 -o cs.npz'
run 'deltaproof diff --spec $SPEC pg.npz cs.npz --limit 1'
say "Same 120 keys from two 232 KB files. Code, Docker setup and all results: github.com/nithinnnv/deltaproof"
