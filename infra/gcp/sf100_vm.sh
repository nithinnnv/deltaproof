#!/bin/sh
# TPC-H lineitem at a large scale factor on one VM: PostgreSQL and Cassandra side by side, bulk loaded,
# 120 faults injected, then verified with deltaproof's parallel scans. Every step leaves a marker in
# $WORK, so after a Spot VM is reclaimed and restarted the job resumes where it stopped:
#   SF=100 sh ~/deltaproof/infra/gcp/sf100_vm.sh > ~/sf100.log 2>&1
set -eu
SF=${SF:-100}
CHUNKS=${CHUNKS:-32}
WORK=${WORK:-/data}
TBL=$WORK/tbl
DB=tpch_sf$SF
log() { echo "$(date '+%F %T') $*"; }
done_() { touch "$WORK/done.$1"; log "step $1 done"; }
is_done() { [ -f "$WORK/done.$1" ]; }
sudo mkdir -p "$WORK" && sudo chown "$(id -u):$(id -g)" "$WORK"

if ! is_done tools; then
  sudo apt-get install -yq build-essential >/dev/null
  rm -rf ~/tpch-dbgen && git clone -q https://github.com/electrum/tpch-dbgen ~/tpch-dbgen && make -s -C ~/tpch-dbgen >/dev/null
  curl -sfL https://github.com/datastax/dsbulk/releases/download/1.11.2/dsbulk-1.11.2.tar.gz | tar -C ~ -xz
  python3 -m venv ~/venv && ~/venv/bin/pip install -q -e "$HOME/deltaproof[db]" duckdb
  done_ tools
fi

if ! is_done gen; then
  log "generating lineitem SF $SF in $CHUNKS chunks"
  mkdir -p "$TBL"
  i=1
  while [ "$i" -le "$CHUNKS" ]; do
    (cd "$TBL" && ~/tpch-dbgen/dbgen -q -f -b ~/tpch-dbgen/dists.dss -s "$SF" -T L -C "$CHUNKS" -S "$i" &&
     sed -i 's/|$//' "lineitem.tbl.$i") &
    i=$((i + 1))
  done
  wait
  log "generated: $(du -sh "$TBL" | cut -f1)"
  done_ gen
fi

if ! is_done stores; then
  sudo docker rm -f pg cass >/dev/null 2>&1 || true
  sudo docker run -d --name pg -p 55432:5432 --shm-size=8g -e POSTGRES_PASSWORD=deltaproof -e TZ=UTC -e PGTZ=UTC \
    -v "$WORK/pg:/var/lib/postgresql/data" postgres:16.4 -c shared_buffers=24GB -c maintenance_work_mem=8GB \
    -c max_wal_size=64GB -c checkpoint_timeout=30min -c max_parallel_maintenance_workers=16 \
    -c max_connections=200 -c timezone=UTC >/dev/null
  sudo docker run -d --name cass -p 59042:9042 -e MAX_HEAP_SIZE=24G -e HEAP_NEWSIZE=4G \
    -e CASSANDRA_CLUSTER_NAME=deltaproof -v "$WORK/cass:/var/lib/cassandra" cassandra:4.1.6 >/dev/null
  until sudo docker exec pg pg_isready -U postgres >/dev/null 2>&1; do sleep 5; done
  until sudo docker exec cass cqlsh -e "describe keyspaces" >/dev/null 2>&1; do sleep 10; done
  done_ stores
else
  sudo docker start pg cass >/dev/null 2>&1 || true
  until sudo docker exec pg pg_isready -U postgres >/dev/null 2>&1; do sleep 5; done
  until sudo docker exec cass cqlsh -e "describe keyspaces" >/dev/null 2>&1; do sleep 10; done
fi

COLS="l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment"

load_pg() {
  if is_done pg; then return; fi
  log "loading PostgreSQL"
  sudo docker exec pg psql -qU postgres -c "DROP DATABASE IF EXISTS $DB WITH (FORCE)" -c "CREATE DATABASE $DB"
  # The table without its primary key; the key is added after the load, which is much faster.
  ddl=$(~/venv/bin/python -c "import re, sys; sys.path.insert(0, '$HOME/deltaproof/bench'); from tpch_e2e import PG_DDL; print(re.sub(r',\s*PRIMARY KEY.*', ')', PG_DDL, flags=re.S))")
  sudo docker exec pg psql -qU postgres -d "$DB" -c "$ddl"
  ls "$TBL"/lineitem.tbl.* | xargs -P 16 -I{} sh -c \
    "sudo docker exec -i pg psql -qU postgres -d $DB -c \"\\copy lineitem ($COLS) FROM STDIN WITH (FORMAT text, DELIMITER '|')\" < {}"
  sudo docker exec pg psql -qU postgres -d "$DB" -c "ALTER TABLE lineitem ADD PRIMARY KEY (l_orderkey, l_linenumber)" -c "ANALYZE lineitem"
  log "PostgreSQL rows: $(sudo docker exec pg psql -tAU postgres -d "$DB" -c 'SELECT count(*) FROM lineitem')"
  done_ pg
}

load_cass() {
  if is_done cass; then return; fi
  log "loading Cassandra with DSBulk"
  ddl=$(~/venv/bin/python -c "import sys; sys.path.insert(0, '$HOME/deltaproof/bench'); from tpch_e2e import LINEITEM; print(LINEITEM.cql_ddl('$DB'))")
  sudo docker exec cass cqlsh -e "CREATE KEYSPACE IF NOT EXISTS $DB WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
  sudo docker exec cass cqlsh -e "DROP TABLE IF EXISTS $DB.lineitem"
  sudo docker exec cass cqlsh -e "$ddl"
  mapping=$(echo "$COLS" | tr ',' '\n' | awk '{printf "%s%d=%s", (NR>1?",":""), NR-1, $0}')
  ~/dsbulk-1.11.2/bin/dsbulk load -url "$TBL" --connector.csv.fileNamePattern '**/lineitem.tbl.*' \
    -delim '|' -header false -k "$DB" -t lineitem -m "$mapping" -h 127.0.0.1 -port 59042 \
    --connector.csv.maxConcurrentFiles 16 --executor.maxPerSecond -1 \
    --log.directory "$WORK/dsbulk-logs" 2>&1 | tail -5
  done_ cass
}

load_pg &
load_cass &
wait
is_done pg && is_done cass || { log "a load failed"; exit 1; }

if ! is_done verify; then
  cd ~/deltaproof
  if [ -f "$WORK/inject.started" ]; then
    # Faults were injected by an interrupted attempt: verify only. The ground truth is then the exact
    # difference of full scans of both stores (it must be the 120 injected keys).
    log "resuming: verifying without injecting again"
    ~/venv/bin/python bench/tpch_scale.py --sf "$SF" --skip-load --workers 32 --max-differences 100000 --cache "$WORK/scan-cache"
  else
    touch "$WORK/inject.started"
    log "injecting 120 faults and verifying"
    ~/venv/bin/python bench/tpch_scale.py --sf "$SF" --loaded --workers 32 --max-differences 100000 --cache "$WORK/scan-cache"
  fi
  done_ verify
fi
log "SF100 DONE"
