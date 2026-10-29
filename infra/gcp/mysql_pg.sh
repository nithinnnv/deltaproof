#!/bin/sh
# Second engine pair on Google Cloud: MySQL (source, own VM) -> Debezium/Kafka -> PostgreSQL (target).
# Two VMs, labelled deltaproof=mysqlpg so the PostgreSQL -> Cassandra cluster's teardown never touches them:
#   dpm-mysql  MySQL 8.0 with the binlog
#   dpm-app    Kafka + Debezium Connect, the PostgreSQL target, BenchBase, and the replicator/verifier
#
#   infra/gcp/mysql_pg.sh up          # create VMs, start services, prepare the app VM
#   infra/gcp/mysql_pg.sh series      # load, then the runs below, in the background on dpm-app
#   infra/gcp/mysql_pg.sh watch 240   # collect results/mysql_pg and delete both VMs when done, or at a deadline
#   infra/gcp/mysql_pg.sh down
set -eu
ZONE=${ZONE:-us-central1-a}
LABEL=deltaproof=mysqlpg
BUCKET=${BUCKET:-$(gcloud config get-value project 2>/dev/null)-deltaproof-results}
EXT=${DELTAPROOF_EXT:-$HOME/workspace/deltaproof-ext}
REPO=${REPO:-https://github.com/nithinnnv/deltaproof}
HERE=$(cd "$(dirname "$0")/../.." && pwd)
STARTUP='#!/bin/sh
set -e
if ! command -v docker >/dev/null; then
  apt-get update -q && apt-get install -yq docker.io git python3-venv python3-pip >/dev/null
  systemctl enable --now docker
fi
touch /var/run/deltaproof-ready'
log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $*"; }
ip() { gcloud compute instances describe "$1" --zone "$ZONE" --format "value(networkInterfaces[0].networkIP)"; }
on() { name=$1; shift; gcloud compute ssh "$name" --zone "$ZONE" --quiet --command "$*"; }

up() {
  gcloud storage buckets describe "gs://$BUCKET" >/dev/null 2>&1 ||
    gcloud storage buckets create "gs://$BUCKET" --location us-central1 --uniform-bucket-level-access >/dev/null
  # The VMs' default service account may create objects in this bucket only (newer projects grant it no roles).
  sa=$(gcloud iam service-accounts list --filter "email~compute@developer" --format "value(email)" | head -1)
  gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" --member "serviceAccount:$sa" \
    --role roles/storage.objectCreator >/dev/null
  for spec in dpm-mysql:n2-standard-8:200 dpm-app:n2-standard-16:200; do
    name=${spec%%:*}; rest=${spec#*:}; type=${rest%%:*}; disk=${rest#*:}
    gcloud compute instances describe "$name" --zone "$ZONE" >/dev/null 2>&1 && { echo "$name exists"; continue; }
    gcloud compute instances create "$name" --zone "$ZONE" --machine-type "$type" \
      --max-run-duration="${MAX_RUN:-4h}" --instance-termination-action=DELETE \
      --image-family debian-12 --image-project debian-cloud --boot-disk-size "${disk}GB" --boot-disk-type pd-ssd \
      --labels "$LABEL" --scopes default,storage-rw \
      --metadata startup-script="$STARTUP",results-bucket="$BUCKET" --format "value(name)" &
  done
  wait
  for name in dpm-mysql dpm-app; do
    until on "$name" "test -f /var/run/deltaproof-ready" >/dev/null 2>&1; do sleep 10; done
    echo "$name ready"
  done
  MYSQL_IP=$(ip dpm-mysql); APP_IP=$(ip dpm-app)

  echo "== MySQL ($MYSQL_IP)"
  on dpm-mysql "sudo docker rm -f mysql >/dev/null 2>&1; sudo docker run -d --name mysql --network host \
    -e MYSQL_ROOT_PASSWORD=deltaproof -e TZ=UTC -v /srv/mysql:/var/lib/mysql mysql:8.0.39 \
    --server-id=1 --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL --default-time-zone=+00:00 \
    --max-connections=300 --innodb-buffer-pool-size=8G >/dev/null; \
    until sudo docker exec mysql mysqladmin -uroot -pdeltaproof ping >/dev/null 2>&1; do sleep 3; done"

  echo "== app: Kafka, Debezium Connect, PostgreSQL target ($APP_IP)"
  on dpm-app "sudo docker rm -f kafka connect pgt >/dev/null 2>&1; \
    sudo docker run -d --name pgt --network host -e POSTGRES_PASSWORD=deltaproof -e TZ=UTC -e PGTZ=UTC \
      -v /srv/pgt:/var/lib/postgresql/data postgres:16.4 -c shared_buffers=8GB -c max_connections=200 -c timezone=UTC >/dev/null; \
    sudo docker run -d --name kafka --network host -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
      -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://127.0.0.1:9093 \
      -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://$APP_IP:9092 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
      -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
      -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
      -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
      -e KAFKA_NUM_PARTITIONS=1 -e KAFKA_LOG_RETENTION_HOURS=48 apache/kafka:3.8.0 >/dev/null && sleep 15 && \
    sudo docker run -d --name connect --network host -e BOOTSTRAP_SERVERS=$APP_IP:9092 -e GROUP_ID=dpm \
      -e CONFIG_STORAGE_TOPIC=dpm_configs -e OFFSET_STORAGE_TOPIC=dpm_offsets -e STATUS_STORAGE_TOPIC=dpm_statuses \
      -e CONFIG_STORAGE_REPLICATION_FACTOR=1 -e OFFSET_STORAGE_REPLICATION_FACTOR=1 -e STATUS_STORAGE_REPLICATION_FACTOR=1 \
      quay.io/debezium/connect:2.7.3.Final >/dev/null; \
    until curl -sf http://localhost:8083/connector-plugins | grep -q MySqlConnector; do sleep 10; done; \
    until sudo docker exec pgt pg_isready -U postgres >/dev/null 2>&1; do sleep 2; done"
  echo "   Debezium MySqlConnector available"

  tar -C "$EXT" -czf /tmp/dpm-benchbase.tgz benchbase-mysql
  gcloud compute scp /tmp/dpm-benchbase.tgz dpm-app:/tmp/ --zone "$ZONE" --quiet
  on dpm-app "set -e; rm -rf ~/deltaproof ~/benchbase-mysql; git clone -q $REPO ~/deltaproof; \
    tar -C ~ -xzf /tmp/dpm-benchbase.tgz; python3 -m venv ~/venv; \
    ~/venv/bin/pip install -q -e \"\$HOME/deltaproof[db]\" confluent-kafka requests; \
    echo MYSQL_IP=$MYSQL_IP PGT_IP=127.0.0.1 KAFKA_IP=$APP_IP > ~/cluster.env; cat ~/cluster.env; \
    git -C ~/deltaproof log --oneline -1"
}

series() {
  on dpm-app "cat > ~/run_series.sh <<'EOF'
#!/bin/sh
cd ~/deltaproof
PY=~/venv/bin/python
W='--warehouses 4 --terminals 16'
\$PY bench/live_mysql_pg.py load \$W
\$PY bench/live_mysql_pg.py baseline --rate 100 --seconds 300 \$W
\$PY bench/live_mysql_pg.py live --rate 100 --lag 2 --seconds 300 \$W
\$PY bench/live_mysql_pg.py live --rate 100 --lag 2 --reorder 0.05 --seconds 300 \$W
sh ~/deltaproof/infra/gcp/upload_results.sh mysql_pg ~/series.log
echo SERIES DONE
EOF
chmod +x ~/run_series.sh; setsid nohup ~/run_series.sh > ~/series.log 2>&1 < /dev/null & echo launched"
}

collect() {
  on dpm-app "tar -C ~/deltaproof/results -czf /tmp/mpg-results.tgz mysql_pg && cp ~/series.log /tmp/mpg-series.log" >/dev/null 2>&1 &&
  gcloud compute scp dpm-app:/tmp/mpg-results.tgz dpm-app:/tmp/mpg-series.log "$HERE/results/" --zone "$ZONE" --quiet >/dev/null 2>&1 &&
  tar -C "$HERE/results" -xzf "$HERE/results/mpg-results.tgz" && rm -f "$HERE/results/mpg-results.tgz" &&
  mv "$HERE/results/mpg-series.log" "$HERE/results/mysql_pg/series.log" &&
  log "results copied: $(ls "$HERE/results/mysql_pg" | tr '\n' ' ')"
}

down() {
  names=$(gcloud compute instances list --filter "labels.deltaproof=mysqlpg" --format "value(name)")
  # shellcheck disable=SC2086
  [ -n "$names" ] && gcloud compute instances delete $names --zone "$ZONE" --quiet --delete-disks all
  echo "mysqlpg VMs left: $(gcloud compute instances list --filter "labels.deltaproof=mysqlpg" --format "value(name)" | wc -l)"
}

watch() {
  end=$(( $(date +%s) + ${1:-240} * 60 ))
  log "mysql_pg watchdog started; deadline in ${1:-240} min"
  while :; do
    if on dpm-app "grep -q 'SERIES DONE' ~/series.log" >/dev/null 2>&1; then log "series finished"; break; fi
    if [ "$(date +%s)" -ge "$end" ]; then log "deadline reached"; break; fi
    gcloud compute instances describe dpm-app --zone "$ZONE" >/dev/null 2>&1 || { log "dpm-app gone"; break; }
    sleep 120
  done
  collect || log "WARNING: could not copy results"
  down
  log "watchdog finished"
}

case "${1:-}" in
  up) up ;;
  series) series ;;
  watch) watch "${2:-240}" ;;
  collect) collect ;;
  down) down ;;
  *) echo "usage: $0 up|series|watch [minutes]|collect|down" >&2; exit 2 ;;
esac
