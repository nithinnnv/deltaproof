#!/bin/sh
# Start the services on a cluster created by cluster.sh, and prepare the app VM.
#
#   infra/gcp/services.sh            # PostgreSQL, Cassandra (CASS_NODES), Kafka + Debezium Connect, app
#   infra/gcp/services.sh check      # print each service's health
set -eu
ZONE=${ZONE:-us-central1-a}
CASS_NODES=${CASS_NODES:-3}
EXT=${DELTAPROOF_EXT:-$HOME/workspace/deltaproof-ext}
REPO=${REPO:-https://github.com/nithinnnv/deltaproof}

ip() { gcloud compute instances describe "$1" --zone "$ZONE" --format "value(networkInterfaces[0].networkIP)"; }
on() { name=$1; shift; gcloud compute ssh "$name" --zone "$ZONE" --quiet --command "$*"; }

start() {
  PG_IP=$(ip dp-pg); KAFKA_IP=$(ip dp-kafka); SEED=$(ip dp-cass-1)

  echo "== PostgreSQL ($PG_IP)"
  on dp-pg "sudo docker rm -f pg >/dev/null 2>&1; sudo docker run -d --name pg --network host --shm-size=2g \
    -e POSTGRES_PASSWORD=deltaproof -e TZ=UTC -e PGTZ=UTC -v /srv/pg:/var/lib/postgresql/data postgres:16.4 \
    -c wal_level=logical -c max_wal_senders=10 -c max_replication_slots=10 -c shared_buffers=8GB \
    -c max_connections=300 -c timezone=UTC >/dev/null"

  i=1
  while [ "$i" -le "$CASS_NODES" ]; do
    node=dp-cass-$i; NIP=$(ip "$node")
    echo "== Cassandra $node ($NIP)"
    on "$node" "sudo docker rm -f cass >/dev/null 2>&1; sudo docker run -d --name cass --network host \
      -e CASSANDRA_CLUSTER_NAME=deltaproof -e CASSANDRA_SEEDS=$SEED -e CASSANDRA_LISTEN_ADDRESS=$NIP \
      -e CASSANDRA_BROADCAST_ADDRESS=$NIP -e CASSANDRA_RPC_ADDRESS=0.0.0.0 -e CASSANDRA_BROADCAST_RPC_ADDRESS=$NIP \
      -e MAX_HEAP_SIZE=8G -e HEAP_NEWSIZE=1G -v /srv/cass:/var/lib/cassandra cassandra:4.1.6 >/dev/null"
    # Nodes must join one at a time.
    until on "$node" "sudo docker exec cass nodetool status 2>/dev/null | grep -q \"UN  $NIP\""; do sleep 10; done
    echo "   $node joined"
    i=$((i + 1))
  done

  echo "== Kafka + Debezium Connect ($KAFKA_IP)"
  on dp-kafka "sudo docker rm -f kafka connect >/dev/null 2>&1; sudo docker run -d --name kafka --network host \
    -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://127.0.0.1:9093 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://$KAFKA_IP:9092 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    -e KAFKA_NUM_PARTITIONS=1 -e KAFKA_LOG_RETENTION_HOURS=48 apache/kafka:3.8.0 >/dev/null && sleep 15 && \
    sudo docker run -d --name connect --network host -e BOOTSTRAP_SERVERS=$KAFKA_IP:9092 -e GROUP_ID=dp \
    -e CONFIG_STORAGE_TOPIC=dp_configs -e OFFSET_STORAGE_TOPIC=dp_offsets -e STATUS_STORAGE_TOPIC=dp_statuses \
    -e CONFIG_STORAGE_REPLICATION_FACTOR=1 -e OFFSET_STORAGE_REPLICATION_FACTOR=1 -e STATUS_STORAGE_REPLICATION_FACTOR=1 \
    quay.io/debezium/connect:2.7.3.Final >/dev/null"
  until on dp-kafka "curl -sf http://localhost:8083/connector-plugins | grep -q PostgresConnector"; do sleep 10; done
  echo "   Debezium PostgresConnector available"

  echo "== app"
  tar -C "$EXT" -czf /tmp/dp-benchbase.tgz benchbase-postgres
  gcloud compute scp /tmp/dp-benchbase.tgz dp-app:/tmp/ --zone "$ZONE" --quiet
  on dp-app "set -e; rm -rf ~/deltaproof ~/benchbase-postgres; git clone -q $REPO ~/deltaproof; \
    tar -C ~ -xzf /tmp/dp-benchbase.tgz; python3 -m venv ~/venv; \
    ~/venv/bin/pip install -q -e \"\$HOME/deltaproof[db]\" confluent-kafka requests orjson py-spy; \
    echo PG_IP=$PG_IP KAFKA_IP=$KAFKA_IP CASS_IPS=$(for j in $(seq 1 "$CASS_NODES"); do printf '%s,' "$(ip dp-cass-$j)"; done) > ~/cluster.env"
  on dp-app "cat ~/cluster.env"
}

check() {
  on dp-pg "sudo docker exec pg pg_isready -U postgres"
  on dp-cass-1 "sudo docker exec cass nodetool status | grep -E '^(UN|DN|UJ)'"
  on dp-kafka "curl -s http://localhost:8083/ | head -c 120; echo"
  on dp-app "cat ~/cluster.env; ~/venv/bin/python -c 'import deltaproof, confluent_kafka, cassandra; print(\"app ok\")'"
}

case "${1:-start}" in
  start) start ;;
  check) check ;;
  *) echo "usage: $0 [start|check]" >&2; exit 2 ;;
esac
