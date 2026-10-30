#!/bin/sh
# Local Docker stack for bench/live_mysql_pg.py: MySQL 8.0 (binlog), PostgreSQL 16 (target), Kafka, Debezium Connect.
#   sh bench/mysql_pg_local.sh up      # start, and print the environment for live_mysql_pg.py
#   sh bench/mysql_pg_local.sh down
set -eu
NET=dpm
case "${1:-up}" in
up)
  docker network inspect $NET >/dev/null 2>&1 || docker network create $NET >/dev/null
  docker rm -f -v dpm-mysql dpm-pgt dpm-kafka dpm-connect >/dev/null 2>&1 || true  # -v: also their data volumes
  docker run -d --name dpm-mysql --network $NET -p 13306:3306 -e MYSQL_ROOT_PASSWORD=deltaproof -e TZ=UTC \
    mysql:8.0.39 --server-id=1 --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL \
    --default-time-zone=+00:00 --max-connections=300 >/dev/null
  docker run -d --name dpm-pgt --network $NET -p 15432:5432 -e POSTGRES_PASSWORD=deltaproof -e TZ=UTC -e PGTZ=UTC \
    postgres:16.4 -c timezone=UTC -c max_connections=200 >/dev/null
  docker run -d --name dpm-kafka --network $NET -p 19092:19092 \
    -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:19092,CONTROLLER://127.0.0.1:9093 \
    -e KAFKA_ADVERTISED_LISTENERS=INTERNAL://dpm-kafka:9092,EXTERNAL://127.0.0.1:19092 \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    -e KAFKA_NUM_PARTITIONS=1 apache/kafka:3.8.0 >/dev/null
  sleep 15
  docker run -d --name dpm-connect --network $NET -p 18083:8083 -e BOOTSTRAP_SERVERS=dpm-kafka:9092 -e GROUP_ID=dpm \
    -e CONFIG_STORAGE_TOPIC=dpm_configs -e OFFSET_STORAGE_TOPIC=dpm_offsets -e STATUS_STORAGE_TOPIC=dpm_statuses \
    -e CONFIG_STORAGE_REPLICATION_FACTOR=1 -e OFFSET_STORAGE_REPLICATION_FACTOR=1 -e STATUS_STORAGE_REPLICATION_FACTOR=1 \
    quay.io/debezium/connect:2.7.3.Final >/dev/null
  until docker exec dpm-mysql mysqladmin -uroot -pdeltaproof ping >/dev/null 2>&1; do sleep 3; done
  until docker exec dpm-pgt pg_isready -U postgres >/dev/null 2>&1; do sleep 2; done
  until curl -sf http://127.0.0.1:18083/connector-plugins | grep -q MySqlConnector; do sleep 5; done
  echo "export MYSQL_IP=127.0.0.1 MYSQL_PORT=13306 MYSQL_CONNECT_HOST=dpm-mysql MYSQL_CONNECT_PORT=3306" \
       "PGT_IP=127.0.0.1 PGT_PORT=15432 KAFKA=127.0.0.1:19092 CONNECT_KAFKA=dpm-kafka:9092" \
       "CONNECT=http://127.0.0.1:18083 BB_NET=$NET BB_DIR=\$HOME/workspace/deltaproof-ext/benchbase-mysql"
  ;;
down)
  docker rm -f -v dpm-mysql dpm-pgt dpm-kafka dpm-connect >/dev/null 2>&1 || true  # -v: also their data volumes
  docker network rm $NET >/dev/null 2>&1 || true
  ;;
esac
