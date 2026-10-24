#!/bin/sh
# On the app VM: remove every Debezium connector, replication slot and publication left by interrupted runs.
KAFKA=$(grep -o 'KAFKA_IP=[0-9.]*' ~/cluster.env | cut -d= -f2)
PG=$(grep -o 'PG_IP=[0-9.]*' ~/cluster.env | cut -d= -f2)
for c in $(curl -s "http://$KAFKA:8083/connectors" | tr -d '[]"' | tr ',' ' '); do
  curl -s -X DELETE "http://$KAFKA:8083/connectors/$c" && echo "deleted connector $c"
done
sleep 5
~/venv/bin/python - "$PG" <<'EOF'
import sys
import psycopg
c = psycopg.connect(f"host={sys.argv[1]} user=postgres password=deltaproof dbname=tpcc", autocommit=True)
for (s,) in c.execute("SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'dp_%'").fetchall():
    c.execute("SELECT pg_drop_replication_slot(%s)", (s,))
    print("dropped slot", s)
for (p,) in c.execute("SELECT pubname FROM pg_publication WHERE pubname LIKE 'dp_%'").fetchall():
    c.execute(f"DROP PUBLICATION {p}")
    print("dropped publication", p)
EOF
