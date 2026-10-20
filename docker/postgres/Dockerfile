# PostgreSQL 16 with the wal2json logical-decoding plugin (change stream for bench/live_e2e.py).
FROM postgres:16.4
RUN apt-get update \
 && apt-get install -y --no-install-recommends postgresql-16-wal2json \
 && rm -rf /var/lib/apt/lists/*
