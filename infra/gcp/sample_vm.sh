#!/bin/sh
# On any cluster VM: sample CPU, disk and (on Cassandra nodes) queues every minute, and upload the log to the results
# bucket every 10 minutes, so a run's resource use survives the VMs being deleted.
M=http://metadata.google.internal/computeMetadata/v1
BUCKET=$(curl -sf -H "Metadata-Flavor: Google" "$M/instance/attributes/results-bucket")
H=$(hostname); F=/tmp/sample-$H.log; n=0
while :; do
  { date -u +%FT%TZ; vmstat 5 2 | tail -1; df -h / | tail -1
    sudo docker ps --format '{{.Names}}' | grep -q '^cass$' && {
      sudo docker exec cass nodetool compactionstats | head -1
      sudo docker exec cass nodetool tpstats | grep -E '^(MutationStage|ReadStage|Native-Transport)'
      sudo docker exec cass nodetool tpstats | grep -E '^MUTATION'; }
    top -b -n1 -w 100 | sed -n '8,14p'; } >> "$F" 2>&1
  n=$((n + 1))
  if [ $((n % 10)) -eq 0 ]; then
    T=$(curl -sf -H "Metadata-Flavor: Google" "$M/instance/service-accounts/default/token" |
        python3 -c 'import json, sys; print(json.load(sys.stdin)["access_token"])')
    curl -sf -X POST -H "Authorization: Bearer $T" -H "Content-Type: text/plain" --data-binary "@$F" \
      "https://storage.googleapis.com/upload/storage/v1/b/$BUCKET/o?uploadType=media&name=samples/$H-$(date -u +%Y%m%dT%H%M%SZ).log" >/dev/null
  fi
  sleep 55
done
