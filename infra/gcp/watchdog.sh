#!/bin/sh
# Collect results and tear the cluster down when the series finishes, or at a deadline regardless.
# Runs on the workstation, independent of any interactive session:
#   nohup caffeinate -i infra/gcp/watchdog.sh 180 > watchdog.log 2>&1 &
# Argument: deadline in minutes (default 180).
ZONE=${ZONE:-us-central1-a}
DEADLINE_MIN=${1:-180}
HERE=$(cd "$(dirname "$0")/../.." && pwd)
OUT="$HERE/results/cluster"
end=$(( $(date +%s) + DEADLINE_MIN * 60 ))
log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $*"; }

collect() {
  mkdir -p "$OUT"
  gcloud compute ssh dp-app --zone "$ZONE" --quiet --command \
    "tar -C ~/deltaproof/results -czf /tmp/cluster-results.tgz cluster && cp ~/series.log /tmp/series.log" >/dev/null 2>&1 &&
  gcloud compute scp "dp-app:/tmp/cluster-results.tgz" "dp-app:/tmp/series.log" "$OUT/" --zone "$ZONE" --quiet >/dev/null 2>&1 &&
  tar -C "$HERE/results" -xzf "$OUT/cluster-results.tgz" && rm -f "$OUT/cluster-results.tgz" &&
  log "results copied to $OUT: $(ls "$OUT" | tr '\n' ' ')"
}

log "watchdog started; deadline in $DEADLINE_MIN min"
while :; do
  if gcloud compute ssh dp-app --zone "$ZONE" --quiet --command "grep -q 'SERIES DONE' ~/series.log" >/dev/null 2>&1; then
    log "series finished"; break
  fi
  if [ "$(date +%s)" -ge "$end" ]; then
    log "deadline reached before the series finished"; break
  fi
  if ! gcloud compute instances describe dp-app --zone "$ZONE" >/dev/null 2>&1; then
    log "dp-app no longer exists; nothing to collect"; break
  fi
  sleep 120
done
collect || log "WARNING: could not copy results"
"$HERE/infra/gcp/cluster.sh" down
log "watchdog finished"
