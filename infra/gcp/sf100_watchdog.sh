#!/bin/sh
# Collect the SF100 result and delete the VM (and its disk) when the job finishes, or at a deadline.
#   infra/gcp/sf100_watchdog.sh 300        # deadline in minutes
ZONE=${ZONE:-us-central1-a}
DEADLINE_MIN=${1:-300}
HERE=$(cd "$(dirname "$0")/../.." && pwd)
end=$(( $(date +%s) + DEADLINE_MIN * 60 ))
log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $*"; }
log "sf100 watchdog started; deadline in $DEADLINE_MIN min"
while :; do
  if gcloud compute ssh dp-sf100 --zone "$ZONE" --quiet --command "grep -q 'SF100 DONE' ~/sf100.log" >/dev/null 2>&1; then
    log "job finished"; break
  fi
  if [ "$(date +%s)" -ge "$end" ]; then log "deadline reached"; break; fi
  if ! gcloud compute instances describe dp-sf100 --zone "$ZONE" >/dev/null 2>&1; then log "VM gone"; exit 1; fi
  sleep 120
done
gcloud compute scp "dp-sf100:~/deltaproof/results/tpch_scale_sf100.csv" "dp-sf100:~/sf100.log" "$HERE/results/" \
  --zone "$ZONE" --quiet >/dev/null 2>&1 && log "copied: $(ls "$HERE/results/" | grep -E 'sf100')" || log "WARNING: copy failed"
gcloud compute instances delete dp-sf100 --zone "$ZONE" --quiet --delete-disks all >/dev/null 2>&1
log "VMs left: $(gcloud compute instances list --format='value(name)' | wc -l), disks left: $(gcloud compute disks list --format='value(name)' | wc -l)"
