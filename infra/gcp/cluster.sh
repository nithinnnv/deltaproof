#!/bin/sh
# Multi-machine cluster for the live experiments on Google Cloud.
#
#   infra/gcp/cluster.sh up        # create VMs (Docker installed at boot), wait until ready
#   infra/gcp/cluster.sh status    # list cluster VMs, internal IPs and state
#   infra/gcp/cluster.sh down      # delete every VM and disk with label deltaproof=cluster
#
# Environment: ZONE (us-central1-a), SPOT=1 for Spot VMs (batch work only; live latency runs use
# regular VMs so a reclaimed node cannot pass for a fault), and per-role machine types below.
set -eu
ZONE=${ZONE:-us-central1-a}
SPOT=${SPOT:-0}
PG_TYPE=${PG_TYPE:-n2-standard-8}
CASS_TYPE=${CASS_TYPE:-n2-standard-8}
KAFKA_TYPE=${KAFKA_TYPE:-n2-standard-4}
APP_TYPE=${APP_TYPE:-n2-standard-8}
CASS_NODES=${CASS_NODES:-3}
LABEL=deltaproof=cluster
# Results bucket: VMs upload their results here when a series ends (infra/gcp/upload_results.sh), so they
# survive the VMs deleting themselves even if the workstation that collects them is asleep.
BUCKET=${BUCKET:-$(gcloud config get-value project 2>/dev/null)-deltaproof-results}
STARTUP='#!/bin/sh
set -e
if ! command -v docker >/dev/null; then
  apt-get update -q && apt-get install -yq docker.io git python3-venv python3-pip openjdk-17-jre-headless >/dev/null
  systemctl enable --now docker
fi
sysctl -w vm.max_map_count=1048575 >/dev/null
touch /var/run/deltaproof-ready'

vms() {
  echo "dp-pg:$PG_TYPE:200 dp-kafka:$KAFKA_TYPE:100 dp-app:$APP_TYPE:100"
  i=1
  while [ "$i" -le "$CASS_NODES" ]; do printf ' dp-cass-%s:%s:200' "$i" "$CASS_TYPE"; i=$((i + 1)); done
  echo
}

up() {
  gcloud storage buckets describe "gs://$BUCKET" >/dev/null 2>&1 ||
    gcloud storage buckets create "gs://$BUCKET" --location us-central1 --uniform-bucket-level-access >/dev/null
  # Regular VMs delete themselves after MAX_RUN (default 8h) even if nobody runs `down`; Google does
  # not allow adding this to a running VM. Spot VMs stop instead when reclaimed, so batch jobs resume.
  spot="--max-run-duration=${MAX_RUN:-8h} --instance-termination-action=DELETE"
  [ "$SPOT" = 1 ] && spot="--provisioning-model=SPOT --instance-termination-action=STOP"
  for spec in $(vms); do
    name=${spec%%:*}; rest=${spec#*:}; type=${rest%%:*}; disk=${rest#*:}
    if gcloud compute instances describe "$name" --zone "$ZONE" >/dev/null 2>&1; then
      echo "$name exists"; continue
    fi
    # shellcheck disable=SC2086
    gcloud compute instances create "$name" --zone "$ZONE" --machine-type "$type" $spot \
      --image-family debian-12 --image-project debian-cloud \
      --boot-disk-size "${disk}GB" --boot-disk-type pd-ssd \
      --labels "$LABEL" --scopes default,storage-rw \
      --metadata startup-script="$STARTUP",results-bucket="$BUCKET" --format "value(name)" &
  done
  wait
  for spec in $(vms); do
    name=${spec%%:*}
    printf 'waiting for %s ' "$name"
    until gcloud compute ssh "$name" --zone "$ZONE" --quiet --command "test -f /var/run/deltaproof-ready" >/dev/null 2>&1; do
      printf .; sleep 10
    done
    echo " ready"
  done
  status
}

status() {
  gcloud compute instances list --filter "labels.deltaproof=cluster" \
    --format "table(name,machineType.basename(),networkInterfaces[0].networkIP,status,scheduling.provisioningModel)"
}

down() {
  names=$(gcloud compute instances list --filter "labels.deltaproof=cluster" --format "value(name)")
  if [ -n "$names" ]; then
    # shellcheck disable=SC2086
    gcloud compute instances delete $names --zone "$ZONE" --quiet --delete-disks all
  fi
  left=$(gcloud compute instances list --filter "labels.deltaproof=cluster" --format "value(name)" | wc -l)
  disks=$(gcloud compute disks list --filter "labels.deltaproof=cluster OR name~^dp-" --format "value(name)" | wc -l)
  echo "cluster VMs left: $left, disks left: $disks"
}

case "${1:-}" in
  up) up ;;
  status) status ;;
  down) down ;;
  *) echo "usage: $0 up|status|down" >&2; exit 2 ;;
esac
