#!/bin/sh
# On a cluster VM: upload a results directory and the series log to Cloud Storage, so results survive the VM
# deleting itself even if the workstation that would collect them is asleep. Uses the VM's own credentials
# (created with storage read-write scope) through the metadata server; no gcloud needed on the VM.
#   sh ~/deltaproof/infra/gcp/upload_results.sh cluster ~/series.log
set -eu
SUB=$1
LOG=${2:-}
BUCKET=$(curl -sf -H "Metadata-Flavor: Google" \
  "http://metadata.google.internal/computeMetadata/v1/instance/attributes/results-bucket")
NAME=$(hostname)-$(date -u +%Y%m%dT%H%M%SZ)-$SUB.tgz
TMP=$(mktemp -d)
cp -r "$HOME/deltaproof/results/$SUB" "$TMP/$SUB"
[ -n "$LOG" ] && cp "$LOG" "$TMP/$SUB/series.log"
tar -C "$TMP" -czf "$TMP/out.tgz" "$SUB"
TOKEN=$(curl -sf -H "Metadata-Flavor: Google" \
  "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token" |
  python3 -c 'import json, sys; print(json.load(sys.stdin)["access_token"])')
curl -sf -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/gzip" --data-binary "@$TMP/out.tgz" \
  "https://storage.googleapis.com/upload/storage/v1/b/$BUCKET/o?uploadType=media&name=$NAME" >/dev/null
echo "uploaded gs://$BUCKET/$NAME"
rm -rf "$TMP"
