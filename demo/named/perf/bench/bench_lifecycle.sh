#!/usr/bin/env bash
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
#
# The three-device optimistic benchmark as one command (../METHODOLOGY.md), from the operator's
# machine: terraform apply (terraform/perf), wait until every device's startup script has finished
# (its ready marker names the commit), run the repetitions (three_device_run.sh), copy the results
# back, then terraform destroy. Nothing is kept between sessions but the seed image, so nothing
# else bills between them. Destroy runs on failure and on interrupt too, unless --keep is given.
#
# The backends device boots from the stored seed image (seed_lifecycle.sh) named after the seed
# hash at the commit (seed_inputs.py); without it the run stops before any apply. Each finished run
# is checkpointed to the run's bucket (three_device_run.sh). A run that fails while a device is
# stopped (a reclaimed Spot device) is recovered: the stopped devices are started again (or, if
# that fails, every device is replaced from its image, the bucket kept) and the runs not yet
# checkpointed are repeated, at most 3 times. A failure with every device running is not retried.
#
# Usage:
#   bench_lifecycle.sh --project P --zone Z --commit SHA --ssh-cidr CIDR
#                      [--provisioning-model SPOT|ON_DEMAND]
#                      [--backends-provisioning-model SPOT|ON_DEMAND] [--reps 5] [--engine pg]
#                      [--ready-timeout 7200] [--keep] [--dry-run] [--machine-type TYPE]
#                      [--backends-machine-type TYPE] [--disk-gb GB]
#                      [--variants ... --caches ... --telemetry ... --steps ... --window-s ...
#                       --warmup-s ...]
#   --machine-type  every device's machine type (terraform/perf's default otherwise); a cheap
#                   stage uses a small one.
#   --backends-machine-type  the backends device's machine type alone (after --machine-type):
#                   its stores are sized for a large box (shared_buffers), so a cheap stage gives it
#                   the memory they need and keeps the others small.
#   --disk-gb       every device's boot disk, GB (terraform/perf's default otherwise): the three
#                   share the region's Hyperdisk quota. At least the seed image's (seed_lifecycle.sh).
#   --backends-provisioning-model  the backends device's alone (the others' otherwise): an
#                   on-demand backends device beside Spot server and client.
#   --variants ...  a governance pass (three_device_run.sh), the six flags together.
#   --dry-run   terraform plan and the run's commands (three_device_run.sh --dry-run); nothing
#               is created.
#   --keep      leave the devices up afterwards (their state stays in the run directory; destroy
#               with `terraform -chdir=<run dir>/tf destroy` and the same variables).
#
# The commit must be on GitHub: the devices clone it from there.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/../../../.." && pwd)"
MODULE="$REPO_ROOT/terraform/perf"
REPO_DIR=/opt/provisa  # terraform/perf's repo_dir default: PERF_REPO on every device

PROJECT="" ZONE="" COMMIT="" SSH_CIDR="" MODEL=SPOT BACKENDS_MODEL="" REPS=5 ENGINE=pg
READY_TIMEOUT=7200 KEEP="" DRY_RUN="" MACHINE_TYPE="" BACKENDS_MACHINE_TYPE="" DISK_GB="" PASS_ARGS=()
MAX_RECOVERIES=3
while [ "$#" -gt 0 ]; do
  case "$1" in
    --project) PROJECT="$2"; shift 2 ;;
    --zone) ZONE="$2"; shift 2 ;;
    --commit) COMMIT="$2"; shift 2 ;;
    --ssh-cidr) SSH_CIDR="$2"; shift 2 ;;
    --provisioning-model) MODEL="$2"; shift 2 ;;
    --backends-provisioning-model) BACKENDS_MODEL="$2"; shift 2 ;;
    --reps) REPS="$2"; shift 2 ;;
    --engine) ENGINE="$2"; shift 2 ;;
    --ready-timeout) READY_TIMEOUT="$2"; shift 2 ;;
    --keep) KEEP=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    --machine-type) MACHINE_TYPE="$2"; shift 2 ;;
    --backends-machine-type) BACKENDS_MACHINE_TYPE="$2"; shift 2 ;;
    --disk-gb) DISK_GB="$2"; shift 2 ;;
    --variants|--caches|--telemetry|--steps|--window-s|--warmup-s) PASS_ARGS+=("$1" "$2"); shift 2 ;;
    *) echo "Unknown option: $1"; exit 2 ;;
  esac
done
for v in PROJECT ZONE COMMIT SSH_CIDR; do
  [ -n "${!v}" ] || { echo "--$(echo "$v" | tr 'A-Z_' 'a-z-') is required"; exit 2; }
done
REGION="${ZONE%-*}"
DEVICES="server client backends"
# shellcheck source=lifecycle_common.sh
. "$HERE/lifecycle_common.sh"

# The seeded data comes from the stored seed image; a run never seeds. No image for the seed hash
# at this commit means the seed inputs changed since the last seed job.
seed_image_for "$REPO_ROOT" "$HERE"
if [ "$(seed_image_state)" = missing ]; then
  echo "the seed inputs changed: there is no seed image $SEED_IMAGE for seed hash $SEED_HASH;" \
    "store it first with demo/named/perf/bench/seed_lifecycle.sh (same --project, --zone, --commit)"
  exit 1
fi
echo "seed image: $SEED_IMAGE (seed hash $SEED_HASH)"

# Each run keeps its terraform state and results in a directory of its own, outside the repository.
# Its name is the run id, the same across recoveries; the checkpoint bucket is named after it.
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="${PROVISA_BENCH_RUNS:-$HOME/.provisa-bench/runs}/$RUN_ID"
BUCKET="provisa-perf-$(echo "$RUN_ID" | tr 'A-Z' 'a-z')-$PROJECT"
[ "${#BUCKET}" -le 63 ] || { echo "bucket name $BUCKET is longer than 63 characters"; exit 2; }
mkdir -p "$RUN_DIR/tf"
cp "$MODULE"/*.tf "$MODULE"/*.tftpl "$RUN_DIR/tf/"
TF=(terraform -chdir="$RUN_DIR/tf")
VARS=(-var "project=$PROJECT" -var "region=$REGION" -var "zone=$ZONE" -var "commit=$COMMIT"
  -var "operator_ssh_cidrs=[\"$SSH_CIDR\"]" -var "provisioning_model=$MODEL"
  -var "seed_mode=from_image" -var "backends_image=projects/$PROJECT/global/images/$SEED_IMAGE"
  -var "results_bucket=$BUCKET")
[ -z "$BACKENDS_MODEL" ] || VARS+=(-var "backends_provisioning_model=$BACKENDS_MODEL")
if [ -n "$MACHINE_TYPE" ]; then
  VARS+=(-var "server_machine_type=$MACHINE_TYPE" -var "client_machine_type=$MACHINE_TYPE"
    -var "backends_machine_type=$MACHINE_TYPE")
fi
[ -z "$BACKENDS_MACHINE_TYPE" ] || VARS+=(-var "backends_machine_type=$BACKENDS_MACHINE_TYPE")
[ -z "$DISK_GB" ] || VARS+=(-var "disk_gb=$DISK_GB")
"${TF[@]}" init -input=false -backend=false >/dev/null
echo "run directory: $RUN_DIR"

if [ -n "$DRY_RUN" ]; then
  "${TF[@]}" plan -input=false "${VARS[@]}"
  PERF_ZONE="$ZONE" PERF_REPO="$REPO_DIR" PERF_SERVER_IP="<server private ip>" \
    PERF_BACKENDS_IP="<backends private ip>" PERF_LOCAL_OUT="$RUN_DIR/results" \
    PERF_BUCKET="$BUCKET" PERF_RUN_ID="$RUN_ID" \
    bash "$HERE/three_device_run.sh" --reps "$REPS" --engine "$ENGINE" ${PASS_ARGS[@]+"${PASS_ARGS[@]}"} --dry-run
  exit 0
fi

# The bucket's checkpoints (every finished run's results) are copied into the run directory
# before destroy, which deletes the bucket. One process: gsutil's worker processes deadlock on
# macOS, with -m or without it (rsync forks them unless the process count is 1).
copy_checkpoints() {
  mkdir -p "$RUN_DIR/checkpoints"
  gsutil -o "GSUtil:parallel_process_count=1" rsync -r "gs://$BUCKET/$RUN_ID" "$RUN_DIR/checkpoints" \
    || echo "the checkpoints in gs://$BUCKET/$RUN_ID were not copied into $RUN_DIR/checkpoints"
}

destroy() {
  copy_journals
  copy_checkpoints
  if [ -n "$KEEP" ]; then
    echo "--keep: the devices are left up; state in $RUN_DIR/tf"
    return
  fi
  echo "=== terraform destroy ==="
  "${TF[@]}" destroy -input=false -auto-approve "${VARS[@]}"
}
trap destroy EXIT

echo "=== terraform apply ($MODEL) ==="
"${TF[@]}" apply -input=false -auto-approve "${VARS[@]}"
ip() { "${TF[@]}" output -json private_ip | python3 -c "import json,sys; print(json.load(sys.stdin)['$1'])"; }
read_ips() { SERVER_IP="$(ip server)" BACKENDS_IP="$(ip backends)"; }
read_ips
wait_ready

run_bench() {
  CLOUDSDK_CORE_PROJECT="$PROJECT" PERF_ZONE="$ZONE" PERF_REPO="$REPO_DIR" \
    PERF_SERVER_IP="$SERVER_IP" PERF_BACKENDS_IP="$BACKENDS_IP" PERF_LOCAL_OUT="$RUN_DIR/results" \
    PERF_BUCKET="$BUCKET" PERF_RUN_ID="$RUN_ID" \
    bash "$HERE/three_device_run.sh" --reps "$REPS" --engine "$ENGINE" ${PASS_ARGS[@]+"${PASS_ARGS[@]}"}
}

# The devices stopped (a reclaimed Spot device stops, its disk kept), by instance name.
stopped_devices() {
  local device status
  for device in $DEVICES; do
    status="$(gcloud compute instances describe "provisa-perf-$device" --zone "$ZONE" \
      --project "$PROJECT" --format='value(status)')" \
      || { echo "provisa-perf-$device: its status could not be read" >&2; return 1; }
    case "$status" in
      STOPPED|TERMINATED) printf 'provisa-perf-%s ' "$device" ;;
    esac
  done
}

recoveries=0
until run_bench; do
  stopped="$(stopped_devices)"
  if [ -z "$stopped" ]; then
    echo "the run failed with every device running: not a preemption, so it is not retried"
    exit 1
  fi
  if [ "$recoveries" -ge "$MAX_RECOVERIES" ]; then
    echo "the run failed again after $MAX_RECOVERIES recoveries ($stopped stopped); giving up"
    exit 1
  fi
  recoveries=$((recoveries + 1))
  echo "=== recovery $recoveries of $MAX_RECOVERIES: $stopped stopped ==="
  # shellcheck disable=SC2086  # one instance name per word
  if ! gcloud compute instances start $stopped --zone "$ZONE" --project "$PROJECT"; then
    # The devices alone are replaced (each from its image, the backends device from the seed
    # image); the bucket, and the runs checkpointed in it, are kept.
    echo "the stopped devices did not start; replacing every device"
    "${TF[@]}" destroy -input=false -auto-approve -target=google_compute_instance.device "${VARS[@]}"
    "${TF[@]}" apply -input=false -auto-approve "${VARS[@]}"
    read_ips
  fi
  wait_ready
done
echo "results: $RUN_DIR/results-report (published.json, internal.json)"
