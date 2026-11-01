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
# (its ready marker names the commit), run the units (three_device_run.sh), copy the results back,
# then terraform destroy. Nothing is kept between sessions but the seed image, so nothing else bills
# between them. Destroy runs on failure and on interrupt too, unless --keep is given, with one
# exception: a run that stops on its own (an invalid unit, a flag with no explanation, a failed
# step) stops its devices and keeps them, disks and state, for --resume.
#
# The backends device boots from the stored seed image (seed_lifecycle.sh) named after the seed
# hash at the commit (seed_inputs.py); without it the run stops before any apply. Each finished run
# is checkpointed to the run's bucket (three_device_run.sh). A run that fails while a device is
# stopped (a reclaimed Spot device) is recovered: the stopped devices are started again (or, if
# that fails, every device is replaced from its image, the bucket kept) and the units not yet
# checkpointed are repeated, at most 3 times. A device that is gone (Spot with a self-destruct is
# deleted when reclaimed) ends the run: a replaced machine is a new run, and starting one again is
# the maintainer's decision. A failure with every device running is not retried.
#
# Usage:
#   bench_lifecycle.sh --project P --zone Z --commit SHA --ssh-cidr CIDR
#                      [--provisioning-model SPOT|ON_DEMAND]
#                      [--backends-provisioning-model SPOT|ON_DEMAND] [--reps 5] [--engine pg]
#                      [--ready-timeout 7200] [--keep] [--dry-run] [--machine-type TYPE]
#                      [--backends-machine-type TYPE] [--disk-gb GB]
#                      [--variants ... --caches ... --telemetry ... --steps ... --window-s ...
#                       --warmup-s ...] [--profile] [--transports a,b] [--smoke]
#                      [--baseline-ledger FILE] [--stop-after UNIT] [--max-run-hours 24]
#                      [--second-server] [--resume RUN_ID --fix TEXT] [--abandon RUN_ID --fix TEXT]
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
#   --profile   ends with a py-spy profile per published transport (three_device_run.sh).
#   --transports, --smoke, --baseline-ledger, --stop-after  three_device_run.sh's.
#   --max-run-hours  every device's self-destruct: GCE deletes it, disk and all, this long after it
#                   starts. A stage sets it long enough for the stage.
#   --second-server  also creates server2 for the kill-a-node check (chaos_check.py).
#   --resume    continues a stopped run from where it stopped: its devices, state and checkpoints,
#               at --commit, which may be a fix's. --fix says what was fixed; the ledger records
#               it, and the run's report states every commit. The seed image must be the same.
#   --abandon   ends a stopped run for a restart from the beginning, which the maintainer decides:
#               the ledger records the fix and the restart, and the devices are destroyed.
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
MAX_RUN_HOURS=24 RESUME="" ABANDON="" FIX="" SECOND_SERVER=""
MAX_RECOVERIES=3
ORIGINAL_ARGS=("$@")
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
    --profile|--smoke) PASS_ARGS+=("$1"); shift ;;
    --transports|--baseline-ledger|--stop-after) PASS_ARGS+=("$1" "$2"); shift 2 ;;
    --max-run-hours) MAX_RUN_HOURS="$2"; shift 2 ;;
    --second-server) SECOND_SERVER=1; shift ;;
    --resume) RESUME="$2"; shift 2 ;;
    --abandon) ABANDON="$2"; shift 2 ;;
    --fix) FIX="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 2 ;;
  esac
done
for v in PROJECT ZONE COMMIT SSH_CIDR; do
  [ -n "${!v}" ] || { echo "--$(echo "$v" | tr 'A-Z_' 'a-z-') is required"; exit 2; }
done
REGION="${ZONE%-*}"
if [ -n "$RESUME$ABANDON" ] && [ -z "$FIX" ]; then
  echo "--resume and --abandon need --fix: what stopped the run, and what was done"
  exit 2
fi
DEVICES="server client backends"
TF_DEVICES='["server","client","backends"]'
if [ -n "$SECOND_SERVER" ]; then
  DEVICES="$DEVICES server2"
  TF_DEVICES='["server","client","backends","server2"]'
fi
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
# Its name is the run id, the same across recoveries and resumes; the checkpoint bucket is named
# after it.
RUN_ID="${RESUME:-${ABANDON:-$(date -u +%Y%m%dT%H%M%SZ)}}"
RUN_DIR="${PROVISA_BENCH_RUNS:-$HOME/.provisa-bench/runs}/$RUN_ID"
LEDGER="$RUN_DIR/results-ledger.json"
BUCKET="provisa-perf-$(echo "$RUN_ID" | tr 'A-Z' 'a-z')-$PROJECT"
[ "${#BUCKET}" -le 63 ] || { echo "bucket name $BUCKET is longer than 63 characters"; exit 2; }
if [ -n "$RESUME$ABANDON" ]; then
  [ -f "$RUN_DIR/seed-image" ] || { echo "$RUN_DIR is not a stopped run"; exit 2; }
  if [ "$(cat "$RUN_DIR/seed-image")" != "$SEED_IMAGE" ]; then
    echo "$RUN_ID ran on $(cat "$RUN_DIR/seed-image"), and $COMMIT seeds $SEED_IMAGE: the data" \
      "changed, so this is a restart from the beginning, not a resume"
    exit 1
  fi
fi
mkdir -p "$RUN_DIR/tf"
echo "$SEED_IMAGE" > "$RUN_DIR/seed-image"
cp "$MODULE"/*.tf "$MODULE"/*.tftpl "$RUN_DIR/tf/"
TF=(terraform -chdir="$RUN_DIR/tf")
VARS=(-var "project=$PROJECT" -var "region=$REGION" -var "zone=$ZONE" -var "commit=$COMMIT"
  -var "operator_ssh_cidrs=[\"$SSH_CIDR\"]" -var "provisioning_model=$MODEL"
  -var "seed_mode=from_image" -var "backends_image=projects/$PROJECT/global/images/$SEED_IMAGE"
  -var "results_bucket=$BUCKET" -var "max_run_hours=$MAX_RUN_HOURS"
  -var "devices=$TF_DEVICES")
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
# before destroy, which deletes the bucket. One process: gsutil -m deadlocks on macOS.
copy_checkpoints() {
  mkdir -p "$RUN_DIR/checkpoints"
  gsutil rsync -r "gs://$BUCKET/$RUN_ID" "$RUN_DIR/checkpoints" \
    || echo "the checkpoints in gs://$BUCKET/$RUN_ID were not copied into $RUN_DIR/checkpoints"
}

units() { "$REPO_ROOT/.venv/bin/python3" "$HERE/bench_units.py" "$@"; }

# A stopped run keeps its devices stopped, so its figures stay on one machine: its disks bill, and
# nothing else, until --resume or --abandon.
STOPPED_FOR_RESUME=""
stop_for_resume() {
  local device
  for device in $DEVICES; do
    gcloud compute instances stop "provisa-perf-$device" --zone "$ZONE" --project "$PROJECT" \
      || echo "provisa-perf-$device did not stop"
  done
  echo "the run stopped. Its devices are stopped, their disks kept. Fix it, then:"
  echo "  bench_lifecycle.sh ${ORIGINAL_ARGS[*]} --resume $RUN_ID --fix '<what was done>'"
  echo "or, for a restart from the beginning (the maintainer's decision):"
  echo "  bench_lifecycle.sh ${ORIGINAL_ARGS[*]} --abandon $RUN_ID --fix '<what was done>'"
}

destroy() {
  copy_journals
  copy_checkpoints
  if [ -n "$STOPPED_FOR_RESUME" ]; then
    stop_for_resume
    return
  fi
  if [ -n "$KEEP" ]; then
    echo "--keep: the devices are left up; state in $RUN_DIR/tf"
    return
  fi
  echo "=== terraform destroy ==="
  "${TF[@]}" destroy -input=false -auto-approve "${VARS[@]}"
}
trap destroy EXIT

if [ -n "$ABANDON" ]; then
  units resolve --ledger "$LEDGER" --fix "$FIX" --commit "$COMMIT" --decision restarted
  echo "$ABANDON is abandoned for a restart: its ledger records why"
  exit 0
fi

echo "=== terraform apply ($MODEL) ==="
"${TF[@]}" apply -input=false -auto-approve "${VARS[@]}"
ip() { "${TF[@]}" output -json private_ip | python3 -c "import json,sys; print(json.load(sys.stdin)['$1'])"; }
read_ips() { SERVER_IP="$(ip server)" BACKENDS_IP="$(ip backends)"; }
read_ips
if [ -n "$RESUME" ]; then
  # shellcheck disable=SC2086  # one instance name per word
  gcloud compute instances start $(for d in $DEVICES; do echo "provisa-perf-$d"; done) \
    --zone "$ZONE" --project "$PROJECT"
fi
wait_ready
if [ -n "$RESUME" ]; then
  units resolve --ledger "$LEDGER" --fix "$FIX" --commit "$COMMIT" --decision resumed
fi

run_bench() {
  CLOUDSDK_CORE_PROJECT="$PROJECT" PERF_ZONE="$ZONE" PERF_REPO="$REPO_DIR" \
    PERF_SERVER_IP="$SERVER_IP" PERF_BACKENDS_IP="$BACKENDS_IP" PERF_LOCAL_OUT="$RUN_DIR/results" \
    PERF_BUCKET="$BUCKET" PERF_RUN_ID="$RUN_ID" \
    bash "$HERE/three_device_run.sh" --reps "$REPS" --engine "$ENGINE" ${PASS_ARGS[@]+"${PASS_ARGS[@]}"}
}

# The devices stopped (a reclaimed Spot device stops, its disk kept), by instance name; a device
# gone is named with gone: in front.
stopped_devices() {
  local device status
  for device in $DEVICES; do
    if ! status="$(gcloud compute instances describe "provisa-perf-$device" --zone "$ZONE" \
        --project "$PROJECT" --format='value(status)' 2>&1)"; then
      case "$status" in
        *"was not found"*) printf 'gone:provisa-perf-%s ' "$device"; continue ;;
      esac
      echo "provisa-perf-$device: its status could not be read: $status" >&2
      return 1
    fi
    case "$status" in
      STOPPED|TERMINATED) printf 'provisa-perf-%s ' "$device" ;;
    esac
  done
}

recoveries=0
rc=0
until run_bench; do
  rc=$?
  if [ "$rc" -eq 3 ]; then
    STOPPED_FOR_RESUME=1
    exit 3
  fi
  stopped="$(stopped_devices)"
  if [ -z "$stopped" ]; then
    echo "the run failed with every device running: not a preemption, so it is not retried"
    units stop --ledger "$LEDGER" --unit "-" --reason "a step failed with every device running"
    STOPPED_FOR_RESUME=1
    exit 1
  fi
  case "$stopped" in
    *gone:*)
      echo "a device is gone ($stopped): a replaced machine is a new run, and a restart is the" \
        "maintainer's decision"
      units stop --ledger "$LEDGER" --unit "-" --reason "device gone: $stopped"
      exit 1
      ;;
  esac
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
echo "results: $RUN_DIR/results-report (published.json or variants.json, with internal.json)"
