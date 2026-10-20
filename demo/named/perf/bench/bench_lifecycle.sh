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
# (its ready marker names the commit; the backends device's includes the first seed), run the
# repetitions (three_device_run.sh), copy the results back, then terraform destroy. Nothing is
# kept between sessions, so nothing bills between them. Destroy runs on failure and on interrupt
# too, unless --keep is given.
#
# Usage:
#   bench_lifecycle.sh --project P --zone Z --commit SHA --ssh-cidr CIDR
#                      [--provisioning-model SPOT|ON_DEMAND] [--reps 5] [--engine pg]
#                      [--ready-timeout 7200] [--keep] [--dry-run] [--machine-type TYPE]
#                      [--variants ... --caches ... --telemetry ... --steps ... --window-s ...
#                       --warmup-s ...]
#   --machine-type  every device's machine type (terraform/perf's default otherwise); a cheap
#                   stage uses a small one.
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

PROJECT="" ZONE="" COMMIT="" SSH_CIDR="" MODEL=SPOT REPS=5 ENGINE=pg
READY_TIMEOUT=7200 KEEP="" DRY_RUN="" MACHINE_TYPE="" PASS_ARGS=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    --project) PROJECT="$2"; shift 2 ;;
    --zone) ZONE="$2"; shift 2 ;;
    --commit) COMMIT="$2"; shift 2 ;;
    --ssh-cidr) SSH_CIDR="$2"; shift 2 ;;
    --provisioning-model) MODEL="$2"; shift 2 ;;
    --reps) REPS="$2"; shift 2 ;;
    --engine) ENGINE="$2"; shift 2 ;;
    --ready-timeout) READY_TIMEOUT="$2"; shift 2 ;;
    --keep) KEEP=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    --machine-type) MACHINE_TYPE="$2"; shift 2 ;;
    --variants|--caches|--telemetry|--steps|--window-s|--warmup-s) PASS_ARGS+=("$1" "$2"); shift 2 ;;
    *) echo "Unknown option: $1"; exit 2 ;;
  esac
done
for v in PROJECT ZONE COMMIT SSH_CIDR; do
  [ -n "${!v}" ] || { echo "--$(echo "$v" | tr 'A-Z_' 'a-z-') is required"; exit 2; }
done
REGION="${ZONE%-*}"

# Each run keeps its terraform state and results in a directory of its own, outside the repository.
RUN_DIR="${PROVISA_BENCH_RUNS:-$HOME/.provisa-bench/runs}/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN_DIR/tf"
cp "$MODULE"/*.tf "$MODULE"/*.tftpl "$RUN_DIR/tf/"
TF=(terraform -chdir="$RUN_DIR/tf")
VARS=(-var "project=$PROJECT" -var "region=$REGION" -var "zone=$ZONE" -var "commit=$COMMIT"
  -var "operator_ssh_cidrs=[\"$SSH_CIDR\"]" -var "provisioning_model=$MODEL")
if [ -n "$MACHINE_TYPE" ]; then
  VARS+=(-var "server_machine_type=$MACHINE_TYPE" -var "client_machine_type=$MACHINE_TYPE"
    -var "backends_machine_type=$MACHINE_TYPE")
fi
"${TF[@]}" init -input=false -backend=false >/dev/null
echo "run directory: $RUN_DIR"

if [ -n "$DRY_RUN" ]; then
  "${TF[@]}" plan -input=false "${VARS[@]}"
  PERF_ZONE="$ZONE" PERF_REPO="$REPO_DIR" PERF_SERVER_IP="<server private ip>" \
    PERF_BACKENDS_IP="<backends private ip>" PERF_LOCAL_OUT="$RUN_DIR/results" \
    bash "$HERE/three_device_run.sh" --reps "$REPS" --engine "$ENGINE" ${PASS_ARGS[@]+"${PASS_ARGS[@]}"} --dry-run
  exit 0
fi

destroy() {
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
SERVER_IP="$(ip server)" BACKENDS_IP="$(ip backends)"

# Every device's startup script writes the commit to its ready marker when every step has passed
# (on the backends device, after the first seed).
echo "=== waiting for the devices to be ready (up to ${READY_TIMEOUT}s) ==="
deadline=$(( $(date +%s) + READY_TIMEOUT ))
for device in server client backends; do
  until [ "$(gcloud compute ssh "provisa-perf-$device" --zone "$ZONE" --project "$PROJECT" --quiet \
      --command 'cat /var/lib/provisa-perf/ready 2>/dev/null' 2>/dev/null)" = "$COMMIT" ]; do
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "device provisa-perf-$device was not ready at $COMMIT within ${READY_TIMEOUT}s;" \
        "its setup log: gcloud compute ssh provisa-perf-$device -- journalctl -t provisa-perf-startup"
      exit 1
    fi
    sleep 30
  done
  echo "provisa-perf-$device ready"
done

CLOUDSDK_CORE_PROJECT="$PROJECT" PERF_ZONE="$ZONE" PERF_REPO="$REPO_DIR" \
  PERF_SERVER_IP="$SERVER_IP" PERF_BACKENDS_IP="$BACKENDS_IP" PERF_LOCAL_OUT="$RUN_DIR/results" \
  bash "$HERE/three_device_run.sh" --reps "$REPS" --engine "$ENGINE" ${PASS_ARGS[@]+"${PASS_ARGS[@]}"}
echo "results: $RUN_DIR/results-report (published.json, internal.json)"
