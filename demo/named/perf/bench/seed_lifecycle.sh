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
# The seed job (../METHODOLOGY.md), from the operator's machine: seeding is its own job, run once
# per seed hash, and every run boots its backends device from the image it stores. terraform apply
# (terraform/perf) with the backends device alone (seed_mode seed), wait until its ready marker
# names the commit (its startup script has brought the data stack up and seeded it), stop the
# device, store its boot disk as the image provisa-perf-seed-<first 12 of the seed hash>
# (seed_inputs.py at the commit), labelled seed_hash=<the hash's first 63 characters, the most a
# GCE label value holds>, then terraform destroy. The image is not a terraform resource, so destroy
# keeps it. Destroy runs on failure and on interrupt too. An image already stored for the hash is
# said so, and nothing is created.
#
# Usage:
#   seed_lifecycle.sh --project P --zone Z --commit SHA --ssh-cidr CIDR
#                     [--backends-provisioning-model SPOT|ON_DEMAND] [--machine-type TYPE]
#                     [--disk-gb GB] [--ready-timeout 7200]
#   --machine-type  the backends device's (terraform/perf's default otherwise).
#   --disk-gb       its boot disk, GB (terraform/perf's default otherwise): the image is this size,
#                   so a run's devices take at least this.
#   --backends-provisioning-model  the device's provisioning (terraform/perf's provisioning_model
#                   otherwise, Spot).
#
# The commit must be on GitHub: the device clones it from there.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/../../../.." && pwd)"
MODULE="$REPO_ROOT/terraform/perf"
REPO_DIR=/opt/provisa  # terraform/perf's repo_dir default: where the device keeps the repository

PROJECT="" ZONE="" COMMIT="" SSH_CIDR="" BACKENDS_MODEL="" MACHINE_TYPE="" DISK_GB=""
READY_TIMEOUT=7200
while [ "$#" -gt 0 ]; do
  case "$1" in
    --project) PROJECT="$2"; shift 2 ;;
    --zone) ZONE="$2"; shift 2 ;;
    --commit) COMMIT="$2"; shift 2 ;;
    --ssh-cidr) SSH_CIDR="$2"; shift 2 ;;
    --backends-provisioning-model) BACKENDS_MODEL="$2"; shift 2 ;;
    --machine-type) MACHINE_TYPE="$2"; shift 2 ;;
    --disk-gb) DISK_GB="$2"; shift 2 ;;
    --ready-timeout) READY_TIMEOUT="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 2 ;;
  esac
done
for v in PROJECT ZONE COMMIT SSH_CIDR; do
  [ -n "${!v}" ] || { echo "--$(echo "$v" | tr 'A-Z_' 'a-z-') is required"; exit 2; }
done
REGION="${ZONE%-*}"
DEVICES="backends"
# shellcheck source=lifecycle_common.sh
. "$HERE/lifecycle_common.sh"

seed_image_for "$REPO_ROOT" "$HERE"
if [ "$(seed_image_state)" = present ]; then
  echo "seed image $SEED_IMAGE for seed hash $SEED_HASH already exists; nothing to do"
  exit 0
fi

# The job keeps its terraform state and setup log in a directory of its own, outside the repository.
RUN_DIR="${PROVISA_BENCH_RUNS:-$HOME/.provisa-bench/runs}/seed-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN_DIR/tf"
cp "$MODULE"/*.tf "$MODULE"/*.tftpl "$RUN_DIR/tf/"
TF=(terraform -chdir="$RUN_DIR/tf")
VARS=(-var "project=$PROJECT" -var "region=$REGION" -var "zone=$ZONE" -var "commit=$COMMIT"
  -var "operator_ssh_cidrs=[\"$SSH_CIDR\"]" -var 'devices=["backends"]' -var "seed_mode=seed")
[ -z "$BACKENDS_MODEL" ] || VARS+=(-var "backends_provisioning_model=$BACKENDS_MODEL")
[ -z "$MACHINE_TYPE" ] || VARS+=(-var "backends_machine_type=$MACHINE_TYPE")
[ -z "$DISK_GB" ] || VARS+=(-var "disk_gb=$DISK_GB")
"${TF[@]}" init -input=false -backend=false >/dev/null
echo "run directory: $RUN_DIR"
echo "seeding for seed hash $SEED_HASH (image $SEED_IMAGE)"

destroy() {
  copy_journals
  echo "=== terraform destroy ==="
  "${TF[@]}" destroy -input=false -auto-approve "${VARS[@]}"
}
trap destroy EXIT

echo "=== terraform apply (backends device alone) ==="
"${TF[@]}" apply -input=false -auto-approve "${VARS[@]}"
wait_ready

# The image is taken from a stopped device whose stores were stopped cleanly first, so every
# store's files are at rest on its disk.
echo "=== storing $SEED_IMAGE ==="
gcloud compute ssh provisa-perf-backends --zone "$ZONE" --project "$PROJECT" --quiet \
    --command "cd $REPO_DIR/demo/named/perf && sudo docker compose --profile three-device stop" \
  || { echo "provisa-perf-backends: docker compose stop failed; no image stored"; exit 1; }
gcloud compute instances stop provisa-perf-backends --zone "$ZONE" --project "$PROJECT"
gcloud compute images create "$SEED_IMAGE" --project "$PROJECT" \
  --source-disk provisa-perf-backends --source-disk-zone "$ZONE" \
  --labels "seed_hash=${SEED_HASH:0:63}"
echo "seed image $SEED_IMAGE stored (seed hash $SEED_HASH)"
