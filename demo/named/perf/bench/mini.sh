#!/bin/bash
# Mini benchmark (METHODOLOGY.md, "Correctness first"): the query matrix on DuckDB, PostgreSQL and
# Trino at one commit, on one small Spot VM, ROUNDS rounds. Every cell must answer and row counts
# must agree across transports; bench_sanity.py writes each engine's timing flags. Timings are
# internal and never published.
#
#   demo/named/perf/bench/mini.sh [--ref SHA|origin/main] [--rounds N] [--machine n2-standard-8] [--stage-spans]
#
# The VM is created, used and deleted by spot-bench.sh (Spot, 4 h max run, termination DELETE, a
# trap that deletes it on any exit and lists instances and disks afterwards). It is labelled
# job=$JOB_LABEL (default bench-mini) and the date. Results land in $OUT/fetched/roundN/<engine>/<timestamp>.json with
# <engine>-sanity.json and trial-summary.json beside them; the VM's instance id and CPU platform
# are in $OUT/cpu-platform.txt and in every report's host facts.
#
# Two VMs are never compared (bench_sanity.py refuses it). To compare a later run with a committed
# baseline, run bench_sanity.py --report <new> --baseline results/<engine>/baseline-*.json on the
# same VM: it refuses a baseline from another instance and still reports the within-run flags.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
SPOT_BENCH=${SPOT_BENCH:-$HOME/.provisa-agent/lead/spot-template/spot-bench.sh}
PROJECT=${PROJECT:-provisa-test-473}
REF=origin/main; ROUNDS=2; MACHINE=n2-standard-8; STAGE_SPANS=
while [ $# -gt 0 ]; do case $1 in
  --ref) REF=$2; shift 2;; --rounds) ROUNDS=$2; shift 2;; --machine) MACHINE=$2; shift 2;; --stage-spans) STAGE_SPANS=1; shift;;
  *) echo "bad arg $1"; exit 2;; esac; done
DATE=$(date +%Y%m%d)
NAME=provisa-bench-mini-$DATE-$(date +%H%M%S)
OUT=${OUT:-$HOME/provisa-agent-logs/mini-bench-$DATE-$(date +%H%M%S)}
mkdir -p "$OUT"
# ROUNDS reaches the job through its own first line: spot-bench.sh passes only REF.
JOB=$OUT/mini-job.sh
{ echo "ROUNDS=$ROUNDS"; echo "STAGE_SPANS=$STAGE_SPANS"; cat "$HERE/mini-job.sh"; } > "$JOB"

# Label the VM once it exists; spot-bench.sh picks the zone.
(
  for _ in $(seq 1 60); do
    zone=$(gcloud compute instances list --project "$PROJECT" --filter="name=$NAME" \
      --format='value(zone.basename())' 2>/dev/null)
    if [ -n "$zone" ]; then
      gcloud compute instances add-labels "$NAME" --zone "$zone" --project "$PROJECT" \
        --labels="job=${JOB_LABEL:-bench-mini},date=$DATE" > "$OUT/labels.log" 2>&1
      exit 0
    fi
    sleep 10
  done
) &

BENCH_VM_NAME=$NAME "$SPOT_BENCH" script --script "$JOB" --ref "$REF" --machine "$MACHINE" \
  --disk-gb 100 --out "$OUT"
