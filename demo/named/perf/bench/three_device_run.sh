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
# The three-device optimistic benchmark (../METHODOLOGY.md), driven from the operator's machine over
# `gcloud compute ssh`: device 3 brought up once, then each repetition runs telemetry off and on in
# turn (off, on, off, on, ...), Provisa restarted on device 1 for every run, the load generated on
# device 2. Each run's results are copied back as soon as it finishes and checkpointed to the run's
# bucket (gs://PERF_BUCKET/PERF_RUN_ID/<run>/, its DONE object written last); at the end they are
# reported (three_device.py report).
#
# A rerun with the same bucket and run id (bench_lifecycle.sh, after a preempted device is
# recovered) skips every run whose DONE object is there, so only the runs not done are repeated.
#
# The devices are the ones terraform/perf creates. The repository is checked out at the commit
# under test on all three, at the same path.
#
# Usage: three_device_run.sh [--reps 5] [--engine pg] [--dry-run]
#        [--variants zero,row_rules_2,... --caches cached,uncached --telemetry off
#         --steps 1,16 --window-s 15 --warmup-s 15]
#   --dry-run prints every command it would run on each device, and runs none.
#   --variants runs a governance pass (REQ-2092) instead: per repetition, every variant
#   (governance_variants.py) at every cache setting and telemetry setting, each its own run
#   (<rep>-<variant>-<cache>-<telemetry>), at the load the last four flags give; the report is
#   three_device.py variant-report. All six go together.
# Needs:
#   PERF_ZONE                    the devices' zone
#   PERF_REPO                    the repository's path on every device
#   PERF_SERVER_IP, PERF_BACKENDS_IP   private addresses (terraform output private_ip)
#   PERF_BUCKET                  the run's checkpoint bucket (terraform output results_bucket)
#   PERF_RUN_ID                  the run id, the bucket prefix: the same across recoveries
#   PERF_CONTRACT                the setup contract, a path on device 2 relative to the bench dir
#                                (default setups/three-device.yaml: its endpoints name device 1
#                                through PROVISA_BENCH_SERVER_HOST, set here to PERF_SERVER_IP)
set -euo pipefail

REPS=5
ENGINE=pg
DRY_RUN=""
VARIANTS="" CACHES="" TELEMETRIES="" STEPS="" WINDOW_S="" WARMUP_S=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --reps) REPS="$2"; shift 2 ;;
    --engine) ENGINE="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --variants) VARIANTS="$2"; shift 2 ;;
    --caches) CACHES="$2"; shift 2 ;;
    --telemetry) TELEMETRIES="$2"; shift 2 ;;
    --steps) STEPS="$2"; shift 2 ;;
    --window-s) WINDOW_S="$2"; shift 2 ;;
    --warmup-s) WARMUP_S="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 2 ;;
  esac
done
PASS="$VARIANTS$CACHES$TELEMETRIES$STEPS$WINDOW_S$WARMUP_S"
if [ -n "$PASS" ]; then
  for v in VARIANTS CACHES TELEMETRIES STEPS WINDOW_S WARMUP_S; do
    [ -n "${!v}" ] || { echo "a governance pass needs --variants, --caches, --telemetry, --steps, --window-s and --warmup-s"; exit 2; }
  done
fi
: "${PERF_ZONE:?}" "${PERF_REPO:?}" "${PERF_SERVER_IP:?}" "${PERF_BACKENDS_IP:?}"
: "${PERF_BUCKET:?}" "${PERF_RUN_ID:?}"
PERF_CONTRACT="${PERF_CONTRACT:-setups/three-device.yaml}"

SERVER=provisa-perf-server
CLIENT=provisa-perf-client
BACKENDS=provisa-perf-backends
AGENT_PORT=9555
BENCH="$PERF_REPO/demo/named/perf/bench"
PY="$PERF_REPO/.venv/bin/python3"
OUT="results/three-device"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LOCAL_OUT="${PERF_LOCAL_OUT:-$(cd "$(dirname "$0")" && pwd)/results/three-device-$STAMP}"
CHECKPOINTS="gs://$PERF_BUCKET/$PERF_RUN_ID"

on() {  # on <instance> <command>
  if [ -n "$DRY_RUN" ]; then
    printf '\n[%s]\n%s\n' "$1" "$2"
    return 0
  fi
  gcloud compute ssh "$1" --zone "$PERF_ZONE" --quiet --command "$2"
}

# The containers whose CPU each step reports beside the source's (compose project "perf").
BACKEND_CONTAINERS=(perf-engine-postgresql-1 perf-control-plane-1 perf-redis-1
  perf-otel-collector-1 perf-otlp2sql-1 perf-ops-postgresql-1)
[ "$ENGINE" = trino ] && BACKEND_CONTAINERS+=(perf-trino-1)
_containers=""
for c in "${BACKEND_CONTAINERS[@]}"; do _containers+=" --backend-container $c"; done

# The runs already checkpointed: a run is done when its DONE object is in the bucket. An empty
# prefix is the one listing failure that means none is done; any other is an error.
DONE_RUNS=" "
if [ -z "$DRY_RUN" ]; then
  if listing="$(gsutil ls "$CHECKPOINTS/*/DONE" 2>&1)"; then
    for url in $listing; do
      run="${url%/DONE}"
      DONE_RUNS+="${run##*/} "
    done
    echo "already checkpointed in $CHECKPOINTS, skipped:$DONE_RUNS"
  elif [ "$listing" != "CommandException: One or more URLs matched no objects." ]; then
    echo "the checkpoints in $CHECKPOINTS could not be listed: $listing"
    exit 1
  fi
fi

# Each device powers off after 30 idle minutes (vm_idle_shutdown.sh). Device 3 only serves its
# containers during a run, which the idle check does not count, so every device is held up for
# the run and released after it, also when the run fails.
HOLD=/var/lib/provisa-idle/hold
for d in "$BACKENDS" "$SERVER" "$CLIENT"; do on "$d" "sudo touch $HOLD"; done
release() { for d in "$BACKENDS" "$SERVER" "$CLIENT"; do on "$d" "sudo rm -f $HOLD"; done; }
trap release EXIT

echo "=== device 3: backends ==="
_profiles="--profile three-device"
[ "$ENGINE" = trino ] && _profiles+=" --profile three-device-trino"
# The device booted from the stored seed image and its startup script brought the stack up; this
# is a no-op then, and adds Trino when a run names it. The seeder never runs in a run: a device
# without seed.py's done-marker is refused. Docker and the agent's docker stats need root. Whether the agent runs is
# asked of the agent itself: a process match would find the remote shell running this command,
# whose own command line names the agent, and never start it.
on "$BACKENDS" "cd $PERF_REPO/demo/named/perf && export PROVISA_BENCH_BIND=$PERF_BACKENDS_IP \
  && { test -f data/.seeded || { echo 'data/.seeded is absent: this device is not seeded'; exit 1; }; } \
  && sudo -E docker compose $_profiles up -d --build --scale seeder=0 \
  && ( { curl -sf http://$PERF_BACKENDS_IP:$AGENT_PORT/host >/dev/null || sudo nohup setsid $PY $BENCH/host_agent.py \
      --listen $PERF_BACKENDS_IP:$AGENT_PORT --fact device=backends > ~/host-agent.log 2>&1; } >/dev/null 2>&1 </dev/null & )"

run_one() {  # run_one <rep> <telemetry> [<variant> <cache>]
  local name="$1-$2" contract_args=""
  if [ "$#" -eq 4 ]; then
    name="$1-$3-$4-$2"
    contract_args="--variant $3 --cache $4 --steps $STEPS --window-s $WINDOW_S --warmup-s $WARMUP_S"
  fi
  if [[ "$DONE_RUNS" == *" $name "* ]]; then
    echo "=== run $name: checkpointed already, skipped ==="
    return 0
  fi
  echo "=== run $name (engine $ENGINE) ==="
  on "$SERVER" "export PROVISA_BENCH_BACKENDS_HOST=$PERF_BACKENDS_IP \
    PROVISA_BENCH_AGENT_LISTEN=$PERF_SERVER_IP:$AGENT_PORT; \
    nohup setsid env --default-signal=INT \
      $BENCH/orchestrate.sh serve --telemetry $2 --engine $ENGINE > ~/serve-$name.log 2>&1 < /dev/null &"
  # serving once the server's agent answers: serve_three_device.sh starts it after every worker
  # is ready
  on "$CLIENT" "for i in \$(seq 1 210); do curl -sf http://$PERF_SERVER_IP:$AGENT_PORT/host >/dev/null && exit 0; sleep 2; done; exit 1"
  # the contract as this run states it: its telemetry the server's (REQ-2092)
  on "$CLIENT" "cd $BENCH && rm -rf $OUT/$name && mkdir -p $OUT/$name && $PY three_device.py contract --setup $PERF_CONTRACT --telemetry $2 \
    $contract_args --out $OUT/$name/setup.yaml"
  on "$CLIENT" "cd $BENCH && PROVISA_BENCH_SERVER_HOST=$PERF_SERVER_IP \
    PROVISA_HTTP_BASE_URL=http://$PERF_SERVER_IP:8001 \
    $PY run_benchmark.py --engine $ENGINE --optimistic \
    --setup $OUT/$name/setup.yaml \
    --server-cpu-url http://$PERF_SERVER_IP:$AGENT_PORT/cpu \
    --source-cpu-url http://$PERF_BACKENDS_IP:$AGENT_PORT/docker-stats $_containers \
    --device server=http://$PERF_SERVER_IP:$AGENT_PORT \
    --device backends=http://$PERF_BACKENDS_IP:$AGENT_PORT \
    --output-dir $OUT/$name"
  # '[s]erve...': the pattern does not match the remote shell running this command, whose own
  # command line names the script.
  # A job a non-interactive shell starts in the background begins with SIGINT ignored, and bash
  # can neither trap nor reset a signal ignored on entry: the serve is started with SIGINT at its
  # default (env --default-signal=INT above), so it stops on SIGINT as a server does. A stop that
  # does not end in 60 s says what is still running and what the serve last wrote.
  on "$SERVER" "pkill -INT -f '[s]erve_three_device.sh'; for i in \$(seq 1 60); do pgrep -f '[s]erve_three_device.sh' >/dev/null || exit 0; sleep 1; done; \
    echo 'the serve did not stop within 60 s of SIGINT; still running:'; \
    ps -o pid,ppid,etime,stat,cmd -p \$(pgrep -d, -f '[s]erve_three_device.sh|[u]vicorn main:app|[h]ost_agent.py') | cut -c1-200; \
    echo '--- ~/serve-$name.log (tail)'; tail -40 ~/serve-$name.log; exit 1"
  checkpoint "$name"
}

# The run's results copied back, then to the bucket, then its DONE object: a run is done only
# once all of it is in the bucket, so a preemption mid-copy repeats the run.
checkpoint() {  # checkpoint <run>
  local copy=(gcloud compute scp --recurse --zone "$PERF_ZONE" "$CLIENT:$BENCH/$OUT/$1" "$LOCAL_OUT/")
  local upload=(gsutil -m cp -r "$LOCAL_OUT/$1" "$CHECKPOINTS/")
  if [ -n "$DRY_RUN" ]; then
    printf '\n[operator]\n%s\n%s\nprintf %%s %s | gsutil cp - %s\n' \
      "${copy[*]}" "${upload[*]}" "$1" "$CHECKPOINTS/$1/DONE"
    return 0
  fi
  mkdir -p "$LOCAL_OUT"
  rm -rf "${LOCAL_OUT:?}/$1"
  "${copy[@]}"
  "${upload[@]}"
  printf %s "$1" | gsutil cp - "$CHECKPOINTS/$1/DONE"
}

for rep in $(seq 1 "$REPS"); do
  if [ -n "$PASS" ]; then
    for variant in ${VARIANTS//,/ }; do
      for cache in ${CACHES//,/ }; do
        for telemetry in ${TELEMETRIES//,/ }; do
          run_one "$rep" "$telemetry" "$variant" "$cache"
        done
      done
    done
  else
    run_one "$rep" off
    run_one "$rep" on
  fi
done
REPORT=(report --root "$LOCAL_OUT" --out "$LOCAL_OUT-report")
[ -n "$PASS" ] && REPORT=(variant-report --root "$LOCAL_OUT" --out "$LOCAL_OUT-report")

if [ -n "$DRY_RUN" ]; then
  printf '\n[operator]\nthree_device.py %s\n' "${REPORT[*]}"
  exit 0
fi
"$(cd "$(dirname "$0")/../../../.." && pwd)/.venv/bin/python3" "$(dirname "$0")/three_device.py" \
  "${REPORT[@]}"
if [ -n "$PASS" ]; then
  echo "governance pass (internal): $LOCAL_OUT-report/variants.json"
else
  echo "published: $LOCAL_OUT-report/published.json (telemetry off); internal: $LOCAL_OUT-report/internal.json"
fi
