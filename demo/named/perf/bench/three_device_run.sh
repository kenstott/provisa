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
# `gcloud compute ssh`: device 3 brought up once, then the plan's units, least risky first
# (bench_units.py plan). A unit is one transport, one repetition. Provisa is restarted on device 1
# for each repetition and telemetry setting, the load generated on device 2. Each unit's results
# are copied back as soon as it finishes and rechecked (bench_units.py check). A valid one is
# checkpointed to the run's bucket (gs://PERF_BUCKET/PERF_RUN_ID/<unit>/, its DONE object written
# last). An invalid one is discarded with its reason and the run stops, with exit code 3, as it does
# for a sanity flag no note explains. At the end the units are reported (three_device.py report).
#
# A rerun with the same bucket and run id resumes: every unit whose DONE object is there is skipped,
# so only the units not done are run, and only their servers are started.
#
# The devices are the ones terraform/perf creates. The repository is checked out at the commit
# under test on all three, at the same path.
#
# Usage: three_device_run.sh [--reps 5] [--engine pg] [--profile] [--dry-run] [--transports a,b]
#        [--baseline-ledger FILE | --smoke]
#        [--variants zero,row_rules_2,... --caches cached,uncached --telemetry off
#         --steps 1,16 --window-s 15 --warmup-s 15]
#        [--stop-after UNIT]
#   --dry-run prints every command it would run on each device, and runs none.
#   --transports the units' transports (default all ten); the pgwire uncached unit runs regardless.
#   --profile ends the repetitions with one py-spy profile per published transport, at the step
#   they saturate; its runs stay out of the report.
#   --variants runs a governance pass (REQ-2092) instead: per repetition, every variant
#   (governance_variants.py) at every cache setting and telemetry setting, one unit per transport
#   (<rep>-<variant>-<cache>-<transport>-<telemetry>), at the load the last four flags give; the
#   report is three_device.py variant-report. All six go together. A pass needs the baseline passed:
#   --baseline-ledger names that run's ledger. --smoke runs it without one, for a run whose numbers
#   are not published, and says so in its report.
#   --stop-after stops the run with exit code 3 once UNIT is checkpointed: a deliberate abort, for
#   proving a resume.
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
TRANSPORTS="" BASELINE_LEDGER="" SMOKE="" STOP_AFTER=""
PROFILE="" PROFILE_STEP=64 PROFILE_S=30 PROFILE_WARM_S=20 PY_SPY_VERSION=0.4.0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --reps) REPS="$2"; shift 2 ;;
    --engine) ENGINE="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --profile) PROFILE=1; shift ;;
    --variants) VARIANTS="$2"; shift 2 ;;
    --caches) CACHES="$2"; shift 2 ;;
    --telemetry) TELEMETRIES="$2"; shift 2 ;;
    --steps) STEPS="$2"; shift 2 ;;
    --window-s) WINDOW_S="$2"; shift 2 ;;
    --warmup-s) WARMUP_S="$2"; shift 2 ;;
    --transports) TRANSPORTS="$2"; shift 2 ;;
    --baseline-ledger) BASELINE_LEDGER="$2"; shift 2 ;;
    --smoke) SMOKE=1; shift ;;
    --stop-after) STOP_AFTER="$2"; shift 2 ;;
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
LEDGER="$LOCAL_OUT-ledger.json"
CHECKPOINTS="gs://$PERF_BUCKET/$PERF_RUN_ID"
HERE="$(cd "$(dirname "$0")" && pwd)"
LOCAL_PY="$HERE/../../../../.venv/bin/python3"
units() { "$LOCAL_PY" "$HERE/bench_units.py" "$@"; }

# Baseline before features: a pass starts only on a baseline that passed, or as a smoke.
if [ -n "$PASS" ] && [ -z "$SMOKE$DRY_RUN" ]; then
  [ -n "$BASELINE_LEDGER" ] || { echo "a governance pass needs --baseline-ledger or --smoke"; exit 2; }
  units gate --ledger "$BASELINE_LEDGER"
fi

# The plan, every unit in order. Read whole before anything runs: `on` reads stdin.
PLAN_ARGS=(--reps "$REPS")
[ -z "$TRANSPORTS" ] || PLAN_ARGS+=(--transports "$TRANSPORTS")
[ -z "$PASS" ] || PLAN_ARGS+=(--variants "$VARIANTS" --caches "$CACHES" --telemetry "$TELEMETRIES")
[ -z "$PROFILE" ] || PLAN_ARGS+=(--profile)
PLAN=()
while IFS= read -r line; do PLAN+=("$line"); done < <(units plan "${PLAN_ARGS[@]}")

on() {  # on <instance> <command>
  if [ -n "$DRY_RUN" ]; then
    printf '\n[%s]\n%s\n' "$1" "$2"
    return 0
  fi
  gcloud compute ssh "$1" --zone "$PERF_ZONE" --quiet --command "$2"
}

# The containers whose CPU each step reports beside the source's (compose project "perf"): device
# 3's, and the engine's on device 1.
BACKEND_CONTAINERS=(perf-control-plane-1 perf-redis-1
  perf-otel-collector-1 perf-otlp2sql-1 perf-ops-postgresql-1)
_containers=""
for c in "${BACKEND_CONTAINERS[@]}"; do _containers+=" --backend-container $c"; done
ENGINE_CONTAINER=""
[ "$ENGINE" = pg ] && ENGINE_CONTAINER=perf-engine-postgresql-1
[ "$ENGINE" = trino ] && ENGINE_CONTAINER=perf-trino-1
_engine_cpu=""
[ -n "$ENGINE_CONTAINER" ] && _engine_cpu="--engine-container $ENGINE_CONTAINER \
    --engine-cpu-url http://$PERF_SERVER_IP:$AGENT_PORT/docker-stats"

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

# Device 3's services: all of the profile but the engine, which is on device 1.
three_device_services() {
  echo postgresql mongodb clickhouse neo4j redis control-plane otel-collector otlp2sql ops-postgresql
}

# Each device powers off after 30 idle minutes (vm_idle_shutdown.sh). Device 3 only serves its
# containers during a run, which the idle check does not count, so every device is held up for
# the run and released after it, also when the run fails.
HOLD=/var/lib/provisa-idle/hold
for d in "$BACKENDS" "$SERVER" "$CLIENT"; do on "$d" "sudo touch $HOLD"; done
release() { for d in "$BACKENDS" "$SERVER" "$CLIENT"; do on "$d" "sudo rm -f $HOLD"; done; }
trap release EXIT

echo "=== device 3: backends ==="
_profiles="--profile three-device"
# The device booted from the stored seed image and its startup script brought the stack up; this
# is a no-op then, and adds Trino when a run names it. The seeder never runs in a run: the services
# are named and it is not one of them, and a device without seed.py's done-marker is refused. Docker and the agent's docker stats need root. Whether the agent runs is
# asked of the agent itself: a process match would find the remote shell running this command,
# whose own command line names the agent, and never start it.
on "$BACKENDS" "cd $PERF_REPO/demo/named/perf && export PROVISA_BENCH_BIND=$PERF_BACKENDS_IP \
  && { test -f data/.seeded || { echo 'data/.seeded is absent: this device is not seeded'; exit 1; }; } \
  && sudo -E docker compose $_profiles up -d --build $(three_device_services) \
  && { sudo -E docker compose $_profiles stop engine-postgresql || true; } \
  && ( { curl -sf http://$PERF_BACKENDS_IP:$AGENT_PORT/host >/dev/null || sudo nohup setsid $PY $BENCH/host_agent.py \
      --listen $PERF_BACKENDS_IP:$AGENT_PORT --fact device=backends > ~/host-agent.log 2>&1; } >/dev/null 2>&1 </dev/null & )"

# The stores against what the compose states, before any unit: the seed hash says which inputs made
# the image, the counts say the image holds what they make. Once per run id.
SEED_COUNTS="$LOCAL_OUT-seed-counts.txt"
if [ ! -f "$SEED_COUNTS" ]; then
  on "$BACKENDS" "$PY $BENCH/seed_counts.py --compose $PERF_REPO/demo/named/perf/docker-compose.yml \
    --host $PERF_BACKENDS_IP --out ~/seed-counts.json"
  [ -n "$DRY_RUN" ] || echo "counted" > "$SEED_COUNTS"
fi

# The engine runs beside Provisa on device 1, out of the same compose file.
if [ -n "$ENGINE_CONTAINER" ]; then
  echo "=== device 1: engine ==="
  _engine_service=engine-postgresql
  [ "$ENGINE" = trino ] && _engine_service=trino
  on "$SERVER" "cd $PERF_REPO/demo/named/perf && export PROVISA_BENCH_BIND=$PERF_SERVER_IP \
    && sudo -E docker compose --profile three-device --profile three-device-trino \
      up -d --build --no-deps $_engine_service"
fi

[ -z "$PROFILE" ] || on "$SERVER" "uv tool install py-spy==$PY_SPY_VERSION"

start_server() {  # start_server <group> <telemetry>
  echo "=== server for $1 (engine $ENGINE) ==="
  on "$SERVER" "export PROVISA_BENCH_BACKENDS_HOST=$PERF_BACKENDS_IP \
    PROVISA_BENCH_AGENT_LISTEN=$PERF_SERVER_IP:$AGENT_PORT \
    PROVISA_BENCH_ENGINE_HOST=$PERF_SERVER_IP; \
    nohup setsid env --default-signal=INT \
      $BENCH/orchestrate.sh serve --telemetry $2 --engine $ENGINE > ~/serve-$1.log 2>&1 < /dev/null &"
  # serving once the server's agent answers: serve_three_device.sh starts it after every worker
  # is ready
  on "$CLIENT" "for i in \$(seq 1 210); do curl -sf http://$PERF_SERVER_IP:$AGENT_PORT/host >/dev/null && exit 0; sleep 2; done; exit 1"
}

# '[s]erve...': the pattern does not match the remote shell running this command, whose own
# command line names the script.
# A job a non-interactive shell starts in the background begins with SIGINT ignored, and bash
# can neither trap nor reset a signal ignored on entry: the serve is started with SIGINT at its
# default (env --default-signal=INT above), so it stops on SIGINT as a server does. A stop that
# does not end in 60 s says what is still running and what the serve last wrote.
stop_server() {  # stop_server <group>
  on "$SERVER" "pkill -INT -f '[s]erve_three_device.sh'; for i in \$(seq 1 60); do pgrep -f '[s]erve_three_device.sh' >/dev/null || exit 0; sleep 1; done; \
    echo 'the serve did not stop within 60 s of SIGINT; still running:'; \
    ps -o pid,ppid,etime,stat,cmd -p \$(pgrep -d, -f '[s]erve_three_device.sh|[u]vicorn main:app|[h]ost_agent.py') | cut -c1-200; \
    echo '--- ~/serve-$1.log (tail)'; tail -40 ~/serve-$1.log; exit 1"
}

run_unit() {  # run_unit <stage> <unit> <telemetry> <transport> <variant> <cache>
  local stage="$1" name="$2" telemetry="$3" transport="$4" variant="$5" cache="$6" spy=""
  local contract_args="--only $transport"
  if [ "$stage" = profile ]; then
    spy="$transport"
    contract_args+=" --steps $PROFILE_STEP --window-s $PROFILE_S --warmup-s $PROFILE_WARM_S"
  elif [ "$variant" != - ]; then
    contract_args="--variant $variant --cache $cache --steps $STEPS --window-s $WINDOW_S --warmup-s $WARMUP_S $contract_args"
  elif [ "$cache" = uncached ]; then
    contract_args+=" --uncached"
  fi
  echo "=== run $name (engine $ENGINE) ==="
  # the contract as this unit states it: its telemetry the server's (REQ-2092)
  on "$CLIENT" "cd $BENCH && rm -rf $OUT/$name && mkdir -p $OUT/$name && $PY three_device.py contract --setup $PERF_CONTRACT --telemetry $telemetry \
    $contract_args --out $OUT/$name/setup.yaml"
  # A profile slows what it watches, so it runs apart from the repetitions, at the step they
  # saturate, after that step's warm-up.
  if [ -n "$spy" ]; then
    on "$SERVER" "nohup setsid bash -c 'sleep $PROFILE_WARM_S; sudo ~/.local/bin/py-spy record -d $PROFILE_S -r 100 -f raw -t \
      --subprocesses -o ~/spy-$spy.txt --pid \$(pgrep -o -f \"uvicorn main:app\")' > ~/spy-$spy.log 2>&1 < /dev/null &"
  fi
  on "$CLIENT" "cd $BENCH && PROVISA_BENCH_SERVER_HOST=$PERF_SERVER_IP \
    PROVISA_HTTP_BASE_URL=http://$PERF_SERVER_IP:8001 \
    $PY run_benchmark.py --engine $ENGINE --optimistic \
    --setup $OUT/$name/setup.yaml \
    --server-cpu-url http://$PERF_SERVER_IP:$AGENT_PORT/cpu \
    --source-cpu-url http://$PERF_BACKENDS_IP:$AGENT_PORT/docker-stats $_containers $_engine_cpu \
    --device server=http://$PERF_SERVER_IP:$AGENT_PORT \
    --device backends=http://$PERF_BACKENDS_IP:$AGENT_PORT \
    --output-dir $OUT/$name"
  if [ -n "$spy" ] && [ -z "$DRY_RUN" ]; then
    mkdir -p "$LOCAL_OUT/$name"
    gcloud compute scp --zone "$PERF_ZONE" "$SERVER:spy-$spy.txt" "$LOCAL_OUT/$name/"
  fi
}

# The unit's results copied back.
copy_back() {  # copy_back <unit>
  local copy=(gcloud compute scp --recurse --zone "$PERF_ZONE" "$CLIENT:$BENCH/$OUT/$1" "$LOCAL_OUT/")
  if [ -n "$DRY_RUN" ]; then
    printf '\n[operator]\n%s\n' "${copy[*]}"
    return 0
  fi
  mkdir -p "$LOCAL_OUT"
  rm -rf "${LOCAL_OUT:?}/$1"
  "${copy[@]}"
}

# The unit to the bucket, then its DONE object: a unit is done only once all of it is in the
# bucket, so a preemption mid-copy repeats the unit. The ledger goes with it. One process: gsutil -m
# deadlocks on macOS.
checkpoint() {  # checkpoint <unit>
  local upload=(gsutil cp -r "$LOCAL_OUT/$1" "$CHECKPOINTS/")
  if [ -n "$DRY_RUN" ]; then
    printf '\n[operator]\n%s\nprintf %%s %s | gsutil cp - %s\n' \
      "${upload[*]}" "$1" "$CHECKPOINTS/$1/DONE"
    return 0
  fi
  "${upload[@]}"
  [ ! -f "$LEDGER" ] || gsutil cp "$LEDGER" "$CHECKPOINTS/ledger.json"
  printf %s "$1" | gsutil cp - "$CHECKPOINTS/$1/DONE"
}

# The recheck after a unit (bench_units.py check). An invalid unit is discarded and not
# checkpointed; a flag no note explains keeps the unit and stops the run all the same.
recheck() {  # recheck <stage> <unit>; sets STOPPING
  [ "$1" != profile ] || return 0
  if [ -n "$DRY_RUN" ]; then
    printf '\n[operator]\nbench_units.py check --ledger %s --results %s --unit %s\n' "$LEDGER" "$LOCAL_OUT" "$2"
    return 0
  fi
  local rc=0
  units check --ledger "$LEDGER" --results "$LOCAL_OUT" --unit "$2" || rc=$?
  case "$rc" in
    0) ;;
    3) STOPPING="invalid"; return 3 ;;
    4) STOPPING="flag" ;;
    *) echo "the check of $2 failed with $rc"; exit "$rc" ;;
  esac
}

SERVING="" STOPPING="" LAST_STAGE=""
stage_passed() {  # stage_passed <stage>
  [ -z "$DRY_RUN" ] || return 0
  units stage-passed --ledger "$LEDGER" --stage "$1"
  gsutil cp "$LEDGER" "$CHECKPOINTS/ledger.json"
}
for entry in ${PLAN[@]+"${PLAN[@]}"}; do
  IFS=$'\t' read -r stage group name rep telemetry transport variant cache <<< "$entry"
  if [ -n "$LAST_STAGE" ] && [ "$stage" != "$LAST_STAGE" ]; then
    [ -z "$SERVING" ] || { stop_server "$SERVING"; SERVING=""; }
    stage_passed "$LAST_STAGE"
  fi
  LAST_STAGE="$stage"
  if [[ "$DONE_RUNS" == *" $name "* ]]; then
    echo "=== run $name: checkpointed already, skipped ==="
    continue
  fi
  if [ "$group" != "$SERVING" ]; then
    [ -z "$SERVING" ] || stop_server "$SERVING"
    start_server "$group" "$telemetry"
    SERVING="$group"
  fi
  run_unit "$stage" "$name" "$telemetry" "$transport" "$variant" "$cache"
  copy_back "$name"
  if ! recheck "$stage" "$name"; then
    stop_server "$SERVING"
    echo "run stopped at $name: invalid, nothing checkpointed for it. See $LEDGER."
    exit 3
  fi
  checkpoint "$name"
  if [ "$name" = "$STOP_AFTER" ] && [ -z "$DRY_RUN" ]; then
    units stop --ledger "$LEDGER" --unit "$name" --reason "deliberate abort, --stop-after"
    STOPPING="deliberate"
  fi
  if [ -n "$STOPPING" ]; then
    stop_server "$SERVING"
    echo "run stopped after $name: $STOPPING. See $LEDGER."
    exit 3
  fi
done
[ -z "$SERVING" ] || stop_server "$SERVING"
[ -z "$LAST_STAGE" ] || stage_passed "$LAST_STAGE"

REPORT=(report --root "$LOCAL_OUT" --out "$LOCAL_OUT-report" --ledger "$LEDGER")
[ -n "$PASS" ] && REPORT=(variant-report --root "$LOCAL_OUT" --out "$LOCAL_OUT-report" --ledger "$LEDGER")

if [ -n "$DRY_RUN" ]; then
  printf '\n[operator]\nthree_device.py %s\n' "${REPORT[*]}"
  exit 0
fi
"$LOCAL_PY" "$HERE/three_device.py" "${REPORT[@]}"
if [ -n "$PASS" ]; then
  [ -z "$SMOKE" ] || echo "a smoke: its numbers are not published"
  echo "governance pass (internal): $LOCAL_OUT-report/variants.json"
else
  echo "published: $LOCAL_OUT-report/published.json (telemetry off); internal: $LOCAL_OUT-report/internal.json"
fi
