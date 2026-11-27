#!/bin/bash
# The mini benchmark's job on the Spot VM (mini.sh sends it; spot-bench.sh runs it with REF set).
# Each step's output is fetched as soon as it finishes (step.sh, sync.sh), so a preemption loses
# only the round in progress.
#
#   setup:    clone the public repo at REF, install Rust (the encoders compile in uv sync), uv sync;
#             with BASE_REF, the same again for BASE_REF in its own source directory
#   roundN:   harness_trial.py on DuckDB, PostgreSQL and Trino: its own seeded stack, every cell,
#             row agreement, and <engine>-sanity.json. Each round has its own output directory, so
#             no round is compared with another (the sanity report checks within the run only).
#   baseN:    with BASE_REF, the same rounds at BASE_REF, run just before roundN, so drift on the VM
#             reaches base and candidate alike
#   compare:  with BASE_REF, bench_sanity.py per engine and round: roundN's report against baseN's,
#             both from this VM, into compare/roundN/<engine>-vs-base.json
set -uo pipefail
source ~/step.sh
ROUNDS=${ROUNDS:-2}
STAGE_SPANS=${STAGE_SPANS:-}
BASE_REF=${BASE_REF:-}
ENGINES=(duckdb pg trino)
SRC=~/src
BASE_SRC=~/src-base

checkout() {
  # checkout DIR REF SHAFILE: the source at REF in DIR with its own environment
  cd "$1" && git checkout -q "$2"
  git rev-parse HEAD > "$3"
  uv sync --frozen --extra dev -q
}

setup() {
  set -e
  git clone -q https://github.com/kenstott/provisa.git "$SRC"
  mkdir -p ~/out/setup
  curl -fsSL https://sh.rustup.rs | sh -s -- -y -q --profile minimal
  . "$HOME/.cargo/env"
  checkout "$SRC" "$REF" ~/out/setup/sha
  if [ -n "$BASE_REF" ]; then
    git -C "$SRC" worktree add -q --detach "$BASE_SRC" "$BASE_REF"
    checkout "$BASE_SRC" "$BASE_REF" ~/out/setup/base-sha
  fi
  nproc > ~/out/setup/nproc
  free -g > ~/out/setup/free
}

round() {
  # round SRC_DIR OUT_NAME: the source's own harness on its own checkout
  cd "$1"
  . "$HOME/.cargo/env"
  .venv/bin/python demo/named/perf/bench/harness_trial.py \
    --engine duckdb --engine pg --engine trino --output-dir ~/out/"$2" ${STAGE_SPANS:+--stage-spans}
}

newest_report() {
  # the matrix report a round wrote for an engine (harness_trial.py keeps the newest)
  ls ~/out/"$1"/"$2"/*.json 2>/dev/null | sort | tail -1
}

compare() {
  # Every round and engine is compared; one without both reports fails the step after the rest.
  local missing=0 r e cand base dest
  for r in $(seq 1 "$ROUNDS"); do
    dest=~/out/compare/round$r; mkdir -p "$dest"
    for e in "${ENGINES[@]}"; do
      cand=$(newest_report "round$r" "$e"); base=$(newest_report "base$r" "$e")
      if [ -z "$cand" ] || [ -z "$base" ]; then
        echo "round$r $e: no report (candidate '${cand}', base '${base}')"; missing=1; continue
      fi
      echo "round$r $e: $cand vs $base"
      "$SRC"/.venv/bin/python "$SRC"/demo/named/perf/bench/bench_sanity.py \
        --report "$cand" --baseline "$base" --out "$dest/$e-vs-base.json" || missing=1
    done
  done
  return "$missing"
}

step setup setup
if grep -q '^setup rc=0' ~/out/rc; then
  for r in $(seq 1 "$ROUNDS"); do
    [ -n "$BASE_REF" ] && step "base$r" round "$BASE_SRC" "base$r"
    step "round$r" round "$SRC" "round$r"
  done
  [ -n "$BASE_REF" ] && step compare compare
fi
finish
