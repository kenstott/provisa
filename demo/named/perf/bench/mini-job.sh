#!/bin/bash
# The mini benchmark's job on the Spot VM (mini.sh sends it; spot-bench.sh runs it with REF set).
# Each step's output is fetched as soon as it finishes (step.sh, sync.sh), so a preemption loses
# only the round in progress.
#
#   setup:    clone the public repo at REF, install Rust (the encoders compile in uv sync), uv sync
#   roundN:   harness_trial.py on DuckDB, PostgreSQL and Trino: its own seeded stack, every cell,
#             row agreement, and <engine>-sanity.json. Each round has its own output directory, so
#             no round is compared with another (the sanity report checks within the run only).
set -uo pipefail
source ~/step.sh
ROUNDS=${ROUNDS:-2}
STAGE_SPANS=${STAGE_SPANS:-}
SRC=~/src

setup() {
  set -e
  git clone -q https://github.com/kenstott/provisa.git "$SRC"
  cd "$SRC" && git checkout -q "$REF"
  mkdir -p ~/out/setup && git rev-parse HEAD > ~/out/setup/sha
  curl -fsSL https://sh.rustup.rs | sh -s -- -y -q --profile minimal
  . "$HOME/.cargo/env"
  uv sync --frozen --extra dev -q
  nproc > ~/out/setup/nproc
  free -g > ~/out/setup/free
}

round() {
  cd "$SRC"
  . "$HOME/.cargo/env"
  .venv/bin/python demo/named/perf/bench/harness_trial.py \
    --engine duckdb --engine pg --engine trino --output-dir ~/out/"$1" ${STAGE_SPANS:+--stage-spans}
}

step setup setup
if grep -q '^setup rc=0' ~/out/rc; then
  for r in $(seq 1 "$ROUNDS"); do step "round$r" round "round$r"; done
fi
finish
