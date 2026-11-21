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
# Diagnostics only (ui-e2e-core.yml `pyspy`, off by default): a py-spy recording of every Provisa
# backend the Playwright harness starts, one file per process, written when that process exits.
# Each file is py-spy's raw (collapsed-stack) format with one root per thread, which speedscope
# opens as it is and which a flamegraph tool renders.
#
#   pyspy-watch.sh <dir> <py-spy>    watches until killed (the lane's end); <py-spy> is the binary's path
#
# The runner's ptrace scope lets a process trace only its own children, so py-spy runs under sudo.
set -euo pipefail

out="${1:?the directory to write recordings to}"
pyspy="${2:?the py-spy binary}"
mkdir -p "$out"
seen=" "
while true; do
  # The backends are `uvicorn main:app --port N` (playwright.config.ts); the demo servers are not.
  for pid in $(pgrep -f 'uvicorn main:app' || true); do
    case "$seen" in *" $pid "*) continue ;; esac
    seen="$seen$pid "
    port="$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null | sed -nE 's/.*--port ([0-9]+).*/\1/p')"
    sudo -n "$pyspy" record --pid "$pid" --threads --nonblocking --rate 50 --format raw \
      --output "$out/backend-${port:-x}-$pid.txt" > "$out/py-spy-$pid.log" 2>&1 &
  done
  sleep 1
done
