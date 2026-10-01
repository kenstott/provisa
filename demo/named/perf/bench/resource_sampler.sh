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
# Who used the box during a benchmark window. Everything in the single-VM run shares 16 vCPUs —
# Provisa, the control plane, Redis, every source database, the trace receiver and the load
# generator — so a result is only readable next to how much of the machine each of them took.
#
# Usage: resource_sampler.sh <seconds> <label> [out_dir]
# Prints (and writes <out_dir>/resources-<label>.txt) one line per consumer: average CPU cores over
# the window (from each process's cumulative CPU time, so short bursts are counted) and resident
# memory at the end; container rows are the mean of `docker stats` snapshots taken every ~2 s;
# the host row is busy vCPUs, steal % and available memory from /proc.
set -euo pipefail

SECS="${1:?seconds}"
LABEL="${2:?label}"
OUT_DIR="${3:-.}"
mkdir -p "$OUT_DIR"
OUT="$OUT_DIR/resources-$LABEL.txt"
HZ="$(getconf CLK_TCK)"
CONTROL_PG_DIR="${PROVISA_HOME:-$HOME/.provisa}/demo/control-pg"

# name|pgrep -f pattern. A consumer with no matching process reports 0.
CLASSES=(
  "provisa-workers|multiprocessing\.spawn|uvicorn main:app"
  "control-plane-pg|@postmaster"
  "trace-receiver|noop_otlp_receiver|otlp2parquet|otlp2sql|otelcol"
  "load-generator|(^|/)ab |artillery|run_benchmark\.py"
  "ui-dev-server|vite"
)

class_pids() {
  local pat="${1#*|}"
  if [ "$pat" = "@postmaster" ]; then
    # The embedded control plane's postmaster and its backends. Matched by process tree, not by
    # name: the benchmark Postgres container's backends are "postgres: ..." on the host as well.
    local pm
    pm="$(head -1 "$CONTROL_PG_DIR/postmaster.pid" 2>/dev/null || true)"
    [ -n "$pm" ] && { echo "$pm"; pgrep -P "$pm" 2>/dev/null || true; }
    return 0
  fi
  pgrep -f -- "$pat" 2>/dev/null | grep -vx "$$" || true
}

# Sum of utime+stime (clock ticks) and RSS (kB) over a class's processes.
class_ticks() {
  local total=0 pid
  for pid in $(class_pids "$1"); do
    [ -r "/proc/$pid/stat" ] || continue
    # Fields 14/15 follow the ") " that ends the command name, which may itself contain spaces.
    total=$(( total + $(sed 's/^.*) //' "/proc/$pid/stat" | awk '{print $12 + $13}') ))
  done
  echo "$total"
}
class_rss_kb() {
  local total=0 pid kb
  for pid in $(class_pids "$1"); do
    kb="$(awk '/^VmRSS:/{print $2}' "/proc/$pid/status" 2>/dev/null || true)"
    total=$(( total + ${kb:-0} ))
  done
  echo "$total"
}
host_cpu() { awk '/^cpu /{print $2+$3+$4+$7+$8, $9, $2+$3+$4+$5+$6+$7+$8+$9}' /proc/stat; }

declare -A T0
for c in "${CLASSES[@]}"; do T0["${c%%|*}"]="$(class_ticks "$c")"; done
read -r H_BUSY0 H_STEAL0 H_ALL0 < <(host_cpu)

DOCKER_LOG="$(mktemp)"
trap 'rm -f "$DOCKER_LOG"' EXIT
END=$(( $(date +%s) + SECS ))
while [ "$(date +%s)" -lt "$END" ]; do
  docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' >> "$DOCKER_LOG" 2>/dev/null || true
done

read -r H_BUSY1 H_STEAL1 H_ALL1 < <(host_cpu)
{
  printf '%-26s %10s %10s\n' "consumer ($LABEL, ${SECS}s)" "cpu_cores" "rss_mb"
  for c in "${CLASSES[@]}"; do
    name="${c%%|*}"
    t1="$(class_ticks "$c")"
    printf '%-26s %10.2f %10.0f\n' "$name" \
      "$(awk -v a="${T0[$name]}" -v b="$t1" -v hz="$HZ" -v s="$SECS" 'BEGIN{d=b-a; if(d<0)d=0; print d/hz/s}')" \
      "$(awk -v kb="$(class_rss_kb "$c")" 'BEGIN{print kb/1024}')"
  done
  # Container rows: mean CPU% / 100 = cores; memory is the last snapshot's usage. A paused
  # container reports 0.00% and keeps its memory.
  awk '{gsub("%","",$2); n[$1]++; cpu[$1]+=$2; mem[$1]=$3}
       END{for(k in n) printf "%-26s %10.2f %10s\n", "container:" k, cpu[k]/n[k]/100, mem[k]}' "$DOCKER_LOG" | sort
  awk -v b0="$H_BUSY0" -v b1="$H_BUSY1" -v s0="$H_STEAL0" -v s1="$H_STEAL1" -v a0="$H_ALL0" -v a1="$H_ALL1" \
      -v n="$(nproc)" -v avail="$(awk '/^MemAvailable:/{print $2}' /proc/meminfo)" \
      'BEGIN{d=a1-a0; printf "%-26s %10.2f %10s\n", "host busy vCPUs of " n, (b1-b0)/d*n, "";
             printf "%-26s %9.2f%% %10s\n", "host steal", (s1-s0)/d*100, "";
             printf "%-26s %10s %10.0f\n", "host memory available", "", avail/1024}'
} | tee "$OUT"
