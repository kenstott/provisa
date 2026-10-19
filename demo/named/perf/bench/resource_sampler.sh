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
# the window (from each process's cumulative CPU time, so short bursts are counted), resident
# memory at the end and major faults over the window (majflt + cmajflt from /proc/<pid>/stat);
# container rows are the mean of `docker stats` snapshots taken every ~2 s; the host rows are busy
# vCPUs, steal %, available memory at the start and end of the window, and the window's major
# faults and swap in/out from /proc/vmstat — CPU that rises with load on a small machine can be
# paging, and these rows say whether it was (REQ-2092).
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
# Major faults (majflt + the reaped children's cmajflt) over a class's processes.
class_majflt() {
  local total=0 pid
  for pid in $(class_pids "$1"); do
    [ -r "/proc/$pid/stat" ] || continue
    # Fields 12/13 are the 10th/11th after the ") " that ends the command name.
    total=$(( total + $(sed 's/^.*) //' "/proc/$pid/stat" | awk '{print $10 + $11}') ))
  done
  echo "$total"
}
vmstat_paging() { awk '$1=="pgmajfault"{f=$2} $1=="pswpin"{i=$2} $1=="pswpout"{o=$2} END{print f, i, o}' /proc/vmstat; }
mem_available_kb() { awk '/^MemAvailable:/{print $2}' /proc/meminfo; }
host_cpu() { awk '/^cpu /{print $2+$3+$4+$7+$8, $9, $2+$3+$4+$5+$6+$7+$8+$9}' /proc/stat; }

declare -A T0 F0
for c in "${CLASSES[@]}"; do
  T0["${c%%|*}"]="$(class_ticks "$c")"
  F0["${c%%|*}"]="$(class_majflt "$c")"
done
read -r H_BUSY0 H_STEAL0 H_ALL0 < <(host_cpu)
read -r PGMAJ0 SWPIN0 SWPOUT0 < <(vmstat_paging)
AVAIL0="$(mem_available_kb)"

DOCKER_LOG="$(mktemp)"
trap 'rm -f "$DOCKER_LOG"' EXIT
END=$(( $(date +%s) + SECS ))
while [ "$(date +%s)" -lt "$END" ]; do
  docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' >> "$DOCKER_LOG" 2>/dev/null || true
done

read -r H_BUSY1 H_STEAL1 H_ALL1 < <(host_cpu)
read -r PGMAJ1 SWPIN1 SWPOUT1 < <(vmstat_paging)
AVAIL1="$(mem_available_kb)"
{
  printf '%-26s %10s %10s %10s\n' "consumer ($LABEL, ${SECS}s)" "cpu_cores" "rss_mb" "majflt"
  for c in "${CLASSES[@]}"; do
    name="${c%%|*}"
    t1="$(class_ticks "$c")"
    # A process that exits in the window takes its faults with it unless its parent reaped it.
    f="$(( $(class_majflt "$c") - ${F0[$name]} ))"
    printf '%-26s %10.2f %10.0f %10d\n' "$name" \
      "$(awk -v a="${T0[$name]}" -v b="$t1" -v hz="$HZ" -v s="$SECS" 'BEGIN{d=b-a; if(d<0)d=0; print d/hz/s}')" \
      "$(awk -v kb="$(class_rss_kb "$c")" 'BEGIN{print kb/1024}')" \
      "$(( f < 0 ? 0 : f ))"
  done
  # Container rows: mean CPU% / 100 = cores; memory is the last snapshot's usage. A paused
  # container reports 0.00% and keeps its memory.
  awk '{gsub("%","",$2); n[$1]++; cpu[$1]+=$2; mem[$1]=$3}
       END{for(k in n) printf "%-26s %10.2f %10s\n", "container:" k, cpu[k]/n[k]/100, mem[k]}' "$DOCKER_LOG" | sort
  awk -v b0="$H_BUSY0" -v b1="$H_BUSY1" -v s0="$H_STEAL0" -v s1="$H_STEAL1" -v a0="$H_ALL0" -v a1="$H_ALL1" \
      -v n="$(nproc)" -v avail0="$AVAIL0" -v avail1="$AVAIL1" \
      -v pg="$(( PGMAJ1 - PGMAJ0 ))" -v si="$(( SWPIN1 - SWPIN0 ))" -v so="$(( SWPOUT1 - SWPOUT0 ))" \
      'BEGIN{d=a1-a0; printf "%-26s %10.2f %10s\n", "host busy vCPUs of " n, (b1-b0)/d*n, "";
             printf "%-26s %9.2f%% %10s\n", "host steal", (s1-s0)/d*100, "";
             printf "%-26s %10s %10.0f\n", "host memory avail start", "", avail0/1024;
             printf "%-26s %10s %10.0f\n", "host memory avail end", "", avail1/1024;
             printf "%-26s %10d\n", "host major faults", pg;
             printf "%-26s %10d\n", "host swap in (pages)", si;
             printf "%-26s %10d\n", "host swap out (pages)", so}'
} | tee "$OUT"
