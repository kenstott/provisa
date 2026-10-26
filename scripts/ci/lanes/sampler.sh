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
# What the runner was doing while a lane ran (lane.yml `sampler`): load, memory, and the
# processes taking the most CPU and the most memory, every 30 seconds, into one file the lane
# uploads. The core UI lane stopped passing for 80 minutes of a run with nothing in its log to
# say which process held the machine (run 37918605161); this names it.
#
#   sampler.sh <file>     samples until killed (the job's end)
#
# "by cpu" is each process's CPU over the interval since the previous sample (from /proc/<pid>/stat),
# not ps's average over the process's whole life: a browser that has burned a core for forty minutes
# and one that just started are told apart (run 40256175245). A process is shown by its NAME and its
# arguments, never its environment. Arguments are written whole up to 1000 characters -- enough for
# a chrome process's --type and the flags that name it -- and the value of any argument that names a
# password, token, secret or key is replaced before it is written.
set -euo pipefail

out="${1:?the file to write samples to}"
mkdir -p "$(dirname "$out")"

redact() {
  sed -E 's/(([A-Za-z_-]*(pass(word)?|token|secret|key)[A-Za-z_-]*)[= ])[^ ]+/\1<redacted>/Ig'
}

hz="$(getconf CLK_TCK)"
prev="$(mktemp)"
prev_uptime=""

ticks() {
  # pid and its user+system CPU ticks so far, for every live process. The command is cut at its
  # last ")" so a name holding spaces or parentheses does not shift the fields.
  cat /proc/[0-9]*/stat 2>/dev/null \
    | awk '{ i = index($0, ") "); split(substr($0, i + 2), f, " "); print $1, f[12] + f[13] }' \
    | sort -k1,1
}

args_of() {
  tr '\0' ' ' < "/proc/$1/cmdline" 2>/dev/null | cut -c1-1000
}

top_by_interval_cpu() {
  # pid, % of one CPU over the last interval, resident MiB, name and arguments
  local now uptime
  now="$(mktemp)"
  ticks > "$now"
  uptime="$(cut -d' ' -f1 /proc/uptime)"
  if [ -n "$prev_uptime" ]; then
    join "$prev" "$now" \
      | awk -v hz="$hz" -v dt="$(awk -v a="$prev_uptime" -v b="$uptime" 'BEGIN { print b - a }')" \
          '{ printf "%s %.1f\n", $1, ($3 - $2) * 100 / (hz * dt) }' \
      | sort -k2,2 -rn | head -n 5 \
      | while read -r pid pct; do
          rss="$(awk '/^VmRSS/ { printf "%.0f", $2 / 1024 }' "/proc/$pid/status" 2>/dev/null)"
          printf "  %7s %6s%% cpu %7s MiB %s\n" "$pid" "$pct" "${rss:-?}" "$(args_of "$pid")"
        done | redact
  else
    echo "  (first sample: no interval yet)"
  fi
  mv "$now" "$prev"
  prev_uptime="$uptime"
}

top_by_memory() {
  # pid, %mem, resident MiB, name and arguments
  ps -eo pid=,pmem=,rss= --sort=-rss | head -n 5 \
    | while read -r pid pmem rss; do
        printf "  %7s %5s%% mem %7.0f MiB %s\n" "$pid" "$pmem" "$((rss / 1024))" "$(args_of "$pid")"
      done | redact
}

while true; do
  {
    echo "=== $(date -u +%Y-%m-%dT%H:%M:%SZ)  load: $(cut -d' ' -f1-3 /proc/loadavg)"
    free -m | awk 'NR == 2 { printf "  memory: %s MiB used of %s, %s available\n", $3, $2, $7 }'
    echo "  by cpu (last interval):"
    top_by_interval_cpu
    echo "  by memory:"
    top_by_memory
  } >> "$out"
  sleep 30
done
