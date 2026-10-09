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
# A process is shown by its NAME and its arguments, never its environment (`ps` is not asked for
# it). Arguments are cut to 100 characters, and the value of any argument that names a
# password, token, secret or key is replaced before it is written.
set -euo pipefail

out="${1:?the file to write samples to}"
mkdir -p "$(dirname "$out")"

redact() {
  sed -E 's/(([A-Za-z_-]*(pass(word)?|token|secret|key)[A-Za-z_-]*)[= ])[^ ]+/\1<redacted>/Ig'
}

top_by() {
  # pid, %cpu, %mem, resident MiB, name and arguments
  ps -eo pid=,pcpu=,pmem=,rss=,args= --sort="-$1" | head -n 5 \
    | awk '{ rss = $4 / 1024; $4 = ""; args = ""; for (i = 5; i <= NF; i++) args = args " " $i;
             printf "  %7s %5s%% cpu %5s%% mem %7.0f MiB %s\n", $1, $2, $3, rss, substr(args, 2, 100) }' \
    | redact
}

while true; do
  {
    echo "=== $(date -u +%Y-%m-%dT%H:%M:%SZ)  load: $(cut -d' ' -f1-3 /proc/loadavg)"
    free -m | awk 'NR == 2 { printf "  memory: %s MiB used of %s, %s available\n", $3, $2, $7 }'
    echo "  by cpu:"
    top_by pcpu
    echo "  by memory:"
    top_by rss
  } >> "$out"
  sleep 30
done
