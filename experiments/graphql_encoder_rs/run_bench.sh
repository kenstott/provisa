#!/bin/bash
# The 2M-row benchmark of every surface, one process each, through heavy.sh. Output: bench-2m-<surface>.{log,json}.
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
cd "$here"
for s in ${SURFACES:-graphql rest jsonapi odata}; do
  PYTHONPATH=build/ref:build:. ~/.provisa-agent/heavy.sh ../../.venv/bin/python bench.py --surface "$s" --rows 2000000 --out "bench-2m-$s.json" 2>&1 | tee "bench-2m-$s.log"
done
