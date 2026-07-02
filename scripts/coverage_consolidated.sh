#!/usr/bin/env bash
# Consolidated line-coverage across all four Python test suites (unit,
# integration, e2e, bdd) against the `provisa` package.
#
# Each suite runs into its own coverage data file, then `coverage combine`
# unions the executed lines into one consolidated metric — a line counted by
# ANY suite counts once. Per-suite percentages are reported alongside the
# combined total so you can see what each tier contributes.
#
# Usage:
#   scripts/coverage_consolidated.sh            # all suites; skips Docker bring-up
#   scripts/coverage_consolidated.sh --with-docker   # let integration/e2e spin up infra
#   scripts/coverage_consolidated.sh unit bdd   # only the named suites
#
# Requires: coverage, pytest-cov (already in the venv).
set -uo pipefail

cd "$(dirname "$0")/.."
PY="${PYTHON:-python}"

WITH_DOCKER=0
SUITES=()
for arg in "$@"; do
  case "$arg" in
    --with-docker) WITH_DOCKER=1 ;;
    unit|integration|e2e|bdd) SUITES+=("$arg") ;;
    *) echo "unknown arg: $arg" >&2; exit 2 ;;
  esac
done
[ ${#SUITES[@]} -eq 0 ] && SUITES=(unit integration e2e bdd)

# Skip the conftest Docker bring-up and the session Trino-wait unless the caller
# explicitly opted into infra. Suites needing services will still fail their own
# fixtures, but coverage is captured for whatever executes and the run never
# aborts on missing Docker.
if [ "$WITH_DOCKER" -eq 0 ]; then
  export PYTEST_NO_DOCKER=1
  export PROVISA_SKIP_TRINO_WAIT=1
fi

suite_target() {
  case "$1" in
    unit)        echo "tests/unit" ;;
    integration) echo "tests/integration -m integration" ;;
    e2e)         echo "tests/e2e -m e2e" ;;
    bdd)         echo "tests/steps" ;;
  esac
}

rm -f .coverage .coverage.* .cov.* coverage.json 2>/dev/null

# Parallel indexed arrays (bash 3.2 on macOS has no associative arrays).
REPORT_NAMES=()
REPORT_PCTS=()
RAN=()
for s in "${SUITES[@]}"; do
  echo "==================== suite: $s ===================="
  # shellcheck disable=SC2046
  COVERAGE_FILE=".cov.$s" "$PY" -m pytest $(suite_target "$s") \
    --cov=provisa --cov-report= --no-header -q
  REPORT_NAMES+=("$s")
  if [ -f ".cov.$s" ]; then
    REPORT_PCTS+=("$("$PY" -m coverage report --data-file=".cov.$s" --format=total 2>/dev/null)%")
    RAN+=("$s")
  else
    REPORT_PCTS+=("n/a (no data — suite could not execute)")
  fi
done

echo
echo "############## CONSOLIDATED COVERAGE ##############"
if [ ${#RAN[@]} -gt 0 ]; then
  DATA_FILES=(); for s in "${RAN[@]}"; do DATA_FILES+=(".cov.$s"); done
  "$PY" -m coverage combine --keep "${DATA_FILES[@]}" >/dev/null 2>&1
  "$PY" -m coverage json -o coverage.json >/dev/null 2>&1
  COMBINED="$("$PY" -m coverage report --format=total 2>/dev/null)%"
else
  COMBINED="n/a"
fi

echo
printf "%-14s %s\n" "suite" "line coverage"
printf "%-14s %s\n" "-----" "-------------"
i=0
while [ $i -lt ${#REPORT_NAMES[@]} ]; do
  printf "%-14s %s\n" "${REPORT_NAMES[$i]}" "${REPORT_PCTS[$i]}"
  i=$((i + 1))
done
printf "%-14s %s\n" "CONSOLIDATED" "$COMBINED"
echo
echo "Detail: coverage.json  |  HTML: coverage html && open htmlcov/index.html"
