#!/usr/bin/env bash
# Runs INSIDE the linux-mem container (docker/mem-lane.Dockerfile). Started by
# scripts/run-linux-mem-lane.sh. Installs the project editable and runs the RLIMIT_AS
# memory-bounded tests against the harness-provisioned isolated-stack PostgreSQL.
#
# No PG bootstrap here: the isolated Docker stack (tests/conftest.py) already provisions a
# real PostgreSQL per worktree. PROVISA_MEM_TEST_DSN (exported by run-linux-mem-lane.sh)
# points the test at that server over host.docker.internal — so this lane reuses the standard
# provisioning instead of standing up a throwaway PG. PROVISA_MEM_TEST_DSN also makes the test
# skip embedded pgserver, which has no linux/aarch64 wheel. PYTEST_NO_DOCKER=1 keeps this
# in-container session from re-provisioning the stack; PROVISA_SKIP_TRINO_WAIT=1 skips the
# 360s Trino wait.
set -euo pipefail

pip install -q -e ".[dev,embedded]"

export PYTEST_NO_DOCKER=1
export PROVISA_SKIP_TRINO_WAIT=1

exec python -m pytest tests/integration/test_streaming_memory_bounded_e2e.py -rs "$@"
