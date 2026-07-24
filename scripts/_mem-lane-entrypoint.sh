#!/usr/bin/env bash
# Runs INSIDE the linux-mem container (docker/mem-lane.Dockerfile). Started by
# scripts/run-linux-mem-lane.sh. Boots a native PostgreSQL, installs the project
# editable, and runs the RLIMIT_AS memory-bounded tests against that PG.
#
# PROVISA_MEM_TEST_DSN makes the test use this external PG instead of embedded pgserver
# (no linux/aarch64 wheel). PYTEST_NO_DOCKER=1 tells the suite conftest not to provision
# the isolated docker stack (there is no docker CLI in here) and to no-op the pg_dump/
# pg_restore snapshot fixture. PROVISA_SKIP_TRINO_WAIT=1 skips the 360s Trino wait.
set -euo pipefail

PGDATA=/var/lib/pg
PGBIN=$(ls -d /usr/lib/postgresql/*/bin | head -1)
export PATH="$PGBIN:$PATH"

install -d -o postgres -g postgres "$PGDATA"
runuser -u postgres -- initdb -A trust -D "$PGDATA" >/dev/null
runuser -u postgres -- pg_ctl -D "$PGDATA" -o "-p 5432" -w start >/dev/null
runuser -u postgres -- psql -p 5432 -d postgres -v ON_ERROR_STOP=1 -c \
  "CREATE ROLE provisa SUPERUSER LOGIN PASSWORD 'provisa'" >/dev/null
runuser -u postgres -- createdb -p 5432 -O provisa provisa

pip install -q -e ".[dev,embedded]"

export PROVISA_MEM_TEST_DSN="postgresql://provisa:provisa@localhost:5432/provisa"
export PYTEST_NO_DOCKER=1
export PROVISA_SKIP_TRINO_WAIT=1

exec python -m pytest tests/integration/test_streaming_memory_bounded_e2e.py -rs "$@"
