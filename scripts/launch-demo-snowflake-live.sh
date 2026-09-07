#!/usr/bin/env bash
# Launch/restart the demo-snowflake-live instance (port 8200/3200) with the
# Snowflake Horizon metadata-export connection wired up correctly.
#
# Do NOT touch local-dev or demo-snowflake with this script.
#
# Why this script exists: `PROVISA_ENGINE`/`PROVISA_ENGINE_URL` are NOT read
# from .env by the app — they must be exported into the process environment
# before launch. Blanket `source .env` also breaks this instance: .env
# contains POSTGRES_HOST (for an unrelated test lane) which hijacks the
# embedded control-plane's own pgserver connection. Only the four
# SNOWFLAKE_* values are extracted here, nothing else from .env is sourced.
#
# Requires SNOWFLAKE_DATABASE in .env (or exported) in addition to the
# existing SNOWFLAKE_ACCOUNT/SNOWFLAKE_USER/SNOWFLAKE_PASSWORD/SNOWFLAKE_WAREHOUSE.
# Without a database in the DSN, tag/data_product publish fails with
# `Database '""' does not exist or not authorized` (descriptions/COMMENT
# publish still works since it uses per-table physical parts only).

set -euo pipefail
cd "$(dirname "$0")/.."

ENV_FILE=".env"
INSTANCE_DIR="$HOME/.provisa/demo-snowflake-live"
LOG_FILE="/private/tmp/demo-snowflake-live.log"
UI_PORT=3200
API_PORT=8200

SNOWFLAKE_ACCOUNT=$(grep '^SNOWFLAKE_ACCOUNT=' "$ENV_FILE" | cut -d= -f2-)
SNOWFLAKE_USER=$(grep '^SNOWFLAKE_USER=' "$ENV_FILE" | cut -d= -f2-)
SNOWFLAKE_PASSWORD=$(grep '^SNOWFLAKE_PASSWORD=' "$ENV_FILE" | cut -d= -f2-)
SNOWFLAKE_WAREHOUSE=$(grep '^SNOWFLAKE_WAREHOUSE=' "$ENV_FILE" | cut -d= -f2-)
SNOWFLAKE_DATABASE=$(grep '^SNOWFLAKE_DATABASE=' "$ENV_FILE" | cut -d= -f2- || true)

if [[ -z "$SNOWFLAKE_ACCOUNT" || -z "$SNOWFLAKE_USER" || -z "$SNOWFLAKE_PASSWORD" || -z "$SNOWFLAKE_WAREHOUSE" ]]; then
  echo "Missing SNOWFLAKE_ACCOUNT/USER/PASSWORD/WAREHOUSE in $ENV_FILE" >&2
  exit 1
fi
if [[ -z "$SNOWFLAKE_DATABASE" ]]; then
  echo "SNOWFLAKE_DATABASE not set in $ENV_FILE — tags/data_products publish will fail" \
       "with Database '\"\"' does not exist or not authorized. Add SNOWFLAKE_DATABASE=<db> to .env." >&2
fi

pkill -f "provisa run.*$INSTANCE_DIR" 2>/dev/null || true

export PROVISA_ENGINE=snowflake
if [[ -n "$SNOWFLAKE_DATABASE" ]]; then
  export PROVISA_ENGINE_URL="snowflake://${SNOWFLAKE_USER}:${SNOWFLAKE_PASSWORD}@${SNOWFLAKE_ACCOUNT}/${SNOWFLAKE_DATABASE}?warehouse=${SNOWFLAKE_WAREHOUSE}"
else
  export PROVISA_ENGINE_URL="snowflake://${SNOWFLAKE_USER}:${SNOWFLAKE_PASSWORD}@${SNOWFLAKE_ACCOUNT}/?warehouse=${SNOWFLAKE_WAREHOUSE}"
fi

nohup .venv/bin/provisa run --demo --data-dir "$INSTANCE_DIR" --api-port "$API_PORT" --ui-port "$UI_PORT" --no-browser \
  > "$LOG_FILE" 2>&1 &

echo "Launched demo-snowflake-live (PID $!). Log: $LOG_FILE"
echo "Tail: tail -f $LOG_FILE"
