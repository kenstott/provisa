#!/bin/sh
# Fetch the current SCRAM hash from PG and write userlist.txt before starting PgBouncer.
# This ensures PgBouncer always has the correct hash, even after volume recreation.

set -e

PGHOST="${PGBOUNCER_PG_HOST:-postgres}"
PGPORT="${PGBOUNCER_PG_PORT:-5432}"
PGUSER="${PGBOUNCER_PG_USER:-provisa}"
PGPASSWORD="${PGBOUNCER_PG_PASSWORD:-provisa}"
PGDATABASE="${PGBOUNCER_PG_DATABASE:-provisa}"

echo "Fetching SCRAM hash from PG at ${PGHOST}:${PGPORT}..."

# Wait for PG (should already be healthy due to depends_on, but be safe)
for i in $(seq 1 30); do
  if PGPASSWORD="$PGPASSWORD" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -c "SELECT 1" > /dev/null 2>&1; then
    break
  fi
  echo "Waiting for PG... ($i)"
  sleep 1
done

# Extract SCRAM hash and write userlist.txt
HASH=$(PGPASSWORD="$PGPASSWORD" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -tAc "SELECT rolpassword FROM pg_authid WHERE rolname='$PGUSER'")

if [ -z "$HASH" ]; then
  echo "ERROR: Could not fetch SCRAM hash from PG"
  exit 1
fi

echo "\"${PGUSER}\" \"${HASH}\"" > /etc/pgbouncer/userlist.txt
echo "userlist.txt updated with fresh SCRAM hash"

# Start PgBouncer via original entrypoint + CMD
exec /entrypoint.sh /usr/bin/pgbouncer /etc/pgbouncer/pgbouncer.ini
