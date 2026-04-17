#!/usr/bin/env bash
# Start Provisa using the install-time configuration (core services only, no dev extras).
# Simulates the installed product: postgres + trino + redis, no kafka/mongo/observability.
# Backend logs go to ./.logs/server-install.log

set -euo pipefail

KEEP_DOCKER=false
FAST=false
for arg in "$@"; do
  case "$arg" in
    --keep-docker) KEEP_DOCKER=true ;;
    --fast) FAST=true; KEEP_DOCKER=true ;;
    *) echo "Unknown option: $arg"; echo "Usage: $0 [--keep-docker] [--fast]"; exit 1 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/.logs"
mkdir -p "$LOG_DIR"

# Load .env if present
if [ -f "$SCRIPT_DIR/.env" ]; then
  set -a
  . "$SCRIPT_DIR/.env"
  set +a
fi

export PG_PASSWORD="${PG_PASSWORD:-provisa}"
export REDIS_URL="${REDIS_URL:-redis://localhost:6379}"
export PROVISA_CONFIG=config/provisa-install.yaml
export PROVISA_CONFIG_REPLACE=true
export PROVISA_REDIRECT_ENABLED="${PROVISA_REDIRECT_ENABLED:-true}"
export PROVISA_REDIRECT_ENDPOINT="${PROVISA_REDIRECT_ENDPOINT:-http://localhost:9000}"
export PROVISA_REDIRECT_ACCESS_KEY="${PROVISA_REDIRECT_ACCESS_KEY:-minioadmin}"
export PROVISA_REDIRECT_SECRET_KEY="${PROVISA_REDIRECT_SECRET_KEY:-minioadmin}"
export PROVISA_REDIRECT_BUCKET="${PROVISA_REDIRECT_BUCKET:-provisa-results}"

# Core + install overlay (port bindings only — no kafka/mongo/elasticsearch/observability)
COMPOSE_FILES="-f docker-compose.core.yml -f docker-compose.dev-install.yml"

# Ensure demo files exist (SQLite inquiries DB, etc.)
if [ -f "$SCRIPT_DIR/demo/files/create_demo_files.py" ]; then
  echo "Generating demo files..."
  "$SCRIPT_DIR/.venv/bin/python" "$SCRIPT_DIR/demo/files/create_demo_files.py" 2>/dev/null || true
fi

echo "Starting Docker Compose services (core only)..."
cd "$SCRIPT_DIR"
docker compose $COMPOSE_FILES up -d 2>&1 || true

CREATED=$(docker ps -a --filter "label=com.docker.compose.project=provisa" \
  --filter "status=created" \
  --format '{{.Label "com.docker.compose.service"}}' 2>/dev/null | sort -u | tr '\n' ' ')
if [ -n "$CREATED" ]; then
  echo "Starting remaining services: $CREATED"
  # shellcheck disable=SC2086
  docker compose $COMPOSE_FILES up -d --no-deps $CREATED 2>&1 || true
fi

echo -n "Waiting for infrastructure services"
for i in $(seq 1 120); do
  PG_OK=$(docker inspect --format '{{.State.Health.Status}}' provisa-postgres-1 2>/dev/null || echo "missing")
  REDIS_OK=$(docker inspect --format '{{.State.Health.Status}}' provisa-redis-1 2>/dev/null || echo "missing")
  if [ "$PG_OK" = "healthy" ] && [ "$REDIS_OK" = "healthy" ]; then
    echo " OK"
    break
  fi
  if [ "$i" -eq 120 ]; then
    echo " TIMEOUT"
    echo "Services did not become healthy. postgres=$PG_OK redis=$REDIS_OK"
    exit 1
  fi
  echo -n "."
  sleep 2
done

echo -n "Waiting for Trino"
for i in $(seq 1 90); do
  TRINO_OK=$(docker inspect --format '{{.State.Health.Status}}' provisa-trino-1 2>/dev/null || echo "missing")
  if [ "$TRINO_OK" = "healthy" ]; then
    echo " OK"
    break
  fi
  if [ "$i" -eq 90 ]; then
    echo " TIMEOUT (continuing anyway)"
    break
  fi
  echo -n "."
  sleep 2
done

echo "Docker Compose services ready."

# Ensure pet_store schema exists (idempotent — init.sql only runs on first volume creation)
docker exec provisa-postgres-1 psql -U provisa -d provisa -c "
  CREATE SCHEMA IF NOT EXISTS pet_store;
  CREATE TABLE IF NOT EXISTS pet_store.pets (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    species VARCHAR(50) NOT NULL,
    breed_name VARCHAR(100) NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    available BOOLEAN NOT NULL DEFAULT TRUE
  );
  INSERT INTO pet_store.pets (name, species, breed_name, price, available)
  SELECT * FROM (VALUES
    ('Whiskers', 'cat', 'Maine Coon',        450.00, TRUE),
    ('Mittens',  'cat', 'Siamese',           380.00, TRUE),
    ('Shadow',   'cat', 'British Shorthair', 420.00, FALSE),
    ('Luna',     'cat', 'Persian',           520.00, TRUE),
    ('Oreo',     'cat', 'Maine Coon',        430.00, TRUE),
    ('Buddy',    'dog', 'Golden Retriever',  800.00, TRUE),
    ('Max',      'dog', 'Labrador',          750.00, FALSE),
    ('Bella',    'dog', 'Beagle',            600.00, TRUE),
    ('Charlie',  'dog', 'Poodle',            950.00, TRUE),
    ('Rocky',    'dog', 'Golden Retriever',  820.00, TRUE),
    ('Daisy',    'cat', 'Siamese',           390.00, TRUE),
    ('Milo',     'dog', 'Labrador',          710.00, TRUE)
  ) AS v(name, species, breed_name, price, available)
  WHERE NOT EXISTS (SELECT 1 FROM pet_store.pets LIMIT 1);
" 2>/dev/null || echo "pet_store schema setup skipped (will retry on next start)"

# Ensure Python dependencies are installed (skipped in --fast mode)
if [ "$FAST" = false ] && [ -f "$SCRIPT_DIR/pyproject.toml" ] && [ -d "$SCRIPT_DIR/.venv" ]; then
  echo "Syncing Python dependencies..."
  "$SCRIPT_DIR/.venv/bin/pip" install -e "$SCRIPT_DIR" -q
fi

lsof -i :8001 -P -t 2>/dev/null | xargs kill 2>/dev/null || true
lsof -i :3000 -P -t 2>/dev/null | xargs kill 2>/dev/null || true
sleep 1

> "$LOG_DIR/server-install.log"

echo "Starting Provisa backend on port 8001..."
cd "$SCRIPT_DIR"
"$SCRIPT_DIR/.venv/bin/uvicorn" main:app --reload --reload-dir provisa --reload-dir config --host 0.0.0.0 --port 8001 \
  >> "$LOG_DIR/server-install.log" 2>&1 &
BACKEND_PID=$!

echo -n "  Waiting for backend"
for i in $(seq 1 60); do
  if curl -sf http://localhost:8001/health > /dev/null 2>&1; then
    echo " OK (PID $BACKEND_PID)"
    break
  fi
  if ! kill -0 $BACKEND_PID 2>/dev/null; then
    echo " FAILED"
    echo "Backend crashed. Last 20 lines of log:"
    tail -20 "$LOG_DIR/server-install.log"
    exit 1
  fi
  echo -n "."
  sleep 1
done

if ! curl -sf http://localhost:8001/health > /dev/null 2>&1; then
  echo " TIMEOUT"
  echo "Backend did not become healthy. Last 20 lines of log:"
  tail -20 "$LOG_DIR/server-install.log"
  exit 1
fi

echo "Starting Provisa UI on port 3000..."
cd "$SCRIPT_DIR/provisa-ui"
npx vite --host 0.0.0.0 &
UI_PID=$!

echo -n "  Waiting for UI"
for i in $(seq 1 15); do
  if curl -sf http://localhost:3000 > /dev/null 2>&1; then
    echo " OK (PID $UI_PID)"
    break
  fi
  if ! kill -0 $UI_PID 2>/dev/null; then
    echo " FAILED"
    echo "UI dev server crashed."
    exit 1
  fi
  echo -n "."
  sleep 1
done

echo ""
echo "Provisa running (install config):"
echo "  Backend: http://localhost:8001  (logs: $LOG_DIR/server-install.log)"
echo "  UI:      http://localhost:3000"
echo ""
echo "Check that 'pet-store' domain appears with 3 sources:"
echo "  - pet-store-pg  (PostgreSQL, pet_store schema)"
echo "  - petstore-api  (OpenAPI, petstore3.swagger.io)"
echo "  - inquiries-sqlite  (SQLite, demo/files/inquiries.sqlite)"
echo ""
echo "Press Ctrl+C to stop."

cleanup() {
  echo ""
  echo "Shutting down..."
  kill $BACKEND_PID $UI_PID 2>/dev/null || true
  wait $BACKEND_PID $UI_PID 2>/dev/null || true
  if [ "$KEEP_DOCKER" = true ]; then
    echo "Leaving Docker Compose services running (--keep-docker)."
  else
    echo "Stopping Docker Compose services..."
    cd "$SCRIPT_DIR"
    docker compose $COMPOSE_FILES down --remove-orphans
  fi
  echo "Done."
}
trap cleanup EXIT INT TERM

wait
