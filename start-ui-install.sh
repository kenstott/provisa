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
export POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
export REDIS_URL="${REDIS_URL:-redis://localhost:6379}"
export PETSTORE_BASE_URL="${PETSTORE_BASE_URL:-http://localhost:18080/api/v3}"
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
  TRUNCATE pet_store.pets RESTART IDENTITY;
  INSERT INTO pet_store.pets (name, species, breed_name, price, available)
  VALUES
    ('Cat 1',    'cat',    'Siamese',          380.00, TRUE),
    ('Cat 2',    'cat',    'Maine Coon',        450.00, TRUE),
    ('Dog 1',    'dog',    'Golden Retriever',  800.00, TRUE),
    ('Lion 1',   'lion',   'African Lion',     1500.00, FALSE),
    ('Lion 2',   'lion',   'African Lion',     1500.00, TRUE),
    ('Lion 3',   'lion',   'Barbary Lion',     1600.00, TRUE),
    ('Rabbit 1', 'rabbit', 'Holland Lop',       150.00, TRUE);
" 2>/dev/null || echo "pet_store schema setup skipped (will retry on next start)"

# Seed petstore-mock with demo customer names so get_user_by_name lookups succeed
echo -n "Waiting for petstore-mock"
for i in $(seq 1 30); do
  if curl -sf "${PETSTORE_BASE_URL}/openapi.json" > /dev/null 2>&1; then
    echo " OK"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo " TIMEOUT (skipping user seed)"
    break
  fi
  echo -n "."
  sleep 2
done
curl -sf "${PETSTORE_BASE_URL}/openapi.json" > /dev/null 2>&1 && \
curl -s -X POST "${PETSTORE_BASE_URL}/user/createWithList" \
  -H "Content-Type: application/json" \
  -d '[
    {"id":101,"username":"Sara Kim","firstName":"Sara","lastName":"Kim","email":"sara.kim@example.com","password":"demo","phone":"555-0101","userStatus":1},
    {"id":102,"username":"Tom Evans","firstName":"Tom","lastName":"Evans","email":"tom.evans@example.com","password":"demo","phone":"555-0102","userStatus":1},
    {"id":103,"username":"Amy Zhao","firstName":"Amy","lastName":"Zhao","email":"amy.zhao@example.com","password":"demo","phone":"555-0103","userStatus":1},
    {"id":104,"username":"Carlos Ruiz","firstName":"Carlos","lastName":"Ruiz","email":"carlos.ruiz@example.com","password":"demo","phone":"555-0104","userStatus":1},
    {"id":105,"username":"Nina Patel","firstName":"Nina","lastName":"Patel","email":"nina.patel@example.com","password":"demo","phone":"555-0105","userStatus":1},
    {"id":106,"username":"James Park","firstName":"James","lastName":"Park","email":"james.park@example.com","password":"demo","phone":"555-0106","userStatus":1},
    {"id":107,"username":"Lisa Chen","firstName":"Lisa","lastName":"Chen","email":"lisa.chen@example.com","password":"demo","phone":"555-0107","userStatus":1},
    {"id":108,"username":"Mark Torres","firstName":"Mark","lastName":"Torres","email":"mark.torres@example.com","password":"demo","phone":"555-0108","userStatus":1},
    {"id":109,"username":"Jen Wu","firstName":"Jen","lastName":"Wu","email":"jen.wu@example.com","password":"demo","phone":"555-0109","userStatus":1},
    {"id":110,"username":"Derek Hall","firstName":"Derek","lastName":"Hall","email":"derek.hall@example.com","password":"demo","phone":"555-0110","userStatus":1},
    {"id":111,"username":"Rachel Scott","firstName":"Rachel","lastName":"Scott","email":"rachel.scott@example.com","password":"demo","phone":"555-0111","userStatus":1}
  ]' > /dev/null 2>&1 && echo "Petstore users seeded." || echo "Petstore user seed skipped."


pkill -9 -f "uvicorn main:app" 2>/dev/null || true
lsof -i :8001 -P -t 2>/dev/null | xargs kill -9 2>/dev/null || true
lsof -i :3000 -P -t 2>/dev/null | xargs kill -9 2>/dev/null || true
for i in $(seq 1 20); do
  lsof -i :8001 -P -t 2>/dev/null | grep -q . || break
  sleep 0.5
done

> "$LOG_DIR/server-install.log"

_start_backend() {
  "$SCRIPT_DIR/.venv/bin/uvicorn" main:app --reload --reload-dir provisa --reload-dir config --host 0.0.0.0 --port 8001 \
    >> "$LOG_DIR/server-install.log" 2>&1 &
  BACKEND_PID=$!
}

restart_backend() {
  echo ""
  echo "Restarting backend (Ctrl-R)..."
  kill "$BACKEND_PID" 2>/dev/null || true
  wait "$BACKEND_PID" 2>/dev/null || true
  rm -f "$LOG_DIR/server-install.log"
  cd "$SCRIPT_DIR"
  _start_backend
  echo "Backend restarted (PID $BACKEND_PID)"
}
trap restart_backend USR1

echo "Starting Provisa backend on port 8001..."
cd "$SCRIPT_DIR"
_start_backend

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
echo "Press Ctrl+C to stop. Press Ctrl+R to restart backend."

cleanup() {
  echo ""
  echo "Shutting down..."
  kill $BACKEND_PID $UI_PID "${KEY_READER_PID:-}" 2>/dev/null || true
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

_key_reader() {
  local key
  while true; do
    IFS= read -rsn1 -t 0.5 key </dev/tty 2>/dev/null || continue
    [[ "$key" == $'\x12' ]] && kill -USR1 $$ 2>/dev/null || true
  done
}
_key_reader &
KEY_READER_PID=$!

while true; do
  wait || true
done
