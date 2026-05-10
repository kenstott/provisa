#!/usr/bin/env bash
# Start Provisa using the install-time configuration (core services only, no dev extras).
# Simulates the installed product: postgres + trino + redis, no kafka/mongo/observability.
# Backend runs locally via uvicorn. UI runs on the host via vite.

set -euo pipefail

KEEP_DOCKER=false
FAST=false
DEMO=false
IDP=""
for arg in "$@"; do
  case "$arg" in
    --keep-docker) KEEP_DOCKER=true ;;
    --fast) FAST=true; KEEP_DOCKER=true ;;
    --demo) DEMO=true ;;
    --idp=*) IDP="${arg#--idp=}" ;;
    *) echo "Unknown option: $arg"; echo "Usage: $0 [--keep-docker] [--fast] [--demo] [--idp=basic|firebase]"; exit 1 ;;
  esac
done
if [ -n "$IDP" ] && [ "$IDP" != "basic" ] && [ "$IDP" != "firebase" ]; then
  echo "Unknown IDP: $IDP. Must be 'basic' or 'firebase'"; exit 1
fi

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
export PETSTORE_BASE_URL="${PETSTORE_BASE_URL:-http://localhost:18080/api/v3}"
export GRAPHQL_DEMO_ENABLED="${GRAPHQL_DEMO_ENABLED:-$DEMO}"
export PROVISA_DEMO="${DEMO}"
export PROVISA_IDP="${IDP}"
if [ "$DEMO" = true ]; then
  export PROVISA_CONFIG="config/provisa-install.yaml"
else
  export PROVISA_CONFIG="config/provisa-install-base.yaml"
fi

# Core + install overlay (port bindings only — no kafka/mongo/elasticsearch/observability)
COMPOSE_FILES="-f docker-compose.core.yml -f docker-compose.dev-install.yml"

if [ "$DEMO" = true ]; then
  COMPOSE_FILES="$COMPOSE_FILES -f docker-compose.observability.yml -f docker-compose.demo.yml"
  # Clear MinIO data so the demo starts with a fresh empty bucket (no stale OTel data).
  echo "Clearing MinIO volume (demo reset)..."
  timeout 5 docker stop --timeout 2 provisa-minio-1 2>/dev/null || true
  timeout 5 docker volume rm provisa_minio_data 2>/dev/null || true
  # Ensure demo files exist (SQLite inquiries DB, etc.)
  if [ -f "$SCRIPT_DIR/demo/files/create_demo_files.py" ]; then
    echo "Generating demo files..."
    "$SCRIPT_DIR/.venv/bin/python" "$SCRIPT_DIR/demo/files/create_demo_files.py" 2>/dev/null || true
  fi
fi

# Remove macOS AppleDouble metadata files that break Docker builds on exFAT volumes
for _build_ctx in "$SCRIPT_DIR" "$SCRIPT_DIR/zaychik" "$SCRIPT_DIR/demo/graphql_server"; do
  [ -d "$_build_ctx" ] && find "$_build_ctx" -name "._*" -not -path "*/.git/*" -not -path "*/.claude/*" -not -path "*/node_modules/*" -maxdepth 5 -delete 2>/dev/null || true
done

if [ "$DEMO" = true ]; then
  echo "Starting Docker Compose services (core + observability + demo)..."
else
  echo "Starting Docker Compose services (core only)..."
fi
cd "$SCRIPT_DIR"
# shellcheck disable=SC2086
docker compose $COMPOSE_FILES up -d || true

CREATED=$(docker ps -a --filter "label=com.docker.compose.project=provisa" \
  --filter "status=created" \
  --format '{{.Label "com.docker.compose.service"}}' 2>/dev/null | sort -u | tr '\n' ' ')
if [ -n "$CREATED" ]; then
  echo "Starting remaining services: $CREATED"
  # shellcheck disable=SC2086
  docker compose $COMPOSE_FILES up -d --no-deps $CREATED || true
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

# On demo reset, MinIO data is deleted but the Iceberg JDBC catalog in Postgres retains
# stale table entries. Clear them so Trino recreates the tables against the fresh bucket.
if [ "$DEMO" = true ]; then
  docker exec provisa-postgres-1 psql -U provisa -d provisa -c "
    DELETE FROM iceberg_tables WHERE catalog_name = 'otel';
    DELETE FROM iceberg_namespace_properties WHERE catalog_name = 'otel';
  " 2>/dev/null || true
fi

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

if [ "$DEMO" = true ]; then
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

  # Seed one order per pet — each animal is unique, so one sale per pet makes sense.
  if curl -sf "${PETSTORE_BASE_URL}/openapi.json" > /dev/null 2>&1; then
    for i in 1 2 3 4 5 6 7 8 9 10; do
      curl -s -X DELETE "${PETSTORE_BASE_URL}/store/order/$i" > /dev/null 2>&1 || true
    done
    for order in \
      '{"id":1,"petId":1,"quantity":1,"status":"delivered","complete":true}' \
      '{"id":2,"petId":2,"quantity":1,"status":"delivered","complete":true}' \
      '{"id":3,"petId":4,"quantity":1,"status":"approved","complete":false}' \
      '{"id":4,"petId":7,"quantity":1,"status":"placed","complete":false}' \
      '{"id":5,"petId":8,"quantity":1,"status":"placed","complete":false}' \
      '{"id":6,"petId":9,"quantity":1,"status":"approved","complete":false}' \
      '{"id":7,"petId":10,"quantity":1,"status":"delivered","complete":true}' \
    ; do
      curl -s -X POST "${PETSTORE_BASE_URL}/store/order" \
        -H "Content-Type: application/json" \
        -d "$order" > /dev/null 2>&1
    done
    echo "Petstore orders seeded."
  else
    echo "Petstore order seed skipped."
  fi
fi

# Kill any existing UI and backend processes, including the uvicorn reloader parent
# (killing only the port-8000 worker leaves the reloader alive to respawn it immediately)
lsof -i :3000 -P -t 2>/dev/null | xargs kill -9 2>/dev/null || true
pkill -9 -f "uvicorn main:app.*--port 8000" 2>/dev/null || true
# Wait until port 8000 is free before starting a new backend
for _i in $(seq 1 15); do
  lsof -i :8000 -P -t 2>/dev/null || break
  sleep 1
done

BACKEND_PID=""

start_backend() {
  cd "$SCRIPT_DIR"
  _BACKEND_ENV=(
    PG_HOST=localhost
    PG_PORT=5432
    PG_DATABASE=provisa
    PG_USER=provisa
    PG_PASSWORD="${PG_PASSWORD:-provisa}"
    POSTGRES_HOST=localhost
    TRINO_HOST=localhost
    TRINO_PORT=8080
    TRINO_FLIGHT_PORT=8480
    REDIS_URL="redis://localhost:6379"
    REDIS_HOST=localhost
    PETSTORE_BASE_URL="${PETSTORE_BASE_URL:-http://localhost:18080/api/v3}"
    GRAPHQL_DEMO_ENABLED="${GRAPHQL_DEMO_ENABLED:-false}"
    PROVISA_DEMO="${DEMO}"
    PROVISA_IDP="${IDP}"
    GRAPHQL_DEMO_URL="http://localhost:4000/graphql"
    PROVISA_CONFIG="${PROVISA_CONFIG}"
    PROVISA_CONFIG_REPLACE="true"
  )
  if [ "$DEMO" = true ]; then
    _BACKEND_ENV+=(
      PROVISA_REDIRECT_ENABLED="true"
      PROVISA_REDIRECT_ENDPOINT="http://localhost:9000"
      PROVISA_REDIRECT_ACCESS_KEY="${PROVISA_REDIRECT_ACCESS_KEY:-minioadmin}"
      PROVISA_REDIRECT_SECRET_KEY="${PROVISA_REDIRECT_SECRET_KEY:-minioadmin}"
      PROVISA_REDIRECT_BUCKET="${PROVISA_REDIRECT_BUCKET:-provisa-results}"
      PROVISA_OTEL_S3_ENDPOINT="http://localhost:9000"
      PROVISA_OTEL_S3_ACCESS_KEY="${PROVISA_OTEL_S3_ACCESS_KEY:-minioadmin}"
      PROVISA_OTEL_S3_SECRET_KEY="${PROVISA_OTEL_S3_SECRET_KEY:-minioadmin}"
      OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4319"
      OTEL_SERVICE_NAME="provisa"
    )
  fi
  env "${_BACKEND_ENV[@]}" \
    "$SCRIPT_DIR/.venv/bin/uvicorn" main:app \
      --reload --reload-dir provisa --reload-dir config \
      --host 0.0.0.0 --port 8000 \
      >> "$LOG_DIR/backend.log" 2>&1 &
  BACKEND_PID=$!
}

restart_backend() {
  echo ""
  echo "Restarting backend (Ctrl-R)..."
  kill "$BACKEND_PID" 2>/dev/null || true
  wait "$BACKEND_PID" 2>/dev/null || true
  start_backend
  echo "Backend restarted (PID $BACKEND_PID)."
}
trap restart_backend USR1

echo "Starting Provisa backend on port 8000..."
start_backend

echo -n "Waiting for Provisa backend"
for i in $(seq 1 90); do
  if curl -sf http://localhost:8000/health > /dev/null 2>&1; then
    echo " OK (PID $BACKEND_PID)"
    break
  fi
  if ! kill -0 "$BACKEND_PID" 2>/dev/null; then
    echo " FAILED"
    echo "Backend crashed. Last logs:"
    tail -20 "$LOG_DIR/backend.log"
    exit 1
  fi
  if [ "$i" -eq 90 ]; then
    echo " TIMEOUT"
    echo "Backend did not become healthy. Last logs:"
    tail -20 "$LOG_DIR/backend.log"
    exit 1
  fi
  echo -n "."
  sleep 2
done

echo "Starting Provisa UI on port 3000..."
find "$SCRIPT_DIR/provisa-ui" -name "._*" -delete 2>/dev/null || true
# Load nvm and switch to the Node version specified in .nvmrc (requires Node 20.19+ or 22+)
export NVM_DIR="${NVM_DIR:-$HOME/.nvm}"
# shellcheck disable=SC1091
[ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh"
if command -v nvm >/dev/null 2>&1; then
  nvm use 2>/dev/null || nvm use 22 2>/dev/null || true
fi
cd "$SCRIPT_DIR/provisa-ui"
VITE_AUTH_ENABLED="${IDP:+true}" npx vite --host 0.0.0.0 --force &
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
if [ "$DEMO" = true ]; then
  echo "Provisa running (demo mode):"
  echo "  Backend: http://localhost:8000  (logs: tail -f $LOG_DIR/backend.log)"
  echo "  UI:      http://localhost:3000"
  echo ""
  echo "Demo sources:"
  echo "  - pet-store-pg       (PostgreSQL, pet_store schema)"
  echo "  - petstore-api       (OpenAPI, petstore3.swagger.io)"
  echo "  - inquiries-sqlite   (SQLite, demo/files/inquiries.sqlite)"
  echo "  - graphql-demo       (GraphQL remote, http://localhost:4000/graphql)"
else
  echo "Provisa running (install mode):"
  echo "  Backend: http://localhost:8000  (logs: tail -f $LOG_DIR/backend.log)"
  echo "  UI:      http://localhost:3000"
  echo ""
  echo "No demo services started. Use --demo to include petstore-mock, graphql-demo, and SQLite ETL."
fi
echo ""
echo "Press Ctrl+C to stop. Press Ctrl+R to restart backend."

cleanup() {
  trap - EXIT INT TERM
  echo ""
  echo "Shutting down..."
  kill $UI_PID "${KEY_READER_PID:-}" "$BACKEND_PID" 2>/dev/null || true
  wait $UI_PID "$BACKEND_PID" 2>/dev/null || true
  if [ "$KEEP_DOCKER" = true ]; then
    echo "Leaving Docker Compose services running (--keep-docker)."
  else
    echo "Stopping Docker Compose services..."
    cd "$SCRIPT_DIR"
    # shellcheck disable=SC2086
    docker compose $COMPOSE_FILES down --remove-orphans
  fi
  echo "Done."
  exit 0
}
trap cleanup EXIT INT TERM

_key_reader() {
  local key
  while true; do
    IFS= read -rsn1 -t 1 key </dev/tty 2>/dev/null || continue
    [[ "$key" == $'\x12' ]] && kill -USR1 $$ 2>/dev/null || true
  done
}
_key_reader &
KEY_READER_PID=$!

while true; do
  wait || true
done
