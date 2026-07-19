#!/usr/bin/env bash
# Start Provisa UI (frontend) and backend API server
# Backend logs go to ./.logs/server.log

set -euo pipefail

SEED_DATA=false
OBSERVABILITY=true
KEEP_DOCKER=false
DEMO=false
RESET_VOLUMES=false
IDP=""
for arg in "$@"; do
  case "$arg" in
    --seed-data) SEED_DATA=true ;;
    --no-observability) OBSERVABILITY=false ;;
    --keep-docker) KEEP_DOCKER=true ;;
    --demo) DEMO=true ;;
    --reset-volumes) RESET_VOLUMES=true ;;
    --idp=*) IDP="${arg#--idp=}" ;;
    *) echo "Unknown option: $arg"; echo "Usage: $0 [--seed-data] [--no-observability] [--keep-docker] [--demo] [--reset-volumes] [--idp=basic|firebase]"; exit 1 ;;
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

# Ensure required env vars are set
export PG_PASSWORD="${PG_PASSWORD:-provisa}"
export REDIS_URL="${REDIS_URL:-redis://localhost:6379}"
export PETSTORE_BASE_URL="${PETSTORE_BASE_URL:-http://localhost:18080/api/v3}"
export GRAPHQL_DEMO_URL="${GRAPHQL_DEMO_URL:-http://localhost:4000/graphql}"
export GRAPHQL_DEMO_ENABLED="${GRAPHQL_DEMO_ENABLED:-$DEMO}"
export PROVISA_DEMO="${DEMO}"
export PROVISA_ENABLE_TEST_ENDPOINTS="${PROVISA_ENABLE_TEST_ENDPOINTS:-$DEMO}"
export PROVISA_IDP="${IDP}"
export PROVISA_REDIRECT_ENABLED="${PROVISA_REDIRECT_ENABLED:-true}"
export PROVISA_REDIRECT_ENDPOINT="${PROVISA_REDIRECT_ENDPOINT:-http://localhost:9000}"
export PROVISA_REDIRECT_ACCESS_KEY="${PROVISA_REDIRECT_ACCESS_KEY:-minioadmin}"
export PROVISA_REDIRECT_SECRET_KEY="${PROVISA_REDIRECT_SECRET_KEY:-minioadmin}"
export PROVISA_REDIRECT_BUCKET="${PROVISA_REDIRECT_BUCKET:-provisa-results}"
export PROVISA_CHANGE_EVENT_BOOTSTRAP="${PROVISA_CHANGE_EVENT_BOOTSTRAP:-localhost:9092}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
export QUERY_ENGINE_CONTAINER="${QUERY_ENGINE_CONTAINER:-provisa-trino-1}"

# Compose files for dev: core services + dev overlay (ports, kafka, mongo, elasticsearch, observability)
COMPOSE_FILES="-f docker-compose.core.yml -f docker-compose.dev.yml"
if [ "$DEMO" = true ]; then
  # Demo servers (petstore-mock, graphql-demo) run as host processes, not Docker.
  echo "Resetting volumes for pristine demo environment..."
  docker compose $COMPOSE_FILES down -v 2>/dev/null || true
  if [ -f "$SCRIPT_DIR/demo/files/create_demo_files.py" ]; then
    echo "Generating demo files..."
    "$SCRIPT_DIR/.venv/bin/python" "$SCRIPT_DIR/demo/files/create_demo_files.py" 2>/dev/null || true
  fi
elif [ "$RESET_VOLUMES" = true ]; then
  echo "Resetting volumes for crash recovery..."
  docker compose $COMPOSE_FILES down -v 2>/dev/null || true
fi

# Start infrastructure services via Docker Compose
echo "Starting Docker Compose services..."
cd "$SCRIPT_DIR"
JVM_CONFIG_PATCHED=false
if [ "$OBSERVABILITY" = true ]; then
  # Download OTel Java agent for Trino if not already present
  OTEL_AGENT="$SCRIPT_DIR/observability/trino-otel/opentelemetry-javaagent.jar"
  if [ ! -f "$OTEL_AGENT" ]; then
    echo "Downloading OTel Java agent for Trino..."
    curl -sL "https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/latest/download/opentelemetry-javaagent.jar" \
      -o "$OTEL_AGENT"
  fi
  # Patch jvm.config so the agent loads inside Trino's JVM (JAVA_TOOL_OPTIONS breaks Vector API)
  JVM_CFG="$SCRIPT_DIR/trino/etc/jvm.config"
  if [ -f "$OTEL_AGENT" ] && ! grep -q "opentelemetry-javaagent" "$JVM_CFG"; then
    cp "$JVM_CFG" "${JVM_CFG}.bak"
    printf '\n-javaagent:/etc/trino/otel/opentelemetry-javaagent.jar\n-Dotel.service.name=trino\n-Dotel.exporter.otlp.endpoint=http://otel-collector:4317\n-Dotel.exporter.otlp.protocol=grpc\n' >> "$JVM_CFG"
    JVM_CONFIG_PATCHED=true
  fi
fi

# Run compose up; suppress exit code — Docker bug: a zombie "dead" postgres container
# (no files on disk, stuck in daemon memory) causes compose to fail and leaves
# dependent services (pgbouncer, debezium, trino, zaychik) in Created state.
docker compose $COMPOSE_FILES up -d 2>&1 || true

# Second pass: start any services still in Created state using --no-deps so the
# zombie postgres dependency check is bypassed.
CREATED=$(docker ps -a --filter "label=com.docker.compose.project=provisa" \
  --filter "status=created" \
  --format '{{.Label "com.docker.compose.service"}}' 2>/dev/null | sort -u | tr '\n' ' ')
if [ -n "$CREATED" ]; then
  echo "Starting remaining services: $CREATED"
  # shellcheck disable=SC2086
  docker compose $COMPOSE_FILES up -d --no-deps $CREATED 2>&1 || true
fi

# Wait for critical services to be healthy
echo -n "Waiting for infrastructure services"
for i in $(seq 1 120); do
  PG_OK=$(docker inspect --format '{{.State.Health.Status}}' provisa-postgres-1 2>/dev/null)
  KF_OK=$(docker inspect --format '{{.State.Health.Status}}' provisa-kafka-1 2>/dev/null)
  REDIS_OK=$(docker inspect --format '{{.State.Health.Status}}' provisa-redis-1 2>/dev/null)
  if [ "$PG_OK" = "healthy" ] && [ "$KF_OK" = "healthy" ] && [ "$REDIS_OK" = "healthy" ]; then
    echo " OK"
    break
  fi
  if [ "$i" -eq 120 ]; then
    echo " TIMEOUT"
    echo "Critical services did not become healthy. postgres=$PG_OK kafka=$KF_OK redis=$REDIS_OK"
    exit 1
  fi
  echo -n "."
  sleep 2
done
echo "Docker Compose services are healthy."
if [ "$OBSERVABILITY" = true ]; then
  echo "  Grafana: http://localhost:3100"
fi

if [ "$DEMO" = true ]; then
  "$SCRIPT_DIR/demo/run-demo-servers.sh" start
  _petstore_ready=true
  if [ "$_petstore_ready" = true ]; then
    _users_seeded=false
    for _attempt in 1 2 3; do
      if curl -sf -X POST "${PETSTORE_BASE_URL}/user/createWithList" \
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
        ]' > /dev/null 2>&1; then
        echo "Petstore users seeded."
        _users_seeded=true
        break
      fi
      sleep 2
    done
    [ "$_users_seeded" = false ] && echo "Petstore user seed skipped."
  fi
  if [ "$_petstore_ready" = true ]; then
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
  fi
fi

# Seed Kafka with demo data (only if --seed-data flag passed)
if [ "$SEED_DATA" = true ] && [ -f "$SCRIPT_DIR/scripts/seed-kafka.py" ] && \
   docker compose $COMPOSE_FILES ps kafka --status running 2>/dev/null | grep -q kafka; then
  echo "Seeding Kafka..."
  "$SCRIPT_DIR/.venv/bin/python" "$SCRIPT_DIR/scripts/seed-kafka.py" 2>/dev/null || true
fi

# Ensure Python dependencies are installed (skip if lockfile unchanged; 30s timeout guards against hangs)
if [ -f "$SCRIPT_DIR/pyproject.toml" ] && [ -d "$SCRIPT_DIR/.venv" ]; then
  LOCK_HASH=$(md5 -q "$SCRIPT_DIR/pyproject.toml" "$SCRIPT_DIR/uv.lock" 2>/dev/null || echo "")
  CACHED_HASH=$(cat "$SCRIPT_DIR/.venv/.dep-hash" 2>/dev/null || echo "none")
  if [ "$LOCK_HASH" != "$CACHED_HASH" ]; then
    echo "Syncing Python dependencies..."
    "$SCRIPT_DIR/.venv/bin/pip" install -e "$SCRIPT_DIR" -q &
    PIP_PID=$!
    sleep 30 &
    SLEEP_PID=$!
    wait -n $PIP_PID $SLEEP_PID 2>/dev/null
    kill $PIP_PID $SLEEP_PID 2>/dev/null
    wait $PIP_PID 2>/dev/null && echo "$LOCK_HASH" > "$SCRIPT_DIR/.venv/.dep-hash" || echo "Warning: dependency sync timed out or failed — continuing"
  fi
fi

# Kill any stale processes on our ports
lsof -i :8001 -P -t 2>/dev/null | xargs kill 2>/dev/null || true
lsof -i :3000 -P -t 2>/dev/null | xargs kill 2>/dev/null || true
sleep 1

# Truncate old log
> "$LOG_DIR/server.log"

_start_backend() {
  if [ "$OBSERVABILITY" = true ]; then
    export OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT:-http://localhost:4317}"
    export OTEL_SERVICE_NAME="${OTEL_SERVICE_NAME:-provisa}"
  fi
  "$SCRIPT_DIR/.venv/bin/uvicorn" main:app --reload --reload-dir provisa --reload-dir config --host 0.0.0.0 --port 8001 \
    >> "$LOG_DIR/server.log" 2>&1 &
  BACKEND_PID=$!
}

restart_backend() {
  echo ""
  echo "Restarting backend (Ctrl-R)..."
  kill "$BACKEND_PID" 2>/dev/null || true
  wait "$BACKEND_PID" 2>/dev/null || true
  rm -f "$LOG_DIR/server.log"
  cd "$SCRIPT_DIR"
  _start_backend
  echo "Backend restarted (PID $BACKEND_PID)"
}
trap restart_backend USR1

echo "Starting Provisa backend on port 8001..."
cd "$SCRIPT_DIR"
_start_backend

# Wait for backend to be healthy
echo -n "  Waiting for backend"
for i in $(seq 1 60); do
  if curl -sf http://localhost:8001/health > /dev/null 2>&1; then
    echo " OK (PID $BACKEND_PID)"
    break
  fi
  # Check if process died
  if ! kill -0 $BACKEND_PID 2>/dev/null; then
    echo " FAILED"
    echo "Backend crashed. Last 20 lines of log:"
    tail -20 "$LOG_DIR/server.log"
    exit 1
  fi
  echo -n "."
  sleep 1
done

# Final check
if ! curl -sf http://localhost:8001/health > /dev/null 2>&1; then
  echo " TIMEOUT"
  echo "Backend did not become healthy. Last 20 lines of log:"
  tail -20 "$LOG_DIR/server.log"
  exit 1
fi

echo "Starting Provisa UI on port 3000..."
cd "$SCRIPT_DIR/provisa-ui"
npx vite --host 0.0.0.0 &
UI_PID=$!

# Wait for UI to be reachable
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
  echo "  Backend: http://localhost:8001  (logs: $LOG_DIR/server.log)"
  echo "  UI:      http://localhost:3000"
  echo ""
  echo "Demo sources:"
  echo "  - pet-store-pg       (PostgreSQL, pet_store schema)"
  echo "  - petstore-api       (OpenAPI, host process, http://localhost:18080/api/v3)"
  echo "  - inquiries-sqlite   (SQLite, demo/files/inquiries.sqlite)"
  echo "  - graphql-demo       (GraphQL remote, http://localhost:4000/graphql)"
else
  echo "Provisa running:"
  echo "  Backend: http://localhost:8001  (logs: $LOG_DIR/server.log)"
  echo "  UI:      http://localhost:3000"
fi
echo ""
echo "Press Ctrl+C to stop. Press Ctrl+R to restart backend."

cleanup() {
  echo ""
  echo "Shutting down..."
  kill $BACKEND_PID $UI_PID "${KEY_READER_PID:-}" 2>/dev/null || true
  wait $BACKEND_PID $UI_PID 2>/dev/null || true
  if [ "$DEMO" = true ]; then
    "$SCRIPT_DIR/demo/run-demo-servers.sh" stop 2>/dev/null || true
  fi
  if [ "$KEEP_DOCKER" = true ]; then
    echo "Leaving Docker Compose services running (--keep-docker)."
  else
    echo "Stopping Docker Compose services..."
    cd "$SCRIPT_DIR"
    docker compose $COMPOSE_FILES down --remove-orphans
  fi
  if [ "$JVM_CONFIG_PATCHED" = true ]; then
    mv "${SCRIPT_DIR}/trino/etc/jvm.config.bak" "${SCRIPT_DIR}/trino/etc/jvm.config"
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
