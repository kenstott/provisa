#!/usr/bin/env bash
# Start Provisa UI (frontend) and backend API server
# Backend logs go to ./.logs/server.log

set -euo pipefail

RESET_VOLUMES=false
OBSERVABILITY=false
for arg in "$@"; do
  case "$arg" in
    --reset-volumes) RESET_VOLUMES=true ;;
    --observability) OBSERVABILITY=true ;;
    *) echo "Unknown option: $arg"; echo "Usage: $0 [--reset-volumes] [--observability]"; exit 1 ;;
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

# Ensure required env vars are set
export PG_PASSWORD="${PG_PASSWORD:-provisa}"
export REDIS_URL="${REDIS_URL:-redis://localhost:6379}"
export PROVISA_REDIRECT_ENABLED="${PROVISA_REDIRECT_ENABLED:-true}"
export PROVISA_REDIRECT_ENDPOINT="${PROVISA_REDIRECT_ENDPOINT:-http://localhost:9000}"
export PROVISA_REDIRECT_ACCESS_KEY="${PROVISA_REDIRECT_ACCESS_KEY:-minioadmin}"
export PROVISA_REDIRECT_SECRET_KEY="${PROVISA_REDIRECT_SECRET_KEY:-minioadmin}"
export PROVISA_REDIRECT_BUCKET="${PROVISA_REDIRECT_BUCKET:-provisa-results}"
export PROVISA_CHANGE_EVENT_BOOTSTRAP="${PROVISA_CHANGE_EVENT_BOOTSTRAP:-localhost:9092}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

# Recreate data volumes if requested (useful after Docker crashes)
if [ "$RESET_VOLUMES" = true ]; then
  echo "WARNING: Resetting all volumes. All data will be lost."
  sleep 3
  cd "$SCRIPT_DIR"
  ALL_PROFILES="--profile observability --profile neo4j --profile sparql --profile dev"
  docker compose $ALL_PROFILES down --remove-orphans
  docker compose $ALL_PROFILES down -v --remove-orphans
  echo "Data volumes removed. Containers will reinitialize from scratch."
fi

# Start infrastructure services via Docker Compose
echo "Starting Docker Compose services..."
cd "$SCRIPT_DIR"
COMPOSE_PROFILES="--profile dev"
JVM_CONFIG_PATCHED=false
if [ "$OBSERVABILITY" = true ]; then
  COMPOSE_PROFILES="--profile dev --profile observability"
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
docker compose $COMPOSE_PROFILES up -d --wait
echo "Docker Compose services are healthy."
if [ "$OBSERVABILITY" = true ]; then
  echo "  Grafana: http://localhost:3100"
fi

# Seed Kafka with demo data (only if Kafka is running)
if [ -f "$SCRIPT_DIR/scripts/seed-kafka.py" ] && \
   docker compose ps kafka --status running 2>/dev/null | grep -q kafka; then
  echo "Seeding Kafka..."
  "$SCRIPT_DIR/.venv/bin/python" "$SCRIPT_DIR/scripts/seed-kafka.py" 2>/dev/null || true
fi

# Ensure Python dependencies are installed
if [ -f "$SCRIPT_DIR/pyproject.toml" ] && [ -d "$SCRIPT_DIR/.venv" ]; then
  echo "Syncing Python dependencies..."
  "$SCRIPT_DIR/.venv/bin/pip" install -e "$SCRIPT_DIR" -q
fi

# Kill any stale processes on our ports
lsof -i :8001 -P -t 2>/dev/null | xargs kill 2>/dev/null || true
lsof -i :3000 -P -t 2>/dev/null | xargs kill 2>/dev/null || true
sleep 1

# Truncate old log
> "$LOG_DIR/server.log"

echo "Starting Provisa backend on port 8001..."
cd "$SCRIPT_DIR"
if [ "$OBSERVABILITY" = true ]; then
  export OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT:-http://localhost:4317}"
  export OTEL_SERVICE_NAME="${OTEL_SERVICE_NAME:-provisa}"
fi
"$SCRIPT_DIR/.venv/bin/uvicorn" main:app --reload --reload-dir provisa --reload-dir config --host 0.0.0.0 --port 8001 \
  >> "$LOG_DIR/server.log" 2>&1 &
BACKEND_PID=$!

# Wait for backend to be healthy
echo -n "  Waiting for backend"
for i in $(seq 1 30); do
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
echo "Provisa running:"
echo "  Backend: http://localhost:8001  (logs: $LOG_DIR/server.log)"
echo "  UI:      http://localhost:3000"
echo ""
echo "Press Ctrl+C to stop."

cleanup() {
  echo ""
  echo "Shutting down..."
  kill $BACKEND_PID $UI_PID 2>/dev/null || true
  wait $BACKEND_PID $UI_PID 2>/dev/null || true
  echo "Stopping Docker Compose services..."
  docker compose --profile dev --profile observability --profile neo4j --profile sparql -f "$SCRIPT_DIR/docker-compose.yml" down --remove-orphans
  if [ "$JVM_CONFIG_PATCHED" = true ]; then
    mv "${SCRIPT_DIR}/trino/etc/jvm.config.bak" "${SCRIPT_DIR}/trino/etc/jvm.config"
  fi
  echo "Done."
}
trap cleanup EXIT INT TERM

wait
