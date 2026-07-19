#!/usr/bin/env bash
# Start Provisa using the install-time configuration (core services only, no dev extras).
# Simulates the installed product: postgres + trino + redis, no kafka/mongo/observability.
# Backend runs locally via uvicorn. UI runs on the host via vite.

set -euo pipefail

KEEP_DOCKER=false
FAST=false
DEMO=false
NATIVE=false
IDP=""
for arg in "$@"; do
  case "$arg" in
    --keep-docker) KEEP_DOCKER=true ;;
    --fast) FAST=true; KEEP_DOCKER=true ;;
    --demo) DEMO=true; NATIVE=true ;;  # demo is always native: no Docker, in-process engine + SQLite control plane
    --native) NATIVE=true ;;
    --idp=*) IDP="${arg#--idp=}" ;;
    *) echo "Unknown option: $arg"; echo "Usage: $0 [--keep-docker] [--fast] [--demo] [--native] [--idp=basic|firebase]"; exit 1 ;;
  esac
done
if [ -n "$IDP" ] && [ "$IDP" != "basic" ] && [ "$IDP" != "firebase" ]; then
  echo "Unknown IDP: $IDP. Must be 'basic' or 'firebase'"; exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/.logs"
mkdir -p "$LOG_DIR"

# Print PIDs holding a TCP port. macOS ships no `timeout` binary and `lsof -i`
# can block while scanning a process whose fds are stuck on a hung mount — such
# an lsof enters uninterruptible (U) state and ignores SIGKILL, so we must NOT
# `wait` on it. Poll for completion up to ~3s, then move on (empty on timeout),
# leaking the stuck lsof rather than hanging the script.
lsof_pids() {
  local port="$1" tmp lp _w
  tmp=$(mktemp 2>/dev/null) || tmp="${TMPDIR:-/tmp}/.provisa-lsof.$$"
  # -S 2: bound lsof's own kernel stat/readlink calls with a 2s alarm so a stalled
  # network mount (smbfs) can't wedge it in uninterruptible (U) state — without this,
  # lsof scanning a process with an fd on a hung SMB share becomes an unkillable zombie.
  lsof -S 2 -bnP -i ":$port" -t >"$tmp" 2>/dev/null &
  lp=$!
  for _w in $(seq 1 6); do
    kill -0 "$lp" 2>/dev/null || break
    sleep 0.5
  done
  kill -9 "$lp" 2>/dev/null || true   # may be uninterruptible; never wait on it
  cat "$tmp" 2>/dev/null || true
  rm -f "$tmp" 2>/dev/null || true
}

# True (0) if something is listening on 127.0.0.1:$1. Uses an `nc -z` connect probe
# (1s timeout) — it touches only the loopback socket, never the process/fd table, so
# a stalled network mount (smbfs) cannot wedge it the way `lsof -i` can. This is the
# preferred "is the port free yet?" check; lsof_pids() is reserved for the rare case
# where we must map a still-occupied port to an unknown PID to evict it.
port_in_use() {
  local port="$1"
  nc -z -G 1 127.0.0.1 "$port" >/dev/null 2>&1
}

# Wait up to $2 seconds for port $1 to become free, probing once per second without
# scanning fds. Returns 0 when free, 1 on timeout.
wait_port_free() {
  local port="$1" secs="$2" _i
  for _i in $(seq 1 "$secs"); do
    port_in_use "$port" || return 0
    sleep 1
  done
  ! port_in_use "$port"
}

# Running under WSL? (Windows kernel string leaks into /proc/version). This matters because Claude
# Desktop runs on the Windows side, across the WSL VM boundary.
is_wsl() { grep -qiE "microsoft|wsl" /proc/version 2>/dev/null; }

# Probe the MCP port's REAL scheme — never guess from env. The runtime scheme depends on more than
# PROVISA_MCP_TLS (TLS also needs a cert to have been created, else the server falls back to http),
# so the only truth is what's listening: a successful TLS handshake => https, otherwise http.
_mcp_scheme() {
  local port="$1"
  curl -sk -o /dev/null --max-time 2 "https://localhost:$port/mcp" 2>/dev/null && echo https || echo http
}

# Print the MCP connect line + (under WSL) the explanation the WSL/Windows split demands.
print_mcp_info() {
  [ -n "${PROVISA_MCP_PORT:-}" ] || return 0
  local port="$PROVISA_MCP_PORT" scheme host
  scheme=$(_mcp_scheme "$port")
  if is_wsl; then
    # The backend runs in the WSL VM; Claude Desktop runs on Windows. Windows CANNOT reliably reach
    # a WSL server over `localhost` — the WSL2 localhost relay accepts the TCP connect but drops
    # MCP's streamed (SSE) response (connect-then-ReadError). So the backend binds 0.0.0.0 (see
    # PROVISA_MCP_HOST default below) and Windows must connect to the WSL VM IP directly.
    host=$(hostname -I 2>/dev/null | awk '{print $1}'); host="${host:-<wsl-ip>}"
    echo "  mcp:     ${scheme}://${host}:${port}/mcp   ← use this WSL IP, NOT localhost"
    echo ""
    echo "  Connect Claude Desktop (Windows) to this WSL backend:"
    echo "    1. On Windows, install a bridge:  pip install mcp-proxy   (needs Windows Python)"
    echo "    2. Edit %APPDATA%\\Claude\\claude_desktop_config.json (Claude → Settings → Developer →"
    echo "       Edit Config), then restart Claude:"
    echo "         {\"mcpServers\":{\"provisa\":{"
    echo "           \"command\":\"C:\\\\path\\\\to\\\\python.exe\","
    echo "           \"args\":[\"-m\",\"mcp_proxy\",\"--transport\",\"streamablehttp\",\"${scheme}://${host}:${port}/mcp\"]}}}"
    echo "    Notes: use the ABSOLUTE python.exe path (the 'python3' Store alias fails to spawn);"
    echo "           the WSL IP changes on WSL restart (re-check: wsl hostname -I);"
    echo "           localhost DOES work here inside WSL but NOT from Windows Claude Desktop."
    echo "    The native Windows installer needs none of this — it runs on Windows (127.0.0.1)."
  else
    echo "  mcp:     ${scheme}://localhost:${port}/mcp  (Model Context Protocol; role via OAuth or PROVISA_MCP_ROLE)"
  fi
}

# True if the given path lives on an exFAT volume (where macOS AppleDouble "._*"
# files are created and break Docker build contexts). APFS/HFS+ don't need cleanup.
is_exfat() {
  case "$(uname -s)" in
    Darwin)
      local dev
      dev=$(df "$1" 2>/dev/null | awk 'NR==2 {print $1}')
      [ -n "$dev" ] && diskutil info "$dev" 2>/dev/null | grep -qiE "Personality:.*ExFAT|Bundle\): *exfat"
      ;;
    *)
      # Linux/BSD: GNU stat reports the filesystem type name directly.
      [ "$(stat -f -c %T "$1" 2>/dev/null)" = "exfat" ]
      ;;
  esac
}

# Singleton: a second copy of this script would share ~/.provisa-server-version,
# and each instance's version watcher cross-triggers backend restarts in the others
# (restart storm). Kill any previous instance and the process group it owns
# (backend + UI + watchers) before starting.
for _other in $(pgrep -f "start-ui-install.sh" 2>/dev/null); do
  [ "$_other" = "$$" ] && continue
  echo "Stopping previous start-ui-install.sh instance (PID $_other) and its services..."
  kill -CONT "-$_other" 2>/dev/null || true   # un-stop the process group if suspended
  kill -9 "-$_other" 2>/dev/null || true       # kill whole group: script + uvicorn + vite + watchers
  kill -9 "$_other" 2>/dev/null || true         # and the leader directly, if not a group leader
done

# Download GovData JAR from GitHub Packages if not present
GOVDATA_JAR="$SCRIPT_DIR/lib/calcite-govdata-all.jar"
if [ ! -f "$GOVDATA_JAR" ]; then
  mkdir -p "$SCRIPT_DIR/lib"
  _GOVDATA_TOKEN="${GITHUB_TOKEN:-$(gh auth token 2>/dev/null || true)}"
  if [ -n "$_GOVDATA_TOKEN" ]; then
    echo -n "Downloading GovData JAR..."
    _GOVDATA_BASE="https://maven.pkg.github.com/kenstott/calcite/ai/askamerica/askamerica-engine"
    _GOVDATA_META=$(curl -fsSL -H "Authorization: Bearer $_GOVDATA_TOKEN" "$_GOVDATA_BASE/maven-metadata.xml" 2>/dev/null || true)
    # grep exits 1 when the metadata is empty/inaccessible (token without package access). Under
    # `set -euo pipefail` that unguarded pipeline killed the whole installer right after printing
    # "Downloading GovData JAR..." — GovData is an OPTIONAL subscription, so guard the probe and let
    # the ${:-} default + the graceful curl-FAILED path below handle an unavailable package.
    _GOVDATA_VER=$(echo "$_GOVDATA_META" | grep -oE '<version>0\.[0-9]+\.[0-9]+</version>' | tail -1 | sed 's/<\/*version>//g' || true)
    _GOVDATA_VER="${_GOVDATA_VER:-0.9.15}"
    _GOVDATA_URL="$_GOVDATA_BASE/$_GOVDATA_VER/askamerica-engine-${_GOVDATA_VER}.jar"
    if curl -fsSL \
        -H "Authorization: Bearer $_GOVDATA_TOKEN" \
        -o "$GOVDATA_JAR" \
        "$_GOVDATA_URL"; then
      echo " OK"
    else
      echo " FAILED (GovData subscriptions unavailable)"
      rm -f "$GOVDATA_JAR"
    fi
  else
    echo "Warning: no GitHub token — set GITHUB_TOKEN or run 'gh auth login'. GovData subscriptions unavailable."
  fi
fi

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
export PROVISA_ENABLE_TEST_ENDPOINTS="${PROVISA_ENABLE_TEST_ENDPOINTS:-$DEMO}"
export PROVISA_IDP="${IDP}"
if [ "$DEMO" = true ]; then
  export PROVISA_CONFIG="config/provisa-install.yaml"
else
  export PROVISA_CONFIG="config/provisa-install-base.yaml"
fi

# Core + install overlay (port bindings only — no kafka/mongo/elasticsearch/observability)
COMPOSE_FILES="-f docker-compose.core.yml -f docker-compose.dev-install.yml"

if [ "$DEMO" = true ]; then
  # Demo servers (petstore-mock, graphql-demo) run as host processes, not Docker.
  COMPOSE_FILES="$COMPOSE_FILES -f docker-compose.observability.yml"
  echo "Resetting volumes for pristine demo environment..."
  docker compose $COMPOSE_FILES down -v 2>/dev/null || true
  # The demo control plane is file-based SQLite — wipe it so every start is pristine (session-created
  # sources/tables/views are cleared and rebuilt from the config). Data files are regenerated below.
  rm -f "${PROVISA_HOME:-$HOME/.provisa}/demo/tenant.db" "${PROVISA_HOME:-$HOME/.provisa}/demo/platform.db"
  # Ensure demo files exist (SQLite inquiries DB, etc.)
  if [ -f "$SCRIPT_DIR/demo/files/create_demo_files.py" ]; then
    echo "Generating demo files..."
    "$SCRIPT_DIR/.venv/bin/python" "$SCRIPT_DIR/demo/files/create_demo_files.py" 2>/dev/null || true
  fi
fi

# Remove macOS AppleDouble metadata files that break Docker builds — only on exFAT
# volumes, where they're created. Skips the (slow) recursive find on APFS/HFS+.
if is_exfat "$SCRIPT_DIR"; then
  echo "exFAT volume detected — clearing AppleDouble (._*) files from build contexts..."
  for _build_ctx in "$SCRIPT_DIR" "$SCRIPT_DIR/zaychik"; do
    [ -d "$_build_ctx" ] && find "$_build_ctx" -name "._*" -not -path "*/.git/*" -not -path "*/.claude/*" -not -path "*/node_modules/*" -maxdepth 5 -delete 2>/dev/null || true
  done
fi

if [ "$NATIVE" = true ]; then
  echo "Native mode (--native): no Docker — embedded control-plane PG + DuckDB engine + fakeredis."
fi
if [ "$NATIVE" = false ]; then
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
for i in $(seq 1 270); do
  TRINO_OK=$(docker inspect --format '{{.State.Health.Status}}' provisa-trino-1 2>/dev/null || echo "missing")
  if [ "$TRINO_OK" = "healthy" ]; then
    echo " OK"
    break
  fi
  if [ "$i" -eq 270 ]; then
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
fi  # end NATIVE=false Docker block

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

  # Seed one order per pet — each animal is unique, so one sale per pet makes sense.
  if [ "$_petstore_ready" = true ] && curl -sf "${PETSTORE_BASE_URL}/pet/findByStatus?status=available" > /dev/null 2>&1; then
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

# Kill any existing UI and backend processes.
# uvicorn --reload spawns a reloader parent + a multiprocessing-forked worker child.
# The worker has a different cmdline so pattern-kill on "uvicorn main:app" misses it.
# If the reloader is already dead the worker is re-parented to PID 1 — must kill by port.
echo -n "Stopping any previous UI/backend processes (ports 3000/8000)"
# A prior run suspended with Ctrl-Z leaves its backend + reload worker STOPPED
# (T state) but still bound to port 8000. SIGCONT first so they can actually
# die, then kill every reloader AND its worker children (there may be several).
pkill -CONT -f "uvicorn main:app" 2>/dev/null || true
for _pid in $(pgrep -f "uvicorn main:app" 2>/dev/null); do
  pkill -9 -P "$_pid" 2>/dev/null || true   # multiprocessing worker children
  kill -9 "$_pid" 2>/dev/null || true       # the reloader itself
done
# Un-stop and kill any prior Vite/UI process group holding port 3000 by name (no fd
# scan). Only fall back to the lsof PID-by-port lookup if the port is STILL held —
# i.e. an orphan we can't match by command — so a normal restart never scans SMB.
pkill -CONT -f "node.*vite" 2>/dev/null || true
pkill -9 -f "node.*vite" 2>/dev/null || true
port_in_use 3000 && lsof_pids 3000 | xargs kill -9 2>/dev/null || true
# Same for port 8000: the uvicorn pattern-kill above handles the common case; only
# probe-then-lsof if a straggler (e.g. a reload worker re-parented to PID 1) lingers.
port_in_use 8000 && lsof_pids 8000 | xargs kill -9 2>/dev/null || true
for _i in $(seq 1 10); do
  port_in_use 8000 || break
  echo -n "."
  sleep 1
done
echo " OK"

# Telemetry (ops) store — default embedded DuckDB (no separate process, no
# startup delay). Set PROVISA_OPS_DB_URL to point telemetry at another store
# (e.g. the embedded PostgreSQL via `provisa.observability.telemetry_pg start`,
# or a warehouse) when volume warrants.
TELEM_OPS_URL="${PROVISA_OPS_DB_URL:-}"
if [ -n "$TELEM_OPS_URL" ]; then
  echo "Telemetry store: $TELEM_OPS_URL (PROVISA_OPS_DB_URL)"
else
  echo "Telemetry store: DuckDB default (embedded)"
fi

# Native (no-Docker) tier: the control plane is an embedded PostgreSQL (pgserver),
# the federation engine is in-process DuckDB, and the cache is embedded fakeredis —
# no postgres/trino/redis/zaychik/minio containers. Backend PG env points at the
# embedded instance's unix socket. Defaults (Docker mode) are localhost:5432.
CP_PG_HOST=localhost
CP_PG_PORT=5432
# Human-readable control-plane store, reported on startup.
CP_STORE_DESC=""
if [ "$DEMO" = true ]; then
  # Demo control plane: file-based SQLite (no pgserver) — instant, zero external process, and reset
  # by simply wiping the files above. The SQLAlchemy control-plane abstraction runs the same code
  # path on SQLite (REQ-837). Tenant + platform registries share one directory (single-tenant).
  CP_SQLITE_DIR="${PROVISA_HOME:-$HOME/.provisa}/demo"
  mkdir -p "$CP_SQLITE_DIR"
  export TENANT_DATABASE_URL="sqlite+aiosqlite:///$CP_SQLITE_DIR/tenant.db"
  export PLATFORM_DATABASE_URL="sqlite+aiosqlite:///$CP_SQLITE_DIR/platform.db"
  CP_STORE_DESC="SQLite files under $CP_SQLITE_DIR (platform.db + tenant.db)"
  echo "Control plane: $CP_STORE_DESC"
elif [ "$NATIVE" = true ]; then
  CP_PG_DIR="${PROVISA_HOME:-$HOME/.provisa}/control-pg"
  echo -n "Booting embedded control-plane PostgreSQL ($CP_PG_DIR)... "
  if _CP_OUT="$("$SCRIPT_DIR/.venv/bin/python" -m provisa.core.control_plane_pg start "$CP_PG_DIR" --init-sql db/init.sql 2>>"$LOG_DIR/control-plane-pg.log")"; then
    CP_PG_HOST="$(echo "$_CP_OUT" | sed -n 's/^PG_HOST=//p')"
    CP_PG_PORT="$(echo "$_CP_OUT" | sed -n 's/^PG_PORT=//p')"
    echo "OK (socket $CP_PG_HOST:$CP_PG_PORT)"
    # Both SQLAlchemy control planes (tenant + platform) connect to the embedded
    # instance over its unix socket — the directory travels in ?host=. The tenant
    # and platform planes share this one embedded database (single-tenant desktop).
    export TENANT_DATABASE_URL="postgresql+asyncpg://provisa:provisa@/provisa?host=${CP_PG_HOST}"
    export PLATFORM_DATABASE_URL="postgresql+asyncpg://provisa:provisa@/provisa?host=${CP_PG_HOST}"
    CP_STORE_DESC="embedded PostgreSQL (pgserver, socket $CP_PG_HOST:$CP_PG_PORT)"
  else
    echo "FAILED — see $LOG_DIR/control-plane-pg.log"
    echo "Native bring-up requires pgserver (Python <=3.12). Aborting."
    exit 1
  fi
else
  CP_STORE_DESC="PostgreSQL container (localhost:${CP_PG_PORT})"
fi

BACKEND_PID=""

start_backend() {
  cd "$SCRIPT_DIR"
  _BACKEND_ENV=(
    PG_HOST="$CP_PG_HOST"
    PG_PORT="$CP_PG_PORT"
    PG_DATABASE=provisa
    PG_USER=provisa
    PG_PASSWORD="${PG_PASSWORD:-provisa}"
    POSTGRES_HOST="$CP_PG_HOST"
    TRINO_HOST=localhost
    TRINO_PORT=8080
    TRINO_FLIGHT_PORT=8480
    REDIS_URL="redis://localhost:6379"
    REDIS_HOST=localhost
    PETSTORE_BASE_URL="${PETSTORE_BASE_URL:-http://localhost:18080/api/v3}"
    GRAPHQL_DEMO_ENABLED="${GRAPHQL_DEMO_ENABLED:-false}"
    PROVISA_DEMO="${DEMO}"
    PROVISA_ENABLE_TEST_ENDPOINTS="${PROVISA_ENABLE_TEST_ENDPOINTS:-$DEMO}"
    PROVISA_IDP="${IDP}"
    GRAPHQL_DEMO_URL="http://localhost:4000/graphql"
    PROVISA_CONFIG="${PROVISA_CONFIG}"
    PROVISA_CONFIG_REPLACE="true"
    PROVISA_PGWIRE_PORT=5439
    # MCP on by default in dev too (parity with the native tier) so the Explore -> MCP panel and the
    # config-file/mcp-proxy bridge work here. Plain HTTP: Claude Desktop reaches a local server only
    # via the stdio bridge (works over http); its custom-connector URL path is brokered from Anthropic
    # (public HTTPS only). TLS stays opt-in via PROVISA_MCP_TLS=1. All overridable via env/.env.
    PROVISA_MCP_PORT="${PROVISA_MCP_PORT:-8009}"
    # Bind 0.0.0.0 under WSL so Windows Claude Desktop can reach the backend via the WSL VM IP
    # (the localhost relay drops MCP's streamed response); loopback-only otherwise.
    PROVISA_MCP_HOST="${PROVISA_MCP_HOST:-$(is_wsl && echo 0.0.0.0 || echo 127.0.0.1)}"
    PROVISA_MCP_ROLE="${PROVISA_MCP_ROLE:-admin}"
    PROVISA_MCP_BRIDGE_COMMAND="${PROVISA_MCP_BRIDGE_COMMAND:-$SCRIPT_DIR/.venv/bin/python}"
  )
  # Native tier: in-process DuckDB engine + embedded fakeredis, so no Trino/Redis
  # server is contacted. (Docker mode keeps the Trino engine and real Redis above.)
  if [ "$NATIVE" = true ]; then
    _BACKEND_ENV+=( PROVISA_ENGINE=duckdb PROVISA_REDIS_EMBEDDED=1 )
  fi
  # Telemetry lands in its own embedded-pg instance when available.
  [ -n "${TELEM_OPS_URL:-}" ] && _BACKEND_ENV+=( PROVISA_OPS_DB_URL="$TELEM_OPS_URL" )
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
  pkill -9 -P "$BACKEND_PID" 2>/dev/null || true
  kill -9 "$BACKEND_PID" 2>/dev/null || true
  wait "$BACKEND_PID" 2>/dev/null || true
  # We just killed BACKEND_PID and its workers by PID above; only reach for the
  # fd-scanning lsof lookup if port 8000 is somehow still held (orphan re-parented
  # to PID 1). Probe with /dev/tcp first so the common path never scans SMB.
  port_in_use 8000 && lsof_pids 8000 | xargs kill -9 2>/dev/null || true
  wait_port_free 8000 10 || true
  start_backend
  echo "Backend restarted (PID $BACKEND_PID)."
}
trap restart_backend USR1

echo "Starting Provisa backend on port 8000..."
start_backend

# Stream the backend's startup-phase log lines to the console so the wait isn't a
# silent black box (the backend logs to backend.log, not this terminal).
echo "Waiting for Provisa backend — startup phases:"
# Reap any follower orphaned by an earlier interrupted run. Without this, each
# surviving `tail -f backend.log | grep` re-prints every appended startup line, so
# the console shows one copy per leaked follower.
pkill -f "tail -n0 -f ${LOG_DIR}/backend.log" 2>/dev/null || true
# Wrap the tail|grep pipeline in a subshell so $! is the subshell (the pipeline's
# parent). `$!` on a bare `tail | grep &` captures grep, not tail — leaking the tail
# follower as a live child that the final `wait` then blocks on forever. Killing the
# subshell's children (below) reaps both tail and grep.
( tail -n0 -f "$LOG_DIR/backend.log" 2>/dev/null \
  | grep --line-buffered -E "startup phase|introspect_tables|Catalog .* not yet ready|Application startup complete" ) &
BACKEND_TAIL_PID=$!
_backend_ok=false
for i in $(seq 1 90); do
  if curl -sf http://localhost:8000/health > /dev/null 2>&1; then
    _backend_ok=true
    break
  fi
  if ! kill -0 "$BACKEND_PID" 2>/dev/null; then
    pkill -P "$BACKEND_TAIL_PID" 2>/dev/null || true
    kill "$BACKEND_TAIL_PID" 2>/dev/null || true
    echo "Backend crashed. Last logs:"
    tail -20 "$LOG_DIR/backend.log"
    exit 1
  fi
  sleep 2
done
pkill -P "$BACKEND_TAIL_PID" 2>/dev/null || true   # reap the tail+grep inside the subshell
kill "$BACKEND_TAIL_PID" 2>/dev/null || true
wait "$BACKEND_TAIL_PID" 2>/dev/null || true
BACKEND_TAIL_PID=""
if [ "$_backend_ok" = true ]; then
  echo "Backend healthy (PID $BACKEND_PID)"
else
  echo "Backend did not become healthy. Last logs:"
  tail -20 "$LOG_DIR/backend.log"
  exit 1
fi

echo "Starting Provisa UI on port 3000..."
is_exfat "$SCRIPT_DIR/provisa-ui" && find "$SCRIPT_DIR/provisa-ui" -name "._*" -delete 2>/dev/null || true
# Load nvm and switch to the Node version specified in .nvmrc (requires Node 20.19+ or 22+)
export NVM_DIR="${NVM_DIR:-$HOME/.nvm}"
# shellcheck disable=SC1091
[ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh"
if command -v nvm >/dev/null 2>&1; then
  nvm use 2>/dev/null || nvm use 22 2>/dev/null || true
fi
start_ui() {
  cd "$SCRIPT_DIR/provisa-ui"
  : > "$LOG_DIR/ui.log"
  VITE_AUTH_ENABLED="${IDP:+true}" npx vite --host 0.0.0.0 \
    >> "$LOG_DIR/ui.log" 2>&1 &
  UI_PID=$!
}

# Stream Vite's build output live while polling port 3000. Returns 0 when the
# dev server answers, 1 if the process died, 2 on timeout (still building).
wait_for_ui() {
  echo "Building UI with Vite (live output below)..."
  tail -n +1 -f "$LOG_DIR/ui.log" 2>/dev/null &
  UI_TAIL_PID=$!
  local rc=2
  for _i in $(seq 1 60); do
    if curl -sf http://localhost:3000 > /dev/null 2>&1; then rc=0; break; fi
    if ! kill -0 "$UI_PID" 2>/dev/null; then rc=1; break; fi
    sleep 1
  done
  kill "$UI_TAIL_PID" 2>/dev/null || true
  wait "$UI_TAIL_PID" 2>/dev/null || true
  UI_TAIL_PID=""
  case "$rc" in
    0) echo "UI ready on http://localhost:3000 (PID $UI_PID)" ;;
    1) echo "UI dev server crashed — see $LOG_DIR/ui.log" ;;
    2) echo "UI still building after 60s — continuing; see $LOG_DIR/ui.log" ;;
  esac
  return "$rc"
}

restart_ui() {
  echo ""
  echo "Restarting UI with clean Vite cache (Ctrl-U)..."
  kill "$UI_PID" 2>/dev/null || true
  wait "$UI_PID" 2>/dev/null || true
  pkill -9 -f "node.*vite" 2>/dev/null || true
  # Only scan fds (lsof) if the port is still held after the name-based kill above.
  port_in_use 3000 && lsof_pids 3000 | xargs kill -9 2>/dev/null || true
  # Clear Vite's compiled-config temp and dep-optimization cache so the next
  # start recompiles vite.config.ts and re-bundles deps from scratch.
  rm -rf "$SCRIPT_DIR/provisa-ui/node_modules/.vite-temp" \
         "$SCRIPT_DIR/provisa-ui/node_modules/.vite"
  echo "Vite cache cleared."
  start_ui
  wait_for_ui || true
}
trap restart_ui USR2

start_ui
_ui_rc=0
wait_for_ui || _ui_rc=$?
[ "$_ui_rc" -eq 1 ] && exit 1

echo ""
if [ "$DEMO" = true ]; then
  echo "Provisa running (demo mode):"
  echo "  Backend: http://localhost:8000  (logs: tail -f $LOG_DIR/backend.log)"
  echo "  UI:      http://localhost:3000"
  echo "  Control plane (platform + tenant registries): ${CP_STORE_DESC:-unknown}"
  echo "  pgwire:  postgresql://admin:ignored@localhost:5439/provisa  (username = role)"
  [ -n "${PROVISA_BOLT_PORT:-}" ] && echo "  bolt:    bolt://localhost:${PROVISA_BOLT_PORT}  (username = role)"
  print_mcp_info
  echo ""
  echo "Demo sources:"
  echo "  - pet-store-pg       (PostgreSQL, pet_store schema)"
  echo "  - petstore-api       (OpenAPI, host process, http://localhost:18080/api/v3)"
  echo "  - inquiries-sqlite   (SQLite, demo/files/inquiries.sqlite)"
  echo "  - graphql-demo       (GraphQL remote, http://localhost:4000/graphql)"
else
  echo "Provisa running (install mode):"
  echo "  Backend: http://localhost:8000  (logs: tail -f $LOG_DIR/backend.log)"
  echo "  UI:      http://localhost:3000"
  echo "  Control plane (platform + tenant registries): ${CP_STORE_DESC:-unknown}"
  echo "  pgwire:  postgresql://admin:ignored@localhost:5439/provisa  (username = role)"
  [ -n "${PROVISA_BOLT_PORT:-}" ] && echo "  bolt:    bolt://localhost:${PROVISA_BOLT_PORT}  (username = role)"
  print_mcp_info
  echo ""
  echo "No demo services started. Use --demo to include petstore-mock, graphql-demo, and SQLite ETL."
fi
echo ""
echo "Press Ctrl+C to stop (and tear down Docker). Press Ctrl+E to stop the Python + UI servers but leave Docker running. Press Ctrl+R to restart backend. Press Ctrl+U to clear the Vite cache and restart the UI. Run 'touch ~/.provisa-server-version' to restart after Python edits."

cleanup() {
  trap - EXIT INT TERM
  echo ""
  echo "Shutting down..."
  # Reap children of the backend-tail subshell (tail+grep) before killing it, else
  # the `tail -f` follower survives as an orphan.
  [ -n "${BACKEND_TAIL_PID:-}" ] && pkill -P "$BACKEND_TAIL_PID" 2>/dev/null || true
  kill $UI_PID "${UI_TAIL_PID:-}" "${BACKEND_TAIL_PID:-}" "${KEY_READER_PID:-}" "${VERSION_WATCHER_PID:-}" "$BACKEND_PID" 2>/dev/null || true
  wait $UI_PID "$BACKEND_PID" 2>/dev/null || true
  if [ "$DEMO" = true ]; then
    "$SCRIPT_DIR/demo/run-demo-servers.sh" stop 2>/dev/null || true
  fi
  if [ "$NATIVE" = true ]; then
    # Embedded control-plane + telemetry PostgreSQL are persistent (reused next run);
    # leave them running. No Docker to tear down.
    echo "Native mode: leaving embedded PostgreSQL instances running for next start."
  elif [ "$KEEP_DOCKER" = true ]; then
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

# Ctrl-E: stop the Python + UI servers but leave the Docker services running.
# Sets KEEP_DOCKER so the EXIT trap (cleanup) skips `docker compose down`.
stop_servers_keep_docker() {
  echo ""
  echo "Stopping Python + UI servers (Ctrl-E), leaving Docker services running..."
  KEEP_DOCKER=true
  exit 0
}
trap stop_servers_keep_docker HUP

_key_reader() {
  local key
  while true; do
    IFS= read -rsn1 -t 1 key </dev/tty 2>/dev/null || continue
    case "$key" in
      $'\x12') kill -USR1 $$ 2>/dev/null || true ;;  # Ctrl-R: restart backend
      $'\x15') kill -USR2 $$ 2>/dev/null || true ;;  # Ctrl-U: clear Vite cache + restart UI
      $'\x05') kill -HUP  $$ 2>/dev/null || true ;;  # Ctrl-E: stop servers, keep Docker
    esac
  done
}
_key_reader &
KEY_READER_PID=$!

# Watch ~/.provisa-server-version for mtime changes and send USR1 to restart backend.
# Developer workflow: `touch ~/.provisa-server-version` after editing Python files on T9.
_VERSION_FILE="${HOME}/.provisa-server-version"
touch "$_VERSION_FILE" 2>/dev/null || true
# mtime lookup MUST be portable: `stat -f %m` is BSD/macOS. On Linux `stat -f` means
# --file-system and prints filesystem info (Free/Available block counts) that fluctuate under any
# disk activity — so on Linux it "changed" every poll and USR1-restarted the backend every 2s
# whenever the demo was writing to disk. Try GNU (`-c %Y`) first, then fall back to BSD (`-f %m`).
_mtime() { stat -c "%Y" "$1" 2>/dev/null || stat -f "%m" "$1" 2>/dev/null || echo 0; }
(
  _LAST_MTIME=$(_mtime "$_VERSION_FILE")
  while sleep 2; do
    _CURR_MTIME=$(_mtime "$_VERSION_FILE")
    if [ "$_CURR_MTIME" != "$_LAST_MTIME" ]; then
      _LAST_MTIME="$_CURR_MTIME"
      echo "~/.provisa-server-version changed — restarting backend..."
      kill -USR1 $$ 2>/dev/null || true
    fi
  done
) &
VERSION_WATCHER_PID=$!

while true; do
  wait || true
done
