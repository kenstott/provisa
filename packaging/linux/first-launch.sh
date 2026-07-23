#!/usr/bin/env bash
# First-launch setup for Linux AppImage.
# Loads bundled Docker images and installs the provisa CLI.
# Always uses bundled rootless dockerd — no system Docker required.
set -euo pipefail

APPDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGES_DIR="${APPDIR}/images"
COMPOSE_DIR="${APPDIR}/compose"
CORE_COMPOSE="${COMPOSE_DIR}/core.yml"
OBS_COMPOSE="${COMPOSE_DIR}/observability.yml"
PROVISA_HOME="${HOME}/.provisa"
SENTINEL="${PROVISA_HOME}/.first-launch-complete"
LOCAL_BIN="${HOME}/.local/bin"

BUNDLED_ROOTLESS="${APPDIR}/bin/dockerd-rootless.sh"
BUNDLED_SOCKET="${PROVISA_HOME}/run/docker.sock"
BUNDLED_DATA="${PROVISA_HOME}/docker-data"
BUNDLED_PID="${PROVISA_HOME}/run/dockerd.pid"

# Release version baked into the AppDir (VERSION), used to pin the online native
# pip install to the matching release (parity with macOS Resources/VERSION).
PROVISA_VERSION="${PROVISA_VERSION:-$(cat "${APPDIR}/VERSION" 2>/dev/null || true)}"

# Globals set during setup
ROLE=""          # "primary" | "secondary"
PRIMARY_IP=""    # set when ROLE=secondary
TRINO_WORKERS=0

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'
info()  { printf "${CYAN}[provisa]${NC} %s\n" "$*"; }
ok()    { printf "${GREEN}[provisa]${NC} %s\n" "$*"; }
warn()  { printf "${YELLOW}[provisa]${NC} %s\n" "$*"; }
err()   { printf "${RED}[provisa]${NC} %s\n" "$*" >&2; }
_lc()   { printf '%s' "$1" | tr '[:upper:]' '[:lower:]'; }

# ── Argument parsing ──────────────────────────────────────────────────────────
# Supports non-interactive invocation from Terraform / cloud-init:
#   first-launch.sh --role primary --ram-gb 32 --non-interactive
#   first-launch.sh --role secondary --primary-ip 10.0.0.4 --ram-gb 0 --non-interactive
NON_INTERACTIVE=false
CLI_ROLE=""
CLI_PRIMARY_IP=""
CLI_RAM_GB=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --non-interactive) NON_INTERACTIVE=true; shift ;;
    --role)            CLI_ROLE="$2"; shift 2 ;;
    --primary-ip)      CLI_PRIMARY_IP="$2"; shift 2 ;;
    --ram-gb)          CLI_RAM_GB="$2"; shift 2 ;;
    *) shift ;;
  esac
done

# ── Role selection ────────────────────────────────────────────────────────────
ask_role() {
  if [ -n "$CLI_ROLE" ]; then
    ROLE="$CLI_ROLE"
    ok "Role: ${ROLE} (from --role flag)"
    return
  fi

  printf "\n${BOLD}Node Role${NC}\n"
  printf "  [1] Primary   — runs all services (API, databases, object store, query engine)\n"
  printf "  [2] Secondary — runs API + query workers only; connects to an existing primary\n"
  printf "\n"

  local choice
  while true; do
    printf "Enter choice [1-2]: "
    read -r choice
    case "$choice" in
      1) ROLE=primary;   break ;;
      2) ROLE=secondary; break ;;
      *) printf "Enter 1 or 2.\n" ;;
    esac
  done
  ok "Role: ${ROLE}"
}

# ── Primary IP (secondary only) ───────────────────────────────────────────────
ask_primary_ip() {
  local ip=""
  if [ -n "$CLI_PRIMARY_IP" ]; then
    ip="$CLI_PRIMARY_IP"
  else
    while true; do
      printf "Primary node IP or hostname: "
      read -r ip
      ip="${ip//[[:space:]]/}"
      [ -n "$ip" ] && break
      printf "Required.\n"
    done
  fi

  # Verify reachability before proceeding
  info "Checking connectivity to ${ip}:8000 ..."
  if curl -fsS --max-time 5 "http://${ip}:8000/health" >/dev/null 2>&1; then
    ok "Primary reachable."
  else
    warn "Could not reach http://${ip}:8000/health — check firewall rules."
    warn "Continuing anyway; services will retry at startup."
  fi

  PRIMARY_IP="$ip"
}

# ── Derive Trino worker count from RAM budget ─────────────────────────────────
_workers_from_budget() {
  local gb="$1"
  if   [ "$gb" -ge 96 ]; then echo 4
  elif [ "$gb" -ge 48 ]; then echo 2
  elif [ "$gb" -ge 24 ]; then echo 1
  else echo 0
  fi
}

ask_ram_budget() {
  local total_gb
  total_gb="$(awk '/MemTotal/ {printf "%d", $2/1024/1024}' /proc/meminfo)"

  # Non-interactive: --ram-gb 0 means use all available RAM
  if [ -n "$CLI_RAM_GB" ]; then
    local budget_gb
    if [ "$CLI_RAM_GB" -eq 0 ]; then
      budget_gb="$total_gb"
    else
      budget_gb="$CLI_RAM_GB"
    fi
    TRINO_WORKERS="$(_workers_from_budget "$budget_gb")"
    ok "RAM budget: ${budget_gb}GB → Trino workers: ${TRINO_WORKERS} (from --ram-gb flag)"
    return
  fi

  printf "\n${BOLD}RAM Budget${NC}\n"
  printf "How much RAM should Provisa use? (host total: %dGB)\n\n" "$total_gb"

  local options=()
  for size in 4 8 16 32 64 128; do
    [ "$size" -le "$total_gb" ] && options+=("${size}GB")
  done
  options+=("All (${total_gb}GB)")

  local i=1
  for opt in "${options[@]}"; do
    printf "  [%d] %s\n" "$i" "$opt"
    i=$((i + 1))
  done
  printf "\n"

  local choice
  while true; do
    printf "Enter choice [1-%d]: " "${#options[@]}"
    read -r choice
    if [[ "$choice" =~ ^[0-9]+$ ]] && [ "$choice" -ge 1 ] && [ "$choice" -le "${#options[@]}" ]; then
      break
    fi
    printf "Invalid choice. Try again.\n"
  done

  local selected="${options[$((choice - 1))]}"
  local budget_gb
  if [[ "$selected" == All* ]]; then
    budget_gb="$total_gb"
  else
    budget_gb="${selected%GB}"
  fi

  TRINO_WORKERS="$(_workers_from_budget "$budget_gb")"
  ok "RAM budget: ${budget_gb}GB → Trino workers: ${TRINO_WORKERS}"
}

# ── Start bundled rootless Docker ─────────────────────────────────────────────
start_docker() {
  if [ ! -x "$BUNDLED_ROOTLESS" ]; then
    err "Bundled Docker runtime not found at ${APPDIR}/bin/ — reinstall Provisa."
    exit 1
  fi

  mkdir -p "${PROVISA_HOME}/run" "$BUNDLED_DATA"
  export XDG_RUNTIME_DIR="${PROVISA_HOME}/run"
  export DOCKER_HOST="unix://${BUNDLED_SOCKET}"
  export PATH="${APPDIR}/bin:${PATH}"

  info "Starting bundled Docker runtime..."
  "$BUNDLED_ROOTLESS" \
    --data-root "$BUNDLED_DATA" \
    --host "unix://${BUNDLED_SOCKET}" \
    --pidfile "$BUNDLED_PID" \
    --log-level error \
    >/dev/null 2>&1 &

  local retries=30
  while [ $retries -gt 0 ]; do
    docker info &>/dev/null 2>&1 && break
    sleep 1
    retries=$((retries - 1))
  done
  if [ $retries -eq 0 ]; then
    err "Bundled Docker failed to start within 30 seconds."
    exit 1
  fi
  ok "Docker started."
}

# ── Acquire + load images (slim AppImage ships none — get them on demand) ─────
# Discovery: local-first (beside the AppImage / ~/Downloads / cwd) for airgap, else
# download provisa-core-images-amd64-<version>.zip from the release. It contains the
# gzipped `docker save` tarballs (registry images + zaychik + provisa app).
load_images() {
  local staged="${PROVISA_HOME}/images"
  mkdir -p "$staged"

  if ! ls "${staged}"/*.tar.gz >/dev/null 2>&1; then
    if ! command -v unzip >/dev/null 2>&1; then
      err "unzip is required to extract the core images. Install it (e.g. apt-get install unzip) and re-run."
      exit 1
    fi
    local zip="provisa-core-images-amd64-${PROVISA_VERSION}.zip"
    local src="" cand appdir_parent
    appdir_parent="$(dirname "$APPDIR")"
    for cand in "${appdir_parent}/${zip}" "${HOME}/Downloads/${zip}" "${PWD}/${zip}"; do
      [ -f "$cand" ] && { src="$cand"; break; }
    done
    if [ -z "$src" ] && [ -n "$PROVISA_VERSION" ]; then
      info "Downloading core images (${zip})..."
      if curl -fL --retry 3 --retry-delay 5 -o "${PROVISA_HOME}/${zip}" \
           "https://github.com/kenstott/provisa/releases/download/${PROVISA_VERSION}/${zip}"; then
        src="${PROVISA_HOME}/${zip}"
      fi
    fi
    if [ -z "$src" ]; then
      err "Core images not found. Place ${zip} beside the AppImage (airgap) or connect to the network, then re-run."
      exit 1
    fi
    info "Extracting core images..."
    ( cd "$staged" && unzip -o -q "$src" )
    [ "$src" = "${PROVISA_HOME}/${zip}" ] && rm -f "$src"
  fi

  # Secondary nodes skip database/store images — they don't run them.
  local skip_pattern=""
  [ "$ROLE" = "secondary" ] && skip_pattern="postgres|pgbouncer|minio|redis"

  local count=0
  for tar_file in "${staged}"/*.tar.gz; do
    [ -f "$tar_file" ] || continue
    local name; name="$(basename "$tar_file")"
    if [ -n "$skip_pattern" ] && echo "$name" | grep -qE "$skip_pattern"; then
      continue
    fi
    info "  Loading: ${name}"
    gunzip -c "$tar_file" | docker load
    count=$((count + 1))
  done
  ok "Loaded ${count} images."
}

# ── Ask hostname ──────────────────────────────────────────────────────────────
ask_hostname() {
  local default
  default="$(hostname -I 2>/dev/null | awk '{print $1}')"
  default="${default:-localhost}"
  if [ "$NON_INTERACTIVE" = true ]; then
    echo "$default"
    return
  fi
  printf "This node's hostname or IP [${default}]: "
  local input
  read -r input
  input="${input//[[:space:]]/}"
  echo "${input:-$default}"
}

# ── Ask API port ──────────────────────────────────────────────────────────────
ask_api_port() {
  if [ "$NON_INTERACTIVE" = true ]; then
    echo "8000"
    return
  fi
  local default=8000
  local port=""
  while true; do
    printf "API port [${default}]: "
    read -r port
    port="${port//[[:space:]]/}"
    port="${port:-$default}"
    if [[ "$port" =~ ^[0-9]+$ ]] && [ "$port" -ge 1024 ] && [ "$port" -le 65535 ]; then
      break
    fi
    printf "Invalid port. Enter a number between 1024 and 65535.\n"
  done
  echo "$port"
}

# ── Install systemd service (non-interactive / cloud deployments) ─────────────
install_systemd() {
  local unit="/etc/systemd/system/provisa.service"
  # Persist auth env (PROVISA_IDP + provider secrets) so the server, running
  # under systemd rather than this first-launch process, auto-configures the IdP
  # and can resolve ${env:...} secret placeholders at runtime.
  local env_file="${PROVISA_HOME}/provisa.env"
  : > "$env_file"
  chmod 600 "$env_file"
  for var in PROVISA_IDP FIREBASE_PROJECT_ID FIREBASE_SERVICE_ACCOUNT_KEY \
             KEYCLOAK_URL KEYCLOAK_REALM KEYCLOAK_CLIENT_ID \
             OAUTH_ISSUER OAUTH_CLIENT_ID OAUTH_CLIENT_SECRET; do
    if [ -n "${!var:-}" ]; then
      printf '%s=%s\n' "$var" "${!var}" >> "$env_file"
    fi
  done
  cat > "$unit" <<UNIT
[Unit]
Description=Provisa Data Platform
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${USER}
Environment=DOCKER_HOST=unix://${BUNDLED_SOCKET}
Environment=XDG_RUNTIME_DIR=${PROVISA_HOME}/run
EnvironmentFile=-${env_file}
ExecStart=${LOCAL_BIN}/provisa start --foreground
ExecStop=${LOCAL_BIN}/provisa stop
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
UNIT
  systemctl daemon-reload
  systemctl enable --now provisa
  ok "systemd unit installed and started: ${unit}"
}

# ── Resolve deployment (parity with macOS wizard / install.sh, REQ-972..979) ──
# Non-interactive (Terraform / cloud-init exports the env) reads the wizard vars;
# interactive prompts. Only the primary node carries these fields — secondaries
# pull shared config from the primary DB at runtime.
# Sets globals: DEPLOY_ENGINE ENGINE_URL MATERIALIZE_URL OBS_MODE OTLP_ENDPOINT
#               INSTALL_DEMO DEMO_MODE NEEDS_DOCKER
# NEEDS_DOCKER is false (native tier) only for the self-contained DuckDB default:
# engine=duckdb AND obs!=docker AND not (demo on docker); else the Docker tier.
_compute_needs_docker() {
  DEMO_MODE="${PROVISA_DEMO_MODE:-native}"
  NEEDS_DOCKER=false
  # if-form, not `[ cond ] && VAR=true`: under `set -e` a false single-line test
  # makes the statement return non-zero and aborts the whole script.
  if [ "$DEPLOY_ENGINE" != "duckdb" ]; then NEEDS_DOCKER=true; fi
  if [ "$OBS_MODE" = "docker" ]; then NEEDS_DOCKER=true; fi
  if [ "$(_lc "$INSTALL_DEMO")" = "y" ] && [ "$DEMO_MODE" = "docker" ]; then NEEDS_DOCKER=true; fi
}
resolve_deployment() {
  if [ "$NON_INTERACTIVE" = true ]; then
    DEPLOY_ENGINE="${PROVISA_ENGINE:-duckdb}"
    ENGINE_URL="${PROVISA_ENGINE_URL:-}"
    MATERIALIZE_URL="${PROVISA_MATERIALIZE_URL:-}"
    OBS_MODE="${PROVISA_OBS_MODE:-none}"
    OTLP_ENDPOINT="${PROVISA_OTLP_ENDPOINT:-}"
    INSTALL_DEMO="${PROVISA_INSTALL_DEMO:-n}"
    _compute_needs_docker
    ok "Deployment: engine=${DEPLOY_ENGINE} obs=${OBS_MODE} demo=${INSTALL_DEMO}/${DEMO_MODE} docker=${NEEDS_DOCKER}"
    return
  fi

  printf "\n${BOLD}Federation engine${NC}\n"
  printf "  1) DuckDB — native (recommended)\n  2) Trino\n  3) External engine\n"
  local ec; read -rp "$(printf "${CYAN}[provisa]${NC} Choose 1-3 [1]: ")" ec
  case "$ec" in 2) DEPLOY_ENGINE="trino" ;; 3) DEPLOY_ENGINE="sqlalchemy" ;; *) DEPLOY_ENGINE="duckdb" ;; esac
  ENGINE_URL=""; MATERIALIZE_URL=""
  if [ "$DEPLOY_ENGINE" = "sqlalchemy" ]; then
    read -rp "$(printf "${CYAN}[provisa]${NC} External engine URL: ")" ENGINE_URL
    read -rp "$(printf "${CYAN}[provisa]${NC} Materialization store URL (optional): ")" MATERIALIZE_URL
  fi

  printf "\n${BOLD}Observability integration${NC}\n"
  printf "  1) Built-in only\n  2) In-cluster Grafana/Prometheus stack\n  3) Export to my collector\n"
  local oc; read -rp "$(printf "${CYAN}[provisa]${NC} Choose 1-3 [1]: ")" oc
  case "$oc" in 2) OBS_MODE="docker" ;; 3) OBS_MODE="collector" ;; *) OBS_MODE="none" ;; esac
  OTLP_ENDPOINT=""
  if [ "$OBS_MODE" = "collector" ]; then
    read -rp "$(printf "${CYAN}[provisa]${NC} OTLP collector endpoint: ")" OTLP_ENDPOINT
  fi

  printf "${CYAN}[provisa]${NC} The demo is a complete, fully functional install — pick it with confidence; nothing is limited.\n"
  printf "${CYAN}[provisa]${NC} To reconfigure with other options later, just run this setup again.\n"
  local dm; read -rp "$(printf "${CYAN}[provisa]${NC} Install the demo dataset with guided tour (y/N): ")" dm
  case "$dm" in [yY]|[yY][eE][sS]) INSTALL_DEMO="y" ;; *) INSTALL_DEMO="n" ;; esac
  _compute_needs_docker
  ok "Deployment: engine=${DEPLOY_ENGINE} obs=${OBS_MODE} demo=${INSTALL_DEMO}/${DEMO_MODE} docker=${NEEDS_DOCKER}"
}

# ── Network check (online vs airgapped) ──────────────────────────────────────
_online() { curl -fsI --max-time 8 https://pypi.org/simple/ >/dev/null 2>&1; }

# ── Locate a native-tier payload dir bundled inside the AppDir ────────────────
# The bare interpreter (python-base/), wheelhouse (wheels/) and built UI
# (ui-dist/) are staged into the AppDir at build time.
_find_payload() {
  local name="$1" test_glob="$2" cand="${APPDIR}/${name}"
  if [ -d "$cand" ] && { [ -z "$test_glob" ] || ls "$cand"/$test_glob >/dev/null 2>&1; }; then
    printf '%s' "$cand"; return 0
  fi
  return 1
}

# ── Native tier: build a Python venv from the bundled interpreter + wheelhouse ─
# Online → pip install provisa[embedded] from PyPI (pinned to the release). Airgapped →
# --no-index --find-links against the bundled wheelhouse (always staged in the AppDir).
setup_native_venv() {
  local venv="${PROVISA_HOME}/venv"
  if [ -x "${venv}/bin/python3" ] && "${venv}/bin/python3" -c "import provisa" 2>/dev/null; then
    return 0
  fi

  local base_src
  base_src="$(_find_payload python-base bin/python3)" || {
    err "Bundled Python interpreter not found in the AppImage — reinstall Provisa."
    exit 1
  }

  # Stage the interpreter into ~/.provisa (no codesign/xattr — that's macOS-only).
  local base="${PROVISA_HOME}/python-base"
  if [ ! -x "${base}/bin/python3" ]; then
    info "Staging Python interpreter..."
    mkdir -p "$base"; cp -R "$base_src"/. "$base/"
    chmod -R u+rwX "$base"
    chmod +x "${base}/bin/"* 2>/dev/null || true
  fi

  info "Creating Python environment..."
  "${base}/bin/python3" -m venv "$venv"
  local pip="${venv}/bin/pip"
  "$pip" install --quiet --upgrade pip 2>/dev/null || true

  local pin=""
  [ -n "$PROVISA_VERSION" ] && pin="==${PROVISA_VERSION#v}"
  local wheels; wheels="$(_find_payload wheels '*.whl' || true)"

  if _online; then
    info "Installing Provisa from PyPI..."
    "$pip" install --quiet "provisa[embedded]${pin}" uvicorn mcp-proxy
  elif [ -n "$wheels" ]; then
    info "Installing Provisa from bundled wheels (offline)..."
    "$pip" install --quiet --no-index --find-links "$wheels" "provisa[embedded]" uvicorn mcp-proxy
  else
    err "No network and no bundled wheels found — reinstall Provisa."
    exit 1
  fi

  # Place the built UI where ui_server resolves it (<site-packages>/static).
  local ui_src; ui_src="$(_find_payload ui-dist '' || true)"
  if [ -n "$ui_src" ]; then
    local site; site="$("${venv}/bin/python3" -c 'import sysconfig;print(sysconfig.get_paths()["purelib"])')"
    mkdir -p "${site}/static"; cp -R "$ui_src"/. "${site}/static/"
  fi
  ok "Native environment ready."
}

# ── Write config ───────────────────────────────────────────────────────────────
write_config() {
  mkdir -p "$PROVISA_HOME"
  if [ -f "${PROVISA_HOME}/config.yaml" ]; then
    return
  fi

  local hostname api_port
  hostname="$(ask_hostname)"
  api_port="$(ask_api_port)"

  local demo_flag
  case "${INSTALL_DEMO:-n}" in [yY]|[yY][eE][sS]) demo_flag=true ;; *) demo_flag=false ;; esac

  # runtime: `native` (Python venv, no Docker) vs `bundled` (rootless dockerd).
  # image_source: tarball on the Docker tier so the shared CLI adds the airgap
  # overlay (docker-compose.airgap.yml) — belt-and-suspenders alongside runtime.
  local runtime img_src_line=""
  if [ "${NEEDS_DOCKER:-true}" = false ]; then
    runtime="native"
  else
    runtime="bundled"
    img_src_line="image_source: tarball"
  fi

  if [ "$ROLE" = "primary" ]; then
    cat > "${PROVISA_HOME}/config.yaml" <<YAML
# Provisa configuration — primary node
#
# Machine-specific (this node only — do not copy to secondaries):
#   hostname, api_port, federation_workers, runtime, docker_host, project_dir
#
# Shared state (lives in PostgreSQL on this node — secondaries connect to it):
#   Data source definitions, semantic model, security policies,
#   role mappings, masking rules, Trino catalog properties.
#   Secondaries pull shared config at runtime via the database connection —
#   no manual sync required.
#
# Singleton services on this node (secondaries point here, never run their own):
#   PostgreSQL  — shared schema, config, semantic model
#   Redis       — shared query result cache and subscription state
#   MinIO       — shared object store for redirect results and MV snapshots
#   Trino coordinator — all workers (primary + secondary nodes) register here

role: primary
hostname: ${hostname}
api_port: ${api_port}
runtime: ${runtime}
${img_src_line}
docker_host: "unix://${BUNDLED_SOCKET}"
project_dir: "${COMPOSE_DIR}"
federation_workers: ${TRINO_WORKERS}
# Deployment (REQ-972..979): parity with the desktop wizard.
engine: ${DEPLOY_ENGINE:-duckdb}
engine_url: "${ENGINE_URL:-}"
materialize_url: "${MATERIALIZE_URL:-}"
obs_mode: ${OBS_MODE:-none}
otlp_endpoint: "${OTLP_ENDPOINT:-}"
demo: ${demo_flag}
YAML

  else
    cat > "${PROVISA_HOME}/config.yaml" <<YAML
# Provisa configuration — secondary node
#
# Machine-specific (this node only):
#   hostname, api_port, federation_workers, runtime, docker_host, project_dir
#
# This node does NOT run PostgreSQL, Redis, MinIO, or Trino coordinator.
# Those are singletons on the primary — shared across the entire cluster.
# Shared application config (data sources, policies, semantic model) is read
# from PostgreSQL on the primary at runtime; nothing needs to be copied here.

role: secondary
hostname: ${hostname}
api_port: ${api_port}
runtime: ${runtime}
${img_src_line}
docker_host: "unix://${BUNDLED_SOCKET}"
project_dir: "${COMPOSE_DIR}"
federation_workers: ${TRINO_WORKERS}

# Singleton services — primary node endpoints
# These are intentionally single-instance. Do not run local copies.
pg_host: ${PRIMARY_IP}          # shared schema and application config
redis_host: ${PRIMARY_IP}       # shared cache and subscription state
minio_host: ${PRIMARY_IP}       # shared object store
trino_coordinator_host: ${PRIMARY_IP}  # all Trino workers register here
YAML
  fi

  ok "Config written to ${PROVISA_HOME}/config.yaml"
}

# ── Install CLI ────────────────────────────────────────────────────────────────
install_cli() {
  mkdir -p "$LOCAL_BIN"
  cp "${APPDIR}/provisa-cli" "${LOCAL_BIN}/provisa"
  chmod +x "${LOCAL_BIN}/provisa"
  ok "CLI installed to ${LOCAL_BIN}/provisa"

  for rc in "${HOME}/.bashrc" "${HOME}/.zshrc"; do
    [ -f "$rc" ] || continue
    if ! grep -q "PROVISA_DOCKER_HOST" "$rc" 2>/dev/null; then
      printf '\n# Provisa bundled Docker runtime\nexport DOCKER_HOST="%s"\n' \
        "unix://${BUNDLED_SOCKET}" >> "$rc"
    fi
  done

  case ":${PATH}:" in
    *":${LOCAL_BIN}:"*) ;;
    *)
      printf "\n${CYAN}[provisa]${NC} Add %s to your PATH:\n" "$LOCAL_BIN"
      printf "  echo 'export PATH=\"\$HOME/.local/bin:\$PATH\"' >> ~/.bashrc\n"
      printf "  source ~/.bashrc\n\n"
      ;;
  esac
}

# ── Load balancer guidance (printed after primary setup) ──────────────────────
print_lb_guidance() {
  local hostname api_port
  hostname="$(grep '^hostname:' "${PROVISA_HOME}/config.yaml" | awk '{print $2}')"
  api_port="$(grep '^api_port:' "${PROVISA_HOME}/config.yaml" | awk '{print $2}')"

  printf "\n${BOLD}Horizontal Scaling${NC}\n"
  printf "To add capacity, install Provisa on additional machines and choose ${BOLD}Secondary${NC}.\n"
  printf "Provide this node's IP when prompted: ${BOLD}%s${NC}\n\n" "$hostname"
  printf "Once secondaries are running, place all nodes behind a load balancer.\n"
  printf "Example nginx upstream block:\n\n"
  printf "${CYAN}"
  cat <<NGINX
  upstream provisa {
      least_conn;
      server ${hostname}:${api_port};   # primary
      # server <secondary-1-ip>:${api_port};
      # server <secondary-2-ip>:${api_port};
      keepalive 32;
  }

  server {
      listen 80;
      location / {
          proxy_pass         http://provisa;
          proxy_http_version 1.1;
          proxy_set_header   Connection "";
          proxy_set_header   Host \$host;
          proxy_set_header   X-Real-IP \$remote_addr;
      }
      # Arrow Flight (gRPC) — separate listener, TCP passthrough
      listen 8815;
      location / {
          grpc_pass grpc://provisa;
      }
  }
NGINX
  printf "${NC}"
  printf "\nFirewall: secondaries need inbound 8000 (API) and 8815 (Flight).\n"
  printf "Primary needs inbound 5432 (PG), 6379 (Redis), 9000 (MinIO), 8080 (Trino) from secondaries only.\n\n"
}

# ── Main ───────────────────────────────────────────────────────────────────────
main() {
  printf "\n${BOLD}Provisa — First Launch Setup${NC}\n"
  printf "═══════════════════════════════════════════\n\n"

  mkdir -p "$PROVISA_HOME"
  resolve_deployment   # sets DEPLOY_ENGINE OBS_MODE INSTALL_DEMO DEMO_MODE NEEDS_DOCKER

  # ── Native tier (default): a Python venv, no Docker ──
  # Single-node — no primary/secondary role prompt; the venv serves everything.
  if [ "$NEEDS_DOCKER" = false ]; then
    # The native tier is single-node. A multi-node deploy (Terraform passes
    # --role secondary) only makes sense on the Trino/Docker tier, so fail loud
    # rather than silently degrading a secondary into a standalone primary.
    if [ "$CLI_ROLE" = "secondary" ]; then
      err "engine=${DEPLOY_ENGINE} runs the single-node native tier, which has no secondary role."
      err "For a multi-node cluster set PROVISA_ENGINE=trino (the Docker tier)."
      exit 1
    fi
    info "Setting up Provisa (native — no Docker)..."
    ROLE=primary
    setup_native_venv
    write_config          # runtime=native
    install_cli

    if [ "$NON_INTERACTIVE" = true ]; then
      install_systemd
    fi

    touch "$SENTINEL"
    ok "First-launch setup complete (native — no Docker)."

    if [ "$NON_INTERACTIVE" = true ]; then
      ok "Node configured (native). systemd service enabled and started."
      return
    fi
    printf "\n${GREEN}${BOLD}Provisa is ready.${NC}\n"
    printf "Run: ${BOLD}provisa start${NC}\n\n"
    return
  fi

  # ── Docker tier: bundled rootless dockerd + airgap image tarballs ──
  info "Setting up Provisa (no internet required)..."

  start_docker
  ask_role

  if [ "$ROLE" = "secondary" ]; then
    ask_primary_ip
  fi

  ask_ram_budget
  load_images
  write_config
  install_cli

  if [ "$NON_INTERACTIVE" = true ]; then
    install_systemd
  fi

  touch "$SENTINEL"
  ok "First-launch setup complete."

  if [ "$NON_INTERACTIVE" = true ]; then
    ok "Node configured as ${ROLE}. systemd service enabled and started."
    return
  fi

  if [ "$ROLE" = "primary" ]; then
    printf "\n${GREEN}${BOLD}Provisa primary node is ready.${NC}\n"
    printf "Run: ${BOLD}provisa start${NC}\n"
    print_lb_guidance
  else
    printf "\n${GREEN}${BOLD}Provisa secondary node is ready.${NC}\n"
    printf "Run: ${BOLD}provisa start${NC}\n\n"
    printf "This node will serve API traffic and Trino workers.\n"
    printf "Data plane (PostgreSQL, Redis, MinIO) is on the primary at ${BOLD}%s${NC}.\n\n" "$PRIMARY_IP"
  fi
}

main "$@"
