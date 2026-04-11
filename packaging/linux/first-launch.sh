#!/usr/bin/env bash
# First-launch setup for Linux AppImage.
# Loads bundled Docker images and installs the provisa CLI.
# Always uses bundled rootless dockerd — no system Docker required.
set -euo pipefail

APPDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGES_DIR="${APPDIR}/images"
COMPOSE_DIR="${APPDIR}/compose"
PROVISA_HOME="${HOME}/.provisa"
SENTINEL="${PROVISA_HOME}/.first-launch-complete"
LOCAL_BIN="${HOME}/.local/bin"

BUNDLED_ROOTLESS="${APPDIR}/bin/dockerd-rootless.sh"
BUNDLED_SOCKET="${PROVISA_HOME}/run/docker.sock"
BUNDLED_DATA="${PROVISA_HOME}/docker-data"
BUNDLED_PID="${PROVISA_HOME}/run/dockerd.pid"

# Globals set during setup
ROLE=""          # "primary" | "secondary"
PRIMARY_IP=""    # set when ROLE=secondary
TRINO_WORKERS=0

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'
info()  { printf "${CYAN}[provisa]${NC} %s\n" "$*"; }
ok()    { printf "${GREEN}[provisa]${NC} %s\n" "$*"; }
warn()  { printf "${YELLOW}[provisa]${NC} %s\n" "$*"; }
err()   { printf "${RED}[provisa]${NC} %s\n" "$*" >&2; }

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

# ── Load images ────────────────────────────────────────────────────────────────
load_images() {
  # Secondary nodes skip database/store images — they don't run them
  local skip_pattern=""
  if [ "$ROLE" = "secondary" ]; then
    skip_pattern="postgres|pgbouncer|minio|redis"
    info "Secondary node: skipping database/store images..."
  else
    info "Loading bundled container images (no network required)..."
  fi

  local count=0
  for tar_file in "${IMAGES_DIR}"/*.tar.gz; do
    [ -f "$tar_file" ] || continue
    local name
    name="$(basename "$tar_file")"
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
ExecStart=${LOCAL_BIN}/provisa start --foreground
ExecStop=${LOCAL_BIN}/provisa stop
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
UNIT
  systemctl daemon-reload
  ok "systemd unit installed: ${unit}"
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

  if [ "$ROLE" = "primary" ]; then
    cat > "${PROVISA_HOME}/config.yaml" <<YAML
# Provisa configuration — primary node
#
# Machine-specific (this node only — do not copy to secondaries):
#   hostname, api_port, federation_workers, runtime, docker_host, project_dir
#
# Shared state (lives in PostgreSQL on this node — secondaries connect to it):
#   Data source definitions, semantic model, security policies, governed queries,
#   role mappings, masking rules, Trino catalog properties.
#   Secondaries pull shared config at runtime via the database connection —
#   no manual sync required.
#
# Singleton services on this node (secondaries point here, never run their own):
#   PostgreSQL  — shared schema, config, governed queries, semantic model
#   Redis       — shared query result cache and subscription state
#   MinIO       — shared object store for redirect results and MV snapshots
#   Trino coordinator — all workers (primary + secondary nodes) register here

role: primary
hostname: ${hostname}
api_port: ${api_port}
runtime: bundled
docker_host: "unix://${BUNDLED_SOCKET}"
project_dir: "${COMPOSE_DIR}"
federation_workers: ${TRINO_WORKERS}
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
runtime: bundled
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

  mkdir -p "$PROVISA_HOME"
  touch "$SENTINEL"
  ok "First-launch setup complete."

  if [ "$NON_INTERACTIVE" = true ]; then
    ok "Node configured as ${ROLE}. systemd service installed — enable with: systemctl enable --now provisa"
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
