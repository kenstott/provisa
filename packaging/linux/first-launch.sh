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

# Docker runtime selector. `bundled` (default): rootless dockerd shipped in the
# AppImage — right for the desktop, where we run as an unprivileged user with no
# sudo. `system`: an already-running rootful Docker daemon (its socket) — set by
# the cloud/VM startup (PROVISA_DOCKER_MODE=system), where the script runs as root
# and rootless dockerd refuses to start. The socket feeds DOCKER_HOST, the config
# docker_host, and the systemd unit.
DOCKER_MODE="${PROVISA_DOCKER_MODE:-bundled}"
if [ "$DOCKER_MODE" = system ]; then
  DOCKER_SOCKET="${PROVISA_DOCKER_SOCKET:-/var/run/docker.sock}"
else
  DOCKER_SOCKET="$BUNDLED_SOCKET"
fi

# Release version baked into the AppDir (VERSION), used to pin the online native
# pip install to the matching release (parity with macOS Resources/VERSION).
PROVISA_VERSION="${PROVISA_VERSION:-$(cat "${APPDIR}/VERSION" 2>/dev/null || true)}"

# Globals set during setup
# control-plane is the hosted SaaS role (REQ-1451): the app tier plus redis/minio,
# with the control-plane DB on Cloud SQL and every query engine a pod on the GKE
# cluster. It is set by terraform, never offered by the interactive prompt.
ROLE=""          # "primary" | "secondary" | "control-plane"
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
# --refresh-env: rewrite the systemd EnvironmentFile from the environment this script was
# invoked with, then restart the stack — no image load, no compose restage. AppRun uses it on
# the same-version fast path, because settings that terraform can change without a release
# (the engine cluster's location/mode/zone, REQ-1465) reach the node only through that file.
# Without it a metadata edit is invisible until the next version bump.
REFRESH_ENV=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --refresh-env)     REFRESH_ENV=true; shift ;;
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
    case "$CLI_ROLE" in
      primary|secondary|control-plane) ;;
      *) err "Unknown --role ${CLI_ROLE} (primary|secondary|control-plane)."; exit 1 ;;
    esac
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

# ── Start / attach Docker ─────────────────────────────────────────────────────
start_docker() {
  if [ "$DOCKER_MODE" = system ]; then
    export DOCKER_HOST="unix://${DOCKER_SOCKET}"
    if ! docker info >/dev/null 2>&1; then
      err "System Docker not reachable at ${DOCKER_SOCKET}. Start the docker service and re-run."
      exit 1
    fi
    ok "Using system Docker (${DOCKER_SOCKET})."
    return
  fi

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
  local marker="${staged}/.version"
  mkdir -p "$staged"

  # Re-extract when staging is empty OR holds a different version's tarballs. A reset
  # that bumps PROVISA_VERSION must replace the airgap images, not reload the stale
  # ones already sitting here (the tarball names are version-independent, so a plain
  # presence check would silently load the old build forever).
  local staged_version=""; [ -f "$marker" ] && staged_version="$(cat "$marker")"
  if ! ls "${staged}"/*.tar.gz >/dev/null 2>&1 || [ "$staged_version" != "$PROVISA_VERSION" ]; then
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
    rm -f "${staged}"/*.tar.gz
    ( cd "$staged" && unzip -o -q "$src" )
    # The Trino engine ships as its own release asset — bundled it pushed the zip past
    # GitHub's 2 GiB per-asset limit. It is an outer tar.gz wrapping the docker-save
    # tarball; extracting drops trino-481.tar.gz into the same staging dir the image
    # loop below already loads from.
    local trino="provisa-trino-image-amd64-${PROVISA_VERSION}.tar.gz"
    local trino_src=""
    # The control plane runs no Trino at all — every engine is a pod on the GKE
    # cluster (REQ-1451). Downloading a 1.5 GiB engine image it will never start
    # would also make a missing asset fatal for a node that does not need it.
    if [ "$ROLE" = "control-plane" ]; then
      trino=""
    fi
    if [ -n "$trino" ]; then
      for cand in "${appdir_parent}/${trino}" "${HOME}/Downloads/${trino}" "${PWD}/${trino}"; do
        [ -f "$cand" ] && { trino_src="$cand"; break; }
      done
      if [ -z "$trino_src" ] && [ -n "$PROVISA_VERSION" ]; then
        info "Downloading Trino engine image (${trino})..."
        if curl -fL --retry 3 --retry-delay 5 -o "${PROVISA_HOME}/${trino}" \
             "https://github.com/kenstott/provisa/releases/download/${PROVISA_VERSION}/${trino}"; then
          trino_src="${PROVISA_HOME}/${trino}"
        fi
      fi
      if [ -z "$trino_src" ]; then
        err "Trino engine image not found. Place ${trino} beside the AppImage (airgap) or connect to the network, then re-run."
        exit 1
      fi
      tar -xzf "$trino_src" -C "$staged"
      [ "$trino_src" = "${PROVISA_HOME}/${trino}" ] && rm -f "$trino_src"
    fi
    printf '%s' "$PROVISA_VERSION" > "$marker"
    [ "$src" = "${PROVISA_HOME}/${zip}" ] && rm -f "$src"
  fi

  # Secondary nodes skip database/store images — they don't run them. The control
  # plane keeps redis/minio (it runs both) but never postgres — Cloud SQL holds the
  # control plane — and never Trino, which is the cluster's (REQ-1451).
  local skip_pattern=""
  [ "$ROLE" = "secondary" ] && skip_pattern="postgres|pgbouncer|minio|redis"
  [ "$ROLE" = "control-plane" ] && skip_pattern="postgres|pgbouncer|trino"

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

# ── Stage compose out of the AppImage into a persistent, writable location ─────
# The AppImage self-mounts at /tmp/.mount_ProvisXXXX — read-only AND ephemeral
# (the mount vanishes when this first-launch process exits, and its path changes
# every launch). But `provisa start` runs later under systemd, long after the
# AppImage is gone, and resolves compose files from project_dir. Recording the
# mount path there leaves the daemon with a dangling directory. It is also
# read-only, so load_trino_plugins cannot extract into compose/trino/plugins.
# Copy the whole compose tree into ${PROVISA_HOME}/compose once and repoint
# COMPOSE_DIR at it: plugin extraction, project_dir, and the systemd service all
# then reference a stable path that outlives the AppImage.
stage_compose() {
  local dest="${PROVISA_HOME}/compose"
  info "Staging compose files to ${dest}..."
  mkdir -p "$dest"
  cp -a "${COMPOSE_DIR}/." "$dest/"
  COMPOSE_DIR="$dest"
  ok "Compose staged to ${COMPOSE_DIR}."
}

# ── Acquire Trino custom-connector plugins ────────────────────────────────────
# The slim AppImage ships compose/trino WITHOUT plugins/ (build-appimage.sh excludes
# it to stay under GitHub's 2 GB asset limit). The custom connectors (trino-file,
# trino-sharepoint, trino-splunk, …) ride the separate release asset
# provisa-trino-plugins-<version>.tar.gz. docker-compose.core.yml bind-mounts
# ./trino/plugins/<name> into /usr/lib/trino/plugin/<name>; without the jars Docker
# auto-creates an EMPTY source dir and Trino aborts on startup ("No service providers
# of type io.trino.spi.Plugin"). Extract the tarball into compose/trino/plugins/ so
# every mount resolves. Discovery mirrors load_images: local-first for airgap, else
# download from the pinned release.
load_trino_plugins() {
  # The control plane runs no Trino; the connector plugins are baked into the engine
  # image the cluster pulls (docker/trino-engine.Dockerfile), not mounted here.
  if [ "$ROLE" = "control-plane" ]; then
    ok "Control plane runs no Trino — plugins are baked into the engine image."
    return
  fi
  local dest="${COMPOSE_DIR}/trino/plugins"
  # Idempotent: the AppImage never ships this dir, so its presence means a prior run
  # already extracted the plugins.
  if ls "${dest}/trino-file/"*.jar >/dev/null 2>&1; then
    ok "Trino plugins already present."
    return
  fi
  mkdir -p "$dest"

  local tarball="provisa-trino-plugins-${PROVISA_VERSION}.tar.gz"
  local src="" cand appdir_parent
  appdir_parent="$(dirname "$APPDIR")"
  for cand in "${appdir_parent}/${tarball}" "${HOME}/Downloads/${tarball}" "${PWD}/${tarball}"; do
    [ -f "$cand" ] && { src="$cand"; break; }
  done
  if [ -z "$src" ] && [ -n "$PROVISA_VERSION" ]; then
    info "Downloading Trino plugins (${tarball})..."
    if curl -fL --retry 3 --retry-delay 5 -o "${PROVISA_HOME}/${tarball}" \
         "https://github.com/kenstott/provisa/releases/download/${PROVISA_VERSION}/${tarball}"; then
      src="${PROVISA_HOME}/${tarball}"
    fi
  fi
  if [ -z "$src" ]; then
    err "Trino plugins not found. Place ${tarball} beside the AppImage (airgap) or connect to the network, then re-run."
    exit 1
  fi
  info "Extracting Trino plugins..."
  tar -xzf "$src" -C "$dest"
  [ "$src" = "${PROVISA_HOME}/${tarball}" ] && rm -f "$src"
  ok "Trino plugins installed to ${dest}."
}

# ── Protocol overlay (opt-in wire protocols) ──────────────────────────────────
# docker-compose.app.yml publishes only the always-on ports (API 8000, UI 3000,
# Flight 8815). The opt-in wire protocols — pgwire, Bolt, MCP, gRPC — are gated by
# env vars the cloud/VM startup exports (PROVISA_PGWIRE_PORT, PROVISA_BOLT_PORT,
# PROVISA_MCP_PORT, GRPC_PORT). The app image never publishes those container ports
# on its own, so emit an extension overlay that publishes each enabled port and
# passes the listener env through. scripts/provisa auto-includes every
# ~/.provisa/extensions/*/docker-compose.*.yml, so `provisa start` picks it up.
# app_startup binds each listener on 0.0.0.0:<port> inside the container, so the
# host:container mapping is 1:1.
write_protocol_overlay() {
  local dir="${PROVISA_HOME}/extensions/protocols"
  local file="${dir}/docker-compose.protocols.yml"
  local ports="" env=""
  # $1 host/container port, $2 env var name (its value is the port)
  _proto() {
    ports="${ports}      - \"${1}:${1}\""$'\n'
    env="${env}      ${2}: \"${1}\""$'\n'
  }
  [ "${PROVISA_PGWIRE_PORT:-0}" != 0 ] && _proto "$PROVISA_PGWIRE_PORT" PROVISA_PGWIRE_PORT
  [ "${PROVISA_BOLT_PORT:-0}" != 0 ] && _proto "$PROVISA_BOLT_PORT" PROVISA_BOLT_PORT
  [ "${GRPC_PORT:-0}" != 0 ] && _proto "$GRPC_PORT" GRPC_PORT
  if [ "${PROVISA_MCP_PORT:-0}" != 0 ]; then
    _proto "$PROVISA_MCP_PORT" PROVISA_MCP_PORT
    # MCP must bind 0.0.0.0 to be reachable from outside the container; role picks
    # the query identity (defaults set by the deploy env).
    env="${env}      PROVISA_MCP_HOST: \"0.0.0.0\""$'\n'
    env="${env}      PROVISA_MCP_ROLE: \"${PROVISA_MCP_ROLE:-admin}\""$'\n'
  fi

  if [ -z "$ports" ]; then
    rm -f "$file" 2>/dev/null || true
    return
  fi

  mkdir -p "$dir"
  {
    printf '# Auto-generated by first-launch.sh — opt-in wire protocols.\n'
    printf 'services:\n  provisa:\n    ports:\n%s    environment:\n%s' "$ports" "$env"
  } > "$file"
  ok "Protocol overlay written: ${file}"
}

# ── Demo overlay (Docker tier) ────────────────────────────────────────────────
# The app container runs `uvicorn main:app` and loads the config PROVISA_CONFIG points
# at (app.py). The base compose points it at the wizard skeleton, so an unmodified deploy
# comes up in the first-run setup wizard. A demo deploy must instead load the complete,
# pre-federated demo config baked into the image.
# Mirror native launch f7289d27: export PROVISA_CONFIG (the baked demo config),
# PROVISA_DEMO=1 (guided tour + wizard suppression), and PROVISA_DEMO_DIR (the baked
# SQLite sample data the demo config resolves ${env:PROVISA_DEMO_DIR} against). Paths
# live under /app/config, which no compose bind-mount shadows. scripts/provisa
# auto-includes every ~/.provisa/extensions/*/docker-compose.*.yml, so `provisa start`
# picks this up alongside the protocol overlay.
write_demo_overlay() {
  local dir="${PROVISA_HOME}/extensions/demo"
  local file="${dir}/docker-compose.demo.yml"

  # Every node in the cluster must load the identical config — app.py loads config from
  # the local file PROVISA_CONFIG points at and early-returns when that file is absent
  # (app.py:318-320), so a fileless secondary builds no schemas and drops into the setup
  # wizard. There is no runtime "pull shared config from the primary's PostgreSQL" path in
  # the code today. The demo config is static and baked identically into every image, so
  # pinning all roles to the same baked file keeps the whole cluster consistent.
  if [ "$(_lc "${INSTALL_DEMO:-n}")" != "y" ]; then
    rm -f "$file" 2>/dev/null || true
    return
  fi

  mkdir -p "$dir"
  cat > "$file" <<'YAML'
# Auto-generated by first-launch.sh — demo deploy (loads the baked demo config,
# suppresses the first-run setup wizard).
services:
  provisa:
    environment:
      PROVISA_CONFIG: "/app/config/provisa-install.yaml"
      PROVISA_DEMO: "1"
      PROVISA_DEMO_DIR: "/app/config/demo/files"
      # The demo's grpc-kind commands (enrich_grpc_set, random_grpc_set) default their target to
      # localhost:50071 — correct for a native run, wrong inside compose, where the demo server
      # is its own service. Point them at it.
      DEMO_GRPC_TARGET: "grpc-demo:50071"
    depends_on:
      - petstore-mock
      - graphql-demo
      - grpc-demo
  # The demo config registers an OpenAPI source at http://petstore-mock:8080/api/v3 and a
  # GraphQL source at http://graphql-demo:4000/graphql (config/provisa-install.yaml:278-300).
  # A demo deploy that ships neither leaves those sources unresolvable, so both mocks run as
  # first-class services. They use the provisa image — their deps (starlette, uvicorn,
  # strawberry) are already installed there and the sources are baked at
  # /app/config/demo/servers — so no extra image is pulled or built.
  petstore-mock:
    image: provisa/provisa:local
    restart: unless-stopped
    working_dir: /app/config/demo/servers/petstore
    command: ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8080"]
  graphql-demo:
    image: provisa/provisa:local
    restart: unless-stopped
    working_dir: /app/config/demo/servers/graphql
    command: ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "4000"]
  grpc-demo:
    image: provisa/provisa:local
    restart: unless-stopped
    working_dir: /app/config/demo/servers/grpc
    command: ["python", "server.py"]
YAML
  ok "Demo overlay written: ${file}"
}

# ── TLS certificates (all protocol endpoints, REQ-1226) ───────────────────────
# Every Provisa protocol listener — API, UI, pgwire, Bolt, Flight, gRPC, MCP — is
# served over TLS. When the operator supplies a pair via PROVISA_TLS_CERT/KEY,
# adopt it; otherwise generate a self-signed pair. Certs live under
# ${PROVISA_HOME}/certs (persisted across restarts) and are bind-mounted read-only
# into the app containers at /app/certs. app_startup's _resolve_tls falls back to
# PROVISA_TLS_CERT/KEY for every wire protocol, so one node cert covers them all.
CERT_DIR="${PROVISA_HOME}/certs"
CERT_HOST="${CERT_DIR}/provisa.crt"
KEY_HOST="${CERT_DIR}/provisa.key"
ensure_tls_certs() {
  mkdir -p "$CERT_DIR"
  chmod 700 "$CERT_DIR"
  # Operator-supplied pair wins — copy into the canonical mount dir so the
  # container path is uniform regardless of source.
  if [ -n "${PROVISA_TLS_CERT:-}" ] && [ -n "${PROVISA_TLS_KEY:-}" ]; then
    if [ ! -f "${PROVISA_TLS_CERT}" ] || [ ! -f "${PROVISA_TLS_KEY}" ]; then
      err "PROVISA_TLS_CERT/PROVISA_TLS_KEY set but the referenced files are missing."
      exit 1
    fi
    cp "$PROVISA_TLS_CERT" "$CERT_HOST"
    cp "$PROVISA_TLS_KEY" "$KEY_HOST"
    chmod 600 "$KEY_HOST"
    ok "TLS certificate installed from PROVISA_TLS_CERT/KEY."
    return
  fi
  # Idempotent: reuse an existing generated pair (survives restarts / re-runs).
  if [ -f "$CERT_HOST" ] && [ -f "$KEY_HOST" ]; then
    ok "TLS certificate already present at ${CERT_HOST}."
    return
  fi
  if ! command -v openssl >/dev/null 2>&1; then
    err "openssl not found — cannot generate a TLS certificate. Install openssl or supply PROVISA_TLS_CERT/KEY."
    exit 1
  fi
  local cn ip_san=""
  cn="$(hostname -I 2>/dev/null | awk '{print $1}')"
  cn="${cn:-localhost}"
  # A secondary knows the primary's address; add it as a SAN so a client that
  # reaches this node via the primary hostname/IP still validates.
  [ -n "$PRIMARY_IP" ] && [[ "$PRIMARY_IP" =~ ^[0-9]+(\.[0-9]+){3}$ ]] && ip_san=",IP:${PRIMARY_IP}"
  local san
  if [[ "$cn" =~ ^[0-9]+(\.[0-9]+){3}$ ]]; then
    san="DNS:localhost,DNS:primary.provisa.internal,IP:127.0.0.1,IP:${cn}${ip_san}"
  else
    san="DNS:localhost,DNS:primary.provisa.internal,DNS:${cn},IP:127.0.0.1${ip_san}"
  fi
  info "Generating self-signed TLS certificate (CN=${cn})..."
  openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
    -keyout "$KEY_HOST" -out "$CERT_HOST" \
    -subj "/CN=${cn}" -addext "subjectAltName=${san}"
  chmod 600 "$KEY_HOST"
  ok "TLS certificate generated at ${CERT_HOST}."
}

# ── Node overlay: TLS + cluster role (primary) ────────────────────────────────
# The primary runs the full core+app stack; this additive overlay serves the API
# and UI over TLS (uvicorn --ssl-*), mounts the node cert, and pins PROVISA_ROLE.
# scripts/provisa auto-includes ~/.provisa/extensions/*/docker-compose.*.yml.
# The secondary is NOT an overlay — it needs a different service SET (no local
# postgres/redis/minio/trino), which compose cannot express by merging onto app.yml
# because depends_on unions rather than removes; write_secondary_compose emits a
# standalone base instead.
write_node_overlay() {
  local dir="${PROVISA_HOME}/extensions/node"
  local file="${dir}/docker-compose.node.yml"
  if [ "$ROLE" != "primary" ]; then
    rm -f "$file" 2>/dev/null || true
    return
  fi
  mkdir -p "$dir"
  cat > "$file" <<YAML
# Auto-generated by first-launch.sh — REQ-1226: TLS on every endpoint + cluster role.
services:
  provisa:
    command: ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--ssl-certfile", "/app/certs/provisa.crt", "--ssl-keyfile", "/app/certs/provisa.key"]
    volumes:
      - ${CERT_DIR}:/app/certs:ro
    environment:
      PROVISA_ROLE: "primary"
      PROVISA_TLS_CERT: "/app/certs/provisa.crt"
      PROVISA_TLS_KEY: "/app/certs/provisa.key"
      PROVISA_MCP_TLS: "1"
      # REQ-1266: forward the IdP selection + Firebase backend vars into the API
      # container so setup_router._auto_configure_idp wires the provider. Interpolated
      # (not baked) so the secret service-account key stays in the 600 systemd env_file
      # (${PROVISA_HOME}/provisa.env) that \`provisa start\` runs under.
      PROVISA_IDP: "\${PROVISA_IDP:-}"
      FIREBASE_PROJECT_ID: "\${FIREBASE_PROJECT_ID:-}"
      FIREBASE_SERVICE_ACCOUNT_KEY: "\${FIREBASE_SERVICE_ACCOUNT_KEY:-}"
      # REQ-125: break-glass superuser. The auth.superuser block written by
      # _auto_configure_idp resolves these by \${env:...}, so they must be in the container.
      PROVISA_SUPERUSER_USERNAME: "\${PROVISA_SUPERUSER_USERNAME:-}"
      PROVISA_SUPERUSER_PASSWORD: "\${PROVISA_SUPERUSER_PASSWORD:-}"
      # REQ-1266: multitenant onboarding gate. setup_router._auto_configure_idp reads
      # PROVISA_MULTITENANCY at first config to promote the deployment from single-admin
      # bootstrap to invite-based multi-org. Absent/empty (enterprise default) keeps the
      # single-administrator REQ-1266 mode. Interpolated from the 600 systemd env_file.
      PROVISA_MULTITENANCY: "\${PROVISA_MULTITENANCY:-}"
      # REQ-972..979: the deployment's federation-engine selection. resolve_deployment writes
      # it to config.yaml (\`engine:\`) for the node tier, but the API process selects its engine
      # from \$PROVISA_ENGINE / \$PROVISA_ENGINE_URL / \$PROVISA_MATERIALIZE_URL
      # (federation/engine.py:build_engine, configured_engine_url, configured_materialize_url) —
      # unforwarded, a Trino deployment booted the shipped provisa-install.yaml default (duckdb).
      # Empty (enterprise zero-config) leaves the persisted federation_engine field in charge.
      PROVISA_ENGINE: "\${PROVISA_ENGINE:-}"
      PROVISA_ENGINE_URL: "\${PROVISA_ENGINE_URL:-}"
      PROVISA_MATERIALIZE_URL: "\${PROVISA_MATERIALIZE_URL:-}"
      # REQ-1330: outbound-mail provider selection for the EmailSender port. The
      # provisa-install.yaml mail: section interpolates these; the Resend key stays
      # in the 600 systemd env_file. Empty (enterprise default) leaves the SMTP
      # adapter unconfigured — the multitenancy gate stops sending anyway.
      PROVISA_MAIL_PROVIDER: "\${PROVISA_MAIL_PROVIDER:-}"
      PROVISA_EMAIL_API_KEY: "\${PROVISA_EMAIL_API_KEY:-}"
      PROVISA_MAIL_FROM: "\${PROVISA_MAIL_FROM:-}"
      PROVISA_MAIL_BASE_URL: "\${PROVISA_MAIL_BASE_URL:-}"
  provisa-ui:
    command: ["uvicorn", "provisa.ui_server:app", "--host", "0.0.0.0", "--port", "3000", "--ssl-certfile", "/app/certs/provisa.crt", "--ssl-keyfile", "/app/certs/provisa.key"]
    volumes:
      - ${CERT_DIR}:/app/certs:ro
    environment:
      # REQ-1226: the API now serves TLS on 8000, so the UI's reverse-proxy hop
      # must use https — a plaintext http:// hop to a TLS port dies with ReadError.
      PROVISA_API_URL: "https://provisa:8000"
      # REQ-1266: the SPA's Firebase web config (public client keys) — ui_server
      # serves these at /firebase-config.js so the login page's Google sign-in works.
      VITE_FIREBASE_API_KEY: "\${VITE_FIREBASE_API_KEY:-}"
      VITE_FIREBASE_AUTH_DOMAIN: "\${VITE_FIREBASE_AUTH_DOMAIN:-}"
      VITE_FIREBASE_PROJECT_ID: "\${VITE_FIREBASE_PROJECT_ID:-}"
YAML
  ok "Node overlay written: ${file}"
}

# ── Secondary base compose (standalone) ───────────────────────────────────────
# A secondary runs ONLY the app tier (provisa API/UI + its in-process protocol
# listeners) and shares the primary's control plane and query engine. It does not
# run postgres/redis/minio/trino — those are primary singletons reached over the
# intra-cluster network. Every control-plane and store endpoint is repointed at the
# primary so the node joins the SAME shared schema (single-writer invariant: the
# app.py guard already forces load_config replace=OFF off the primary). This is a
# base, not an overlay — scripts/provisa selects it in place of core+app for a
# secondary because compose cannot remove app.yml's depends_on by merging.
write_secondary_compose() {
  local file="${PROVISA_HOME}/compose-secondary.yml"
  if [ "$ROLE" != "secondary" ]; then
    rm -f "$file" 2>/dev/null || true
    return
  fi
  # The primary's PG/Redis/MinIO carry the deployment's default shared credentials
  # (docker-compose.app.yml: provisa/provisa). GCP never overrides them, so the
  # secondary connects with the same pair. Passwords are file-only by design; this
  # is the deployment credential, not a stored secret.
  cat > "$file" <<YAML
# Auto-generated by first-launch.sh — secondary node (app tier only).
# Control plane (PostgreSQL), cache (Redis), object store (MinIO) and query engine
# (Trino coordinator) all live on the primary; this node connects to them over the
# intra-cluster network at ${PRIMARY_IP}. REQ-1226: all endpoints served over TLS.
services:
  provisa:
    restart: unless-stopped
    image: provisa/provisa:local
    command: ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--ssl-certfile", "/app/certs/provisa.crt", "--ssl-keyfile", "/app/certs/provisa.key"]
    ports:
      - "8000:8000"
      - "8815:8815"
    volumes:
      - ${CERT_DIR}:/app/certs:ro
    environment:
      PROVISA_ROLE: "secondary"
      PROVISA_TLS_CERT: "/app/certs/provisa.crt"
      PROVISA_TLS_KEY: "/app/certs/provisa.key"
      PROVISA_MCP_TLS: "1"
      PG_HOST: "${PRIMARY_IP}"
      PG_PORT: "5432"
      PG_DATABASE: "provisa"
      PG_USER: "provisa"
      PG_PASSWORD: "provisa"
      TENANT_DATABASE_URL: "postgresql+asyncpg://provisa:provisa@${PRIMARY_IP}:5432/provisa"
      PLATFORM_DATABASE_URL: "postgresql+asyncpg://provisa:provisa@${PRIMARY_IP}:5432/provisa"
      REDIS_URL: "redis://${PRIMARY_IP}:6379"
      TRINO_HOST: "${PRIMARY_IP}"
      TRINO_PORT: "8080"
      # Arrow Flight SQL proxy — a primary singleton, like Trino itself
      # (trino_lifecycle.connect_infra reads ZAYCHIK_HOST/ZAYCHIK_PORT).
      ZAYCHIK_HOST: "${PRIMARY_IP}"
      ZAYCHIK_PORT: "8480"
      FLIGHT_PORT: "8815"
      PROVISA_OTEL_S3_ENDPOINT: "http://${PRIMARY_IP}:9000"
      # REQ-1266: forward the IdP selection + Firebase backend vars into the API
      # container (the secondary auto-configures the same provider as the primary).
      PROVISA_IDP: "\${PROVISA_IDP:-}"
      FIREBASE_PROJECT_ID: "\${FIREBASE_PROJECT_ID:-}"
      FIREBASE_SERVICE_ACCOUNT_KEY: "\${FIREBASE_SERVICE_ACCOUNT_KEY:-}"
      # REQ-125: break-glass superuser. The auth.superuser block written by
      # _auto_configure_idp resolves these by \${env:...}, so they must be in the container.
      PROVISA_SUPERUSER_USERNAME: "\${PROVISA_SUPERUSER_USERNAME:-}"
      PROVISA_SUPERUSER_PASSWORD: "\${PROVISA_SUPERUSER_PASSWORD:-}"
  provisa-ui:
    restart: unless-stopped
    image: provisa/provisa:local
    command: ["uvicorn", "provisa.ui_server:app", "--host", "0.0.0.0", "--port", "3000", "--ssl-certfile", "/app/certs/provisa.crt", "--ssl-keyfile", "/app/certs/provisa.key"]
    ports:
      # Publish on the deployment's UI port (scripts/provisa exports UI_PORT from
      # config.yaml ui_port), matching the primary's ${UI_PORT}:3000. Cloud sets
      # ui_port: 443 and the shared TCP LB forwards ALL ports to every node, so a
      # secondary that only bound :3000 refuses the LB's :443 traffic (the browser
      # sees ERR_CONNECTION_REFUSED on round-robined asset requests).
      - "\${UI_PORT:-3000}:3000"
    volumes:
      - ${CERT_DIR}:/app/certs:ro
    environment:
      # REQ-1226: the API now serves TLS on 8000, so the UI's reverse-proxy hop
      # must use https — a plaintext http:// hop to a TLS port dies with ReadError.
      PROVISA_API_URL: "https://provisa:8000"
      # REQ-1266: the SPA's Firebase web config (public client keys), served at
      # /firebase-config.js so the login page's Google sign-in works.
      VITE_FIREBASE_API_KEY: "\${VITE_FIREBASE_API_KEY:-}"
      VITE_FIREBASE_AUTH_DOMAIN: "\${VITE_FIREBASE_AUTH_DOMAIN:-}"
      VITE_FIREBASE_PROJECT_ID: "\${VITE_FIREBASE_PROJECT_ID:-}"
    depends_on:
      - provisa
YAML
  ok "Secondary base compose written: ${file}"
}

# ── Control-plane base compose (standalone) ───────────────────────────────────
# The hosted SaaS role (REQ-1451). It runs the app tier plus the two stateful
# singletons that stay on this VM (redis, minio) and the Flight proxy, and NOTHING
# of the query engine: every coordinator is a pod on the GKE cluster, dialed by
# name through the cluster's VPC-scoped Cloud DNS domain. The control-plane
# database is Cloud SQL, reached over the PSA-peered VPC, so postgres and pgbouncer
# are absent too.
#
# Standalone rather than an overlay for the same reason as the secondary: compose
# unions depends_on rather than removing it, so app.yml's `trino: service_healthy`
# would survive any overlay and hold the app down forever on a node that has no
# trino service at all.
write_control_plane_compose() {
  local file="${PROVISA_HOME}/compose-control-plane.yml"
  if [ "$ROLE" != "control-plane" ]; then
    rm -f "$file" 2>/dev/null || true
    return
  fi
  # Every ${...} below is escaped: these resolve at `provisa start` from the 600
  # systemd env_file, not here. CONFIG_DB_* names Cloud SQL and is required (`:?`)
  # rather than defaulted — a control plane that silently fell back to a local
  # postgres would come up healthy against the wrong data plane, which is the failure
  # nothing catches. The engine is not named here at all: it is a shard pod the
  # control plane resolves through the Kubernetes API on each wake (REQ-1448).
  cat > "$file" <<YAML
# Auto-generated by first-launch.sh — control-plane node (REQ-1451).
# Control plane (PostgreSQL) is Cloud SQL; every query engine is a pod on the GKE
# engine cluster. This node runs the API, the UI, redis, minio and the Flight proxy.
services:
  provisa:
    restart: unless-stopped
    image: provisa/provisa:local
    command: ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--ssl-certfile", "/app/certs/provisa.crt", "--ssl-keyfile", "/app/certs/provisa.key"]
    ports:
      - "8000:8000"
      - "8815:8815"
    volumes:
      - ${CERT_DIR}:/app/certs:ro
    environment:
      PROVISA_ROLE: "control-plane"
      # The config the app loads (REQ-528). It has no default in code, and the image bakes no
      # config/provisa.yaml, so a container without this boots into a startup error. The wizard
      # skeleton is the non-demo answer, same as native launch (start-ui-install.sh:190); a demo
      # install's overlay overrides it with the pre-federated demo config.
      PROVISA_CONFIG: "/app/config/provisa-install-base.yaml"
      PROVISA_TLS_CERT: "/app/certs/provisa.crt"
      PROVISA_TLS_KEY: "/app/certs/provisa.key"
      PROVISA_MCP_TLS: "1"
      PROVISA_MULTITENANCY: "\${PROVISA_MULTITENANCY:?the SaaS control plane is multitenant}"
      # Cloud SQL over the PSA-peered VPC.
      PG_HOST: "\${CONFIG_DB_HOST:?Cloud SQL private IP}"
      PG_PORT: "\${CONFIG_DB_PORT:-5432}"
      PG_DATABASE: "\${CONFIG_DB_NAME:-provisa}"
      PG_USER: "\${CONFIG_DB_USER:-provisa}"
      PG_PASSWORD: "\${CONFIG_DB_PASSWORD:?Cloud SQL password}"
      TENANT_DATABASE_URL: "postgresql+asyncpg://\${CONFIG_DB_USER:-provisa}:\${CONFIG_DB_PASSWORD}@\${CONFIG_DB_HOST}:\${CONFIG_DB_PORT:-5432}/\${CONFIG_DB_NAME:-provisa}"
      PLATFORM_DATABASE_URL: "postgresql+asyncpg://\${CONFIG_DB_USER:-provisa}:\${CONFIG_DB_PASSWORD}@\${CONFIG_DB_HOST}:\${CONFIG_DB_PORT:-5432}/\${CONFIG_DB_NAME:-provisa}"
      PROVISA_EXTERNAL_CONTROL_DB: "\${PROVISA_EXTERNAL_CONTROL_DB:-1}"
      # The engine every Starter org queries is a shard POD on the cluster, created on
      # wake and gone on idle, so there is no hostname to pass here: the control plane
      # reads the ready pod's address off the Kubernetes API each time (REQ-1448).
      # TRINO_HOST is still forwarded for a deployment that names a static engine it
      # does not operate, and is empty on this one.
      PROVISA_ENGINE: "trino"
      TRINO_HOST: "\${TRINO_HOST:-}"
      TRINO_PORT: "\${TRINO_PORT:-8080}"
      # Where an isolated (Pro) engine is dialed, and — separately — what this
      # process needs to CREATE one on the cluster (k8s_provisioner).
      PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE: "\${PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE:-}"
      PROVISA_ISOLATED_ENGINE_PORT: "\${PROVISA_ISOLATED_ENGINE_PORT:-8080}"
      PROVISA_ENGINE_CLUSTER_PROJECT: "\${PROVISA_ENGINE_CLUSTER_PROJECT:-}"
      PROVISA_ENGINE_CLUSTER_LOCATION: "\${PROVISA_ENGINE_CLUSTER_LOCATION:-}"
      PROVISA_ENGINE_CLUSTER_NAME: "\${PROVISA_ENGINE_CLUSTER_NAME:-}"
      # Which cluster topology is deployed (autopilot|standard) and the zone a shard
      # pod must land in — an autopilot cluster is regional, so the zone is not
      # implied by the location (REQ-1465).
      PROVISA_ENGINE_CLUSTER_MODE: "\${PROVISA_ENGINE_CLUSTER_MODE:-}"
      PROVISA_ENGINE_CLUSTER_ZONE: "\${PROVISA_ENGINE_CLUSTER_ZONE:-}"
      PROVISA_ENGINE_NAMESPACE: "\${PROVISA_ENGINE_NAMESPACE:-provisa-engines}"
      PROVISA_ENGINE_IMAGE: "\${PROVISA_ENGINE_IMAGE:-}"
      # Which shard this control plane's own terminal is bound to, so boot can wake it
      # when it is found at zero replicas, and the port its coordinator listens on.
      PROVISA_ENGINE_SHARD: "\${PROVISA_ENGINE_SHARD:-}"
      PROVISA_ENGINE_PORT: "\${PROVISA_ENGINE_PORT:-8080}"
      # No ZAYCHIK_HOST/ZAYCHIK_PORT on this role: the Flight SQL proxy runs as a
      # sidecar in the shard's own pod, so the control plane reads its address off
      # the same ready pod it dials for HTTP (shard_flight_endpoint, REQ-1448).
      PROVISA_ZAYCHIK_IMAGE: "\${PROVISA_ZAYCHIK_IMAGE:-}"
      FLIGHT_PORT: "8815"
      REDIS_URL: "redis://redis:6379"
      # This base REPLACES docker-compose.app.yml, so the collector endpoint app.yml declares
      # has to be declared again here or it never reaches the container: scripts/provisa
      # exports it when the resolved compose set includes an observability overlay, but an
      # env var only enters a service that names it. Without these two lines the control
      # plane exported no telemetry at all and every ops report read zero rows while the
      # collector on the same node sat idle. Empty default = telemetry stays local.
      OTEL_EXPORTER_OTLP_ENDPOINT: "\${OTEL_EXPORTER_OTLP_ENDPOINT:-}"
      OTEL_SERVICE_NAME: "\${OTEL_SERVICE_NAME:-provisa}"
      PROVISA_OTEL_S3_ENDPOINT: "http://minio:9000"
      # The same store as Trino sees it. The engine reads the OTel Iceberg tables itself,
      # from pods in the GKE cluster where the compose service name "minio" resolves to
      # nothing (UnknownHostException: minio), so the catalog it is handed carries the
      # coordinator's VPC address instead (engine_visible_s3_endpoint, REQ-1451).
      PROVISA_ENGINE_OTEL_S3_ENDPOINT: "\${PROVISA_ENGINE_OTEL_S3_ENDPOINT:?the coordinator's VPC address for MinIO}"
      PROVISA_OTEL_S3_ACCESS_KEY: "minioadmin"
      PROVISA_OTEL_S3_SECRET_KEY: "minioadmin"
      PROVISA_OTEL_BUCKET: "\${PROVISA_OTEL_BUCKET:-provisa-otel}"
      PROVISA_IDP: "\${PROVISA_IDP:-}"
      FIREBASE_PROJECT_ID: "\${FIREBASE_PROJECT_ID:-}"
      FIREBASE_SERVICE_ACCOUNT_KEY: "\${FIREBASE_SERVICE_ACCOUNT_KEY:-}"
      # REQ-125: break-glass superuser. The auth.superuser block written by
      # _auto_configure_idp resolves these by \${env:...}, so they must be in the container.
      PROVISA_SUPERUSER_USERNAME: "\${PROVISA_SUPERUSER_USERNAME:-}"
      PROVISA_SUPERUSER_PASSWORD: "\${PROVISA_SUPERUSER_PASSWORD:-}"
      PROVISA_MAIL_PROVIDER: "\${PROVISA_MAIL_PROVIDER:-}"
      PROVISA_EMAIL_API_KEY: "\${PROVISA_EMAIL_API_KEY:-}"
      PROVISA_MAIL_FROM: "\${PROVISA_MAIL_FROM:-}"
      PROVISA_MAIL_BASE_URL: "\${PROVISA_MAIL_BASE_URL:-}"
      # REQ-1455/REQ-1474: the merchant of record. This base REPLACES docker-compose.app.yml
      # and forwards no variable it does not name, so the commercial plugin's credentials have
      # to be listed here or they never enter the container — provisa.env holding them is not
      # enough. LEMONSQUEEZY_MODE selects the store: "test" reads TEST_LEMONSQUEEZY_API_KEY,
      # anything else the live key, so both are forwarded and the mode decides which is read.
      LEMONSQUEEZY_MODE: "\${LEMONSQUEEZY_MODE:-}"
      LEMONSQUEEZY_API_KEY: "\${LEMONSQUEEZY_API_KEY:-}"
      TEST_LEMONSQUEEZY_API_KEY: "\${TEST_LEMONSQUEEZY_API_KEY:-}"
      LEMONSQUEEZY_STORE_ID: "\${LEMONSQUEEZY_STORE_ID:-}"
      LEMONSQUEEZY_VARIANT_STARTER: "\${LEMONSQUEEZY_VARIANT_STARTER:-}"
      LEMONSQUEEZY_VARIANT_PRO_S: "\${LEMONSQUEEZY_VARIANT_PRO_S:-}"
      LEMONSQUEEZY_VARIANT_PRO_M: "\${LEMONSQUEEZY_VARIANT_PRO_M:-}"
      LEMONSQUEEZY_VARIANT_PRO_L: "\${LEMONSQUEEZY_VARIANT_PRO_L:-}"
      # REQ-1482: egress rides a second subscription, so each plan names a second variant too.
      LEMONSQUEEZY_VARIANT_EGRESS_STARTER: "\${LEMONSQUEEZY_VARIANT_EGRESS_STARTER:-}"
      LEMONSQUEEZY_VARIANT_EGRESS_PRO_S: "\${LEMONSQUEEZY_VARIANT_EGRESS_PRO_S:-}"
      LEMONSQUEEZY_VARIANT_EGRESS_PRO_M: "\${LEMONSQUEEZY_VARIANT_EGRESS_PRO_M:-}"
      LEMONSQUEEZY_VARIANT_EGRESS_PRO_L: "\${LEMONSQUEEZY_VARIANT_EGRESS_PRO_L:-}"
      LEMONSQUEEZY_SIGNING_SECRET: "\${LEMONSQUEEZY_SIGNING_SECRET:-}"
    depends_on:
      redis:
        condition: service_healthy
      minio:
        condition: service_healthy
  provisa-ui:
    restart: unless-stopped
    image: provisa/provisa:local
    command: ["uvicorn", "provisa.ui_server:app", "--host", "0.0.0.0", "--port", "3000", "--ssl-certfile", "/app/certs/provisa.crt", "--ssl-keyfile", "/app/certs/provisa.key"]
    ports:
      - "\${UI_PORT:-3000}:3000"
    volumes:
      - ${CERT_DIR}:/app/certs:ro
    environment:
      PROVISA_API_URL: "https://provisa:8000"
      VITE_FIREBASE_API_KEY: "\${VITE_FIREBASE_API_KEY:-}"
      VITE_FIREBASE_AUTH_DOMAIN: "\${VITE_FIREBASE_AUTH_DOMAIN:-}"
      VITE_FIREBASE_PROJECT_ID: "\${VITE_FIREBASE_PROJECT_ID:-}"
    depends_on:
      - provisa
  redis:
    restart: unless-stopped
    image: redis:7-alpine
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 30
      start_period: 60s
  minio:
    restart: unless-stopped
    image: minio/minio:latest
    # Published, but only on the VPC address: the engine pods run outside this VM and read
    # the OTel Iceberg tables straight from here, so the port has to leave the compose
    # network. Binding to the node's internal IP rather than 0.0.0.0 keeps it off the
    # external interface, and the firewall admits only the cluster's pod range.
    ports:
      - "\${PROVISA_MINIO_BIND_IP:?the node's VPC-internal address}:9000:9000"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
    volumes:
      # A host path, not a named volume: the Iceberg metastore for otel/results is a table in
      # the external control DB, which outlives this VM, so the warehouse it points at has to
      # outlive it too. A named volume sits on the boot disk and dies with the instance, and
      # every iceberg_tables row then names a metadata object that no longer exists
      # (ICEBERG_MISSING_METADATA on every ops query). The deployment mounts a disk of its own
      # here and passes the path.
      - "\${PROVISA_OBJECT_STORE_DIR:?the object store's data directory}:/data"
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 5s
      timeout: 5s
      retries: 30
      start_period: 60s
  minio-init:
    image: minio/mc:latest
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
        mc alias set local http://minio:9000 minioadmin minioadmin &&
        mc mb --ignore-existing local/\${PROVISA_OTEL_BUCKET:-provisa-otel} &&
        mc mb --ignore-existing local/provisa-results &&
        mc mb --ignore-existing local/provisa-hive-s3
      "
  # No zaychik service on this role. The Arrow Flight SQL proxy holds a JDBC
  # connection to Trino, so it belongs to the engine's lifetime, not this one: it runs
  # as a sidecar in the shard pod, where it is created, woken and destroyed with the
  # coordinator it fronts and reaches it at localhost (k8s_provisioner, REQ-1448).

volumes:
  redis_data:
YAML
  ok "Control-plane base compose written: ${file}"
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
  # CONFIG_DB_* / PROVISA_EXTERNAL_CONTROL_DB: external control-plane database mode
  # (terraform/gcp-saas — Cloud SQL). docker-compose.app.yml interpolates PG_* /
  # PLATFORM_DATABASE_URL / TENANT_DATABASE_URL from CONFIG_DB_*, so persisting them
  # here points the coordinator's control plane at the managed DB instead of the
  # bundled postgres. Absent (enterprise), the compose defaults resolve to the
  # in-stack postgres service.
  # PROVISA_SUPERUSER_*: REQ-125 break-glass account. The config's auth.superuser block
  # references these by ${env:...}, so they must reach the container's environment or the
  # placeholder resolves to nothing and the account cannot authenticate.
  for var in PROVISA_IDP FIREBASE_PROJECT_ID FIREBASE_SERVICE_ACCOUNT_KEY \
             PROVISA_SUPERUSER_USERNAME PROVISA_SUPERUSER_PASSWORD \
             VITE_FIREBASE_API_KEY VITE_FIREBASE_AUTH_DOMAIN VITE_FIREBASE_PROJECT_ID \
             KEYCLOAK_URL KEYCLOAK_REALM KEYCLOAK_CLIENT_ID \
             OAUTH_ISSUER OAUTH_CLIENT_ID OAUTH_CLIENT_SECRET \
             PROVISA_PGWIRE_PORT PROVISA_BOLT_PORT PROVISA_MCP_PORT \
             PROVISA_MCP_HOST PROVISA_MCP_ROLE GRPC_PORT \
             PROVISA_MULTITENANCY \
             PROVISA_ENGINE PROVISA_ENGINE_URL PROVISA_MATERIALIZE_URL \
             PROVISA_MAIL_PROVIDER PROVISA_EMAIL_API_KEY PROVISA_MAIL_FROM PROVISA_MAIL_BASE_URL \
             PROVISA_EXTERNAL_CONTROL_DB \
             CONFIG_DB_HOST CONFIG_DB_PORT CONFIG_DB_NAME CONFIG_DB_USER CONFIG_DB_PASSWORD \
             PROVISA_ENGINE_CLUSTER_PROJECT PROVISA_ENGINE_CLUSTER_LOCATION \
             PROVISA_ENGINE_CLUSTER_NAME \
             PROVISA_ENGINE_CLUSTER_MODE PROVISA_ENGINE_CLUSTER_ZONE \
             PROVISA_ENGINE_NAMESPACE PROVISA_ENGINE_IMAGE PROVISA_ZAYCHIK_IMAGE \
             PROVISA_ENGINE_SHARD \
             PROVISA_ENGINE_PORT \
             PROVISA_MINIO_BIND_IP PROVISA_ENGINE_OTEL_S3_ENDPOINT PROVISA_OBJECT_STORE_DIR \
             TRINO_HOST TRINO_PORT \
             PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE PROVISA_ISOLATED_ENGINE_PORT; do
    if [ -n "${!var:-}" ]; then
      printf '%s=%s\n' "$var" "${!var}" >> "$env_file"
    fi
  done
  # $USER is not exported under the GCE metadata runner / systemd-run; resolve it.
  local run_user; run_user="$(id -un)"
  # System Docker runs as its own systemd service — order after it and skip the
  # rootless XDG_RUNTIME_DIR (only meaningful for the bundled per-user daemon).
  local after="network-online.target" wants="network-online.target" xdg_line=""
  if [ "$DOCKER_MODE" = system ]; then
    after="network-online.target docker.service"
    wants="network-online.target docker.service"
  else
    xdg_line="Environment=XDG_RUNTIME_DIR=${PROVISA_HOME}/run"
  fi
  cat > "$unit" <<UNIT
[Unit]
Description=Provisa Data Platform
After=${after}
Wants=${wants}

[Service]
# oneshot + RemainAfterExit: \`provisa start\` brings the stack up detached
# (compose up -d for the Docker tier, background uvicorn for native) and returns.
# With Type=simple systemd would treat that return as the service exiting and fire
# ExecStop, tearing the stack down. oneshot keeps the unit active after ExecStart
# returns, so ExecStop runs only on an explicit stop/restart.
Type=oneshot
RemainAfterExit=yes
User=${run_user}
Environment=DOCKER_HOST=unix://${DOCKER_SOCKET}
${xdg_line}
EnvironmentFile=-${env_file}
ExecStart=${LOCAL_BIN}/provisa start
ExecStop=${LOCAL_BIN}/provisa stop
TimeoutStartSec=300

[Install]
WantedBy=multi-user.target
UNIT
  systemctl daemon-reload
  systemctl enable provisa
  # restart, not `enable --now`: on a version-change re-provision the stack is already
  # up from boot (WantedBy=multi-user.target), so `--now` is a no-op and the old
  # containers keep running on the previous image. restart forces `provisa stop`+`start`
  # (compose down+up) under the EnvironmentFile, recreating from the freshly-loaded image.
  systemctl restart provisa
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
# REQ-1443: the demo config registers a Great Expectations suite over the pet inventory, so the
# quality scorecard has data only when GX is installed. A demo install that named no checker takes
# GX (Apache 2.0, no hosted-service bar) rather than shipping a demo whose scan cannot run.
apply_demo_checker_default() {
  if [ "$(_lc "$INSTALL_DEMO")" = "y" ] && [ "$DQ_CHECKER" = "none" ]; then
    DQ_CHECKER="gx"
    info "Demo selected: installing Great Expectations (Apache 2.0) so the quality scorecard has data."
  fi
}

resolve_deployment() {
  if [ "$NON_INTERACTIVE" = true ]; then
    DEPLOY_ENGINE="${PROVISA_ENGINE:-duckdb}"
    ENGINE_URL="${PROVISA_ENGINE_URL:-}"
    MATERIALIZE_URL="${PROVISA_MATERIALIZE_URL:-}"
    OBS_MODE="${PROVISA_OBS_MODE:-none}"
    OTLP_ENDPOINT="${PROVISA_OTLP_ENDPOINT:-}"
    INSTALL_DEMO="${PROVISA_INSTALL_DEMO:-n}"
    # REQ-1443: none | soda | gx. Default none — a checker is an external, out-of-process
    # component; it is never installed unless the operator asks for it by name.
    DQ_CHECKER="${PROVISA_DQ_CHECKER:-none}"
    _compute_needs_docker
    apply_demo_checker_default
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

  # ── Data quality checker (REQ-1443, optional) ──
  # An external process aimed at Provisa's own pgwire endpoint; its scan results land as ordinary
  # source rows. Soda is Elastic License 2.0 — self-hosted only, never the hosted cloud plane.
  printf "\n${BOLD}Data quality checker (optional)${NC}\n"
  printf "  1) none  — skip (default)\n  2) soda  — Soda Core contracts (Elastic License 2.0; self-hosted only)\n  3) gx    — Great Expectations suites (Apache 2.0)\n"
  local qc; read -rp "$(printf "${CYAN}[provisa]${NC} Choose 1-3 [1]: ")" qc
  case "$qc" in 2) DQ_CHECKER="soda" ;; 3) DQ_CHECKER="gx" ;; *) DQ_CHECKER="none" ;; esac
  _compute_needs_docker
  apply_demo_checker_default
  ok "Deployment: engine=${DEPLOY_ENGINE} obs=${OBS_MODE} demo=${INSTALL_DEMO}/${DEMO_MODE} docker=${NEEDS_DOCKER} dq=${DQ_CHECKER}"
}

# ── The pyproject extra set for the native venv (REQ-1443) ───────────────────
# The checker the operator chose is installed alongside the base extras. `none` adds nothing, so a
# default install acquires no checker at all — soda-core is Elastic License 2.0 and is never vendored.
_native_extras() {
  case "${DQ_CHECKER:-none}" in
    soda) printf 'embedded,soda' ;;
    gx)   printf 'embedded,gx' ;;
    none) printf 'embedded' ;;
    *) err "Unknown data-quality checker '${DQ_CHECKER}' (expected none|soda|gx)."; exit 1 ;;
  esac
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
  # REQ-1443: the operator's chosen data-quality checker rides in as a pyproject extra.
  local extras; extras="$(_native_extras)"

  if _online; then
    info "Installing Provisa from PyPI..."
    "$pip" install --quiet "provisa[${extras}]${pin}" uvicorn mcp-proxy
  elif [ -n "$wheels" ]; then
    info "Installing Provisa from bundled wheels (offline)..."
    "$pip" install --quiet --no-index --find-links "$wheels" "provisa[${extras}]" uvicorn mcp-proxy
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
  # An existing config is kept only while it describes the same role this run was
  # told to install. A node re-installed as --role control-plane over an earlier
  # primary install kept `role: primary` here, and the CLI then started the whole
  # primary stack — local Trino, local postgres, TRINO_HOST: trino — while the
  # systemd env file named a GKE shard the app never dialed (REQ-1451). Role is
  # deployment identity, so a mismatch rewrites rather than silently persisting.
  if [ -f "${PROVISA_HOME}/config.yaml" ]; then
    local existing_role
    existing_role="$(awk '$1 == "role:" {print $2; exit}' "${PROVISA_HOME}/config.yaml")"
    if [ "$existing_role" = "$ROLE" ]; then
      return
    fi
    warn "config.yaml says role: ${existing_role}, installing role: ${ROLE} — rewriting."
    mv "${PROVISA_HOME}/config.yaml" "${PROVISA_HOME}/config.yaml.bak-${existing_role}"
  fi

  local hostname api_port ui_port
  hostname="$(ask_hostname)"
  api_port="$(ask_api_port)"
  # UI host-publish port. The container always listens on 3000; the base compose
  # publishes ${ui_port}:3000. Cloud deploys export UI_PORT=443 so the shared LB
  # fronts the UI with no port suffix; desktop keeps the 3000 default (REQ-1254).
  # The provisa CLI reads every port from config.yaml (read_config), so this must
  # land here — an exported UI_PORT alone is clobbered at CLI line 41.
  ui_port="${UI_PORT:-3000}"

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
ui_port: ${ui_port}
runtime: ${runtime}
${img_src_line}
docker_host: "unix://${DOCKER_SOCKET}"
project_dir: "${COMPOSE_DIR}"
federation_workers: ${TRINO_WORKERS}
# Deployment (REQ-972..979): parity with the desktop wizard.
engine: ${DEPLOY_ENGINE:-duckdb}
engine_url: "${ENGINE_URL:-}"
materialize_url: "${MATERIALIZE_URL:-}"
obs_mode: ${OBS_MODE:-none}
otlp_endpoint: "${OTLP_ENDPOINT:-}"
demo: ${demo_flag}
# REQ-1443: the optional external data-quality checker (none|soda|gx). scripts/provisa turns it into
# the PROVISA_EXTRAS docker build arg; the native venv installs the matching pyproject extra.
dq_checker: ${DQ_CHECKER:-none}
YAML

  elif [ "$ROLE" = "control-plane" ]; then
    cat > "${PROVISA_HOME}/config.yaml" <<YAML
# Provisa configuration — control-plane node (hosted SaaS, REQ-1451)
#
# This node runs the API, the UI, Redis, MinIO and the Flight proxy. It does NOT
# run PostgreSQL (Cloud SQL holds the control plane) and it does NOT run Trino:
# every federation engine is a pod on the GKE engine cluster, dialed by Service
# name through the cluster's VPC-scoped Cloud DNS domain. Engine addressing lives
# in the systemd env file, not here — nothing in this file selects a shard.

role: control-plane
hostname: ${hostname}
api_port: ${api_port}
ui_port: ${ui_port}
runtime: ${runtime}
${img_src_line}
docker_host: "unix://${DOCKER_SOCKET}"
project_dir: "${COMPOSE_DIR}"
# No local query engine: federation_workers is the count of Trino workers this node
# starts, and it starts none.
federation_workers: 0
engine: trino
engine_url: "${ENGINE_URL:-}"
materialize_url: "${MATERIALIZE_URL:-}"
obs_mode: ${OBS_MODE:-none}
otlp_endpoint: "${OTLP_ENDPOINT:-}"
demo: ${demo_flag}
dq_checker: ${DQ_CHECKER:-none}
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
ui_port: ${ui_port}
runtime: ${runtime}
${img_src_line}
docker_host: "unix://${DOCKER_SOCKET}"
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
      printf '\n# Provisa Docker runtime\nexport DOCKER_HOST="%s"\n' \
        "unix://${DOCKER_SOCKET}" >> "$rc"
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

  # Env-only refresh: the node is already provisioned at this version, so the only thing that
  # can have changed is the environment the caller exported. Rewriting the EnvironmentFile and
  # restarting is the whole job.
  if [ "$REFRESH_ENV" = true ]; then
    install_systemd
    ok "Environment refreshed from the current startup environment."
    return
  fi

  resolve_deployment   # sets DEPLOY_ENGINE OBS_MODE INSTALL_DEMO DEMO_MODE NEEDS_DOCKER

  # ── Native tier (default): a Python venv, no Docker ──
  # Single-node — no primary/secondary role prompt; the venv serves everything.
  if [ "$NEEDS_DOCKER" = false ]; then
    # The native tier is single-node. A multi-node deploy (Terraform passes
    # --role secondary) only makes sense on the Trino/Docker tier, so fail loud
    # rather than silently degrading a secondary into a standalone primary.
    if [ "$CLI_ROLE" = "secondary" ] || [ "$CLI_ROLE" = "control-plane" ]; then
      err "engine=${DEPLOY_ENGINE} runs the single-node native tier, which has no ${CLI_ROLE} role."
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

    printf '%s' "$PROVISA_VERSION" > "$SENTINEL"
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
  stage_compose
  load_trino_plugins
  write_protocol_overlay
  write_demo_overlay
  ensure_tls_certs
  write_node_overlay
  write_secondary_compose
  write_control_plane_compose
  write_config
  install_cli

  if [ "$NON_INTERACTIVE" = true ]; then
    install_systemd
  fi

  printf '%s' "$PROVISA_VERSION" > "$SENTINEL"
  ok "First-launch setup complete."

  if [ "$NON_INTERACTIVE" = true ]; then
    ok "Node configured as ${ROLE}. systemd service enabled and started."
    return
  fi

  if [ "$ROLE" = "primary" ]; then
    printf "\n${GREEN}${BOLD}Provisa primary node is ready.${NC}\n"
    printf "Run: ${BOLD}provisa start${NC}\n"
    print_lb_guidance
  elif [ "$ROLE" = "control-plane" ]; then
    printf "\n${GREEN}${BOLD}Provisa control plane is ready.${NC}\n"
    printf "Run: ${BOLD}provisa start${NC}\n\n"
    printf "Control-plane database: Cloud SQL. Query engines: the GKE engine cluster.\n\n"
  else
    printf "\n${GREEN}${BOLD}Provisa secondary node is ready.${NC}\n"
    printf "Run: ${BOLD}provisa start${NC}\n\n"
    printf "This node will serve API traffic and Trino workers.\n"
    printf "Data plane (PostgreSQL, Redis, MinIO) is on the primary at ${BOLD}%s${NC}.\n\n" "$PRIMARY_IP"
  fi
}

main "$@"
