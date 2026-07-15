#!/usr/bin/env bash
# Provisa Installer — Phase AF1
# Detects container runtime, creates ~/.provisa/, installs CLI wrapper.
set -euo pipefail

PROVISA_HOME="${HOME}/.provisa"
CLI_INSTALL_DIR="/usr/local/bin"
CLI_NAME="provisa"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NON_INTERACTIVE=false
AIRGAP=false
MIN_DOCKER_VERSION="20.10"
MIN_COMPOSE_VERSION="2.0"

# Parse flags
for arg in "$@"; do
    case "$arg" in
        --non-interactive) NON_INTERACTIVE=true ;;
        --airgap) AIRGAP=true; NON_INTERACTIVE=true ;;
    esac
done

# ── Colors ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'
BOLD='\033[1m'; NC='\033[0m'

info()  { printf "${CYAN}[provisa]${NC} %s\n" "$*"; }
ok()    { printf "${GREEN}[provisa]${NC} %s\n" "$*"; }
warn()  { printf "${YELLOW}[provisa]${NC} %s\n" "$*"; }
err()   { printf "${RED}[provisa]${NC} %s\n" "$*" >&2; }

prompt_or_default() {
    local prompt_msg="$1"
    local default_val="$2"
    if [ "$NON_INTERACTIVE" = true ]; then
        echo "$default_val"
        return
    fi
    local answer
    read -rp "$(printf "${CYAN}[provisa]${NC} ${prompt_msg} [${default_val}]: ")" answer
    echo "${answer:-$default_val}"
}

# ── Version comparison ──────────────────────────────────────────────────────
version_gte() {
    # Returns 0 if $1 >= $2
    local v1="$1" v2="$2"
    if [ "$v1" = "$v2" ]; then return 0; fi
    local IFS=.
    local i v1parts=($v1) v2parts=($v2)
    for ((i=0; i<${#v2parts[@]}; i++)); do
        local a="${v1parts[i]:-0}"
        local b="${v2parts[i]:-0}"
        if ((a > b)); then return 0; fi
        if ((a < b)); then return 1; fi
    done
    return 0
}

# ── Container runtime detection ─────────────────────────────────────────────
detect_runtime() {
    if command -v docker &>/dev/null; then
        local docker_info
        docker_info="$(docker info 2>/dev/null || true)"
        if echo "$docker_info" | grep -qi "orbstack"; then
            echo "orbstack"
        elif echo "$docker_info" | grep -qi "colima"; then
            echo "colima"
        else
            echo "docker"
        fi
    else
        echo ""
    fi
}

check_docker_version() {
    local version
    version="$(docker version --format '{{.Server.Version}}' 2>/dev/null || echo "0.0.0")"
    # Strip any suffix (e.g., -ce, -beta)
    version="${version%%-*}"
    if version_gte "$version" "$MIN_DOCKER_VERSION"; then
        ok "Docker version ${version} (>= ${MIN_DOCKER_VERSION})"
        return 0
    else
        err "Docker version ${version} is below minimum ${MIN_DOCKER_VERSION}"
        return 1
    fi
}

check_compose() {
    if docker compose version &>/dev/null 2>&1; then
        local version
        version="$(docker compose version --short 2>/dev/null || echo "0.0.0")"
        version="${version#v}"
        if version_gte "$version" "$MIN_COMPOSE_VERSION"; then
            ok "Docker Compose version ${version} (>= ${MIN_COMPOSE_VERSION})"
            return 0
        else
            err "Docker Compose version ${version} is below minimum ${MIN_COMPOSE_VERSION}"
            return 1
        fi
    elif command -v docker-compose &>/dev/null; then
        warn "Found docker-compose (legacy). Docker Compose v2+ is recommended."
        return 0
    else
        return 1
    fi
}

# ── Deployment selection (parity with macOS SwiftUI wizard, REQ-972..979) ────
# Sets globals: DEPLOY_ENGINE ENGINE_URL MATERIALIZE_URL TRINO_HOST TRINO_PORT
#               OBS_MODE OTLP_ENDPOINT INSTALL_DEMO DEMO_MODE
resolve_deployment() {
    # Non-interactive / CI: read the same env vars the macOS wizard forwards.
    if [ "$NON_INTERACTIVE" = true ]; then
        DEPLOY_ENGINE="${PROVISA_ENGINE:-duckdb}"
        ENGINE_URL="${PROVISA_ENGINE_URL:-}"
        MATERIALIZE_URL="${PROVISA_MATERIALIZE_URL:-}"
        TRINO_HOST="${PROVISA_TRINO_HOST:-}"
        TRINO_PORT="${PROVISA_TRINO_PORT:-}"
        OBS_MODE="${PROVISA_OBS_MODE:-none}"
        OTLP_ENDPOINT="${PROVISA_OTLP_ENDPOINT:-}"
        INSTALL_DEMO="${PROVISA_INSTALL_DEMO:-n}"
        DEMO_MODE="${PROVISA_DEMO_MODE:-docker}"
        ok "Deployment: engine=${DEPLOY_ENGINE} obs=${OBS_MODE} demo=${INSTALL_DEMO}"
        return
    fi

    # ── Federation engine ──
    printf "\n${BOLD}Federation engine${NC}\n"
    printf "  1) DuckDB — native (recommended)\n"
    printf "  2) Trino — Docker\n"
    printf "  3) External engine\n"
    local engine_choice
    engine_choice="$(prompt_or_default "Choose 1-3" "1")"
    case "$engine_choice" in
        2) DEPLOY_ENGINE="trino" ;;
        3) DEPLOY_ENGINE="sqlalchemy" ;;
        *) DEPLOY_ENGINE="duckdb" ;;
    esac
    ENGINE_URL=""; MATERIALIZE_URL=""; TRINO_HOST=""; TRINO_PORT=""
    if [ "$DEPLOY_ENGINE" = "sqlalchemy" ]; then
        ENGINE_URL="$(prompt_or_default "External engine URL (e.g. postgresql+psycopg://user:pass@host:5432/db)" "")"
        MATERIALIZE_URL="$(prompt_or_default "Materialization store URL (optional)" "")"
    fi

    # ── Observability integration (built-in telemetry is always on) ──
    printf "\n${BOLD}Observability integration${NC}\n"
    printf "  1) Built-in only\n"
    printf "  2) Bundled Grafana/Prometheus demo (Docker)\n"
    printf "  3) Export to my collector\n"
    local obs_choice
    obs_choice="$(prompt_or_default "Choose 1-3" "1")"
    case "$obs_choice" in
        2) OBS_MODE="docker" ;;
        3) OBS_MODE="collector" ;;
        *) OBS_MODE="none" ;;
    esac
    OTLP_ENDPOINT=""
    if [ "$OBS_MODE" = "collector" ]; then
        OTLP_ENDPOINT="$(prompt_or_default "OTLP collector endpoint (e.g. http://collector-host:4317)" "")"
    fi

    # ── Demo ──
    echo "The demo is a complete, fully functional install — pick it with confidence; nothing is limited."
    echo "To reconfigure with other options later, just run this installer again."
    local demo_ans
    demo_ans="$(prompt_or_default "Install the demo dataset with guided tour (y/n)" "n")"
    case "$demo_ans" in
        [yY]|[yY][eE][sS]) INSTALL_DEMO="y" ;;
        *) INSTALL_DEMO="n" ;;
    esac
    DEMO_MODE="docker"

    ok "Deployment: engine=${DEPLOY_ENGINE} obs=${OBS_MODE} demo=${INSTALL_DEMO}"
}

# ── Config generation ────────────────────────────────────────────────────────
generate_config() {
    if [ -f "${PROVISA_HOME}/config.yaml" ] && [ "$NON_INTERACTIVE" = false ]; then
        warn "Config already exists at ${PROVISA_HOME}/config.yaml"
        local overwrite
        read -rp "$(printf "${CYAN}[provisa]${NC} Overwrite? [y/N]: ")" overwrite
        case "$overwrite" in
            [yY]|[yY][eE][sS]) ;;
            *) warn "Keeping existing config"; return ;;
        esac
    fi

    local ui_port api_port auto_open
    ui_port="$(prompt_or_default "UI port" "3000")"
    api_port="$(prompt_or_default "API port" "8000")"
    auto_open="$(prompt_or_default "Auto-open browser on start (true/false)" "true")"

    resolve_deployment

    local demo_flag
    case "$INSTALL_DEMO" in [yY]|[yY][eE][sS]) demo_flag=true ;; *) demo_flag=false ;; esac

    cat > "${PROVISA_HOME}/config.yaml" <<YAML
# Provisa configuration
project_dir: "${SCRIPT_DIR}"
ui_port: ${ui_port}
api_port: ${api_port}
auto_open_browser: ${auto_open}
# Deployment (REQ-972..979): parity with the macOS SwiftUI wizard.
engine: ${DEPLOY_ENGINE}
engine_url: "${ENGINE_URL}"
materialize_url: "${MATERIALIZE_URL}"
trino_host: "${TRINO_HOST}"
trino_port: "${TRINO_PORT}"
obs_mode: ${OBS_MODE}
otlp_endpoint: "${OTLP_ENDPOINT}"
demo: ${demo_flag}
demo_mode: ${DEMO_MODE}
YAML
    ok "Created config at ${PROVISA_HOME}/config.yaml"
}

# ── Download/clone if not present ────────────────────────────────────────────
ensure_project_source() {
    # If install.sh is running from a git repo, source is already present
    if [ -f "${SCRIPT_DIR}/docker-compose.core.yml" ]; then
        ok "Project source found at ${SCRIPT_DIR}"
        return 0
    fi

    # Try to clone
    info "Project source not found locally. Cloning..."
    if command -v git &>/dev/null; then
        git clone https://github.com/kenstott/provisa.git "${SCRIPT_DIR}/provisa-source" 2>&1
        SCRIPT_DIR="${SCRIPT_DIR}/provisa-source"
        ok "Cloned Provisa source to ${SCRIPT_DIR}"
    else
        err "docker-compose.core.yml not found and git is not available."
        err "Clone the Provisa repo first, then run install.sh from inside it."
        exit 1
    fi
}

# ── Main ────────────────────────────────────────────────────────────────────
main() {
    printf "\n${BOLD}Provisa Installer${NC}\n"
    printf "═══════════════════════════════════════════\n\n"

    # 1. Detect container runtime
    info "Detecting container runtime..."
    RUNTIME="$(detect_runtime)"
    if [ -z "$RUNTIME" ]; then
        err "No container runtime found."
        err "Install one of: Docker Desktop, OrbStack, or Colima."
        exit 1
    fi
    ok "Found runtime: ${RUNTIME}"

    # 2. Check Docker version
    info "Checking Docker version..."
    if ! check_docker_version; then
        err "Please upgrade Docker to version ${MIN_DOCKER_VERSION} or later."
        exit 1
    fi

    # 3. Check Docker Compose v2
    info "Checking Docker Compose..."
    if ! check_compose; then
        err "Docker Compose not found. Install Docker Compose v2+."
        exit 1
    fi

    # 4. Ensure project source is available
    info "Checking project source..."
    ensure_project_source

    # 5. Create ~/.provisa/
    info "Creating ${PROVISA_HOME}..."
    mkdir -p "${PROVISA_HOME}/data"
    mkdir -p "${PROVISA_HOME}/.logs"

    # 6. Generate config (interactive or defaults)
    info "Configuring Provisa..."
    generate_config

    # 7. Install CLI wrapper
    info "Installing CLI wrapper..."
    # In airgap mode install to ~/.provisa/bin to avoid needing sudo
    if [ "$AIRGAP" = true ]; then
        CLI_INSTALL_DIR="${PROVISA_HOME}/bin"
        mkdir -p "${CLI_INSTALL_DIR}"
    fi

    if [ ! -d "${CLI_INSTALL_DIR}" ]; then
        err "${CLI_INSTALL_DIR} does not exist."
        exit 1
    fi

    local cli_src="${SCRIPT_DIR}/scripts/provisa"
    if [ ! -f "$cli_src" ]; then
        err "CLI script not found at ${cli_src}"
        exit 1
    fi

    if [ -w "${CLI_INSTALL_DIR}" ]; then
        cp "$cli_src" "${CLI_INSTALL_DIR}/${CLI_NAME}"
        chmod +x "${CLI_INSTALL_DIR}/${CLI_NAME}"
    else
        info "Requires sudo to install to ${CLI_INSTALL_DIR}"
        sudo cp "$cli_src" "${CLI_INSTALL_DIR}/${CLI_NAME}"
        sudo chmod +x "${CLI_INSTALL_DIR}/${CLI_NAME}"
    fi
    ok "Installed ${CLI_INSTALL_DIR}/${CLI_NAME}"

    # 8. Verify installation by starting services (skipped in airgap mode)
    if [ "$AIRGAP" = false ]; then
        printf "\n${BOLD}Verifying installation...${NC}\n"
        info "Starting Provisa services..."
        "${CLI_INSTALL_DIR}/${CLI_NAME}" start
    fi

    # 9. Done
    printf "\n${GREEN}${BOLD}Installation complete.${NC}\n\n"
    printf "  ${BOLD}provisa start${NC}       Start all services\n"
    printf "  ${BOLD}provisa stop${NC}        Stop all services\n"
    printf "  ${BOLD}provisa restart${NC}     Restart all services\n"
    printf "  ${BOLD}provisa status${NC}      Show service status\n"
    printf "  ${BOLD}provisa open${NC}        Open Provisa in browser\n"
    printf "  ${BOLD}provisa logs${NC}        Tail service logs\n"
    printf "  ${BOLD}provisa upgrade${NC}     Update to latest version\n"
    printf "  ${BOLD}provisa uninstall${NC}   Remove Provisa CLI and data\n\n"
}

main "$@"
