#!/usr/bin/env bash
# Provisa Installer — Phase AF1
# Detects container runtime, creates ~/.provisa/, installs CLI wrapper.
set -euo pipefail

PROVISA_HOME="${HOME}/.provisa"
CLI_INSTALL_DIR="/usr/local/bin"
CLI_NAME="provisa"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Colors ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'
BOLD='\033[1m'; NC='\033[0m'

info()  { printf "${CYAN}[provisa]${NC} %s\n" "$*"; }
ok()    { printf "${GREEN}[provisa]${NC} %s\n" "$*"; }
warn()  { printf "${YELLOW}[provisa]${NC} %s\n" "$*"; }
err()   { printf "${RED}[provisa]${NC} %s\n" "$*" >&2; }

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

check_compose() {
    if docker compose version &>/dev/null 2>&1; then
        return 0
    elif command -v docker-compose &>/dev/null; then
        return 0
    else
        return 1
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

    # 2. Check docker compose
    info "Checking Docker Compose..."
    if ! check_compose; then
        err "Docker Compose not found. Install Docker Compose v2."
        exit 1
    fi
    ok "Docker Compose available"

    # 3. Create ~/.provisa/
    info "Creating ${PROVISA_HOME}..."
    mkdir -p "${PROVISA_HOME}/data"
    mkdir -p "${PROVISA_HOME}/logs"

    # 4. Write default config
    if [ ! -f "${PROVISA_HOME}/config.yaml" ]; then
        cat > "${PROVISA_HOME}/config.yaml" <<'YAML'
# Provisa configuration
project_dir: ""
ui_port: 3000
api_port: 8000
auto_open_browser: true
YAML
        ok "Created default config at ${PROVISA_HOME}/config.yaml"
    else
        warn "Config already exists, skipping"
    fi

    # 5. Store project directory in config
    sed -i.bak "s|^project_dir:.*|project_dir: \"${SCRIPT_DIR}\"|" "${PROVISA_HOME}/config.yaml"
    rm -f "${PROVISA_HOME}/config.yaml.bak"

    # 6. Install CLI wrapper
    info "Installing CLI wrapper..."
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

    # 7. Done
    printf "\n${GREEN}${BOLD}Installation complete.${NC}\n\n"
    printf "  ${BOLD}provisa start${NC}      Start all services\n"
    printf "  ${BOLD}provisa stop${NC}       Stop all services\n"
    printf "  ${BOLD}provisa status${NC}     Show service status\n"
    printf "  ${BOLD}provisa open${NC}       Open Provisa in browser\n"
    printf "  ${BOLD}provisa uninstall${NC}  Remove Provisa CLI and data\n\n"
}

main "$@"
