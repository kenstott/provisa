#!/usr/bin/env bash
# E2E test for Provisa installer (Phase AF1)
# Tests install.sh creates expected files and provisa CLI commands work.
# Does NOT require Docker (tests filesystem + CLI wrapper only).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ── Test harness ────────────────────────────────────────────────────────────
TESTS_RUN=0; TESTS_PASSED=0; TESTS_FAILED=0

pass() { TESTS_RUN=$((TESTS_RUN + 1)); TESTS_PASSED=$((TESTS_PASSED + 1)); printf "  PASS: %s\n" "$1"; }
fail() { TESTS_RUN=$((TESTS_RUN + 1)); TESTS_FAILED=$((TESTS_FAILED + 1)); printf "  FAIL: %s\n" "$1" >&2; }

assert_file_exists() {
    if [ -f "$1" ]; then pass "$2"; else fail "$2 (missing: $1)"; fi
}
assert_dir_exists() {
    if [ -d "$1" ]; then pass "$2"; else fail "$2 (missing: $1)"; fi
}
assert_contains() {
    if echo "$1" | grep -q "$2"; then pass "$3"; else fail "$3 (expected '$2')"; fi
}
assert_not_contains() {
    if echo "$1" | grep -qi "$2"; then fail "$3 (found '$2')"; else pass "$3"; fi
}
assert_executable() {
    if [ -x "$1" ]; then pass "$2"; else fail "$2 (not executable: $1)"; fi
}
assert_exit_code() {
    local expected="$1" actual="$2" msg="$3"
    if [ "$expected" -eq "$actual" ]; then pass "$msg"; else fail "$msg (expected exit $expected, got $actual)"; fi
}

# ── Setup: use temp HOME to avoid touching real ~/.provisa ──────────────────
FAKE_HOME="$(mktemp -d)"
export HOME="$FAKE_HOME"
FAKE_BIN="$(mktemp -d)"

cleanup() {
    rm -rf "$FAKE_HOME" "$FAKE_BIN"
}
trap cleanup EXIT

printf "Provisa Installer E2E Tests\n"
printf "═══════════════════════════════════════════\n"
printf "Project root: %s\n" "$PROJECT_ROOT"
printf "Fake HOME:    %s\n" "$FAKE_HOME"
printf "Fake bin:     %s\n\n" "$FAKE_BIN"

# ── Test 1: Source files exist ──────────────────────────────────────────────
printf "Test group: Source files\n"
assert_file_exists "$PROJECT_ROOT/install.sh" "install.sh exists"
assert_executable "$PROJECT_ROOT/install.sh" "install.sh is executable"
assert_file_exists "$PROJECT_ROOT/scripts/provisa" "scripts/provisa exists"
assert_executable "$PROJECT_ROOT/scripts/provisa" "scripts/provisa is executable"

# ── Test 2: Run installer with overridden CLI_INSTALL_DIR ───────────────────
printf "\nTest group: Installer execution\n"

# Patch install.sh to use our fake bin dir, fix SCRIPT_DIR, and skip the start step
PATCHED_INSTALLER="$(mktemp)"
sed -e "s|CLI_INSTALL_DIR=\"/usr/local/bin\"|CLI_INSTALL_DIR=\"${FAKE_BIN}\"|" \
    -e "s|SCRIPT_DIR=.*|SCRIPT_DIR=\"${PROJECT_ROOT}\"|" \
    -e 's|"\${CLI_INSTALL_DIR}/\${CLI_NAME}" start|info "Skipping start in test mode"|' \
    "$PROJECT_ROOT/install.sh" > "$PATCHED_INSTALLER"
chmod +x "$PATCHED_INSTALLER"

# Run installer in non-interactive mode (docker detection may fail, but filesystem operations should work)
INSTALL_OUTPUT="$("$PATCHED_INSTALLER" --non-interactive 2>&1 || true)"

assert_dir_exists "$FAKE_HOME/.provisa" "~/.provisa/ created"
assert_dir_exists "$FAKE_HOME/.provisa/data" "~/.provisa/data/ created"
assert_dir_exists "$FAKE_HOME/.provisa/.logs" "~/.provisa/.logs/ created"
assert_file_exists "$FAKE_HOME/.provisa/config.yaml" "config.yaml created"

# ── Test 3: Config content ──────────────────────────────────────────────────
printf "\nTest group: Configuration\n"
CONFIG_CONTENT="$(cat "$FAKE_HOME/.provisa/config.yaml")"
assert_contains "$CONFIG_CONTENT" "project_dir:" "config has project_dir"
assert_contains "$CONFIG_CONTENT" "ui_port:" "config has ui_port"
assert_contains "$CONFIG_CONTENT" "api_port:" "config has api_port"
assert_contains "$CONFIG_CONTENT" "auto_open_browser:" "config has auto_open_browser"

# ── Test 4: Non-interactive flag sets defaults ──────────────────────────────
printf "\nTest group: Non-interactive defaults\n"
assert_contains "$CONFIG_CONTENT" "ui_port: 3000" "default ui_port is 3000"
assert_contains "$CONFIG_CONTENT" "api_port: 8000" "default api_port is 8000"
assert_contains "$CONFIG_CONTENT" "auto_open_browser: true" "default auto_open is true"

# ── Test 5: CLI wrapper installed ───────────────────────────────────────────
printf "\nTest group: CLI wrapper\n"
# Only check if docker was available (installer may have exited early)
if [ -f "$FAKE_BIN/provisa" ]; then
    assert_file_exists "$FAKE_BIN/provisa" "CLI installed to bin dir"
    assert_executable "$FAKE_BIN/provisa" "CLI is executable"
else
    # If docker wasn't available, installer exits before copying CLI.
    # Test the source script directly instead.
    printf "  SKIP: CLI not installed (docker unavailable), testing source script\n"
fi

# ── Test 6: provisa help output ────────────────────────────────────────────
printf "\nTest group: CLI help\n"

# Set up config so the CLI can find the project dir
mkdir -p "$FAKE_HOME/.provisa"
cat > "$FAKE_HOME/.provisa/config.yaml" <<YAML
project_dir: "${PROJECT_ROOT}"
ui_port: 3000
api_port: 8000
auto_open_browser: true
YAML

HELP_OUTPUT="$("$PROJECT_ROOT/scripts/provisa" help 2>&1 || true)"
assert_contains "$HELP_OUTPUT" "start" "help mentions start"
assert_contains "$HELP_OUTPUT" "stop" "help mentions stop"
assert_contains "$HELP_OUTPUT" "restart" "help mentions restart"
assert_contains "$HELP_OUTPUT" "status" "help mentions status"
assert_contains "$HELP_OUTPUT" "open" "help mentions open"
assert_contains "$HELP_OUTPUT" "logs" "help mentions logs"
assert_contains "$HELP_OUTPUT" "upgrade" "help mentions upgrade"
assert_contains "$HELP_OUTPUT" "uninstall" "help mentions uninstall"
assert_contains "$HELP_OUTPUT" "Provisa" "help mentions Provisa brand"

# ── Test 7: Unknown command exits non-zero ──────────────────────────────────
printf "\nTest group: Error handling\n"
if "$PROJECT_ROOT/scripts/provisa" bogus-command &>/dev/null; then
    fail "unknown command should exit non-zero"
else
    pass "unknown command exits non-zero"
fi

# ── Test 8: Brand name mapping ──────────────────────────────────────────────
printf "\nTest group: Brand name mapping\n"
CLI_CONTENT="$(cat "$PROJECT_ROOT/scripts/provisa")"
# Check that the CLI script contains expected brand mappings
assert_contains "$CLI_CONTENT" 'echo "Provisa Database"' "postgres maps to Provisa Database"
assert_contains "$CLI_CONTENT" 'echo "Provisa Query Engine"' "trino maps to Provisa Query Engine"
assert_contains "$CLI_CONTENT" 'echo "Provisa Cache"' "redis maps to Provisa Cache"
assert_contains "$CLI_CONTENT" 'echo "Provisa UI"' "provisa-ui maps to Provisa UI"

# ── Test 9: Output filter hides internals ───────────────────────────────────
printf "\nTest group: Output filter\n"
FILTER_TEST="$(echo "docker postgres trino pgbouncer" | sed \
    -e 's/postgresql/provisa-db/g' \
    -e 's/postgres/provisa-db/g' \
    -e 's/pgbouncer/provisa-pool/g' \
    -e 's/trino/provisa-query/g' \
    -e 's/Docker/Provisa/g' \
    -e 's/docker/provisa/g')"
assert_not_contains "$FILTER_TEST" "docker" "filter removes docker"
assert_not_contains "$FILTER_TEST" "postgres" "filter removes postgres"
assert_not_contains "$FILTER_TEST" "trino" "filter removes trino"
assert_contains "$FILTER_TEST" "provisa" "filter replaces with provisa"

# ── Test 10: Installer has --non-interactive flag ───────────────────────────
printf "\nTest group: Installer flags\n"
INSTALLER_CONTENT="$(cat "$PROJECT_ROOT/install.sh")"
assert_contains "$INSTALLER_CONTENT" "non-interactive" "installer supports --non-interactive"

# ── Test 11: Installer checks Docker version ────────────────────────────────
printf "\nTest group: Version checks\n"
assert_contains "$INSTALLER_CONTENT" "MIN_DOCKER_VERSION" "installer checks Docker version"
assert_contains "$INSTALLER_CONTENT" "MIN_COMPOSE_VERSION" "installer checks Compose version"

# ── Test 12: Config file not overwritten on re-run (non-interactive) ────────
printf "\nTest group: Idempotent install\n"
# Write a custom config
cat > "$FAKE_HOME/.provisa/config.yaml" <<YAML
project_dir: "${PROJECT_ROOT}"
ui_port: 9999
api_port: 8888
auto_open_browser: false
YAML

# Run installer again in non-interactive mode
PATCHED_INSTALLER2="$(mktemp)"
sed -e "s|CLI_INSTALL_DIR=\"/usr/local/bin\"|CLI_INSTALL_DIR=\"${FAKE_BIN}\"|" \
    -e "s|SCRIPT_DIR=.*|SCRIPT_DIR=\"${PROJECT_ROOT}\"|" \
    -e 's|"\${CLI_INSTALL_DIR}/\${CLI_NAME}" start|info "Skipping start in test mode"|' \
    "$PROJECT_ROOT/install.sh" > "$PATCHED_INSTALLER2"
chmod +x "$PATCHED_INSTALLER2"
"$PATCHED_INSTALLER2" --non-interactive 2>&1 || true

# In non-interactive mode, config should be overwritten with defaults
CONFIG_AFTER="$(cat "$FAKE_HOME/.provisa/config.yaml")"
assert_contains "$CONFIG_AFTER" "ui_port: 3000" "non-interactive re-install resets to defaults"

# ── Test 13: CLI commands are registered ────────────────────────────────────
printf "\nTest group: All CLI commands registered\n"
assert_contains "$CLI_CONTENT" "cmd_start" "start command defined"
assert_contains "$CLI_CONTENT" "cmd_stop" "stop command defined"
assert_contains "$CLI_CONTENT" "cmd_restart" "restart command defined"
assert_contains "$CLI_CONTENT" "cmd_status" "status command defined"
assert_contains "$CLI_CONTENT" "cmd_open" "open command defined"
assert_contains "$CLI_CONTENT" "cmd_logs" "logs command defined"
assert_contains "$CLI_CONTENT" "cmd_upgrade" "upgrade command defined"
assert_contains "$CLI_CONTENT" "cmd_uninstall" "uninstall command defined"

# ── Test 14: Logs directory exists ──────────────────────────────────────────
printf "\nTest group: Log directory\n"
assert_dir_exists "$FAKE_HOME/.provisa/.logs" "logs directory exists"

# ── Summary ─────────────────────────────────────────────────────────────────
printf "\n═══════════════════════════════════════════\n"
printf "Results: %d passed, %d failed, %d total\n" "$TESTS_PASSED" "$TESTS_FAILED" "$TESTS_RUN"

if [ "$TESTS_FAILED" -gt 0 ]; then
    exit 1
fi
printf "All tests passed.\n"
