#!/usr/bin/env bash
# E2E test for Provisa installer (Phase AF1)
# Tests install.sh creates expected files and provisa --help works.
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
assert_executable() {
    if [ -x "$1" ]; then pass "$2"; else fail "$2 (not executable: $1)"; fi
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

# Patch install.sh to use our fake bin dir (avoid needing sudo)
PATCHED_INSTALLER="$(mktemp)"
sed "s|CLI_INSTALL_DIR=\"/usr/local/bin\"|CLI_INSTALL_DIR=\"${FAKE_BIN}\"|" \
    "$PROJECT_ROOT/install.sh" > "$PATCHED_INSTALLER"
chmod +x "$PATCHED_INSTALLER"

# Run installer (docker detection may fail, but filesystem operations should work)
INSTALL_OUTPUT="$("$PATCHED_INSTALLER" 2>&1 || true)"

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

# ── Test 4: CLI wrapper installed ───────────────────────────────────────────
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

# ── Test 5: provisa --help output ──────────────────────────────────────────
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
assert_contains "$HELP_OUTPUT" "status" "help mentions status"
assert_contains "$HELP_OUTPUT" "open" "help mentions open"
assert_contains "$HELP_OUTPUT" "uninstall" "help mentions uninstall"
assert_contains "$HELP_OUTPUT" "Provisa" "help mentions Provisa brand"

# ── Test 6: Unknown command exits non-zero ──────────────────────────────────
printf "\nTest group: Error handling\n"
if "$PROJECT_ROOT/scripts/provisa" bogus-command &>/dev/null; then
    fail "unknown command should exit non-zero"
else
    pass "unknown command exits non-zero"
fi

# ── Summary ─────────────────────────────────────────────────────────────────
printf "\n═══════════════════════════════════════════\n"
printf "Results: %d passed, %d failed, %d total\n" "$TESTS_PASSED" "$TESTS_FAILED" "$TESTS_RUN"

if [ "$TESTS_FAILED" -gt 0 ]; then
    exit 1
fi
printf "All tests passed.\n"
