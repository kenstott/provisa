#!/usr/bin/env bash
# Create or update the Python virtualenv (.venv) for Provisa.
#
# Installs the provisa package (editable) with the 'dev' extras, plus the two
# editable sub-projects the venv depends on (provisa-client, vendored buenavista).
# Idempotent — run it anytime to (re)sync dependencies after a pull or on a new
# machine/disk.
#
# Usage:
#   ./venv.sh                       # provisa[dev] + provisa-client + buenavista
#   ./venv.sh govdata firebase      # also install those optional-dependency groups
#   PYTHON=python3.12 ./venv.sh     # override the interpreter (default: python3.12)
#
# Optional groups available (see pyproject.toml): mysql, sqlserver, oracle,
# all-drivers, govdata, firebase.

set -euo pipefail
cd "$(dirname "$0")"

PYTHON="${PYTHON:-python3.12}"
VENV="${VENV:-.venv}"

# Always include dev tooling; append any optional groups passed as args.
# 'desktop' pulls in mcp-proxy so the dev backend's MCP Claude-Desktop bridge fallback resolves
# (start-ui-install points PROVISA_MCP_BRIDGE_COMMAND at this venv's python).
EXTRAS="dev,desktop"
for g in "$@"; do EXTRAS="$EXTRAS,$g"; done

if ! command -v "$PYTHON" >/dev/null 2>&1; then
  echo "ERROR: '$PYTHON' not found. Install Python 3.12+ or set PYTHON=..." >&2
  exit 1
fi

if [ ! -d "$VENV" ]; then
  echo "Creating $VENV with $($PYTHON --version)…"
  "$PYTHON" -m venv "$VENV"
fi

PIP="$VENV/bin/pip"
echo "Upgrading pip…"
"$PIP" install -U pip

# Install the vendored editables FIRST. provisa depends on buenavista>=0.5.0.post1,
# which only exists as the in-repo fork (PyPI tops out at 0.5.0); installing it
# editable up front satisfies that constraint so provisa won't try (and fail) to
# fetch it from PyPI.
echo "Installing editable sub-projects (buenavista fork, provisa-client)…"
"$PIP" install -e vendor/buenavista -e provisa-client

echo "Installing provisa[$EXTRAS] (editable)…"
"$PIP" install -e ".[$EXTRAS]"

# Match setup.sh: route git hooks through the repo's .githooks/.
git config core.hooksPath .githooks 2>/dev/null && echo "Git hooks → .githooks/" || true

echo "Done. $("$VENV"/bin/python --version) at $VENV"
