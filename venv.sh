#!/usr/bin/env bash
# Create or update the Python virtualenv (.venv) for Provisa.
#
# Syncs provisa (editable) from uv.lock with the 'dev' and 'desktop' extras, plus the editable
# provisa-client. Through the lock, the vector extra's torch comes from PyTorch's CPU-only index on
# Linux ([tool.uv.sources]), never PyPI's CUDA build (REQ-419). Idempotent: run it anytime to
# (re)sync dependencies after a pull or on a new machine/disk. Packages the lock does not name stay
# installed (--inexact).
#
# Usage:
#   ./venv.sh                       # provisa[dev,desktop] + provisa-client
#   ./venv.sh govdata firebase      # also sync those optional-dependency groups
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
EXTRA_ARGS=(--extra dev --extra desktop)
for g in "$@"; do EXTRA_ARGS+=(--extra "$g"); done

if ! command -v uv >/dev/null 2>&1; then
  echo "ERROR: uv not found. Install uv (https://docs.astral.sh/uv/getting-started/installation/ or" >&2
  echo "       'brew install uv') and run this again." >&2
  exit 1
fi
if ! command -v "$PYTHON" >/dev/null 2>&1; then
  echo "ERROR: '$PYTHON' not found. Install Python 3.12+ or set PYTHON=..." >&2
  exit 1
fi

echo "Syncing $VENV from uv.lock (${EXTRA_ARGS[*]})…"
UV_PROJECT_ENVIRONMENT="$VENV" uv sync --locked --inexact --python "$PYTHON" "${EXTRA_ARGS[@]}"

echo "Installing provisa-client (editable)…"
uv pip install --python "$VENV/bin/python" -e provisa-client

# Match setup.sh: route git hooks through the repo's .githooks/.
git config core.hooksPath .githooks 2>/dev/null && echo "Git hooks → .githooks/" || true

echo "Done. $("$VENV"/bin/python --version) at $VENV"
