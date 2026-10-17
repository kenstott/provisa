#!/usr/bin/env bash
# REQ-2071: a checkout's Data Science area runs on provisa-notebook, a dependency of provisa that
# lives beside it in this repository. The launchers call this before they start anything:
#   1. provisa-notebook installed (editable) into the checkout's .venv, as `uv sync` would from the
#      lock's path source -- `pip install -e .` alone looks for it on PyPI;
#   2. its assets built (`python -m provisa_notebook.build`) into the directory ui_server serves
#      from the package and vite serves in development, and built again whenever what they are
#      built from has changed (the manifest records a fingerprint of it), so a change reaches the
#      next start.
# Usage: scripts/ensure-notebook.sh [repo-dir]   (default: this script's repository)
set -euo pipefail
REPO="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
PY="$REPO/.venv/bin/python"
if ! "$PY" -c "import provisa_notebook" 2>/dev/null; then
  echo "Installing provisa-notebook into .venv..."
  "$PY" -m pip install --quiet --no-deps -e "$REPO/provisa-notebook"
fi
if ! (cd "$REPO/provisa-notebook" && "$PY" -c "import sys; from provisa_notebook.build import is_current; sys.exit(0 if is_current() else 1)"); then
  echo "Building the Data Science area's Python (Pyodide, its packages and the notebook)..."
  (cd "$REPO/provisa-notebook" && "$PY" -m provisa_notebook.build > /dev/null)
fi
