#!/usr/bin/env bash
# REQ-2071: build the provisa-notebook wheel (Pyodide, its packages and the provisa-client wheel)
# into OUT-DIR with PYTHON. An installer's wheelhouse needs it beside provisa's own wheel: provisa
# pins it, and PyPI does not have this version until the release publishes it.
# Usage: scripts/build-notebook-wheel.sh PYTHON OUT-DIR
set -euo pipefail
PYTHON="$1"
OUT="$2"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
"$PYTHON" -m pip install --quiet build
(cd "$REPO/provisa-notebook" && "$PYTHON" -m provisa_notebook.build > /dev/null)
"$PYTHON" -m build --wheel "$REPO/provisa-notebook" --outdir "$OUT"
