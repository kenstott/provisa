#!/usr/bin/env bash
# Developer setup: checks the toolchain (uv, Rust, Node, Go, Docker) against the versions the dev
# container pins, installs what is missing or too old after a yes/no per tool (--yes installs
# without asking), then syncs the Python dependencies, installs provisa-ui's packages and sets
# the git hooks. Safe to rerun.
set -euo pipefail
cd "$(dirname "$0")"

# Where the installers put tools, so a tool installed in this run is found by the steps below.
for d in "$HOME/.cargo/bin" "$HOME/.local/bin" "$HOME/.local/go/bin" "$HOME/.local/node/bin"; do
    PATH="$d:$PATH"
done
export PATH

if ! command -v python3 >/dev/null 2>&1; then
    echo "error: python3 is needed to run scripts/dev_prereqs.py" >&2
    exit 1
fi
python3 scripts/dev_prereqs.py "$@"

# From uv.lock: the dev extra carries the vector extra, whose torch the lock takes from PyTorch's
# CPU-only index on Linux, never PyPI's CUDA build (REQ-419). The editable build compiles
# provisa._encoders from rust/.
UV_PROJECT_ENVIRONMENT=.venv uv sync --locked --extra dev

(cd provisa-ui && npm ci)

git config core.hooksPath .githooks
echo "Git hooks configured → .githooks/"
