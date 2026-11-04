#!/bin/bash
# Runs once when the dev container is created (REQ-2205): the locked Python dependencies with the
# extras CI's unit job installs, provisa._encoders compiled for this checkout, and provisa-ui's
# packages. Afterwards `pytest tests/unit -n auto` and `npm run test:unit` (in provisa-ui) run.
set -euo pipefail
cd "$(dirname "$0")/.."

# The named volumes compose mounts over these directories start out owned by root.
sudo chown vscode:vscode provisa-ui/node_modules rust/encoders/target

# The environment lives at $UV_PROJECT_ENVIRONMENT, outside the checkout, so a host's own .venv in
# the same checkout is left alone. The editable build of provisa compiles provisa._encoders
# (setuptools-rust) to provisa/_encoders.cpython-312-<arch>-linux-gnu.so. Python on Linux loads
# that name first, and a host's own build of the module keeps its name and is not overwritten.
uv sync --frozen --extra dev
uv pip install --python "$UV_PROJECT_ENVIRONMENT/bin/python" -e "provisa-client/[dev]"
"$UV_PROJECT_ENVIRONMENT/bin/python" -c 'import provisa._encoders as e; print("provisa._encoders:", e.__file__)'

(cd provisa-ui && npm ci)
