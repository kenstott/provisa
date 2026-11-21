#!/bin/bash
# The dev container's slow setup (REQ-2205): the locked Python dependencies with the extras CI's
# unit job installs, provisa._encoders compiled for this checkout, and provisa-ui's packages. It is
# the updateContentCommand, so it runs when the container is created and again when the checkout's
# content changes, and a Codespaces prebuild runs it ahead of time. Afterwards
# `pytest tests/unit -n auto` and `npm run test:unit` (in provisa-ui) run.
set -euo pipefail
cd "$(dirname "$0")/.."

# The named volumes devcontainer.json mounts over these directories (so a macOS or Windows host's
# own node_modules, .venv and cargo output in the same checkout are neither used nor overwritten)
# start out owned by root.
sudo chown vscode:vscode .venv provisa-ui/node_modules rust/encoders/target

# The environment is the checkout's .venv (what start-ui-install.sh and rust/build-local.sh use),
# on a named volume, so a host's own .venv in the same checkout is left alone. The editable build of provisa compiles provisa._encoders
# (setuptools-rust) to provisa/_encoders.cpython-312-<arch>-linux-gnu.so. Python on Linux loads
# that name first, and a host's own build of the module keeps its name and is not overwritten.
uv sync --frozen --extra dev
uv pip install --python "$UV_PROJECT_ENVIRONMENT/bin/python" -e "provisa-client/[dev]"
"$UV_PROJECT_ENVIRONMENT/bin/python" -c 'import provisa._encoders as e; print("provisa._encoders:", e.__file__)'

(cd provisa-ui && npm ci)
