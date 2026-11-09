#!/usr/bin/env bash
# REQ-2223: a start refuses a checkout whose virtual environment is out of step with uv.lock, and
# names the command that fixes it. It only compares (`uv sync --dry-run`): nothing is installed,
# removed or written, and a start never changes the environment without being asked.
#   --inexact: packages the lock's default set does not name stay out of the comparison, so optional
#     extras (dev, the drivers) and a demo's requirements are not reported as removals.
#   --no-install-project: provisa itself is rebuilt by uv whenever a [tool.uv] cache key changes,
#     which every edit under rust/ does. That is not a difference from the lock, and
#     rust/ensure-local-build.sh already looks after the build.
# uv is required. Without it the comparison cannot be made, so the start stops and says so.
# Usage: scripts/ensure-venv-in-step.sh <repo-dir>
set -euo pipefail
REPO="$1"

if ! command -v uv >/dev/null 2>&1; then
  echo "ERROR: uv not found. A start compares .venv with uv.lock using uv, and cannot do so" >&2
  echo "       without it. Install uv (https://docs.astral.sh/uv/getting-started/installation/ or" >&2
  echo "       'brew install uv') and start again." >&2
  exit 1
fi

_rc=0
_out="$(cd "$REPO" && UV_PROJECT_ENVIRONMENT=.venv uv sync --locked --inexact --no-install-project --dry-run 2>&1)" || _rc=$?
# uv exits 0 when it would change something, so the verdict is its own sentence.
if [ "$_rc" -eq 0 ]; then
  case "$_out" in *"Would make no changes"*) exit 0 ;; esac
fi

{
  echo "ERROR: $REPO/.venv is out of step with uv.lock, so nothing was started and nothing was changed."
  echo "$_out" | sed 's/^/       /'
  echo "       To bring it in step, run:"
  echo
  printf '    cd %q && UV_PROJECT_ENVIRONMENT=.venv uv sync --locked --inexact\n' "$REPO"
  echo
} >&2
exit 1
