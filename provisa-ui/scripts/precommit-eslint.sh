#!/usr/bin/env bash
# Pre-commit ESLint for provisa-ui — lints ONLY the staged .ts/.tsx files passed
# by the pre-commit framework (fast: avoids the ~full-tree lint cost). Blocks the
# commit on any error. Warnings are treated as errors via --max-warnings 0 so the
# zero-warning state is enforced — every intentional deviation must carry a
# justified eslint-disable comment (see .claude/skills/react-graphql).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"   # provisa-ui/

# pass-through staged files are repo-root-relative; strip the provisa-ui/ prefix.
files=()
for f in "$@"; do
  files+=("${f#provisa-ui/}")
done
[ ${#files[@]} -eq 0 ] && exit 0

# Load nvm and the repository's pinned Node version (.nvmrc at the repo root).
export NVM_DIR="${NVM_DIR:-$HOME/.nvm}"
# shellcheck disable=SC1091
[ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh"
if command -v nvm >/dev/null 2>&1; then
  nvm use "$(cat "$SCRIPT_DIR/../.nvmrc")" >/dev/null \
    || { echo "Node $(cat "$SCRIPT_DIR/../.nvmrc") (.nvmrc) is not installed: nvm install" >&2; exit 1; }
fi

cd "$SCRIPT_DIR"
npx eslint --max-warnings 0 "${files[@]}"
