#!/usr/bin/env bash
# One-time dev setup: virtual env, dependencies, git hooks.
set -e

python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

git config core.hooksPath .githooks
echo "Git hooks configured → .githooks/"
