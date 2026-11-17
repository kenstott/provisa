#!/usr/bin/env bash
# One-time dev setup: virtual env, dependencies, git hooks.
set -e

# From uv.lock: the dev extra carries the vector extra, whose torch the lock takes from PyTorch's
# CPU-only index on Linux, never PyPI's CUDA build (REQ-419).
UV_PROJECT_ENVIRONMENT=.venv uv sync --locked --extra dev

git config core.hooksPath .githooks
echo "Git hooks configured → .githooks/"
