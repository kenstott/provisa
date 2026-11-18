#!/usr/bin/env bash
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
#
# The vector extra (sentence-transformers) in the repo-root .venv, for a lane whose server embeds
# with a local model: torch from PyTorch's CPU-only wheel index first, so the extra pulls no GPU
# build. The extra's requirements are the ones the sonnets demo's demo.yaml names (REQ-2016).
# Run from the repo root by a lane's prepare script.
set -euo pipefail

uv pip install --index-url https://download.pytorch.org/whl/cpu torch
# shellcheck disable=SC2046
uv pip install $(uv run python scripts/demo_extras.py demo/named/sonnets)
uv run python -c "import sentence_transformers, torch; print('vector extra, torch', torch.__version__)"
