# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Loaded by every Python process serve_three_device.sh starts the server with (this directory is
on their PYTHONPATH): each records its collections and stalls in PROVISA_BENCH_STALL_DIR
(stall_probe.py)."""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import stall_probe  # noqa: E402

stall_probe.install_to_dir(Path(os.environ["PROVISA_BENCH_STALL_DIR"]))
