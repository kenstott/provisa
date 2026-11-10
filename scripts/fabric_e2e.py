#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 2b8e5d71-9c34-4a6f-8e12-5f0d3c7a9b48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The run's own Fabric capacity, for a lane's steps (maintainer 2026-11-10, "Paid cloud
resources"): ``up`` creates it, tagged with the run, and assigns the lane's workspace to it;
``down`` unassigns the workspace, deletes the capacity and reads it back gone, failing if it is
still there. tests/integration/fabric_capacity.py holds both.

    python3 scripts/fabric_e2e.py up
    python3 scripts/fabric_e2e.py down

Needs FABRIC_RESOURCE_GROUP, FABRIC_CAPACITY_NAME, FABRIC_WORKSPACE_ID and an Azure credential
(the CI principal's AZURE_* or ``az login``).
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

from tests.integration.fabric_capacity import create_run_capacity, delete_run_capacity  # noqa: E402

if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ("up", "down"):
        print("usage: fabric_e2e.py <up|down>", file=sys.stderr)
        sys.exit(2)
    (create_run_capacity if sys.argv[1] == "up" else delete_run_capacity)()
