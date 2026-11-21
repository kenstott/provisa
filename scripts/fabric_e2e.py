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

"""The run's own Fabric, for a lane's steps (maintainer 2026-11-10, "Paid cloud resources";
maintainer 2026-11-21, per-run workspace): ``up`` creates the capacity, tagged with the run, a
workspace on it and a warehouse in that, and appends FABRIC_SQL_SERVER, FABRIC_DATABASE and
FABRIC_WORKSPACE_ID lines to the env file it is given (``$GITHUB_ENV`` in CI, so the lane's later
steps read them); ``down`` deletes the workspace (its items with it), then the capacity, and reads
back that each is gone, failing if one is still there. tests/integration/fabric_capacity.py holds
both.

    python3 scripts/fabric_e2e.py up "$GITHUB_ENV"
    python3 scripts/fabric_e2e.py down

Needs FABRIC_RESOURCE_GROUP, FABRIC_CAPACITY_NAME and an Azure credential (the CI principal's
AZURE_* or ``az login``).
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

from tests.integration.fabric_capacity import create_run_fabric, delete_run_fabric  # noqa: E402

_USAGE = "usage: fabric_e2e.py up <env file> | fabric_e2e.py down"

if __name__ == "__main__":
    args = sys.argv[1:]
    if args[:1] == ["up"] and len(args) == 2:
        made = create_run_fabric()
        with open(args[1], "a", encoding="utf-8") as env_file:
            env_file.write("".join(f"{line}\n" for line in made.env_lines()))
        print(f"Fabric for this run: {', '.join(made.env_lines())}", flush=True)
    elif args == ["down"]:
        delete_run_fabric()
    else:
        print(_USAGE, file=sys.stderr)
        sys.exit(2)
