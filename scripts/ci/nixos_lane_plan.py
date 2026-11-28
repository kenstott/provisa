# Copyright (c) 2026 Kenneth Stott
# Canary: 4e60fcae-5d48-4de7-a356-19714a3747f7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The NixOS workflow's lane matrix: run_lane.py's, each job's limit raised for the guest, and each
core file that must run alone in a job of its own.

A file runs alone when its bound measures the host and the guest's 4 vCPUs shared with a whole
core shard miss it: test_demo_open_steady_e2e.py keeps its 20 s bound (REQ-2006) and runs with no
other test beside it (maintainer 2026-11-26). test_master_key_survives_recreate_compose.py builds
the app image inside its fixture (about 300 s on an Ubuntu runner): in a core shard's guest, beside
the session's running stacks, the build took 767 s of its 900 s bound (run 45912217581) and then
passed it (run 45921735865, core 12/21). Its core shards deselect each, so each core file still
runs exactly once. A targeted run (PROVISA_LANE_TESTS) runs only what it names, as run_lane.py
routes it.

Usage: nixos_lane_plan.py '<run_lane.py --matrix JSON>'  (prints the NixOS matrix as JSON)
"""

from __future__ import annotations

import json
import os
import sys

#: The guest's own minutes on each job's limit: its build, its boot, and installs with no cache.
GUEST_MINUTES = 45

#: Core files that run alone on NixOS, each in its own job.
ALONE = (
    "tests/integration/test_demo_open_steady_e2e.py",
    "tests/integration/test_master_key_survives_recreate_compose.py",
)


def plan(matrix: dict, targeted: bool) -> dict:
    include = [{**e, "timeout": e["timeout"] + GUEST_MINUTES} for e in matrix["include"]]
    core = [e for e in include if e["lane"] == "core"]
    if targeted or not core:
        return {"include": include}
    deselect = " ".join(f"--deselect {f}" for f in ALONE)
    include = [{**e, "deselect": deselect} if e["lane"] == "core" else e for e in include]
    include += [
        {"lane": "core", "shard": "", "alone": f, "timeout": core[0]["timeout"]} for f in ALONE
    ]
    return {"include": include}


def main(argv: list[str]) -> int:
    targeted = bool(os.environ.get("PROVISA_LANE_TESTS", "").split())
    print(json.dumps(plan(json.loads(argv[0]), targeted)))  # noqa: T201
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
