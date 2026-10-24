# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Run a command with the ephemeral warehouses up (the core UI lane, ui-e2e-core.yml).

    with_warehouses.py <command> [args...]

source-to-query-cloud-warehouse.spec.ts reads two warehouses that are not left standing between
runs: Redshift Serverless and the Synapse serverless workspace. They are stood up as the
integration warehouse lane stands them up -- tests/integration/redshift_cluster.py's
``redshift_lane`` (a stamped workgroup, its REDSHIFT_* exported) and synapse_provision.py's
``synapse_lane`` (the pinned workspace's firewall opened for this runner) -- the command runs
with them, and both are torn down when it ends, however it ends. A run whose named specs
(PROVISA_E2E_SPECS) leave that spec out stands up neither. A credential the warehouses need that
is absent fails the run by name: the spec would otherwise be counted NOT EXECUTED.
"""

from __future__ import annotations

import os
import subprocess
import sys
from contextlib import ExitStack
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
SPEC = "source-to-query-cloud-warehouse.spec.ts"
NEEDS = (
    "REDSHIFT_AWS_ACCESS_KEY_ID",
    "REDSHIFT_AWS_SECRET_ACCESS_KEY",
    "REDSHIFT_AWS_REGION",
    "SYNAPSE_SQL_SERVER",
    "SYNAPSE_DATABASE",
    "SYNAPSE_ADLS_URL",
    "FABRIC_RESOURCE_GROUP",
)


def wanted(specs: str) -> bool:
    """Whether the run reads the warehouses: every spec (none named), or the spec named."""
    named = specs.split()
    return not named or any(SPEC in s for s in named)


def main(argv: list[str]) -> int:
    if not argv:
        raise SystemExit(__doc__)
    if not wanted(os.environ.get("PROVISA_E2E_SPECS", "")):
        return subprocess.call(argv)  # noqa: S603 -- the lane's own command
    missing = [name for name in NEEDS if not os.environ.get(name)]
    if missing:
        print(f"::error::the warehouses cannot be stood up; not set: {' '.join(missing)}")
        return 1
    sys.path.insert(0, str(REPO))
    from tests.integration.redshift_cluster import redshift_lane
    from tests.integration.synapse_provision import synapse_lane

    with ExitStack() as stack:
        stack.enter_context(redshift_lane())  # exports REDSHIFT_* into this environment
        stack.enter_context(synapse_lane())
        return subprocess.call(argv, env=os.environ.copy())  # noqa: S603 -- the lane's own command


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
