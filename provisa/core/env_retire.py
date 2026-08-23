# Copyright (c) 2026 Kenneth Stott
# Canary: 8c5be27a-14d9-4f60-b3e1-90a7d6c4fe25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Retiring an environment: its schemas, its registry row, and optionally its branch (REQ-1542).

WHY THIS IS NOT JUST THE DELETE ENDPOINT'S BODY. A merge that retires the environment it came from
has to do exactly what a delete does -- a half-retired environment holding schemas with no registry
row is worse than either state -- and a merge is decided in one place while a delete is called from
another. One function, called from both, is what keeps them the same act.

WHY THE BRANCH IS OPTIONAL. Deleting an environment through the delete door leaves its branch,
because the branch is the record of what that environment held and a person deleting a schema has
not asked to lose the history. A merge that retires its source is the other case: the work landed
in the target, the feature is over, and leaving the ref behind leaves a branch nobody writes and
nobody reads. So the caller says which one this is, and neither guesses.

WHAT IS NEVER LOST. Deleting a ref deletes a NAME. The commits remain in the object store and
remain reachable by sha, so a retired branch is still browsable and still deployable by anybody who
kept one -- retiring is tidying, not destruction.
"""

# Requirements: REQ-1542, REQ-1524, REQ-1488, REQ-1487

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from provisa.core.env_repo import delete_branch
from provisa.core.env_store import forget_env
from provisa.core.environments import PROD

if TYPE_CHECKING:
    from provisa.core.db import Database


class RetirementError(Exception):
    """An environment that may not be retired was named."""


async def retire_environment(
    pool: "Database",
    admin_db: "Database",
    org_id: str,
    name: str,
    *,
    drop_branch: bool,
) -> dict:
    """Drop ``name``'s stores and registry row, and its branch when asked. Returns what was done.

    ``prod`` is refused here rather than at each caller, because REQ-1487 makes it exist from the
    organization's creation: an org without a prod environment is not a state this platform has.
    """
    if name == PROD:
        raise RetirementError(
            f"{PROD!r} exists from the organization's creation and cannot be retired; delete the "
            "organization to remove it."
        )
    from provisa.core.org_provisioning import deprovision_org

    await deprovision_org(pool, org_id, redis_url=os.environ.get("REDIS_URL"), env=name)
    await forget_env(admin_db, org_id, name)
    dropped = delete_branch(org_id, name) if drop_branch else False
    return {"retired": name, "branch_deleted": dropped}
