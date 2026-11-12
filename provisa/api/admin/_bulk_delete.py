# Copyright (c) 2026 Kenneth Stott
# Canary: bb94c49b-1bc3-44ed-813c-197de84414a1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Several rows of a list deleted in one call (REQ-2240).

Each row is deleted by the single delete of its kind, with the same rights and the same guard as
deleting it alone: there is no second way to delete it. What can be deleted is deleted; each row
refused is named with the reason its single delete gave and the dependents it named.
"""

# Requirements: REQ-2240

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import TypeVar

from provisa.api.admin.types import BulkDeleteRefusal, BulkDeleteResult, MutationResult

K = TypeVar("K")


async def delete_each(
    keys: Sequence[K],
    name_of: Callable[[K], Awaitable[str]],
    delete_one: Callable[[K], Awaitable[MutationResult]],
) -> BulkDeleteResult:
    """Each of ``keys`` deleted by ``delete_one``, in order. ``name_of`` names a row as its page
    names it, read before the row is deleted. A refusal raised by a rights check is that row's
    answer, as a refusal returned is."""
    deleted: list[str] = []
    refused: list[BulkDeleteRefusal] = []
    for key in keys:
        name = await name_of(key)
        try:
            result = await delete_one(key)
        except PermissionError as denied:
            result = MutationResult(success=False, message=str(denied))
        if result.success:
            deleted.append(name)
            continue
        params = result.params if isinstance(result.params, dict) else {}
        # A refusal that is not over dependents (not found, not permitted) names none.
        dependents = params["dependents"] if "dependents" in params else []
        refused.append(BulkDeleteRefusal(name=name, reason=result.message, dependents=dependents))
    return BulkDeleteResult(deleted=deleted, refused=refused)


async def same_name(key: str) -> str:
    """The name of a row keyed by the name its page shows (a metric, source, role, domain, tag or
    data product)."""
    return key


async def relationship_name(rel_id: str) -> str:
    """A relationship as its page names it: its two tables, ``users -> inquiries``."""
    from provisa.api.admin._table_ops import _get_pool
    from provisa.core.repositories import relationship as rel_repo
    from provisa.core.repositories import table as table_repo

    pool = await _get_pool()
    async with pool.acquire() as conn:
        rel = await rel_repo.get(conn, rel_id)
        # A relationship that is not stored has no name but the id it was asked by; its own
        # delete then refuses it as not found.
        if rel is None:
            return rel_id
        source = await table_repo.get(conn, rel["source_table_id"])
        target = await table_repo.get(conn, rel["target_table_id"])
    if source is None or target is None:
        raise LookupError(f"relationship {rel_id!r} joins a table that is not registered")
    return f"{source['table_name']} -> {target['table_name']}"


async def scheduled_task_name(task_id: str) -> str:
    """A scheduled task as its page names it: its name."""
    from provisa.api.admin._table_ops import _get_pool
    from provisa.core.repositories import scheduled_trigger as trigger_repo

    pool = await _get_pool()
    async with pool.acquire() as conn:
        task = await trigger_repo.get(conn, task_id)
    # A task that is not stored has no name but the id it was asked by; its own delete then
    # refuses it as not found.
    return task_id if task is None else task["name"]
