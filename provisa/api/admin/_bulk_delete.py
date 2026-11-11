# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
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
