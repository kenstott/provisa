# Copyright (c) 2026 Kenneth Stott
# Canary: 5ba575c6-05a3-4257-a9db-cdf892f15d8a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code source saved (REQ-1984, REQ-1993): its code is fetched, installed and read before the
source is kept, and the content hash of what was read is recorded in its mapping. A later install
that finds other content is refused by both hashes; saving the source again is how its code is
moved on. Code that cannot be used refuses the save by name."""

# Requirements: REQ-1984, REQ-1993
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.api.admin.types import MutationResult


async def record_code(source_id: str, source_type: str, mapping: dict) -> "MutationResult | None":
    """Prepare a code source being saved and record its content hash in ``mapping`` (in place).
    None when it is not a code source or it is ready; else the refusal."""
    from provisa.api.admin.types import MutationResult
    from provisa.api.app import state
    from provisa.codesource.bundle import BundleRefused
    from provisa.codesource.source import CODE_SOURCE_TYPES, context_for, forget, prepare
    from provisa.runtime_deps.code_runtimes import RuntimeUnavailable

    if source_type not in CODE_SOURCE_TYPES:
        return None
    forget(source_id)  # whatever was prepared for an earlier save of it
    if mapping.get("mode") != "built":
        mapping.pop("content_hash", None)  # pinned afresh: the code as it is now
    # REQ-2098: a built source's code runs on its remote host and is not read here; it keeps the
    # hash it is pinned to, which only a re-pin (or going back to dev) changes.
    try:
        ready = await prepare(source_id, source_type, mapping, await context_for(state))
    except BundleRefused as refused:
        return MutationResult(
            success=False, message=str(refused), code=refused.code, params=refused.params
        )
    except RuntimeUnavailable as missing:
        return MutationResult(
            success=False,
            message=str(missing),
            code="codesource.runtime_unavailable",
            params={"reason": str(missing)},
        )
    mapping["content_hash"] = ready.content_hash
    return None
