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
moved on. Code that cannot be used refuses the save by name. A source a configuration declares is
pinned the same way once its environment's model is built (pin_in_background)."""

# Requirements: REQ-1984, REQ-1993
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.api.admin.types import MutationResult
    from provisa.core.database import Database


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


#: The code sources being pinned now, and those whose code was refused, by (org, env, source):
#: each refused one with the code it was refused for, so a later build tries it again only once
#: its code has changed.
_pinning: set[tuple[str, str, str]] = set()
_refused: dict[tuple[str, str, str], str] = {}


def unpinned(sources: dict[str, dict]) -> list[tuple[str, str, dict]]:
    """The code sources among an environment's ``sources`` (id: row) whose code no save pinned:
    those its configuration declared, written to the model with no content hash."""
    return [
        (source_id, row["type"], dict(row.get("mapping") or {}))
        for source_id, row in sources.items()
        if row.get("type") in _code_types() and not (row.get("mapping") or {}).get("content_hash")
    ]


def _code_types() -> frozenset[str]:
    from provisa.codesource.source import CODE_SOURCE_TYPES

    return CODE_SOURCE_TYPES


async def pin_declared_code(
    org_id: str, env: str, model_db: "Database", found: list[tuple[str, str, dict]]
) -> list[str]:
    """Pin each of ``found``'s code as a save of it does (``record_code``): its code prepared and
    its content hash written into its stored mapping, so a source its configuration declared is
    saved like one saved in the UI (its image can be built, REQ-2098). One whose code is refused
    stays unpinned, logged with why. The sources pinned."""
    import json
    import logging

    from sqlalchemy import update

    from provisa.core import model_change
    from provisa.core.schema_org import sources

    log = logging.getLogger(__name__)
    pinned = []
    for source_id, source_type, mapping in found:
        key = (org_id, env, source_id)
        code = json.dumps(mapping.get("code"), sort_keys=True)
        if key in _pinning or _refused.get(key) == code:
            continue
        _pinning.add(key)
        try:
            refusal = await record_code(source_id, source_type, mapping)
            if refusal is not None:
                _refused[key] = code
                log.warning(
                    "code source %s of %s is not pinned: %s", source_id, org_id, refusal.message
                )
                continue
            _refused.pop(key, None)
            async with model_change.scope(f"pin the code of source {source_id}"):
                async with model_db.acquire() as conn:
                    await conn.execute_core(
                        update(sources).where(sources.c.id == source_id).values(mapping=mapping)
                    )
            pinned.append(source_id)
        finally:
            _pinning.discard(key)
    return pinned


def pin_in_background(org_id: str, env: str, model_db: "Database", rows: dict[str, dict]) -> None:
    """After an environment's model is built: pin, in the background, the code its configuration
    declared with no content hash. Preparing code installs it, which no build waits for."""
    from provisa.core import process_mode
    from provisa.core.connection_loop import spawn_background

    found = unpinned(rows)
    if found and process_mode.runs_background_work():
        spawn_background(pin_declared_code(org_id, env, model_db, found), name="pin-declared-code")
