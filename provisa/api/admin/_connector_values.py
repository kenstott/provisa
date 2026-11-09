# Copyright (c) 2026 Kenneth Stott
# Canary: b7e4d1a2-6c38-4f95-8a17-d2c90e3f6b58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a connector's standard file sets on the sources made from it (REQ-2221).

A default is applied when the source is created, and a steward can change it. A fixed value is
applied at create and no later save may change it, by the form or by any of the mutations that set
one setting. The source's own stored mapping says which connector it came from, so a save cannot
step out from under a fixed value by leaving the connector's name out.
"""

# Requirements: REQ-2221
from __future__ import annotations

import dataclasses
import json
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.core.models import CONNECTOR_SETTINGS
from provisa.core.schema_org import sources

if TYPE_CHECKING:
    from provisa.api.admin.types import MutationResult, SourceInput
    from provisa.core.database import Connection


def _refused(source_id: str, connector: str, name: str, fixed: Any) -> "MutationResult":
    from provisa.api.admin.types import MutationResult

    return MutationResult(
        success=False,
        message=f"Source {source_id!r}: {name} is fixed at {fixed!r} by connector {connector}",
        code="schema.connector_value_fixed",
        params={"source": source_id, "connector": connector, "name": name},
    )


def _unset_values() -> dict[str, Any]:
    """What a create that says nothing of a setting carries for it."""
    from provisa.api.admin.types import SourceInput

    defaults = {f.name: f.default for f in dataclasses.fields(SourceInput)}
    return {name: defaults[name] for name in CONNECTOR_SETTINGS}


async def _block(name: str) -> dict[str, Any] | None:
    from provisa.api.admin.schema_mutation import _get_pool
    from provisa.core.repositories import connector as connector_repo

    pool = await _get_pool()
    async with pool.acquire() as conn:
        return await connector_repo.get(conn, name)


def _settle_settings(
    input: "SourceInput", block: dict[str, Any], chose: set[str]
) -> "MutationResult | None":
    unset = _unset_values()
    for setting in block["settings"]:
        name, fixed, default = (
            setting["name"],
            setting.get("fixed_value"),
            setting.get("default_value"),
        )
        given = getattr(input, name)
        if fixed is not None:
            if given not in (fixed, unset[name]):
                return _refused(input.id, block["name"], name, fixed)
            setattr(input, name, fixed)
        elif name not in chose:
            setattr(input, name, default)
    return None


def _settle_requires(
    input: "SourceInput", block: dict[str, Any], mapping: dict[str, Any]
) -> "MutationResult | None":
    for required in block["requires"]:
        fixed, default = required.get("fixed_value"), required.get("default_value")
        if fixed is None and default is None:
            continue
        settings = mapping.setdefault("settings", {})
        name = required["name"]
        if fixed is not None:
            if settings.get(name, fixed) != fixed:
                return _refused(input.id, block["name"], name, fixed)
            settings[name] = fixed
        else:
            settings.setdefault(name, default)
    return None


async def settle_on_create(input: "SourceInput") -> "MutationResult | None":
    """``input``, joining a connector, with that connector's defaults and fixed values applied, or
    the refusal of a value that differs from a fixed one. ``connector_join.chose`` names the
    settings the creator changed from their defaults; every other default applies. A connector the
    model does not offer is left for the join to refuse by name."""
    if not input.mapping_json:
        return None
    mapping = json.loads(input.mapping_json)
    join = mapping.get("connector_join") if isinstance(mapping, dict) else None
    if not isinstance(join, dict):
        return None
    chose = set(join.pop("chose", None) or [])
    block = await _block(join.get("connector") or "")
    if block is None:
        input.mapping_json = json.dumps(mapping)
        return None
    refused = _settle_settings(input, block, chose) or _settle_requires(input, block, mapping)
    if refused is not None:
        return refused
    input.mapping_json = json.dumps(mapping)
    return None


async def fixed_refusal(
    conn: "Connection",
    source_id: str,
    stored_mapping: dict[str, Any],
    changes: dict[str, Any],
    new_mapping: dict[str, Any] | None = None,
) -> "MutationResult | None":
    """The refusal of a save of ``changes`` (source settings by name) and ``new_mapping`` that
    changes a value the source's connector fixes; None when it changes none. A kept source stays
    its connector's whatever the save's mapping says."""
    from provisa.core.repositories import connector as connector_repo

    name = stored_mapping.get("connector")
    if not name:
        return None
    if new_mapping is not None:
        new_mapping["connector"] = name
    block = await connector_repo.get(conn, name)
    if block is None:
        return None
    for setting in block["settings"]:
        fixed = setting.get("fixed_value")
        if fixed is not None and setting["name"] in changes and changes[setting["name"]] != fixed:
            return _refused(source_id, name, setting["name"], fixed)
    if new_mapping is not None:
        settings = new_mapping.get("settings") or {}
        for required in block["requires"]:
            fixed = required.get("fixed_value")
            if fixed is not None and settings.get(required["name"]) != fixed:
                return _refused(source_id, name, required["name"], fixed)
    return None


async def fixed_refusal_by_id(
    conn: "Connection", source_id: str, changes: dict[str, Any]
) -> "MutationResult | None":
    """:func:`fixed_refusal` for a mutation that sets some settings of the source ``source_id``."""
    found = (
        await conn.execute_core(select(sources.c.mapping).where(sources.c.id == source_id))
    ).fetchone()
    if found is None:
        return None
    return await fixed_refusal(conn, source_id, found.mapping or {}, changes)


def input_settings(input: "SourceInput") -> dict[str, Any]:
    """The standard settings a whole-source save carries."""
    return {name: getattr(input, name) for name in CONNECTOR_SETTINGS}
