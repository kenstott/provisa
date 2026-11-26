# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7d9e21-6a4c-4c58-8f02-e1c5a7b94d63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin routes of the column rules keyed on a tag or a classification (REQ-2278).

* ``GET    /admin/column-rules`` -- the rules, narrowed by ``tag`` or ``classification``.
* ``PUT    /admin/column-rules`` -- save the rule of a key and a role (a role has one per key).
* ``DELETE /admin/column-rules/{rule_id}`` -- take a rule away.

A rule reaches every column carrying its key, in every domain, so it is managed by a holder of the
right it exercises in every domain: ``column_grant`` for a hide rule, ``masking_config`` for a mask
rule. The rules are read by a holder of either. A save or a removal rebuilds the runtime, so the
rule holds, or is lifted, at once.
"""

# Requirements: REQ-2278

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from pydantic import ValidationError

from provisa.api.admin.capabilities import (
    allowed_domains_for_capability_request,
    has_capability_request,
    require_capability_request,
)
from provisa.api.errors import ApiError

router = APIRouter(prefix="/admin", tags=["admin", "security"])

#: The right a rule of each action exercises.
_RIGHT = {"hide": "column_grant", "mask": "masking_config"}


def _db() -> Any:
    from provisa.api.app import state

    if state.model_db is None:
        raise ApiError(503, "column_rules.database_unavailable", "Database unavailable")
    return state.model_db


def _require_everywhere(request: Request, right: str) -> None:
    """Refuse a caller without ``right``, or holding it in some domains only: a rule reaches
    columns in every domain."""
    require_capability_request(request, right)
    if allowed_domains_for_capability_request(request, right) is not None:
        raise ApiError(
            403,
            "column_rules.right_not_everywhere",
            f"A column rule reaches every domain; it needs {right!r} in every domain",
            right=right,
        )


def _require_reader(request: Request) -> None:
    """Refuse a caller holding neither right a rule exercises."""
    if not has_capability_request(request, _RIGHT["hide"]):
        require_capability_request(request, _RIGHT["mask"])


def _jsonable(rule: dict) -> dict:
    return {
        "id": rule["id"],
        "tagId": rule["tag_id"],
        "classification": rule["classification"],
        "roleId": rule["role_id"],
        "action": rule["action"],
        "mask": rule["mask"],
    }


async def keyed_by(conn: Any, tags: "tuple[str, ...]" = (), kinds: "tuple[str, ...]" = ()) -> bool:
    """Whether a column rule is keyed on one of ``tags`` (a tag, or its base) or ``kinds``: a
    change of a column's tags or classifications then rebuilds the runtime, so the column comes
    under the rule at once, and is released at once when the tag or classification comes off."""
    from provisa.core.models import base_tag_id
    from provisa.core.repositories import column_rule as repo

    keyed_tags, keyed_kinds = await repo.keyed(conn)
    named = {t for tag in tags for t in (tag, base_tag_id(tag))}
    return bool(named & keyed_tags or set(kinds) & keyed_kinds)


@router.get("/column-rules")
async def list_column_rules(
    request: Request, tag: str | None = None, classification: str | None = None
) -> dict:
    from provisa.core.repositories import column_rule as repo

    _require_reader(request)
    async with _db().acquire() as conn:
        rules = await repo.list_all(conn)
    return {
        "rules": [
            _jsonable(r)
            for r in rules
            if (tag is None or r["tag_id"] == tag)
            and (classification is None or r["classification"] == classification)
        ]
    }


async def _require_key(conn: Any, tag_id: str | None, classification: str | None) -> None:
    from provisa.core.repositories import tag as tag_repo

    if tag_id is not None:
        if await tag_repo.get(conn, tag_id) is None:
            raise ApiError(
                404, "column_rules.tag_not_found", f"Tag {tag_id!r} not found", tag=tag_id
            )
        return
    from provisa.profiler.classify import kind_of

    try:
        kind_of(str(classification))
    except KeyError as exc:
        raise ApiError(
            422, "profile.classification_kind_unknown", str(exc), kind=classification
        ) from exc


async def _require_role(conn: Any, role_id: str) -> None:
    from sqlalchemy import select

    from provisa.core.schema_org import roles

    found = (await conn.execute_core(select(roles.c.id).where(roles.c.id == role_id))).fetchone()
    if found is None:
        raise ApiError(404, "roles.not_found", f"No role {role_id!r}", role=role_id)


@router.put("/column-rules")
async def save_column_rule(request: Request, body: dict) -> dict:
    from provisa.api.app import _rebuild_schemas
    from provisa.core.models import ColumnRuleModel
    from provisa.core.repositories import column_rule as repo

    try:
        rule = ColumnRuleModel(**body)
    except ValidationError as exc:
        reason = str(exc.errors()[0]["msg"])
        raise ApiError(422, "column_rules.invalid", reason, reason=reason) from exc
    _require_everywhere(request, _RIGHT[rule.action])
    async with _db().acquire() as conn:
        await _require_key(conn, rule.tag_id, rule.classification)
        await _require_role(conn, rule.role_id)
        saved = await repo.upsert(conn, rule)
    await _rebuild_schemas()
    return {"rule": _jsonable(saved)}


@router.delete("/column-rules/{rule_id}")
async def delete_column_rule(request: Request, rule_id: int) -> dict:
    from provisa.api.app import _rebuild_schemas
    from provisa.core.repositories import column_rule as repo

    async with _db().acquire() as conn:
        found = await repo.get(conn, rule_id)
        if found is None:
            raise ApiError(404, "column_rules.not_found", f"No column rule {rule_id}", rule=rule_id)
        _require_everywhere(request, _RIGHT[found["action"]])
        await repo.delete(conn, rule_id)
    await _rebuild_schemas()
    return {"deleted": rule_id}
