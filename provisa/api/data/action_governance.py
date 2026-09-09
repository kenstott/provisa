# Copyright (c) 2026 Kenneth Stott
# Canary: 7a3e9c1d-5b8f-4d2a-9e6c-0f4b7a1d3e85
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A tracked function's or webhook's response, governed like a table's rows (REQ-1679).

The rows an action returns are bound as a relation whose columns are the action's declared
output contract, and that relation goes through the ONE governance stage
(:func:`provisa.compiler.stage2.apply_governance`) exactly as a hot-table CTE does (REQ-233):
the governed SELECT is written over a VALUES CTE holding the rows, session variables are
resolved the way the pipeline resolves them for the federation engine, and the engine evaluates
it. Row filters come from the role's RLS rule for the action (or its domain's rule), column
visibility and masks from the contract's per-column settings, inheritance from the role chain
(REQ-1677). No second implementation of RLS, masking or visibility exists here — the SQL is
governed by the same function every table read is governed by.

A response that does not fit the declared contract is refused: an off-contract row is a
data-integrity fault, not something to pass through ungoverned. An action that declares no
column contract returns a scalar (or nothing) and has no columns to bind; it is returned as is.
"""

# Requirements: REQ-1679

from __future__ import annotations

import json
import time
import zlib
from dataclasses import dataclass, field
from typing import Any

from provisa.api.errors import ApiError

_JSON_TO_IR = {
    "integer": "integer",
    "number": "double",
    "string": "varchar",
    "boolean": "boolean",
    "object": "json",
    "array": "json",
}


@dataclass
class ActionEnforcement:
    """What governance did to the response — surfaced by the admin test-invoke and the audit."""

    role_used: str
    rls_filters_applied: list[str] = field(default_factory=list)
    columns_excluded: list[str] = field(default_factory=list)
    masking_applied: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "role_used": self.role_used,
            "rls_filters_applied": list(self.rls_filters_applied),
            "columns_excluded": list(self.columns_excluded),
            "masking_applied": list(self.masking_applied),
        }


def _as_list(value: Any) -> list:
    """A JSON list column read back as a string (raw SQL on SQLite/asyncpg) or as a list."""
    if value is None:
        return []
    if isinstance(value, str):
        return json.loads(value) if value.strip() else []
    return list(value)


def _as_dict(value: Any) -> dict:
    if value is None:
        return {}
    if isinstance(value, str):
        return json.loads(value) if value.strip() else {}
    return dict(value)


def contract_columns(action: dict) -> list[dict] | None:
    """The action's declared output columns as ``{name, type, …governance}`` dicts, or None when
    the action declares no column contract (a scalar return).

    A function's contract is ``output_columns`` (REQ-1159); when it declares none but its
    ``return_schema`` is a JSON-schema array of objects, that object's properties are the contract
    (the demo functions declare their datasets that way). A webhook's is ``inline_return_type``.
    """
    cols = _as_list(action.get("output_columns"))
    if cols:
        return [dict(c) for c in cols]
    inline = _as_list(action.get("inline_return_type"))
    if inline:
        return [dict(c) for c in inline]
    schema = _as_dict(action.get("return_schema"))
    items = schema.get("items") if schema.get("type") == "array" else None
    props = (items or {}).get("properties") if isinstance(items, dict) else None
    if props:
        return [
            {"name": name, "type": _JSON_TO_IR.get((spec or {}).get("type", "string"), "varchar")}
            for name, spec in props.items()
        ]
    return None


def relation_name(action_name: str) -> str:
    return f"_action_{action_name}"


def synthetic_table_id(action_name: str) -> int:
    """A stable negative id for the action's relation, so it can never collide with a registered
    table's id in the governance context."""
    return -(zlib.crc32(action_name.encode("utf-8")) & 0x7FFFFFFF) - 1


def _off_contract(action_name: str, rows: list[dict], names: list[str]) -> str | None:
    declared = set(names)
    for row in rows:
        if not isinstance(row, dict):
            return f"{action_name!r} returned a non-object row"
        extra = sorted(set(row) - declared)
        if extra:
            return f"{action_name!r} returned columns outside its contract: {', '.join(extra)}"
    return None


class _Rows:
    """The VALUES-CTE builder's ``HotRows`` view of a response."""

    def __init__(self, column_names: list[str], rows: list[dict]) -> None:
        self.column_names = column_names
        self.rows = rows


def _governance_context(
    action: dict, cols: list[dict], role_id: str, state: Any
) -> tuple[Any, ActionEnforcement]:
    from provisa.compiler.stage2 import GovernanceContext, resolve_row_cap
    from provisa.security.inheritance import holds_grant
    from provisa.security.masking import MaskingRule, MaskType, validate_masking_rule
    from provisa.security.rights import Capability, has_capability

    name = action["name"]
    tid = synthetic_table_id(name)
    rel = relation_name(name)
    role = state.roles.get(role_id) or {}
    chains = getattr(state, "role_chains", None) or {}
    is_admin = has_capability(role, Capability.ADMIN)
    enforcement = ActionEnforcement(role_used=role_id)

    gov = GovernanceContext()
    gov.table_map[rel] = tid
    gov.all_columns[tid] = [(c["name"], c.get("type") or "varchar") for c in cols]

    if is_admin:
        gov.visible_columns[tid] = None
    else:
        visible: set[str] = set()
        for c in cols:
            granted = c.get("visible_to")
            # A contract column that declares no visible_to is visible: the contract is the
            # action's public shape, and an undeclared grant list restricts nothing (unlike a
            # registered table's column, which is seeded with an explicit list).
            if granted is None or holds_grant(role_id, granted, chains):
                visible.add(c["name"])
            else:
                enforcement.columns_excluded.append(c["name"])
        gov.visible_columns[tid] = frozenset(visible)

    for c in cols:
        if not c.get("mask_type"):
            continue
        if holds_grant(role_id, c.get("unmasked_to") or [], chains):
            continue
        rule = MaskingRule(
            mask_type=MaskType(c["mask_type"]),
            pattern=c.get("mask_pattern"),
            replace=c.get("mask_replace"),
            value=c.get("mask_value"),
            precision=c.get("mask_precision"),
        )
        dtype = c.get("type") or "varchar"
        validate_masking_rule(rule, c["name"], dtype, True)
        gov.masking_rules[(tid, c["name"])] = (rule, dtype)
        enforcement.masking_applied.append(f"{c['name']} -> {rule.mask_type.value}")

    rls = state.rls_contexts.get(role_id)
    if rls is not None:
        expr = rls.action_rules.get(name)
        if expr is None and action.get("domain_id"):
            expr = rls.domain_rules.get(action["domain_id"])
        if expr:
            gov.rls_rules[tid] = expr
            enforcement.rls_filters_applied.append(expr)

    gov.limit_ceiling = resolve_row_cap(role, role.get("max_rows"))
    return gov, enforcement


async def govern_action_rows(
    rows: list[dict], action: dict, role_id: str, state: Any
) -> tuple[list[dict], ActionEnforcement | None]:
    """The response ``rows`` of ``action`` as ``role_id`` may see them, plus what was applied.

    ``None`` enforcement means the action declares no column contract, so there was nothing to
    bind; the rows are returned unchanged.
    """
    cols = contract_columns(action)
    if cols is None:
        return rows, None
    names = [c["name"] for c in cols]
    problem = _off_contract(action["name"], rows, names)
    if problem is not None:
        raise ApiError(502, "actions.response_off_contract", problem, action=action["name"])

    from provisa.cache.values_cte import build_values_cte_sql
    from provisa.compiler.stage2 import apply_governance
    from provisa.pgwire._pipeline import _resolve_session_settings

    gov, enforcement = _governance_context(action, cols, role_id, state)
    rel = relation_name(action["name"])
    # Each column carries its own name as an alias so a masked projection keeps the column's
    # name in the result — the same shape the pipeline's star expansion produces.
    quoted = ", ".join(f'"{n}" AS "{n}"' for n in names)
    governed = apply_governance(f'SELECT {quoted} FROM "{rel}"', gov)
    sql = build_values_cte_sql(governed, rel, _Rows(names, rows))
    role = state.roles.get(role_id) or {}
    sql = _resolve_session_settings(sql, role.get("session_vars") or {})

    engine = state.federation_engine
    started = time.monotonic()
    result = await engine.execute_engine(engine.transpile_physical(sql))
    # The engine hands back its own scalar types (Decimal for a numeric literal, date objects);
    # convert them the way every data surface converts a query result.
    from provisa.executor.serialize import _convert_value

    out = [
        {name: _convert_value(v) for name, v in zip(result.column_names, r)} for r in result.rows
    ]
    await _audit(sql, role_id, state, started)
    return out, enforcement


async def _audit(sql: str, role_id: str, state: Any, started: float) -> None:
    """The governed statement lands in the query audit log like a table read. No registered table
    is involved, so the usage list is empty; the statement text carries the relation name."""
    from provisa.audit.context import current_audit_identity
    from provisa.audit.pipeline import PendingAudit, write_audit

    identity = current_audit_identity()
    if identity is None:
        return
    pending = PendingAudit(
        user_id=identity.user_id,
        surface=identity.surface,
        role_id=role_id,
        query_text=sql,
        table_ids=[],
        started=started,
    )
    await write_audit(pending, 200, state)
