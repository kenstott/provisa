# Copyright (c) 2026 Kenneth Stott
# Canary: 0c6f3b2e-5a7d-4e19-9b84-71d2e6a3f5c8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Who may change how a table's columns are hidden, and in which domains (REQ-1943, REQ-1944).

A table save carries every column's hiding definition -- its read grants, its role masks, its fake
and its synthetic rule. Each of those is governed by its own right, exercised only in the domain of
the table it changes:

- ``visible_to`` (column grants): ``column_grant`` or ``access_config``;
- ``unmasked_to`` and the ``mask_*`` fields (role masks): ``masking_config``;
- ``fake``, ``fake_stable``, ``synthetic_rule``: open to the table's editor; a caller saving
  without ``table_registration`` needs one of the governance rights above in the table's domain;
- any of them on a SENSITIVE column (one carrying a sensitive tag): ``sensitive_data``.

A caller holding ``table_registration`` saves the whole table as before, these field changes
included only when it also holds their rights in the table's domain. A caller without it may save
a table only when the save changes nothing BUT hiding fields (:func:`non_hiding_changes`) -- the
governance a data steward owns without the table editing it does not.
"""

# Requirements: REQ-1943, REQ-1944

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.security.sensitive import HIDING_FIELDS, SENSITIVE_DATA, _value

if TYPE_CHECKING:
    from provisa.core.database import Connection

VISIBILITY_RIGHTS: tuple[str, ...] = ("column_grant", "access_config")
MASK_RIGHTS: tuple[str, ...] = ("masking_config",)
#: The governance rights any one of which admits a fake or synthetic-rule edit without the table
#: editor's right (REQ-1944).
GOVERNANCE_RIGHTS: tuple[str, ...] = (
    "column_grant",
    "access_config",
    "masking_config",
    SENSITIVE_DATA,
)
_MASK_FIELDS = frozenset(
    {"unmasked_to", "mask_type", "mask_pattern", "mask_replace", "mask_value", "mask_precision"}
)
_FAKE_FIELDS = frozenset({"fake", "fake_stable", "synthetic_rule"})

#: A column not yet stored: hidden by nothing (visible_to [] is visible to every role).
_UNSTORED_COLUMN: dict[str, Any] = {
    "visible_to": [],
    "scope": "domain",
    "unmasked_to": [],
    "mask_type": None,
    "mask_pattern": None,
    "mask_replace": None,
    "mask_value": None,
    "mask_precision": None,
    "fake": None,
    "fake_stable": False,
    "synthetic_rule": None,
}


def rights_for(field: str, *, sensitive: bool, editor: bool) -> tuple[str, ...] | None:
    """The rights (any one of) a change to ``field`` needs, or None when the editor's own right
    covers it."""
    if sensitive:
        return (SENSITIVE_DATA,)
    if field == "visible_to":
        return VISIBILITY_RIGHTS
    if field == "scope":
        # Reached for a change that WIDENS who is served the column (:func:`scope_widens`); a
        # narrowing is the table editor's own to make.
        return VISIBILITY_RIGHTS
    if field in _MASK_FIELDS:
        return MASK_RIGHTS
    if field in _FAKE_FIELDS:
        return None if editor else GOVERNANCE_RIGHTS
    raise ValueError(f"{field!r} is not a hiding field")


#: Who a scope serves, narrowest first (REQ-1959): the roles a grant names; the roles that
#: reach the table's domain; roles outside it too.
_SCOPE_REACH = {"restricted": 0, "domain": 1, "public": 2}


def scope_widens(before: str, after: str) -> bool:
    """Whether changing a column's scope from ``before`` to ``after`` serves it to more roles."""
    return _SCOPE_REACH[after] > _SCOPE_REACH[before]


async def stored_table(conn: "Connection", model: Any) -> dict | None:
    """The stored row of the table ``model`` saves (by source/schema/table), or None if new."""
    from provisa.core.schema_org import registered_tables as rt

    row = (
        await conn.execute_core(
            select(rt.c.id, rt.c.domain_id).where(
                rt.c.source_id == model.source_id,
                rt.c.schema_name == model.schema_name,
                rt.c.table_name == model.table_name,
            )
        )
    ).fetchone()
    return None if row is None else {"id": int(row[0]), "domain_id": row[1]}


async def hiding_refusal(
    conn: "Connection", model: Any, *, identity: Any, state: Any, editor: bool
) -> str | None:
    """Why this save of ``model`` may not change how its columns are hidden, or None.

    Each changed hiding field is checked for its right in the table's domain -- the stored one,
    and the one it is saved into. The refusal names every column and field, and the domain.
    """
    from provisa.api.admin.capabilities import right_domain_refusal
    from provisa.core.repositories.table import load_columns
    from provisa.security.sensitive import sensitive_columns

    stored = await stored_table(conn, model)
    domains = {model.domain_id}
    stored_columns: dict[str, dict] = {}
    sensitive: frozenset[str] = frozenset()
    if stored is not None:
        domains.add(stored["domain_id"])
        stored_columns = {c["column_name"]: c for c in await load_columns(conn, stored["id"])}
        sensitive = await sensitive_columns(conn, stored["id"])
    refusals: list[str] = []
    for column in model.columns:
        before = stored_columns.get(column.name, _UNSTORED_COLUMN)
        for field in HIDING_FIELDS:
            if _value(before, field) == _value(column, field):
                continue
            tagged = column.name in sensitive
            if (
                field == "scope"
                and editor
                and not tagged
                and not scope_widens(_value(before, field), _value(column, field))
            ):
                # Narrowing who is served a column is the table editor's to do, as setting its
                # scope always was; only widening it needs the right to grant (REQ-1959). On a
                # sensitive column every change needs sensitive_data (REQ-1943), below.
                continue
            rights = rights_for(field, sensitive=tagged, editor=editor)
            if rights is None:
                continue
            refusal = right_domain_refusal(identity, state, rights, domains)
            if refusal is not None:
                refusals.append(f"{column.name} ({field}): {refusal}")
    if not refusals:
        return None
    return "changing how a column is hidden is refused -- " + "; ".join(refusals)


#: Fields of a ``TableInput`` the table read (``table_edit.table_input``) does not carry; a
#: hiding-only save must leave them unset.
_UNREAD_FIELDS = ("pagination", "view_metrics", "discover")


def _normal(value: Any) -> Any:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {f.name: _normal(getattr(value, f.name)) for f in dataclasses.fields(value)}
    if isinstance(value, dict):
        return {k: _normal(v) for k, v in value.items()} or None
    if isinstance(value, (list, tuple)):
        return [_normal(v) for v in value] or None
    if value == "":
        return None
    return value


def _set(value: Any, default: Any) -> bool:
    """Whether a save gives ``value`` rather than leaving the field at its input default."""
    return _normal(value) != _normal(default)


def _defaults(obj: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for f in dataclasses.fields(obj):
        if f.default is not dataclasses.MISSING:
            out[f.name] = f.default
        elif f.default_factory is not dataclasses.MISSING:
            out[f.name] = f.default_factory()
        else:
            out[f.name] = None
    return out


async def steward_input(input_: Any) -> tuple[Any, list[str]]:
    """The save a caller without ``table_registration`` makes, and what else it tried to change.

    The save is the table AS STORED (``table_edit.table_input``, the editor's own full input)
    with each column's hiding fields taken from ``input_``: every setting the caller does not
    govern is written back as it is, whether its form sent it or left it at the input default --
    a form that omits a field (load protection, the modeling role) must not reset it. A field
    ``input_`` does set, to a value other than the stored one, is a change the caller may not
    make, and is named. A table not yet registered is all change.
    """
    from provisa.api.admin.schema_helpers import _get_pool
    from provisa.api.mcp.table_edit import read_table, table_input

    pool = await _get_pool()
    async with pool.acquire() as conn:
        stored = await stored_table(conn, input_)
    if stored is None:
        return input_, [f"table {input_.table_name!r} (not registered)"]
    current = table_input(await read_table(stored["id"]))
    changes: list[str] = []
    defaults = _defaults(input_)
    for name in _UNREAD_FIELDS:
        if _set(getattr(input_, name), defaults[name]):
            changes.append(name)
    for f in dataclasses.fields(current):
        if f.name in _UNREAD_FIELDS or f.name == "columns":
            continue
        given = getattr(input_, f.name)
        if _set(given, defaults[f.name]) and _normal(given) != _normal(getattr(current, f.name)):
            changes.append(f.name)
    given_columns = {c.name: c for c in input_.columns}
    held_columns = {c.name: c for c in current.columns}
    if set(given_columns) != set(held_columns):
        changes.append("columns")
    for name in sorted(set(given_columns) & set(held_columns)):
        given, held = given_columns[name], held_columns[name]
        column_defaults = _defaults(given)
        for f in dataclasses.fields(given):
            value = getattr(given, f.name)
            if f.name in HIDING_FIELDS:
                setattr(held, f.name, value)
            elif _set(value, column_defaults[f.name]) and _normal(value) != _normal(
                getattr(held, f.name)
            ):
                changes.append(f"{name}.{f.name}")
    return current, changes


# What ``view_governance`` shows (REQ-1134) and so what a save may set only with it (REQ-1958):
# a column's grant lists and its mask.
GOVERNANCE_FIELDS = (
    "visible_to",
    "writable_by",
    "unmasked_to",
    "mask_type",
    "mask_pattern",
    "mask_replace",
    "mask_value",
    "mask_precision",
)


def governance_sets(info: Any, input_: Any) -> list[str]:  # REQ-1958, REQ-1134
    """The grant lists and mask fields ``input_`` SETS, for a caller without ``view_governance``.

    Such a caller is answered null for them, so what it sends back is the empty value; anything
    else is a value it chose without being shown what it replaces. Empty for a caller holding
    the right. Read off the input as sent, before :func:`keep_governance` fills it in.
    """
    from provisa.api.admin.capabilities import has_capability

    if has_capability(info, "view_governance"):
        return []
    return [
        f"{column.name}.{field}"
        for column in input_.columns
        for field in GOVERNANCE_FIELDS
        if getattr(column, field) not in (None, [], "")
    ]


def refuse_governance_sets(sets: list[str]) -> None:  # REQ-1958
    """Refuse a save that :func:`governance_sets` found setting a field its caller is not shown.

    Raised after the hiding rights are checked (:func:`table_hiding_refusal`), so a caller that
    lacks the right to change a mask or a grant at all is told that, as before. An equal value
    is refused too, or the refusal would tell a caller whether its guess of a hidden grant list
    was right.
    """
    if sets:
        raise PermissionError(
            "Missing capability: 'view_governance' -- without it a table save may not set a "
            "column's grant lists or mask, which it is not shown; this one sets " + ", ".join(sets)
        )


async def keep_governance(info: Any, input_: Any) -> Any:  # REQ-1958, REQ-1134
    """``input_`` with every grant list and mask field it leaves empty taken from the STORED
    table, for a caller without ``view_governance``.

    Such a caller is answered null for those fields (admin/catalog_scope.py), and a table save
    replaces each column's definition whole — so its save would write "nobody" and "no mask"
    over grants it was never shown. Its save does not touch them. A field the save SETS is left
    as sent for the rights checks to judge, and the save is then refused
    (:func:`refuse_governance_sets`). A column the table does not have yet has no stored grants:
    it is saved open to its domain and unmasked, as a column registered without any is.

    A caller holding ``view_governance`` saves what it sends.
    """
    from provisa.api.admin.capabilities import has_capability
    from provisa.api.admin.schema_helpers import _get_pool
    from provisa.api.mcp.table_edit import read_table, table_input

    if has_capability(info, "view_governance"):
        return input_
    pool = await _get_pool()
    async with pool.acquire() as conn:
        stored = await stored_table(conn, input_)
    if stored is None:
        return input_
    held = {c.name: c for c in table_input(await read_table(stored["id"])).columns}
    for column in input_.columns:
        if column.name not in held:
            continue
        for field in GOVERNANCE_FIELDS:
            if getattr(column, field) in (None, [], ""):
                setattr(column, field, getattr(held[column.name], field))
    return input_


async def require_table_save(info: Any, input_: Any) -> tuple[bool, Any]:  # REQ-1944
    """The gate on ``update_table``: whether the caller saves as the table's editor, and the input
    to save.

    A ``table_registration`` holder saves ``input_`` into ``input_.domain_id`` as before. A caller
    without it may save only a change to columns' hiding fields, and only while holding one of the
    governance rights -- which right each field needs, in which domain, is
    :func:`table_hiding_refusal`'s question -- and what is saved is :func:`steward_input`'s
    stored table with those fields applied. Anything else the save sets is refused by name.
    """
    from provisa.api.admin.capabilities import has_capability, require_capability

    # REQ-1958: first, so that whichever way the save goes on from here it carries the stored
    # grant lists and masks of a caller who is not shown them.
    input_ = await keep_governance(info, input_)
    if has_capability(info, "table_registration"):
        require_capability(info, "table_registration", domain_id=input_.domain_id)
        return True, input_
    if not any(has_capability(info, right) for right in GOVERNANCE_RIGHTS):
        require_capability(info, "table_registration")
    saved, changes = await steward_input(input_)
    if changes:
        raise PermissionError(
            "Missing capability: 'table_registration' -- without it a table save may change "
            "only how its columns are hidden; this one also changes " + ", ".join(changes)
        )
    return False, saved


async def table_hiding_refusal(
    info: Any, conn: "Connection", model: Any, *, editor: bool
) -> Any:  # REQ-1943, REQ-1944
    """:func:`hiding_refusal` for a GraphQL save, as the ``MutationResult`` it answers with."""
    from provisa.api.admin.capabilities import _identity_from_info
    from provisa.api.admin.types import MutationResult
    from provisa.api.app import state

    refusal = await hiding_refusal(
        conn, model, identity=_identity_from_info(info), state=state, editor=editor
    )
    if refusal is None:
        return None
    return MutationResult(success=False, message=refusal, code="schema.hiding_right_required")


def require_hiding_editor_request(request: Any) -> None:  # REQ-1944
    """REST gate for the surfaces that edit or check a column's fake: the table's editor, or a
    holder of a governance right (whose domain the save itself then checks)."""
    from provisa.api.admin.capabilities import has_capability_request, require_capability_request

    if any(has_capability_request(request, right) for right in GOVERNANCE_RIGHTS):
        return
    require_capability_request(request, "table_registration")
