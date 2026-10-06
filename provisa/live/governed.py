# Copyright (c) 2026 Kenneth Stott
# Canary: fd9bf010-e780-48af-b1b3-a8fb0afad9be
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Live delivery is governed per subscriber key (REQ-286, REQ-336, REQ-1266).

Every row a live stream delivers -- a poll's rows, a change notification's row -- is read through
the ONE governed pipeline as its subscriber: the role, and the session values its rules read.
Row rules, column visibility and masks apply exactly as to a query; nothing is fanned out from a
raw read of the table.

The governance key is everything governance resolves against: the org, the role, and the session
values (``session_vars_for``: the role's constants, overlaid by what the request bound). Equal keys
see equal rows, so a poll is shared by the subscribers of one key and never across keys.
"""

# Requirements: REQ-286, REQ-336, REQ-1266

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from provisa.api.errors import ApiError

if TYPE_CHECKING:
    from provisa.compiler.sql_types import TableMeta


@dataclass(frozen=True)
class GovernanceKey:
    """What a live delivery is governed as: an org's role, with the session values rules read."""

    org_id: str
    role_id: str
    session_vars: tuple[tuple[str, str], ...]

    @property
    def digest(self) -> str:
        """A stable short name for the key, for job ids and bookkeeping rows."""
        raw = repr((self.org_id, self.role_id, self.session_vars)).encode()
        return hashlib.sha256(raw).hexdigest()[:16]


def subscriber_key(role_id: str | None) -> GovernanceKey:
    """The key of the bound request's subscriber: its role in the bound org, and the session
    values governance would resolve its rules against for this request."""
    from provisa.api.app import state
    from provisa.core.request_context import require_current_org, session_vars_for

    org_id = require_current_org()
    if not role_id:
        raise ApiError(403, "subscribe.role_required", "A live subscription acts as a role")
    role = state.roles.get(role_id)
    if role is None:
        raise ApiError(403, "subscribe.unknown_role", f"Unknown role {role_id!r}", role=role_id)
    return GovernanceKey(org_id, role_id, tuple(sorted(session_vars_for(role).items())))


def output_key(role_id: str) -> GovernanceKey:
    """The key a configured output (a Kafka sink) publishes as: its named role in the bound org,
    with that role's own session values -- no request's, since no request opens it."""
    from provisa.api.app import state
    from provisa.core.request_context import (
        require_current_org,
        reset_session_vars,
        session_vars_for,
        set_session_vars,
    )

    org_id = require_current_org()
    role = state.roles.get(role_id)
    if role is None:
        raise LookupError(f"live output role {role_id!r} is not a role of org {org_id!r}")
    token = set_session_vars({})
    try:
        values = session_vars_for(role)
    finally:
        reset_session_vars(token)
    return GovernanceKey(org_id, role_id, tuple(sorted(values.items())))


def table_meta(table_id: int) -> "TableMeta":
    """The bound org's model table ``table_id``, from the model-wide context."""
    from provisa.api.app import state

    ctx = state.view_context
    if ctx is None:
        raise LookupError("the org's model is not built: no model-wide context")
    for meta in ctx.tables.values():
        if meta.table_id == table_id:
            return meta
    raise LookupError(f"table {table_id} is not in the org's model")


def table_ref(meta: "TableMeta") -> str:
    """The semantic reference a governed statement names the table by."""
    from provisa.compiler.sql_rewrite import semantic_ref

    return semantic_ref(meta)


def primary_key(meta: "TableMeta") -> list[str]:
    """The table's primary key columns, from the model."""
    from provisa.api.app import state

    ctx = state.view_context
    assert ctx is not None  # table_meta resolved this table from it
    return list(ctx.pk_columns.get(meta.table_id) or [])


async def governed_rows(
    sql: str, key: GovernanceKey, params: list | None = None
) -> list[dict[str, Any]]:
    """Run ``sql`` through the one governed pipeline as ``key``, in its org."""
    from provisa.core.request_context import reset_current_org, set_current_org
    from provisa.pgwire._pipeline import _execute_plan, _govern_and_route

    token = set_current_org(key.org_id)
    try:
        plan = await _govern_and_route(
            sql, key.role_id, session_vars=dict(key.session_vars), params=params
        )
        result = await _execute_plan(plan)
    finally:
        reset_current_org(token)
    return [dict(zip(result.column_names, row)) for row in result.rows]
