# Copyright (c) 2026 Kenneth Stott
# Canary: 91c7b204-58ea-4d3f-a6c1-7be0f2d9a834
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The audit write the ONE query pipeline performs.

REQ-074/REQ-1386: every governed statement appends a ``query_audit_log`` row, and the ops reports
(`usage_ranking`, `deprecated_usage`, `pii_access`, `join_hotspots`, `policy_denials`,
`surface_mix`, `query_health`) read nothing else. Because the pipeline is the only code that may
execute governed SQL, the row is written there — once — and every surface inherits it rather than
each transport re-implementing an audit call it can silently omit.

Two moments are recorded:

    denial     governance refused the statement (403), written before the PermissionError leaves
               the planner — this is what ``policy_denials`` reports on
    completion  the statement reached a terminal, with the real status and wall-clock duration

The row lands in the ORG's tenant database (``query_audit_log`` lives in ``org_<id>``, created by
:func:`provisa.audit.query_log.init_audit_schema`) and the query text is encrypted (REQ-689).
"""

# Requirements: REQ-074, REQ-689, REQ-1386

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from provisa.audit.context import current_audit_identity

if TYPE_CHECKING:
    import sqlglot.expressions as exp

    from provisa.compiler.stage2 import GovernanceContext

log = logging.getLogger(__name__)


@dataclass
class PendingAudit:
    """A statement in flight — everything the row needs except its outcome."""

    user_id: str
    surface: str
    role_id: str
    query_text: str
    table_ids: list[int]
    started: float


def resolve_table_ids(tree: "exp.Expr", gov_ctx: "GovernanceContext") -> list[int]:
    """The registered_tables ids this statement reads, in first-reference order.

    ``Expr``, not ``Expression``: the tree handed here is whatever ``sqlglot.parse_one`` returned
    for the statement the pipeline is auditing — including the ``Block`` a multi-statement string
    parses into — and the walk below needs nothing but ``find_all``, which the base declares.

    ``gov_ctx.table_map`` is the same name→id resolution governance and the domain-access check
    use, keyed by both the semantic ``domain.table`` form and the bare name, so a reference that
    governance could resolve resolves here too. A reference it cannot resolve is not a registered
    table (a CTE alias, an engine-internal relation) and contributes no usage row.
    """
    import sqlglot.expressions as exp_mod

    ids: list[int] = []
    for tbl in tree.find_all(exp_mod.Table):
        qualified = f"{tbl.db}.{tbl.name}" if tbl.db else tbl.name
        table_id = gov_ctx.table_map.get(qualified)
        if table_id is None:
            table_id = gov_ctx.table_map.get(tbl.name)
        if table_id is not None and table_id not in ids:
            ids.append(table_id)
    return ids


def begin_audit(
    query_text: str, role_id: str, tree: "exp.Expr", gov_ctx: "GovernanceContext"
) -> PendingAudit | None:
    """Open an audit record for a statement, or None when nothing user-initiated is running.

    An unbound identity means no acting principal — startup seeding, schema rebuild, a
    materialization refresh. Those are not user queries and are deliberately not audited; see
    :mod:`provisa.audit.context`.
    """
    identity = current_audit_identity()
    if identity is None:
        return None
    return PendingAudit(
        user_id=identity.user_id,
        surface=identity.surface,
        role_id=role_id,
        query_text=query_text,
        table_ids=resolve_table_ids(tree, gov_ctx),
        started=time.monotonic(),
    )


async def write_audit(pending: PendingAudit | None, status_code: int, state: Any = None) -> None:
    """Append ``pending``'s row with its outcome. A None record is a statement with no acting
    principal (see :func:`begin_audit`) and writes nothing."""
    if pending is None:
        return
    if state is None:
        from provisa.api.app import state  # type: ignore[assignment]

    from provisa.api.org_runtime import current_org
    from provisa.audit.query_log import log_query
    from provisa.encryption.runtime import encryption_service

    tenant_db = state.tenant_db
    if tenant_db is None:
        raise RuntimeError(
            "audit write has no tenant database — query_audit_log lives in the org's tenant "
            "schema and the org runtime must be bound before a governed statement runs"
        )
    # The tenant IS the org (REQ-594): `current_org` when a surface bound one, the default org
    # otherwise — the same resolution AppState._active_runtime uses to pick the tenant_db this row
    # is about to land in, so the recorded tenant always names the schema holding the row. The
    # meta-RLS ContextVar that used to be read here is set by nothing in production, so every
    # audit row carried a NULL tenant and every ops report showed a NULL tenant column.
    await log_query(
        tenant_db,
        tenant_id=current_org.get() or state.org_id,
        user_id=pending.user_id,
        role_id=pending.role_id,
        query_text=pending.query_text,
        table_ids=pending.table_ids,
        source=pending.surface,
        status_code=status_code,
        duration_ms=int((time.monotonic() - pending.started) * 1000),
        encryption=encryption_service(),
    )
    await _meter_active_hour(state)


async def _meter_active_hour(state: Any) -> None:
    """Mark the org's current clock hour active (REQ-1454).

    Placed on the audit seam, and after its ``pending is None`` guard, so the meter inherits the
    audit's definition of a user query exactly: every protocol reaches this point through
    ``finalize_audit``, and no new surface can be added that executes governed SQL and bills
    nothing. The bill counts hours, so N statements in an hour collapse to one row.
    """
    from provisa.api.org_runtime import current_org
    from provisa.core.commerce import meter_op

    pool = state.admin_db
    if pool is None:
        # Single-tenant / desktop: no control plane, so no org registry, no subscription and
        # nothing to meter. The SaaS deployment always has one.
        return
    await meter_op(pool, current_org.get() or state.org_id)


async def write_denial(
    query_text: str,
    role_id: str,
    tree: "exp.Expr | None",
    gov_ctx: "GovernanceContext | None",
    state: Any = None,
) -> None:
    """Record a governance refusal (403) — what ``policy_denials`` reports on.

    The tree/context are absent when the statement was refused before governance could resolve
    anything (an unknown role); the row then carries no table ids, which is the truth about a
    statement that never resolved a table.
    """
    identity = current_audit_identity()
    if identity is None:
        return
    table_ids = resolve_table_ids(tree, gov_ctx) if tree is not None and gov_ctx is not None else []
    await write_audit(
        PendingAudit(
            user_id=identity.user_id,
            surface=identity.surface,
            role_id=role_id,
            query_text=query_text,
            table_ids=table_ids,
            started=time.monotonic(),
        ),
        403,
        state,
    )
