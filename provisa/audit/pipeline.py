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
from collections.abc import Callable, Iterable
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
    # The registered tables the statement touches — or a function that resolves them, called on
    # the audit writer's thread when resolving costs a parse the request thread should not pay.
    table_ids: "list[int] | Callable[[], tuple[int, ...]]"
    started: float
    # Provenance (provisa/audit/provenance.py), taken where the statement was governed: the model
    # stamp in force, and what was enforced — or a resolver the writer thread calls.
    model_stamp: int | None
    enforced: "dict[str, Any] | Callable[[], dict[str, Any]]"


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
    query_text: str,
    role_id: str,
    tree: "exp.Expr",
    gov_ctx: "GovernanceContext",
    model_stamp: int | None,
) -> PendingAudit | None:
    """Open an audit record for a statement, or None when nothing user-initiated is running.

    An unbound identity means no acting principal — startup seeding, schema rebuild, a
    materialization refresh. Those are not user queries and are deliberately not audited; see
    :mod:`provisa.audit.context`.
    """
    identity = current_audit_identity()
    if identity is None:
        return None
    from provisa.audit.provenance import enforced_summary

    table_ids = resolve_table_ids(tree, gov_ctx)
    return PendingAudit(
        user_id=identity.user_id,
        surface=identity.surface,
        role_id=role_id,
        query_text=query_text,
        table_ids=table_ids,
        started=time.monotonic(),
        model_stamp=model_stamp,
        enforced=enforced_summary(gov_ctx, table_ids, tree),
    )


async def write_audit(
    pending: PendingAudit | None,
    status_code: int,
    state: Any = None,
    *,
    route: str | None = None,
    row_count: int | None = None,
    route_reason: str | None = None,
    sources: "Iterable[str]" = (),
    data_age: dict[str, Any] | None = None,
) -> None:
    """Record ``pending``'s row with its outcome. A None record is a statement with no acting
    principal (see :func:`begin_audit`) and writes nothing.

    Awaitable for its callers' sake; it does not wait on the database — see :func:`enqueue_audit`."""
    enqueue_audit(
        pending,
        status_code,
        state,
        route=route,
        row_count=row_count,
        route_reason=route_reason,
        sources=sources,
        data_age=data_age,
    )


def enqueue_audit(
    pending: PendingAudit | None,
    status_code: int,
    state: Any = None,
    *,
    route: str | None = None,
    row_count: int | None = None,
    route_reason: str | None = None,
    sources: "Iterable[str]" = (),
    data_age: dict[str, Any] | None = None,
) -> None:
    """Hand ``pending``'s finished row to the audit writer (:mod:`provisa.audit.writer`) and
    return: the INSERT, and the active-hour meter that rides the same seam (REQ-1454), happen on
    the writer's thread."""
    record = build_audit_record(
        pending,
        status_code,
        state,
        route=route,
        row_count=row_count,
        route_reason=route_reason,
        sources=sources,
        data_age=data_age,
    )
    if record is not None:
        from provisa.audit.writer import audit_writer

        audit_writer().enqueue(record)


def complete_audit_record(record: Any, started: float, status_code: int, row_count: int) -> None:
    """Enqueue a record built ahead of its statement's outcome (a streamed result's, built when
    the stream was opened) now that the drain has ended: its status, the rows delivered, and a
    duration and time that cover the delivery. Any thread may call this — everything the record
    needed from the request's context was resolved when it was built."""
    import dataclasses
    from datetime import datetime, timezone

    from provisa.audit.writer import audit_writer

    audit_writer().enqueue(
        dataclasses.replace(
            record,
            status_code=status_code,
            row_count=row_count,
            duration_ms=int((time.monotonic() - started) * 1000),
            logged_at=datetime.now(timezone.utc),
        )
    )


def _with_acting_roles(pending: PendingAudit, state: Any) -> Any:
    """``pending``'s enforced summary, naming every role a meta-role acted as (REQ-1620): the record
    shows exactly which rights the statement was made with."""
    from provisa.security.meta_role import is_meta_role_id

    if not is_meta_role_id(pending.role_id):
        return pending.enforced
    members = list(state.meta_roles[pending.role_id])
    enforced = pending.enforced
    if callable(enforced):
        resolve = enforced
        return lambda: {**resolve(), "acting_roles": members}
    return {**enforced, "acting_roles": members}


def build_audit_record(
    pending: PendingAudit | None,
    status_code: int,
    state: Any = None,
    *,
    route: str | None = None,
    row_count: int | None = None,
    route_reason: str | None = None,
    sources: "Iterable[str]" = (),
    data_age: dict[str, Any] | None = None,
) -> Any:
    """``pending``'s audit record, or None when there is no acting principal. Everything that
    depends on the request's context — the org, its tenant database, its encryption key, the UDF
    correlation id — is resolved HERE, on the request thread.

    The meter rides the record, after the ``pending is None`` guard, so it inherits the audit's
    definition of a user query exactly: no surface can execute governed SQL and bill nothing."""
    if pending is None:
        return None
    if state is None:
        from provisa.api.app import state  # type: ignore[assignment]

    from datetime import datetime, timezone

    from provisa.audit.writer import AuditRecord
    from provisa.core.environments import PROD
    from provisa.core.request_context import active_env, current_env, current_org
    from provisa.encryption.runtime import encryption_service
    from provisa.federation.replica_hot import count_scope
    from provisa.otel_compat import current_udf_correlation_id

    record_db = state.record_db
    if record_db is None:
        raise RuntimeError(
            "audit write has no record database — query_audit_log lives in the org's record "
            "in this region and the org runtime must be bound before a governed statement runs"
        )
    # The tenant IS the org (REQ-594): `current_org` when a surface bound one, the default org
    # otherwise — the same resolution AppState._active_runtime uses to pick the tenant_db this row
    # is about to land in, so the recorded tenant always names the schema holding the row. The
    # meta-RLS ContextVar that used to be read here is set by nothing in production, so every
    # audit row carried a NULL tenant and every ops report showed a NULL tenant column.
    org_id = current_org.get() or state.org_id
    return AuditRecord(
        record_db=record_db,
        tenant_id=org_id,
        user_id=pending.user_id,
        role_id=pending.role_id,
        query_text=pending.query_text,
        table_ids=(pending.table_ids if callable(pending.table_ids) else tuple(pending.table_ids)),
        source=pending.surface,
        status_code=status_code,
        duration_ms=int((time.monotonic() - pending.started) * 1000),
        logged_at=datetime.now(timezone.utc),
        # REQ-886: a row written under a UDF's minted session joins back to its trace.
        trace_id=current_udf_correlation_id(),
        encryption=encryption_service(),
        # REQ-1454: the org's clock hour is marked active for this statement. No control
        # plane (single-tenant / desktop) = no org registry, no subscription, nothing to meter.
        meter_pool=state.admin_db,
        meter_org=org_id,
        model_env=active_env(),
        # REQ-826: the statement's tables count toward Hot replication in this org environment.
        hot_counts=state.hot_counts,
        hot_scope=count_scope(org_id, current_env.get() or PROD),
        route=route,
        row_count=row_count,
        model_stamp=pending.model_stamp,
        enforced=_with_acting_roles(pending, state),
        route_reason=route_reason,
        sources=tuple(sources),
        data_age=data_age,
    )


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
    if state is None:
        from provisa.api.app import state  # type: ignore[assignment]
    await write_audit(
        PendingAudit(
            user_id=identity.user_id,
            surface=identity.surface,
            role_id=role_id,
            query_text=query_text,
            table_ids=table_ids,
            started=time.monotonic(),
            model_stamp=state.model_stamp,
            # A refused statement had nothing enforced on its rows: it was given none.
            enforced={},
        ),
        403,
        state,
    )
