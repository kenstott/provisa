# Copyright (c) 2026 Kenneth Stott
# Canary: 7c1f3e58-9a24-4b6d-8e03-5d2a9f6c4b17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The audit record of a GraphQL request (REQ-074/REQ-1386).

``/data/graphql`` executes its compiled fields at its own terminal, not through
``_execute_plan``, so it hands its record to the same audit seam every other surface uses
(:func:`provisa.audit.pipeline.enqueue_audit`) itself — one record per request: a query, a query
answered from a cached plan or a cached response, a mutation.

The request thread only enqueues. Which registered tables the document touches is resolved on the
audit writer's thread, from the document and the role's compilation context, and kept per
(schema generation, role, document) — a repeated query resolves with one dictionary lookup.
"""

# Requirements: REQ-074, REQ-1386

from __future__ import annotations

import collections
import threading
from typing import TYPE_CHECKING, Any

from provisa.audit.context import current_audit_identity
from provisa.audit.pipeline import PendingAudit, enqueue_audit

if TYPE_CHECKING:
    from provisa.audit.context import RequestAudit
    from provisa.compiler.sql_types import CompilationContext, TableMeta

_RESOLVED_MAX = 4096
_resolved: "collections.OrderedDict[tuple[str, int, str, str], tuple[int, ...]]" = (
    collections.OrderedDict()
)
_resolved_lock = threading.Lock()


def _root_table(name: str, mutation: bool, ctx: "CompilationContext") -> "TableMeta | None":
    if not mutation:
        return ctx.tables.get(name)
    from provisa.compiler.mutation_gen import _get_mutation_meta

    try:
        return _get_mutation_meta(name, ctx)[2]
    except ValueError:
        return None  # an action or other non-table mutation field: no registered table


def document_table_ids(query: str, ctx: "CompilationContext") -> tuple[int, ...]:
    """The registered tables a GraphQL document reads or writes, in first-reference order: each
    root field's table, and the target of every relationship field selected beneath it. A field
    that names no registered table (an action, an aggregate wrapper, ``__typename``) contributes
    nothing — the same rule the SQL surfaces' ``resolve_table_ids`` applies."""
    from graphql import GraphQLSyntaxError, parse
    from graphql.language import ast as gql

    try:
        document = parse(query)
    except GraphQLSyntaxError:
        return ()  # a document that does not parse reached no table; its row records the refusal
    fragments = {
        d.name.value: d for d in document.definitions if isinstance(d, gql.FragmentDefinitionNode)
    }
    ids: list[int] = []

    def _walk(selection_set: Any, table: "TableMeta | None", mutation: bool) -> None:
        if selection_set is None:
            return
        for sel in selection_set.selections:
            if isinstance(sel, gql.FragmentSpreadNode):
                fragment = fragments.get(sel.name.value)
                if fragment is not None:
                    _walk(fragment.selection_set, table, mutation)
            elif isinstance(sel, gql.InlineFragmentNode):
                _walk(sel.selection_set, table, mutation)
            elif isinstance(sel, gql.FieldNode):
                name = sel.name.value
                if table is None:
                    target = _root_table(name, mutation, ctx)
                else:
                    join = ctx.joins.get((table.type_name, name))
                    # A non-relationship field with a selection set (``returning``, ``nodes``)
                    # stays on the same table.
                    target = join.target if join is not None else table
                if target is not None and target.table_id not in ids:
                    ids.append(target.table_id)
                _walk(sel.selection_set, target, mutation)

    for definition in document.definitions:
        if isinstance(definition, gql.OperationDefinitionNode):
            _walk(
                definition.selection_set,
                None,
                definition.operation == gql.OperationType.MUTATION,
            )
    return tuple(ids)


def _kept_table_ids(key: tuple[str, int, str, str], ctx: "CompilationContext") -> tuple[int, ...]:
    with _resolved_lock:
        ids = _resolved.get(key)
        if ids is not None:
            _resolved.move_to_end(key)
            return ids
    ids = document_table_ids(key[3], ctx)
    with _resolved_lock:
        _resolved[key] = ids
        while len(_resolved) > _RESOLVED_MAX:
            _resolved.popitem(last=False)
    return ids


def audit_graphql_request(
    state: Any,
    role_id: str,
    query: str,
    ctx: "CompilationContext",
    status_code: int,
    started: float,
    outcome: "RequestAudit | None" = None,
) -> None:
    """Enqueue the audit record of one GraphQL request. Nothing is parsed and no database is
    touched on the calling thread; a request with no acting principal records nothing.

    ``outcome`` is what the request's own terminal noted (``provisa.audit.context``): the route
    its fields were answered by and the rows returned. A request whose statement already wrote a
    row of its own — an action field's governed statement — is not recorded a second time: that
    row is the request's record."""
    identity = current_audit_identity()
    if identity is None:
        return
    if outcome is not None and outcome.statements_audited:
        return
    ok = status_code == 200  # noqa: PLR2004 - HTTP OK
    key = (str(state.schema_boot_id), int(state.schema_version), role_id, query)
    enqueue_audit(
        PendingAudit(
            user_id=identity.user_id,
            surface=identity.surface,
            role_id=role_id,
            query_text=query,
            table_ids=lambda: _kept_table_ids(key, ctx),
            started=started,
        ),
        status_code,
        state,
        route=outcome.route() if outcome is not None and ok else None,
        row_count=outcome.rows if outcome is not None and ok else None,
    )
