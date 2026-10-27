# Copyright (c) 2026 Kenneth Stott
# Canary: ac182da0-b085-4c3d-ad6d-bcd236aaa1f7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A steward's read of one registered table: the table detail's Preview and Profile.

Both read the table as their caller — the caller's role, its row filters, its masks, the columns
it may see — through the one pipeline, and both need the right to register tables in the
table's own domain.

A table in service is read as any statement reads it. A DRAFT is out of service: it is in no
role's schema and every statement that names it is refused (REQ-1921). The people who decide to
release it have to see it first, so this module — and nothing else — reads a draft: for a caller
who holds the right ``set_table_draft`` itself requires, it writes the one statement (``SELECT *``
of that table, with a row limit; the caller supplies no SQL), and governs and routes it against
the caller's role *with that one table added*, as the role would see it once released. It is
read live, in the table's own region, and nothing is kept of it: no kept plan, no cached
response, no replica. The table is still in no schema, and the same words sent as an ordinary
statement are refused as before — by the same caller too.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from provisa.api.errors import ApiError
from provisa.api.role_surface import RoleSurface, role_surface


class _WithDraft:
    """The application state as a statement that may read ONE draft sees it: everything is the
    real state's, except that the table is among the registered tables, the caller's role has
    it in its compilation context, and no plan or response the read produces is kept anywhere
    the real state reads from.

    The table's ROWS are reached the way a table in service's are. Where the engine reads its
    source in place, it is read there. Where the engine reaches it only through a replica, the
    read asks for the replica and waits on it as any read does (``query_residency``, which
    judges the draft this view names) and is addressed at it by ``routes`` — the published
    routes and this draft's own. The replica is kept: a draft is offered to no reader, and it
    may hold its data. The engine face is bound to this view, so only this read is addressed
    there."""

    def __init__(
        self,
        state: Any,
        draft: dict,
        role_id: str,
        surface: RoleSurface,
        routes: Any,
        view_body: str | None,
    ) -> None:
        from provisa.cache.store import NoopCacheStore
        from provisa.compiler.compiled_query_cache import CompiledQueryCache

        self._state = state
        self.tables = [*state.tables, draft]
        self.surfaces = {**state.surfaces, role_id: surface}
        # Its own, empty, and dropped with this object: a plan or a validation kept for this
        # read is never found by a statement governed against the real state.
        self.compiled_query_cache = CompiledQueryCache(expires=False)
        self.routing_cache = CompiledQueryCache()
        self.response_cache_store = NoopCacheStore()
        self.previewed_draft_id = draft["id"]
        if view_body is not None:
            # A draft VIEW is in no published view map, so no other statement expands it. This
            # read expands it as a view in service is expanded: its body, cut to what the
            # caller may read of every table it names (mv/view_read.py).
            self.view_sql_map = {**state.view_sql_map, draft["table_name"]: view_body}
        self.replica_routes = routes
        real = state.federation_engine
        self.federation_engine = type(real)(real.engine, self)

    @property
    def contexts(self) -> Any:
        # Views of this read's surfaces: a role is built when its part is read, not all of them.
        from provisa.api.role_surface import SurfacePart

        return SurfacePart(self, "context")

    @property
    def rls_contexts(self) -> Any:
        from provisa.api.role_surface import SurfacePart

        return SurfacePart(self, "rls")

    def __getattr__(self, name: str) -> Any:
        return getattr(self._state, name)


def _quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def table_sql_ref(table: dict) -> str:
    """The table as a statement names it: its domain's SQL name, then its own."""
    from provisa.compiler.naming import domain_to_sql_name

    own = table.get("alias") or table["table_name"]
    return f"{_quoted(domain_to_sql_name(table['domain_id']))}.{_quoted(own)}"


def _draft(state: Any, table_id: int) -> dict | None:
    for table in state.role_build_inputs["draft_tables"]:
        if table["id"] == table_id:
            return table
    return None


def _in_service(state: Any, table_id: int) -> dict | None:
    for table in state.tables:
        if table["id"] == table_id:
            return table
    return None


def _surface_with(state: Any, role_id: str, draft: dict) -> RoleSurface:
    """The role's surface with ``draft`` in its compilation context — built as the role build
    builds it, from this generation's inputs, with the table moved from the drafts to the
    tables. Its columns' types are the registered ones."""
    from provisa.api.app_loaders import build_context, schema_input_for
    from provisa.compiler.introspect import ColumnMetadata

    surface = role_surface(state, role_id)
    if surface is None:
        raise ApiError(
            400, "data.no_schema_for_role", f"No schema for role {role_id!r}", role_id=role_id
        )
    inputs = state.role_build_inputs
    si = schema_input_for(
        state, state.roles[role_id], [*inputs["tables"], draft], inputs["metrics"]
    )
    si = replace(
        si,
        draft_tables=[t for t in inputs["draft_tables"] if t["id"] != draft["id"]],
        column_types={
            **inputs["column_types"],
            draft["id"]: [
                ColumnMetadata(
                    column_name=c["column_name"], data_type=c["data_type"], is_nullable=True
                )
                for c in draft["columns"]
            ],
        },
    )
    ctx = build_context(si)
    ctx.refusal = surface.context.refusal
    ctx.kept = dict(surface.context.kept)
    return replace(surface, context=ctx)


def _view_body(state: Any, draft: dict) -> str | None:
    """What a draft VIEW's reference becomes for its steward's read; None for a table.

    A view that is not materialized, and an ordinary materialized one, is its SQL: read live, as
    a materialized view in service is while its build is not fresh. A materialized draft view's
    build is kept registered and disabled (app_rebuild): nothing schedules it, so this read asks
    for it — started here and not waited for — and the view is landed for its release.

    A bitemporal materialized view has no live reading: its rows are what its stored log says
    was true (mv/bitemporal.py). Until that log has been built once the read answers as any
    read does whose copy is still being built; after, it is read from the log."""
    if not draft.get("view_sql"):
        return None
    from provisa.api.app_rebuild import compile_view_sql_to_physical

    # Physical, as every published view's entry is (app_rebuild): the engine is handed the
    # tables' own addresses, not their model names.
    body = compile_view_sql_to_physical(draft["view_sql"].rstrip().rstrip(";"), state.view_context)
    # A draft view is materialized exactly when its build is registered (app_rebuild).
    mv = state.mv_registry.get(f"view-{draft['table_name']}")
    if mv is None:
        return body
    from provisa.core.connection_loop import spawn_background
    from provisa.mv.models import MVStatus
    from provisa.mv.refresh import refresh_mv

    if mv.status not in (MVStatus.FRESH, MVStatus.REFRESHING):
        # On a worker of its own: this read's loop ends with the read, and a task left on it
        # would never run to its end.
        spawn_background(
            refresh_mv(
                state.federation_engine,
                mv,
                state.mv_registry,
                store=state.model_db,
                ledger=state.tenant_db,
            ),
            name=f"view-build:{draft['table_name']}",
        )
    if mv.bitemporal is None:
        return body
    if mv.last_refresh_at is None:
        from provisa.federation.replica_state import ReplicaBuilding

        raise ReplicaBuilding(draft["table_name"], 0.0)
    from provisa.mv.bitemporal import view_read_sql

    stored = mv.stored_ref()
    return view_read_sql(stored, mv.bitemporal)


def _statement(table: dict, limit: int | None, request: Any) -> tuple[str, list]:
    """The one statement the read is: the table's first ``limit`` rows, or the page the viewer
    asked for (``request``: its filters, sorts, grouping and page), written here from the
    table's registration with every value bound (admin/viewer_statement.py)."""
    from provisa.api.admin.viewer_statement import viewer_statement

    if request is not None:
        return viewer_statement(table, table_sql_ref(table), request)
    return f"SELECT * FROM {table_sql_ref(table)} LIMIT {int(limit or 0)}", []


async def read_as_caller(
    state: Any, table_id: int, role_id: str, *, limit: int | None = None, request: Any = None
) -> Any:
    """Rows of the table as ``role_id`` reads it — a ``QueryResult``: its first ``limit`` rows,
    or the page of it the viewer's ``request`` describes. Either way the statement is written
    here; the caller sends none.

    The caller has already been checked for the right in the table's domain
    (:func:`table_for_steward`)."""
    from provisa.compiler.directives import NO_CACHE_HINT
    from provisa.federation.query_residency import require_draft_read_in_its_region
    from provisa.federation.registry_view import registered_sources
    from provisa.federation.replica_routing import routes_with_draft
    from provisa.pgwire._pipeline import (
        _execute_plan,
        _govern_and_route,
        _govern_and_route_compiled,
    )

    table = _in_service(state, table_id)
    if table is not None:
        sql, params = _statement(table, limit, request)
        plan = (
            await _govern_and_route(sql, role_id, params=params)
            if params
            else await _govern_and_route(sql, role_id)
        )
        return await _execute_plan(plan, state)

    draft = _draft(state, table_id)
    if draft is None:
        raise ApiError(404, "schema.table_not_found", f"Table {table_id} not found", table=table_id)
    # Refused by name before any statement is built: a column or operator the table has not.
    sql, params = _statement(draft, limit, request)
    # Where a regional table may be read is the regions' rule, in its own module.
    require_draft_read_in_its_region(draft["table_name"], draft.get("region"))
    # A built-in source (a view's) is not among the registered sources and owns no replica
    # (registry_view.registered_sources): a draft of one is read under the published routes.
    source = next((s for s in await registered_sources(state) if s.id == draft["source_id"]), None)
    routes = state.replica_routes if source is None else routes_with_draft(state, draft, source)
    seen = _WithDraft(
        state,
        draft,
        role_id,
        _surface_with(state, role_id, draft),
        routes,
        _view_body(state, draft),
    )
    plan = await _govern_and_route_compiled(
        sql,
        role_id,
        exec_params=params or None,
        state=seen,
        cache_hint=NO_CACHE_HINT,
        sdl_joins=False,
    )
    return await _execute_plan(plan, seen)


def table_for_steward(request: Any, state: Any, table_id: int) -> dict:
    """The registered table ``table_id`` — in service or draft — for a caller who may register
    tables in its domain: the right a table's release needs, checked in the table's own domain.
    Refused otherwise, as the release would be."""
    from provisa.api.admin.capabilities import require_capability_request, require_domain_request

    table = _in_service(state, table_id) or _draft(state, table_id)
    if table is None:
        raise ApiError(404, "schema.table_not_found", f"Table {table_id} not found", table=table_id)
    require_capability_request(request, "table_registration")
    require_domain_request(request, table["domain_id"])
    return table
