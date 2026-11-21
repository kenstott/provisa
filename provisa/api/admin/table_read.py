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

Both read the table as their caller -- the caller's role, its row filters, its masks, the columns
it may see -- through the one pipeline, and both need the right to register tables in the
table's own domain.

A table in service is read as any statement reads it. A WITHDRAWN table is out of production: it
is in no role's schema and no statement a caller sends names it (REQ-1921). The people who decide
to put it back may preview it, so this module -- and nothing else -- reads a withdrawn table: for
a caller who holds table registration in its domain, it writes the one statement (``SELECT *`` of
that table, with a row limit, or the viewer's page; the caller supplies no SQL), and governs and
routes it against the caller's role *with that one table added*, as the role would see it once
put back. It is read live from its source, in the table's own region (a withdrawn table keeps no
replica), and no plan or response of it is kept. Every such read is recorded as a withdrawn
preview naming the table it read.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from provisa.api.errors import ApiError
from provisa.api.role_surface import RoleSurface, role_surface
from provisa.security.meta_role import is_meta_role_id


class _Previewing:
    """The application state as a steward's preview of withdrawn tables sees it: everything is
    the real state's, except that the withdrawn tables are among the registered tables, the
    caller's role has them in its compilation context, and no plan or response the read produces
    is kept anywhere the real state reads from.

    A withdrawn table keeps no replica, so its rows are read live from its source, in place
    (``routes``: the published routes with these tables read live). The engine face is bound to
    this view, so only this read is addressed there."""

    def __init__(
        self,
        state: Any,
        withdrawn: list[dict],
        role_id: str,
        surface: RoleSurface,
        routes: Any,
        view_bodies: dict[str, str],
    ) -> None:
        from provisa.cache.store import NoopCacheStore
        from provisa.compiler.compiled_query_cache import CompiledQueryCache

        self._state = state
        self.tables = [*state.tables, *withdrawn]
        self.surfaces = {**state.surfaces, role_id: surface}
        # Its own, empty, and dropped with this object: a plan or a validation kept for this
        # read is never found by a statement governed against the real state.
        self.compiled_query_cache = CompiledQueryCache(expires=False)
        self.routing_cache = CompiledQueryCache()
        self.response_cache_store = NoopCacheStore()
        self.previewed_withdrawn_ids = frozenset(t["id"] for t in withdrawn)
        if view_bodies:
            # A withdrawn VIEW is in no published view map, so no other statement expands it.
            # This read expands it as a view in service is expanded: its body, cut to what the
            # caller may read of every table it names (mv/view_read.py).
            self.view_sql_map = {**state.view_sql_map, **view_bodies}
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


def withdrawn_table(state: Any, table_id: int) -> dict | None:
    """The withdrawn table ``table_id`` (REQ-1921), or None when no withdrawn table has that id.
    It is read only by its steward's preview: it is in no role's context, so no statement a
    caller sends names it. Whom it is shown to is asked elsewhere (:func:`table_for_steward`)."""
    for table in state.role_build_inputs["withdrawn_tables"]:
        if table["id"] == table_id:
            return table
    return None


def _require_previewable(state: Any, table: dict, source: Any) -> None:
    """REQ-1921: a withdrawn table keeps no replica and a withdrawn source is attached by no
    engine, so a preview of one is refused, saying why, where the engine could reach its rows
    only through what it does not keep."""
    from provisa.federation.replica_routing import reads_replica

    if table.get("source_withdrawn"):
        raise ApiError(
            409,
            "schema.withdrawn_source_not_previewable",
            f"table {table['table_name']!r} cannot be previewed: its source "
            f"{table['source_id']!r} is withdrawn, and no engine reads a withdrawn source",
            table=table["table_name"],
            source=table["source_id"],
        )
    if source is not None and reads_replica(
        source, table, state.federation_engine.engine, promoted=False
    ):
        raise ApiError(
            409,
            "schema.withdrawn_table_not_previewable",
            f"table {table['table_name']!r} cannot be previewed: it is withdrawn and keeps no "
            "replica, and the engine reads its source only through one",
            table=table["table_name"],
        )


def _in_service(state: Any, table_id: int) -> dict | None:
    for table in state.tables:
        if table["id"] == table_id:
            return table
    return None


def _surface_with(state: Any, role_id: str, withdrawn: list[dict]) -> RoleSurface:
    """The role's surface with the ``withdrawn`` tables in its compilation context -- built as the
    role build builds it (app_loaders.schema_input_with_withdrawn). Its schema is the published
    one: no document names a withdrawn table."""
    from provisa.api.app_loaders import build_context, schema_input_with_withdrawn

    surface = role_surface(state, role_id)
    if surface is None:
        raise ApiError(
            400, "data.no_schema_for_role", f"No schema for role {role_id!r}", role_id=role_id
        )
    ctx = build_context(schema_input_with_withdrawn(state, state.roles[role_id], withdrawn))
    ctx.refusal = surface.context.refusal
    ctx.kept = dict(surface.context.kept)
    return replace(surface, context=ctx)


def _meta_surface_with(state: Any, meta_id: str, withdrawn: list[dict]) -> RoleSurface:
    """A meta-role's surface with the ``withdrawn`` tables in it (REQ-1620): the union of its
    members' rights over each, folded as the meta-role's build folds a table in service
    (security/meta_role.fold_tables) -- from each member's own surface with them added -- on
    copies, so no grant list the real state holds is changed."""
    from copy import deepcopy

    from provisa.security.meta_role import fold_tables

    granted = deepcopy(withdrawn)
    members = {m: _surface_with(state, m, withdrawn) for m in state.meta_roles[meta_id]}
    folded = fold_tables(_Members(state, members), meta_id, granted)
    surface = _surface_with(state, meta_id, granted)
    rls = replace(
        surface.rls,
        rules={**surface.rls.rules, **folded.rules},
        member_reads={**surface.rls.member_reads, **folded.member_reads},
    )
    return replace(surface, rls=rls)


class _Members:
    """The state as a meta-role's fold reads its members: each member's surface is the one with
    the withdrawn tables added, everything else the real state's."""

    def __init__(self, state: Any, surfaces: dict[str, RoleSurface]) -> None:
        self._state = state
        self.surfaces = {**state.surfaces, **surfaces}

    @property
    def rls_contexts(self) -> Any:
        from provisa.api.role_surface import SurfacePart

        return SurfacePart(self, "rls")

    def __getattr__(self, name: str) -> Any:
        return getattr(self._state, name)


def _view_body(state: Any, table: dict) -> str | None:
    """What a withdrawn VIEW's reference becomes for its steward's read; None for a table.

    A view that is not materialized, and an ordinary materialized one, is its SQL: read live, as
    a materialized view in service is while its build is not fresh. A materialized withdrawn
    view's build is kept registered and disabled (app_rebuild): nothing schedules it, so this
    read asks for it -- started here and not waited for -- and the view is built for its return.

    A bitemporal materialized view has no live reading: its rows are what its stored log says
    was true (mv/bitemporal.py). Until that log has been built once the read answers as any
    read does whose copy is still being built; after, it is read from the log."""
    if not table.get("view_sql"):
        return None
    from provisa.api.app_rebuild import compile_view_sql_to_physical

    # Physical, as every published view's entry is (app_rebuild): the engine is handed the
    # tables' own addresses, not their model names.
    body = compile_view_sql_to_physical(table["view_sql"].rstrip().rstrip(";"), state.view_context)
    # A withdrawn view is materialized exactly when its build is registered (app_rebuild).
    mv = state.mv_registry.get(f"view-{table['table_name']}")
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
            name=f"view-build:{table['table_name']}",
        )
    if mv.bitemporal is None:
        return body
    if mv.last_refresh_at is None:
        from provisa.federation.replica_state import ReplicaBuilding

        raise ReplicaBuilding(table["table_name"], 0.0)
    from provisa.mv.bitemporal import view_read_sql

    stored = mv.stored_ref()
    return view_read_sql(stored, mv.bitemporal)


def _statement(table: dict, limit: int | None, request: Any) -> tuple[str, list]:
    """The one statement the read is: the table's first ``limit`` rows, or the page the viewer
    asked for (``request``: its filters, sorts, grouping and page), written here from the
    table's registration with every value bound (admin/viewer_statement.py)."""
    from provisa.api.admin.viewer_statement import viewer_statement

    refuse_unparameterized(table, request)
    if request is not None:
        return viewer_statement(table, table_sql_ref(table), request)
    return f"SELECT * FROM {table_sql_ref(table)} LIMIT {int(limit or 0)}", []


def refuse_unparameterized(table: dict, request: Any = None) -> None:
    """Refuse a read of ``table``'s rows that gives no value to a parameter its metadata states
    as required (REQ-1986): its first rows or a sample (no ``request``), or a viewer's page whose
    ``request`` leaves one out. Asked of the registration, before any statement is written."""
    from provisa.core.parameters import (
        canonical_name,
        parameter_facts,
        required_of,
        whole_read_refusal,
    )

    facts = [f for f in parameter_facts(table["columns"]) if f.required]
    if request is not None:
        named = {(c.get("alias") or c["column_name"]): c["column_name"] for c in table["columns"]}
        given = {
            canonical_name(named.get(p.column, p.column).removeprefix("_nf_"))
            for p in request.params
        }
        facts = [f for f in facts if canonical_name(f.name) not in given]
    refusal = whole_read_refusal(table["table_name"], required_of(facts))
    if refusal is not None:
        raise refusal


async def read_as_caller(
    state: Any, table_id: int, role_id: str, *, limit: int | None = None, request: Any = None
) -> Any:
    """Rows of the table as ``role_id`` reads it -- a ``QueryResult``: its first ``limit`` rows,
    or the page of it the viewer's ``request`` describes. Either way the statement is written
    here; the caller sends none.

    The caller has already been checked for the right in the table's domain
    (:func:`table_for_steward`)."""
    from provisa.pgwire._pipeline import _execute_plan, _govern_and_route

    table = _in_service(state, table_id)
    if table is not None:
        sql, params = _statement(table, limit, request)
        plan = (
            await _govern_and_route(sql, role_id, params=params)
            if params
            else await _govern_and_route(sql, role_id)
        )
        return await _execute_plan(plan, state)

    withdrawn = withdrawn_table(state, table_id)
    if withdrawn is None:
        raise ApiError(404, "schema.table_not_found", f"Table {table_id} not found", table=table_id)
    # Refused by name before any statement is built: a column or operator the table has not.
    sql, params = _statement(withdrawn, limit, request)
    plan, seen = await plan_previewing(state, sql, role_id, [withdrawn], exec_params=params or None)
    return await _execute_plan(plan, seen)


async def plan_previewing(
    state: Any,
    sql: str,
    role_id: str,
    withdrawn: list[dict],
    *,
    exec_params: list | None,
) -> tuple[Any, _Previewing]:
    """``sql`` governed and routed as ``role_id`` with the ``withdrawn`` tables added to what it
    reads, and the view of the state it was governed in, which is the one it runs in: the one
    path a withdrawn table is read by, its steward's preview (REQ-1921). Nothing of it is kept:
    no plan, no response."""
    from provisa.compiler.directives import NO_CACHE_HINT
    from provisa.pgwire._pipeline import _govern_and_route_compiled

    seen = await previewing(state, role_id, withdrawn)
    plan = await _govern_and_route_compiled(
        sql,
        role_id,
        exec_params=exec_params,
        state=seen,
        cache_hint=NO_CACHE_HINT,
        sdl_joins=False,
    )
    return plan, seen


def surface_with_withdrawn(state: Any, role_id: str, withdrawn: list[dict]) -> RoleSurface:
    """``role_id``'s surface with the ``withdrawn`` tables in its compilation context: what a
    preview of them is governed in (:func:`previewing`). Under "Role: All" it is the meta-role's:
    the union of its members' rights (REQ-1620)."""
    if is_meta_role_id(role_id):
        return _meta_surface_with(state, role_id, withdrawn)
    return _surface_with(state, role_id, withdrawn)


async def previewing(state: Any, role_id: str, withdrawn: list[dict]) -> _Previewing:
    """The view of ``state`` a preview of the ``withdrawn`` tables as ``role_id`` is governed and
    run in: the role's surface with them added, read live from their sources, and nothing it keeps
    reaching the real state. Refused for a table kept in another region than this node's, and for
    one whose rows no engine reaches without the replica it does not keep."""
    from provisa.federation.query_residency import require_preview_in_its_region
    from provisa.federation.registry_view import registered_sources
    from provisa.federation.replica_routing import routes_with_previewed

    # Where a regional table may be read is the regions' rule, in its own module.
    for table in withdrawn:
        require_preview_in_its_region(table["table_name"], table.get("region"))
    # A built-in source (a view's) is not among the registered sources and owns no replica
    # (registry_view.registered_sources): a withdrawn view is read under the published routes.
    sources = {s.id: s for s in await registered_sources(state)}
    routes = state.replica_routes
    for table in withdrawn:
        source = sources.get(table["source_id"])
        # REQ-1921: a withdrawn table keeps no replica, whatever its replicate setting: its
        # preview reads its source in place, or is refused.
        from provisa.core.replicate import NEVER

        _require_previewable(state, {**table, "replicate": NEVER}, source)
        if source is not None:
            routes = routes_with_previewed(routes, table["id"], source.id)
    surface = surface_with_withdrawn(state, role_id, withdrawn)
    from provisa.compiler.view_expand import view_key

    bodies = {
        view_key(t["schema_name"], t["table_name"]): body
        for t in withdrawn
        if (body := _view_body(state, t)) is not None
    }
    return _Previewing(state, withdrawn, role_id, surface, routes, bodies)


def table_for_steward(request: Any, state: Any, table_id: int) -> dict:
    """The registered table ``table_id`` -- in service or withdrawn -- for a caller who may
    register tables in its domain: the right withdrawing and putting it back needs, checked in
    the table's own domain. Refused otherwise, as those would be."""
    from provisa.api.admin.capabilities import require_capability_request, require_domain_request

    table = _in_service(state, table_id) or withdrawn_table(state, table_id)
    if table is None:
        raise ApiError(404, "schema.table_not_found", f"Table {table_id} not found", table=table_id)
    require_capability_request(request, "table_registration")
    require_domain_request(request, table["domain_id"])
    return table
