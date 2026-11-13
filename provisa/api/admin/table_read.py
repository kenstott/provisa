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
read live, in the table's own region, and no plan or response of it is kept. The table is still
in no schema, and the same words sent as an ordinary statement are refused as before.

A role holding table registration in a draft's domain (A DRAFT IS SERVED TO ORG_ADMIN, decided
2026-11-12: security.rights.registers_tables_in) reads the draft as a served table is read: a statement of its own on a SQL surface that names drafts is
governed and run here, through the same path, with those drafts added (:func:`plan_reading_drafts`).
Every such read is recorded as a draft preview naming the drafts it read.

Where the bound engine cannot read a draft's source in place, a draft's rows are reached through
its replica as a table in service's are: the read asks for it and waits on it (``query_residency``,
which judges the drafts this view names), and the replica is kept. No other read is addressed there.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from provisa.api.errors import ApiError
from provisa.api.role_surface import RoleSurface, role_surface
from provisa.security.meta_role import is_meta_role_id


class _WithDrafts:
    """The application state as a statement that may read some drafts sees it: everything is the
    real state's, except that the drafts are among the registered tables, the caller's role has
    them in its compilation context, and no plan or response the read produces is kept anywhere
    the real state reads from.

    The drafts' ROWS are reached the way a table in service's are. Where the engine reads a
    source in place, it is read there. Where the engine reaches it only through a replica, the
    read asks for the replica and waits on it as any read does (``query_residency``, which judges
    the drafts this view names) and is addressed at it by ``routes`` — the published routes and
    these drafts' own. The replica is kept: a draft is offered to no other reader, and it may hold
    its data. The engine face is bound to this view, so only this read is addressed there."""

    def __init__(
        self,
        state: Any,
        drafts: list[dict],
        role_id: str,
        surface: RoleSurface,
        routes: Any,
        view_bodies: dict[str, str],
    ) -> None:
        from provisa.cache.store import NoopCacheStore
        from provisa.compiler.compiled_query_cache import CompiledQueryCache

        self._state = state
        self.tables = [*state.tables, *drafts]
        self.surfaces = {**state.surfaces, role_id: surface}
        # Its own, empty, and dropped with this object: a plan or a validation kept for this
        # read is never found by a statement governed against the real state.
        self.compiled_query_cache = CompiledQueryCache(expires=False)
        self.routing_cache = CompiledQueryCache()
        self.response_cache_store = NoopCacheStore()
        self.previewed_draft_ids = frozenset(d["id"] for d in drafts)
        if view_bodies:
            # A draft VIEW is in no published view map, so no other statement expands it. This
            # read expands it as a view in service is expanded: its body, cut to what the
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


def draft_table(state: Any, table_id: int) -> dict | None:
    """The draft ``table_id``, or None when no draft has that id. Whom it is served to is asked
    elsewhere (:func:`table_for_steward`, :func:`draft_served_to`)."""
    for table in state.role_build_inputs["draft_tables"]:
        if table["id"] == table_id:
            return table
    return None


def _in_service(state: Any, table_id: int) -> dict | None:
    for table in state.tables:
        if table["id"] == table_id:
            return table
    return None


def _surface_with(state: Any, role_id: str, drafts: list[dict]) -> RoleSurface:
    """The role's surface with ``drafts`` in its compilation context — built as the role build
    builds it (app_loaders.schema_input_with_drafts). Its schema is the published one: a role is
    offered there already the drafts it is served (security.rights.registers_tables_in), and no
    other role reads a draft by a document of its own."""
    from provisa.api.app_loaders import build_context, schema_input_with_drafts

    surface = role_surface(state, role_id)
    if surface is None:
        raise ApiError(
            400, "data.no_schema_for_role", f"No schema for role {role_id!r}", role_id=role_id
        )
    ctx = build_context(schema_input_with_drafts(state, state.roles[role_id], drafts, marked=False))
    ctx.refusal = surface.context.refusal
    ctx.kept = dict(surface.context.kept)
    return replace(surface, context=ctx)


def _meta_surface_with(state: Any, meta_id: str, drafts: list[dict]) -> RoleSurface:
    """A meta-role's surface with ``drafts`` in it (REQ-1620): the union of its members' rights
    over each draft, folded as the meta-role's build folds a table in service
    (security/meta_role.fold_tables) -- from each member's own surface with the drafts added --
    on copies of the drafts, so no grant list the real state holds is changed."""
    from copy import deepcopy

    from provisa.security.meta_role import fold_tables

    granted = deepcopy(drafts)
    members = {m: _surface_with(state, m, drafts) for m in state.meta_roles[meta_id]}
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
    the draft added, everything else the real state's."""

    def __init__(self, state: Any, surfaces: dict[str, RoleSurface]) -> None:
        self._state = state
        self.surfaces = {**state.surfaces, **surfaces}

    @property
    def rls_contexts(self) -> Any:
        from provisa.api.role_surface import SurfacePart

        return SurfacePart(self, "rls")

    def __getattr__(self, name: str) -> Any:
        return getattr(self._state, name)


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

    draft = draft_table(state, table_id)
    if draft is None:
        raise ApiError(404, "schema.table_not_found", f"Table {table_id} not found", table=table_id)
    # Refused by name before any statement is built: a column or operator the table has not.
    sql, params = _statement(draft, limit, request)
    plan, seen = await plan_reading_drafts(state, sql, role_id, [draft], exec_params=params or None)
    return await _execute_plan(plan, seen)


def _served_draft_names(state: Any, role_id: str) -> dict[str, int]:
    """Each name ``role_id``'s context gives a draft that is served to it, with the draft's id: a
    draft of a domain the role holds table registration in (A DRAFT IS SERVED TO ORG_ADMIN,
    security.rights.registers_tables_in)."""
    from provisa.security.rights import registers_tables_in

    surface = role_surface(state, role_id)
    if surface is None or not surface.context.draft_ids:
        return {}
    role = state.roles[role_id]
    domains = {t["id"]: t["domain_id"] for t in state.role_build_inputs["draft_tables"]}
    return {
        name: table_id
        for name, table_id in surface.context.draft_ids.items()
        if registers_tables_in(role, state.roles, domains[table_id])
    }


def drafts_a_statement_names(state: Any, sql: str, role_id: str) -> list[dict]:
    """The drafts ``sql`` names that are served to ``role_id`` (:func:`_served_draft_names`), each
    once; any other draft it names goes on to be refused naming it as a draft, as before. A draft
    is named by any name the role's context records for it (compiler/context._register_draft_names),
    read as the statement's validation reads a table's name."""
    named = _served_draft_names(state, role_id)
    if not named:
        return []
    # Parsed only when a draft's name is in the text: no other statement pays for it here.
    text = sql.lower()
    if not any(name.rsplit(".", 1)[-1].strip('"').lower() in text for name in named):
        return []
    from provisa.compiler.names_read import sql_relations

    ids: list[int] = []
    for said in sql_relations(sql):
        table_id = named.get(said)
        if table_id is not None and table_id not in ids:
            ids.append(table_id)
    by_id = {t["id"]: t for t in state.role_build_inputs["draft_tables"]}
    return [by_id[i] for i in ids]


def drafts_a_document_reads(state: Any, query: str, role_id: str) -> list[dict]:
    """The drafts served to ``role_id`` (:func:`_served_draft_names`) the root fields of GraphQL
    ``query`` name, each once, when every operation in it is a query; none otherwise, and the
    document goes on to be refused naming a draft as before (a mutation or subscription of a
    draft is refused, for a role it is served to too). A document that does not parse names none: its parse refuses it
    where every document is parsed."""
    from provisa.compiler.names_read import query_root_fields
    from provisa.compiler.parser import table_field_candidates

    named = _served_draft_names(state, role_id)
    if not named:
        return []
    # Parsed only when a draft's name is in the text: no other document pays for it here.
    text = query.lower()
    if not any(name.rsplit(".", 1)[-1].strip('"').lower() in text for name in named):
        return []
    ids: list[int] = []
    for name in query_root_fields(query):
        for candidate in table_field_candidates(name):
            table_id = named.get(candidate)
            if table_id is not None and table_id not in ids:
                ids.append(table_id)
    by_id = {t["id"]: t for t in state.role_build_inputs["draft_tables"]}
    return [by_id[i] for i in ids]


async def plan_reading_drafts(
    state: Any,
    sql: str,
    role_id: str,
    drafts: list[dict],
    *,
    exec_params: list | None,
    deliver: Any = None,
    buffered: bool = False,
    explain: bool | None = None,
    as_of: str | None = None,
) -> tuple[Any, _WithDrafts]:
    """``sql`` governed and routed as ``role_id`` with ``drafts`` added to what it reads, and the
    view of the state it was governed in, which is the one it runs in: the one path a draft is
    read by, a steward's preview and a statement of a role it is served to alike (REQ-1921). Nothing of it is
    kept: no plan, no response."""
    from provisa.compiler.directives import NO_CACHE_HINT
    from provisa.pgwire._pipeline import _govern_and_route_compiled

    seen = await reading_drafts(state, role_id, drafts)
    plan = await _govern_and_route_compiled(
        sql,
        role_id,
        exec_params=exec_params,
        state=seen,
        cache_hint=NO_CACHE_HINT,
        sdl_joins=False,
        deliver=deliver,
        buffered=buffered,
        explain=explain,
        as_of=as_of,
    )
    return plan, seen


def surface_with_drafts(state: Any, role_id: str, drafts: list[dict]) -> RoleSurface:
    """``role_id``'s surface with ``drafts`` in its compilation context: what a read of them is
    governed in (:func:`reading_drafts`), and what names them. Under "Role: All" it is the
    meta-role's: the union of its members' rights (REQ-1620)."""
    if is_meta_role_id(role_id):
        return _meta_surface_with(state, role_id, drafts)
    return _surface_with(state, role_id, drafts)


def draft_served_to(state: Any, role_id: str, table_id: int) -> dict | None:
    """The draft ``table_id`` when it is served to ``role_id`` (:func:`_served_draft_names`: its
    context names it and the role holds table registration in its domain). None otherwise: to
    that role the table is not served, draft or not."""
    if table_id not in _served_draft_names(state, role_id).values():
        return None
    return draft_table(state, table_id)


async def reading_drafts(state: Any, role_id: str, drafts: list[dict]) -> _WithDrafts:
    """The view of ``state`` a read of ``drafts`` as ``role_id`` is governed and run in: the
    role's surface with them added, their routes, and nothing it keeps reaching the real state.
    Refused for a draft kept in another region than this node's."""
    from provisa.federation.query_residency import require_draft_read_in_its_region
    from provisa.federation.registry_view import registered_sources
    from provisa.federation.replica_routing import routes_with_draft

    # Where a regional table may be read is the regions' rule, in its own module.
    for draft in drafts:
        require_draft_read_in_its_region(draft["table_name"], draft.get("region"))
    # A built-in source (a view's) is not among the registered sources and owns no replica
    # (registry_view.registered_sources): a draft of one is read under the published routes.
    sources = {s.id: s for s in await registered_sources(state)}
    routes = state.replica_routes
    for draft in drafts:
        source = sources.get(draft["source_id"])
        if source is not None:
            routes = routes_with_draft(state, draft, source, routes)
    surface = surface_with_drafts(state, role_id, drafts)
    bodies = {d["table_name"]: body for d in drafts if (body := _view_body(state, d)) is not None}
    return _WithDrafts(state, drafts, role_id, surface, routes, bodies)


def table_for_steward(request: Any, state: Any, table_id: int) -> dict:
    """The registered table ``table_id`` — in service or draft — for a caller who may register
    tables in its domain: the right a table's release needs, checked in the table's own domain.
    Refused otherwise, as the release would be."""
    from provisa.api.admin.capabilities import require_capability_request, require_domain_request

    table = _in_service(state, table_id) or draft_table(state, table_id)
    if table is None:
        raise ApiError(404, "schema.table_not_found", f"Table {table_id} not found", table=table_id)
    require_capability_request(request, "table_registration")
    require_domain_request(request, table["domain_id"])
    return table
