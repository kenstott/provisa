# Copyright (c) 2026 Kenneth Stott
# Canary: c1d2e3f4-a5b6-7890-cd12-ef3456789012
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Test helpers shared across the test suite."""

from __future__ import annotations

import re
from types import SimpleNamespace

from provisa.federation import store_writer
from provisa.security.rights import PLATFORM_RIGHTS, Capability
from tests.role_state import RoleState


#: Every data-plane right a role can be GIVEN, named one by one — for a test role that is to be
#: limited by nothing but the grants the test itself writes (``visible_to``, ``writable_by``,
#: ``domain_access``). No capability stands in for another (REQ-1327), so a role that holds them
#: all lists them all. Left out: the two platform rights, which are over the deployment and not
#: over data; ``ddl``, which a test grants on purpose; and ``no_aggregations``, which withholds.
def unscoped_role(role_id: str, *capabilities: str) -> dict:
    """A role that reaches every domain, said explicitly — ``domain_access: ["*"]``.

    Governance always takes the acting role's own dict, and a role reaches only the domains it
    lists, so a test that is about something other than domain scope names the role and gives it
    the wildcard rather than leaving either out.
    """
    return {
        "id": role_id,
        "description": "Test role.",
        "capabilities": list(capabilities),
        "domain_access": ["*"],
    }


#: A Data Profiler source's run defaults as the source form writes them (REQ-1934): every one is
#: stored, so a test profiler's mapping is its cron plus these.
PROFILER_RUN_DEFAULTS: dict = {
    "low_cardinality_max": 100,
    "drift_window": 7,
    "drift_season": "none",
    "drift_distance": 3.0,
    "drift_slope": 3.0,
    "drift_ks": 0.2,
    "drift_psi": 0.25,
    "correlation_max_columns": 20,
    "joint_max_distinct": 20,
    "category_max_columns": 10,
}


ALL_DATA_CAPABILITIES: list[str] = sorted(
    c.value
    for c in Capability
    if c.value not in PLATFORM_RIGHTS
    and c is not Capability.NO_AGGREGATIONS
    # REQ-1921: data_residency exists only when the platform declares regions; a role granted it
    # elsewhere is refused at load, so it is a region test's to grant, not every role's.
    and c is not Capability.DATA_RESIDENCY
)

_ALIAS_RE = re.compile(r"\b(t|a|j|n|sub|cte)\d+\b", re.IGNORECASE)
_QUOTED_ALIAS_RE = re.compile(r'"(t|a|j|n|sub|cte)\d+"', re.IGNORECASE)


def _normalize_sql(sql: str) -> str:
    """Strip generated numeric aliases so assertions are alias-position-insensitive."""
    sql = _QUOTED_ALIAS_RE.sub("__alias__", sql)
    sql = _ALIAS_RE.sub("__alias__", sql)
    return re.sub(r"\s+", " ", sql).strip()


class RegisteredNames:
    """The address face of an engine stand-in whose registered tables are all read live: a
    registered table (its identity) resolves to ``"<source>"."<schema>"."<table>"`` and the
    address seam leaves every statement as written. The tests bind their tables to source
    ``src``, schema ``public`` (:func:`src_table`)."""

    #: The dialect statements bound for this engine are written in (``EngineRuntime.dialect``).
    dialect = "trino"

    #: The engine it stands for names a catalog in its SQL (REQ-1730).
    engine = SimpleNamespace(catalog_qualified=True)

    def engine_physical(self, sql: str) -> str:
        return self.address_replicas(sql)

    def address_replicas(self, sql: str) -> str:
        return sql

    def read_address(self, catalog, schema: str, table: str):
        return (catalog, schema, table)

    async def registered_key(self, table):
        return (table.source_id, table.schema_name, table.table_name)

    async def read_ref(self, table) -> str:
        return f'"{table.source_id}"."{table.schema_name}"."{table.table_name}"'


def src_table(name: str):
    """The registered table ``name`` on source ``src``, schema ``public`` — where
    :class:`RegisteredNames` stand-ins keep their tables."""
    from provisa.mv.models import TableIdentity

    return TableIdentity("src", "public", name)


def no_engine_store(_state) -> str:
    """Stands in for ``replica_builds.store_identity`` for a faked state with no engine bound:
    the store the (absent) promoted tables would have been built in."""
    return "no-engine-store"


async def no_promoted_tables(_conn, _store) -> tuple[frozenset, frozenset]:
    """Stands in for ``replica_state.promotion`` on a faked control plane: no table has been
    promoted, so none is served from a replica for being busy. A test that fakes the registry
    read fakes this read of the same connection."""
    return frozenset(), frozenset()


class DsnEngine:
    """Minimal write-face stand-in for a non-embedded store: forwards to ``store_writer`` against a
    fixed DSN, mirroring ``EngineBackend``'s base-class default (the path every non-DuckDB engine
    actually takes in production). ``make_source_land``/``make_mv_generate``/``make_mv_incremental``
    take an ``engine`` write-face object, not a bare DSN string."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    async def land_source_table(self, **kw):
        return await store_writer.land(self._dsn, **kw)

    async def persist_mv_table(self, **kw):
        return await store_writer.persist_land(self._dsn, **kw)

    def replica_address(self, *, source_id: str, schema_name: str, table_name: str):
        """As ``EngineRuntime.replica_address``: the one replica address, for the test org."""
        from provisa.federation.replica_address import replica_address

        return replica_address(
            org_id="test", source_id=source_id, schema_name=schema_name, table_name=table_name
        )


#: A role surface's part -> the AppState map that is a view of it (provisa/api/role_surface.py).
_SURFACE_VIEWS = {
    "schema": "schemas",
    "context": "contexts",
    "rls": "rls_contexts",
    "table_path_map": "table_path_maps",
    "proto": "proto_files",
}


def serve_surfaces(state) -> None:
    """Serve a MagicMock AppState's roles through ``surfaces``, as the runtime does.

    A request takes its role's whole surface from ``state.surfaces`` (``role_surface``); the
    per-part maps (``schemas``, ``contexts``, ...) are views of it. A MagicMock set up part by
    part has a mock ``surfaces``, whose ``get(role).context.refusal`` is truthy and refuses every
    query. This builds ``surfaces`` from the maps the state was given and makes each map a view
    of it, as AppState's properties are: reading one is a view, and assigning a whole map (a test
    that swaps a role's row filters) writes it into ``surfaces``. A MagicMock has a class of its
    own, so the properties go on that class and no other mock gets them. Call it once the maps
    are set."""
    from unittest.mock import NonCallableMock

    from provisa.api.role_surface import SurfacePart, set_part

    assert isinstance(state, NonCallableMock), "only a mock's class is its own to give properties"
    holder = SimpleNamespace(surfaces={})
    for part, view in _SURFACE_VIEWS.items():
        given = getattr(state, view)
        if isinstance(given, dict):
            set_part(holder, part, given)
    state.surfaces = holder.surfaces
    for part, view in _SURFACE_VIEWS.items():
        setattr(
            type(state),
            view,
            property(
                lambda s, p=part: SurfacePart(s, p),
                lambda s, v, p=part: set_part(s, p, v),
            ),
        )


def build_through_a_stand_in_pipeline(monkeypatch, physical=None) -> list:
    """Stand in for the one pipeline a view's build is governed through (REQ-1922,
    ``provisa.mv.governed_build``), for a test of what a refresh does with the rows it reads --
    landing, size guard, preflight, coordination -- not of how its SELECT is governed. The plan's
    engine SQL is ``physical(mv)``, the view's own SQL when not given. Returns the views built."""
    from provisa.federation import query_residency
    from provisa.mv import governed_build
    from provisa.transpiler.router import Route

    built: list = []

    async def _plan(_state, mv):
        built.append(mv)
        sql = physical(mv) if physical is not None else mv.sql
        assert sql is not None, f"{mv.id}: a stand-in plan needs the engine SQL it would run"
        return SimpleNamespace(route=Route.ENGINE, physical_sql=sql, exec_params=None, columns=None)

    async def _resident(_state, _plan):
        return None

    monkeypatch.setattr(governed_build, "_governed_plan", _plan)
    monkeypatch.setattr(query_residency, "prepare_engine_residency", _resident)
    return built


def stub_materialization_noop(state) -> None:
    """Pin the post-governance materialization inputs on a MagicMock AppState.

    ``_materialize_api_to_engine_cache`` reads ``hot_manager``, ``api_endpoints``,
    ``graphql_remote_sources`` and ``tenant_db`` per FROM/JOIN table. A bare MagicMock
    auto-vivifies each into a truthy mock, faking an API/hot table and injecting a garbage
    ``VALUES`` CTE (``() AS (VALUES )``) that fails to parse. A real ServerState has no hot
    tier / API endpoints / GQL remotes in these tests, so set them to their real-state empties
    to keep the direct-execution path a no-op.
    """
    state.hot_manager = None
    state.api_endpoints = {}
    state.graphql_remote_sources = {}
    state.tenant_db = None


def assert_sql_contains(sql: str, fragment: str) -> None:
    """Assert that *fragment* appears in *sql* after normalizing generated aliases.

    Both sides have numeric table aliases (t0, t1, a2, …) replaced with a
    placeholder so tests don't break when the compiler changes join order.
    """
    norm_sql = _normalize_sql(sql)
    norm_frag = _normalize_sql(fragment)
    assert norm_frag in norm_sql, (
        f"SQL fragment not found.\nFragment: {norm_frag}\nSQL:      {norm_sql}"
    )


def governance_for_role(
    ctx,
    role_id: str,
    *,
    rls=None,
    masking_rules: dict | None = None,
    column_types: dict[int, list[tuple[str, str]]] | None = None,
):
    """The GovernanceContext the server builds for ``role_id`` (compiler/stage2): every table
    ``ctx`` registers and every join target, ``rls`` (an RLSContext or a table_id -> predicate
    map), ``masking_rules`` keyed ``(table_id, role_id)``.

    The role holds ``full_results`` so no default row cap is added: a statement governed with
    it carries its row filters and masks alone. ``column_types`` registers each table's columns
    (table_id -> [(name, type)]) where a test reads them (a session-variable cast).
    """
    from provisa.compiler.rls import RLSContext
    from provisa.compiler.stage2 import build_governance_context

    metas = {m.table_id: m for m in ctx.tables.values()}
    for join in ctx.joins.values():
        metas.setdefault(join.target.table_id, join.target)
    tables = [
        {
            "id": tid,
            "domain_id": m.domain_id,
            "source_id": m.source_id,
            "schema_name": m.schema_name,
            "table_name": m.table_name,
            "columns": [
                {"column_name": name, "data_type": dtype, "visible_to": []}
                for name, dtype in (column_types or {}).get(tid, [])
            ],
        }
        for tid, m in metas.items()
    ]
    if not isinstance(rls, RLSContext):
        rls = RLSContext(rules=dict(rls or {}))
    return build_governance_context(
        role_id,
        rls,
        masking_rules or {},
        ctx,
        tables,
        unscoped_role(role_id, Capability.FULL_RESULTS.value),
    )


def governed_for_role(
    sql: str,
    ctx,
    role_id: str,
    *,
    rls=None,
    masking_rules: dict | None = None,
    column_types: dict[int, list[tuple[str, str]]] | None = None,
) -> str:
    """``sql`` as the one governed pipeline serves it to ``role_id``: ``apply_governance`` with
    the role's :func:`governance_for_role` context."""
    from provisa.compiler.stage2 import apply_governance

    gov = governance_for_role(
        ctx, role_id, rls=rls, masking_rules=masking_rules, column_types=column_types
    )
    return apply_governance(sql, gov)


def assert_span_emitted(exporter, name_fragment: str) -> None:
    spans = exporter.get_finished_spans()
    names = [s.name for s in spans]
    assert any(name_fragment in n for n in names), (
        f"No span matching {name_fragment!r} found. Emitted: {names}"
    )


def assert_sql_matches(sql: str, pattern: str) -> None:
    """Assert that regex *pattern* matches *sql* after normalizing generated aliases."""
    norm_sql = _normalize_sql(sql)
    assert re.search(pattern, norm_sql, re.IGNORECASE), (
        f"Pattern not matched.\nPattern: {pattern}\nSQL: {norm_sql}"
    )


def dq_contexts(*tables: tuple, role: str = "analyst") -> dict:
    """``state.contexts``-shaped input for the DQ dataset resolver (REQ-1443).

    Each entry is ``(table_id, source_id, domain_id, schema_name, table_name)`` or the same with a
    trailing ``display_name`` (the DB alias pgwire publishes instead of the physical name).
    """
    from types import SimpleNamespace

    from provisa.compiler.sql_types import TableMeta

    metas = {}
    for spec in tables:
        table_id, source_id, domain_id, schema_name, table_name, *alias = spec
        metas[f"t{table_id}"] = TableMeta(
            table_id=table_id,
            field_name=table_name,
            type_name=table_name.capitalize(),
            source_id=source_id,
            catalog_name=source_id,
            schema_name=schema_name,
            table_name=table_name,
            domain_id=domain_id,
            display_name=alias[0] if alias else "",
        )
    return {role: SimpleNamespace(tables=metas)}


async def delete_source_and_its_tables(client, source_id: str) -> None:
    """Remove a source a test registered, and what the test registered against it (REQ-1918).

    A source is deleted only when nothing refers to it: ``deleteSource`` is refused while a table
    is registered against it, and ``deleteTable`` while a relationship takes part in the table.
    So a cleanup removes them in that order, each through its own mutation, as an operator would.
    ``client`` is an httpx ``AsyncClient`` for the app. A source that is not there is left
    alone; one still refused after its tables are gone raises.
    """

    async def gql(query: str) -> dict:
        resp = await client.post("/admin/graphql", json={"query": query})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        # A refused mutation is an error, not a None to index into: say what it was.
        assert not body.get("errors"), f"{query}: {body['errors']}"
        return body["data"]

    listed = await gql(
        "{ tables { id sourceId } relationships { id sourceTableId targetTableId } }"
    )
    table_ids = {t["id"] for t in listed["tables"] if t["sourceId"] == source_id}
    for rel in listed["relationships"]:
        if rel["sourceTableId"] in table_ids or rel["targetTableId"] in table_ids:
            await gql(f'mutation {{ deleteRelationship(id: "{rel["id"]}") {{ success }} }}')
    for table_id in sorted(table_ids):
        await gql(f"mutation {{ deleteTable(id: {table_id}) {{ success }} }}")
    outcome = (
        await gql(f'mutation {{ deleteSource(id: "{source_id}") {{ success code message }} }}')
    )["deleteSource"]
    assert outcome["success"] or outcome["code"] == "schema.source_not_found", outcome


def hold_registered_tables(
    monkeypatch, *names: str, source_id: str = "src", schema: str = "public"
):
    """Make the app state's model hold registered tables ``names`` (on ``source_id``/``schema``)
    and nothing else — what a view's inputs are resolved against before it is refreshed or wired
    (provisa/mv/view_inputs.py, which also reads the compiled role contexts: a context an app
    built earlier in the process left behind would name tables of another model)."""
    from provisa.api.app import state

    monkeypatch.setattr(
        state,
        "tables",
        [
            {"id": i, "source_id": source_id, "schema_name": schema, "table_name": name}
            for i, name in enumerate(names, 1)
        ],
    )
    monkeypatch.setattr(state, "contexts", {})
    # ... and no model store: the readable-inputs check (provisa/mv/readable_inputs.py) also asks
    # the store which registered tables keep rows by key. A store an earlier test left on the
    # state is another model's -- or a database since dropped: core 1/6 of run 37874425906 failed
    # steps_materialization_store there, `relation "registered_tables" does not exist`.
    monkeypatch.setattr(state, "model_db", None)


def derived_lineage(views: list, tables: list[tuple[str, str, str]]) -> dict[str, set[str]]:
    """The event graph's edges for ``views`` derived from their SQL (REQ-939, REQ-964), resolved
    against a model holding the registered ``tables`` — each ``(source_id, schema, table)`` — and
    the views themselves (provisa.events.nodes.lineage_graph)."""
    from types import SimpleNamespace

    from provisa.events.nodes import lineage_graph

    by_id = {v.id: v for v in views}
    model = RoleState(
        tables=[
            {"id": i, "source_id": s, "schema_name": sch, "table_name": t}
            for i, (s, sch, t) in enumerate(tables, 1)
        ],
        source_catalogs={},
        contexts={},
        mv_registry=SimpleNamespace(get_enabled=lambda: list(views), get=by_id.get),
    )
    return lineage_graph(views, model)


def registry_write_ops(source_type: str, *, view: bool = False) -> list[str]:
    """The ``write_ops`` a registered table's record carries for a table on ``source_type`` (a
    view's, when ``view``), by the product's own rule (executor/write_capability.table_write_ops),
    for fixtures that build registry rows by hand. No engine is bound, so a source writable only
    through the engine takes none here."""
    from provisa.executor.write_capability import table_write_ops

    return sorted(
        table_write_ops(
            {"view_sql": "SELECT 1"} if view else {}, source_type, None, source_path=None
        )
    )


def registry_write_returns_rows(source_type: str, *, view: bool = False) -> bool:
    """The ``write_returns_rows`` a registered table's record carries beside ``write_ops`` (see
    :func:`registry_write_ops`), by the product's own rule."""
    from provisa.executor.write_capability import table_write_returns_rows

    return table_write_returns_rows(
        {"view_sql": "SELECT 1"} if view else {}, source_type, None, source_path=None
    )


def attaching(source, catalog: str):
    """``source`` as an engine attaches it: its fields, plus the catalog name the engine names
    what it keeps for the source after (REQ-1266/1529 — the org-scoped catalog the compiler
    emits)."""
    from types import SimpleNamespace

    return SimpleNamespace(**dict(source), catalog=catalog)


# -- REQ-1921: a table or view registered through the admin starts as draft ---------------------


def registered_id(message: str) -> int:
    """The id a successful ``registerTable`` answer names (``... registered (id=N)``)."""
    found = re.search(r"\(id=(\d+)\)", message)
    assert found is not None, f"registerTable named no id: {message!r}"
    return int(found.group(1))


def release_field(table_id: int) -> str:
    """The admin GraphQL field that releases a draft table or view into service: what a domain
    owner does after registering one through the admin, before anything reads it."""
    return f"setTableDraft(tableId: {table_id}, draft: false)"


def release_mutation(table_id: int) -> str:
    """:func:`release_field` as a whole mutation document."""
    return f"mutation {{ {release_field(table_id)} {{ success message }} }}"


def config_write_grants(roles: list[str], columns: list[str]) -> list[dict]:
    """A configuration table's ``write_grants``: each of ``roles`` may insert and update
    ``columns`` and delete rows, its read row filter bounding each (REQ-2074)."""
    return [
        {"role": r, "operation": op, **({"columns": list(columns)} if op != "delete" else {})}
        for r in roles
        for op in ("insert", "update", "delete")
    ]


def write_grants_field(table_id: int, roles: list[str], columns: list[str]) -> str:
    """The admin GraphQL field that lets each of ``roles`` insert and update ``columns`` of a
    table and delete its rows, its read row filter bounding each (REQ-2074): what an
    administrator does to make a registered table writable."""
    cols = ", ".join(f'"{c}"' for c in columns)
    grants = ", ".join(
        f'{{ role: "{r}", operation: "{op}", columns: [{cols if op != "delete" else ""}] }}'
        for r in roles
        for op in ("insert", "update", "delete")
    )
    return f"setTableWriteGrants(tableId: {table_id}, grants: [{grants}])"


def write_grants_mutation(table_id: int, roles: list[str], columns: list[str]) -> str:
    """:func:`write_grants_field` as a whole mutation document."""
    return f"mutation {{ {write_grants_field(table_id, roles, columns)} {{ success message }} }}"


def ready_org_folder(org_id: str | None = None):
    """The org's folder on this node made ready, as its runtime's build makes it (REQ-1992): the
    bound org's when ``org_id`` is None. For a test that moved the data directory, or bound an
    org whose runtime it does not build."""
    from provisa.core import org_files
    from provisa.core.request_context import require_current_org

    path = org_files.folder(org_id or require_current_org())
    path.mkdir(parents=True, exist_ok=True)
    return path


async def register_org(admin_db, org_id: str) -> bool:
    """The org's row on the admin plane, as creating an org writes it, for a test that builds the
    org's runtime directly (REQ-1992: the build reads the org's file-folder setting from that row).
    True when this call wrote it; a row already there is left as it is."""
    from sqlalchemy import insert, select

    from provisa.core.schema_admin import orgs

    async with admin_db.acquire() as conn:
        held = (await conn.execute_core(select(orgs.c.id).where(orgs.c.id == org_id))).fetchone()
        if held is not None:
            return False
        await conn.execute_core(insert(orgs).values(id=org_id, name=org_id))
    return True


async def unregister_org(admin_db, org_id: str) -> None:
    """Remove the row :func:`register_org` wrote."""
    from sqlalchemy import delete

    from provisa.core.schema_admin import orgs

    async with admin_db.acquire() as conn:
        await conn.execute_core(delete(orgs).where(orgs.c.id == org_id))
