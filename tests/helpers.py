# Copyright (c) 2026 Kenneth Stott
# Canary: c1d2e3f4-a5b6-7890-cd12-ef3456789012
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Test helpers shared across the test suite."""

from __future__ import annotations

import re

from provisa.federation import store_writer
from provisa.security.rights import PLATFORM_RIGHTS, Capability


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
    return {"id": role_id, "capabilities": list(capabilities), "domain_access": ["*"]}


ALL_DATA_CAPABILITIES: list[str] = sorted(
    c.value
    for c in Capability
    if c.value not in PLATFORM_RIGHTS and c is not Capability.NO_AGGREGATIONS
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
    — what a view's inputs are resolved against before it is refreshed or wired
    (provisa/mv/view_inputs.py)."""
    from provisa.api.app import state

    monkeypatch.setattr(
        state,
        "tables",
        [
            {"id": i, "source_id": source_id, "schema_name": schema, "table_name": name}
            for i, name in enumerate(names, 1)
        ],
    )


def derived_lineage(views: list, tables: list[tuple[str, str, str]]) -> dict[str, set[str]]:
    """The event graph's edges for ``views`` derived from their SQL (REQ-939, REQ-964), resolved
    against a model holding the registered ``tables`` — each ``(source_id, schema, table)`` — and
    the views themselves (provisa.events.nodes.lineage_graph)."""
    from types import SimpleNamespace

    from provisa.events.nodes import lineage_graph

    by_id = {v.id: v for v in views}
    model = SimpleNamespace(
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

    return sorted(table_write_ops({"view_sql": "SELECT 1"} if view else {}, source_type, None))
