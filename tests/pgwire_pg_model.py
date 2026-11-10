# Copyright (c) 2026 Kenneth Stott
# Canary: 18c0a228-259c-4703-813f-70fe7e9b02f6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The model the real-Postgres pgwire server serves (REQ-883, REQ-2190): one registered Postgres
table, the meta domain's catalog views as the product registers them, and the app state a request
reads them through. ``tests/integration/conftest.pgwire_pg_backend`` serves it to DuckDB and JDBC
clients; a unit test governs a client's catalog statement against the same model."""

# Requirements: REQ-883, REQ-2190

from __future__ import annotations

SOURCE_ID = "sales-pg"
SCHEMA = "public"
TABLE = "duckattach_orders_e2e"
# Rows the client must read back (covers int4/float8/text + a NULL).
ROWS = [
    (1, 19.98, "us-east"),
    (2, 49.99, "us-west"),
    (3, 199.99, "eu-west"),
    (4, 5.0, None),
]

# REQ-2190: the org schema the meta domain's catalog views live in on the control plane, and the
# first table id they take (clear of the registered source table's).
CATALOG_SCHEMA = "org_default"
META_FIRST_ID = 1000
ROLE = {
    "description": "Test role.",
    "id": "admin",
    "domain_access": ["*"],
    "capabilities": ["usage"],
}

TABLES = [
    {
        "id": 1,
        "source_id": SOURCE_ID,
        # domain_id drives the catalog SQL schema clients see (pgwire exposes tables under
        # domain_to_sql_name(domain_id)); it must equal the physical Postgres schema so the
        # DIRECT-routed, transpiled SQL resolves against the real table.
        "domain_id": SCHEMA,
        "schema_name": SCHEMA,
        "table_name": TABLE,
        "columns": [
            {"column_name": "id", "data_type": "integer", "visible_to": ["admin"]},
            {"column_name": "amount", "data_type": "double precision", "visible_to": ["admin"]},
            {"column_name": "region", "data_type": "varchar", "visible_to": ["admin"]},
        ],
        "write_ops": ["delete", "insert", "update"],
        "write_returns_rows": True,
        "write_refused_forms": [],
    }
]


def no_replica_routes(state):
    """Give a stand-in app state the replica routes a schema build publishes for a registry with
    no table on a replica (``ReplicaRoutes()``). The governed pipeline reads them for the
    operator's floor (``registry_view.operator_floor``), which refuses anything else by name — a
    bare ``MagicMock`` attribute there would otherwise stand in for real routes."""
    from provisa.federation.replica_address import ReplicaRoutes

    state.replica_routes = ReplicaRoutes()
    return state


def meta_catalog(model_db, loop) -> tuple[list[dict], dict]:
    """The meta domain's SQL catalog views (REQ-2190) as the product registers them: tables of
    ``provisa-admin`` in domain ``meta`` of the org schema, their columns reflected off the control
    plane (startup_seed._seed_meta_domain). A client's catalog statement is addressed to them and
    governed as a read of them, so a model without them refuses every catalog read (V006)."""
    import asyncio

    from provisa.compiler.introspect import ColumnMetadata
    from provisa.security.default_grant import VIEWER_ROLE
    from provisa.transpiler.metadata_views import CATALOG_VIEW_NAMES, NEUTRAL_VIEW_NAMES

    names = [*NEUTRAL_VIEW_NAMES, *CATALOG_VIEW_NAMES]

    async def _reflect() -> dict[str, list[dict]]:
        async with model_db.acquire() as conn:
            return await conn.reflect_columns_many(names, schema=CATALOG_SCHEMA)

    reflected = asyncio.run_coroutine_threadsafe(_reflect(), loop).result(timeout=60)
    missing = sorted(set(names) - set(reflected))
    assert not missing, f"control plane lacks catalog views: {missing}"
    tables: list[dict] = []
    col_types: dict[int, list] = {}
    for offset, name in enumerate(names):
        table_id = META_FIRST_ID + offset
        cols = reflected[name]
        tables.append(
            {
                "id": table_id,
                "source_id": "provisa-admin",
                "domain_id": "meta",
                "schema_name": CATALOG_SCHEMA,
                "table_name": name,
                "columns": [
                    {
                        "column_name": c["column_name"],
                        "data_type": c["data_type"],
                        # REQ-2154: granted to Viewer, as seeded; expand_meta_grants serves it.
                        "visible_to": [VIEWER_ROLE],
                    }
                    for c in cols
                ],
            }
        )
        col_types[table_id] = [
            ColumnMetadata(
                column_name=c["column_name"],
                data_type=c["data_type"],
                is_nullable=c.get("is_nullable", True),
            )
            for c in cols
        ]
    return tables, col_types


def build_state(pool, meta_tables: list[dict], meta_col_types: dict):
    from unittest.mock import MagicMock

    from provisa.executor.write_capability import (
        table_write_ops,
        table_write_refused_forms,
        table_write_returns_rows,
    )
    from provisa.security.inheritance import expand_meta_grants

    from provisa.compiler import naming as _naming
    from tests.helpers import serve_surfaces
    from provisa.compiler.context import build_context
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.compiler.schema_gen import SchemaInput
    from provisa.federation.engine import build_engine
    from provisa.federation.runtime import EngineRuntime

    col_types = {
        1: [
            ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
            ColumnMetadata(column_name="amount", data_type="double", is_nullable=True),
            ColumnMetadata(column_name="region", data_type="varchar", is_nullable=True),
        ]
    }
    col_types.update(meta_col_types)
    tables = [*TABLES, *meta_tables]
    # REQ-1132: the build serves meta's columns by capability (app.py's schema build).
    expand_meta_grants(tables, [ROLE])
    engine = build_engine("duckdb")
    for t in meta_tables:
        # The writes each table's source takes, decided as the schema build decides them.
        t["write_ops"] = sorted(table_write_ops(t, "postgresql", engine, source_path=None))
        t["write_returns_rows"] = table_write_returns_rows(
            t, "postgresql", engine, source_path=None
        )
        t["write_refused_forms"] = sorted(
            table_write_refused_forms(t, "postgresql", engine, source_path=None)
        )
    _naming.configure(gql="snake")
    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types=col_types,
        naming_rules=[],
        role=dict(ROLE),
        domains=[{"id": SCHEMA, "graphql_alias": None}],
    )
    ctx = build_context(si)

    state = no_replica_routes(MagicMock())
    state.security_high = False  # REQ-693: MagicMock auto-creates attrs as truthy; explicit False
    # REQ-1235: a single-tenant deployment serves one database, "provisa", which the pgwire catalog
    # names; a bare MagicMock here reads as multi-tenant and the catalog demands a bound org.
    state.multitenancy = False
    # A request reads its role's whole surface from `surfaces` (role_surface); `contexts` and
    # `rls_contexts` are views of it, as in the runtime, and a test assigning one writes it there.
    state.contexts = {"admin": ctx}
    state.rls_contexts = {}
    serve_surfaces(state)
    state.roles = {"admin": dict(ROLE)}
    state.schema_build_cache = {
        "column_types": col_types,
        "tables": tables,
        "domains": [{"id": SCHEMA, "graphql_alias": None}],
    }
    # Governance serves a role the columns of the registry a server loads (visible_to).
    state.tables = tables
    state.auth_config = {"provider": "none"}  # trust mode: username -> role_id
    state.auth_middleware_active = False
    state.masking_rules = {}
    # provisa-admin is the control plane's own source, typed by its backend (startup_seed).
    state.source_types = {SOURCE_ID: "postgresql", "provisa-admin": "postgresql"}
    state.source_dialects = {SOURCE_ID: "postgres", "provisa-admin": "postgres"}
    state.source_pools = pool
    state.server_limits = {}
    state.engine_conn = None
    # The governed pipeline keys its compile/routing caches on the schema identity and reads the
    # response cache; bare MagicMocks there read as a non-string cache key and as cache HITs.
    from provisa.compiler.compiled_query_cache import CompiledQueryCache

    state.schema_boot_id = "pgw-itest"
    state.schema_version = 1
    state.compiled_query_cache = CompiledQueryCache()
    state.routing_cache = CompiledQueryCache()
    from provisa.cache.store import NoopCacheStore

    state.response_cache_store = NoopCacheStore()  # AppState's caching-off store (REQ-1897)
    # AppState's cache-policy inputs (no per-source/per-table overrides, the deployment default
    # TTL, no org settings overrides) — a bare MagicMock there reads as a truthy non-int.
    state.source_cache = {}
    state.table_cache = {}
    state.response_cache_default_ttl = 300
    state.settings_overrides = {}
    from tests.helpers import stub_materialization_noop

    stub_materialization_noop(state)
    # Real federation engine — present for catalog probes; DIRECT execution goes through
    # execute_native → execute_direct on the real source pool, not the engine.
    state.federation_engine = EngineRuntime(engine, state)
    return state
