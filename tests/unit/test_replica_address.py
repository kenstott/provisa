# Copyright (c) 2026 Kenneth Stott
# Canary: ed55a562-9119-4495-9995-d2154c049257
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One replica address on every engine, and a read addressed to it (REQ-1912).

A replica lives in its org environment's replicas schema under ``<source>__<schema>__<table>``,
whatever the engine. A statement bound for the engine names each table served from its replica at
that address; a table read live is left as written."""

# Requirements: REQ-1912, REQ-826

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.federation.engine import _ENGINE_BUILDERS, build_engine
from provisa.federation.replica_address import (
    AmbiguousReplica,
    ReplicaRoute,
    ReplicaRoutes,
    address_replicas,
    export_view_address,
    engine_table_keys,
    is_replicas_schema,
    is_write_surface,
    mv_schema,
    replica_address,
    replica_schema,
    replica_table_name,
)

_STATE = SimpleNamespace(org_id="acme")


@pytest.fixture(autouse=True)
def _serving_acme(bind_org):
    """The work is acme's, the org _STATE serves, bound as its entrypoint binds it (REQ-1266)."""
    bind_org("acme")


_ENGINES = sorted(_ENGINE_BUILDERS)


@pytest.fixture
def engine(request, monkeypatch):
    # a generic engine reads its URL when it is built; none is dialled here
    monkeypatch.setenv("PROVISA_ENGINE_URL", "mssql://user:secret@engine-host/warehouse")
    built = build_engine(request.param)
    monkeypatch.setattr(type(built), "materialize_store", lambda _self: "postgresql:///store")
    return built


# -- the write address: one rule on every engine --------------------------------------------------


@pytest.mark.parametrize("engine", _ENGINES, indirect=True)
def test_every_engine_writes_a_replica_to_the_replicas_schema_under_the_one_name(engine):
    address = engine.backend.replica_address(
        _STATE, source_id="sales-pg", schema_name="public", table_name="orders"
    )
    assert (address.schema, address.table) == ("org_acme_replicas", "sales-pg__public__orders")
    assert is_replicas_schema(address.schema)


@pytest.mark.parametrize("engine", _ENGINES, indirect=True)
def test_no_engine_places_a_replica_at_its_tables_registered_address(engine):
    address = engine.backend.replica_address(
        _STATE, source_id="sales-pg", schema_name="public", table_name="orders"
    )
    assert (address.schema, address.table) != ("public", "orders")
    assert address.schema != "sales_pg_public"  # nor at the folded live name
    # the per-engine override the address used to come from is gone, not kept beside the resolver
    assert not hasattr(engine.backend, "landing_target")


@pytest.mark.parametrize("engine", _ENGINES, indirect=True)
def test_replicas_and_materialized_views_never_share_a_schema(engine):
    address = engine.backend.replica_address(
        _STATE, source_id="s", schema_name="public", table_name="t"
    )
    assert address.schema == replica_schema("acme") != mv_schema("acme") == "org_acme_mv_cache"


def test_the_address_follows_the_org_and_environment_bound_to_the_request(monkeypatch):
    from provisa.core.request_context import (
        reset_current_env,
        reset_current_org,
        set_current_env,
        set_current_org,
    )

    engine = build_engine("trino")
    monkeypatch.setattr(type(engine), "materialize_store", lambda _self: "postgresql:///store")
    org, env = set_current_org("globex"), set_current_env("feature_x")
    try:
        address = engine.backend.replica_address(
            _STATE, source_id="s", schema_name="public", table_name="t"
        )
    finally:
        reset_current_env(env)
        reset_current_org(org)
    assert address.schema == "org_globex_env_feature_x_replicas"


def test_a_replica_name_stays_within_the_identifier_limit_and_stays_distinct():
    long_a = replica_table_name("warehouse-postgres-primary", "analytics_reporting", "a" * 60)
    long_b = replica_table_name("warehouse-postgres-primary", "analytics_reporting", "a" * 61)
    assert len(long_a.encode()) <= 63 and len(long_b.encode()) <= 63
    assert long_a != long_b
    assert replica_table_name("s", "public", "t") == "s__public__t"


def test_an_export_view_is_outside_the_registered_address_and_both_read_schemas():
    """A store that publishes a view of each replica for its catalog export (Snowflake) puts it
    in a schema of its own: not the table's registered schema, not the replicas schema a read is
    addressed to, not the materialized-view schema."""
    view = export_view_address(
        org_id="acme", source_id="sales-pg", schema_name="public", table_name="orders"
    )
    replica = replica_address(
        org_id="acme", source_id="sales-pg", schema_name="public", table_name="orders"
    )
    assert (view.schema, view.table) == ("org_acme_export", "sales-pg__public__orders")
    assert view.table == replica.table
    assert view.schema not in ("public", replica.schema, mv_schema("acme"))
    assert not is_replicas_schema(view.schema)  # a replica write addressed there is refused
    assert is_write_surface(view.schema)  # and so is a live attach


@pytest.mark.parametrize(
    ("schema", "replicas", "surface"),
    [
        ("org_acme_replicas", True, True),
        ("org_acme_env_dev_replicas", True, True),
        ("org_acme_mv_cache", False, True),
        ("org_acme_export", False, True),
        ("org_acme_env_dev_export", False, True),
        ("org_acme__sales_export", False, False),  # another org's live schema "export", folded
        ("org_landing_never_writes_source_replicas", True, True),  # a boot org named in config
        ("org_acme__sales_replicas", False, False),  # another org's live schema "replicas", folded
        ("org_acme_api_cache", False, False),
        ("org_acme", False, False),
        ("public", False, False),
        ("replicas", False, False),
        ("sales_pg_public", False, False),
    ],
)
def test_which_schemas_are_provisas_to_write(schema, replicas, surface):
    assert is_replicas_schema(schema) is replicas
    assert is_write_surface(schema) is surface


# -- the read: a statement's tables at the engine's names -----------------------------------------


def _routes(engine_name: str, catalog: str | None, *keys) -> ReplicaRoutes:
    route = ReplicaRoute("src", "orders", (catalog, "org_acme_replicas", "src__public__orders"))
    return ReplicaRoutes(engine_name=engine_name, routes={key: route for key in keys})


def test_a_replica_served_table_is_renamed_and_keeps_its_alias():
    routes = _routes("trino", "provisa_admin", ("src", "public", "orders"))
    sql = (
        'SELECT "o"."id", "c"."name" FROM "src"."public"."orders" AS "o" '
        'JOIN "crm"."public"."customers" AS "c" ON "c"."id" = "o"."customer_id"'
    )
    assert address_replicas(sql, routes) == (
        'SELECT "o"."id", "c"."name" '
        'FROM "provisa_admin"."org_acme_replicas"."src__public__orders" AS "o" '
        'JOIN "crm"."public"."customers" AS "c" ON "c"."id" = "o"."customer_id"'
    )


def test_a_table_with_no_alias_keeps_its_own_name_for_its_columns():
    routes = _routes("trino", "provisa_admin", ("src", "public", "orders"))
    assert address_replicas('SELECT "orders"."id" FROM "src"."public"."orders"', routes) == (
        'SELECT "orders"."id" FROM "provisa_admin"."org_acme_replicas"."src__public__orders" '
        'AS "orders"'
    )


def test_a_statement_that_reads_only_live_tables_is_returned_as_written():
    routes = _routes("trino", "provisa_admin", ("src", "public", "orders"))
    sql = 'select  c.name from "crm"."public"."customers" c'  # not re-rendered: the same string
    assert address_replicas(sql, routes) is sql


def test_a_same_named_table_of_another_catalog_is_left_alone():
    routes = _routes("trino", "provisa_admin", ("src", "public", "orders"))
    sql = 'SELECT "o"."id" FROM "other"."public"."orders" AS "o"'
    assert address_replicas(sql, routes) == sql


def test_an_engine_with_no_catalog_reads_the_replicas_schema_alone():
    engine = build_engine("pg")
    keys = engine_table_keys(engine, "src", "public", "orders")
    assert keys == (("src", "public", "orders"), (None, "src_public", "orders"))
    routes = _routes("postgres", None, *keys)
    expected = 'SELECT "o"."id" FROM "org_acme_replicas"."src__public__orders" AS "o"'
    # folded by the query pipeline, or three-part from a stored view body: one address either way
    assert address_replicas('SELECT "o"."id" FROM "src_public"."orders" AS "o"', routes) == expected
    assert (
        address_replicas('SELECT "o"."id" FROM "src"."public"."orders" AS "o"', routes) == expected
    )


def test_a_catalog_qualified_engine_has_the_one_three_part_name():
    assert engine_table_keys(build_engine("trino"), "src", "public", "orders") == (
        ("src", "public", "orders"),
    )


def test_a_name_two_sources_share_is_refused_not_answered_from_one_of_them():
    key = ("WAREHOUSE", "default", "events")
    routes = ReplicaRoutes(engine_name="snowflake", ambiguous={key: ("mongo-a", "mongo-b")})
    with pytest.raises(AmbiguousReplica) as refused:
        address_replicas('SELECT * FROM "WAREHOUSE"."default"."events" AS "e"', routes)
    assert "mongo-a, mongo-b" in str(refused.value)
    assert '"WAREHOUSE"."default"."events"' in str(refused.value)


# -- the runtime seam every engine statement passes -----------------------------------------------


class _Backend:
    computes_fakes = True  # REQ-1494: the seam also checks the engine computes a statement's fakes

    def transpile_physical(self, pg_sql: str) -> str:
        return f"<engine dialect> {pg_sql}"


def _runtime(routes: ReplicaRoutes | None, engine_name: str = "trino"):
    from provisa.federation.runtime import EngineRuntime

    engine = SimpleNamespace(name=engine_name, backend=_Backend(), catalog_qualified=True)
    return EngineRuntime(engine, SimpleNamespace(replica_routes=routes))  # type: ignore[arg-type]


def test_the_transpile_seam_addresses_replicas_before_the_dialect_transpile():
    runtime = _runtime(_routes("trino", "provisa_admin", ("src", "public", "orders")))
    assert runtime.transpile_physical('SELECT * FROM "src"."public"."orders" AS "o"') == (
        '<engine dialect> SELECT * FROM "provisa_admin"."org_acme_replicas"."src__public__orders" '
        'AS "o"'
    )
    assert runtime.engine_physical('SELECT * FROM "src"."public"."orders" AS "o"') == (
        '<engine dialect> SELECT * FROM "provisa_admin"."org_acme_replicas"."src__public__orders" '
        'AS "o"'
    )


def test_routes_published_for_another_engine_are_not_applied():
    runtime = _runtime(_routes("duckdb", "mat_store", ("src", "public", "orders")), "trino")
    with pytest.raises(RuntimeError, match="published for engine 'duckdb'"):
        runtime.transpile_physical('SELECT * FROM "src"."public"."orders" AS "o"')


def test_before_any_registry_is_published_nothing_is_served_from_a_replica():
    runtime = _runtime(ReplicaRoutes())
    sql = 'SELECT * FROM "src"."public"."orders" AS "o"'
    assert runtime.transpile_physical(sql) == f"<engine dialect> {sql}"


# -- the store a floored source lands into, as the plan validates it -------------------------------


def test_an_engine_with_no_store_of_its_own_names_the_store_it_reads_back(monkeypatch):
    """Trino has no native store: a floored source lands into its materialization store, which
    Trino reads through its own connector for that type — so the plan accepts the override."""
    from provisa.federation.materialization import validate_materialization_backend

    trino = build_engine("trino")
    monkeypatch.setattr(
        type(trino), "materialize_store", lambda _self: "postgresql://u:p@store-host/provisa"
    )
    assert trino.native_store is None
    assert trino.replica_store_backend() == "postgresql"
    validate_materialization_backend(trino, trino.replica_store_backend())  # does not raise


def test_an_engine_with_a_native_store_lands_into_it():
    assert build_engine("duckdb").replica_store_backend() == "duckdb"
    assert build_engine("pg").replica_store_backend() == "postgres"
