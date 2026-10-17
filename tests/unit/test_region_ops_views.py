# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""On a node of a region, the ops views read the record and the state that region keeps.

A region keeps its record (the statements it served) and its state (its replica builds) in stores
of its own; the ops views were views of the model's database over its own copies of those tables,
which nothing writes in a region, so ``ops.queries`` read no statement at all. A view over one of
the region's stores is laid out there; one that also names the model's tables is the engine's,
each table read where it is kept."""

# Requirements: REQ-1922, REQ-1386

from __future__ import annotations

from types import SimpleNamespace

from sqlalchemy import insert

from provisa.api import region_ops_views as ops
from provisa.core import process_region
from provisa.core.model_change import ModelPlane
from provisa.core.region_stores import open_org_stores
from provisa.core.schema_org import domains, query_audit_log, registered_tables, sources
import pytest

from tests.unit.test_region_stores import _PLATFORM, _bind, _declare


@pytest.fixture(autouse=True)
def _restore_region():
    was = process_region._region
    yield
    process_region._region = was


@pytest.fixture
def model_engine(control_plane):
    return control_plane.engine()


@pytest.fixture
def region_dbs(control_plane):
    """eu's state and record stores: databases of their own on the plane's server."""
    from tests.pg_control_plane import TEMPLATE, clone

    planes = {store: clone(control_plane.pg, TEMPLATE) for store in ("eu-state", "eu-record")}
    yield {store: plane.name for store, plane in planes.items()}
    for plane in planes.values():
        plane.close()


def test_each_ops_view_is_read_where_its_tables_are():
    assert {"ops_table_usage", "service_level", "surface_mix", "query_health"} <= set(
        ops.store_views(ops.RECORD)
    )
    assert set(ops.store_views(ops.STATE)) == {
        "replica_builds",
        "replica_build_log_ops",
        "replica_build_health",
    }
    assert {"queries", "query_audit_log_ops", "usage_ranking", "tag_usage"} <= set(
        ops.engine_views()
    )


def test_only_a_node_of_a_region_reads_them_under_the_regions_catalog():
    process_region.bind_launch({}, requested=None)
    assert ops.table_catalogs() == {}
    process_region.bind_launch(_PLATFORM, requested="eu")
    catalogs = ops.table_catalogs()
    assert catalogs[("provisa-admin", "queries")] == ops.CATALOG
    assert ("provisa-admin", "registered_tables") not in catalogs


async def test_a_regions_node_reads_the_statements_it_served_named_from_the_model(
    model_engine,
    control_plane,
    region_dbs,
    tmp_path,
):
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime
    from provisa.federation.native_backend import libpq_dsn

    process_region.bind_launch(_PLATFORM, requested="eu")
    stores = open_org_stores("org_default", ModelPlane("acme", None), model_engine=model_engine)
    await _declare(stores.model_db, control_plane.pg, region_dbs)
    bound = await _bind(stores)
    await ops.lay_out(bound.record_db, ops.RECORD)
    await ops.lay_out(bound.tenant_db, ops.STATE)

    table_id = await _registered(bound.model_db)
    await _meta_view(bound.model_db, "tags")  # tags_meta, which tag_usage reads
    async with bound.record_db.acquire() as conn:
        await conn.execute_core(
            insert(query_audit_log).values(
                user_id="u1",
                role_id="analyst",
                query_hash="h",
                table_ids=[table_id],
                table_names=["orders"],
                source="sql",
                status_code=200,
                duration_ms=7,
                region="eu",
            )
        )

    runtime = DuckDBFederationRuntime(materialize_dsn=f"duckdb:///{tmp_path / 'mat.duckdb'}")
    try:
        runtime.attach_control_plane(libpq_dsn(bound.model_db.engine.url))
        schema, stores_, views = ops.engine_attachment(
            SimpleNamespace(
                model_db=bound.model_db, record_db=bound.record_db, tenant_db=bound.tenant_db
            )
        )
        runtime.attach_region_ops(ops.CATALOG, schema, stores_, views)
        rows = runtime._con.execute(
            f'SELECT region, table_name, table_count FROM "{ops.CATALOG}"."{schema}"."queries"'
        ).fetchall()
        served = runtime._con.execute(
            f'SELECT query_count FROM "{ops.CATALOG}"."{schema}"."surface_mix"'
        ).fetchall()
    finally:
        runtime.close()
    assert rows == [("eu", "orders", 1)]
    assert sum(r[0] for r in served) == 1


async def _registered(model_db) -> int:
    """A table of the model, as a registration names it: ``sales.orders`` of source ``shop``."""
    from provisa.core import model_change

    async with model_change.scope("a table to read"), model_db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="shop", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="sales"))
        row = (
            await conn.execute_core(
                insert(registered_tables)
                .values(
                    source_id="shop", domain_id="sales", schema_name="public", table_name="orders"
                )
                .returning(registered_tables.c.id)
            )
        ).fetchone()
    assert row is not None
    return row[0]


async def _meta_view(model_db, table: str) -> None:
    """The model's meta view of ``table``, as a node's seed creates it."""
    from provisa.api._meta_views import _META_TABLE_VIEWS
    from provisa.api.startup_seed import _seed_view

    async with model_db.acquire() as conn:
        await _seed_view(conn, _META_TABLE_VIEWS[table])


@pytest.mark.parametrize(
    ("table_catalogs", "catalog"),
    [(None, "shop_pg"), ({("shop", "orders"): "elsewhere"}, "elsewhere")],
)
def test_a_table_given_a_catalog_of_its_own_is_read_under_it(table_catalogs, catalog):
    """The compiler reads a table under the catalog its schema input names for it, else under
    its source's: how a region's node reads these views under theirs."""
    from provisa.compiler.context import build_context
    from provisa.compiler.schema_gen import SchemaInput
    from tests.unit.test_sql_gen import _col

    si = SchemaInput(
        tables=[
            {
                "id": 1,
                "source_id": "shop",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "orders",
                "columns": [{"column_name": "id", "visible_to": ["admin"]}],
                "write_ops": [],
                "write_returns_rows": False,
                "write_refused_forms": [],
            }
        ],
        relationships=[],
        column_types={1: [_col("id", "integer")]},
        naming_rules=[],
        role={"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
        source_catalogs={"shop": "shop_pg"},
        table_catalogs=table_catalogs,
    )
    assert build_context(si).tables["orders"].catalog_name == catalog


def test_a_statement_reading_one_of_these_views_in_a_region_is_the_engines():
    """The provisa-admin terminal reads the model's database alone; an ops view a region's node
    reads under the region's catalog is read by the engine, which attaches the region's stores."""
    from provisa.compiler.sql_types import CompilationContext, TableMeta

    def _meta(table_id: int, name: str, catalog: str) -> TableMeta:
        return TableMeta(
            table_id=table_id,
            field_name=name,
            type_name=name.title(),
            source_id="provisa-admin",
            catalog_name=catalog,
            schema_name="org_acme",
            table_name=name,
            domain_id="ops",
        )

    ctx = CompilationContext()
    ctx.tables["queries"] = _meta(7, "queries", ops.CATALOG)
    ctx.tables["roles"] = _meta(8, "roles", "provisa_admin")
    assert ops.reads_region_views(ctx, (7, 8))
    assert not ops.reads_region_views(ctx, (8,))
