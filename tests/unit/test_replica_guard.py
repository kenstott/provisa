# Copyright (c) 2026 Kenneth Stott
# Canary: 2a8e6f13-b4d7-4c90-8e25-6f1a9d3c7b04
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Replication writes only into an ordinary table of the replicas schema (REQ-1912, REQ-826,
REQ-1141, REQ-030).

The Postgres side of the guard needs a real server and is covered in
``tests/integration/test_pg_engine_landing_never_writes_source_e2e.py``. Here: the named errors,
the schema check, the DuckDB store's check, a DuckDB table's live relation and its replica as two
separate objects, and the state of a table whose replica could not be reconciled."""

# Requirements: REQ-1912, REQ-826, REQ-1141, REQ-030

from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest

from provisa.core.models import SourceType
from provisa.federation.replica_guard import (
    ReplicaSurfaceError,
    ReplicaTargetError,
    ReplicaUnavailable,
    refuse_live_in_write_surface,
    require_duckdb_replica_table,
    require_replicas_schema,
)


def test_the_error_names_the_relation_what_it_is_and_what_a_write_would_reach():
    error = ReplicaTargetError(
        '"src_public"."orders"',
        "view",
        "write the replica",
        "fdw_src.orders on foreign server src (host=db, dbname=shop)",
    )
    assert str(error) == (
        'refusing to write the replica "src_public"."orders": a replica is written only into an '
        'ordinary table of the store, and "src_public"."orders" is a view, which reads '
        "fdw_src.orders on foreign server src (host=db, dbname=shop). Writing it would write "
        "into the source, so nothing was written."
    )
    assert (error.kind, error.action) == ("view", "write the replica")


# -- where a replica may be written, and where a live attach may not be (REQ-1912) ------------------


@pytest.mark.parametrize("schema", ["org_acme_replicas", "org_acme_env_feature_x_replicas"])
def test_a_replica_write_into_a_replicas_schema_is_accepted(schema):
    require_replicas_schema(schema, "src__public__orders", action="write the replica")


@pytest.mark.parametrize(
    "schema",
    [
        "public",  # a source's own schema
        "src_public",  # a live attach's folded schema
        "org_acme",  # the control plane
        "org_acme_mv_cache",  # materialized views have a schema of their own
        "org_acme_api_cache",
        "mat",
    ],
)
def test_a_replica_write_anywhere_else_is_refused_naming_the_target(schema):
    with pytest.raises(ReplicaSurfaceError) as refused:
        require_replicas_schema(schema, "orders", action="write the replica")
    assert str(refused.value) == (
        f'refusing to write the replica "{schema}"."orders": a replica is written only into the '
        f'replicas schema, and "{schema}" is not it. Nothing was written.'
    )


@pytest.mark.parametrize("schema", ["org_acme_replicas", "org_acme_env_dev_mv_cache"])
def test_a_live_attach_is_never_created_in_a_schema_provisa_writes(schema):
    with pytest.raises(ReplicaSurfaceError, match="which holds nothing else"):
        refuse_live_in_write_surface(schema, "orders")


def test_a_live_attach_in_a_sources_own_schema_is_accepted():
    refuse_live_in_write_surface("src_public", "orders")
    refuse_live_in_write_surface("replicas", "orders")  # a source may name a schema this


# -- the DuckDB store ------------------------------------------------------------------------------


def test_the_duckdb_store_refuses_a_view_at_the_replicas_name():
    import duckdb

    from provisa.federation.store_connection import land_duckdb_native, reconcile_duckdb_native

    con = duckdb.connect()
    con.execute("CREATE SCHEMA mat")
    con.execute("CREATE TABLE elsewhere (id INTEGER)")
    con.execute("INSERT INTO elsewhere VALUES (1), (2)")
    con.execute("CREATE VIEW mat.orders AS SELECT * FROM elsewhere")
    columns = [("id", "integer")]
    with pytest.raises(ReplicaTargetError, match="is a view"):
        reconcile_duckdb_native(
            con, catalog="memory", schema="mat", table="orders", columns=columns
        )
    with pytest.raises(ReplicaTargetError, match="is a view"):
        land_duckdb_native(
            con, catalog="memory", schema="mat", table="orders", columns=columns, rows=[{"id": 9}]
        )
    assert con.execute("SELECT id FROM elsewhere ORDER BY id").fetchall() == [(1,), (2,)]
    # A table, or nothing, is what a replica write expects.
    require_duckdb_replica_table(con, "memory", "mat", "elsewhere_not_there", action="write")
    require_duckdb_replica_table(con, "memory", "main", "elsewhere", action="write")


@pytest.mark.parametrize(
    "write",
    ["persist", "apply_cdc", "upsert_arrow", "ensure_row_cache_table", "tombstone_row_cache"],
)
def test_every_duckdb_store_writer_refuses_a_view_at_the_replicas_name(write):
    import duckdb
    import pyarrow as pa

    from provisa.federation import store_connection as sc

    con = duckdb.connect()
    con.execute("CREATE SCHEMA mat")
    con.execute("CREATE TABLE elsewhere (id INTEGER, amount DOUBLE)")
    con.execute("INSERT INTO elsewhere VALUES (1, 1.5), (2, 3.0)")
    con.execute("CREATE VIEW mat.orders AS SELECT * FROM elsewhere")
    target = {"catalog": "memory", "schema": "mat", "table": "orders"}
    columns = [("id", "integer"), ("amount", "double")]
    calls = {
        "persist": lambda: sc.persist_duckdb_native(
            con, **target, columns=columns, rows=[{"id": 9, "amount": 0.0}], persist="replace"
        ),
        "apply_cdc": lambda: sc.apply_cdc_duckdb_native(
            con, **target, columns=columns, pk_columns=["id"], events=[]
        ),
        "upsert_arrow": lambda: sc.upsert_arrow_duckdb_native(
            con,
            **target,
            columns=columns,
            pk_columns=["id"],
            data=pa.table({"id": [9], "amount": [0.0]}),
        ),
        "ensure_row_cache_table": lambda: sc.ensure_row_cache_table_duckdb_native(
            con, **target, columns=columns
        ),
        "tombstone_row_cache": lambda: sc.tombstone_row_cache_duckdb_native(
            con, **target, pk_columns=["id"], keys=[(1,)]
        ),
    }
    with pytest.raises(ReplicaTargetError, match="is a view"):
        calls[write]()
    assert con.execute("SELECT * FROM elsewhere ORDER BY id").fetchall() == [(1, 1.5), (2, 3.0)]


# -- the DuckDB engine: a table's live relation and its replica are two objects (REQ-1912) -----------


def _sqlite_source(tmp_path) -> SimpleNamespace:
    database = tmp_path / "shop.sqlite"
    conn = sqlite3.connect(database)
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL)")
    conn.executemany("INSERT INTO orders VALUES (?, ?)", [(i, i * 1.5) for i in range(1, 6)])
    conn.commit()
    conn.close()
    return SimpleNamespace(
        id="src",
        type=SourceType.sqlite,
        path=str(database),
        host=None,
        port=None,
        database=None,
        username=None,
        password=None,
        base_url=None,
        federation_hints={},
        mapping={},
        schema_name="main",
        table_name="orders",
    )


_COLUMNS = [("id", "integer"), ("amount", "double")]
_REPLICA = ("org_acme_replicas", "src__main__orders")


async def _replicate(runtime, rows: list[dict]) -> None:
    schema, table = _REPLICA
    await runtime.reconcile_replica(schema=schema, table=table, columns=_COLUMNS, pk_columns=["id"])
    await runtime.land_table(
        schema=schema, table=table, columns=_COLUMNS, rows=rows, pk_columns=["id"]
    )


async def test_duckdb_reads_a_table_live_at_its_name_and_its_replica_at_its_address(tmp_path):
    """The replica holds one row and the source five, so each read says which one answered. The
    two never share a name: replicating a table changes nothing at its live name, and removing
    the live attach changes nothing at the replica's address."""
    import duckdb

    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    runtime = DuckDBFederationRuntime(materialize_dsn=f"duckdb:///{tmp_path / 'mat.duckdb'}")
    source = _sqlite_source(tmp_path)
    physical = runtime._phys_name(source)
    replica = f'mat_store."{_REPLICA[0]}"."{_REPLICA[1]}"'

    async def _rows(relation: str) -> int:
        return (await runtime.run(f"SELECT COUNT(*) FROM {relation}")).rows[0][0]

    runtime.attach_source(source)
    assert await _rows(physical) == 5  # live
    await _replicate(runtime, [{"id": 1, "amount": 1.5}])
    assert await _rows(replica) == 1  # the replica, where it lives
    assert await _rows(physical) == 5  # the live name still reads the source
    runtime.detach_source(source)  # the table's reads moved to its replica
    with pytest.raises(duckdb.Error):
        await _rows(physical)  # nothing stands at the live name: the source cannot be read
    assert await _rows(replica) == 1
    runtime.attach_source(source)
    assert await _rows(physical) == 5  # live again
    await _replicate(runtime, [{"id": 1, "amount": 1.5}, {"id": 2, "amount": 3.0}])
    assert await _rows(replica) == 2  # and replicated again


# -- a replica that cannot be reconciled -------------------------------------------------------------


async def test_a_table_whose_replica_cannot_be_reconciled_is_not_read(monkeypatch):
    """Reconcile records the failure against that table and goes on to the others. A read that
    names the table is refused with the reason — never answered by anything else; a read of
    another table is addressed to its replica as usual."""
    from provisa.federation import replica_routing
    from provisa.federation.engine import build_engine
    from provisa.federation.native_backend import NativeEngineBackend
    from provisa.federation.replica_address import (
        ReplicaRoute,
        ReplicaRoutes,
        address_replicas,
    )

    refused = ReplicaTargetError(
        '"org_acme_replicas"."src__public__orders"', "foreign table", "reconcile the replica at"
    )
    attempts: list[str] = []

    class _Runtime:
        async def reconcile_replica(self, *, schema, table, columns, pk_columns=None):
            attempts.append(table)
            if table == "src__public__orders":
                raise refused
            return "created"

    async def _worklist(engine, state):
        src = SimpleNamespace(id="src", type=SourceType.postgresql)
        other = SimpleNamespace(id="other", type=SourceType.postgresql)
        return [
            (src, "public", "orders", [("id", "integer")], ["id"]),
            (other, "public", "customers", [("id", "integer")], ["id"]),
        ]

    monkeypatch.setattr(replica_routing, "landing_worklist", _worklist)
    backend = NativeEngineBackend(build_engine("pg"))
    monkeypatch.setattr(backend, "_runtime_for", lambda state: _Runtime())
    state = SimpleNamespace(org_id="acme")

    reconciled = await backend.reconcile_landed_tables(state)
    # the failure did not stop the other table
    assert attempts == ["src__public__orders", "other__public__customers"]
    assert reconciled == [("other", "customers")]

    # The routes a read is addressed with hold the backend's own record of the failure.
    routes = ReplicaRoutes(
        engine_name="pg",
        routes={
            (None, "src_public", "orders"): ReplicaRoute(
                "src", "orders", (None, "org_acme_replicas", "src__public__orders")
            ),
            (None, "other_public", "customers"): ReplicaRoute(
                "other", "customers", (None, "org_acme_replicas", "other__public__customers")
            ),
        },
        unreconciled=backend.unreconciled,
    )
    # a table with no failed replica reads normally, at its replica
    assert (
        address_replicas('SELECT * FROM "other_public"."customers" AS "c"', routes)
        == 'SELECT * FROM "org_acme_replicas"."other__public__customers" AS "c"'
    )
    with pytest.raises(ReplicaUnavailable) as unavailable:
        address_replicas(
            'SELECT * FROM "other_public"."customers" AS "c" '
            'JOIN "src_public"."orders" AS "o" ON "o"."id" = "c"."id"',
            routes,
        )
    assert unavailable.value.__cause__ is refused
    assert "table 'orders' of source 'src' cannot be read" in str(unavailable.value)
    assert "could not be reconciled" in str(unavailable.value)

    # A later reconcile that succeeds clears the state.
    monkeypatch.setattr(_Runtime, "reconcile_replica", _ok)
    await backend.reconcile_landed_tables(state)
    assert (
        address_replicas('SELECT * FROM "src_public"."orders" AS "o"', routes)
        == 'SELECT * FROM "org_acme_replicas"."src__public__orders" AS "o"'
    )


async def _ok(self, *, schema, table, columns, pk_columns=None):
    return "kept"
