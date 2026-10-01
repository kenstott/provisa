# Copyright (c) 2026 Kenneth Stott
# Canary: 2a8e6f13-b4d7-4c90-8e25-6f1a9d3c7b04
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Replication writes only into an ordinary table of the store (REQ-826, REQ-1141, REQ-030).

The Postgres side of the guard needs a real server and is covered in
``tests/integration/test_pg_engine_landing_never_writes_source_e2e.py``. Here: the named error,
the DuckDB store's check, the DuckDB engine's switch between a live and a replicated table in
both directions, and the state of a table whose replica could not be reconciled."""

# Requirements: REQ-826, REQ-1141, REQ-030

from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest

from provisa.core.models import SourceType
from provisa.federation.replica_guard import (
    ReplicaTargetError,
    ReplicaUnavailable,
    require_duckdb_replica_table,
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


# -- the DuckDB engine: one relation per physical name, whichever way the table switches --------------


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


async def _replicate(runtime, source, rows: list[dict]) -> None:
    from provisa.federation.duckdb_runtime import _mat_table_name

    await runtime.attach_landed_source(source, _COLUMNS, pk_columns=["id"])
    await runtime.land_table(
        schema=runtime._store_schema(),
        table=_mat_table_name(source),
        columns=_COLUMNS,
        rows=rows,
        pk_columns=["id"],
    )


async def test_duckdb_switches_a_table_from_live_to_its_replica_and_back(tmp_path):
    """The replica holds one row and the source five, so each read says which one answered."""
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    runtime = DuckDBFederationRuntime(materialize_dsn=f"duckdb:///{tmp_path / 'mat.duckdb'}")
    source = _sqlite_source(tmp_path)
    physical = runtime._phys_name(source)

    async def _rows() -> int:
        return (await runtime.run(f"SELECT COUNT(*) FROM {physical}")).rows[0][0]

    runtime.attach_source(source)
    assert await _rows() == 5  # live
    await _replicate(runtime, source, [{"id": 1, "amount": 1.5}])
    assert await _rows() == 1  # the replica
    runtime.attach_source(source)
    assert await _rows() == 5  # live again: the replica's exposure is gone
    await _replicate(runtime, source, [{"id": 1, "amount": 1.5}, {"id": 2, "amount": 3.0}])
    assert await _rows() == 2  # and replicated again


# -- a replica that cannot be reconciled -------------------------------------------------------------


async def test_a_table_whose_replica_cannot_be_reconciled_is_not_read(monkeypatch):
    """Reconcile records the failure against that table, goes on to the others, and a read of
    the table's source is refused with the reason — never answered by whatever is at its name."""
    from provisa.federation import backend as backend_mod
    from provisa.federation.engine import build_engine
    from provisa.federation.native_backend import NativeEngineBackend

    refused = ReplicaTargetError(
        '"src_public"."orders"', "foreign table", "reconcile the replica at"
    )
    attempts: list[str] = []

    class _Runtime:
        async def attach_landed_source(self, source, columns, *, pk_columns=None):
            attempts.append(source.table_name)
            if source.table_name == "orders":
                raise refused

    async def _worklist(engine, state):
        src = SimpleNamespace(id="src", type=SourceType.postgresql)
        other = SimpleNamespace(id="other", type=SourceType.postgresql)
        return [
            (src, "public", "orders", [("id", "integer")], ["id"]),
            (other, "public", "customers", [("id", "integer")], ["id"]),
        ]

    monkeypatch.setattr(backend_mod, "landing_worklist", _worklist)
    backend = NativeEngineBackend(build_engine("pg"))
    monkeypatch.setattr(backend, "_runtime_for", lambda state: _Runtime())

    reconciled = await backend.reconcile_landed_tables(SimpleNamespace())
    assert attempts == ["orders", "customers"]  # the failure did not stop the other table
    assert reconciled == [("other", "customers")]

    backend.require_reconciled(["other"])  # a source with no failed replica reads normally
    with pytest.raises(ReplicaUnavailable) as unavailable:
        backend.require_reconciled(["other", "src"])
    assert unavailable.value.__cause__ is refused
    assert "source 'src' cannot be read" in str(unavailable.value)
    assert "'orders' could not be reconciled" in str(unavailable.value)

    # A later reconcile that succeeds clears the state.
    monkeypatch.setattr(_Runtime, "attach_landed_source", _ok)
    await backend.reconcile_landed_tables(SimpleNamespace())
    backend.require_reconciled(["other", "src"])


async def _ok(self, source, columns, *, pk_columns=None):
    return "kept"
