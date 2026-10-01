# Copyright (c) 2026 Kenneth Stott
# Canary: 1f7c3a92-5e84-4d06-b2a9-8c6e0d3f4b71
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1901: the API-result cache terminal (``isolated_sync``) of an engine whose DuckDB-file store
is held by the store broker runs its statements against the store file, never as ``mat_store.*``
SQL on the engine connection (which never ATTACHes that store)."""

# Requirements: REQ-1901

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import duckdb

from provisa.executor.session import StoreBrokerSession
from provisa.federation.materialize_broker import get_broker


def test_cache_statements_run_in_the_store_file(tmp_path: Path) -> None:
    store = tmp_path / "materialize.duckdb"
    session = StoreBrokerSession(get_broker(str(store)))

    session.execute('CREATE SCHEMA IF NOT EXISTS mat_store."org_x_gql_cache"').fetchall()
    session.execute('CREATE TABLE mat_store."org_x_gql_cache"."t" (id INTEGER)').fetchall()
    session.execute('INSERT INTO mat_store."org_x_gql_cache"."t" VALUES (1), (2)').fetchall()
    assert session.execute(
        'SELECT id FROM mat_store."org_x_gql_cache"."t" ORDER BY id'
    ).fetchall() == [(1,), (2,)]

    con = duckdb.connect(str(store), read_only=True)
    try:
        assert con.execute('SELECT count(*) FROM "org_x_gql_cache"."t"').fetchone() == (2,)
    finally:
        con.close()

    session.execute('DROP TABLE IF EXISTS mat_store."org_x_gql_cache"."t"').fetchall()
    con = duckdb.connect(str(store), read_only=True)
    try:
        assert con.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE schema_name = 'org_x_gql_cache'"
        ).fetchone() == (0,)
    finally:
        con.close()


def test_a_value_binds_at_the_question_mark(tmp_path: Path) -> None:
    session = StoreBrokerSession(get_broker(str(tmp_path / "materialize.duckdb")))
    assert session.placeholder == "?"
    assert session.execute("SELECT ?", ["a'b"]).fetchall() == [("a'b",)]


def test_the_native_cache_terminal_uses_the_broker_for_a_duckdb_file_store(
    tmp_path: Path,
) -> None:
    from provisa.executor.session import EngineSession
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime
    from provisa.federation.native_backend import NativeEngineBackend

    rt = DuckDBFederationRuntime(materialize_dsn=f"duckdb:///{tmp_path / 'materialize.duckdb'}")
    backend = NativeEngineBackend.__new__(NativeEngineBackend)
    # The session carries the engine's dialect, which the backend reads off its engine.
    backend.engine = SimpleNamespace(native_store="duckdb", name="duckdb")  # type: ignore[assignment]
    backend._runtime_for = lambda _state: rt  # type: ignore[method-assign]
    with backend.isolated_sync(None) as session:
        assert isinstance(session, StoreBrokerSession)
        session.execute('CREATE SCHEMA IF NOT EXISTS mat_store."s"')

    rt_pg = DuckDBFederationRuntime(materialize_dsn="sqlite:///" + str(tmp_path / "m.db"))
    backend._runtime_for = lambda _state: rt_pg  # type: ignore[method-assign]
    with backend.isolated_sync(None) as session:
        assert isinstance(session, EngineSession)
        assert (session.dialect, session.placeholder) == ("duckdb", None)
