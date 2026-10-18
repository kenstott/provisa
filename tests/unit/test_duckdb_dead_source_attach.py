# Copyright (c) 2026 Kenneth Stott
# Canary: 52e958da-d94c-4e57-8dd2-740adfd924e6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A source whose server has gone fails nothing of another source's on DuckDB.

DuckDB reads every attached database for any catalog listing (``information_schema``,
``duckdb_columns()``). A PostgreSQL source left attached after its server went away failed every
later listing with "Unable to connect to Postgres": the replica copy's vector-index check listed
columns that way on every read of a landed table, so a deleted source failed the reads of every
other source's replicas. A source still registered while its server is down (#256) is set aside
by the runtime's listings and attached again by the next read of it.
"""

from __future__ import annotations

from types import SimpleNamespace

import duckdb
import psycopg
import pytest

import provisa.api.app  # noqa: F401 -- the API layer says whether the deployment holds many orgs, which reading a source's secrets asks
from provisa.core.models import SourceType
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime, SourceUnreachable
from tests.embedded_pg import DATABASE, PASSWORD, ROLE, start_local_postgres


def _source(source_id: str, pg) -> SimpleNamespace:
    """A table of a PostgreSQL source, as the attach walk hands it over
    (native_backend._attach_tbl)."""
    return SimpleNamespace(
        id=source_id,
        type=SourceType.postgresql,
        catalog=source_id,
        host="127.0.0.1",
        port=pg.port,
        database=DATABASE,
        username=ROLE,
        password=PASSWORD,
        federation_hints={},
        mapping={},
        max_live_concurrency=None,
        schema_name="public",
        table_name="widgets",
    )


def _with_widgets(pg) -> None:
    with psycopg.connect(pg.dsn(), autocommit=True) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS widgets (id INTEGER)")


@pytest.fixture
def gone_source():
    """A runtime holding a live attach of a PostgreSQL source, whose server is then stopped."""
    pg = start_local_postgres()
    try:
        _with_widgets(pg)
        source = _source("e2e_postgresql_1", pg)
        runtime = DuckDBFederationRuntime()
        runtime.attach_source(source)
    finally:
        pg.stop()
    yield runtime, source
    runtime.close()


@pytest.fixture
def down_and_up():
    """A runtime holding two PostgreSQL sources, the first of whose servers is then stopped: the
    runtime, the down source and its server (to start again), the live source."""
    down, up = start_local_postgres(), start_local_postgres()
    try:
        for pg in (down, up):
            _with_widgets(pg)
        runtime = DuckDBFederationRuntime()
        down_source, up_source = _source("pg_down", down), _source("pg_up", up)
        runtime.attach_source(down_source)
        runtime.attach_source(up_source)
        down._ctl("stop", "-m", "fast")  # its data kept, to start again
        yield runtime, down_source, down, up_source
        runtime.close()
    finally:
        if not (down.data / "postmaster.pid").exists():
            down._start(())  # stop() stops a running server and removes its directory
        down.stop()
        up.stop()


def _listing(runtime: DuckDBFederationRuntime) -> list:
    return runtime._con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'landed'"
    ).fetchall()


def test_a_dropped_sources_attach_is_let_go(gone_source):
    runtime, source = gone_source
    runtime._con.execute("CREATE TABLE landed (id INTEGER)")
    with pytest.raises(duckdb.IOException, match="Unable to connect to Postgres"):
        _listing(runtime)  # the reproducer: the dead attach fails an unrelated listing
    runtime.drop_source(source.id)
    assert _listing(runtime) == [("id",)]
    databases = runtime._con.execute("SELECT database_name FROM duckdb_databases()").fetchall()
    attached = {row[0] for row in databases}
    assert not {a for a in attached if source.id in a}


def test_a_replica_copy_indexes_its_vectors_with_a_dead_source_attached(gone_source):
    runtime = gone_source[0]
    cur = runtime._con.cursor()
    cur.execute("CREATE TABLE landed (id INTEGER, label VARCHAR)")
    runtime._index_replica_vectors(cur, "landed", "replicas__landed")  # no listing of every catalog


def test_a_down_source_fails_no_listing_of_another(down_and_up):
    runtime, _down, _server, up = down_and_up
    assert "public" in runtime.introspect_schemas(up)
    assert runtime.introspect_tables(up, "public") == ["widgets"]
    runtime.detach_source(up)  # its view is found in the catalog and dropped
    runtime.attach_source(up)


def test_a_read_of_the_down_source_is_refused_by_its_name(down_and_up):
    runtime, down, _server, up = down_and_up
    runtime.introspect_schemas(up)  # sets the down source aside
    with pytest.raises(SourceUnreachable, match="'pg_down' cannot be reached") as refused:
        runtime.run_sync(f'SELECT * FROM "{down.id}"."public"."widgets"')
    assert refused.value.params == {"source": "pg_down"}
    assert "password" not in str(refused.value)


def test_a_down_source_is_read_again_once_its_server_is_back(down_and_up):
    runtime, down, server, up = down_and_up
    runtime.introspect_schemas(up)  # sets the down source aside
    server._start(())
    stream = runtime.run_sync(f'SELECT count(*) FROM "{down.id}"."public"."widgets"')
    assert [tuple(r) for r in stream.rows] == [(0,)]
