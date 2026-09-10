# Copyright (c) 2026 Kenneth Stott
# Canary: 2e987179-b398-466e-b39f-37ff9f2b63c5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-954: the `files` landing path for an engine that has NO connector of its own for it.

DuckDB reaches `files` natively (DuckDBFilesConnector — a `read_csv_auto` scanner view, REQ-229);
Trino reaches it through its own Hive/file connector. Every other engine (any plain SQLAlchemy
target — Postgres/MySQL/etc, REQ-905) has neither, so `provisa.federation.pgwire_replica` lands a
replica through the connector's bundled Calcite pgwire server instead: `ConnectorReplica.load()`
(or the `make_pgwire_loader` adapter it backs) starts the REAL `pgwire-file` bundle (a real JVM),
configures it against a CSV directory, and SELECTs the landed rows back over a real Postgres wire
connection (asyncpg) — the same bundle and mechanism ``test_duckdb_attach_calcite_pgwire.py``
verifies for DuckDB's *live-attach* path (REQ-1690); this is the *land* path (REQ-954) for engines
with no attach reach at all.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation import pgwire_replica as pr
from provisa.federation.engine import build_duckdb_engine

pytestmark = [pytest.mark.integration]


@pytest.fixture
def csv_dir(tmp_path: Path) -> Path:
    d = tmp_path / "data"
    d.mkdir()
    (d / "items.csv").write_text("id,name,qty\n1,alpha,10\n2,beta,20\n3,gamma,30\n")
    return d


@pytest.fixture
def source(csv_dir: Path) -> Source:
    return Source(id="repl-files", type=SourceType.files, path=str(csv_dir))


@pytest.fixture
def replica(source: Source):
    r = pr.ConnectorReplica(source)
    try:
        yield r
    finally:
        r.close()


def test_connector_replica_lands_real_rows(replica: pr.ConnectorReplica):
    rows = _run(replica.load("items"))
    assert sorted(rows, key=lambda r: r["id"]) == [
        {"id": 1, "name": "alpha", "qty": 10},
        {"id": 2, "name": "beta", "qty": 20},
        {"id": 3, "name": "gamma", "qty": 30},
    ]


def test_connector_replica_reuses_one_server_across_loads(replica: pr.ConnectorReplica):
    """Two loads of the same source hit the same started server (one port, one JVM)."""
    _run(replica.load("items"))
    server_first = replica._server  # noqa: SLF001 - internal state, asserted for the reuse guarantee
    assert server_first is not None
    _run(replica.load("items"))
    assert replica._server is server_first  # noqa: SLF001


def test_make_pgwire_loader_lands_via_the_same_path(source: Source):
    """The TYPE-level adapter loader (what build_adapter_loaders registers for a source whose
    engine needs the bridge, REQ-954) drives the identical ConnectorReplica machinery."""
    loader = pr.make_pgwire_loader()
    try:
        rows = _run(loader(source, "items"))
        assert sorted(rows, key=lambda r: r["id"])[0] == {"id": 1, "name": "alpha", "qty": 10}
    finally:
        pr.stop_all_servers()


# -- REQ-954: engine routing, against REAL engine builders -----------------------------------
#
# needs_pgwire_replica(source, engine) gates the bridge on whether the engine reads the type LIVE
# through a connector of its own (engine_attaches). DuckDB's DuckDBFilesConnector (REQ-229) is a
# native CSV SCAN, so the bridge is skipped there. Every self-only warehouse builder in engine.py
# (build_sqlalchemy_engine, build_pg_engine, snowflake, databricks, bigquery, mssql) synthesizes a
# FETCH WarehouseNativeConnector placeholder for the pgwire-replica types at construction; that
# placeholder is the landing path, not a reader, so the bridge is still needed (issue #114).


def test_needs_pgwire_replica_true_for_an_engine_with_no_connector(source: Source):
    class _BareEngine:
        connectors: dict = {}

    assert pr.needs_pgwire_replica(source, _BareEngine()) is True


def test_needs_pgwire_replica_true_for_a_warehouse_engine_with_the_land_placeholder(source: Source):
    from provisa.federation.engine import build_sqlalchemy_engine

    assert pr.needs_pgwire_replica(source, build_sqlalchemy_engine("mysql://h/db")) is True


def test_needs_pgwire_replica_false_for_duckdb_native_files_connector(source: Source):
    assert pr.needs_pgwire_replica(source, build_duckdb_engine()) is False


def _run(coro):
    import asyncio

    return asyncio.run(coro)
