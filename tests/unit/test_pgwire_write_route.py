# Copyright (c) 2026 Kenneth Stott
# Canary: 8b3e5d17-2c94-4f60-a1d8-6e0f7b9c4a25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1946: a SharePoint or Salesforce source is written through its own pgwire server, on every
engine; a files or Splunk source, which has such a server too, is not written."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.data import pgwire_write
from provisa.core.models import Source, SourceType
from provisa.core.source_registry import SOURCE_TO_DIALECT
from provisa.executor.drivers.registry import has_driver
from provisa.executor.writable import (
    PGWIRE_SERVER_WRITTEN,
    WritePath,
    is_written_through_pgwire_server,
    resolve_write_path,
)
from provisa.executor.write_capability import table_write_ops
from provisa.federation import pgwire_replica as pr
from provisa.federation.engine import (
    build_clickhouse_engine,
    build_duckdb_engine,
    build_pg_engine,
    build_trino_engine,
)
from provisa.transpiler.router import Route, decide_route

_aio = pytest.mark.asyncio(loop_scope="session")

_ENGINES = [
    build_trino_engine,
    build_duckdb_engine,
    build_pg_engine,
    build_clickhouse_engine,
]


def _salesforce() -> Source:
    return Source(
        id="sf-sales",
        type=SourceType.salesforce,
        base_url="https://acme.my.salesforce.com",
        username="key",
        password="secret",
    )


def test_the_two_types_written_through_their_server():
    assert PGWIRE_SERVER_WRITTEN == {"sharepoint", "salesforce"}
    assert PGWIRE_SERVER_WRITTEN <= pr.PGWIRE_REPLICA_TYPES


@pytest.mark.parametrize("source_type", ["sharepoint", "salesforce"])
@pytest.mark.parametrize("build", _ENGINES)
def test_written_on_every_engine(source_type, build):
    engine = build()
    assert resolve_write_path(source_type, engine) is WritePath.PGWIRE
    assert table_write_ops({"table_name": "Account"}, source_type, engine) == {
        "insert",
        "update",
        "delete",
    }


@pytest.mark.parametrize("source_type", ["sharepoint", "salesforce"])
def test_the_route_does_not_depend_on_an_engine(source_type):
    assert is_written_through_pgwire_server(source_type)
    assert resolve_write_path(source_type, None) is WritePath.PGWIRE


@pytest.mark.parametrize("source_type", ["files", "splunk"])
@pytest.mark.parametrize("build", _ENGINES)
def test_a_files_or_splunk_source_takes_no_writes(source_type, build):
    engine = build()
    assert not is_written_through_pgwire_server(source_type)
    assert resolve_write_path(source_type, engine) is None
    assert table_write_ops({"table_name": "t"}, source_type, engine) == frozenset()


@pytest.mark.parametrize("source_type", ["sharepoint", "salesforce"])
def test_the_server_takes_postgresql(source_type):
    assert SOURCE_TO_DIALECT[source_type] == "postgres"


@pytest.mark.parametrize("source_type", ["sharepoint", "salesforce"])
def test_a_read_is_never_routed_to_the_server(source_type):
    # No direct driver is registered for the type, so a single-source read goes to the engine.
    assert not has_driver(source_type)
    decision = decide_route(
        sources={"s"},
        source_types={"s": source_type},
        source_dialects={"s": "postgres"},
        operator_floor={},
    )
    assert decision.route is Route.ENGINE


@pytest.mark.parametrize("source_type", ["sharepoint", "salesforce"])
def test_a_write_routes_direct(source_type):
    decision = decide_route(
        sources={"s"},
        source_types={"s": source_type},
        source_dialects={"s": "postgres"},
        operator_floor={},
        is_mutation=True,
    )
    assert decision.route is Route.DIRECT
    assert decision.source_id == "s"


# -- the pool a write runs on ------------------------------------------------------------------


class _Pools:
    def __init__(self) -> None:
        self.added: list[dict] = []

    def has(self, source_id: str) -> bool:
        return any(a["source_id"] == source_id for a in self.added)

    async def add(self, **kw) -> None:
        self.added.append(kw)


def _state(source_type: str = "salesforce") -> SimpleNamespace:
    return SimpleNamespace(source_types={"sf-sales": source_type}, source_pools=_Pools())


@pytest.fixture
def registered(monkeypatch):
    from provisa.api.admin import schema_query

    async def _source(source_id: str):
        return _salesforce() if source_id == "sf-sales" else None

    monkeypatch.setattr(schema_query, "_source_for_introspection", _source)


@_aio
async def test_the_pool_is_the_postgresql_driver_on_the_servers_endpoint(monkeypatch, registered):
    ports = pr.PortPair(pgwire_port=5999, calcite_child_host="127.0.0.1", calcite_child_port=6099)
    monkeypatch.setattr(pr, "ensure_endpoint_for_discovery", lambda source: ports)
    state = _state()
    await pgwire_write.ensure_write_pool(state, "sf-sales")
    assert state.source_pools.added == [
        {
            "source_id": "sf-sales",
            "source_type": "postgresql",
            "host": "127.0.0.1",
            "port": 5999,
            "database": "provisa",
            "user": "provisa",
            "password": "",
        }
    ]


@_aio
async def test_an_open_pool_is_kept(monkeypatch, registered):
    calls: list[str] = []
    ports = pr.PortPair(pgwire_port=5999, calcite_child_host="127.0.0.1", calcite_child_port=6099)

    def _endpoint(source):
        calls.append(source.id)
        return ports

    monkeypatch.setattr(pr, "ensure_endpoint_for_discovery", _endpoint)
    state = _state()
    await pgwire_write.ensure_write_pool(state, "sf-sales")
    await pgwire_write.ensure_write_pool(state, "sf-sales")
    assert calls == ["sf-sales"]
    assert len(state.source_pools.added) == 1


@_aio
async def test_a_write_while_the_server_starts_is_answered_not_held(monkeypatch, registered):
    def _starting(source):
        raise pr.SourceStillStartingError(source.id)

    monkeypatch.setattr(pr, "ensure_endpoint_for_discovery", _starting)
    state = _state()
    with pytest.raises(pr.SourceStillStartingError, match="STARTING:"):
        await pgwire_write.ensure_write_pool(state, "sf-sales")
    assert state.source_pools.added == []


@_aio
@pytest.mark.parametrize("source_type", ["postgresql", "files", "splunk"])
async def test_no_pool_is_opened_for_any_other_type(monkeypatch, source_type):
    def _never(source):
        raise AssertionError("no server is asked for")

    monkeypatch.setattr(pr, "ensure_endpoint_for_discovery", _never)
    state = _state(source_type)
    await pgwire_write.ensure_write_pool(state, "sf-sales")
    assert state.source_pools.added == []


# -- the server's lifecycle ----------------------------------------------------------------------


def test_starting_the_server_does_not_wait_for_it_to_listen(monkeypatch):
    started: list[str] = []

    class _Replica:
        def __init__(self, source, **kw) -> None:
            self.id = source.id

        def start(self) -> None:
            started.append(self.id)

        def endpoint(self, **kw):
            raise AssertionError("readiness is not asked for")

        def close(self) -> None:
            started.remove(self.id)

    monkeypatch.setattr(pr, "ConnectorReplica", _Replica)
    monkeypatch.setattr(pr, "_ENDPOINTS", {})
    monkeypatch.setattr(pr, "_reap_on_exit", lambda: None)
    pr.start_endpoint(_salesforce())
    pr.start_endpoint(_salesforce())
    assert started == ["sf-sales", "sf-sales"]  # asked twice of ONE replica
    assert list(pr._ENDPOINTS) == ["sf-sales"]
    pr.stop_endpoint("sf-sales")
    assert pr._ENDPOINTS == {}


def test_the_write_server_is_started_only_for_a_type_written_through_one(monkeypatch):
    from provisa.core import connection_loop

    spawned: list[str] = []

    def _spawn(coro, *, name=None):
        coro.close()
        spawned.append(name)

    monkeypatch.setattr(connection_loop, "spawn_background", _spawn)
    pgwire_write.start_write_server(_salesforce())
    pgwire_write.start_write_server(Source(id="f", type=SourceType.files, path="/data"))
    pgwire_write.start_write_server(Source(id="pg", type=SourceType.postgresql, host="h"))
    assert spawned == ["pgwire-write-server:sf-sales"]


# -- the writes that reach the pool ----------------------------------------------------------------


@_aio
async def test_a_copy_opens_the_pool_before_its_first_insert(monkeypatch, registered):
    from provisa.executor import direct
    from provisa.pgwire import copy_handler

    ports = pr.PortPair(pgwire_port=5999, calcite_child_host="127.0.0.1", calcite_child_port=6099)
    monkeypatch.setattr(pr, "ensure_endpoint_for_discovery", lambda source: ports)
    state = _state()
    ran: list[tuple[bool, str]] = []

    async def _execute(pools, source_id, sql, params, span_attrs=None):
        ran.append((pools.has(source_id), sql))

    monkeypatch.setattr(direct, "execute_direct", _execute)
    monkeypatch.setattr(copy_handler, "state", state)
    inserted = await copy_handler._insert_rows("sf-sales", "sf_sales", "Account", ["Name"], [["a"]])
    assert inserted == 1
    assert ran == [(True, 'INSERT INTO "sf_sales"."Account" ("Name") VALUES ($1)')]


@_aio
async def test_the_direct_terminal_opens_the_pool_for_a_write(monkeypatch):
    from provisa.pgwire import _pipeline

    opened: list[str] = []

    async def _ensure(state, source_id):
        opened.append(source_id)

    monkeypatch.setattr(pgwire_write, "ensure_write_pool", _ensure)

    class _Engine:
        async def execute_native(self, pools, source_id, sql, params, span_attrs):
            return ("ran", source_id, sql)

    state = SimpleNamespace(
        federation_engine=_Engine(),
        source_types={"sf-sales": "salesforce"},
        source_pools=SimpleNamespace(has=lambda source_id: True),
    )

    def _plan(writes: bool):
        return SimpleNamespace(
            kept=None,
            materialize=None,
            auto_deliver=None,
            engine_landing=None,
            route=Route.DIRECT,
            writes_tables=writes,
            source_id="sf-sales",
            sql="DELETE FROM t",
            exec_params=None,
            span_attrs=None,
        )

    assert await _pipeline._run_plan_terminal(_plan(False), state) == (  # pyright: ignore[reportArgumentType]
        "ran",
        "sf-sales",
        "DELETE FROM t",
    )
    assert opened == []  # a read asks for no write pool
    await _pipeline._run_plan_terminal(_plan(True), state)  # pyright: ignore[reportArgumentType]
    assert opened == ["sf-sales"]


# -- a write route that does not return the rows it wrote -------------------------------------------


@pytest.mark.parametrize("source_type", ["sharepoint", "salesforce"])
def test_the_pgwire_route_does_not_return_written_rows(source_type):
    from provisa.executor.writable import write_returns_rows
    from provisa.executor.write_capability import table_write_returns_rows

    assert write_returns_rows(source_type, None) is False
    assert table_write_returns_rows({"table_name": "Account"}, source_type, None) is False


def test_the_flag_belongs_to_the_route_not_the_source_type(monkeypatch):
    from provisa.executor import writable

    monkeypatch.setitem(writable._ROUTE_RETURNS_WRITTEN_ROWS, WritePath.PGWIRE, True)
    assert writable.write_returns_rows("salesforce", None) is True


def test_every_route_declares_whether_it_returns_rows():
    from provisa.executor.writable import _ROUTE_RETURNS_WRITTEN_ROWS

    assert set(_ROUTE_RETURNS_WRITTEN_ROWS) == set(WritePath)


@pytest.mark.parametrize(
    "source_type, returns", [("postgresql", True), ("mongodb", False), ("files", False)]
)
def test_other_sources_return_rows_exactly_when_they_take_writes(source_type, returns):
    from provisa.executor.write_capability import table_write_returns_rows

    assert table_write_returns_rows({"table_name": "t"}, source_type, None) is returns


def test_a_view_returns_no_written_rows():
    from provisa.executor.write_capability import table_write_returns_rows

    assert table_write_returns_rows({"view_sql": "SELECT 1"}, "postgresql", None) is False


def _write_gov(returns_rows: bool):
    from provisa.compiler.stage2 import GovernanceContext

    gov = GovernanceContext()
    gov.role_id = "writer"
    gov.can_write = True
    gov.table_map = {"account": 1, "sf.account": 1}
    gov.all_columns = {1: [("id", "varchar"), ("name", "varchar")]}
    gov.writable_columns = {1: frozenset({"id", "name"})}
    gov.write_ops = {1: frozenset({"insert", "update", "delete"})}
    gov.write_returns_rows = {1: returns_rows}
    return gov


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO sf.account (id, name) VALUES ('1', 'Acme') RETURNING id",
        "UPDATE sf.account SET name = 'Acme' WHERE id = '1' RETURNING *",
        "DELETE FROM sf.account WHERE id = '1' RETURNING id",
    ],
)
def test_a_returning_clause_is_refused_at_admission_naming_the_reason(sql):
    import sqlglot

    from provisa.compiler.write_admission import ReturningNotSupported, admit_write

    with pytest.raises(ReturningNotSupported, match="'account' does not take RETURNING"):
        admit_write(sqlglot.parse_one(sql, read="postgres"), _write_gov(False))
    admit_write(sqlglot.parse_one(sql, read="postgres"), _write_gov(True))


def test_the_same_write_without_returning_is_admitted():
    import sqlglot

    from provisa.compiler.write_admission import admit_write

    admit_write(
        sqlglot.parse_one("UPDATE sf.account SET name = 'Acme' WHERE id = '1'", read="postgres"),
        _write_gov(False),
    )


def _schema(returns_rows: bool):
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.compiler.schema_gen import SchemaInput, generate_schema

    table = {
        "id": 1,
        "source_id": "sf-sales",
        "domain_id": "sales",
        "schema_name": "sf_sales",
        "table_name": "account",
        "columns": [
            {"column_name": "id", "visible_to": ["admin"]},
            {"column_name": "name", "visible_to": ["admin"]},
        ],
        "write_ops": ["delete", "insert", "update"],
        "write_returns_rows": returns_rows,
    }
    cols = [
        ColumnMetadata(column_name="id", data_type="varchar(18)", is_nullable=False),
        ColumnMetadata(column_name="name", data_type="varchar(255)", is_nullable=False),
    ]
    return generate_schema(
        SchemaInput(
            tables=[table],
            relationships=[],
            column_types={1: cols},
            naming_rules=[],
            role={"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]},
            domains=[{"id": "sales", "description": ""}],
            source_types={"sf-sales": "salesforce"},
        )
    )


def test_graphql_offers_no_mutation_field_where_written_rows_are_not_returned():
    # Decided by the record's flag: the same table, same source type, with and without it.
    without = _schema(False)
    assert without.mutation_type is None or not [
        name for name in without.mutation_type.fields if "account" in name.lower()
    ]
    offered = _schema(True)
    assert offered.mutation_type is not None
    assert [name for name in offered.mutation_type.fields if "account" in name.lower()]
