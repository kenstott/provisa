# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

import json

import pytest
import httpx
from pytest_bdd import given, when, then, scenarios


scenarios("../features/REQ-673.feature")


@pytest.fixture
def shared_data():
    return {}


class _StubGraphQLTransport(httpx.AsyncBaseTransport):
    """Records GraphQL requests and returns a canned aggregate count response."""

    def __init__(self, count_value):
        self.count_value = count_value
        self.requests = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8")) if request.content else {}
        self.requests.append(body)
        payload = {"data": {"users": {"totalCount": self.count_value}}}
        return httpx.Response(
            200,
            json=payload,
            request=request,
            headers={"content-type": "application/json"},
        )


@given("a GQL remote source with count_query configured and a cold Trino cache")
def gql_source_with_count_query(shared_data):
    from provisa.sources.gql import GQLRemoteSource, GQLSourceConfig

    transport = _StubGraphQLTransport(count_value=4242)
    client = httpx.AsyncClient(
        transport=transport,
        base_url="http://remote-gql.test",
    )

    config = GQLSourceConfig(
        name="users_source",
        endpoint="http://remote-gql.test/graphql",
        count_query="query { users { totalCount } }",
    )

    source = GQLRemoteSource(config=config, http_client=client)

    # Simulate a cold Trino cache: no cached counts available.
    shared_data["transport"] = transport
    shared_data["client"] = client
    shared_data["config"] = config
    shared_data["source"] = source
    shared_data["cache_warm"] = False

    assert config.count_query, "count_query must be configured for this scenario"
    assert shared_data["cache_warm"] is False


@when("the graph-counts endpoint is called")
def call_graph_counts(shared_data):
    import asyncio
    from provisa.sources.counts import graph_counts

    source = shared_data["source"]

    async def _run():
        result = await graph_counts(
            source=source,
            cache_warm=shared_data["cache_warm"],
        )
        shared_data["result"] = result
        await shared_data["client"].aclose()

    asyncio.run(_run())


@then("the remote GraphQL API is queried to return node counts instead of returning no count")
def assert_remote_queried(shared_data):
    transport = shared_data["transport"]
    result = shared_data["result"]

    # The remote GraphQL API must have been contacted exactly because the
    # local Trino cache was cold and a count_query was configured.
    assert len(transport.requests) == 1, (
        "expected exactly one remote GraphQL query when cache is cold, "
        f"got {len(transport.requests)}"
    )

    sent_query = transport.requests[0].get("query", "")
    assert "totalCount" in sent_query, (
        "the configured count_query should have been sent to the remote API"
    )

    # A real, non-empty count must be returned rather than no count.
    assert result is not None, "graph-counts must not return None for a cold GQL source"
    count = result["users_source"] if isinstance(result, dict) else result
    assert count == 4242, f"expected remote-derived node count 4242, got {count}"


import asyncio
import os

import pytest
from pytest_bdd import parsers

from provisa.federation.connector import (
    MysqlFdwConnector,
    SqliteFdwConnector,
    Mechanism,
    ProbeResult,
)
from provisa.federation.engine import FederationEngine


scenarios("../features/REQ-907.feature")


@pytest.fixture
def shared_data():
    return {}


class _FakeFetch:
    """Async fetch callable returning canned rows for probe SQL."""

    def __init__(self, *, installed: set | None = None, available: set | None = None):
        self._installed = set(installed or ())
        self._available = set(available or ())

    async def __call__(self, sql: str):
        for ext in self._installed:
            if "pg_extension" in sql and f"'{ext}'" in sql:
                return [{"one": 1}]
        for ext in self._available:
            if "pg_available_extensions" in sql and f"'{ext}'" in sql:
                return [{"one": 1}]
        return []


@given(
    parsers.parse(
        'a source configured with sqlite_fdw (source_type "sqlite", key "sqlite_fdw") or mysql_fdw (source_type "mysql", key "mysql_fdw")'
    )
)
def source_configured_with_fdw_connectors(shared_data):
    sqlite_connector = SqliteFdwConnector()
    mysql_connector = MysqlFdwConnector()

    assert sqlite_connector.source_type == "sqlite"
    assert sqlite_connector.key == "sqlite_fdw"
    assert sqlite_connector.engine == "postgres"
    assert sqlite_connector.mechanism is Mechanism.ATTACH

    assert mysql_connector.source_type == "mysql"
    assert mysql_connector.key == "mysql_fdw"
    assert mysql_connector.engine == "postgres"
    assert mysql_connector.mechanism is Mechanism.ATTACH

    shared_data["sqlite_connector"] = sqlite_connector
    shared_data["mysql_connector"] = mysql_connector

    # Build a federation engine with both connectors as candidates
    engine = FederationEngine(
        name="postgres",
        connectors=[sqlite_connector, mysql_connector],
    )
    shared_data["engine"] = engine


@when("the federation engine initializes")
def federation_engine_initializes(shared_data):
    engine: FederationEngine = shared_data["engine"]

    # Probe with both extensions "installed" and "available"
    fetch_both = _FakeFetch(
        installed={"sqlite_fdw", "mysql_fdw"},
        available={"sqlite_fdw", "mysql_fdw"},
    )

    async def _run():
        report = await engine.discover(fetch_both)
        shared_data["probe_report"] = report

    asyncio.run(_run())


@then(
    "the connector probes functional availability (sqlite_fdw via loaded FDW, mysql_fdw via probe and library bundling)"
)
def connector_probes_functional_availability(shared_data):
    report = shared_data["probe_report"]

    # Both connectors must appear in the probe report
    assert "sqlite_fdw" in report, "sqlite_fdw probe result must be in report"
    assert "mysql_fdw" in report, "mysql_fdw probe result must be in report"

    sqlite_result: ProbeResult = report["sqlite_fdw"]
    mysql_result: ProbeResult = report["mysql_fdw"]

    assert isinstance(sqlite_result, ProbeResult)
    assert isinstance(mysql_result, ProbeResult)

    # With installed extensions both should be available
    assert sqlite_result.available is True, (
        f"sqlite_fdw should be available when extension is installed; reason: {sqlite_result.reason}"
    )
    assert mysql_result.available is True, (
        f"mysql_fdw should be available when extension is installed; reason: {mysql_result.reason}"
    )

    # Verify runtime_deps are declared for packaging surface
    sqlite_connector: SqliteFdwConnector = shared_data["sqlite_connector"]
    mysql_connector: MysqlFdwConnector = shared_data["mysql_connector"]

    assert len(sqlite_connector.runtime_deps) > 0, "SqliteFdwConnector must declare runtime_deps"
    assert any("sqlite" in dep.lower() for dep in sqlite_connector.runtime_deps), (
        "SqliteFdwConnector runtime_deps must reference libsqlite3"
    )
    assert any("system" in dep.lower() for dep in sqlite_connector.runtime_deps), (
        "SqliteFdwConnector libsqlite3 must be tagged as system-provided"
    )

    assert len(mysql_connector.runtime_deps) > 0, "MysqlFdwConnector must declare runtime_deps"
    assert any("mysql" in dep.lower() or "mariadb" in dep.lower() for dep in mysql_connector.runtime_deps), (
        "MysqlFdwConnector runtime_deps must reference libmysqlclient or mariadb-connector-c"
    )
    assert any("bundled" in dep.lower() for dep in mysql_connector.runtime_deps), (
        "MysqlFdwConnector libmysqlclient must be tagged as bundled"
    )


@then(
    "if available, the connector attaches the SQLite file (CREATE SERVER OPTIONS(database path) + IMPORT FOREIGN SCHEMA) or remote MySQL (CREATE SERVER + CREATE FOREIGN TABLE IMPORT FOREIGN SCHEMA)"
)
def connector_attaches_sources(shared_data):
    from provisa.core.models import Source, SourceType

    sqlite_connector: SqliteFdwConnector = shared_data["sqlite_connector"]
    mysql_connector: MysqlFdwConnector = shared_data["mysql_connector"]

    # Build a minimal SQLite source with a path
    sqlite_source = Source(
        id="test_sqlite",
        type=SourceType.sqlite,
        host="localhost",
        port=5432,
        database="orders",
        username="u",
        password="p",
        path="/data/orders.sqlite",
    )

    sqlite_details = sqlite_connector.details(sqlite_source)
    sqlite_ddl = sqlite_details["attach_ddl"]

    assert isinstance(sqlite_ddl, (list, tuple)), "attach_ddl must be a sequence of DDL statements"
    ddl_text = " ".join(sqlite_ddl)

    assert "CREATE EXTENSION IF NOT EXISTS sqlite_fdw" in sqlite_ddl[0], (
        "First SQLite DDL statement must create the extension"
    )
    assert any("FOREIGN DATA WRAPPER sqlite_fdw" in stmt for stmt in sqlite_ddl), (
        "SQLite DDL must create a foreign server using sqlite_fdw"
    )
    assert any("/data/orders.sqlite" in stmt for stmt in sqlite_ddl), (
        "SQLite DDL must reference the SQLite file path"
    )
    assert any("IMPORT FOREIGN SCHEMA" in stmt for stmt in sqlite_ddl), (
        "SQLite DDL must include IMPORT FOREIGN SCHEMA"
    )
    assert "local_schema" in sqlite_details, "sqlite details must include local_schema"

    # Build a minimal MySQL source
    mysql_source = Source(
        id="test_mysql",
        type=SourceType.mysql,
        host="mysqlhost",
        port=3306,
        database="inventory",
        username="myuser",
        password="mypass",
    )

    mysql_details = mysql_connector.details(mysql_source)
    mysql_ddl = mysql_details["attach_ddl"]

    assert isinstance(mysql_ddl, (list, tuple)), "MySQL attach_ddl must be a sequence of DDL statements"

    assert "CREATE EXTENSION IF NOT EXISTS mysql_fdw" in mysql_ddl[0], (
        "First MySQL DDL statement must create the extension"
    )
    assert any("FOREIGN DATA WRAPPER mysql_fdw" in stmt for stmt in mysql_ddl), (
        "MySQL DDL must create a foreign server using mysql_fdw"
    )
    assert any("mysqlhost" in stmt for stmt in mysql_ddl), (
        "MySQL DDL must reference the remote MySQL host"
    )
    assert any("CREATE USER MAPPING" in stmt for stmt in mysql_ddl), (
        "MySQL DDL must create a user mapping"
    )
    assert any("IMPORT FOREIGN SCHEMA" in stmt for stmt in mysql_ddl), (
        "MySQL DDL must include IMPORT FOREIGN SCHEMA"
    )

    shared_data["sqlite_details"] = sqlite_details
    shared_data["mysql_details"] = mysql_details


@then("queries route through the attached foreign schema to the source.")
def queries_route_through_attached_foreign_schema(shared_data):
    engine: FederationEngine = shared_data["engine"]
    report: dict = shared_data["probe_report"]

    # After discover(), both source types must be reachable through active connectors
    assert engine.reachable("sqlite"), (
        "Engine must have sqlite as reachable after successful probe"
    )
    assert engine.reachable("mysql"), (
        "Engine must have mysql as reachable after successful probe"
    )

    # The active connectors must be the FDW connectors
    sqlite_active = engine.connectors.get("sqlite")
    mysql_active = engine.connectors.get("mysql")

    assert sqlite_active is not None, "sqlite connector must be active in engine"
    assert mysql_active is not None, "mysql connector must be active in engine"

    assert sqlite_active.key == "sqlite_fdw", (
        f"Active sqlite connector must be sqlite_fdw, got {sqlite_active.key!r}"
    )
    assert mysql_active.key == "mysql_fdw", (
        f"Active mysql connector must be mysql_fdw, got {mysql_active.key!r}"
    )

    # Verify local_schema is set so queries can be routed to the foreign schema
    sqlite_details = shared_data["sqlite_details"]
    mysql_details = shared_data["mysql_details"]

    local_schema_sqlite = sqlite_details.get("local_schema", "")
    assert local_schema_sqlite, "SQLite details must declare a local_schema for query routing"

    local_schema_mysql = mysql_details.get("local_schema", "")
    assert local_schema_mysql, "MySQL details must declare a local_schema for query routing"

    # Capability check: both connectors must support predicate pushdown (queries push predicates to source)
    sqlite_cap = sqlite_active.capability()
    mysql_cap = mysql_active.capability()

    assert sqlite_cap.predicate_pushdown is True, (
        "SqliteFdwConnector must support predicate pushdown for efficient query routing"
    )
    assert mysql_cap.predicate_pushdown is True, (
        "MysqlFdwConnector must support predicate pushdown for efficient query routing"
    )

    # Verify probe availability was correctly reported (connectors that are available route queries)
    assert report["sqlite_fdw"].available is True, "sqlite_fdw probe must confirm availability"
    assert report["mysql_fdw"].available is True, "mysql_fdw probe must confirm availability"


scenarios("../features/REQ-908.feature")


@given(
    parsers.parse(
        'a source configured with pg_duckdb_iceberg (source_type "iceberg", key "pg_duckdb_iceberg")'
    )
)
def source_configured_with_pg_duckdb_iceberg(shared_data):
    from provisa.federation.connector import PgDuckdbIcebergConnector

    connector = PgDuckdbIcebergConnector()

    assert connector.engine == "postgres"
    assert connector.source_type == "iceberg"
    assert connector.key == "pg_duckdb_iceberg"
    assert connector.mechanism is Mechanism.ATTACH

    shared_data["iceberg_connector"] = connector

    engine = FederationEngine(
        name="postgres",
        connectors=[connector],
    )
    shared_data["engine"] = engine


@then(
    "the connector probes that iceberg_scan is registered (pg_duckdb without iceberg lacks the function, so probes correctly disable it)"
)
def connector_probes_iceberg_scan_registered(shared_data):
    connector = shared_data["iceberg_connector"]

    # Case 1: pg_duckdb preloaded + installed + iceberg_scan registered -> available
    async def _run_available():
        fetch_ok = _FakeFetch(
            installed={"pg_duckdb"},
            available=set(),
        )

        class _IcebergFetch:
            async def __call__(self, sql: str):
                if "shared_preload_libraries" in sql:
                    return [{"v": "pg_duckdb"}]
                if "pg_extension" in sql and "pg_duckdb" in sql:
                    return [{"one": 1}]
                if "iceberg_scan" in sql:
                    return [{"one": 1}]
                return []

        result = await connector.probe(_IcebergFetch())
        assert result.available is True, (
            f"connector must be available when iceberg_scan is registered; reason: {result.reason}"
        )
        assert "iceberg" in result.reason.lower() or "iceberg" in (result.reason or "").lower(), (
            "probe reason must mention iceberg"
        )
        return result

    # Case 2: pg_duckdb preloaded + installed BUT iceberg_scan NOT registered -> unavailable
    async def _run_unavailable():
        class _NoIcebergFetch:
            async def __call__(self, sql: str):
                if "shared_preload_libraries" in sql:
                    return [{"v": "pg_duckdb"}]
                if "pg_extension" in sql and "pg_duckdb" in sql:
                    return [{"one": 1}]
                if "iceberg_scan" in sql:
                    return []  # not registered
                return []

        result = await connector.probe(_NoIcebergFetch())
        assert result.available is False, (
            "connector must be unavailable when pg_duckdb lacks iceberg extension"
        )
        assert "iceberg" in result.reason.lower(), (
            f"probe reason must mention iceberg when unavailable; got: {result.reason}"
        )
        assert result.remediation is not None and "iceberg" in result.remediation.lower(), (
            f"remediation must mention iceberg; got: {result.remediation}"
        )
        return result

    available_result = asyncio.run(_run_available())
    unavailable_result = asyncio.run(_run_unavailable())

    shared_data["probe_result_available"] = available_result
    shared_data["probe_result_unavailable"] = unavailable_result


@then(
    "if available, queries emit iceberg_scan('<root>', allow_moved_paths := true)"
)
def queries_emit_iceberg_scan_with_allow_moved_paths(shared_data):
    from provisa.core.models import Source, SourceType

    connector = shared_data["iceberg_connector"]

    source = Source(
        id="test_iceberg",
        type=SourceType.iceberg,
        path="s3://my-bucket/warehouse/orders",
    )

    details = connector.details(source)

    assert "scan" in details, "connector details must contain a 'scan' key"
    scan = details["scan"]

    assert "iceberg_scan(" in scan, (
        f"scan must call iceberg_scan(); got: {scan!r}"
    )
    assert "s3://my-bucket/warehouse/orders" in scan, (
        f"scan must reference the table root path; got: {scan!r}"
    )
    assert "allow_moved_paths := true" in scan, (
        f"scan must include allow_moved_paths := true; got: {scan!r}"
    )

    assert details.get("requires_preload") == "pg_duckdb", (
        "details must declare pg_duckdb as required preload"
    )
    assert details.get("reader") == "iceberg_scan", (
        "details must declare iceberg_scan as the reader"
    )

    shared_data["iceberg_details"] = details
    shared_data["iceberg_source"] = source


@then("governance predicates are pushed down into the scan")
def governance_predicates_pushed_down_into_scan(shared_data):
    connector = shared_data["iceberg_connector"]

    cap = connector.capability()

    assert cap.predicate_pushdown is True, (
        "PgDuckdbIcebergConnector must declare predicate_pushdown=True so governance "
        "predicates are pushed into the iceberg_scan at the scan level"
    )

    # Verify the runtime_deps document static-linked libs (no extra runtime dylib required)
    deps = connector.runtime_deps
    assert len(deps) > 0, "PgDuckdbIcebergConnector must declare runtime_deps"
    assert any("libduckdb" in d for d in deps), (
        "runtime_deps must reference libduckdb"
    )
    assert any("aws-sdk-cpp" in d and "static-linked" in d for d in deps), (
        "runtime_deps must document that aws-sdk-cpp is static-linked (no extra runtime dylib)"
    )


@then("results are correctly federated with other sources.")
def results_correctly_federated_with_other_sources(shared_data):
    from provisa.core.models import Source, SourceType

    engine: FederationEngine = shared_data["engine"]
    connector = shared_data["iceberg_connector"]

    # Run discover with iceberg_scan registered so connector becomes active
    class _FullIcebergFetch:
        async def __call__(self, sql: str):
            if "shared_preload_libraries" in sql:
                return [{"v": "pg_duckdb"}]
            if "pg_extension" in sql and "pg_duckdb" in sql:
                return [{"one": 1}]
            if "iceberg_scan" in sql:
                return [{"one": 1}]
            return []

    async def _run():
        report = await engine.discover(_FullIcebergFetch())
        shared_data["iceberg_probe_report"] = report

    asyncio.run(_run())

    # After discover, iceberg must be reachable
    assert engine.reachable("iceberg"), (
        "Engine must treat iceberg as reachable after successful probe"
    )

    active = engine.connectors.get("iceberg")
    assert active is not None, "iceberg connector must be active in engine"
    assert active.key == "pg_duckdb_iceberg", (
        f"Active iceberg connector must be pg_duckdb_iceberg, got {active.key!r}"
    )

    # Verify catalog entry projection works for federation
    source = Source(
        id="federated_orders",
        type=SourceType.iceberg,
        path="s3://my-bucket/warehouse/orders",
    )

    entry = engine.on_asset_create(source)
    assert entry.engine == "postgres", f"catalog entry engine must be postgres; got {entry.engine!r}"
    assert entry.source_type == "iceberg", (
        f"catalog entry source_type must be iceberg; got {entry.source_type!r}"
    )
    assert entry.mechanism is Mechanism.ATTACH, (
        "iceberg connector must use ATTACH mechanism (no data movement)"
    )

    stored = engine.catalog.get("federated_orders")
    assert stored is not None, "catalog entry must be stored after on_asset_create"
    assert stored == entry, "stored catalog entry must match the projected entry"

    report = shared_data["iceberg_probe_report"]
    assert "pg_duckdb_iceberg" in report, "probe report must include pg_duckdb_iceberg"
    assert report["pg_duckdb_iceberg"].available is True, (
        "pg_duckdb_iceberg probe must report available when iceberg_scan is registered"
    )
