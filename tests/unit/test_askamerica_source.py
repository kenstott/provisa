# Copyright (c) 2026 Kenneth Stott
# Canary: 4d33ba33-f00d-4ee3-a7ee-f7b05407a7cc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An AskAmerica source is a Postgres-wire source like its sibling adapters (REQ-540): the API
key resolves to the credentials its data is read with; its bundled ``pgwire-govdata`` server is
started with them and attached by each engine as ``pgwire-cloudops`` is; a table is read from
the adapter schema it names; a statement reading one is routed, joined and replicated as any
attached source's is. No in-process engine, no terminal of its own."""

# Requirements: REQ-492, REQ-540

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation import askamerica as aa
from provisa.federation import pgwire_replica as pr
from provisa.federation.connector_base import Mechanism
from provisa.federation.engine import (
    build_clickhouse_engine,
    build_duckdb_engine,
    build_pg_engine,
    build_trino_engine,
    live_source_types,
    reachable_source_types,
)
from provisa.federation.strategy import Strategy, engine_attaches, federate
from provisa.runtime_deps import pgwire_bundles as rd

_ANSWER = {
    "access_key_id": "AK",
    "secret_access_key": "SK",
    "session_token": "TOKEN",
    "region": "auto",
    "endpoint": "https://acct.r2.example",
    "bucket": "govdata-parquet-v1",
    "expires_in": 3600,
}
_BUNDLE_MODEL = {
    "version": "1.0",
    "defaultSchema": "sec",
    "schemas": [
        {"name": name, "type": "custom", "factory": "F", "operand": {"dataSource": name}}
        for name in ("sec", "geo", "econ", "ref")
    ],
}


def _source(database: str = "sec,ref,geo", key: str = "aa-key") -> Source:
    return Source(id="test-askamerica", type=SourceType.govdata, username=key, database=database)


def _attachable(schema_name: str | None = "sec", table_name: str = "financial_facts") -> Any:
    """The source as an engine runtime hands it to a connector: with its catalog and the table."""
    return SimpleNamespace(
        id="test-askamerica",
        type=SourceType.govdata,
        username="aa-key",
        database="sec,ref,geo",
        catalog="test_askamerica",
        schema_name=schema_name,
        table_name=table_name,
    )


@pytest.fixture
def endpoint(monkeypatch):
    ports = pr.PortPair(5440, "127.0.0.1", 5540)
    monkeypatch.setattr(pr, "ensure_endpoint", lambda source, timeout=None: ports)
    return ports


# -- the key resolves to the credentials the data is read with ---------------------------------


def test_the_key_is_exchanged_for_storage_credentials():
    asked: list[str] = []

    def fetch(key: str):
        asked.append(key)
        return 200, _ANSWER

    creds = aa.resolve_storage_credentials("aa-key", fetch=fetch)
    assert asked == ["aa-key"]
    assert (creds.access_key_id, creds.secret_access_key, creds.session_token) == (
        "AK",
        "SK",
        "TOKEN",
    )
    assert (creds.bucket, creds.endpoint, creds.region) == (
        "govdata-parquet-v1",
        "https://acct.r2.example",
        "auto",
    )
    assert creds.expires_at_millis > 0


def test_resolved_credentials_do_not_print_their_secrets():
    creds = aa.resolve_storage_credentials("aa-key", fetch=lambda key: (200, _ANSWER))
    assert "SK" not in repr(creds) and "TOKEN" not in repr(creds) and "AK" not in repr(creds)


@pytest.mark.parametrize("status", [401, 403])
def test_a_refused_key_is_named_as_refused(status):
    with pytest.raises(aa.AskAmericaKeyRefused, match="invalid_api_key"):
        aa.resolve_storage_credentials(
            "bad", fetch=lambda key: (status, {"error": "invalid_api_key"})
        )


def test_an_api_that_mints_no_credentials_is_not_a_refused_key():
    with pytest.raises(aa.AskAmericaUnavailable, match="502.*credential_mint_failed"):
        aa.resolve_storage_credentials(
            "aa-key", fetch=lambda key: (502, {"error": "credential_mint_failed"})
        )


def test_a_source_with_no_key_is_refused_by_name():
    with pytest.raises(aa.AskAmericaKeyMissing, match="test-askamerica"):
        aa.api_key(_source(key=""))


def test_the_key_is_a_secret_reference(monkeypatch):
    monkeypatch.setenv("ASKAMERICA_API_KEY", "from-env")
    assert aa.api_key(_source(key="${env:ASKAMERICA_API_KEY}")) == "from-env"


# -- the server: the bundle, its model, its environment ----------------------------------------


def test_bundle_is_pgwire_govdata():
    assert rd.bundle_spec_for("govdata").artifact_name == "pgwire-govdata"
    assert "govdata" in pr.PGWIRE_REPLICA_TYPES


def test_the_server_is_started_with_what_the_key_resolves_to():
    creds = aa.resolve_storage_credentials("aa-key", fetch=lambda key: (200, _ANSWER))
    env = aa.server_environment(_source(), resolve=lambda key: creds)
    assert env == {
        "ASKAMERICA_API_KEY": "aa-key",
        "GOVDATA_PARQUET_DIR": "s3://govdata-parquet-v1",
        "AWS_ACCESS_KEY_ID": "AK",
        "AWS_SECRET_ACCESS_KEY": "SK",
        "AWS_SESSION_TOKEN": "TOKEN",
        "AWS_ENDPOINT_OVERRIDE": "https://acct.r2.example",
        "AWS_REGION": "auto",
        "AWS_CREDENTIALS_EXPIRES_AT_MILLIS": str(creds.expires_at_millis),
    }


def test_only_an_askamerica_server_takes_an_environment():
    files = Source(id="docs", type=SourceType.files, path="/data")
    assert pr.server_environment(files) is None


def test_the_model_is_the_bundles_own_narrowed_to_the_sources_schemas(tmp_path):
    (tmp_path / "model").mkdir()
    (tmp_path / "model" / "model.json").write_text(json.dumps(_BUNDLE_MODEL))
    model = pr.build_model_json(_source("geo, SEC"), bundle_dir=tmp_path)
    assert [s["name"] for s in model["schemas"]] == ["geo", "sec"]
    assert model["defaultSchema"] == "geo"
    # No credential is written into the model: the bundle reads them from its environment.
    assert "aa-key" not in json.dumps(model)


def test_a_schema_the_adapter_does_not_serve_is_refused_by_name():
    with pytest.raises(ValueError, match=r"no schema named \['nasa'\]"):
        aa.narrow_model(_BUNDLE_MODEL, _source("sec,nasa"))


def test_a_source_listing_no_schema_is_refused():
    with pytest.raises(ValueError, match="lists no schema"):
        aa.narrow_model(_BUNDLE_MODEL, _source(""))


def test_the_model_needs_the_bundle():
    with pytest.raises(pr.MissingConnectorConfig, match="no bundle directory"):
        pr.build_model_json(_source())


def test_the_spawned_server_receives_its_environment(tmp_path):
    spawned: list[tuple] = []

    def spawn(command, cwd, env=None):
        spawned.append((command, cwd, env))
        return SimpleNamespace()

    server = pr.PgwireServer(
        bundle_dir=tmp_path,
        spec=rd.bundle_spec_for("govdata"),
        model=_BUNDLE_MODEL,
        ports=pr.PortPair(5440, "127.0.0.1", 5540),
        spawn=spawn,
        environment={"ASKAMERICA_API_KEY": "aa-key"},
    )
    server.start()
    assert spawned[0][2] == {"ASKAMERICA_API_KEY": "aa-key"}
    assert json.loads((Path(tmp_path) / "model" / "model.json").read_text()) == _BUNDLE_MODEL


def test_a_server_with_no_environment_is_spawned_as_before(tmp_path):
    spawned: list[tuple] = []
    server = pr.PgwireServer(
        bundle_dir=tmp_path,
        spec=rd.bundle_spec_for("cloudops"),
        model=_BUNDLE_MODEL,
        ports=pr.PortPair(5440, "127.0.0.1", 5540),
        spawn=lambda *args: spawned.append(args) or SimpleNamespace(),
    )
    server.start()
    assert len(spawned[0]) == 2  # command and directory, nothing else


# -- a table is read from the adapter schema it names ------------------------------------------


def test_a_table_is_read_from_its_own_schema():
    assert pr.serves_table_schemas(_source())
    assert pr.remote_schema(_source(), "sec") == "sec"
    with pytest.raises(pr.MissingConnectorConfig, match="none was given"):
        pr.remote_schema(_source(), None)


def test_a_single_schema_sibling_is_read_from_the_schema_named_after_its_source():
    files = Source(id="my-docs", type=SourceType.files, path="/data")
    assert not pr.serves_table_schemas(files)
    assert pr.remote_schema(files, "anything") == "my_docs"


@pytest.mark.asyncio
async def test_a_landed_table_is_selected_from_its_own_schema():
    statements: list[str] = []

    class _Conn:
        async def fetch(self, sql, *args, timeout=None):
            statements.append(sql)
            return [{"cik": "1"}]

        async def close(self):
            return None

    async def connect(host, port):
        return _Conn()

    replica = pr.ConnectorReplica(_source(), connect=connect)
    replica.endpoint = lambda timeout=None: pr.PortPair(5440, "127.0.0.1", 5540)  # type: ignore[method-assign]
    rows = await replica.load(SimpleNamespace(schema_name="sec", table_name="financial_facts"))
    assert rows == [{"cik": "1"}]
    assert statements == ['SELECT * FROM "sec"."financial_facts"']


# -- each engine attaches the server as it attaches a sibling's ---------------------------------


@pytest.mark.parametrize(
    "engine", [build_duckdb_engine(), build_pg_engine(), build_clickhouse_engine()]
)
def test_the_engine_attaches_the_sources_pgwire_server(engine):
    assert engine_attaches(engine, "govdata")
    assert engine.connectors["govdata"].mechanism == Mechanism.ATTACH_R


def test_duckdb_attaches_the_endpoint_and_reads_each_table_from_its_schema(endpoint):
    details = build_duckdb_engine().connectors["govdata"].details(_attachable())
    assert details["attach"] == (
        "ATTACH 'host=127.0.0.1 port=5440 user=provisa dbname=provisa' "
        'AS "_src_test-askamerica" (TYPE postgres, READ_ONLY)'
    )
    assert details["raw_alias"] == "_src_test-askamerica"
    # No schema of the connector's own: the runtime reads the table from the schema it names.
    assert "remote_schema" not in details


def test_a_single_schema_sibling_still_names_its_one_schema_on_duckdb(endpoint):
    cloud: Any = SimpleNamespace(
        id="cloud-estate", type=SourceType.cloudops, catalog="cloud_estate"
    )
    details = build_duckdb_engine().connectors["cloudops"].details(cloud)
    assert details["remote_schema"] == "cloud_estate"


def test_postgres_imports_the_table_from_its_schema_into_that_schemas_foreign_schema(endpoint):
    details = build_pg_engine().connectors["govdata"].details(_attachable("sec"))
    assert details["local_schema"] == "fdw_pgwire_test_askamerica_sec"
    assert details["remote_schema"] == "sec"
    assert details["server"] == "fdw_pgwire_test_askamerica"
    assert "port '5440'" in details["server_ddl_for_copy"][1]
    assert details["server_ddl_for_copy"][-1] == (
        'CREATE SCHEMA IF NOT EXISTS "fdw_pgwire_test_askamerica_sec"'
    )
    other = build_pg_engine().connectors["govdata"].details(_attachable("econ"))
    assert other["local_schema"] == "fdw_pgwire_test_askamerica_econ"
    assert other["server"] == details["server"]  # one server, one foreign schema per schema


def test_clickhouse_mounts_one_database_per_adapter_schema(endpoint):
    details = build_clickhouse_engine().connectors["govdata"].details(_attachable("sec"))
    assert details["local_schema"] == "ch_pgwire_test_askamerica_sec"
    assert details["attach_ddl"] == [
        'CREATE DATABASE IF NOT EXISTS "ch_pgwire_test_askamerica_sec" ENGINE = PostgreSQL('
        "'127.0.0.1:5440', 'provisa', 'provisa', '', 'sec')"
    ]


def test_an_attach_of_a_table_with_no_schema_is_refused(endpoint):
    with pytest.raises(pr.MissingConnectorConfig, match="none was given"):
        build_pg_engine().connectors["govdata"].details(_attachable(None))


# -- reach, strategy, replicas, routing --------------------------------------------------------


def test_reachable_on_every_engine_and_live_where_attached():
    for key in ("trino", "duckdb", "pg", "clickhouse"):
        assert "govdata" in reachable_source_types(key), key
    for key in ("duckdb", "pg", "clickhouse"):
        assert "govdata" in live_source_types(key), key


def test_strategy_is_virtual_where_attached_and_a_replica_on_trino():
    assert federate(_source(), build_duckdb_engine()) is Strategy.VIRTUAL
    assert federate(_source(), build_trino_engine()) is Strategy.MATERIALIZED
    # Trino has no connector of its own for it: its replica is landed through the server.
    assert pr.needs_pgwire_replica(_source(), build_trino_engine())
    assert not pr.needs_pgwire_replica(_source(), build_duckdb_engine())


def test_it_owns_replicas_as_its_siblings_do():
    from provisa.federation.replica_routing import NO_REPLICA_TYPES, owns_replica

    assert "govdata" not in NO_REPLICA_TYPES
    assert owns_replica(_source())


def test_a_statement_reading_it_alone_is_the_engines():
    """No terminal of its own: a single-source read goes where a sibling's goes."""
    from provisa.transpiler.router import Route, decide_route

    decision = decide_route(
        sources={"test-askamerica"},
        source_types={"test-askamerica": "govdata"},
        source_dialects={},
        operator_floor={},
    )
    assert decision.route == Route.ENGINE


def test_a_join_with_another_source_is_the_engines():
    from provisa.transpiler.router import Route, decide_route

    decision = decide_route(
        sources={"test-askamerica", "sales-pg"},
        source_types={"test-askamerica": "govdata", "sales-pg": "postgresql"},
        source_dialects={"sales-pg": "postgres"},
        operator_floor={},
    )
    assert decision.route == Route.ENGINE


def test_the_bridge_is_gone():
    import importlib

    import provisa.api.data.endpoint_dev as endpoint_dev
    import provisa.pgwire._pipeline as pipeline

    assert not hasattr(endpoint_dev, "_execute_govdata")
    assert "govdata" not in Path(pipeline.__file__).read_text()
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("provisa.govdata.source")


# -- the key is checked where it is exchanged, at save -----------------------------------------


def _input(key: str | None) -> Any:
    return SimpleNamespace(id="test-askamerica", type="govdata", username=key, database="sec")


@pytest.mark.asyncio
async def test_saving_a_source_with_no_key_is_refused():
    from provisa.api.admin.schema_common import _validate_govdata_api_key

    refused = await _validate_govdata_api_key(_input(None))
    assert refused is not None and refused.code == "schema.askamerica_key_required"


@pytest.mark.asyncio
async def test_saving_a_source_whose_key_askamerica_refuses_is_refused(monkeypatch):
    from provisa.api.admin.schema_common import _validate_govdata_api_key

    def refuse(key):
        raise aa.AskAmericaKeyRefused("invalid_api_key")

    monkeypatch.setattr(aa, "resolve_storage_credentials", refuse)
    refused = await _validate_govdata_api_key(_input("bad"))
    assert refused is not None and refused.code == "schema.invalid_askamerica_key"
    assert refused.message == "Invalid AskAmerica API Key: invalid_api_key"


@pytest.mark.asyncio
async def test_saving_while_askamerica_issues_no_credentials_is_not_called_a_bad_key(monkeypatch):
    from provisa.api.admin.schema_common import _validate_govdata_api_key

    def down(key):
        raise aa.AskAmericaUnavailable("AskAmerica's credentials API answered 502: no detail")

    monkeypatch.setattr(aa, "resolve_storage_credentials", down)
    with pytest.raises(aa.AskAmericaUnavailable, match="answered 502"):
        await _validate_govdata_api_key(_input("aa-key"))


@pytest.mark.asyncio
async def test_a_key_askamerica_accepts_saves(monkeypatch):
    from provisa.api.admin.schema_common import _validate_govdata_api_key

    monkeypatch.setattr(aa, "resolve_storage_credentials", lambda key: None)
    assert await _validate_govdata_api_key(_input("aa-key")) is None


# -- a server that is not serving costs only its own tables ------------------------------------


def test_an_attach_does_not_wait_for_a_server_that_takes_minutes_to_start(monkeypatch):
    """Started when its source is saved; until it listens an attach is answered at once."""
    waited: list[float | None] = []

    class _Replica:
        def endpoint(self, *, timeout=None):
            waited.append(timeout)
            raise pr.ServerLifecycleError("did not accept connections")

    monkeypatch.setattr(pr, "_endpoint_replica", lambda source: _Replica())
    with pytest.raises(pr.SourceStillStartingError, match="test-askamerica"):
        pr.ensure_endpoint(_source())
    assert waited == [pr.DISCOVERY_READY_SECONDS]


def test_a_sibling_whose_server_starts_in_seconds_is_still_waited_for(monkeypatch):
    waited: list[float | None] = []

    class _Replica:
        def endpoint(self, *, timeout=None):
            waited.append(timeout)
            return pr.PortPair(5440, "127.0.0.1", 5540)

    monkeypatch.setattr(pr, "_endpoint_replica", lambda source: _Replica())
    pr.ensure_endpoint(Source(id="docs", type=SourceType.files, path="/data"))
    assert waited == [None]


@pytest.mark.parametrize(
    "failure",
    [
        pr.SourceStillStartingError("test-askamerica"),
        pr.ServerExited("test-askamerica", 1, "boom"),
        pr.ServerLifecycleError("did not accept connections"),
        aa.AskAmericaKeyRefused("invalid_api_key"),
        aa.AskAmericaUnavailable("answered 502"),
        aa.AskAmericaKeyMissing("test-askamerica"),
        rd.BundleUnavailable("no bundle for this host"),
        pr.MissingConnectorConfig("incomplete"),
    ],
)
def test_every_native_engine_skips_a_table_whose_server_cannot_be_attached(failure):
    """An attach that fails for the source's own server is "this table is not queryable now",
    logged by name — never an error raised into a statement that does not read the source."""
    from provisa.federation.duckdb_backend import DuckDBBackend
    from provisa.federation.native_backend import NativeEngineBackend
    from provisa.federation.pg_backend import PgBackend

    for backend in (NativeEngineBackend, DuckDBBackend, PgBackend):
        assert isinstance(failure, backend._attach_errors), backend.__name__


def test_the_source_is_started_when_it_is_saved(monkeypatch):
    started: list[str] = []

    def spawn(coro, name):
        coro.close()
        started.append(name)

    monkeypatch.setattr("provisa.core.connection_loop.spawn_background", spawn)
    pr.start_when_saved(_source())
    pr.start_when_saved(Source(id="docs", type=SourceType.files, path="/data"))
    assert started == ["pgwire-server:test-askamerica"]
