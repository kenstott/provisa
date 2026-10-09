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
import threading
import time
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
_RECORDED_SCHEMAS = json.loads(
    (Path(aa.__file__).resolve().parents[1] / "govdata" / "bundle_schemas.json").read_text()
)["schemas"]
_BUNDLE_MODEL = {
    "version": "1.0",
    "defaultSchema": "sec",
    "schemas": [
        {"name": name, "type": "custom", "factory": "F", "operand": {"dataSource": name}}
        # The schemas recorded for the pinned bundle: a server is started only from a bundle
        # that serves exactly these (govdata.subjects.require_recorded_schemas).
        for name in sorted(_RECORDED_SCHEMAS)
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
    catalog = Path("/bundle/model/.duckdb/govdata.duckdb")
    env = aa.server_environment(_source(), catalog=catalog, resolve=lambda key: creds)
    assert env == {
        "ASKAMERICA_API_KEY": "aa-key",
        "GOVDATA_DUCKDB_CATALOG": "/bundle/model/.duckdb/govdata.duckdb",
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
    assert pr.server_environment(files, Path("/bundle")) is None


# The bundle as it is installed: its own model file, the catalog prebuilt for that model beside
# it, and its own launcher. The model's bytes are deliberately not what json.dumps would write.
_INSTALLED_MODEL = (
    b'{"version": "1.0", "defaultSchema": "sec", "schemas": ['
    + b",".join(b'{"name": "%s"}' % name.encode() for name in sorted(_RECORDED_SCHEMAS))
    + b"]}"
)


def _seed_name(model_bytes: bytes) -> str:
    """The catalog cache the adapter looks for beside a model: named by the model file's bytes
    (pgwire_calcite.catalog_populate: sha256 of the bytes, first 16 hex digits)."""
    import hashlib

    return f"catalog-cache-{hashlib.sha256(model_bytes).hexdigest()[:16]}.pkl"


_SEED_SCHEMAS = ("sec", "econ", "ref")


def _seed_zip(*, catalog: bytes = b"the prebuilt catalog") -> bytes:
    """A seed as the bundle's jar carries it: the catalog and one conversion record a schema."""
    import io
    import zipfile

    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as seed:
        seed.writestr(".duckdb/govdata.duckdb", catalog)
        for schema in _SEED_SCHEMAS:
            seed.writestr(f".aperio/{schema}/.conversions.json", f'{{"schema": "{schema}"}}')
    return out.getvalue()


def _write_jar(bundle: Path, entries: dict[str, bytes]) -> None:
    import zipfile

    (bundle / "jars").mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(bundle / "jars" / "calcite-govdata-1.42.0-SNAPSHOT.jar", "w") as jar:
        for name, data in entries.items():
            jar.writestr(name, data)


def _install_bundle(root: Path, *, seed: bytes | None = None) -> Path:
    (root / "model").mkdir(parents=True)
    (root / "bin").mkdir()
    (root / "model" / "model.json").write_bytes(_INSTALLED_MODEL)
    (root / "model" / _seed_name(_INSTALLED_MODEL)).write_bytes(b"prebuilt")
    (root / "bin" / "pgwire-govdata").write_text("#!/bin/sh\n")
    _write_jar(
        root,
        {
            aa.SEED_ZIP_RESOURCE: seed if seed is not None else _seed_zip(),
            aa.SCHEMA_CACHE_RESOURCE: b'{"tables": {}}',
        },
    )
    return root


@pytest.fixture(autouse=True)
def _schema_cache_in_a_directory_of_the_tests_own(tmp_path, monkeypatch):
    """The adapter keeps its Iceberg schema cache under the user's home; no test installs there."""
    monkeypatch.setattr(aa, "_schema_cache_dir", lambda: tmp_path / "iceberg-schema-cache")


def _replica_on(bundle: Path, spawned: list, monkeypatch) -> pr.ConnectorReplica:
    monkeypatch.setattr(aa, "_fetch", lambda key: (200, _ANSWER))

    def spawn(command, cwd, env=None):
        spawned.append((command, cwd, env))
        return SimpleNamespace(exit_code=lambda: None)

    return pr.ConnectorReplica(
        _source(),
        resolver=SimpleNamespace(resolve=lambda spec: bundle),  # type: ignore[arg-type]
        allocator=pr.PortAllocator(is_free=lambda port: True),
        spawn=spawn,
        health_check=lambda host, port: True,
        prepare_catalog=lambda ports: None,
    )


def test_the_server_is_the_installed_bundle_started_by_its_own_launcher(tmp_path, monkeypatch):
    """Installed, then called: the bundle's launcher, in the bundle, with a port and the
    environment the key resolves to. Nothing of the bundle is copied, linked or rewritten; the
    one thing added is its own seed, laid down beside its model."""
    bundle = _install_bundle(tmp_path / "bundle")
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path / "data"))
    before = sorted(p.relative_to(bundle) for p in bundle.rglob("*"))
    spawned: list = []
    _replica_on(bundle, spawned, monkeypatch).endpoint()
    added = set(p.relative_to(bundle) for p in bundle.rglob("*")) - set(before)
    assert all(str(p).startswith(("model/.duckdb", "model/.aperio")) for p in added), added
    before = sorted(p.relative_to(bundle) for p in bundle.rglob("*"))

    command, cwd, env = spawned[0]
    assert command[0] == str(bundle / "bin" / "pgwire-govdata")
    assert command[1:5] == ["--port", "5433", "--calcite-child", "127.0.0.1:5533"]
    assert cwd == bundle
    assert env["ASKAMERICA_API_KEY"] == "aa-key" and env["AWS_ACCESS_KEY_ID"] == "AK"
    assert sorted(p.relative_to(bundle) for p in bundle.rglob("*")) == before
    assert not (tmp_path / "data").exists()  # no per-source copy of the tree


def test_the_server_runs_on_the_bundles_model_byte_for_byte_with_its_prebuilt_catalog(
    tmp_path, monkeypatch
):
    """The adapter finds its prebuilt catalog by the model file's bytes. A model Provisa had
    rewritten -- narrowed, or only re-serialized -- has another name's catalog, and the adapter
    then builds one from scratch: the multi-minute start this replaces."""
    bundle = _install_bundle(tmp_path / "bundle")
    spawned: list = []
    _replica_on(bundle, spawned, monkeypatch).endpoint()

    model = (bundle / "model" / "model.json").read_bytes()
    assert model == _INSTALLED_MODEL
    assert (bundle / "model" / _seed_name(model)).is_file()
    # What a re-serialized model would have been named: not what is installed.
    reserialized = json.dumps(json.loads(_INSTALLED_MODEL), indent=2).encode()
    assert _seed_name(reserialized) != _seed_name(_INSTALLED_MODEL)


def test_a_bundle_whose_model_serves_other_schemas_than_recorded_is_not_started(
    tmp_path, monkeypatch
):
    """The installed model is read, never changed, and held to the record of its release
    (provisa/govdata/bundle_schemas.json): a difference is refused by name before the launcher
    is run, naming both sides of it and the command that records the bundle."""
    from provisa.govdata.subjects import BundleSchemasChanged

    bundle = _install_bundle(tmp_path / "bundle")
    moved = _INSTALLED_MODEL.replace(b'{"name": "law"}', b'{"name": "space"}')
    (bundle / "model" / "model.json").write_bytes(moved)
    spawned: list = []
    with pytest.raises(BundleSchemasChanged) as refused:
        _replica_on(bundle, spawned, monkeypatch).endpoint()
    assert spawned == []
    assert "only in the bundle ['space']" in str(refused.value)
    assert "only in the record ['law']" in str(refused.value)
    assert "scripts/record_govdata_bundle_schemas.py" in str(refused.value)
    assert (bundle / "model" / "model.json").read_bytes() == moved
    assert isinstance(refused.value, pr.server_start_errors())


# -- installing the bundle installs its seed; the server is started on it ------------------------
#
# The bundle carries, in its govdata jar, the DuckDB catalog and the per-schema conversion
# records the adapter otherwise builds at every start from object storage. The adapter's own
# installer runs only on its embedded JDBC path; a pgwire server never reaches it.


def test_the_seed_is_laid_down_where_the_server_reads_it(tmp_path):
    bundle = _install_bundle(tmp_path / "bundle")
    catalog = aa.install_seed(bundle)

    assert catalog == bundle / "model" / ".duckdb" / "govdata.duckdb"
    assert catalog.read_bytes() == b"the prebuilt catalog"
    for schema in _SEED_SCHEMAS:  # <model dir>/.aperio/<schema>: each schema's working directory
        record = bundle / "model" / ".aperio" / schema / ".conversions.json"
        assert record.read_text() == f'{{"schema": "{schema}"}}'
    # The marker the adapter's installer writes: the seed's SHA-256, beside the catalog.
    import hashlib

    marker = bundle / "model" / ".duckdb" / "govdata.duckdb.version"
    assert marker.read_text() == hashlib.sha256(_seed_zip()).hexdigest()
    assert (bundle / "model" / "model.json").read_bytes() == _INSTALLED_MODEL


def test_the_server_is_started_on_the_seeded_catalog(tmp_path, monkeypatch):
    bundle = _install_bundle(tmp_path / "bundle")
    spawned: list = []
    _replica_on(bundle, spawned, monkeypatch).endpoint()
    _, _, env = spawned[0]
    catalog = bundle / "model" / ".duckdb" / "govdata.duckdb"
    assert env["GOVDATA_DUCKDB_CATALOG"] == str(catalog)
    assert catalog.is_file()  # installed before the launcher is run


def test_an_installed_seed_is_left_alone_and_a_new_bundles_seed_replaces_it(tmp_path):
    bundle = _install_bundle(tmp_path / "bundle")
    catalog = aa.install_seed(bundle)
    catalog.write_bytes(b"the catalog as the running server has kept it")
    aa.install_seed(bundle)
    assert catalog.read_bytes() == b"the catalog as the running server has kept it"

    # A new release of the bundle: another seed, so another fingerprint.
    _write_jar(bundle, {aa.SEED_ZIP_RESOURCE: _seed_zip(catalog=b"the next release's catalog")})
    aa.install_seed(bundle)
    assert catalog.read_bytes() == b"the next release's catalog"


def test_a_catalog_the_server_built_for_itself_is_not_taken_for_a_seed(tmp_path):
    """Before the seed was installed a server built its own catalog and records in these
    directories. They carry no marker: the seed replaces them, and the write-ahead log of the
    replaced catalog goes with it."""
    bundle = _install_bundle(tmp_path / "bundle")
    built = bundle / "model" / ".duckdb"
    built.mkdir(parents=True)
    (built / "govdata.duckdb").write_bytes(b"built cold this morning")
    (built / "govdata.duckdb.wal").write_bytes(b"its log")
    record = bundle / "model" / ".aperio" / "sec" / ".conversions.json"
    record.parent.mkdir(parents=True)
    record.write_text("built cold")

    catalog = aa.install_seed(bundle)
    assert catalog.read_bytes() == b"the prebuilt catalog"
    assert not (built / "govdata.duckdb.wal").exists()
    assert record.read_text() == '{"schema": "sec"}'


def test_a_missing_catalog_is_seeded_again_whatever_the_marker_says(tmp_path):
    bundle = _install_bundle(tmp_path / "bundle")
    catalog = aa.install_seed(bundle)
    catalog.unlink()
    assert aa.install_seed(bundle).read_bytes() == b"the prebuilt catalog"


def test_a_bundle_with_no_seed_is_refused_by_name_and_its_server_is_not_started(
    tmp_path, monkeypatch
):
    """A packaging defect of the bundle, not a cold start to sit through."""
    bundle = _install_bundle(tmp_path / "bundle")
    _write_jar(bundle, {"org/apache/calcite/adapter/govdata/Some.class": b""})
    spawned: list = []
    with pytest.raises(aa.BundleSeedMissing) as refused:
        _replica_on(bundle, spawned, monkeypatch).endpoint()
    assert spawned == []
    assert "calcite-govdata-1.42.0-SNAPSHOT.jar" in str(refused.value)
    assert "duckdb/seed/govdata-seed.zip" in str(refused.value)
    assert isinstance(refused.value, pr.server_start_errors())


def test_a_seed_with_no_catalog_in_it_is_refused(tmp_path):
    import io
    import zipfile

    empty = io.BytesIO()
    with zipfile.ZipFile(empty, "w") as seed:
        seed.writestr(".aperio/sec/.conversions.json", "{}")
    bundle = _install_bundle(tmp_path / "bundle", seed=empty.getvalue())
    with pytest.raises(aa.BundleSeedMissing, match="holds no .duckdb/govdata.duckdb"):
        aa.install_seed(bundle)


def test_a_seed_entry_that_leaves_the_seed_directory_is_refused(tmp_path):
    import io
    import zipfile

    hostile = io.BytesIO()
    with zipfile.ZipFile(hostile, "w") as seed:
        seed.writestr("../../outside.txt", "x")
    bundle = _install_bundle(tmp_path / "bundle", seed=hostile.getvalue())
    with pytest.raises(aa.BundleSeedMissing, match="leaves the seed directory"):
        aa.install_seed(bundle)
    assert not (tmp_path / "outside.txt").exists()


def test_the_bundled_iceberg_schema_cache_is_installed_where_the_adapter_reads_it(tmp_path):
    """As the adapter's own installer: the file, and the marker of the bundled copy installed
    (its MD5), so a copy the adapter has since refreshed is not put back."""
    import hashlib

    bundle = _install_bundle(tmp_path / "bundle")
    cache_dir = tmp_path / "cache"
    aa.install_seed(bundle, schema_cache_dir=cache_dir)
    target = cache_dir / "iceberg-schema-cache.json"
    assert target.read_bytes() == b'{"tables": {}}'
    marker = cache_dir / "iceberg-schema-cache.json.bundled"
    assert marker.read_text() == hashlib.md5(b'{"tables": {}}', usedforsecurity=False).hexdigest()

    target.write_bytes(b"refreshed by the adapter")
    aa.install_seed(bundle, schema_cache_dir=cache_dir)
    assert target.read_bytes() == b"refreshed by the adapter"


def test_no_model_is_built_for_a_source_that_runs_on_its_bundles_own():
    with pytest.raises(pr.MissingConnectorConfig, match="runs on its bundle's own model"):
        pr.build_model_json(_source())
    assert not hasattr(aa, "narrow_model")


def test_a_sibling_still_runs_from_a_state_directory_of_its_own_with_its_model_written(
    tmp_path, monkeypatch
):
    bundle = tmp_path / "bundle"
    (bundle / "bin").mkdir(parents=True)
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path / "data"))
    spawned: list = []
    replica = pr.ConnectorReplica(
        Source(id="docs", type=SourceType.files, path="/data"),
        resolver=SimpleNamespace(resolve=lambda spec: bundle),  # type: ignore[arg-type]
        allocator=pr.PortAllocator(is_free=lambda port: True),
        spawn=lambda command, cwd: spawned.append((command, cwd)) or SimpleNamespace(),
        health_check=lambda host, port: True,
    )
    replica.endpoint()
    _, cwd = spawned[0]
    assert cwd != bundle and (cwd / "model" / "model.json").is_file()
    assert not (bundle / "model").exists()


# -- one AskAmerica source runs from the one installed bundle ------------------------------------


def test_a_second_askamerica_source_is_not_started_beside_the_first(monkeypatch):
    monkeypatch.setattr(pr, "_ENDPOINTS", {})
    monkeypatch.setattr(pr, "_reap_on_exit", lambda: None)
    first = pr._endpoint_replica(_source())
    assert pr._endpoint_replica(_source()) is first  # the same source: its one server
    other = Source(id="second-askamerica", type=SourceType.govdata, username="k", database="sec")
    with pytest.raises(pr.InstalledBundleInUse, match="'test-askamerica' already runs"):
        pr._endpoint_replica(other)
    assert isinstance(pr.InstalledBundleInUse("b", "govdata", "a"), pr.server_start_errors())
    # A sibling type is one server per source, as before.
    pr._endpoint_replica(Source(id="docs-a", type=SourceType.files, path="/a"))
    pr._endpoint_replica(Source(id="docs-b", type=SourceType.files, path="/b"))


class _OneRow:
    def __init__(self, row) -> None:
        self._row = row

    def acquire(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute_core(self, statement):
        return SimpleNamespace(fetchone=lambda: self._row)


@pytest.mark.asyncio
async def test_saving_a_second_askamerica_source_is_refused_naming_the_first():
    from provisa.api.admin.schema_common import _another_govdata_source

    refused = await _another_govdata_source(_OneRow(("test-askamerica",)), _input("aa-key"))
    assert refused is not None and not refused.success
    assert "AskAmerica source 'test-askamerica' already exists" in refused.message
    assert await _another_govdata_source(_OneRow(None), _input("aa-key")) is None


# -- which schemas a source offers is Provisa's own ----------------------------------------------
#
# The adapter serves every schema it has. The source offers the ones its subjects bring.


def test_a_schema_outside_the_sources_list_is_refused_by_name():
    source = _source("sec, ECON ,ref")
    assert aa.serves_schema(source, "sec") and aa.serves_schema(source, "Econ")
    assert not aa.serves_schema(source, "weather")
    aa.require_schema_served(source, "ref")
    with pytest.raises(aa.SchemaNotServed) as refused:
        aa.require_schema_served(source, "weather")
    assert "'test-askamerica' does not serve schema 'weather'" in str(refused.value)
    assert "['sec', 'econ', 'ref']" in str(refused.value)


# -- discovery asks the adapter's information_schema, never its pg_catalog -----------------------
#
# The adapter answers information_schema through Calcite as soon as it listens. Its pg_catalog is
# another matter: the first pg_catalog query makes it count the rows of every table it serves
# (673 for the full model), minutes during which nothing that waits on it can answer. An engine's
# attach reads pg_catalog (its own scanner), so statements wait for that; discovery does not.


class _Adapter:
    """The adapter over a plain pg connection: answers information_schema, records statements."""

    def __init__(self, answers) -> None:
        self._answers = answers
        self.statements: list[str] = []

    async def fetch(self, sql, *args, timeout=None):
        self.statements.append(sql)
        return self._answers(sql)

    async def close(self):
        self.statements.append("closed")


@pytest.fixture
def adapter(monkeypatch):
    """A listening server whose catalog is NOT prepared: anything that waits on it would raise."""

    class _Replica:
        def endpoint(self, *, timeout=None):
            return pr.PortPair(5440, "127.0.0.1", 5540)

        def await_catalog(self, ports, timeout):
            raise AssertionError("discovery waited on the adapter's pg_catalog")

    def _answers(sql):
        if "information_schema.tables" in sql:
            return [{"table_name": "filings"}, {"table_name": "financial_facts"}]
        return [
            {"table_name": "filings", "column_name": "cik", "data_type": "character varying"},
            {"table_name": "filings", "column_name": "form", "data_type": "character varying"},
            {"table_name": "financial_facts", "column_name": "cik", "data_type": "bigint"},
        ]

    conn = _Adapter(_answers)

    async def _connect(host, port):
        return conn

    monkeypatch.setattr(pr, "_endpoint_replica", lambda source: _Replica())
    monkeypatch.setattr(pr, "_pg_connect", _connect)
    return conn


@pytest.mark.asyncio
async def test_a_schemas_tables_are_one_information_schema_statement(adapter):
    assert await pr.adapter_tables(_source(), "sec") == ["filings", "financial_facts"]
    assert len(adapter.statements) == 2 and adapter.statements[1] == "closed"
    assert "information_schema.tables" in adapter.statements[0]
    assert "table_schema = 'sec'" in adapter.statements[0]
    assert "pg_catalog" not in adapter.statements[0]


@pytest.mark.asyncio
async def test_a_schemas_columns_are_one_information_schema_statement(adapter):
    columns = await pr.adapter_columns(_source(), "sec")
    assert columns == {
        "filings": [("cik", "character varying"), ("form", "character varying")],
        "financial_facts": [("cik", "bigint")],
    }
    assert len(adapter.statements) == 2
    assert "information_schema.columns" in adapter.statements[0]
    assert "ORDER BY table_name, ordinal_position" in adapter.statements[0]
    assert "pg_catalog" not in adapter.statements[0]


@pytest.mark.asyncio
async def test_one_tables_columns_are_asked_for_by_table(adapter):
    await pr.adapter_columns(_source(), "sec", "filings")
    assert "table_name = 'filings'" in adapter.statements[0]


@pytest.mark.asyncio
async def test_discovery_answers_while_the_adapters_catalog_is_still_being_prepared(adapter):
    """The fixture's server is listening with its catalog unprepared: both reads answer."""
    assert await pr.adapter_tables(_source(), "sec")
    assert await pr.adapter_columns(_source(), "sec")
    assert await pr.schema_columns(_source(), "sec") == {
        "filings": ["cik", "form"],
        "financial_facts": ["cik"],
    }


@pytest.mark.asyncio
async def test_discovery_of_a_server_that_does_not_listen_yet_is_starting(monkeypatch):
    class _Replica:
        def endpoint(self, *, timeout=None):
            raise pr.ServerLifecycleError("did not accept connections")

    monkeypatch.setattr(pr, "_endpoint_replica", lambda source: _Replica())
    with pytest.raises(pr.SourceStillStartingError, match="still starting up"):
        await pr.adapter_tables(_source(), "sec")


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", ["sec'; DROP", "", "a b", "pg_catalog.pg_class"])
async def test_a_name_that_is_not_an_identifier_is_refused(adapter, bad):
    with pytest.raises(ValueError, match="not a schema name|not a table name"):
        await pr.adapter_tables(_source(), bad)
    with pytest.raises(ValueError, match="not a schema name|not a table name"):
        await pr.adapter_columns(_source(), "sec", bad)
    assert adapter.statements == []


@pytest.mark.asyncio
async def test_the_one_reader_refuses_a_statement_on_pg_catalog(adapter):
    with pytest.raises(ValueError, match="pg_catalog"):
        await pr._read_information_schema(_source(), "SELECT 1 FROM pg_catalog.pg_class")
    assert adapter.statements == []


class _Unbound:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


@pytest.fixture
def discovered(monkeypatch, adapter):
    """The admin's listers for a source saved with sec, ref and geo."""
    from provisa.api.admin import introspect, schema_query

    async def _source_of(source_id):
        return _source("sec,ref,geo")

    monkeypatch.setattr(schema_query, "_source_for_introspection", _source_of)
    monkeypatch.setattr(introspect, "_adapter_bound", _Unbound, raising=False)
    return adapter


@pytest.mark.asyncio
async def test_the_register_table_list_is_read_from_the_adapters_information_schema(discovered):
    from provisa.api.admin.introspect import native_tables

    conn = _OneRow(SimpleNamespace(id="test-askamerica", database="sec,ref,geo"))
    inside = await native_tables("test-askamerica", "govdata", "sec", None, conn, None)  # type: ignore[arg-type]
    assert inside is not None and [t.name for t in inside] == ["filings", "financial_facts"]
    assert "information_schema.tables" in discovered.statements[0]


@pytest.mark.asyncio
async def test_a_schema_outside_the_sources_list_lists_no_tables(discovered):
    from provisa.api.admin.introspect import native_tables

    conn = _OneRow(SimpleNamespace(id="test-askamerica", database="sec,ref,geo"))
    outside = await native_tables("test-askamerica", "govdata", "weather", None, conn, None)  # type: ignore[arg-type]
    assert outside == []  # never listed
    assert discovered.statements == []  # and the adapter is not asked


@pytest.mark.asyncio
async def test_a_selected_tables_columns_are_read_from_the_adapters_information_schema(
    discovered,
):
    from provisa.api.admin.introspect import native_columns

    columns = await native_columns("test-askamerica", "govdata", "sec", "filings", None)  # type: ignore[arg-type]
    assert columns == [("cik", "character varying"), ("form", "character varying")]
    assert "table_name = 'filings'" in discovered.statements[0]
    assert "pg_catalog" not in discovered.statements[0]


@pytest.mark.asyncio
async def test_columns_of_a_schema_outside_the_sources_list_are_refused_by_name(discovered):
    from provisa.api.admin.introspect import native_columns

    with pytest.raises(aa.SchemaNotServed, match="weather"):
        await native_columns("test-askamerica", "govdata", "weather", "stations", None)  # type: ignore[arg-type]


def test_the_wait_a_statement_meets_says_what_it_is():
    """Discovery no longer waits on the catalog; a statement the engine computes still does,
    because the engine's attach reads pg_catalog. The message names that wait."""
    said = str(pr.SourceStillStartingError("test-askamerica", preparing_catalog=True))
    assert said.startswith("STARTING:")
    assert "can be registered now" in said and "counting the rows" in said


def test_registration_refuses_a_schema_outside_the_list_before_anything_is_written():
    """The check sits before the table row is written (schema_mutation_ops.register_table)."""
    import inspect

    from provisa.api.admin import schema_mutation_ops

    body = inspect.getsource(schema_mutation_ops.register_table)
    check = body.index("require_schema_served(_given, input.schema_name)")
    assert check < body.index("_effective_view_sql = input.view_sql")
    assert "return MutationResult(success=False, message=str(_not_served))" in body


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

        def await_catalog(self, ports, timeout):
            return None

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


# -- nothing changed for the single-schema siblings --------------------------------------------

_SIBLINGS = ("files", "sharepoint", "splunk", "salesforce", "cloudops")


def _sibling(stype: str) -> Any:
    return SimpleNamespace(
        id="my-src", type=SourceType(stype), catalog="my_src", schema_name="x", table_name="t"
    )


@pytest.mark.parametrize("stype", _SIBLINGS)
def test_a_single_schema_sibling_attaches_on_duckdb_exactly_as_before(endpoint, stype):
    assert build_duckdb_engine().connectors[stype].details(_sibling(stype)) == {
        "attach": "ATTACH 'host=127.0.0.1 port=5440 user=provisa dbname=provisa' "
        'AS "_src_my-src" (TYPE postgres, READ_ONLY)',
        "raw_alias": "_src_my-src",
        "remote_schema": "my_src",
    }


@pytest.mark.parametrize("stype", _SIBLINGS)
def test_a_single_schema_sibling_attaches_on_postgres_exactly_as_before(endpoint, stype):
    assert build_pg_engine().connectors[stype].details(_sibling(stype)) == {
        "attach_ddl": [
            "CREATE EXTENSION IF NOT EXISTS postgres_fdw",
            'CREATE SERVER IF NOT EXISTS "fdw_pgwire_my_src" FOREIGN DATA WRAPPER postgres_fdw '
            "OPTIONS (host '127.0.0.1', port '5440', dbname 'provisa')",
            'CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER "fdw_pgwire_my_src" '
            "OPTIONS (user 'provisa')",
            'CREATE SCHEMA IF NOT EXISTS "fdw_pgwire_my_src"',
            'IMPORT FOREIGN SCHEMA my_src FROM SERVER "fdw_pgwire_my_src" INTO "fdw_pgwire_my_src"',
        ],
        "local_schema": "fdw_pgwire_my_src",
    }


@pytest.mark.parametrize("stype", _SIBLINGS)
def test_a_single_schema_sibling_attaches_on_clickhouse_exactly_as_before(endpoint, stype):
    assert build_clickhouse_engine().connectors[stype].details(_sibling(stype)) == {
        "attach_ddl": [
            'CREATE DATABASE IF NOT EXISTS "ch_pgwire_my_src" ENGINE = PostgreSQL('
            "'127.0.0.1:5440', 'provisa', 'provisa', '', 'my_src')"
        ],
        "local_schema": "ch_pgwire_my_src",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("stype", _SIBLINGS)
async def test_a_single_schema_sibling_is_landed_from_its_one_schema_as_before(stype):
    statements: list[str] = []

    class _Conn:
        async def fetch(self, sql, *args, timeout=None):
            statements.append(sql)
            return []

        async def close(self):
            return None

    async def connect(host, port):
        return _Conn()

    source = Source(id="my-src", type=SourceType(stype))
    replica = pr.ConnectorReplica(source, connect=connect)
    replica.endpoint = lambda timeout=None: pr.PortPair(5440, "127.0.0.1", 5540)  # type: ignore[method-assign]
    await replica.load(SimpleNamespace(schema_name="anything", table_name="t"))
    await replica.load_keys(SimpleNamespace(schema_name="anything", table_name="t"), ["id"], [(1,)])
    assert statements == [
        'SELECT * FROM "my_src"."t"',
        'SELECT * FROM "my_src"."t" WHERE "id" = ANY($1)',
    ]


# -- the key and what it resolves to are never written anywhere a reader could find them -------

_SECRETS = ("aa-key", "AK", "SK", "TOKEN")


def test_no_error_this_source_raises_carries_the_key_or_its_credentials():
    creds = aa.resolve_storage_credentials("aa-key", fetch=lambda key: (200, _ANSWER))
    said = [
        repr(creds),
        str(creds),
        str(aa.AskAmericaKeyMissing("test-askamerica")),
        str(pr.SourceStillStartingError("test-askamerica")),
        str(pr.ServerExited("test-askamerica", 1, "the server's own log tail")),
    ]
    for refusal in (
        lambda: aa.resolve_storage_credentials(
            "aa-key", fetch=lambda key: (401, {"error": "invalid_api_key"})
        ),
        lambda: aa.resolve_storage_credentials(
            "aa-key", fetch=lambda key: (502, {"error": "credential_mint_failed"})
        ),
    ):
        with pytest.raises((aa.AskAmericaKeyRefused, aa.AskAmericaUnavailable)) as raised:
            refusal()
        said.append(str(raised.value))
    for text in said:
        for secret in _SECRETS:
            assert secret not in text.replace("AskAmerica", ""), text


def test_the_servers_command_line_carries_no_secret_and_no_file_is_written(tmp_path, monkeypatch):
    """A command line is in the process table and a file is on disk; the environment is the only
    carrier. What the install lays down is the bundle's own seed, which holds neither."""
    bundle = _install_bundle(tmp_path / "bundle")
    before = {p: p.read_bytes() for p in bundle.rglob("*") if p.is_file()}
    spawned: list = []
    _replica_on(bundle, spawned, monkeypatch).endpoint()
    command, _, _ = spawned[0]
    for secret in _SECRETS:
        assert secret not in " ".join(command)
    after = {p: p.read_bytes() for p in bundle.rglob("*") if p.is_file()}
    assert {p: data for p, data in after.items() if p in before} == before
    for data in after.values():
        for secret in ("aa-key", "TOKEN"):
            assert secret.encode() not in data


def test_starting_the_server_logs_no_secret(tmp_path, caplog, monkeypatch):
    import logging

    _install_bundle(tmp_path / "bundle")
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setattr(aa, "_fetch", lambda key: (200, _ANSWER))
    replica = pr.ConnectorReplica(
        _source(),
        resolver=SimpleNamespace(resolve=lambda spec: tmp_path / "bundle"),  # type: ignore[arg-type]
        allocator=pr.PortAllocator(is_free=lambda port: True),
        spawn=lambda *args: SimpleNamespace(exit_code=lambda: None),
        health_check=lambda host, port: True,
    )
    with caplog.at_level(logging.DEBUG):
        replica.endpoint()
    for secret in _SECRETS:
        assert secret not in caplog.text.replace("AskAmerica", "")


# -- while the adapter starts, a statement reading the source says so --------------------------


def _replica_with_server(
    *, healthy: bool, exit_code: int | None, catalog_ready: bool = True, prepare=None
):
    replica = pr.ConnectorReplica(_source(), prepare_catalog=prepare)
    replica._server = SimpleNamespace(  # type: ignore[assignment]
        health=lambda: healthy,
        exit_code=lambda: exit_code,
        log_tail=lambda: "the log's end",
        ports=pr.PortPair(5440, "127.0.0.1", 5540),
        stop=lambda: None,
    )
    replica._catalog_ready = catalog_ready
    return replica


def test_a_statement_reading_a_starting_source_is_refused_as_starting(monkeypatch):
    monkeypatch.setitem(
        pr._ENDPOINTS, "test-askamerica", _replica_with_server(healthy=False, exit_code=None)
    )
    with pytest.raises(pr.SourceStillStartingError, match="STARTING: 'test-askamerica'"):
        pr.require_serving("test-askamerica", "govdata")


def test_a_statement_reading_a_source_whose_server_exited_is_told_why(monkeypatch):
    monkeypatch.setitem(
        pr._ENDPOINTS, "test-askamerica", _replica_with_server(healthy=False, exit_code=3)
    )
    with pytest.raises(pr.ServerExited, match=r"(?s)exited with code 3.*the log's end") as raised:
        pr.require_serving("test-askamerica", "govdata")
    assert "aa-key" not in str(raised.value)


def test_a_serving_source_and_a_source_with_no_server_yet_are_not_refused(monkeypatch):
    monkeypatch.setitem(
        pr._ENDPOINTS, "test-askamerica", _replica_with_server(healthy=True, exit_code=None)
    )
    pr.require_serving("test-askamerica", "govdata")
    pr.require_serving("never-started", "govdata")
    pr.require_serving("some-pg", "postgresql")


def test_the_pipeline_asks_only_for_a_statement_the_engine_computes(monkeypatch):
    from provisa.pgwire import _pipeline
    from provisa.transpiler.router import Route

    asked: list[tuple[str, str]] = []
    monkeypatch.setattr(pr, "require_serving", lambda sid, stype: asked.append((sid, stype)))
    state = SimpleNamespace(source_types={"test-askamerica": "govdata", "pg": "postgresql"})
    sources = {"test-askamerica", "pg"}
    _pipeline._refuse_while_source_server_starts(
        SimpleNamespace(route=Route.DIRECT), sources, state
    )
    assert asked == []
    _pipeline._refuse_while_source_server_starts(
        SimpleNamespace(route=Route.ENGINE), sources, state
    )
    assert asked == [("pg", "postgresql"), ("test-askamerica", "govdata")]


# -- subscriptions: as a sibling's -------------------------------------------------------------


@pytest.mark.parametrize("stype", ["govdata", "sharepoint", "salesforce", "cloudops"])
def test_no_pgwire_family_source_has_a_subscription_provider(stype):
    """The family has none today; a subscription on one is refused naming the type. AskAmerica
    has no provider of its own."""
    from provisa.subscriptions.registry import get_provider, supports_polling_fallback

    assert not supports_polling_fallback(stype)
    with pytest.raises(ValueError, match=f"No subscription provider for source_type='{stype}'"):
        get_provider(stype, {})


# -- the server's first catalog query is paid for off the request path --------------------------
#
# A listening adapter's FIRST catalog query can take far longer than any later one (a catalog
# not prebuilt for its model is built then). An engine's attach issues that query, and the attach pass runs on the request path
# under the engine's one attach lock: paid for there, it held every statement of every source.


def test_a_listening_server_is_still_starting_until_its_catalog_is_prepared():
    import threading

    release = threading.Event()
    asked: list[pr.PortPair] = []

    def prepare(ports):
        asked.append(ports)
        assert release.wait(5)

    replica = _replica_with_server(
        healthy=True, exit_code=None, catalog_ready=False, prepare=prepare
    )
    ports = pr.PortPair(5440, "127.0.0.1", 5540)
    for _ in range(3):  # every attach meanwhile: answered at once, the query sent only once
        with pytest.raises(pr.SourceStillStartingError, match="test-askamerica"):
            replica.await_catalog(ports, 0)
    with pytest.raises(pr.SourceStillStartingError):
        replica.require_serving()
    release.set()
    replica.await_catalog(ports, 5)  # prepared: the attach that follows goes ahead
    replica.require_serving()
    assert asked == [ports]


def test_the_attach_itself_never_waits_for_the_catalog(monkeypatch):
    """What the engine's attach pass calls: it returns or refuses at once, whatever the count
    takes, so the pass never holds its lock for it."""
    import threading
    import time

    release = threading.Event()
    replica = _replica_with_server(
        healthy=True,
        exit_code=None,
        catalog_ready=False,
        prepare=lambda ports: release.wait(30),
    )
    replica.endpoint = lambda timeout=None: pr.PortPair(5440, "127.0.0.1", 5540)  # type: ignore[method-assign]
    monkeypatch.setattr(pr, "_endpoint_replica", lambda source: replica)
    started = time.monotonic()
    with pytest.raises(pr.SourceStillStartingError):
        pr.ensure_endpoint(_source())
    assert time.monotonic() - started < 2
    release.set()


def test_a_caller_that_gives_a_wait_gets_the_endpoint_once_the_catalog_is_prepared(monkeypatch):
    prepared: list[pr.PortPair] = []
    replica = _replica_with_server(
        healthy=True, exit_code=None, catalog_ready=False, prepare=prepared.append
    )
    replica.endpoint = lambda timeout=None: pr.PortPair(5440, "127.0.0.1", 5540)  # type: ignore[method-assign]
    monkeypatch.setattr(pr, "_endpoint_replica", lambda source: replica)
    assert pr.ensure_endpoint(_source(), timeout=5) == pr.PortPair(5440, "127.0.0.1", 5540)
    assert len(prepared) == 1


def test_a_catalog_that_cannot_be_prepared_is_reported_and_not_asked_for_again():
    calls: list[int] = []

    def prepare(ports):
        calls.append(1)
        raise RuntimeError("Object 'x' not found")

    replica = _replica_with_server(
        healthy=True, exit_code=None, catalog_ready=False, prepare=prepare
    )
    ports = pr.PortPair(5440, "127.0.0.1", 5540)
    for _ in range(3):
        with pytest.raises(
            pr.ServerCatalogFailed, match="could not prepare its catalog.*not found"
        ):
            replica.await_catalog(ports, 5)
    assert calls == [1]
    assert isinstance(pr.ServerCatalogFailed("s", RuntimeError("x")), pr.server_start_errors())


def test_a_restarted_server_has_its_catalog_prepared_afresh():
    prepared: list[pr.PortPair] = []
    replica = _replica_with_server(
        healthy=True, exit_code=None, catalog_ready=False, prepare=prepared.append
    )
    ports = pr.PortPair(5440, "127.0.0.1", 5540)
    replica.await_catalog(ports, 5)
    replica.close()
    replica.await_catalog(ports, 5)
    assert len(prepared) == 2


@pytest.mark.parametrize("stype", _SIBLINGS)
def test_every_sibling_waits_for_its_catalog_within_the_bound_it_waits_for_its_port(
    monkeypatch, stype
):
    """The family shares the server and its row counting. A sibling's server starts in seconds
    and is still waited for; its catalog is now waited for too, within the same bound, where
    the attach used to wait on it with none."""
    waits: list[tuple[str, float | None]] = []

    class _Replica:
        def endpoint(self, *, timeout=None):
            waits.append(("port", timeout))
            return pr.PortPair(5440, "127.0.0.1", 5540)

        def await_catalog(self, ports, timeout):
            waits.append(("catalog", timeout))

    monkeypatch.setattr(pr, "_endpoint_replica", lambda source: _Replica())
    pr.ensure_endpoint(Source(id="docs", type=SourceType(stype)))
    pr.ensure_endpoint(Source(id="docs", type=SourceType(stype)), timeout=3)
    assert waits == [
        ("port", None),
        ("catalog", pr.SERVER_READY_SECONDS),
        ("port", 3),
        ("catalog", 3),
    ]


def test_the_two_waits_are_told_apart_and_both_are_starting():
    port = str(pr.SourceStillStartingError("test-askamerica"))
    catalog = str(pr.SourceStillStartingError("test-askamerica", preparing_catalog=True))
    assert port.startswith("STARTING:") and "still starting up" in port
    assert catalog.startswith("STARTING:") and "counting the rows" in catalog
    replica = _replica_with_server(
        healthy=True, exit_code=None, catalog_ready=False, prepare=lambda ports: time.sleep(5)
    )
    with pytest.raises(pr.SourceStillStartingError, match="counting the rows"):
        replica.await_catalog(pr.PortPair(5440, "127.0.0.1", 5540), 0)
    with pytest.raises(pr.SourceStillStartingError, match="still starting up"):
        _replica_with_server(healthy=False, exit_code=None).require_serving()


def test_a_preparation_outlived_by_its_server_reports_to_no_one():
    release = threading.Event()

    def prepare(ports):
        release.wait(5)
        raise RuntimeError("connection closed: the server was stopped")

    replica = _replica_with_server(
        healthy=True, exit_code=None, catalog_ready=False, prepare=prepare
    )
    ports = pr.PortPair(5440, "127.0.0.1", 5540)
    with pytest.raises(pr.SourceStillStartingError):
        replica.await_catalog(ports, 0)
    stale = replica._catalog_thread
    assert stale is not None
    replica.close()  # the server is stopped under the preparation
    release.set()
    stale.join(5)
    assert replica._catalog_error is None and not replica._catalog_ready


def test_preparing_the_catalog_logs_nothing_and_its_failure_carries_no_secret(caplog):
    import logging

    def prepare(ports):
        raise RuntimeError("could not connect to server")

    replica = _replica_with_server(
        healthy=True, exit_code=None, catalog_ready=False, prepare=prepare
    )
    with caplog.at_level(logging.DEBUG), pytest.raises(pr.ServerCatalogFailed) as failed:
        replica.await_catalog(pr.PortPair(5440, "127.0.0.1", 5540), 5)
    for secret in _SECRETS:
        assert secret not in caplog.text.replace("AskAmerica", "")
        assert secret not in str(failed.value).replace("AskAmerica", "")


def test_the_real_preparation_is_one_catalog_query_on_a_connection_it_closes(monkeypatch):
    events: list[str] = []

    class _Conn:
        async def fetch(self, sql, *args, timeout=None):
            events.append(sql)
            return [(1,)]

        async def close(self):
            events.append("closed")

    async def connect(host, port):
        events.append(f"connect {host}:{port}")
        return _Conn()

    monkeypatch.setattr(pr, "_pg_connect", connect)
    pr._prepare_catalog.real(pr.PortPair(5440, "127.0.0.1", 5540))  # type: ignore[attr-defined]
    assert events == [
        "connect 127.0.0.1:5440",
        "SELECT count(*) FROM pg_catalog.pg_class",
        "closed",
    ]


def test_a_failed_catalog_query_still_closes_its_connection(monkeypatch):
    events: list[str] = []

    class _Conn:
        async def fetch(self, sql, *args, timeout=None):
            raise RuntimeError("server closed the connection")

        async def close(self):
            events.append("closed")

    async def connect(host, port):
        return _Conn()

    monkeypatch.setattr(pr, "_pg_connect", connect)
    with pytest.raises(RuntimeError, match="server closed"):
        pr._prepare_catalog.real(pr.PortPair(5440, "127.0.0.1", 5540))  # type: ignore[attr-defined]
    assert events == ["closed"]


def test_the_engines_attach_pass_never_waits_on_a_source_whose_catalog_is_being_prepared(
    monkeypatch,
):
    """The pass runs under the engine's one attach lock, on the request path. Over [a fast
    source, an AskAmerica source whose catalog is still being prepared, another fast source] it
    returns at once, attaches the other two, and leaves the pass incomplete so the next one
    retries the source that was starting."""
    release = threading.Event()
    replica = _replica_with_server(
        healthy=True,
        exit_code=None,
        catalog_ready=False,
        prepare=lambda ports: release.wait(30),
    )
    replica.endpoint = lambda timeout=None: pr.PortPair(5440, "127.0.0.1", 5540)  # type: ignore[method-assign]
    monkeypatch.setattr(pr, "_endpoint_replica", lambda source: replica)

    class _Runtime:
        def __init__(self) -> None:
            self.attached: list[str] = []

        def attach_source(self, merged) -> None:
            if merged.type.value == "govdata":
                pr.ensure_endpoint(merged)  # what the connector's details() does first
            self.attached.append(merged.id)

        def detach_source(self, *_a) -> None:
            pass

    def _src(sid: str, stype: str):
        return SimpleNamespace(
            id=sid,
            type=SimpleNamespace(value=stype),
            replicate=None,
            load_protected=False,
            host="h",
            port=None,
            base_url=None,
            database="sec",
            username="aa-key",
            password=None,
            path=None,
            federation_hints={},
            mapping={},
        )

    def _tbl(sid: str, table: str):
        return SimpleNamespace(source_id=sid, schema_name="sec", table_name=table, region=None)

    backend = build_duckdb_engine().backend
    backend._runtime = _Runtime()
    config = SimpleNamespace(
        sources=[_src("pg-a", "postgresql"), _src("aa", "govdata"), _src("pg-b", "postgresql")],
        tables=[_tbl("pg-a", "orders"), _tbl("aa", "financial_facts"), _tbl("pg-b", "items")],
    )
    state = SimpleNamespace(
        runtime_sources={},
        tables=[],
        source_catalogs={"pg-a": "pg_a", "aa": "aa", "pg-b": "pg_b"},
    )
    started = time.monotonic()
    complete = backend._walk_registry(state, config)
    elapsed = time.monotonic() - started
    release.set()

    assert elapsed < 2, f"the attach pass waited {elapsed:.1f}s on the starting source"
    assert backend._runtime.attached == ["pg-a", "pg-b"]
    assert complete is False  # retried by the next pass, when the catalog is prepared
