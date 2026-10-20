# Copyright (c) 2026 Kenneth Stott
# Canary: 9d8b46ad-73a8-43c9-bfe8-1c5faeb38983
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A built code host attached by the engine (REQ-2100): which code tables are attached rather
than landed; the organisation's identity written where each engine reads it, readable by that
engine alone; and each engine's attach of the host, which names those files and the schemas the
source's tables are registered under."""

# Requirements: REQ-2100
from __future__ import annotations

import stat
from pathlib import Path
from types import SimpleNamespace

import pytest

from provisa.codesource import attach, engine_identity
from provisa.codesource.remote import new_identity
from provisa.federation.codehost_connectors import (
    PG_FILES_AT_ENV,
    PG_FILES_ENV,
    DuckDBCodeHostConnector,
    PgCodeHostConnector,
    Prepared,
    postgres_shares_files,
)
from provisa.federation.trino_connectors import TrinoCodeHostConnector
from provisa.federation.connector_base import Mechanism
from provisa.federation.replica_address import ReplicaRoutes

pytestmark = pytest.mark.unit

_TABLE = {
    "id": 4,
    "source_id": "scores-src",
    "schema_name": "default",
    "table_name": "scores",
    "cache_ttl": 0,
    "columns": [
        {"column_name": "id", "native_filter_type": None},
        {"column_name": "_nf_n", "native_filter_type": "query_arg"},
    ],
}


def _manifest(*, preview: bool = False) -> dict:
    from provisa.codesource.manifest import (
        LOOSE,
        Anonymous,
        Export,
        Field,
        Manifest,
        Scalar,
        manifest_json,
    )

    rows = LOOSE if preview else Anonymous((Field("id", Scalar("bigint"), False),))
    return manifest_json(Manifest((Export("scores", "table", "", (), rows, preview),), {}))


_BUILT = {
    "mode": "built",
    "remote": {"host": "gpu.box", "port": 7443, "pin": "p", "manifest": _manifest()},
}


def _state(*, mode="built", engine_connector=True, floored=False, ttl=0, tables=None):
    connectors = {"python": DuckDBCodeHostConnector("python")} if engine_connector else {}
    return SimpleNamespace(
        source_types={"scores-src": "python", "pg": "postgresql"},
        runtime_sources={"scores-src": {"mapping": {**_BUILT, "mode": mode}}},
        federation_engine=SimpleNamespace(engine=SimpleNamespace(connectors=connectors)),
        replica_routes=ReplicaRoutes(floored={4: ("scores-src", "replicate")} if floored else {}),
        source_cache={},
        response_cache_default_ttl=ttl,
        tables=tables if tables is not None else [_TABLE],
    )


# -- which tables are attached ---------------------------------------------------------------------


def test_a_built_sources_table_is_attached_where_the_engine_attaches_a_host():
    state = _state()
    assert attach.attached(state, {**_TABLE, "cache_ttl": None})
    assert attach.parameters(state, [4]) == {"_nf_n"}


@pytest.mark.parametrize(
    "state, table",
    [
        (_state(mode="dev"), _TABLE),  # its code on the server: landed
        (_state(engine_connector=False), _TABLE),  # an engine with no attach for code hosts
        (_state(floored=True), _TABLE),  # the operator's settings put it on its replica
        (_state(), {**_TABLE, "cache_ttl": 300}),  # it declares a landing TTL above 0
    ],
)
def test_a_code_table_is_landed_otherwise(state, table):
    assert not attach.attached(state, table)


def test_a_function_typed_by_a_preview_is_landed():
    # Its columns are not declared, so its host serves it over no pgwire catalog.
    state = _state()
    state.runtime_sources["scores-src"]["mapping"] = {
        **_BUILT,
        "remote": {**_BUILT["remote"], "manifest": _manifest(preview=True)},
    }
    assert not attach.attached(state, _TABLE)


def test_the_response_caches_default_ttl_lands_no_code_table():
    # The response cache is a request's opt-in: its default is no reason to land a table.
    assert attach.attached(_state(ttl=300), {**_TABLE, "cache_ttl": None})


def test_a_table_of_another_source_is_never_attached_as_a_code_host():
    assert not attach.attached(_state(), {**_TABLE, "source_id": "pg"})


# -- the identity's files --------------------------------------------------------------------------


def _mode(path: str) -> int:
    return stat.S_IMODE(Path(path).stat().st_mode)


async def test_trinos_files_are_its_alone_and_named_by_the_org(tmp_path, monkeypatch):
    monkeypatch.setenv("PROVISA_TRINO_ETC_DIR", str(tmp_path))
    monkeypatch.setattr("provisa.federation.k8s_provisioner.provisioning_available", lambda: False)
    identity = new_identity()
    files = await engine_identity.for_trino(None, "acme", identity, {"scores-src": "HOST PEM"})
    assert (files.certificate, files.key, files.key_der) == (
        "/etc/trino/codehost-client/acme.crt",
        "/etc/trino/codehost-client/acme.key",
        "/etc/trino/codehost-client/acme.pk8",
    )
    written = tmp_path / "codehost-client"
    assert (written / "acme.crt").read_text() == identity.certificate
    assert (written / "acme.key").read_text() == identity.key
    assert (written / "acme.pk8").read_bytes()[:1] == b"\x30"  # DER: a SEQUENCE
    assert files.root("scores-src") == "/etc/trino/codehost-client/acme.scores-src.root.crt"
    assert (written / "acme.scores-src.root.crt").read_text() == "HOST PEM"
    assert {_mode(str(p)) for p in written.iterdir()} == {0o600}
    assert _mode(str(written)) == 0o700


async def test_on_a_provisioned_shard_trinos_files_go_into_its_secret(monkeypatch):
    from provisa.federation import k8s_provisioner

    put: list[tuple[str, dict]] = []

    async def _put(shard, files):
        put.append((shard, files))

    monkeypatch.setattr(k8s_provisioner, "provisioning_available", lambda: True)
    monkeypatch.setattr("provisa.federation.engine_wake.active_shard", lambda state: "shard-1")
    monkeypatch.setattr(k8s_provisioner, "put_codehost_files", _put)
    files = await engine_identity.for_trino(object(), "acme", new_identity(), {"s": "PEM"})
    assert files.key_der == "/etc/trino/codehost-client/acme.pk8"
    ((shard, contents),) = put
    assert shard == "shard-1"
    assert sorted(contents) == ["acme.crt", "acme.key", "acme.pk8", "acme.s.root.crt"]


def test_a_postgres_engine_with_no_shared_directory_is_given_none(monkeypatch):
    monkeypatch.delenv(PG_FILES_ENV, raising=False)
    monkeypatch.delenv(PG_FILES_AT_ENV, raising=False)
    assert not postgres_shares_files()
    with pytest.raises(RuntimeError, match="no directory is shared"):
        engine_identity.for_postgres("acme", new_identity(), {})


def test_a_postgres_engines_files_are_named_where_its_server_reads_them(tmp_path, monkeypatch):
    monkeypatch.setenv(PG_FILES_ENV, str(tmp_path / "app-side"))
    monkeypatch.setenv(PG_FILES_AT_ENV, "/etc/provisa/codehost-client")
    files = engine_identity.for_postgres("acme", new_identity(), {})
    assert files.key == "/etc/provisa/codehost-client/acme.key"
    assert (tmp_path / "app-side" / "acme.key").is_file()


# -- each engine's attach --------------------------------------------------------------------------


def _source():
    return SimpleNamespace(
        id="scores-src",
        mapping=_BUILT,
        schema_name="default",
        catalog="org_acme__scores_src",
    )


@pytest.fixture
def registered(monkeypatch):
    monkeypatch.setattr("provisa.core.request_context.require_current_org", lambda: "acme")


def _files(connector, tmp_path, **changed):
    connector.remember(
        "acme",
        Prepared(
            **{
                "certificate": f"{tmp_path}/acme.crt",
                "key": f"{tmp_path}/acme.key",
                "key_der": f"{tmp_path}/acme.pk8",
                "roots": {"scores-src": f"{tmp_path}/acme.scores-src.root.crt"},
                "schemas": {"scores-src": "default,ml"},
                **changed,
            }
        ),
    )


def test_an_engine_attaches_a_host_only_once_its_identity_is_written(registered):
    with pytest.raises(RuntimeError, match="not written for the duckdb engine yet"):
        DuckDBCodeHostConnector("python").details(_source())


def test_a_source_with_no_registered_table_is_not_attached(registered, tmp_path):
    connector = DuckDBCodeHostConnector("python")
    _files(connector, tmp_path, schemas={})
    with pytest.raises(ValueError, match="has no registered table to attach"):
        connector.details(_source())


def test_a_host_whose_certificate_is_not_recorded_is_not_attached(registered, tmp_path):
    connector = DuckDBCodeHostConnector("python")
    _files(connector, tmp_path, roots={})
    with pytest.raises(RuntimeError, match="its host's certificate is not recorded; connect it"):
        connector.details(_source())


def test_duckdb_attaches_the_host_read_only_with_the_identity_and_schemas(registered, tmp_path):
    connector = DuckDBCodeHostConnector("python")
    assert connector.mechanism == Mechanism.ATTACH_R
    _files(connector, tmp_path)
    details = connector.details(_source())
    assert details["raw_alias"] == "_src_scores-src"
    assert details["attach"] == (
        f"ATTACH 'host=gpu.box port=7443 dbname=code user=provisa sslmode=verify-ca "
        f"sslrootcert={tmp_path}/acme.scores-src.root.crt sslcert={tmp_path}/acme.crt "
        f"sslkey={tmp_path}/acme.key "
        "options=''-c provisa.schemas=default,ml''' AS \"_src_scores-src\" (TYPE postgres, "
        "READ_ONLY)"
    )


def test_trino_attaches_the_host_through_its_postgresql_connector(registered, tmp_path):
    connector = TrinoCodeHostConnector("python")
    _files(connector, tmp_path)
    details = connector.details(_source())
    assert details["connection-url"] == (
        f"jdbc:postgresql://gpu.box:7443/code?sslmode=verify-ca"
        f"&sslrootcert={tmp_path}/acme.scores-src.root.crt&sslcert={tmp_path}/acme.crt"
        f"&sslkey={tmp_path}/acme.pk8&options=-c%20provisa.schemas%3Ddefault%2Cml"
    )
    assert details["case-insensitive-name-matching"] == "true"
    assert details["statistics.enabled"] == "true"


def test_a_postgres_engine_attaches_the_host_through_postgres_fdw(registered, tmp_path):
    connector = PgCodeHostConnector("python")
    _files(connector, tmp_path)
    details = connector.details(_source())
    server = details["server"]
    assert details["remote_schema"] == "default"
    assert any(
        f"OPTIONS (host 'gpu.box', port '7443', dbname 'code', sslmode 'verify-ca', "
        f"sslrootcert '{tmp_path}/acme.scores-src.root.crt', sslcert '{tmp_path}/acme.crt', "
        f"sslkey '{tmp_path}/acme.key', options '-c provisa.schemas=default,ml')" in ddl
        for ddl in details["attach_ddl"]
    )
    assert details["attach_ddl"][-1] == (
        f'IMPORT FOREIGN SCHEMA "default" FROM SERVER "{server}" INTO "{details["local_schema"]}"'
    )


async def test_a_postgres_engine_sharing_no_directory_cannot_attach_a_host(monkeypatch):
    monkeypatch.delenv(PG_FILES_ENV, raising=False)

    async def _fetch(_sql):
        return [(1,)]

    probed = await PgCodeHostConnector("python").probe(_fetch)
    assert not probed.available and "shares no directory" in probed.reason


async def test_the_schema_build_writes_the_identity_and_names_each_sources_schemas(monkeypatch):
    from pathlib import Path

    from provisa.codesource import remote_sources

    identity = new_identity()

    async def _identity(org_id):
        assert org_id == "acme"
        return identity

    monkeypatch.setattr(remote_sources, "identity", _identity)
    monkeypatch.setattr("provisa.core.request_context.require_current_org", lambda: "acme")
    state = _state(tables=[_TABLE, {**_TABLE, "id": 5, "schema_name": "ml"}])
    state.runtime_sources["scores-src"].update(
        type="python",
        mapping={**_BUILT, "remote": {**_BUILT["remote"], "certificate": "HOST PEM"}},
    )
    await attach.prepare(state)
    connector = state.federation_engine.engine.connectors["python"]
    prepared = connector.prepared()
    assert prepared.schemas_of("scores-src") == "default,ml"
    assert Path(prepared.root("scores-src")).read_text() == "HOST PEM"
    assert Path(prepared.certificate).read_text() == identity.certificate


def test_trino_gets_no_catalog_for_a_dev_source_or_before_its_identity_is_written(
    registered, tmp_path
):
    connector = TrinoCodeHostConnector("python")
    assert connector.details(_source()) == {}  # built, but the schema build has not written it
    _files(connector, tmp_path)
    dev = _source()
    dev.mapping = {"mode": "dev"}
    assert connector.details(dev) == {}  # landed, never attached
    assert connector.details(_source())["connection-user"] == "provisa"
