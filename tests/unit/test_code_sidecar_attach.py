# Copyright (c) 2026 Kenneth Stott
# Canary: 5b8e2f17-c94a-4d03-a6e1-2f7c90b4d851
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A dev-mode code source read by each federation engine through its pgwire sidecar (REQ-2267
part 3): each engine's attach dials the sidecar where engines reach it, in plain PostgreSQL, naming
the schemas the source's tables are registered under; the schema build names each dev-mode source
to the engine's attach connectors, stopping a sidecar whose code changed; and the engine's catalog
of a source goes when its sidecar stops for good. A real DuckDB attach of a running sidecar is in
tests/unit/test_code_pgwire_sidecar.py."""

# Requirements: REQ-2267
from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.codesource import attach, pgwire_sidecar
from provisa.federation import codehost_connectors
from provisa.federation.codehost_connectors import (
    DevSource,
    DuckDBCodeHostConnector,
    PgCodeHostConnector,
)
from provisa.federation.trino_connectors import TrinoCodeHostConnector

pytestmark = pytest.mark.unit

_PREPARED = SimpleNamespace(org_id="acme", source_id="scores-src", content_hash="h1", remote=None)


@pytest.fixture(autouse=True)
def isolated(monkeypatch):
    """No source named for any engine, a sidecar resolver that records what it was asked for, and
    the bound organisation."""
    asked: list = []

    def _resolver(prepared):
        asked.append(prepared)
        return ("host.docker.internal", 15433)

    monkeypatch.setattr(codehost_connectors, "_PREPARED", {})
    monkeypatch.setattr(codehost_connectors, "_DEV", {})
    monkeypatch.setattr(codehost_connectors, "_sidecar", _resolver)
    monkeypatch.setattr("provisa.core.request_context.require_current_org", lambda: "acme")
    monkeypatch.setattr(pgwire_sidecar, "_listeners", {})
    monkeypatch.setattr(pgwire_sidecar, "_sidecars", {})
    return asked


def _source(mapping=None):
    return SimpleNamespace(
        id="scores-src",
        type=SimpleNamespace(value="python"),
        mapping={"mode": "dev"} if mapping is None else mapping,
        schema_name="default",
        catalog="org_acme__scores_src",
    )


def _named(connector):
    connector.remember_dev("acme", {"scores-src": DevSource(_PREPARED, "default,ml")})


# -- each engine's attach ------------------------------------------------------------------------


def test_duckdb_attaches_a_dev_sources_sidecar_in_plain_postgresql(isolated):
    connector = DuckDBCodeHostConnector("python")
    _named(connector)
    details = connector.details(_source())
    assert details["raw_alias"] == "_src_scores-src"
    assert details["attach"] == (
        "ATTACH 'host=host.docker.internal port=15433 dbname=code user=provisa sslmode=disable "
        "options=''-c provisa.schemas=default,ml''' AS \"_src_scores-src\" (TYPE postgres, "
        "READ_ONLY)"
    )
    assert isolated == [_PREPARED]  # the attach started (or found) the sidecar of its code


def test_a_source_that_names_no_mode_is_dev_and_attached_through_its_sidecar(isolated):
    connector = DuckDBCodeHostConnector("python")
    _named(connector)
    assert "sslmode=disable" in connector.details(_source(mapping={}))["attach"]


def test_a_dev_source_the_schema_build_has_not_named_is_refused_by_name(isolated):
    with pytest.raises(RuntimeError, match="has not named it for the duckdb engine yet"):
        DuckDBCodeHostConnector("python").details(_source())
    assert isolated == []  # no sidecar started for it


def test_trino_attaches_a_dev_sources_sidecar_through_its_postgresql_connector(isolated):
    connector = TrinoCodeHostConnector("python")
    assert connector.details(_source()) == {}  # no catalog before the schema build names it
    assert isolated == []
    _named(connector)
    details = connector.details(_source())
    assert details["connection-url"] == (
        "jdbc:postgresql://host.docker.internal:15433/code?sslmode=disable"
        "&options=-c%20provisa.schemas%3Ddefault%2Cml"
    )
    assert details["connection-user"] == "provisa"
    assert details["case-insensitive-name-matching"] == "true"


def test_a_postgres_engine_attaches_a_dev_sources_sidecar_through_postgres_fdw(isolated):
    connector = PgCodeHostConnector("python")
    _named(connector)
    details = connector.details(_source())
    server = details["server"]
    assert (
        f'CREATE SERVER IF NOT EXISTS "{server}" FOREIGN DATA WRAPPER postgres_fdw OPTIONS '
        "(host 'host.docker.internal', port '15433', dbname 'code', sslmode 'disable', "
        "options '-c provisa.schemas=default,ml')"
    ) in details["attach_ddl"]
    assert details["attach_ddl"][-1] == (
        f'IMPORT FOREIGN SCHEMA "default" FROM SERVER "{server}" INTO "{details["local_schema"]}"'
    )


def test_a_built_source_keeps_its_hosts_tls_attach(isolated, tmp_path):
    connector = DuckDBCodeHostConnector("python")
    _named(connector)
    connector.remember(
        "acme",
        codehost_connectors.Prepared(
            certificate=f"{tmp_path}/acme.crt",
            key=f"{tmp_path}/acme.key",
            key_der=f"{tmp_path}/acme.pk8",
            roots={"scores-src": f"{tmp_path}/root.crt"},
            schemas={"scores-src": "default"},
        ),
    )
    built = {"mode": "built", "remote": {"host": "gpu.box", "port": 7443}}
    attach_sql = connector.details(_source(mapping=built))["attach"]
    assert "host=gpu.box port=7443" in attach_sql and "sslmode=verify-ca" in attach_sql
    assert isolated == []  # a built source has no sidecar


def test_the_sidecar_module_is_how_an_engine_starts_a_sidecar(monkeypatch):
    # pgwire_sidecar registers its resolver at import: the attach starts the sidecar through it.
    monkeypatch.setattr(codehost_connectors, "_sidecar", None)
    with pytest.raises(RuntimeError, match="pgwire_sidecar has not been imported"):
        codehost_connectors.sidecar_endpoint(_PREPARED)
    pgwire_sidecar._register_engine_endpoint()  # what its import runs
    assert codehost_connectors._sidecar is pgwire_sidecar._engine_endpoint


# -- the schema build ----------------------------------------------------------------------------


_TABLES = [
    {"id": 1, "source_id": "scores-src", "schema_name": "ml", "table_name": "scores"},
    {"id": 2, "source_id": "scores-src", "schema_name": "default", "table_name": "runs"},
    {"id": 3, "source_id": "idle-src", "schema_name": "default", "table_name": "x"},
]


def _state(connector, registered: list):
    return SimpleNamespace(
        runtime_sources={
            "scores-src": {"type": "python", "mapping": {"mode": "dev"}},
            "unregistered-src": {"type": "python", "mapping": {"mode": "dev"}},
            "built-src": {"type": "python", "mapping": {"mode": "built"}},
        },
        tables=_TABLES,
        source_catalogs={"scores-src": "org_acme__scores_src"},
        config=SimpleNamespace(sources=[SimpleNamespace(id="scores-src")]),
        federation_engine=SimpleNamespace(
            engine=SimpleNamespace(scan_connectors={"python": connector}),
            register_source=lambda source, password, catalog_name: registered.append(
                (source.id, catalog_name)
            ),
            drop_source=lambda source_id, catalog_name: registered.append(
                ("dropped", source_id, catalog_name)
            ),
        ),
    )


@pytest.fixture
def prepared_code(monkeypatch):
    """The code each dev-mode source is prepared to, by source; and the sources retired."""
    hashes = {"scores-src": "h2"}
    retired: list = []

    async def _context_for(_state):
        return "ctx"

    async def _prepare(source_id, source_type, mapping, ctx):
        assert (source_type, ctx) == ("python", "ctx")
        return SimpleNamespace(
            org_id="acme", source_id=source_id, content_hash=hashes[source_id], remote=None
        )

    def _retire(prepared):
        retired.append((prepared.source_id, prepared.content_hash))
        return False

    monkeypatch.setattr("provisa.codesource.source.context_for", _context_for)
    monkeypatch.setattr("provisa.codesource.source.prepare", _prepare)

    async def _built(_state):
        """A built source's identity is written by its own step (tests/unit/test_code_attach.py)."""

    monkeypatch.setattr(pgwire_sidecar, "retire_changed", _retire)
    monkeypatch.setattr(attach, "_prepare_built", _built)
    return retired


async def test_the_schema_build_names_each_dev_source_with_its_code_and_schemas(prepared_code):
    connector = DuckDBCodeHostConnector("python")
    registered: list = []
    await attach.prepare(_state(connector, registered))
    named = connector.dev_source("scores-src")
    assert named.schemas == "default,ml"
    assert named.prepared.content_hash == "h2"
    # A source with no registered table has nothing to attach; a built one is not a sidecar's.
    assert not connector.dev_ready("unregistered-src")
    assert not connector.dev_ready("built-src")
    # A sidecar serving other code than the source's now is stopped before the engine is told.
    assert prepared_code == [("scores-src", "h2")]
    assert registered == []  # DuckDB attaches at its next read; it has no catalog to create


async def test_the_schema_build_creates_trinos_catalog_of_each_dev_source(prepared_code):
    connector = TrinoCodeHostConnector("python")
    registered: list = []
    await attach.prepare(_state(connector, registered))
    assert connector.dev_ready("scores-src")
    assert registered == [("scores-src", "org_acme__scores_src")]


async def test_a_sidecar_stopped_for_good_drops_the_engines_catalog_of_it(prepared_code):
    connector = DuckDBCodeHostConnector("python")
    registered: list = []
    state = _state(connector, registered)
    await attach.prepare(state)
    await attach.prepare(state)  # a rebuild names its listener again: still one
    assert len(pgwire_sidecar._listeners) == 1
    pgwire_sidecar._stopped([("acme", "scores-src")])
    assert registered == [("dropped", "scores-src", "org_acme__scores_src")]
    # A deleted source's catalog was dropped with it: there is nothing left to drop.
    pgwire_sidecar._stopped([("acme", "deleted-src")])
    assert registered == [("dropped", "scores-src", "org_acme__scores_src")]


async def test_no_dev_source_on_an_engine_with_no_code_host_attach_is_prepared(monkeypatch):
    async def _never(*_a):
        raise AssertionError("prepared for an engine that cannot attach it")

    monkeypatch.setattr("provisa.codesource.source.prepare", _never)
    state = _state(None, [])
    state.federation_engine.engine.scan_connectors = {}
    await attach.prepare(state)
    assert pgwire_sidecar._listeners == {}
