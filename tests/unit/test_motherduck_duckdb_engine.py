# Copyright (c) 2026 Kenneth Stott
# Canary: bcb0124b-75eb-4cf2-b651-c47e01b9a8cb
#
# This source code is licensed under the Business Source License 1.1
"""REQ-1998: a MotherDuck database attached in place by the DuckDB engine (md:<database>), its
token given to the connection as a bound value -- in no statement's text -- before the attach,
a source with no token refused before anything is attached, and one engine signed in to one
MotherDuck account."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from provisa.federation.connector_duckdb import DuckDBMotherDuckConnector
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

TOKEN = "md-token-that-must-not-leak"


def _source(**kw: Any):
    base = dict(
        id="md-share",
        type=SimpleNamespace(value="motherduck"),
        database="analytics",
        password=TOKEN,
        federation_hints={},
    )
    return SimpleNamespace(**{**base, **kw})


def test_on_duckdb_2_a_motherduck_source_needs_its_postgres_endpoint_reader():
    # REQ-1998 (Amended 2026-11-06, MOTHERDUCK IS A REPLICA WHERE IT CANNOT ATTACH; MOTHERDUCK
    # LANDS THROUGH ITS POSTGRES ENDPOINT): MotherDuck's extension does not load on DuckDB 2.0.
    from provisa.federation.engine import build_duckdb_engine

    engine = build_duckdb_engine()
    assert "motherduck" not in engine.connectors
    assert "MotherDuck's Postgres-endpoint reader" in (
        engine.unreachable_reason("motherduck") or ""
    )


def test_the_motherduck_connector_attaches_where_its_extension_loads():
    # What the engine attaches with on a DuckDB MotherDuck supports.
    connector = DuckDBMotherDuckConnector()
    assert connector.extension == "motherduck" and connector.install_from_community is False
    assert connector.capability().write  # writes through the engine, as on the pg engine


def test_the_attach_names_the_database_and_carries_no_token():
    details = DuckDBMotherDuckConnector().details(_source())
    assert details["attach"] == "ATTACH 'md:analytics' AS \"_src_md-share\""
    assert details["raw_alias"] == "_src_md-share" and details["signs_in"] == "motherduck"
    assert TOKEN not in repr(details)


def test_a_source_with_no_token_or_no_database_is_refused_by_name():
    with pytest.raises(ValueError, match="'md-share'.*token"):
        DuckDBMotherDuckConnector().details(_source(password=""))
    with pytest.raises(ValueError, match="name the MotherDuck database"):
        DuckDBMotherDuckConnector().details(_source(database=""))


class _Connection:
    def __init__(self) -> None:
        self.ran: list[tuple[str, list | None]] = []

    def execute(self, sql: str, params: list | None = None) -> None:
        self.ran.append((sql, params))


def _runtime() -> Any:
    runtime: Any = SimpleNamespace(_con=_Connection(), _motherduck_account=None)
    # The runtime's statements on the instance (each on a cursor of its own), recorded here.
    runtime._on_instance = lambda sql, params=None: runtime._con.execute(sql, params)
    runtime._sign_in = lambda src: DuckDBFederationRuntime._sign_in_motherduck(runtime, src)
    runtime._sign_in_motherduck_as = lambda token, who: (
        DuckDBFederationRuntime._sign_in_motherduck_as(runtime, token, who)
    )
    return runtime


def test_the_token_is_a_bound_value_and_never_in_a_statement():
    runtime = _runtime()
    runtime._sign_in(_source())
    assert runtime._con.ran == [("SET GLOBAL motherduck_token = ?", [TOKEN])]
    assert all(TOKEN not in sql for sql, _params in runtime._con.ran)


def test_one_engine_is_signed_in_to_one_motherduck_account():
    runtime = _runtime()
    runtime._sign_in(_source())
    runtime._sign_in(_source(id="md-other-db", database="other"))  # the same account
    assert len(runtime._con.ran) == 1
    with pytest.raises(ValueError, match="'md-third'.*another MotherDuck account"):
        runtime._sign_in(_source(id="md-third", password="a-different-token"))


def test_the_engine_signs_in_before_it_attaches():
    ran: list[tuple[str, list | None]] = []
    runtime: Any = SimpleNamespace(
        _sqlite_loaded=True,
        _pg_ext_loaded=True,
        _con=SimpleNamespace(execute=lambda sql, params=None: ran.append((sql, params))),
        _raw_attached=set(),
        _raw_attach_ddl={},
        _unreachable={},
        _attach_source={},
        _ext_loaded={"motherduck"},
        _engine=SimpleNamespace(
            connector_for=lambda _type: SimpleNamespace(extension="motherduck")
        ),
        _catalog_of=DuckDBFederationRuntime._catalog_of,
        _file_holders={},
        _motherduck_account=None,
    )
    runtime._on_instance = lambda sql, params=None: ran.append((sql, params))
    runtime._reach_location = lambda d: DuckDBFederationRuntime._reach_location(runtime, d)
    runtime._hold_file = lambda s, a: DuckDBFederationRuntime._hold_file(runtime, s, a)
    runtime._sign_in_motherduck = lambda s: DuckDBFederationRuntime._sign_in_motherduck(runtime, s)
    runtime._sign_in_motherduck_as = lambda token, who: (
        DuckDBFederationRuntime._sign_in_motherduck_as(runtime, token, who)
    )
    source = _source()
    source.catalog = "md_share"
    details = DuckDBMotherDuckConnector().details(source)
    DuckDBFederationRuntime._attach_raw(runtime, source, details)
    assert [sql for sql, _ in ran] == [
        "SET GLOBAL motherduck_token = ?",
        "ATTACH 'md:analytics' AS \"_src_md-share\"",
    ]
