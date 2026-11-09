# Copyright (c) 2026 Kenneth Stott
# Canary: ee2562c9-ebf3-4317-8d96-cece3b5c992b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1999: MotherDuck as the engine -- the in-process DuckDB engine whose store is a MotherDuck
database, attached on the engine's own connection and written with the embedded store's DuckDB
statements; its URL's token is a credential the settings surfaces never return."""

# Requirements: REQ-1999

from __future__ import annotations

from types import SimpleNamespace

import duckdb
import pytest

from provisa.api.admin.secret_redaction import redact_url_password, restore_url_password
from provisa.federation import duckdb_runtime as runtime_mod
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime
from provisa.federation.materialize_broker import EngineAttachedStore

STORE = "motherduck:///shop?motherduck_token=tok-a"


# -- the engine kind ------------------------------------------------------------------------------


def test_the_motherduck_engine_is_the_duckdb_engine_with_its_store_at_the_engine_url(monkeypatch):
    from provisa.core.engine_kinds import ENGINE_KINDS, REGION_READERS
    from provisa.federation import engine as engine_mod

    monkeypatch.setattr(engine_mod, "configured_engine_url", lambda: STORE)
    engine = engine_mod.build_motherduck_engine()
    duck = engine_mod.build_duckdb_engine()
    assert engine.name == "motherduck"
    assert list(engine.connectors) == list(duck.connectors)
    assert engine.default_materialize_store() == STORE
    assert "motherduck" in ENGINE_KINDS and "motherduck" in REGION_READERS


def test_the_motherduck_engine_without_a_url_declares_no_store(monkeypatch):
    from provisa.federation import engine as engine_mod

    monkeypatch.setattr(engine_mod, "configured_engine_url", lambda: None)
    # not the embedded DuckDB file: that belongs to one node
    assert engine_mod.build_motherduck_engine().default_materialize_store() is None


def test_the_picker_asks_for_the_database_url_and_says_what_choosing_it_commits_to():
    from provisa.federation.engine import ENGINE_REGISTRY

    (entry,) = [e for e in ENGINE_REGISTRY if e["key"] == "motherduck"]
    (field,) = entry["config_fields"]
    assert (field["config_key"], field["required"]) == ("federation_engine_url", True)
    assert "motherduck_token" in field["placeholder"]
    # what choosing it commits the organization to, said where it is chosen
    for said in ("Every replica of every source", "one account", "moves its rows", "bills"):
        assert said in entry["description"]


# -- the store on the engine connection -----------------------------------------------------------


class _Recording:
    """The engine connection, recording each statement and its bound values."""

    def __init__(self) -> None:
        self.statements: list[tuple[str, list | None]] = []

    def execute(self, sql: str, params: list | None = None):
        self.statements.append((sql, params))
        return self

    def cursor(self):
        # The runtime runs each statement on a cursor of its own (DuckDB 2.0 refuses a busy
        # connection); a cursor's statements reach the same instance, recorded here alike.
        return self

    def close(self) -> None:
        pass


@pytest.fixture
def runtime(monkeypatch):
    loaded: list[str] = []
    monkeypatch.setattr(
        runtime_mod, "install_and_load", lambda _con, ext, **_kw: loaded.append(ext)
    )
    made = DuckDBFederationRuntime(materialize_dsn=STORE)
    setattr(made, "_con", _Recording())
    made.loaded = loaded  # type: ignore[attr-defined]
    return made


def test_the_store_is_attached_on_the_engine_connection_once_it_is_signed_in(runtime):
    assert runtime.ensure_materialize_attached() == "mat_store"
    assert runtime.loaded == ["motherduck"]
    assert runtime._con.statements == [  # noqa: SLF001
        ("SET GLOBAL motherduck_token = ?", ["tok-a"]),  # bound, in no statement's text
        ("ATTACH 'md:shop' AS mat_store", None),
    ]
    assert isinstance(runtime._store_broker, EngineAttachedStore)  # noqa: SLF001
    runtime.ensure_materialize_attached()
    assert len(runtime._con.statements) == 2  # noqa: SLF001


def test_a_motherduck_store_is_written_through_the_broker_and_read_in_place(runtime):
    runtime.ensure_materialize_attached()
    assert runtime._store_is_duckdb() and not runtime._store_is_file()  # noqa: SLF001
    sql = 'SELECT * FROM mat_store."replicas"."orders"'
    assert runtime._refresh_store_relations(sql) == sql  # noqa: SLF001


def test_a_source_of_another_account_is_refused_on_a_motherduck_store_engine(runtime):
    runtime.ensure_materialize_attached()
    other = SimpleNamespace(id="md2", password="tok-b")
    with pytest.raises(ValueError, match="'md2'.*another MotherDuck account"):
        runtime._sign_in_motherduck(other)  # noqa: SLF001
    runtime._sign_in_motherduck(SimpleNamespace(id="md1", password="tok-a"))  # noqa: SLF001


def test_a_token_reference_in_the_store_url_is_resolved_at_use(monkeypatch):
    monkeypatch.setattr(runtime_mod, "install_and_load", lambda *_a, **_kw: None)
    monkeypatch.setenv("IT_MD_TOKEN", "tok-from-env")
    made = DuckDBFederationRuntime(
        materialize_dsn="motherduck:///shop?motherduck_token=${env:IT_MD_TOKEN}"
    )
    setattr(made, "_con", _Recording())
    made.ensure_materialize_attached()
    assert made._con.statements[0] == (  # noqa: SLF001
        "SET GLOBAL motherduck_token = ?",
        ["tok-from-env"],
    )


@pytest.mark.parametrize(
    ("dsn", "missing"),
    [("motherduck:///shop", "motherduck_token"), ("motherduck:///?motherduck_token=t", "database")],
)
def test_a_store_url_missing_its_token_or_database_is_refused_by_name(monkeypatch, dsn, missing):
    monkeypatch.setattr(runtime_mod, "install_and_load", lambda *_a, **_kw: None)
    made = DuckDBFederationRuntime(materialize_dsn=dsn)
    setattr(made, "_con", _Recording())
    with pytest.raises(RuntimeError, match=f"names no {missing}"):
        made.ensure_materialize_attached()
    assert made._con.statements == []  # noqa: SLF001


def test_the_embedded_file_store_is_still_copied_per_statement():
    made = DuckDBFederationRuntime(materialize_dsn="duckdb:////tmp/never-opened.duckdb")
    assert made._store_is_duckdb() and made._store_is_file()  # noqa: SLF001


# -- the broker's statements on an attached store -------------------------------------------------


def test_the_attached_store_takes_the_embedded_stores_statements():
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS mat_store")
    store = EngineAttachedStore(con, lambda run: run())
    columns = [("id", "INTEGER"), ("name", "VARCHAR")]
    store.reconcile("replicas", "orders", columns)
    store.land("replicas", "orders", columns, [{"id": 1, "name": "a"}], "none", None)
    assert store.execute('SELECT id, name FROM mat_store."replicas"."orders"') == [(1, "a")]
    assert store.table_columns("replicas", "orders") == ["id", "name"]
    with pytest.raises(RuntimeError, match="read in place"):
        store.canary()
    with pytest.raises(RuntimeError, match="read in place"):
        store.fetch_arrow("replicas", "orders")
    con.close()


# -- the token in the settings surfaces -----------------------------------------------------------


def test_the_token_never_leaves_in_a_settings_response():
    assert redact_url_password(STORE) == "motherduck:///shop"
    assert redact_url_password("motherduck:///shop?attach_mode=single&motherduck_token=t") == (
        "motherduck:///shop?attach_mode=single"
    )


def test_a_url_posted_back_unchanged_keeps_its_token_and_a_new_database_does_not():
    assert restore_url_password("motherduck:///shop", STORE) == STORE
    assert restore_url_password("motherduck:///other", STORE) == "motherduck:///other"
    changed = "motherduck:///shop?motherduck_token=tok-new"
    assert restore_url_password(changed, STORE) == changed


# REQ-1999 (Amended 2026-11-06, UNAVAILABLE ON DUCKDB 2.0): refused by name when it is configured,
# on every way of configuring it, never at a first query.


def test_building_the_motherduck_engine_on_duckdb_2_is_refused_by_name():
    from provisa.federation import engine as engine_mod

    with pytest.raises(ValueError, match=r"MotherDuck engine .*DuckDB 2\.0\.0"):
        engine_mod.build_engine("motherduck")


async def test_choosing_the_motherduck_engine_on_duckdb_2_is_refused_and_nothing_is_stored(
    monkeypatch,
):
    from types import SimpleNamespace

    from provisa.api.admin import settings_router
    from provisa.api.errors import ApiError

    async def body():
        return {"engine": "motherduck"}

    def stored(*_a, **_k):
        raise AssertionError("the refused choice reached the config file")

    monkeypatch.setattr(settings_router, "require_deployment_settings", lambda _request: None)
    monkeypatch.setattr(settings_router, "read_config", stored)
    with pytest.raises(ApiError) as raised:
        await settings_router.set_federation_engine(SimpleNamespace(json=body))  # type: ignore[arg-type]
    assert raised.value.status_code == 400
    assert "MotherDuck engine" in str(raised.value.detail) and "2.0.0" in str(raised.value.detail)


def test_a_region_on_the_motherduck_engine_is_refused_on_duckdb_2():
    from types import SimpleNamespace

    from provisa.core.regions import require_engine_kind

    store = SimpleNamespace(id="md", kind="motherduck")
    with pytest.raises(ValueError, match=r"MotherDuck engine .*DuckDB 2\.0\.0"):
        require_engine_kind("eu", store)  # type: ignore[arg-type]
