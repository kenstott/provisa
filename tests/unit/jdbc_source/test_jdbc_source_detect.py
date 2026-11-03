# Copyright (c) 2026 Kenneth Stott
# Canary: ae4f733e-a7ab-4cfa-b5e8-096fd9950b09
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Detect (REQ-2191): read-only checks against the real connection, written in the source's SQL
dialect the way real queries are, each passing or naming why it failed. Nothing is saved."""

# Requirements: REQ-2191

from __future__ import annotations

import sqlite3

import pytest

import provisa.core.settings_catalog  # noqa: F401 -- declares the settings read here
from provisa.core import org_files, secrets_providers
from provisa.jdbc_source import detect as gd

pytestmark = pytest.mark.unit


@pytest.fixture
def org(tmp_path, monkeypatch):
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path / "data"))
    folder = org_files.folder("acme")
    (folder / "files").mkdir(parents=True)
    monkeypatch.setattr("provisa.core.org_files.current_org_segment", lambda: "acme")
    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)
    sqlite3.connect(folder / "files" / "shop.db").close()
    return folder


def _hints(url: str, dialect: str) -> dict[str, str]:
    return {"url": url, "sql_dialect": dialect}


def _by_capability(checks: list[gd.Check]) -> dict[str, gd.Check]:
    return {c.capability: c for c in checks}


@pytest.fixture(scope="module")
def duckdb_engine():
    from provisa.federation.engine import build_duckdb_engine

    return build_duckdb_engine().complete_reach()


@pytest.fixture(scope="module")
def trino_engine():
    from provisa.federation.engine import build_trino_engine

    return build_trino_engine().complete_reach()


async def test_a_database_that_answers_every_check_passes_them(org, duckdb_engine):
    checks = _by_capability(
        await gd.detect(_hints("sqlite:///{files}/shop.db", "sqlite"), "", "", duckdb_engine)
    )
    assert list(checks) == ["attach", "predicate", "join", "aggregate", "key_range"]
    for capability in ("attach", "predicate", "join", "aggregate", "key_range"):
        assert checks[capability].passed, checks[capability].reason


async def test_the_attach_check_is_the_active_engines(org, trino_engine):
    # A sqlite URL names the built-in sqlite type, which Trino cannot attach: the check says so.
    checks = _by_capability(
        await gd.detect(_hints("sqlite:///{files}/shop.db", "sqlite"), "", "", trino_engine)
    )
    assert not checks["attach"].passed
    assert checks["attach"].reason == "the trino engine has no attach for a sqlite source"
    assert checks["predicate"].passed
    # A scheme naming no built-in type has none on any engine.
    assert not gd._attach("vertica+vertica_python://h/db", trino_engine, "").passed
    assert gd._attach("jdbc:postgresql://h/db", trino_engine, "").reason == (
        "a jdbc URL never attaches"
    )


async def test_a_check_written_in_a_dialect_the_database_does_not_speak_names_why(
    org, duckdb_engine
):
    # T-SQL counts with COUNT_BIG, a function SQLite does not have.
    checks = _by_capability(
        await gd.detect(_hints("sqlite:///{files}/shop.db", "tsql"), "", "", duckdb_engine)
    )
    assert not checks["aggregate"].passed
    assert "COUNT_BIG" in checks["aggregate"].reason
    assert checks["predicate"].passed


async def test_a_source_that_cannot_be_connected_to_fails_every_check_with_why(org, duckdb_engine):
    checks = await gd.detect(
        _hints("postgresql+psycopg2://127.0.0.1:1/none", "postgres"), "u", "p", duckdb_engine
    )
    assert checks and all(not c.passed and "OperationalError" in c.reason for c in checks)


def test_the_checks_are_written_in_the_sources_dialect():
    assert "COUNT_BIG(*)" in gd.statement("aggregate", "tsql")
    # Every SELECT names a table in Oracle: the rows a check makes come FROM DUAL.
    assert gd.statement("predicate", "oracle").count("FROM dual") == 2
    assert gd.statement("join", "ansi").startswith("SELECT a.x, b.y FROM (SELECT 1 AS x) AS a")


# -- the admin API of the form ---------------------------------------------------------------------


def test_the_form_is_offered_the_active_engine_the_dialects_and_the_capabilities():
    from provisa.api.admin.jdbc_source_admin import setup

    offered = setup("duckdb")
    assert offered.engine == "duckdb"
    assert "ansi" in offered.sql_dialects and "postgres" in offered.sql_dialects
    assert offered.capabilities == ["attach", "predicate", "join", "aggregate", "key_range"]


async def test_detect_from_the_form_checks_the_connection_it_describes(
    org, monkeypatch, duckdb_engine
):
    from contextlib import asynccontextmanager

    from provisa.api.admin.jdbc_source_admin import JdbcSourceDetectInput, detect

    @asynccontextmanager
    async def _bound():
        yield

    monkeypatch.setattr("provisa.core.secrets_store.bound_to_request_org", _bound)
    found = await detect(
        JdbcSourceDetectInput(url="sqlite:///{files}/shop.db", sql_dialect="sqlite"),
        duckdb_engine,
    )
    assert found.engine == "duckdb"
    assert {c.capability: c.passed for c in found.checks} == {
        "attach": True,
        "predicate": True,
        "join": True,
        "aggregate": True,
        "key_range": True,
    }
    # Its settings are refused by name as a save of them would be, before anything is connected.
    from provisa.jdbc_source.url import FileUrlRefused

    with pytest.raises(FileUrlRefused):
        await detect(
            JdbcSourceDetectInput(url="sqlite:///{files}/../../x.db", sql_dialect="sqlite"),
            duckdb_engine,
        )


def test_every_locale_says_each_refusal_of_a_jdbc_source():
    import json
    from pathlib import Path

    locales = Path(__file__).resolve().parents[3] / "provisa-ui" / "src" / "i18n" / "locales"
    catalogs = sorted(locales.glob("*/serverErrors.json"))
    assert len(catalogs) == 13
    for catalog in catalogs:
        said = json.loads(catalog.read_text())["serverErrors"]["schema"]
        for code in (
            "jdbc_invalid",
            "jdbc_file_refused",
            "jdbc_options_refused",
            "jdbc_driver_refused",
        ):
            assert "{{source}}" in said[code], (catalog, code)
