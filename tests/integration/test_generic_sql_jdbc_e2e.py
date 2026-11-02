# Copyright (c) 2026 Kenneth Stott
# Canary: 581b2a26-ee68-45cf-8edd-b6d15a98fd36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a Generic SQL source with a jdbc: URL, a driver jar and its class (REQ-2191, amended
2026-11-01), on a self-hosted install: the real sqlite-jdbc driver opened through JPype in
Provisa's JVM -- no Calcite -- read and written in the source's SQL dialect, listed through the
driver's metadata, and checked by Detect, which offers no attach for it."""

# Requirements: REQ-2191

from __future__ import annotations

import sqlite3
import urllib.request
from pathlib import Path

import pytest

import provisa.core.settings_catalog  # noqa: F401 -- declares the settings read here
from provisa.core import secrets_providers
from provisa.executor.pool import SourcePool
from provisa.generic_sql import detect

# A JVM per process, started once: run in its own pytest process (scripts/test-all).
pytestmark = [pytest.mark.integration, pytest.mark.isolated, pytest.mark.asyncio]

_VERSION = "3.41.2.2"
_JAR_URL = (
    f"https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/{_VERSION}/sqlite-jdbc-{_VERSION}.jar"
)
_JAR = Path.home() / ".cache" / "provisa-test-jars" / f"sqlite-jdbc-{_VERSION}.jar"


@pytest.fixture(scope="module")
def jar() -> str:
    if not _JAR.exists():
        _JAR.parent.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(_JAR_URL, _JAR)  # noqa: S310 -- a fixed Maven Central URL
    return str(_JAR)


@pytest.fixture
def hints(jar, tmp_path, monkeypatch) -> dict[str, str]:
    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)
    monkeypatch.setattr("provisa.security.sandbox_egress.held", lambda _org: False)
    db = tmp_path / "shop.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, region TEXT)")
    con.execute("INSERT INTO orders VALUES (1, 'east'), (2, 'west'), (3, 'east')")
    con.commit()
    con.close()
    return {
        "url": f"jdbc:sqlite:{db}",
        "sql_dialect": "sqlite",
        "jdbc_driver_jar": jar,
        "jdbc_driver_class": "org.sqlite.JDBC",
    }


async def test_a_jdbc_url_reads_lists_and_writes_through_the_customers_driver(hints):
    pool = SourcePool()
    await pool.add("shop", "generic_sql", "", 0, "", "", "", extra=hints)
    try:
        assert pool.dialect_for("shop") == "sqlite"
        result = await pool.execute(
            "shop", "SELECT id FROM orders WHERE region = $1 ORDER BY id", ["east"]
        )
        assert result.rows == [(1,), (3,)]
        driver = pool.get("shop")
        assert await driver.table_names("main") == [("orders", None)]
        assert await driver.column_types("main", "orders") == [
            ("id", "INTEGER"),
            ("region", "TEXT"),
        ]
        stream = await driver.open_stream("SELECT id FROM orders ORDER BY id")
        assert [await stream.fetch(2), await stream.fetch(2), await stream.fetch(2)] == [
            [(1,), (2,)],
            [(3,)],
            [],
        ]
        assert (await pool.execute("shop", "INSERT INTO orders VALUES (4, 'north')")).rowcount == 1
    finally:
        await pool.close_all()


async def test_detect_runs_through_the_driver_and_offers_no_attach(hints):
    checks = {c.capability: c for c in await detect.detect(hints, "", "")}
    assert (
        not checks["attach"].passed and "jdbc URL has no engine attach" in checks["attach"].reason
    )
    for capability in ("predicate", "join", "aggregate", "key_range"):
        assert checks[capability].passed, checks[capability].reason
