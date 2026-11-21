# Copyright (c) 2026 Kenneth Stott
# Canary: 456454b8-d362-4acb-90bf-c2d739b055d9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a JDBC source with a postgresql URL (REQ-2191), read through the SQLAlchemy dialect
its scheme names, against the compose Postgres: written in the SQL dialect its admin chose, listed
by the dialect's inspector, and nothing checked when it connects."""

# Requirements: REQ-2191

from __future__ import annotations

import os

import pytest

from provisa.executor.pool import SourcePool
from provisa.jdbc_source.url import MissingDialect

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_HOST = os.environ.get("PG_HOST", "localhost")
_PORT = int(os.environ.get("PG_PORT", "5432"))
_DB = os.environ.get("PG_DATABASE", "provisa")
_USER = os.environ.get("PG_USER", "provisa")
_PASSWORD = os.environ.get("PG_PASSWORD", "provisa")
_SCHEMA = "jdbc_source_e2e"


def _hints(scheme: str) -> dict[str, str]:
    return {"url": f"{scheme}://{_HOST}:{_PORT}/{_DB}", "sql_dialect": "postgres"}


async def test_a_postgresql_url_reads_lists_and_writes_in_its_chosen_dialect() -> None:
    pool = SourcePool()
    await pool.add("gsql", "jdbc", "", 0, "", _USER, _PASSWORD, extra=_hints("postgresql+psycopg2"))
    try:
        assert pool.dialect_for("gsql") == "postgres"
        assert pool.vector_capability("gsql") is None  # nothing is checked on connect
        await pool.execute("gsql", f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await pool.execute("gsql", f"CREATE SCHEMA {_SCHEMA}")
        await pool.execute(
            "gsql", f"CREATE TABLE {_SCHEMA}.orders (id integer PRIMARY KEY, region text)"
        )
        await pool.execute(
            "gsql", f"INSERT INTO {_SCHEMA}.orders VALUES (1, 'east'), (2, 'west'), (3, 'east')"
        )
        result = await pool.execute(
            "gsql", f"SELECT id FROM {_SCHEMA}.orders WHERE region = $1 ORDER BY id", ["east"]
        )
        assert result.rows == [(1,), (3,)]
        driver = pool.get("gsql")
        assert _SCHEMA in await driver.schema_names()
        assert await driver.table_names(_SCHEMA) == [("orders", None)]
        assert await driver.column_types(_SCHEMA, "orders") == [
            ("id", "INTEGER"),
            ("region", "TEXT"),
        ]
    finally:
        await pool.execute("gsql", f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await pool.close_all()


async def test_a_scheme_whose_driver_is_not_shipped_fails_naming_the_package() -> None:
    pool = SourcePool()
    with pytest.raises(MissingDialect, match="'pg8000'"):
        await pool.add(
            "gsql8000",
            "jdbc",
            "",
            0,
            "",
            _USER,
            _PASSWORD,
            extra=_hints("postgresql+pg8000"),
        )
    assert not pool.has("gsql8000")
