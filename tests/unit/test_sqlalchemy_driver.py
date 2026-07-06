# Copyright (c) 2026 Kenneth Stott
# Canary: 4a9c2e73-6b58-4d75-9e12-3c7a0d4f9e44
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Generic SQLAlchemy fallback direct driver — write path breadth (REQ-229, REQ-550).

Exercised end-to-end against SQLite (a real engine, no extra dependency, and a source type with
no native async driver today) to prove the fallback does genuine writes and reads through the
same DirectDriver contract the native drivers implement.
"""

from __future__ import annotations

import pytest

from provisa.executor.drivers.sqlalchemy_driver import SQLAlchemyDriver, _to_named_params


# ---- placeholder conversion -------------------------------------------------


def test_positional_placeholders_convert_without_collision():
    # $10 must not be mangled by the replacement of $1.
    sql, bind = _to_named_params(
        "INSERT INTO t VALUES ($1, $2, $10)", [f"v{i}" for i in range(1, 11)]
    )
    assert "$1" not in sql and "$10" not in sql
    assert bind["p1"] == "v1" and bind["p10"] == "v10"


def test_no_params_leaves_sql_untouched():
    sql, bind = _to_named_params("SELECT 1", None)
    assert sql == "SELECT 1"
    assert bind == {}


# ---- lifecycle --------------------------------------------------------------


@pytest.mark.asyncio
async def test_is_connected_lifecycle(tmp_path):
    drv = SQLAlchemyDriver("sqlite")
    assert drv.is_connected is False
    await drv.connect("", 0, str(tmp_path / "db.sqlite"), "", "")
    assert drv.is_connected is True
    await drv.close()
    assert drv.is_connected is False


# ---- real write + read roundtrip -------------------------------------------


@pytest.mark.asyncio
async def test_write_then_read_roundtrip(tmp_path):
    drv = SQLAlchemyDriver("sqlite")
    await drv.connect("", 0, str(tmp_path / "db.sqlite"), "", "")
    try:
        await drv.execute_ddl("CREATE TABLE person (id INTEGER, name TEXT)")
        # WRITE — the whole point: a mutation reaches the store and commits.
        await drv.execute("INSERT INTO person (id, name) VALUES ($1, $2)", [1, "ada"])
        await drv.execute("INSERT INTO person (id, name) VALUES ($1, $2)", [2, "grace"])
        # READ — returns QueryResult(rows as tuples, column_names) like the native drivers.
        result = await drv.execute("SELECT id, name FROM person ORDER BY id")
        assert result.column_names == ["id", "name"]
        assert result.rows == [(1, "ada"), (2, "grace")]
    finally:
        await drv.close()


@pytest.mark.asyncio
async def test_write_commits_across_connections(tmp_path):
    # A write must be durable (committed), not rolled back when the connection returns to the pool.
    db = str(tmp_path / "db.sqlite")
    w = SQLAlchemyDriver("sqlite")
    await w.connect("", 0, db, "", "")
    await w.execute_ddl("CREATE TABLE t (n INTEGER)")
    await w.execute("INSERT INTO t (n) VALUES ($1)", [42])
    await w.close()

    r = SQLAlchemyDriver("sqlite")
    await r.connect("", 0, db, "", "")
    try:
        result = await r.execute("SELECT n FROM t")
        assert result.rows == [(42,)]
    finally:
        await r.close()
