# Copyright (c) 2026 Kenneth Stott
# Canary: fefb86cb-0582-452e-8ca3-a9b1d193afa4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The floor of a JDBC source (REQ-2191): with every capability off, a table reads the way
an API table reads -- its column_value parameters (amended 2026-11-02) given by equality, a
required one left out refused by name before anything is sent, one SELECT in the source's SQL
dialect, the rows landed per argument set, and a row limit met by reading the cursor in batches
and stopping."""

# Requirements: REQ-2191, REQ-1986

from __future__ import annotations

import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import provisa.core.settings_catalog  # noqa: F401 -- declares the settings read here
from provisa.core import org_files, secrets_providers
from provisa.core.parameters import PARAMETER_KINDS
from provisa.federation.replica_address import ReplicaRoutes
from provisa.jdbc_source import table as floor

pytestmark = pytest.mark.unit


def _table(*params: tuple[str, bool]) -> dict:
    return {
        "id": 7,
        "source_id": "shop",
        "schema_name": "main",
        "table_name": "orders",
        "columns": [
            {"column_name": "id", "data_type": "integer", "native_filter_type": None},
            {"column_name": "region", "data_type": "text", "native_filter_type": None},
            *(
                {
                    "column_name": f"_nf_{name}",
                    "data_type": "integer" if name == "id" else "text",
                    "path": name,
                    "native_filter_type": floor.COLUMN_VALUE,
                    "native_filter_required": False,
                    "required_by_steward": required,
                }
                for name, required in params
            ),
        ],
    }


def test_column_value_is_a_parameter_kind_and_partition_key_keeps_its_meaning():
    assert {"column_value", "partition_key"} <= PARAMETER_KINDS


def test_the_read_is_a_column_list_with_an_equality_per_parameter_given():
    table = _table(("region", False), ("id", False))
    assert floor.statement(table, {"region": "east"}, "postgres") == (
        'SELECT "id", "region" FROM "main"."orders" WHERE "region" = $1',
        ["east"],
    )
    # Written in the source's SQL dialect, its values bound, never in its text.
    assert floor.statement(table, {"region": "east", "id": "2"}, "tsql") == (
        "SELECT [id], [region] FROM [main].[orders] WHERE [region] = $1 AND [id] = $2",
        ["east", 2],
    )
    assert floor.statement(table, None, "mysql") == (
        "SELECT `id`, `region` FROM `main`.`orders`",
        [],
    )


def test_the_landed_rows_are_kept_under_the_arguments_that_read_them():
    table = _table(("region", False), ("id", False))
    assert floor.cache_arguments(table, {"region": "east"}) == {"region": "east"}
    assert floor.cache_arguments(table, {}) == {}


@pytest.mark.parametrize(
    ("sql", "limit"),
    [
        ("SELECT id FROM orders LIMIT 5", 5),
        ("SELECT id FROM orders ORDER BY id LIMIT 5", None),
        ("SELECT id FROM orders WHERE id > 1 LIMIT 5", None),
        ("SELECT count(*) FROM orders LIMIT 5", None),
        ("SELECT o.id FROM orders o JOIN customers c ON c.id = o.id LIMIT 5", None),
        ("SELECT id FROM orders", None),
    ],
)
def test_a_row_limit_is_the_reads_only_when_the_statement_alone_sets_it(sql, limit):
    assert floor.row_limit(sql, "orders") == limit


def test_a_table_its_replica_serves_is_read_from_the_replica():
    types = {"shop": "jdbc"}
    assert floor.is_floor_table(_table(), types, SimpleNamespace(replica_routes=ReplicaRoutes()))
    served = ReplicaRoutes(serving=frozenset({("shop", "main", "orders")}))
    assert not floor.is_floor_table(_table(), types, SimpleNamespace(replica_routes=served))
    assert not floor.is_floor_table(_table(), {"shop": "postgresql"}, SimpleNamespace())


def test_register_table_offers_each_column_as_an_optional_column_value_parameter():
    assert floor.parameters_of([{"name": "region", "data_type": "text"}]) == [
        {
            "name": "_nf_region",
            "data_type": "text",
            "path": "region",
            "native_filter_type": "column_value",
            "native_filter_required": False,
        }
    ]


def _state(table: dict, **kw) -> SimpleNamespace:
    return SimpleNamespace(
        hot_manager=None,
        api_endpoints={},
        graphql_remote_sources={},
        source_types={"shop": "jdbc"},
        source_dialects={"shop": "sqlite"},
        replica_routes=ReplicaRoutes(),
        tables=[table],
        source_cache={},
        response_cache_default_ttl=300,
        **kw,
    )


async def test_a_required_column_left_out_is_refused_naming_the_table_and_column():
    from provisa.api.data.materialization import _materialize_api_to_engine_cache

    state = _state(_table(("region", True)))
    with patch("provisa.jdbc_source.table.read", AsyncMock()) as ran:
        with pytest.raises(ValueError, match=r"'orders' requires the parameter\(s\) \['region'\]"):
            await _materialize_api_to_engine_cache(
                "SELECT * FROM orders", state, nf_args={}, table_ids=[7]
            )
    ran.assert_not_awaited()  # nothing is sent to the source


@pytest.fixture
def shop(tmp_path, monkeypatch):
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path / "data"))
    folder = org_files.folder("acme")
    (folder / "files").mkdir(parents=True)
    monkeypatch.setattr("provisa.core.org_files.current_org_segment", lambda: "acme")
    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)
    db = sqlite3.connect(folder / "files" / "shop.db")
    db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, region TEXT)")
    db.executemany(
        "INSERT INTO orders VALUES (?, ?)",
        [(i, "east" if i % 2 else "west") for i in range(1, 2501)],
    )
    db.commit()
    db.close()


async def test_a_supplied_column_reads_one_select_and_its_rows_land_for_the_statement(shop):
    from provisa.api.data.materialization import _materialize_api_to_engine_cache
    from provisa.executor.pool import SourcePool

    pool = SourcePool()
    hints = {"url": "sqlite:///{files}/shop.db", "sql_dialect": "sqlite"}
    await pool.add("shop", "jdbc", "", 0, "", "", "", extra=hints)
    sent: list[str] = []
    driver = pool.get("shop")
    opened = driver.open_stream

    async def recording(sql, params=None):
        sent.append(sql)
        return await opened(sql, params)

    driver.open_stream = recording  # type: ignore[method-assign]
    try:
        engine = MagicMock()
        engine.land_source_table = AsyncMock()
        state = _state(_table(("id", True)), source_pools=pool, federation_engine=engine)
        rewrites, inline, _ = await _materialize_api_to_engine_cache(
            "SELECT * FROM orders", state, nf_args={"id": "4"}, table_ids=[7]
        )
        assert rewrites == {}
        assert inline["orders"].rows == [{"id": 4, "region": "west"}]
        assert sent == ['SELECT "id", "region" FROM "main"."orders" WHERE "id" = $1']
        engine.land_source_table.assert_not_awaited()  # no TTL of its own: read for the statement
    finally:
        await pool.close_all()


async def test_a_row_limit_is_met_by_reading_the_cursor_in_batches_and_stopping(shop):
    from provisa.executor.pool import SourcePool

    pool = SourcePool()
    hints = {"url": "sqlite:///{files}/shop.db", "sql_dialect": "sqlite"}
    await pool.add("shop", "jdbc", "", 0, "", "", "", extra=hints)
    fetched: list[int] = []
    driver = pool.get("shop")
    opened = driver.open_stream

    async def counting(sql, params=None):
        stream = await opened(sql, params)
        fetch = stream.fetch

        async def each(size):
            batch = await fetch(size)
            fetched.append(len(batch))
            return batch

        stream.fetch = each  # type: ignore[method-assign]
        return stream

    driver.open_stream = counting  # type: ignore[method-assign]
    try:
        state = _state(_table(), source_pools=pool)
        read = await floor.read(state, _table(), None, "SELECT id FROM orders LIMIT 3")
        assert [r["id"] for r in read.rows] == [1, 2, 3]
        assert fetched == [1000]  # one batch of 2500 rows read, then the cursor closed
    finally:
        await pool.close_all()


def test_a_read_of_a_jdbc_source_is_the_engines_never_sent_whole_to_it():
    from provisa.transpiler.router import Route, decide_route

    decision = decide_route(
        {"shop"},
        {"shop": "jdbc"},
        {"shop": "sqlite"},
        operator_floor={},
        statement="SELECT 1",
    )
    assert decision.route is Route.ENGINE
    assert "jdbc" in decision.reason
