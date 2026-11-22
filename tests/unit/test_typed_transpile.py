# Copyright (c) 2026 Kenneth Stott
# Canary: 2f7c4e9a-1b3d-4a6e-9c58-7d0e2b4f6a13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The typed transpile input (REQ-2236): an engine statement bound for Db2 or SAP HANA is
transpiled with the registered column types of the tables it reads, so an integer column is
known to be an integer there; without them, what needs its operands' types is refused."""

# Requirements: REQ-2236

from __future__ import annotations

from typing import Any

import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.compiler.introspect import ColumnMetadata
from provisa.federation import engine as engine_mod
from provisa.federation.engine import _RDB_KINDS, build_engine
from provisa.federation.replica_address import ReplicaRoute, ReplicaRoutes
from provisa.federation.runtime import EngineRuntime
from provisa.federation.typed_transpile import registered_column_types
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.transpile import transpile
from provisa.transpiler.typed_columns import typed

_COLUMNS = {"qty": "integer", "n": "bigint", "s": "smallint", "price": "numeric(10,2)"}
_TYPES = {
    "sales_pg": {"public": {"orders": _COLUMNS}},
    "": {"sales_pg_public": {"orders": _COLUMNS}},
}


def _column_types(sql: str, column_types: Any = None) -> dict[str, str]:
    tree = typed(sqlglot.parse_one(sql, read="postgres"), column_types or _TYPES)
    return {
        c.sql(dialect="postgres"): c.type.sql(dialect="postgres") if c.type else ""
        for c in tree.find_all(exp.Column)
    }


# -- each column reference typed from the registered tables -----------------------------------


def test_a_qualified_reference_is_typed_under_either_name_of_its_table() -> None:
    assert _column_types('SELECT "o"."qty" FROM "sales_pg"."public"."orders" AS "o"') == {
        '"o"."qty"': "INT"
    }
    assert _column_types('SELECT "o"."n" FROM "sales_pg_public"."orders" AS "o"') == {
        '"o"."n"': "BIGINT"
    }


def test_an_unqualified_reference_is_typed_by_its_one_source_and_written_as_it_came() -> None:
    sql = 'SELECT "qty", "price" FROM "sales_pg_public"."orders"'
    tree = typed(sqlglot.parse_one(sql, read="postgres"), _TYPES)
    assert tree.sql(dialect="postgres") == sql
    assert _column_types(sql) == {'"qty"': "INT", '"price"': "DECIMAL(10, 2)"}


def test_a_reference_through_a_derived_table_or_cte_is_typed() -> None:
    assert _column_types(
        'SELECT "q"."qty" FROM (SELECT "qty" FROM "sales_pg_public"."orders") AS "q"'
    ) == {'"q"."qty"': "INT", '"qty"': "INT"}
    assert _column_types(
        'WITH "w" AS (SELECT "s" FROM "sales_pg_public"."orders") SELECT "s" FROM "w"'
    ) == {'"s"': "SMALLINT"}


def test_an_ambiguous_or_unregistered_reference_is_left_untyped() -> None:
    assert _column_types(
        'SELECT "qty" FROM "sales_pg_public"."orders" AS "a" '
        'JOIN "sales_pg_public"."orders" AS "b" ON TRUE'
    ) == {'"qty"': "UNKNOWN"}
    assert _column_types('SELECT "t"."qty" FROM "other"."t"') == {'"t"."qty"': "UNKNOWN"}


def test_column_types_are_read_only_by_the_typed_dialects() -> None:
    with pytest.raises(ValueError, match="trino dialect reads no column types"):
        transpile('SELECT "qty" FROM "sales_pg_public"."orders"', "trino", column_types=_TYPES)


# -- the transpile with and without the types --------------------------------------------------


@pytest.mark.parametrize("dialect", ["saphana", "db2"])
def test_a_typed_integer_column_casts_to_an_integer_type(dialect: str) -> None:
    sql = 'SELECT CAST("o"."qty" AS BIGINT), CAST("s" AS INTEGER) FROM "sales_pg_public"."orders" AS "o"'
    assert transpile(sql, dialect, column_types=_TYPES) == (
        'SELECT CAST("o"."qty" AS BIGINT), CAST("s" AS INTEGER) FROM "sales_pg_public"."orders" AS "o"'
    )


@pytest.mark.parametrize("dialect", ["saphana", "db2"])
def test_untyped_an_integer_cast_of_a_column_is_still_refused(dialect: str) -> None:
    with pytest.raises(FunctionRefused, match="integer type"):
        transpile(
            'SELECT CAST("o"."qty" AS BIGINT) FROM "sales_pg_public"."orders" AS "o"', dialect
        )


@pytest.mark.parametrize("dialect", ["saphana", "db2"])
def test_a_numeric_column_cast_to_an_integer_type_is_refused_typed(dialect: str) -> None:
    with pytest.raises(FunctionRefused, match="integer type"):
        transpile(
            'SELECT CAST("price" AS INTEGER) FROM "sales_pg_public"."orders"',
            dialect,
            column_types=_TYPES,
        )


def test_hana_typed_integer_division_truncates() -> None:
    assert transpile(
        'SELECT "qty" / "s", "n" / "qty", "s" / "s" FROM "sales_pg_public"."orders"',
        "saphana",
        column_types=_TYPES,
    ) == (
        'SELECT CAST(ROUND("qty" / "s", 0, ROUND_DOWN) AS INTEGER), '
        'CAST(ROUND("n" / "qty", 0, ROUND_DOWN) AS BIGINT), '
        'CAST(ROUND("s" / "s", 0, ROUND_DOWN) AS SMALLINT) FROM "sales_pg_public"."orders"'
    )


def test_hana_untyped_or_numeric_division_is_still_refused() -> None:
    with pytest.raises(FunctionRefused, match="/ of operands not known to be integers"):
        transpile('SELECT "qty" / "s" FROM "sales_pg_public"."orders"', "saphana")
    with pytest.raises(FunctionRefused, match="/ of operands not known to be integers"):
        transpile(
            'SELECT "price" / "qty" FROM "sales_pg_public"."orders"',
            "saphana",
            column_types=_TYPES,
        )


def test_db2_typed_integer_division_is_written_as_it_is() -> None:
    # Db2's quotient of two integers truncates toward zero, as PostgreSQL's.
    assert transpile(
        'SELECT "qty" / "s" FROM "sales_pg_public"."orders"', "db2", column_types=_TYPES
    ) == ('SELECT "qty" / "s" FROM "sales_pg_public"."orders"')


# -- the registered column types, by the names the engine gives each table ---------------------


class _State:
    def __init__(self, routes: ReplicaRoutes | None = None) -> None:
        self.tables = [
            {"id": 1, "source_id": "sales", "schema_name": "public", "table_name": "orders"},
            {"id": 2, "source_id": "sales", "schema_name": "public", "table_name": "empty"},
        ]
        self.schema_build_cache = {
            "column_types": {
                1: [ColumnMetadata(name, kind, True) for name, kind in _COLUMNS.items()]
            }
        }
        self.kafka_table_physical: dict[tuple[str, str], str] = {}
        self.replica_routes = routes if routes is not None else ReplicaRoutes()

    def catalog_for(self, source_id: str) -> str:
        return {"sales": "sales_pg"}[source_id]


@pytest.fixture
def hana_engine(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setattr(engine_mod, "require_engine_extra", lambda kind: None)
    dsn = {key: dsn for key, _label, dsn in _RDB_KINDS}["saphana"]
    monkeypatch.setenv("PROVISA_ENGINE_URL", dsn)
    return build_engine("saphana")


def _types(nested: dict[str, Any]) -> dict[tuple[str, str, str], dict[str, str]]:
    return {
        (catalog, schema, table): {c: t.sql(dialect="postgres") for c, t in columns.items()}
        for catalog, schemas in nested.items()
        for schema, tables in schemas.items()
        for table, columns in tables.items()
    }


_WRITTEN = {"qty": "INT", "n": "BIGINT", "s": "SMALLINT", "price": "DECIMAL(10, 2)"}


def test_registered_types_are_keyed_by_the_three_part_and_the_folded_name(
    hana_engine: Any,
) -> None:
    assert _types(registered_column_types(_State(), hana_engine)) == {
        ("sales_pg", "public", "orders"): _WRITTEN,
        ("", "sales_pg_public", "orders"): _WRITTEN,
    }


def test_a_replica_served_table_is_typed_at_its_replica_address(hana_engine: Any) -> None:
    target = (None, "org_1_replicas", "orders")
    routes = ReplicaRoutes(
        engine_name=hana_engine.name,
        routes={(None, "sales_pg_public", "orders"): ReplicaRoute("sales", "orders", target)},
    )
    nested = registered_column_types(_State(routes), hana_engine)
    assert _types(nested)[("", "org_1_replicas", "orders")] == _WRITTEN
    assert transpile(
        'SELECT "r"."qty" / 2 FROM "org_1_replicas"."orders" AS "r"',
        "saphana",
        column_types=nested,
    ) == (
        'SELECT CAST(ROUND("r"."qty" / 2, 0, ROUND_DOWN) AS INTEGER) '
        'FROM "org_1_replicas"."orders" AS "r"'
    )


def test_a_name_two_tables_of_different_columns_share_is_left_out(hana_engine: Any) -> None:
    state = _State()
    state.tables.append(
        {"id": 3, "source_id": "sales", "schema_name": "public", "table_name": "orders"}
    )
    state.schema_build_cache["column_types"][3] = [ColumnMetadata("qty", "numeric", True)]
    assert registered_column_types(state, hana_engine) == {}


def test_the_engine_runtime_hands_hana_the_registered_types(hana_engine: Any) -> None:
    runtime = EngineRuntime(hana_engine, _State())
    assert runtime.transpile_physical(
        'SELECT "o"."qty" / "o"."s", CAST("o"."n" AS INTEGER) '
        'FROM "sales_pg_public"."orders" AS "o"'
    ) == (
        'SELECT CAST(ROUND("o"."qty" / "o"."s", 0, ROUND_DOWN) AS INTEGER), '
        'CAST("o"."n" AS INTEGER) FROM "sales_pg_public"."orders" AS "o"'
    )
