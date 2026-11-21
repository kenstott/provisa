# Copyright (c) 2026 Kenneth Stott
# Canary: 0c7e5b2a-93d4-4f1e-8a6b-2d91f4c6e3a7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""record_eq and record_ne, and their image kin, from the polyfill grid (REQ-2236): a NULL field
equals a NULL field, as PostgreSQL's btree comparison of a record has it, where Trino's ``=`` of two
rows is NULL. Rows each found wrong by the function support measure on Trino (run 43264139919)."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import duckdb
import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.transpiler.function_macros import MACROS
from provisa.transpiler.polyfill import GRID, records
from provisa.transpiler.polyfill.grid import DIALECTS, NATIVE
from provisa.transpiler.transpile import transpile, transpile_to_duckdb

_TRINO = ("trino", "presto", "athena")


def _probe(function: str) -> str:
    """The statement the measure sent."""
    return (
        f"SELECT pg_catalog.\"{function}\"(ROW(NULL::integer, 'c'::text)::record, "
        "ROW(NULL::integer, 'c'::text)::record) AS v"
    )


def test_the_family_is_in_the_grid_and_not_in_the_old_table():
    names = {f.name for f in records.FEATURES}
    orders = {
        f"{stem}{o}" for stem in ("record_", "record_image_") for o in ("lt", "le", "gt", "ge")
    }
    assert names == {"record_eq", "record_ne", "record_image_eq", "record_image_ne", *orders}
    assert names <= {f.name for f in GRID.features}
    assert not {name for name, _ in MACROS} & names


@pytest.mark.parametrize("feature", records.FEATURES, ids=lambda f: f.name)
def test_every_dialect_but_postgresql_has_a_form(feature):
    """A dialect a rule gives no form is sent the call as written: none is left so."""
    (rule,) = feature.read.rules
    assert set(rule.forms) == set(DIALECTS) - NATIVE


@pytest.mark.parametrize(
    ("function", "written"),
    [
        (
            "record_eq",
            "SELECT CASE WHEN ROW(CAST(NULL AS INTEGER), CAST('c' AS VARCHAR)) IS NULL "
            "OR ROW(CAST(NULL AS INTEGER), CAST('c' AS VARCHAR)) IS NULL "
            "THEN CAST(NULL AS BOOLEAN) "
            "ELSE ROW(CAST(NULL AS INTEGER), CAST('c' AS VARCHAR)) IS NOT DISTINCT FROM "
            "ROW(CAST(NULL AS INTEGER), CAST('c' AS VARCHAR)) END AS v",
        ),
        (
            "record_ne",
            "SELECT CASE WHEN ROW(CAST(NULL AS INTEGER), CAST('c' AS VARCHAR)) IS NULL "
            "OR ROW(CAST(NULL AS INTEGER), CAST('c' AS VARCHAR)) IS NULL "
            "THEN CAST(NULL AS BOOLEAN) "
            "ELSE ROW(CAST(NULL AS INTEGER), CAST('c' AS VARCHAR)) IS DISTINCT FROM "
            "ROW(CAST(NULL AS INTEGER), CAST('c' AS VARCHAR)) END AS v",
        ),
        (
            "record_image_eq",
            "SELECT CASE WHEN ROW(CAST(NULL AS INTEGER), CAST('c' AS VARCHAR)) IS NULL "
            "OR ROW(CAST(NULL AS INTEGER), CAST('c' AS VARCHAR)) IS NULL "
            "THEN CAST(NULL AS BOOLEAN) "
            "ELSE ROW(CAST(NULL AS INTEGER), CAST('c' AS VARCHAR)) IS NOT DISTINCT FROM "
            "ROW(CAST(NULL AS INTEGER), CAST('c' AS VARCHAR)) END AS v",
        ),
        (
            "record_image_ne",
            "SELECT CASE WHEN ROW(CAST(NULL AS INTEGER), CAST('c' AS VARCHAR)) IS NULL "
            "OR ROW(CAST(NULL AS INTEGER), CAST('c' AS VARCHAR)) IS NULL "
            "THEN CAST(NULL AS BOOLEAN) "
            "ELSE ROW(CAST(NULL AS INTEGER), CAST('c' AS VARCHAR)) IS DISTINCT FROM "
            "ROW(CAST(NULL AS INTEGER), CAST('c' AS VARCHAR)) END AS v",
        ),
    ],
)
def test_a_null_field_is_equal_to_a_null_field_on_trino(function, written):
    assert transpile(_probe(function), "trino") == written


@pytest.mark.parametrize("dialect", _TRINO)
def test_presto_and_athena_take_trinos_form(dialect):
    tree = sqlglot.parse_one(transpile(_probe("record_eq"), dialect), read="trino")
    assert tree.find(exp.NullSafeEQ) is not None


@pytest.mark.parametrize(
    ("function", "answer"),
    [
        ("record_eq", True),
        ("record_ne", False),
        ("record_image_eq", True),
        ("record_image_ne", False),
    ],
)
def test_duckdb_answers_as_postgresql_and_as_before(function, answer):
    """DuckDB's own ``=`` of two structs takes a NULL field as equal to a NULL field: its form is
    the one it had."""
    sql = transpile_to_duckdb(_probe(function))
    assert duckdb.connect().execute(sql).fetchall() == [(answer,)]


def test_a_null_record_is_null():
    """record_eq is strict: a NULL argument answers NULL, not FALSE."""
    sql = transpile_to_duckdb(
        "SELECT pg_catalog.\"record_eq\"(NULL::record, ROW(1, 'c'::text)::record) AS v"
    )
    assert duckdb.connect().execute(sql).fetchall() == [(None,)]
