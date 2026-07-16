# Copyright (c) 2026 Kenneth Stott
# Canary: d4e5f6a7-b8c9-0123-defa-123456789013
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PG-type cast rewriting for the DuckDB-backed catalog intercept.

DataGrip / psql catalog probes carry PG-only casts (``::oid``, ``::xid``,
``::regclass``, ``::name`` and array forms). DuckDB has none of those types, so
``_rewrite_for_duckdb`` must rewrite each cast into something DuckDB accepts
*without* dropping the operand's value — a value-losing rewrite silently breaks
predicates like ``relnamespace = 2215::oid``.
"""

from __future__ import annotations

import duckdb
import pytest

from provisa.pgwire.catalog_rewrite import _rewrite_for_duckdb


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    yield c
    c.close()


def _scalar(con, sql: str):
    return con.execute(_rewrite_for_duckdb(sql)).fetchone()[0]


def test_oid_literal_value_preserved(con):
    # Regression: `2215::oid` previously collapsed to literal 0.
    assert _scalar(con, "SELECT 2215::oid AS x") == 2215


def test_oid_predicate_preserved(con):
    rewritten = _rewrite_for_duckdb("SELECT * FROM pg_class WHERE relnamespace = 2215::oid")
    assert "= 0" not in rewritten
    assert "2215" in rewritten


@pytest.mark.parametrize("t", ["oid", "xid", "tid", "cid"])
def test_system_id_casts_keep_value(con, t):
    assert _scalar(con, f"SELECT 42::{t} AS x") == 42


def test_nested_varchar_xid_cast_executes(con):
    # DataGrip emits age(0::varchar::xid); the nested casts must run in DuckDB.
    assert _scalar(con, "SELECT 0::varchar::xid AS x") == 0


def test_oid_array_cast_executes(con):
    assert _scalar(con, "SELECT '{16395}'::oid[] AS arr") == [16395]


@pytest.mark.parametrize(
    "regtype",
    [
        "regclass",
        "regtype",
        "regproc",
        "regprocedure",
        "regoper",
        "regoperator",
        "regconfig",
        "regdictionary",
        "regrole",
        "regnamespace",
    ],
)
def test_reg_casts_stripped_to_operand(con, regtype):
    # reg* casts drop to the bare operand (the numeric oid / identifier value).
    assert _scalar(con, f"SELECT n.oid::{regtype} AS rc FROM (SELECT 5 AS oid) n") == 5


def test_name_cast_becomes_varchar(con):
    assert _scalar(con, "SELECT 'public'::name AS n") == "public"


def test_qualified_regclass_literal_shortened(con):
    # pg_description.classoid stores the short relation name ('pg_class'), so
    # DataGrip's `classoid = 'pg_catalog.pg_class'::regclass` filter must map the
    # schema-qualified literal to its last component or every comment drops.
    out = _rewrite_for_duckdb(
        "SELECT description FROM pg_description WHERE classoid = 'pg_catalog.pg_class'::regclass"
    )
    assert "'pg_class'" in out
    assert "pg_catalog.pg_class" not in out


def test_unqualified_regclass_literal_unchanged(con):
    assert _scalar(con, "SELECT 'pg_class'::regclass AS c") == "pg_class"
