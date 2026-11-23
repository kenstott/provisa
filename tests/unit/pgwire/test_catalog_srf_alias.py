# Copyright (c) 2026 Kenneth Stott
# Canary: 7d3f0b8e-2c4a-4e61-9a57-1f6b0d2e8c93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A one-column set-returning function in FROM aliased without column names (REQ-2204, psql's \\d:
``array(select 'toast.' || x from pg_catalog.unnest(tc.reloptions) x)``): its alias names its one
column, as in PostgreSQL, both where the statement's result types are read and where it is written
for the engine."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import sqlglot
import yaml
from sqlglot import exp

from provisa.pgwire.catalog_rewrite import _rewrite_for_duckdb
from provisa.pgwire.catalog_route import CatalogRequest, addressed_statement
from provisa.pgwire.catalog_types import result_types
from provisa.transpiler.metadata_facts import catalog_facts

pytestmark = pytest.mark.unit

_PSQL = Path(__file__).resolve().parents[3] / "scripts/conformance/bi_tools/psql.yaml"


_REQUEST = CatalogRequest(
    role_id="reader",
    database="sales_db",
    facts=catalog_facts("reader", {1: {"SELECT"}}),
    settings={"server_version": "17.0"},
)


def _unnested(sql: str, dialect: str) -> list[tuple[str, str, list[str]]]:
    """Each unnest in FROM of ``sql``: its schema, alias and alias column names. sqlglot reads an
    unqualified unnest as an Unnest, a schema-qualified one as a table of an Explode."""
    tree = sqlglot.parse_one(sql, read=dialect)
    found = [("", u.alias, u.alias_column_names) for u in tree.find_all(exp.Unnest)]
    return found + [
        (t.db, t.alias, t.alias_column_names)
        for t in tree.find_all(exp.Table)
        if isinstance(t.this, exp.Explode)
    ]


def _replayed(statement_id: int) -> str:
    """psql's replayed statement ``statement_id``, its oid placeholder as a number."""
    statements = yaml.safe_load(_PSQL.read_text())["statements"]
    sql = next(s["sql"] for s in statements if s["id"] == statement_id)
    return re.sub(r"\{\{oid:[^}]*\}\}", "16390", sql)


@pytest.mark.parametrize("statement_id", [10, 18, 30])
def test_psqls_toast_options_have_their_declared_types(statement_id):
    declared = dict(result_types(_replayed(statement_id)))
    assert declared["array_to_string"] == "text"
    assert declared["relkind"] == '"char"'
    assert declared["amname"] == "name"


@pytest.mark.parametrize("statement_id", [10, 18, 30])
def test_psqls_toast_options_unnest_names_its_column_for_the_engine(statement_id):
    written = _rewrite_for_duckdb(_replayed(statement_id), "admin", "provisa")
    assert _unnested(written, "duckdb") == [("", "x", ["x"])]


@pytest.mark.parametrize("statement_id", [10, 18, 30])
def test_psqls_toast_options_unnest_names_its_column_on_the_meta_domain(statement_id):
    # The statement every engine reads: unqualified unnest(...) AS x(x) (Trino needs the column).
    addressed = addressed_statement(_replayed(statement_id), _REQUEST)
    assert _unnested(addressed, "postgres") == [("", "x", ["x"])]


def test_an_aliased_unnest_names_its_column_by_the_alias():
    named = result_types("SELECT x FROM pg_catalog.unnest(ARRAY['a=b']::text[]) x")
    assert [n for n, _ in named] == ["x"]


def test_an_aliased_generate_series_names_its_column_by_the_alias():
    assert [n for n, _ in result_types("SELECT g FROM pg_catalog.generate_series(1, 3) g")] == ["g"]


def test_column_names_the_statement_gives_are_kept():
    assert [n for n, _ in result_types("SELECT y FROM pg_catalog.unnest(ARRAY[1]) x(y)")] == ["y"]
