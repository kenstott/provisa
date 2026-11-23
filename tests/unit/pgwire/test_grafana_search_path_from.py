# Copyright (c) 2026 Kenneth Stott
# Canary: 3b8e5d17-9a2c-4f60-8e41-c7d2a5b90f36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Grafana's catalog statements read its search path in FROM (REQ-2204, BI replay Grafana #4-#8):
``FROM generate_series(...) AS i, string_to_array(current_setting('search_path'), ',') s``.
string_to_array returns no set: in FROM it is one row of one column, named by its alias, as in
PostgreSQL. The catalog route writes it as that one-row subquery, so the statement reads no table
function and is not refused as one (V006). A table function that reads data stays refused."""

# Requirements: REQ-2204

from __future__ import annotations

from pathlib import Path
import duckdb
import pytest
import sqlglot
import yaml

from provisa.compiler.sql_types import CompilationContext
from provisa.compiler.sql_validator import _check_registered_relations
from provisa.compiler.stage2 import GovernanceContext
from provisa.pgwire.catalog_route import (
    CLIENT_TABLE_FUNCTIONS,
    CatalogRequest,
    addressed_statement,
    current_request,
)
from provisa.pgwire.catalog_types import result_types
from provisa.transpiler.metadata_facts import catalog_facts
from tests.unit.test_governed_sql_engine_internals import _govern, _Routed

pytestmark = pytest.mark.unit

_GRAFANA = Path(__file__).resolve().parents[3] / "scripts/conformance/bi_tools/grafana.yaml"
_SEARCH_PATH = '"$user", public'
_REQUEST = CatalogRequest(
    role_id="analyst",
    database="sales_db",
    facts=catalog_facts("analyst", {1: {"SELECT"}}),
    settings={"server_version": "17.0", "search_path": _SEARCH_PATH},
)
#: Grafana's search-path subquery over a registered table, as its #5-#8 filter information_schema.
_OVER_ORDERS = (
    "SELECT o.id FROM sales.orders o WHERE 'public' IN (SELECT CASE WHEN trim(s[i]) = '\"$user\"' "
    "THEN user ELSE trim(s[i]) END FROM generate_series("
    "array_lower(string_to_array(current_setting('search_path'),','),1), "
    "array_upper(string_to_array(current_setting('search_path'),','),1)) as i, "
    "string_to_array(current_setting('search_path'),',') s)"
)


def _statement(statement_id: int) -> str:
    statements = yaml.safe_load(_GRAFANA.read_text())["statements"]
    return next(s["sql"] for s in statements if s["id"] == statement_id)


def _v006(sql: str) -> list[str]:
    """The V006 refusals of ``sql`` on a catalog statement."""
    tree = sqlglot.parse_one(sql, read="postgres")
    found = _check_registered_relations(
        tree, GovernanceContext(), CompilationContext(), table_functions=CLIENT_TABLE_FUNCTIONS
    )
    return [v.message for v in found if v.code == "V006"]


async def _govern_catalog(monkeypatch, sql: str) -> None:
    token = current_request.set(_REQUEST)
    try:
        await _govern(monkeypatch, sql, ["*"])
    finally:
        current_request.reset(token)


@pytest.mark.parametrize("statement_id", [4, 5, 6, 7, 8])
def test_grafanas_search_path_reads_no_table_function_on_the_meta_domain(statement_id):
    addressed = addressed_statement(_statement(statement_id), _REQUEST)
    assert "string_to_array" in addressed.lower()
    assert _v006(addressed) == []


@pytest.mark.parametrize("statement_id", [4, 5, 6, 7, 8])
def test_grafanas_search_path_statement_is_described(statement_id):
    names = [name for name, _ in result_types(_statement(statement_id))]
    assert names == (["table"] if statement_id == 4 else ["column", "type"])


async def test_the_search_path_subquery_clears_governance_on_a_catalog_statement(monkeypatch):
    with pytest.raises(_Routed):
        await _govern_catalog(monkeypatch, addressed_statement(_OVER_ORDERS, _REQUEST))


def test_the_search_path_is_read_as_postgresql_reads_it():
    # PostgreSQL: one row per search-path entry, "$user" the session user.
    addressed = addressed_statement(
        "SELECT CASE WHEN trim(s[i]) = '\"$user\"' THEN 'analyst' ELSE trim(s[i]) END AS e "
        "FROM generate_series(array_lower(string_to_array(current_setting('search_path'),','),1), "
        "array_upper(string_to_array(current_setting('search_path'),','),1)) as i, "
        "string_to_array(current_setting('search_path'),',') s ORDER BY i",
        _REQUEST,
    )
    written = sqlglot.transpile(addressed, read="postgres", write="duckdb")[0]
    assert duckdb.connect().execute(written).fetchall() == [("analyst",), ("public",)]


@pytest.mark.parametrize(
    ("from_item", "column"),
    [
        ("string_to_array('a,b', ',') s", "s"),
        ("pg_catalog.string_to_array('a,b', ',') s", "s"),
        ("string_to_array('a,b', ',') t(c)", "c"),
        ("string_to_array('a,b', ',')", "string_to_array"),
    ],
)
def test_a_scalar_function_in_from_is_one_row_named_as_postgresql_names_it(from_item, column):
    addressed = addressed_statement(f"SELECT * FROM {from_item}", _REQUEST)
    written = sqlglot.transpile(addressed, read="postgres", write="duckdb")[0]
    answered = duckdb.connect().execute(written)
    assert [d[0] for d in answered.description] == [column]
    assert answered.fetchall() == [(["a", "b"],)]


@pytest.mark.parametrize(
    "from_item",
    [
        "read_csv('/etc/passwd') s",
        "duckdb_secrets() s",
        "evil.string_to_array('a', ',') s",
        "cat.pg_catalog.string_to_array('a', ',') s",
    ],
)
async def test_a_table_function_that_is_not_postgresqls_scalar_is_still_refused(
    monkeypatch, from_item
):
    addressed = addressed_statement(f"SELECT o.id FROM sales.orders o, {from_item}", _REQUEST)
    assert _v006(addressed)
    with pytest.raises(PermissionError, match="V006"):
        await _govern_catalog(monkeypatch, addressed)
