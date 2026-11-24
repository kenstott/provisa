# Copyright (c) 2026 Kenneth Stott
# Canary: 3e8a1c57-9b24-4d0f-a6e3-5c71f2d94b08
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Grafana's catalog statements (REQ-2204): its query editor (postgresMetaQuery.ts) reads the
search path as ``string_to_array(current_setting('search_path'),',') s`` in FROM. PostgreSQL reads
any function in FROM as a one-row relation of its result; on a catalog statement string_to_array
is a client table function of its arguments alone, and on any other statement it is refused (V006)."""

# Requirements: REQ-2204

from __future__ import annotations

from pathlib import Path

import pytest
import sqlglot
import yaml
from sqlglot import exp

from provisa.compiler.sql_types import CompilationContext
from provisa.compiler.sql_validator import (
    _ROW_GENERATORS,
    READ,
    client_table_function,
    validate_sql,
)
from provisa.compiler.stage2 import GovernanceContext
from provisa.pgwire.catalog import _declared_types
from provisa.pgwire.catalog_route import (
    CLIENT_TABLE_FUNCTIONS,
    CatalogRequest,
    addressed_statement,
)
from provisa.pgwire.catalog_types import result_types
from provisa.pgwire.server import _described_oid
from provisa.transpiler.metadata_facts import catalog_facts

pytestmark = pytest.mark.unit

_GRAFANA = Path(__file__).resolve().parents[3] / "scripts/conformance/bi_tools/grafana.yaml"

#: Grafana's search-path subquery, as its editor sends it.
_SEARCH_PATH = (
    "SELECT CASE WHEN trim(s[i]) = '\"$user\"' THEN user ELSE trim(s[i]) END "
    "FROM generate_series("
    "array_lower(string_to_array(current_setting('search_path'),','),1), "
    "array_upper(string_to_array(current_setting('search_path'),','),1)) AS i, "
    "string_to_array(current_setting('search_path'),',') s"
)


def _statement(statement_id: int) -> str:
    stored = yaml.safe_load(_GRAFANA.read_text())["statements"]
    return next(s["sql"] for s in stored if s["id"] == statement_id)


def _violations(sql: str, table_functions: frozenset[str]) -> list[str]:
    found = validate_sql(
        sql,
        CompilationContext(),
        GovernanceContext(table_map={}),
        {"domain_access": ["*"]},
        [],
        bypass_relationship_guard=True,
        table_functions=table_functions,
    )
    return [f"{v.code}: {v.message}" for v in found]


@pytest.mark.parametrize("statement_id", [4, 5, 6, 7, 8])
def test_every_function_grafana_reads_in_from_is_a_client_table_function(statement_id):
    tree = sqlglot.parse_one(_statement(statement_id), read=READ)
    functions = [
        t
        for t in tree.find_all(exp.Table)
        if not isinstance(t.this, (exp.Identifier, *_ROW_GENERATORS))
    ]
    assert functions, "the statement reads string_to_array in FROM"
    refused = [
        t.sql("postgres") for t in functions if not client_table_function(t, CLIENT_TABLE_FUNCTIONS)
    ]
    assert refused == []


def test_grafanas_search_path_subquery_is_admitted_on_a_catalog_statement():
    assert _violations(_SEARCH_PATH, CLIENT_TABLE_FUNCTIONS) == []


def test_string_to_array_in_from_is_refused_on_any_other_statement():
    assert any(v.startswith("V006") for v in _violations(_SEARCH_PATH, frozenset()))


@pytest.mark.parametrize(
    "fn",
    [
        "evil.string_to_array('a', ',') s",
        "cat.evil.string_to_array('a', ',') s",
        "split_by_string('a', ',') s",
    ],
)
def test_a_function_that_is_not_postgresqls_string_to_array_is_refused(fn):
    assert any(
        v.startswith("V006") for v in _violations(f"SELECT 1 FROM {fn}", CLIENT_TABLE_FUNCTIONS)
    )


_REQUEST = CatalogRequest(
    role_id="reader",
    database="sales_db",
    facts=catalog_facts("reader", {1: {"SELECT"}}),
    settings={"server_version": "17.0", "search_path": '"$user", public'},
)


def test_grafanas_table_list_is_addressed_and_typed_as_postgresql_types_it():
    sql = _statement(4)
    assert addressed_statement(sql, _REQUEST)
    assert [name for name, _ in result_types(sql)] == ["table"]


@pytest.mark.parametrize("statement_id", [5, 6, 7, 8])
def test_grafanas_column_list_is_addressed_and_typed_as_postgresql_types_it(statement_id):
    sql = _statement(statement_id)
    assert addressed_statement(sql, _REQUEST)
    assert [name for name, _ in result_types(sql)] == ["column", "type"]


@pytest.mark.parametrize("statement_id", [5, 6, 7, 8])
def test_grafanas_column_type_is_described_as_varchar_as_postgresql_describes_it(statement_id):
    """information_schema.columns.data_type is the domain character_data; PostgreSQL describes a
    domain column with its base type (printtup.c getBaseTypeAndTypmod): varchar, oid 1043."""
    sql = _statement(statement_id)
    names = [name for name, _ in result_types(sql)]
    declared = _declared_types(sql, names, ["text"] * len(names))
    assert {n: _described_oid(t) for n, t in zip(names, declared)} == {"column": 25, "type": 1043}
