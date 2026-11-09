# Copyright (c) 2026 Kenneth Stott
# Canary: 6c1f0d0e-2f4b-4c86-9d1b-5a3e7f2c8b41
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A parameter's WHERE condition survives governance for a role served the table (REQ-1986).

A parameter column is an argument, not data: it carries no grant of its own (``visible_to: []``,
which names no role, REQ-2154), and a role served one of the table's data columns may pass it
(REQ-1614). Governance must leave the condition as written, or the argument is never extracted
and the read is refused for want of it.
"""

# Requirements: REQ-1986, REQ-1614, REQ-2154, REQ-2100, REQ-2031, REQ-301

from __future__ import annotations

import duckdb
import sqlglot

from provisa.compiler.context import build_context
from provisa.compiler.governed_relation import expose_read
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.nf_extractor import extract_nf_args
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import SchemaInput
from provisa.compiler.stage2 import apply_governance, build_governance_context
from provisa.pgwire._pipeline import context_parameters

ROLE = {"id": "analyst", "capabilities": ["query_development"], "domain_access": ["*"]}


def _table(data_grant: list[str]) -> dict:
    return {
        "id": 1,
        "source_id": "atlas",
        "domain_id": "films",
        "schema_name": "public",
        "table_name": "movies_matching",
        "columns": [
            {"column_name": "title", "visible_to": data_grant},
            {
                "column_name": "_nf_q",
                "visible_to": [],
                "native_filter_type": "query_arg",
                "native_filter_required": True,
            },
        ],
        "write_ops": [],
        "write_returns_rows": False,
        "write_refused_forms": [],
    }


def _governed(table: dict) -> tuple[str, set[str]]:
    si = SchemaInput(
        tables=[table],
        relationships=[],
        column_types={
            1: [
                ColumnMetadata(column_name="title", data_type="varchar", is_nullable=True),
                ColumnMetadata(column_name="_nf_q", data_type="varchar", is_nullable=True),
            ]
        },
        naming_rules=[],
        role=ROLE,
        domains=[{"id": "films", "description": "Films"}],
    )
    ctx = build_context(si)
    gov = build_governance_context("analyst", RLSContext(rules={}), {}, ctx, [table], ROLE)
    sql = "SELECT title FROM movies_matching WHERE _nf_q = 'dogs'"
    return apply_governance(sql, gov), context_parameters(ctx)


def test_a_parameter_with_no_grant_of_its_own_is_passed_by_a_role_served_the_table():
    governed, parameters = _governed(_table(["analyst"]))
    _sql, _params, args = extract_nf_args(governed, [], parameters)
    assert args == {"q": "dogs"}


def _read(columns: list[dict], sql: str) -> tuple[str, set[str]]:
    table = _table(["analyst"])
    table["columns"] = columns
    si = SchemaInput(
        tables=[table],
        relationships=[],
        column_types={
            1: [
                ColumnMetadata(column_name=c["column_name"], data_type="varchar", is_nullable=True)
                for c in columns
            ]
        },
        naming_rules=[],
        role=ROLE,
        domains=[{"id": "films", "description": "Films"}],
    )
    ctx = build_context(si)
    gov = build_governance_context("analyst", RLSContext(rules={}), {}, ctx, [table], ROLE)
    return apply_governance(sql, gov), context_parameters(ctx)


def _data(name: str) -> dict:
    return {"column_name": name, "visible_to": ["analyst"]}


def _parameter(name: str) -> dict:
    return {"column_name": name, "visible_to": [], "native_filter_type": "query_arg"}


def _binds(sql: str, **columns: list[str]) -> None:
    """``sql`` binds in DuckDB over tables of these columns (its catalog-less names), as an engine
    reading the statement does: a condition on a column the relation lost is a Binder Error."""
    connection = duckdb.connect()
    try:
        for table, names in columns.items():
            connection.execute(
                f"CREATE TABLE {table} ({', '.join(f'''"{n}" text''' for n in names)})"
            )
        connection.execute(sql)
    finally:
        connection.close()


def test_a_condition_that_stays_in_the_statement_reads_a_parameter_the_relation_exposes():
    """REQ-2100: a code table read DIRECT keeps its parameter conditions in the statement, applied
    over rows holding each ``_nf_`` column: the relation drops none of them (run 42527445052,
    Binder Error: Referenced column "_nf_model" not found in FROM clause)."""
    governed, _ = _read(
        [_data("status"), _data("body"), _parameter("_nf_model"), _parameter("_nf_prompt")],
        "SELECT status, body FROM movies_matching WHERE _nf_model = 'm' AND _nf_prompt = 'p'",
    )
    _binds(governed, movies_matching=["status", "body", "_nf_model", "_nf_prompt"])


def test_a_parameter_the_statement_does_not_read_stays_out_of_the_relation():
    governed, _ = _read(
        [_data("status"), _parameter("_nf_model"), _parameter("_nf_prompt")],
        "SELECT status FROM movies_matching WHERE _nf_model = 'm'",
    )
    assert "_nf_model" in governed
    assert "_nf_prompt" not in governed


def test_a_star_does_not_read_the_parameters():
    """REQ-301: a parameter is an argument, not a column of the rows a ``*`` returns."""
    governed, parameters = _read(
        [_data("status"), _parameter("_nf_model")], "SELECT * FROM movies_matching"
    )
    assert "_nf_model" not in governed
    assert parameters == {"_nf_model"}


def test_an_extracted_argument_leaves_the_relation_with_its_condition():
    governed, parameters = _read(
        [_data("status"), _parameter("_nf_model")],
        "SELECT status FROM movies_matching WHERE _nf_model = 'm'",
    )
    sql, _params, args = extract_nf_args(governed, [], parameters)
    assert args == {"model": "m"}
    assert "_nf_model" not in sql
    _binds(sql, movies_matching=["status"])


def test_a_landed_parameter_filters_the_landed_column_the_statement_does_not_select():
    """REQ-2031: a table kept for good filters its landed column of the parameter's name, which
    the read's outputs must keep (run 42527445052, Binder Error: Referenced column "name")."""
    governed, _ = _read(
        [_data("name"), _data("fifa_code"), _parameter("_nf_name")],
        "SELECT fifa_code FROM movies_matching WHERE _nf_name = 'West Germany'",
    )
    sql, _params, args = extract_nf_args(governed, [], set(), {"_nf_name": "name"})
    assert args == {}
    assert "\"name\" = 'West Germany'" in sql
    _binds(sql, movies_matching=["name", "fifa_code"])


def test_a_parameter_is_exposed_through_every_level_of_a_chain():
    tree = sqlglot.parse_one(
        'SELECT a FROM (SELECT "t"."a" AS "a" FROM (SELECT "t"."a" AS "a" FROM t AS "t") AS "t") '
        "AS t WHERE _nf_x = '1'"
    )
    relation = tree.args["from_"].this
    expose_read(tree, relation, "t", ["_nf_x"])
    _binds(tree.sql(), t=["a", "_nf_x"])
