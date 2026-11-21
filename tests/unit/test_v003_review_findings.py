# Copyright (c) 2026 Kenneth Stott
# Canary: 74b3f0c5-5b21-44d4-8a33-e316669dbd83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The RV597 review's table-function finding, and the shapes it found refused (REQ-039, amended
2026-11-04; REQ-2204).

Through validate_sql as the pgwire pipeline calls it: only PostgreSQL's own unnest (and the other
client table functions) is admitted, never another schema's function of that name or another
Explode (H3); every shape the review found refused correctly is still refused. A row rule
governance adds may read a column the role is not served: it is not the role's SQL.

The review's hidden-column findings (C1, C2, H1, H2, H4, H5, M1) are retired here; they run in
tests/unit/test_governance_by_construction.py _RV597 on feat/governance-by-construction. The
served-or-known names (A2) and the every-clause refusal moved to fix/v003-a2 in the 2026-11-05
split.
"""

# Requirements: REQ-039, REQ-2204

from __future__ import annotations

import pytest
import sqlglot

from provisa.compiler.sql_types import CompilationContext
from provisa.compiler.sql_validator import (
    READ,
    _check_registered_relations,
    client_table_function,
    validate_sql,
)
from provisa.compiler.stage2 import apply_governance
from provisa.compiler.view_expand import VIEW_MARK
from provisa.pgwire.catalog_route import CLIENT_TABLE_FUNCTIONS
from tests.unit.test_v003_column_scope import _CUSTOMER, _gov

pytestmark = pytest.mark.unit


def _codes(sql: str, gov=None) -> list[str]:
    """The codes validate_sql refuses ``sql`` for, on a catalog statement (the client table
    functions admitted) for a role of every domain, the relationship guard aside."""
    return sorted(
        {
            v.code
            for v in validate_sql(
                sql,
                CompilationContext(),
                gov or _gov(),
                {"domain_access": ["*"]},
                [],
                bypass_relationship_guard=True,
                table_functions=CLIENT_TABLE_FUNCTIONS,
            )
        }
    )


# -- the review's table-function finding (H3): refused as a relation ---------------------------

_H3 = [
    "SELECT 1 FROM evil.unnest(ARRAY[1]) x",
    "SELECT 1 FROM public.unnest(ARRAY[1]) x",
    "SELECT 1 FROM cat.evil.unnest(ARRAY[1]) x",
    "SELECT 1 FROM explode(ARRAY[1]) x",
    "SELECT 1 FROM pg_catalog.explode(ARRAY[1]) x",
    "SELECT 1 FROM pg_catalog.posexplode(ARRAY[1]) x",
]


@pytest.mark.parametrize("sql", _H3)
def test_a_function_that_is_not_postgresql_unnest_is_refused_as_a_relation(sql):
    assert _codes(sql) == ["V006"], sql
    tree = sqlglot.parse_one(sql, read=READ)
    violations = _check_registered_relations(
        tree, _gov(), CompilationContext(), table_functions=CLIENT_TABLE_FUNCTIONS
    )
    assert [v.code for v in violations] == ["V006"], sql


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT x FROM pg_catalog.unnest(ARRAY[1]) x",
        "SELECT x FROM unnest(ARRAY[1]) x",
        'SELECT x FROM "unnest"(ARRAY[1]) x',
        "SELECT key FROM pg_catalog.json_each('{}') j",
    ],
)
def test_postgresql_unnest_is_still_admitted(sql):
    assert _codes(sql) == []


def test_client_table_function_reads_the_schema_and_the_function():
    def table(sql: str) -> sqlglot.exp.Table:
        found = sqlglot.parse_one(sql, read=READ).find(sqlglot.exp.Table)
        assert found is not None
        return found

    fns = {"unnest", "json_each"}
    assert client_table_function(table("SELECT 1 FROM pg_catalog.unnest(a) x"), fns)
    assert client_table_function(table("SELECT 1 FROM json_each(a) x"), fns)
    assert not client_table_function(table("SELECT 1 FROM evil.json_each(a) x"), fns)
    assert not client_table_function(table("SELECT 1 FROM c.pg_catalog.unnest(a) x"), fns)
    assert not client_table_function(table("SELECT 1 FROM pg_catalog.posexplode(a) x"), fns)
    # PostgreSQL's own reader reads explode(a) as pg_catalog.unnest(a), an Explode; READ reads
    # it by its name
    assert not client_table_function(table("SELECT 1 FROM explode(a) x"), fns)
    assert not client_table_function(table("SELECT 1 FROM pg_catalog.explode(a) x"), fns)


# -- the shapes the review found refused correctly: still refused --------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT c FROM bank.customer c",
        "SELECT row_to_json(c) FROM bank.customer c",
        "SELECT to_jsonb(c.*) FROM bank.customer c",
        "SELECT ROW(c.*) FROM bank.customer c",
        "SELECT (c).ssn FROM bank.customer c",
        "SELECT c.SSN FROM bank.customer c",
        "SELECT SSN FROM bank.customer",
        'SELECT "SSN" FROM bank.customer',
        "SELECT customer.ssn FROM bank.customer",
        "SELECT bank.customer.ssn FROM bank.customer",
        "SELECT a FROM (SELECT ssn FROM bank.customer) AS d(a)",
        "WITH d(a) AS (SELECT ssn FROM bank.customer) SELECT a FROM d",
        "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3) "
        "SELECT (SELECT ssn FROM r) FROM bank.customer c",
        "SELECT ssn FROM bank.customer JOIN (VALUES (1)) v(id) USING (id)",
        "SELECT ssn FROM bank.customer NATURAL JOIN (VALUES (1)) v(id)",
        "SELECT rank() OVER (ORDER BY ssn) FROM bank.customer",
        "SELECT * FROM (VALUES ((SELECT ssn FROM bank.customer LIMIT 1))) v",
        "SELECT (SELECT ssn FROM unnest(ARRAY[1], ARRAY[2])) FROM bank.customer c",
        "SELECT (SELECT ssn FROM json_to_record('{}') AS r(a int)) FROM bank.customer c",
        "SELECT x FROM pg_catalog.unnest((SELECT array_agg(ssn) FROM bank.customer)) x",
    ],
)
def test_a_shape_refused_before_is_still_refused(sql):
    assert "V003" in _codes(sql), sql


# -- what governance adds is not the role's SQL --------------------------------------------------


def test_a_row_rule_on_a_hidden_column_is_governance_s_not_the_role_s():
    gov = _gov()
    gov.rls_rules = {_CUSTOMER: "ssn LIKE '9%'"}
    sql = "SELECT id, name FROM bank.customer WHERE name LIKE 'a%' ORDER BY name"
    assert _codes(sql, gov) == []
    governed = apply_governance(sql, gov)
    assert "\"ssn\" LIKE '9%'" in governed


def test_a_view_s_own_filter_is_its_administrator_s_but_its_list_is_read():
    marked = f'"{VIEW_MARK}bank.served"'
    over = "SELECT id FROM (SELECT * FROM ({body}) AS {mark}) AS v"
    filtered = over.format(body="SELECT id FROM bank.customer WHERE ssn LIKE '9%'", mark=marked)
    assert _codes(filtered) == []
    listed = over.format(body="SELECT id, ssn FROM bank.customer", mark=marked)
    assert _codes(listed) == ["V003"]
