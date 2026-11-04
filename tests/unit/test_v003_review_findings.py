# Copyright (c) 2026 Kenneth Stott
# Canary: 74b3f0c5-5b21-44d4-8a33-e316669dbd83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A hidden column is refused in every clause; a source of unknown columns never ends the check
(REQ-039, amended 2026-11-04: A COLUMN IS JUDGED WHERE IT RESOLVES; A HIDDEN COLUMN IS REFUSED IN
EVERY CLAUSE; REQ-2204).

Every statement of the RV597 review (C1, C2, H1-H5, M1) is refused, through validate_sql as the
pgwire pipeline calls it, and so is every shape that review found refused correctly. A name is
passed to the engine only where a source no registry governs is known to provide it: VALUES its
column1..N, json_each its key and value, unnest the alias. A row rule governance adds may read a
column the role is not served: it is not the role's SQL.
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


# -- the review's findings: each refused ----------------------------------------------------------

_C1 = [
    "SELECT id, (SELECT ssn FROM (VALUES (1)) v) AS leak FROM bank.customer c",
    "SELECT id, ARRAY(SELECT ssn FROM (VALUES (1)) v) AS leak FROM bank.customer c",
    "SELECT id, (SELECT ssn FROM json_each('{}') j) AS leak FROM bank.customer c",
    "SELECT id, (SELECT ssn FROM jsonb_each('{}') j) FROM bank.customer c",
    "SELECT id, (SELECT ssn FROM (VALUES (1)) v UNION SELECT 'a') AS leak FROM bank.customer c",
    "SELECT (SELECT ssn FROM (SELECT * FROM (VALUES (1)) v) d) FROM bank.customer c",
]
_C2 = [
    "SELECT x FROM bank.customer c, LATERAL (SELECT ssn AS x FROM (VALUES (1)) v) l",
    "SELECT l.x FROM bank.customer c CROSS JOIN LATERAL (SELECT ssn AS x FROM (VALUES (1)) v) l",
]
_H1 = [
    "SELECT id, unregistered FROM bank.customer c, (VALUES (1)) v",
    "SELECT unregistered FROM bank.customer c, json_each('{}') j",
]
_H2 = [
    "SELECT x FROM bank.customer c, unnest(ARRAY[c.ssn]) x",
    "SELECT x FROM bank.customer c, unnest(ARRAY[ssn]) x",
    "SELECT x FROM bank.customer c, LATERAL unnest(ARRAY[c.ssn]) x",
]
_H4 = [
    "SELECT C.ssn FROM bank.customer c",
    "SELECT CUSTOMER.ssn FROM bank.customer",
]
_H5 = ["SELECT * FROM bank.customer NATURAL JOIN (VALUES ('x')) v(ssn)"]
_M1 = [
    "SELECT id FROM bank.customer WHERE ssn LIKE '1%'",
    "SELECT id FROM bank.customer ORDER BY ssn",
    "SELECT count(*) FROM bank.customer GROUP BY ssn",
    "SELECT id FROM bank.customer c WHERE EXISTS "
    "(SELECT 1 FROM (VALUES (1)) v WHERE c.ssn LIKE '1%')",
]
_H3 = [
    "SELECT 1 FROM evil.unnest(ARRAY[1]) x",
    "SELECT 1 FROM public.unnest(ARRAY[1]) x",
    "SELECT 1 FROM cat.evil.unnest(ARRAY[1]) x",
    "SELECT 1 FROM explode(ARRAY[1]) x",
    "SELECT 1 FROM pg_catalog.explode(ARRAY[1]) x",
    "SELECT 1 FROM pg_catalog.posexplode(ARRAY[1]) x",
]


@pytest.mark.parametrize("sql", _C1 + _C2 + _H1 + _H2 + _H4 + _H5 + _M1)
def test_a_review_finding_is_refused_as_a_hidden_column(sql):
    assert _codes(sql) == ["V003"], sql


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


# -- every clause --------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        # WHERE, JOIN ON, GROUP BY, HAVING, ORDER BY, windows
        "SELECT id FROM bank.customer WHERE ssn IS NOT NULL",
        "SELECT c.id FROM bank.customer c JOIN pg_catalog.pg_am a ON a.amname = c.ssn",
        "SELECT name FROM bank.customer GROUP BY name, ssn",
        "SELECT name FROM bank.customer GROUP BY name HAVING max(ssn) > '5'",
        "SELECT id FROM bank.customer ORDER BY length(ssn)",
        "SELECT name AS ssn FROM bank.customer GROUP BY ssn",
        "SELECT rank() OVER w FROM bank.customer WINDOW w AS (PARTITION BY ssn)",
        "SELECT count(*) FILTER (WHERE ssn > '5') FROM bank.customer",
        "SELECT DISTINCT ON (ssn) id FROM bank.customer",
        "SELECT string_agg(name, ',' ORDER BY ssn) FROM bank.customer",
        # USING and a NATURAL join's shared column, with or without a star
        "SELECT id FROM bank.customer JOIN (VALUES ('x')) v(ssn) USING (ssn)",
        "SELECT id FROM bank.customer NATURAL JOIN (VALUES ('x')) v(ssn)",
        "SELECT id FROM bank.customer NATURAL JOIN json_populate_record(NULL::bank.customer, '{}') r",
        # FROM-function arguments and LATERAL items
        "SELECT s FROM bank.customer c, generate_series(1, length(c.ssn)) s",
        "SELECT z.n FROM bank.customer c, LATERAL (SELECT 1 AS n WHERE c.ssn > '5') z",
        "SELECT k.key FROM bank.customer c, json_each(json_build_object('a', ssn)) k",
        # inside a subquery, a CTE, a set operation
        "SELECT id FROM bank.customer WHERE id IN (SELECT id FROM bank.customer WHERE ssn > '5')",
        "WITH d AS (SELECT id FROM bank.customer WHERE ssn > '5') SELECT id FROM d",
        "SELECT id FROM bank.customer UNION SELECT id FROM bank.customer ORDER BY 1 LIMIT "
        "(SELECT count(*) FROM bank.customer WHERE ssn > '5')",
        "SELECT id FROM bank.customer EXCEPT SELECT id FROM bank.customer WHERE ssn > '5'",
    ],
)
def test_a_hidden_column_is_refused_in_every_clause(sql):
    assert "V003" in _codes(sql), sql


@pytest.mark.parametrize(
    "sql",
    [
        # the served columns, in every clause
        "SELECT id FROM bank.customer WHERE name LIKE 'a%' ORDER BY name",
        "SELECT name, count(*) FROM bank.customer GROUP BY name HAVING max(id) > 1",
        "SELECT rank() OVER (PARTITION BY name ORDER BY id) FROM bank.customer",
        "SELECT c.id FROM bank.customer c JOIN pg_catalog.pg_am a ON a.amname = c.name",
        "SELECT * FROM bank.customer JOIN (VALUES (1)) v(id) USING (id)",
        # an output column named in ORDER BY or GROUP BY: the output, which the list judged
        "SELECT name AS n FROM bank.customer ORDER BY n",
        "SELECT name AS ssn FROM bank.customer ORDER BY ssn",
        "SELECT name AS n, count(*) FROM bank.customer GROUP BY n",
        "SELECT id FROM bank.customer UNION SELECT id FROM bank.customer ORDER BY id",
        # a name a source no registry governs is known to provide
        "SELECT column1 FROM bank.customer c, (VALUES (1)) v",
        "SELECT a, column2 FROM bank.customer c, (VALUES (1, 2)) v(a)",
        "SELECT key, value FROM bank.customer c, json_each('{}') j",
        "SELECT k, value FROM bank.customer c, jsonb_each('{}') AS j(k)",
        "SELECT x, ordinality FROM bank.customer c, unnest(ARRAY[c.id]) WITH ORDINALITY x",
        "SELECT a FROM bank.customer c, json_to_record('{}') AS r(a int)",
        "SELECT (SELECT column1 FROM (VALUES (c.id)) v) FROM bank.customer c",
    ],
)
def test_a_served_or_known_name_is_not_refused(sql):
    assert _codes(sql) == [], sql


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
