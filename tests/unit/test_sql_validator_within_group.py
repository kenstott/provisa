# Copyright (c) 2026 Kenneth Stott
# Canary: 7b2e9c14-5d3a-4e8f-a061-3c9f2d7b8e45
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A schema-qualified ordered-set aggregate is a function call, not a column (REQ-039, REQ-2167).

sqlglot reads ``pg_catalog."cume_dist"(x) WITHIN GROUP (ORDER BY y)`` -- and the same call with
FILTER -- as a column named by the call and qualified by the schema. Governance judges it as the
call it is: ``pg_catalog`` is the function's schema, not a relation, and the columns inside its
arguments and its ORDER BY stay governed (a hidden one is still refused).
"""

# Requirements: REQ-039, REQ-2167

from __future__ import annotations

import pytest

from provisa.compiler.sql_gen import CompilationContext
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.stage2 import GovernanceContext

_ROLE = {"id": "teller", "capabilities": [], "domain_access": ["*"]}
_CUSTOMER = [("id", "integer"), ("name", "varchar"), ("ssn", "varchar"), ("score", "double")]


def _gov() -> GovernanceContext:
    return GovernanceContext(
        rls_rules={},
        masking_rules={},
        visible_columns={1: frozenset({"id", "name", "score"})},
        table_map={"bank.customer": 1},
        all_columns={1: list(_CUSTOMER)},
        limit_ceiling=None,
        sample_size=None,
    )


def _violations(sql: str) -> list[str]:
    return [
        f"{v.code} {v.message}"
        for v in validate_sql(
            sql, CompilationContext(), _gov(), _ROLE, [], bypass_relationship_guard=True
        )
    ]


# The ten overloads the PostgreSQL function conformance run calls (run 42527458223).
_ORDERED_SET = [
    'SELECT pg_catalog."rank"(NULL::integer) WITHIN GROUP (ORDER BY NULL::integer) AS v',
    'SELECT pg_catalog."dense_rank"(NULL::integer) WITHIN GROUP (ORDER BY NULL::integer) AS v',
    'SELECT pg_catalog."percent_rank"(NULL::integer) WITHIN GROUP (ORDER BY NULL::integer) AS v',
    'SELECT pg_catalog."cume_dist"(NULL::integer) WITHIN GROUP (ORDER BY NULL::integer) AS v',
    'SELECT pg_catalog."mode"() WITHIN GROUP (ORDER BY NULL::integer) AS v',
    'SELECT pg_catalog."percentile_cont"(NULL::double precision) '
    "WITHIN GROUP (ORDER BY NULL::double precision) AS v",
    'SELECT pg_catalog."percentile_cont"(NULL::double precision[]) '
    "WITHIN GROUP (ORDER BY NULL::double precision) AS v",
    'SELECT pg_catalog."percentile_cont"(NULL::double precision) '
    "WITHIN GROUP (ORDER BY NULL::interval) AS v",
    'SELECT pg_catalog."percentile_cont"(NULL::double precision[]) '
    "WITHIN GROUP (ORDER BY NULL::interval) AS v",
    'SELECT pg_catalog."percentile_disc"(NULL::double precision) '
    "WITHIN GROUP (ORDER BY NULL::integer) AS v",
]


@pytest.mark.parametrize("sql", _ORDERED_SET)
def test_a_qualified_ordered_set_aggregate_is_not_a_column(sql):
    assert _violations(sql) == []


@pytest.mark.parametrize(
    "sql",
    [
        'SELECT pg_catalog."cume_dist"(3::integer) WITHIN GROUP (ORDER BY c.id) AS v '
        "FROM bank.customer c",
        'SELECT pg_catalog."percentile_cont"(0.5) WITHIN GROUP (ORDER BY score) AS v '
        "FROM bank.customer",
        'SELECT pg_catalog."mode"() WITHIN GROUP (ORDER BY c.name) FILTER (WHERE c.id > 1) '
        "FROM bank.customer c",
        "SELECT c.id FROM bank.customer c GROUP BY c.id "
        'HAVING pg_catalog."percentile_disc"(0.5) WITHIN GROUP (ORDER BY c.score) > 1',
        'SELECT pg_catalog."count"(*) FILTER (WHERE c.id > 1) AS v FROM bank.customer c',
    ],
)
def test_a_qualified_aggregate_over_a_visible_column_is_served(sql):
    assert _violations(sql) == []


@pytest.mark.parametrize(
    "sql",
    [
        'SELECT pg_catalog."mode"() WITHIN GROUP (ORDER BY c.ssn) AS v FROM bank.customer c',
        'SELECT pg_catalog."mode"() WITHIN GROUP (ORDER BY ssn) AS v FROM bank.customer',
        'SELECT pg_catalog."cume_dist"(c.ssn) WITHIN GROUP (ORDER BY c.name) AS v '
        "FROM bank.customer c",
        "SELECT pg_catalog.\"mode\"() WITHIN GROUP (ORDER BY c.name) FILTER (WHERE c.ssn = 'x') "
        "FROM bank.customer c",
    ],
)
def test_a_hidden_column_inside_a_qualified_aggregate_is_refused(sql):
    assert any(v.startswith("V003") and "ssn" in v for v in _violations(sql)), _violations(sql)


def test_a_qualified_aggregate_inside_its_order_by_naming_no_relation_is_refused():
    sql = 'SELECT pg_catalog."mode"() WITHIN GROUP (ORDER BY x.id) AS v FROM bank.customer c'
    assert any("'x' names no relation" in v for v in _violations(sql)), _violations(sql)
