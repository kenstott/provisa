# Copyright (c) 2026 Kenneth Stott
# Canary: 0c1d7e52-6a3b-4f19-9e8d-2b5a7c40d613
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Shapes that hide a registered relation from governance are refused (REQ-039, RV597b).

Until every reference to a registered relation is governed by construction, a statement whose
shape governance cannot read is refused rather than served: a parenthesised join group holding a
registered relation (C3), a data write anywhere but at the statement's root (C4), and a CTE named
as a registered relation the statement reads (H2). An ORDER BY name matches an output only where
PostgreSQL names one -- a real alias or a column -- so ``'ssn'`` names no output and ``ORDER BY
ssn`` is the hidden input column (H1).
"""

# Requirements: REQ-039

from __future__ import annotations

import pytest

from provisa.compiler.sql_gen import CompilationContext
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.compiler.write_admission import WriteNotAdmitted

_ROLE = {"id": "teller", "capabilities": [], "domain_access": ["*"]}
_CUSTOMER = [("id", "integer"), ("name", "varchar"), ("ssn", "varchar"), ("email", "varchar")]


def _gov(rls_rules: dict[int, str] | None = None, **extra) -> GovernanceContext:
    return GovernanceContext(
        rls_rules=rls_rules or {},
        masking_rules={},
        visible_columns={1: frozenset({"id", "name", "email"})},
        table_map={"bank.customer": 1, "customer": 1},
        all_columns={1: list(_CUSTOMER)},
        limit_ceiling=None,
        sample_size=None,
        **extra,
    )


def _codes(sql: str) -> list[str]:
    return [
        v.code
        for v in validate_sql(
            sql, CompilationContext(), _gov(), _ROLE, [], bypass_relationship_guard=True
        )
    ]


# -- C3: a parenthesised join group hides the governed table ---------------------------------------


_PARENTHESISED_GROUPS = [
    "SELECT ssn FROM (bank.customer CROSS JOIN (VALUES (1)) v)",
    "SELECT j.ssn FROM (bank.customer c CROSS JOIN (VALUES (1)) v) AS j",
    "SELECT * FROM (bank.customer c CROSS JOIN (VALUES (1)) v)",
    "SELECT id FROM (bank.customer c CROSS JOIN (VALUES (1)) v) WHERE c.ssn LIKE '1%'",
    "SELECT count(*) FROM (bank.customer JOIN (VALUES ('111-SECRET')) v(ssn) USING (ssn))",
    "SELECT count(*) FROM (bank.customer NATURAL JOIN (VALUES ('111-SECRET')) v(ssn))",
    "SELECT c.id FROM bank.customer c JOIN (bank.customer d CROSS JOIN (VALUES (1)) v) "
    "ON c.id = d.id WHERE d.ssn LIKE '1%'",
    # a parenthesised registered table with no join is hidden the same way
    "SELECT ssn FROM (bank.customer) AS x",
    "SELECT * FROM ((bank.customer c CROSS JOIN (VALUES (1)) v))",
    # the row-rule bypass: no WHERE was added inside the group
    "SELECT id FROM (bank.customer CROSS JOIN (VALUES (1)) v)",
]


@pytest.mark.parametrize("sql", _PARENTHESISED_GROUPS)
def test_a_parenthesised_join_with_a_registered_relation_is_refused(sql):
    assert "V006" in _codes(sql)


@pytest.mark.parametrize("sql", _PARENTHESISED_GROUPS)
def test_stage_2_refuses_a_parenthesised_join_with_a_registered_relation(sql):
    # a route that skips validate_sql: stage 2 refuses what it cannot govern
    with pytest.raises(PermissionError, match="V006"):
        apply_governance(sql, _gov(rls_rules={1: "id = 2"}))


def test_an_unparenthesised_join_is_still_served():
    sql = "SELECT c.id FROM bank.customer c CROSS JOIN (VALUES (1)) v"
    assert _codes(sql) == []
    assert "id = 2" in apply_governance(sql, _gov(rls_rules={1: "id = 2"})).replace('"', "")


def test_a_parenthesised_join_of_unregistered_relations_is_still_served():
    sql = "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) SELECT a.x FROM (a CROSS JOIN b)"
    assert _codes(sql) == []


def test_a_derived_table_is_still_served():
    assert _codes("SELECT d.id FROM (SELECT id FROM bank.customer) AS d") == []


# -- C4: a data write is admitted only as the statement itself -------------------------------------


_NESTED_WRITES = [
    "WITH u AS (UPDATE bank.customer SET name = name RETURNING ssn) SELECT * FROM u",
    "WITH u AS (UPDATE bank.customer SET name = name WHERE ssn LIKE '1%' RETURNING id) "
    "SELECT * FROM u",
    "WITH u AS (DELETE FROM bank.customer WHERE ssn LIKE '9%' RETURNING ssn) SELECT ssn FROM u",
    "WITH u AS (INSERT INTO bank.customer (id) VALUES (3) RETURNING id) SELECT * FROM u",
]


@pytest.mark.parametrize("sql", _NESTED_WRITES)
def test_a_data_write_inside_a_statement_is_refused(sql):
    assert "V006" in _codes(sql)


@pytest.mark.parametrize("sql", _NESTED_WRITES)
def test_stage_2_does_not_read_a_nested_write_as_a_read(sql):
    gov = _gov(write_ops={1: frozenset({"insert", "update", "delete"})})
    with pytest.raises(WriteNotAdmitted, match="inside"):
        apply_governance(sql, gov)


def test_a_merge_s_own_actions_are_not_nested_writes():
    sql = (
        "MERGE INTO bank.customer t USING (VALUES (1, 'x')) s(id, name) ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET name = s.name "
        "WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)"
    )
    assert "V006" not in _codes(sql)


def test_a_top_level_write_is_not_a_nested_write():
    assert "V006" not in _codes("UPDATE bank.customer SET name = 'x' WHERE id = 1")


# -- H2: a CTE named as a registered relation ------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "WITH customer AS (SELECT 1 AS x) SELECT id FROM bank.customer WHERE ssn LIKE '1%'",
        "WITH customer AS (SELECT 1 AS x) SELECT id, ssn FROM bank.customer ORDER BY ssn",
        # folded as PostgreSQL folds it
        "WITH Customer AS (SELECT 1 AS x) SELECT id FROM bank.customer",
        "SELECT (WITH customer AS (SELECT 1 AS x) SELECT x FROM customer) FROM bank.customer",
    ],
)
def test_a_cte_named_as_a_registered_relation_is_refused(sql):
    assert "V006" in _codes(sql)


def test_a_cte_with_another_name_is_still_served():
    assert _codes("WITH w AS (SELECT id FROM bank.customer) SELECT id FROM w") == []


# -- H1: a string literal names no output ----------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 'ssn', id FROM bank.customer ORDER BY ssn DESC",
        "SELECT 'ssn', id FROM bank.customer ORDER BY ssn ASC",
        "SELECT id FROM bank.customer ORDER BY ssn",
        "SELECT count(*) FROM bank.customer GROUP BY ssn",
        # GROUP BY reads the input column before an output of that name
        "SELECT name AS ssn FROM bank.customer GROUP BY ssn",
    ],
)
def test_an_order_or_group_key_that_names_no_output_is_judged_as_the_input(sql):
    assert "V003" in _codes(sql)


def test_stage_2_does_not_take_a_string_literal_for_an_output_name():
    governed = apply_governance("SELECT 'ssn', id FROM bank.customer ORDER BY ssn DESC", _gov())
    assert "ssn DESC" not in governed.replace('"', "")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT name AS ssn, id FROM bank.customer ORDER BY ssn",
        "SELECT id, name FROM bank.customer ORDER BY name",
        "SELECT name, count(*) FROM bank.customer GROUP BY name ORDER BY name",
        "SELECT name AS n FROM bank.customer GROUP BY n ORDER BY n",
    ],
)
def test_an_order_key_naming_a_real_output_is_still_served(sql):
    assert _codes(sql) == []


def test_stage_2_keeps_an_order_key_naming_a_real_alias():
    governed = apply_governance("SELECT name AS ssn, id FROM bank.customer ORDER BY ssn", _gov())
    assert "ORDER BY ssn" in governed.replace('"', "")
