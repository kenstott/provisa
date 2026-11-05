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
registered relation, unless it is the leftmost FROM item, which is the flat join and governed as
one (C3); a data write anywhere but where REQ-039 admits one -- the statement's root, or
``WITH w AS (<write> RETURNING ...) SELECT ... FROM w`` -- or a second write (C4); and a CTE
named as a registered
relation the statement reads (H2). An ORDER BY name matches an output only where
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
_ORDERS = [("id", "integer"), ("customer_id", "integer")]
_REGIONS = [("customer_id", "integer"), ("label", "varchar")]


def _gov(rls_rules: dict[int, str] | None = None, **extra) -> GovernanceContext:
    return GovernanceContext(
        rls_rules=rls_rules or {},
        masking_rules={},
        visible_columns={1: frozenset({"id", "name", "email"})},
        table_map={
            "bank.customer": 1,
            "customer": 1,
            "bank.orders": 2,
            "bank.regions": 3,
            # the names a BI tool's connection writes
            "public.customers": 1,
            "public.orders": 2,
            "public.regions": 3,
        },
        all_columns={1: list(_CUSTOMER), 2: list(_ORDERS), 3: list(_REGIONS)},
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
#
# Joins associate left to right, so a group that is the leftmost FROM item is the flat join: it is
# flattened and governed as one. A group elsewhere changes meaning and is refused.

_ROW_RULE = {1: "id = 2"}


def _governed(sql: str) -> str:
    return apply_governance(sql, _gov(rls_rules=_ROW_RULE)).replace('"', "")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT ssn FROM (bank.customer CROSS JOIN (VALUES (1)) v)",
        "SELECT count(*) FROM (bank.customer JOIN (VALUES ('111-SECRET')) v(ssn) USING (ssn))",
        "SELECT count(*) FROM (bank.customer NATURAL JOIN (VALUES ('111-SECRET')) v(ssn))",
        "SELECT c.ssn FROM ((bank.customer c CROSS JOIN (VALUES (1)) v))",
    ],
)
def test_a_leftmost_group_s_hidden_column_is_refused_as_in_the_flat_join(sql):
    assert "V003" in _codes(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT id FROM (bank.customer CROSS JOIN (VALUES (1)) v)",
        "SELECT * FROM (bank.customer c CROSS JOIN (VALUES (1)) v)",
        "SELECT id FROM (bank.customer c CROSS JOIN (VALUES (1)) v) WHERE c.ssn LIKE '1%'",
        "SELECT * FROM ((bank.customer c CROSS JOIN (VALUES (1)) v))",
    ],
)
def test_a_leftmost_group_is_governed_as_the_flat_join(sql):
    assert _codes(sql) == []
    governed = _governed(sql)
    assert "customer" in governed and "(bank.customer" not in governed  # flattened
    assert "id = 2" in governed  # the row rule
    assert "ssn" not in governed  # the hidden column, served nowhere


_TABLEAU = (
    'SELECT "o"."id", "c"."name", "r"."label" FROM (("bank"."orders" "o" '
    'INNER JOIN "bank"."customer" "c" ON ("o"."customer_id" = "c"."id")) '
    'LEFT JOIN "bank"."regions" "r" ON ("c"."id" = "r"."customer_id"))'
)


def test_a_bi_tool_left_deep_nested_join_runs_governed():
    assert _codes(_TABLEAU) == []
    governed = _governed(_TABLEAU)
    assert "FROM bank.orders AS o INNER JOIN bank.customer AS c" in governed
    assert "LEFT JOIN bank.regions AS r" in governed
    assert "c.id = 2" in governed


def test_a_bi_tool_left_deep_nested_join_s_hidden_column_is_refused():
    assert "V003" in _codes(_TABLEAU.replace('"c"."name"', '"c"."ssn"'))


# Tableau's classic joins: left-deep, the table's name as its quoted alias, ordinal GROUP BY.
_TABLEAU_CLASSIC = (
    'SELECT "customers"."name" AS "name", "regions"."label" AS "label", '
    'SUM("orders"."id") AS "sum:amount:ok" '
    'FROM (("public"."orders" "orders" '
    'INNER JOIN "public"."customers" "customers" ON ("orders"."customer_id" = "customers"."id")) '
    'LEFT JOIN "public"."regions" "regions" ON ("customers"."id" = "regions"."customer_id")) '
    "GROUP BY 1, 2"
)


def test_tableau_classic_joins_are_flattened_and_governed():
    assert _codes(_TABLEAU_CLASSIC) == []
    governed = _governed(_TABLEAU_CLASSIC)
    assert (
        "FROM public.orders AS orders INNER JOIN public.customers AS customers ON "
        "(orders.customer_id = customers.id) LEFT JOIN public.regions AS regions"
    ) in governed
    assert "customers.id = 2" in governed  # the row rule
    assert governed.endswith("GROUP BY 1, 2")


# Tableau's custom SQL as a join operand: a derived table with its own alias, not an aliased group.
_TABLEAU_CUSTOM_SQL = (
    'SELECT "Custom SQL Query"."n" AS "n", "customers"."name" AS "name" '
    'FROM ((SELECT id AS n FROM bank.orders) "Custom SQL Query" '
    'INNER JOIN "public"."customers" "customers" ON ("Custom SQL Query"."n" = "customers"."id"))'
)


def test_tableau_custom_sql_as_a_join_operand_is_not_refused():
    assert _codes(_TABLEAU_CUSTOM_SQL) == []
    governed = _governed(_TABLEAU_CUSTOM_SQL)
    assert "FROM (SELECT id AS n FROM bank.orders) AS Custom SQL Query INNER JOIN" in governed
    assert "customers.id = 2" in governed


def test_tableau_custom_sql_reading_a_hidden_column_is_refused():
    sql = _TABLEAU_CUSTOM_SQL.replace(
        "SELECT id AS n FROM bank.orders", "SELECT ssn AS n FROM bank.customer"
    )
    assert "V003" in _codes(sql)


def test_an_odbc_left_group_is_flattened_and_governed():
    # MS Query / ODBC: the first join in parentheses, the next outside them
    sql = (
        "SELECT o.id FROM (bank.orders o INNER JOIN bank.customer c ON o.customer_id = c.id) "
        "INNER JOIN bank.regions r ON c.id = r.customer_id"
    )
    assert _codes(sql) == []
    governed = _governed(sql)
    assert (
        "FROM bank.orders AS o INNER JOIN bank.customer AS c ON o.customer_id = c.id "
        "INNER JOIN bank.regions AS r"
    ) in governed
    assert "c.id = 2" in governed


_REFUSED_GROUPS = [
    # the right side of a join: flattening would change what the join means
    "SELECT c.id FROM bank.customer c JOIN (bank.customer d CROSS JOIN (VALUES (1)) v) "
    "ON c.id = d.id WHERE d.ssn LIKE '1%'",
    "SELECT o.id FROM bank.orders o LEFT JOIN (bank.customer c JOIN bank.regions r "
    "ON c.id = r.customer_id) ON o.customer_id = c.id",
    # under an alias: the alias names the group, which the flat join has not
    "SELECT j.ssn FROM (bank.customer c CROSS JOIN (VALUES (1)) v) AS j",
    "SELECT ssn FROM (bank.customer) AS x",
]


@pytest.mark.parametrize("sql", _REFUSED_GROUPS)
def test_a_group_that_is_not_leftmost_or_is_aliased_is_refused(sql):
    assert "V006" in _codes(sql)


@pytest.mark.parametrize("sql", _REFUSED_GROUPS)
def test_stage_2_refuses_a_group_that_is_not_leftmost_or_is_aliased(sql):
    # a route that skips validate_sql: stage 2 refuses what it cannot govern
    with pytest.raises(PermissionError, match="V006"):
        apply_governance(sql, _gov(rls_rules=_ROW_RULE))


def test_an_unparenthesised_join_is_still_served():
    sql = "SELECT c.id FROM bank.customer c CROSS JOIN (VALUES (1)) v"
    assert _codes(sql) == []
    assert "id = 2" in apply_governance(sql, _gov(rls_rules={1: "id = 2"})).replace('"', "")


def test_a_parenthesised_join_of_unregistered_relations_is_still_served():
    sql = "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) SELECT a.x FROM (a CROSS JOIN b)"
    assert _codes(sql) == []


def test_a_derived_table_is_still_served():
    assert _codes("SELECT d.id FROM (SELECT id FROM bank.customer) AS d") == []


# -- C4: a data write stands at the statement's root or in a RETURNING CTE ------------------------


_RETURNING_CTE_WRITES = [
    "WITH u AS (UPDATE bank.customer SET name = name RETURNING ssn) SELECT * FROM u",
    "WITH u AS (UPDATE bank.customer SET name = name WHERE ssn LIKE '1%' RETURNING id) "
    "SELECT * FROM u",
    "WITH u AS (DELETE FROM bank.customer WHERE ssn LIKE '9%' RETURNING ssn) SELECT ssn FROM u",
    "WITH u AS (INSERT INTO bank.customer (id) VALUES (3) RETURNING id) SELECT * FROM u",
]

_MISPLACED_WRITES = [
    "WITH a AS (UPDATE bank.customer SET name = 'a' RETURNING id), "
    "b AS (DELETE FROM bank.customer RETURNING id) SELECT * FROM a, b",
    "WITH w AS (UPDATE bank.customer SET name = 'a') SELECT 1",
    "SELECT * FROM (WITH w AS (UPDATE bank.customer SET name = 'a' RETURNING id) "
    "SELECT * FROM w) d",
    "WITH x AS (SELECT 1), w AS (WITH v AS (DELETE FROM bank.customer RETURNING id) "
    "SELECT * FROM v) SELECT * FROM w",
]


@pytest.mark.parametrize("sql", _RETURNING_CTE_WRITES)
def test_a_write_in_a_returning_cte_is_not_refused_for_its_placement(sql):
    assert "V006" not in _codes(sql)


def _writer() -> GovernanceContext:
    """A role that may insert, update and delete ``bank.customer``'s served columns."""
    ops = ("insert", "update", "delete")
    gov = _gov(
        write_ops={1: frozenset(ops)},
        write_returns_rows={1: True},
        write_refused_forms={1: frozenset()},
    )
    gov.can_write = True
    gov.role_id = "teller"
    gov.write_grants = {
        1: {
            op: {"operation": op, "role": "teller", "columns": ["id", "name", "email"]}
            for op in ops
        }
    }
    return gov


@pytest.mark.parametrize("sql", _RETURNING_CTE_WRITES)
def test_stage_2_takes_a_write_in_a_returning_cte_as_a_write(sql):
    # Admitted where it stands; one that reads the hidden ssn is refused for that (V003), never
    # for where it stands (V006).
    gov = _writer()
    try:
        apply_governance(sql, gov)
    except PermissionError as refused:
        assert str(refused).startswith("[V003]"), refused
    else:
        assert gov.admitted_write is not None


@pytest.mark.parametrize("sql", _MISPLACED_WRITES)
def test_a_data_write_placed_elsewhere_is_refused(sql):
    assert "V006" in _codes(sql)


@pytest.mark.parametrize("sql", _MISPLACED_WRITES)
def test_stage_2_refuses_a_data_write_placed_elsewhere(sql):
    gov = _writer()
    with pytest.raises(WriteNotAdmitted, match="V006"):
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
