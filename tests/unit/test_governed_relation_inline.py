# Copyright (c) 2026 Kenneth Stott
# Canary: 5e2a9c71-0b4d-4f83-a6e1-93d7c2f0b418
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A lone governed relation inlined into the SELECT that reads it (REQ-039, REQ-2000).

A translator into a query language with no derived table (Qdrant's API) reads a governed SELECT
of one table as the table, its row rules in the SELECT's WHERE and its masks in place of the
columns. The inlined statement is governed exactly as the wrapped one: run on PostgreSQL, both
answer the same rows under the same column names, and a hidden column is still not there.
"""

# Requirements: REQ-039, REQ-2000, REQ-913

from __future__ import annotations

import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.compiler.governed_relation import inline
from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.security.masking import MaskingRule, MaskType
from tests.governed_pg import Refused, governed_database

_TABLES = {
    "sales.orders": [
        {"id": 1, "region": "east", "email": "u1@x", "secret": "s1", "amount": 10},
        {"id": 2, "region": "west", "email": "u2@x", "secret": "s2", "amount": 20},
        {"id": 3, "region": "east", "email": "a3@x", "secret": "s3", "amount": 30},
        {"id": 4, "region": "east", "email": "a4@x", "secret": "s4", "amount": 40},
    ],
    "sales.customers": [{"id": 1, "name": "n1"}, {"id": 3, "name": "n3"}],
}


@pytest.fixture(scope="module")
def pg(embedded_postgres):
    return governed_database(embedded_postgres, "test_governed_relation_inline")


def _gov() -> GovernanceContext:
    """``orders``: a row rule (east only), a masked column (email), a hidden one (secret)."""
    gov = GovernanceContext()
    gov.table_map = {"sales.orders": 1, "orders": 1, "sales.customers": 2, "customers": 2}
    gov.all_columns = {
        1: [
            ("id", "integer"),
            ("region", "varchar"),
            ("email", "varchar"),
            ("secret", "varchar"),
            ("amount", "integer"),
        ],
        2: [("id", "integer"), ("name", "varchar")],
    }
    gov.visible_columns = {1: frozenset({"id", "region", "email", "amount"})}
    gov.masking_rules = {
        (1, "email"): (MaskingRule(mask_type=MaskType.constant, value="***"), "varchar")
    }
    gov.rls_rules = {1: "region = 'east'"}
    return gov


def _both(sql: str) -> tuple[exp.Expr, exp.Expr]:
    wrapped = sqlglot.parse_one(apply_governance(sql, _gov()), read="postgres")
    return wrapped, inline(wrapped)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT id, email FROM sales.orders",
        "SELECT * FROM sales.orders",
        "SELECT o.* FROM sales.orders AS o",
        "SELECT o.id AS k, upper(o.email) AS e FROM sales.orders AS o WHERE o.amount > 15",
        # the statement's OR stays one condition beside the row rule
        "SELECT id FROM sales.orders WHERE id = 3 OR id = 2",
        "SELECT id FROM sales.orders WHERE email = 'u1@x'",
        "SELECT id AS amount FROM sales.orders ORDER BY amount DESC LIMIT 2",
        "SELECT region, count(*) AS n, sum(amount) AS s FROM sales.orders GROUP BY region",
        "SELECT email, count(*) AS n FROM sales.orders GROUP BY email HAVING count(*) > 1",
        "SELECT id, row_number() OVER (PARTITION BY region ORDER BY amount DESC) AS r "
        "FROM sales.orders",
        "SELECT DISTINCT region FROM sales.orders",
        # a SELECT reading the relation inside another statement
        "SELECT c.name FROM sales.customers AS c WHERE c.id IN (SELECT id FROM sales.orders)",
    ],
)
def test_the_inlined_statement_answers_what_the_governed_one_answers(pg, sql):
    wrapped, inlined = _both(sql)
    expected = sorted(pg.answer(wrapped.sql(dialect="postgres"), _TABLES))
    names = list(pg.names)
    assert sorted(pg.answer(inlined.sql(dialect="postgres"), _TABLES)) == expected
    assert pg.names == names


def test_a_lone_relation_becomes_its_table_with_the_row_rule_in_the_where():
    _, inlined = _both("SELECT id, email FROM sales.orders WHERE amount > 15")
    assert isinstance(inlined, exp.Select)
    source = inlined.args["from_"].this
    assert isinstance(source, exp.Table) and source.name == "orders"
    assert inlined.find(exp.Subquery) is None
    assert inlined.sql(dialect="postgres") == (
        'SELECT "orders"."id", \'***\' AS email FROM sales.orders AS "orders" '
        'WHERE "orders"."region" = \'east\' AND "orders"."amount" > 15'
    )


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT upper(secret) AS s FROM sales.orders",
        "SELECT id FROM sales.orders WHERE secret = 's1'",
        "SELECT id FROM sales.orders ORDER BY secret",
        "SELECT count(*) FROM sales.orders GROUP BY secret",
    ],
)
def test_a_hidden_column_is_not_there_in_the_inlined_statement_either(pg, sql):
    """Naming a column the relation does not expose leaves the statement as governance built
    it, so the column stays unknown -- never the table's column read past the relation."""
    wrapped = sqlglot.parse_one(
        apply_governance(sql.replace("secret", "region"), _gov()), read="postgres"
    )
    # the governed statement as a role would write it naming the hidden column
    hidden = wrapped.transform(
        lambda n: (
            exp.column("secret", table=n.table)
            if isinstance(n, exp.Column)
            and n.name == "region"
            and n.find_ancestor(exp.Subquery) is None
            else n
        )
    )
    inlined = inline(hidden)
    assert inlined == hidden
    with pytest.raises(Refused, match="secret"):
        pg.answer(inlined.sql(dialect="postgres"), _TABLES)


@pytest.mark.parametrize(
    "sql",
    [
        # another relation beside it
        "SELECT o.id, c.name FROM sales.orders AS o JOIN sales.customers AS c ON c.id = o.id",
        # a nested statement in the SELECT's own scope
        "SELECT id FROM sales.orders WHERE id IN (SELECT id FROM sales.customers)",
    ],
)
def test_a_select_reading_more_than_its_relation_is_left_as_it_stands(pg, sql):
    wrapped, inlined = _both(sql)
    outer = inlined if isinstance(inlined, exp.Select) else None
    assert outer is not None
    assert isinstance(outer.args["from_"].this, exp.Subquery)
    assert sorted(pg.answer(inlined.sql(dialect="postgres"), _TABLES)) == sorted(
        pg.answer(wrapped.sql(dialect="postgres"), _TABLES)
    )


def test_the_statement_given_is_not_changed():
    wrapped = sqlglot.parse_one(apply_governance("SELECT id FROM sales.orders", _gov()))
    before = wrapped.sql(dialect="postgres")
    inline(wrapped)
    assert wrapped.sql(dialect="postgres") == before


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT email, count(*) AS n FROM sales.orders GROUP BY email",
        "SELECT id FROM sales.orders ORDER BY email, id",
        "SELECT DISTINCT ON (email) id FROM sales.orders ORDER BY email, id",
    ],
)
def test_a_mask_s_constant_is_never_made_a_position_by_inlining(pg, sql):
    """A masked column standing as a GROUP BY, ORDER BY or DISTINCT ON item would inline as a
    constant there, which PostgreSQL reads as an output column's number: the SELECT is left as
    governance built it."""
    wrapped, inlined = _both(sql)
    assert inlined == wrapped
