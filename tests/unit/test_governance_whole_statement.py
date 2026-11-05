# Copyright (c) 2026 Kenneth Stott
# Canary: 8e2b6c40-1f73-4d95-a3b8-5c9d0e7f2a16
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Governance applies to the whole statement: a role computes only over what it can see.

Every reference to a masked column reads the mask, a hidden column is in no relation the
statement reads (REQ-039: naming it is refused as an unknown column), and every SELECT in the
statement -- CTE, EXISTS body, subquery, set-operation branch -- reads the role's row filters,
whatever the outer SELECT reads. Each statement is checked by what it answers on PostgreSQL: the
same rows the statement written with the mask in place of each reference answers."""

from __future__ import annotations

import pytest

from provisa.compiler.stage2 import GovernanceContext, apply_governance, govern_fragment
from provisa.security.masking import MaskingRule, MaskType
from tests.governed_pg import governed_database

_TABLES = {
    "sales.orders": [
        {"id": 1, "region": "east", "email": "u1@x", "secret": "s1"},
        {"id": 2, "region": "west", "email": "u2@x", "secret": "s2"},
        {"id": 3, "region": "east", "email": "a3@x", "secret": "s3"},
    ],
    "sales.customers": [
        {"id": 1, "name": "***"},
        {"id": 2, "name": "u2@x"},
        {"id": 3, "name": "a3@x"},
    ],
}


@pytest.fixture(scope="module")
def pg(embedded_postgres):
    return governed_database(embedded_postgres, "test_governance_whole_statement")


_RLS = '("orders"."region" = \'east\')'


def _gov(mask: MaskingRule | None = None) -> GovernanceContext:
    gov = GovernanceContext()
    gov.table_map = {"sales.orders": 1, "orders": 1, "sales.customers": 2, "customers": 2}
    gov.all_columns = {
        1: [("id", "integer"), ("region", "varchar"), ("email", "varchar"), ("secret", "varchar")],
        2: [("id", "integer"), ("name", "varchar")],
    }
    gov.visible_columns = {1: frozenset({"id", "region", "email"})}
    gov.masking_rules = {
        (1, "email"): (mask or MaskingRule(mask_type=MaskType.constant, value="***"), "varchar")
    }
    gov.rls_rules = {1: "region = 'east'"}
    return gov


@pytest.mark.parametrize(
    ("sql", "governed"),
    [
        # select-list expressions
        (
            "SELECT upper(email) AS e FROM sales.orders",
            f"SELECT UPPER('***') AS e FROM sales.orders WHERE {_RLS}",
        ),
        (
            "SELECT email || '' AS e FROM sales.orders",
            f"SELECT '***' || '' AS e FROM sales.orders WHERE {_RLS}",
        ),
        # WHERE: the reader's filter compares the mask, so it matches nothing
        (
            "SELECT id FROM sales.orders WHERE email = 'u1@x'",
            f"SELECT id FROM sales.orders WHERE '***' = 'u1@x' AND {_RLS}",
        ),
        # GROUP BY / HAVING
        (
            "SELECT count(*) FROM sales.orders GROUP BY email HAVING max(email) > 'a'",
            f"SELECT COUNT(*) FROM sales.orders WHERE {_RLS} GROUP BY CAST('***' AS VARCHAR) "
            "HAVING MAX('***') > 'a'",
        ),
        # ORDER BY: ordering by a constant orders nothing
        (
            "SELECT id FROM sales.orders ORDER BY email, id",
            f"SELECT id FROM sales.orders WHERE {_RLS} ORDER BY id",
        ),
        ("SELECT id FROM sales.orders ORDER BY email", f"SELECT id FROM sales.orders WHERE {_RLS}"),
        # window clauses
        (
            "SELECT id, row_number() OVER (PARTITION BY email) AS n FROM sales.orders",
            f"SELECT id, ROW_NUMBER() OVER (PARTITION BY '***') AS n FROM sales.orders WHERE {_RLS}",
        ),
    ],
)
def test_every_reference_to_a_masked_column_reads_the_mask(pg, sql, governed):
    assert sorted(pg.answer(apply_governance(sql, _gov()), _TABLES)) == sorted(
        pg.answer(governed, _TABLES)
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
def test_a_hidden_column_is_in_no_relation_the_statement_reads(sql):
    # REQ-039: refused in every clause, naming the column, before anything is sent
    with pytest.raises(PermissionError, match=r"\[V003\] Column 'secret' is not visible"):
        apply_governance(sql, _gov())


def test_a_relationship_join_the_compiler_writes_on_a_masked_key_reads_the_mask(pg):
    """REQ-740: GraphQL, REST, JSON:API and gRPC compile a relationship to a join of quoted
    aliases, and that statement is governed here as any other: a join on a masked key compares
    the mask, so the role is not answered by the real value."""
    compiled = (
        'SELECT "t0"."id", "t1"."name" FROM "sales"."orders" "t0" '
        'LEFT JOIN "sales"."customers" "t1" ON "t0"."email" = "t1"."name"'
    )
    assert sorted(pg.answer(apply_governance(compiled, _gov()), _TABLES)) == [
        (1, "***"),
        (3, "***"),
    ]


def test_the_row_filter_reads_the_real_values_of_a_masked_column(pg):
    """The filter is the policy, not the reader's computation: it reads the real values; the
    reader's own references read the mask."""
    gov = _gov()
    gov.rls_rules = {1: "email <> 'u2@x'"}
    assert (
        pg.answer(
            apply_governance("SELECT id FROM sales.orders WHERE email = 'u1@x'", gov), _TABLES
        )
        == []
    )
    governed = apply_governance("SELECT id FROM sales.orders ORDER BY id", gov)
    assert pg.answer(governed, _TABLES) == [(1,), (3,)]


def test_a_mask_is_applied_once_however_the_column_is_reached(pg):
    regex = MaskingRule(mask_type=MaskType.regex, pattern=".+@", replace="x@")
    governed = apply_governance(
        "SELECT email, upper(email) AS u FROM sales.orders WHERE email LIKE 'x%' ORDER BY id",
        _gov(regex),
    )
    assert governed.count("PROVISA_REGEX_REPLACE") == 1  # once, in the governed relation
    assert pg.answer(governed, _TABLES) == [("x@x", "X@X"), ("x@x", "X@X")]


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        # the outer SELECT reads another registered table; the nested one must still be governed
        (
            "WITH o AS (SELECT id, region FROM sales.orders) "
            "SELECT c.id, o.region FROM sales.customers c JOIN o ON o.id = c.id",
            [(1, "east"), (3, "east")],
        ),
        (
            "SELECT c.id FROM sales.customers c WHERE EXISTS "
            "(SELECT 1 FROM sales.orders o WHERE o.id = c.id)",
            [(1,), (3,)],
        ),
        (
            "SELECT c.id FROM sales.customers c WHERE c.id IN (SELECT id FROM sales.orders)",
            [(1,), (3,)],
        ),
        (
            "SELECT c.id, (SELECT max(region) FROM sales.orders) AS r FROM sales.customers c",
            [(1, "east"), (2, "east"), (3, "east")],
        ),
        (
            "SELECT id FROM sales.customers UNION ALL SELECT id FROM sales.orders",
            [(1,), (1,), (2,), (3,), (3,)],
        ),
        (
            "SELECT c.id FROM sales.customers c JOIN (SELECT id FROM sales.orders) d ON d.id = c.id",
            [(1,), (3,)],
        ),
    ],
)
def test_every_select_in_the_statement_carries_the_row_filter(pg, sql, answer):
    governed = apply_governance(sql, _gov())
    assert governed.count("\"region\" = 'east'") == 1, governed
    assert sorted(pg.answer(governed, _TABLES)) == answer


def test_a_correlated_reference_to_an_outer_masked_column_is_masked(pg):
    governed = apply_governance(
        "SELECT o.id FROM sales.orders o WHERE EXISTS "
        "(SELECT 1 FROM sales.customers c WHERE c.name = o.email) ORDER BY o.id",
        _gov(),
    )
    # o.email reads the mask, which only customer 1's name matches.
    assert pg.answer(governed, _TABLES) == [(1,), (3,)]


def test_a_fragment_is_governed_without_a_row_cap(pg):
    gov = _gov()
    gov.limit_ceiling = 10
    fragment = govern_fragment("SELECT id, email FROM sales.orders ORDER BY id", gov)
    assert "LIMIT" not in fragment
    assert pg.answer(fragment, _TABLES) == [(1, "***"), (3, "***")]
    assert apply_governance("SELECT id FROM sales.orders", gov).endswith("LIMIT 10")
