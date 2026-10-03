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

Every reference to a masked column is the mask, every reference to a hidden column is NULL, and
every SELECT in the statement — CTE, EXISTS body, subquery, set-operation branch — carries the
role's row filters, whatever the outer SELECT reads."""

from __future__ import annotations

import pytest

from provisa.compiler.stage2 import GovernanceContext, apply_governance, govern_fragment
from provisa.security.masking import MaskingRule, MaskType

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
        # a hidden column, anywhere, is NULL
        (
            "SELECT upper(secret) AS s FROM sales.orders WHERE secret = 'x'",
            f"SELECT UPPER(NULL) AS s FROM sales.orders WHERE NULL = 'x' AND {_RLS}",
        ),
    ],
)
def test_every_reference_to_a_masked_or_hidden_column_is_governed(sql, governed):
    assert apply_governance(sql, _gov()) == governed


def test_the_row_filter_reads_the_real_values_of_a_masked_column():
    """The filter is the policy, not the reader's computation: it is added after the reader's
    references are masked, and is never masked itself."""
    gov = _gov()
    gov.rls_rules = {1: "email <> 'blocked@x'"}
    assert apply_governance("SELECT id FROM sales.orders WHERE email = 'a'", gov) == (
        "SELECT id FROM sales.orders WHERE '***' = 'a' AND (\"orders\".\"email\" <> 'blocked@x')"
    )


def test_a_mask_is_applied_once_however_the_column_is_reached():
    regex = MaskingRule(mask_type=MaskType.regex, pattern=".+@", replace="x@")
    governed = apply_governance(
        "SELECT email, upper(email) AS u FROM sales.orders WHERE email LIKE 'a%'", _gov(regex)
    )
    assert governed.count("REGEXP_REPLACE") == 3  # one per reference, none nested
    assert "REGEXP_REPLACE(REGEXP_REPLACE" not in governed


@pytest.mark.parametrize(
    "sql",
    [
        # the outer SELECT reads another registered table; the nested one must still be governed
        "WITH o AS (SELECT id, region FROM sales.orders) "
        "SELECT c.id, o.region FROM sales.customers c JOIN o ON o.id = c.id",
        "SELECT c.id FROM sales.customers c WHERE EXISTS (SELECT 1 FROM sales.orders o WHERE o.id = c.id)",
        "SELECT c.id FROM sales.customers c WHERE c.id IN (SELECT id FROM sales.orders)",
        "SELECT c.id, (SELECT max(region) FROM sales.orders) AS r FROM sales.customers c",
        "SELECT id FROM sales.customers UNION ALL SELECT id FROM sales.orders",
        "SELECT c.id FROM sales.customers c JOIN (SELECT id FROM sales.orders) d ON d.id = c.id",
    ],
)
def test_every_select_in_the_statement_carries_the_row_filter(sql):
    governed = apply_governance(sql, _gov())
    assert governed.count("\"region\" = 'east'") == 1, governed


def test_a_correlated_reference_to_an_outer_masked_column_is_masked():
    governed = apply_governance(
        "SELECT o.id FROM sales.orders o WHERE EXISTS "
        "(SELECT 1 FROM sales.customers c WHERE c.name = o.email)",
        _gov(),
    )
    assert "c.name = '***'" in governed


def test_a_fragment_is_governed_without_a_row_cap():
    gov = _gov()
    gov.limit_ceiling = 10
    assert govern_fragment("SELECT id, email FROM sales.orders", gov) == (
        f"SELECT id, '***' AS email FROM sales.orders WHERE {_RLS}"
    )
    assert apply_governance("SELECT id FROM sales.orders", gov).endswith("LIMIT 10")
