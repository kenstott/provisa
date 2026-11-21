# Copyright (c) 2026 Kenneth Stott
# Canary: 8a4c1f37-2b6e-4d90-9e15-c7f3a2b80e64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The passes after governance read a governed relation as the table it reads (REQ-039).

Every registered table stands in a governed statement as a derived table that applies the role's
row rule and masks. A column the relation serves as it is reads as the table's column; a masked
one does not, so no key bound or join key is ever taken from a mask's value.
"""

# Requirements: REQ-039, REQ-1865, REQ-1915

from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp

from provisa.compiler.governed_relation import base_table, read_through, relations
from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.federation.query_residency import join_key_column
from provisa.security.masking import MaskingRule, MaskType


def _gov(**masks: MaskingRule) -> GovernanceContext:
    gov = GovernanceContext()
    gov.table_map = {"sales.orders": 1, "orders": 1, "sales.customers": 2, "customers": 2}
    gov.all_columns = {
        1: [("id", "integer"), ("customer_id", "integer"), ("amount", "integer")],
        2: [("id", "integer"), ("name", "varchar")],
    }
    gov.rls_rules = {1: "amount > 4"}
    gov.masking_rules = {(1, name): (rule, "integer") for name, rule in masks.items()}
    return gov


_ZERO = MaskingRule(mask_type=MaskType.constant, value=0)


def _governed(sql: str, gov: GovernanceContext) -> exp.Expr:
    return sqlglot.parse_one(apply_governance(sql, gov), read="postgres")


def test_a_governed_relation_is_read_as_its_table():
    tree = _governed("SELECT o.id FROM sales.orders o WHERE o.id = 7", _gov(amount=_ZERO))
    (relation,) = relations(tree)
    assert relation.table.name == "orders" and relation.alias == "o"
    assert relation.column("id") == "id"
    assert relation.column("amount") is None  # served masked: not the column's value
    assert [p.sql(dialect="postgres") for p in relation.predicates] == ['"o"."amount" > 4']
    assert base_table(relation.relation) is relation.table


def test_a_bound_is_read_through_the_relation_and_never_from_a_mask():
    tree = _governed(
        "SELECT o.id FROM sales.orders o WHERE o.id = 7 AND o.customer_id = 3",
        _gov(customer_id=_ZERO),
    )
    (relation,) = relations(tree)
    where = tree.args["where"].this
    over = relation.over_table(where, sole=True).sql(dialect="postgres")
    assert over == "o.id = 7 AND NULL = 3"


def test_a_derived_table_that_reads_more_than_one_relation_is_not_a_governed_relation():
    tree = sqlglot.parse_one(
        "SELECT x.id FROM (SELECT a.id AS id FROM t AS a JOIN u AS b ON a.id = b.id) AS x",
        read="postgres",
    )
    assert read_through(tree.args["from_"].this) is None
    assert relations(tree) == []


def test_a_join_key_is_read_through_the_relation_it_joins():
    tree = _governed(
        "SELECT o.id FROM sales.customers c JOIN sales.orders o ON o.customer_id = c.id",
        _gov(),
    )
    (join,) = tree.args["joins"]
    found = join_key_column(join)
    assert found is not None and found[0] == "customer_id"
    assert found[1].sql(dialect="postgres") == "c.id"


def test_a_masked_join_key_binds_nothing():
    tree = _governed(
        "SELECT o.id FROM sales.customers c JOIN sales.orders o ON o.customer_id = c.id",
        _gov(customer_id=_ZERO),
    )
    (join,) = tree.args["joins"]
    assert join_key_column(join) is None
