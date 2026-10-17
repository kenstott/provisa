# Copyright (c) 2026 Kenneth Stott
# Canary: da73c266-da08-4253-b89c-e4b40a44d6a1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A star never reads a column the role is not served (REQ-040, REQ-263).

``*`` and ``t.*`` are the role's visible columns of the tables they cover, each masked as the
column named alone would be. A star that covers no column the role may read is refused as a
named hidden column is (V003); it is never passed through to read every column.
"""

# Requirements: REQ-040, REQ-263

from __future__ import annotations

import pytest
import sqlglot

from provisa.compiler.sql_validator import _check_column_visibility
from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.security.masking import MaskingRule, MaskType
from tests.write_governance import grants_over

_COLUMNS = [("id", "integer"), ("name", "varchar"), ("ssn", "varchar"), ("account_no", "varchar")]


def _gov(visible: frozenset[str] | None, masks: dict | None = None) -> GovernanceContext:
    return GovernanceContext(
        rls_rules={},
        masking_rules=masks or {},
        visible_columns={1: visible, 2: None},
        table_map={"bank.customer": 1, "customer": 1, "bank.deal": 2, "deal": 2},
        all_columns={1: list(_COLUMNS), 2: [("deal_id", "integer"), ("customer_id", "integer")]},
        limit_ceiling=None,
        sample_size=None,
    )


def _projection(sql: str) -> list[str]:
    return [e.alias_or_name for e in sqlglot.parse_one(sql, read="postgres").expressions]


def _v003(sql: str, gov: GovernanceContext) -> list[str]:
    tree = sqlglot.parse_one(sql, read="postgres")
    return [v.message for v in _check_column_visibility(tree, gov) if v.code == "V003"]


# -- a star that covers nothing the role reads is refused, never passed through ------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM bank.customer LIMIT 2",
        "SELECT c.* FROM bank.customer AS c",
        "SELECT customer.* FROM bank.customer",
    ],
)
def test_a_star_over_a_table_with_no_visible_column_is_refused(sql):
    gov = _gov(frozenset())
    assert _v003(sql, gov), "the validator refuses it as a hidden column (V003)"
    with pytest.raises(PermissionError, match="no column of"):
        apply_governance(sql, gov)


# -- a star is the visible columns, masked ---------------------------------------------------------


def test_a_star_reads_only_the_visible_columns():
    gov = _gov(frozenset({"id", "name"}))
    governed = apply_governance("SELECT * FROM bank.customer", gov)
    assert _projection(governed) == ["id", "name"]
    assert "ssn" not in governed and "*" not in governed


def test_a_qualified_star_reads_only_that_tables_visible_columns_masked():
    masks = {(1, "name"): (MaskingRule(mask_type=MaskType.constant, value="***"), "varchar")}
    gov = _gov(frozenset({"id", "name"}), masks)
    governed = apply_governance(
        "SELECT c.*, d.deal_id FROM bank.customer AS c JOIN bank.deal AS d ON d.customer_id = c.id",
        gov,
    )
    assert _projection(governed) == ["id", "name", "deal_id"]
    assert "ssn" not in governed and "account_no" not in governed and ".*" not in governed
    assert "'***'" in governed  # the mask, as on the column named alone


def test_a_qualified_star_over_a_fully_visible_table_is_still_masked():
    masks = {(1, "ssn"): (MaskingRule(mask_type=MaskType.constant, value="***"), "varchar")}
    gov = _gov(None, masks)
    governed = apply_governance("SELECT c.* FROM bank.customer AS c", gov)
    assert _projection(governed) == ["id", "name", "ssn", "account_no"]
    assert "'***'" in governed and ".*" not in governed


def test_a_star_over_a_derived_relation_reads_what_its_governed_body_projects():
    gov = _gov(frozenset({"id"}))
    governed = apply_governance("SELECT * FROM (SELECT * FROM bank.customer) AS s", gov)
    assert "ssn" not in governed and _projection(governed) == ["*"]
    inner = sqlglot.parse_one(governed, read="postgres").find(sqlglot.exp.Subquery)
    assert [e.alias_or_name for e in inner.this.expressions] == ["id"]


# -- the rows a write returns are read like a SELECT's ----------------------------------------------


def _writable(gov: GovernanceContext) -> GovernanceContext:
    gov.can_write = True
    gov.write_ops = {1: frozenset({"insert", "update", "delete"})}
    gov.write_returns_rows = {1: True}
    gov.write_refused_forms = {1: frozenset()}
    gov.write_grants = {1: grants_over(gov.role_id, [c for c, _ in _COLUMNS])}
    return gov


def _returning(sql: str) -> list[str]:
    tree = sqlglot.parse_one(sql, read="postgres")
    return [e.alias_or_name for e in tree.args["returning"].expressions]


def test_a_write_returning_star_returns_only_the_served_columns_masked():
    masks = {(1, "name"): (MaskingRule(mask_type=MaskType.constant, value="***"), "varchar")}
    gov = _writable(_gov(frozenset({"id", "name"}), masks))
    governed = apply_governance("DELETE FROM bank.customer WHERE id = 1 RETURNING *", gov)
    assert _returning(governed) == ["id", "name"]
    assert "ssn" not in governed and "'***'" in governed


def test_a_write_returning_a_hidden_column_is_refused():
    gov = _writable(_gov(frozenset({"id"})))
    with pytest.raises(PermissionError, match="ssn"):
        apply_governance("DELETE FROM bank.customer WHERE id = 1 RETURNING id, ssn", gov)


def test_a_write_returning_star_over_a_table_the_role_is_served_nothing_of_is_refused():
    gov = _writable(_gov(frozenset()))
    with pytest.raises(PermissionError, match="no column of"):
        apply_governance("DELETE FROM bank.customer WHERE id = 1 RETURNING *", gov)
