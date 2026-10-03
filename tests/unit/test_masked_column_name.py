# Copyright (c) 2026 Kenneth Stott
# Canary: 9a4c1e72-3d58-4b06-8f21-5e7b0c9d3a64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A masked column keeps its name in a governed SQL statement.

The mask expression replaced the column without an alias, so a client of SQL over HTTP or pgwire
received a nameless column (``?column?``) where it asked for ``email``."""

from __future__ import annotations

import pytest

from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.security.masking import MaskingRule, MaskType


def _gov() -> GovernanceContext:
    gov = GovernanceContext()
    gov.table_map = {"sales.orders": 1, "orders": 1}
    gov.all_columns = {1: [("id", "integer"), ("email", "varchar")]}
    gov.masking_rules = {
        (1, "email"): (MaskingRule(mask_type=MaskType.constant, value="***"), "varchar")
    }
    return gov


@pytest.mark.parametrize(
    ("sql", "governed"),
    [
        ("SELECT id, email FROM sales.orders", "SELECT id, '***' AS email FROM sales.orders"),
        ('SELECT "email" FROM sales.orders', "SELECT '***' AS \"email\" FROM sales.orders"),
        ("SELECT o.email FROM sales.orders o", "SELECT '***' AS email FROM sales.orders AS o"),
        # an alias the statement gave is kept as it was
        ("SELECT email AS e FROM sales.orders", "SELECT '***' AS e FROM sales.orders"),
        # an unmasked column is untouched
        ("SELECT id FROM sales.orders", "SELECT id FROM sales.orders"),
    ],
)
def test_a_masked_column_keeps_its_name(sql, governed):
    assert apply_governance(sql, _gov()) == governed
