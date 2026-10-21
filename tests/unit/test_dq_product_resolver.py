# Copyright (c) 2026 Kenneth Stott
# Canary: 3e9a7c15-2d4b-4f8e-a6c1-9b0d5e7f2a34
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1443 PRODUCT ASSIGNMENT: a checker table's product_id is its own assignment.

The admin GraphQL type answers product_id from the row for every table, a checker table
included: the assignment names the product whose owner controls the check, whatever it scans.
"""

from __future__ import annotations

from typing import Any, cast

from provisa.api.admin.types import RegisteredTableType

CONTRACT = "dataset: provisa/sales/orders\nchecks:\n  - row_count:\n"


def _table(**overrides: Any) -> RegisteredTableType:
    fields: dict[str, Any] = dict(
        id=2,
        source_id="dq",
        domain_id="sales",
        schema_name="quality",
        table_name="orders_scans",
        write_ops=["delete", "insert", "update"],
        alias=None,
        description=None,
        cache_ttl=None,
        replicate=None,
        region=None,
        draft=False,
        load_protected=None,
        off_peak_window=None,
        off_peak_tz=None,
        gql_naming_convention=None,
        watermark_column=None,
        columns=[],
    )
    fields.update(overrides)
    return RegisteredTableType(**fields)


def _product(table: RegisteredTableType) -> str | None:
    # strawberry types the field as its return value; call the resolver the way the schema does.
    return cast(Any, table).product_id()


def test_an_ordinary_table_answers_with_its_own_row():
    table = _table(
        id=1, source_id="wh", schema_name="sales", table_name="orders", stored_product_id="p1"
    )
    assert _product(table) == "p1"


def test_a_checker_table_answers_with_the_product_it_is_assigned_to():
    assert _product(_table(dq_contract=CONTRACT, stored_product_id="quality")) == "quality"


def test_a_checker_table_assigned_to_no_product_belongs_to_none():
    assert _product(_table(dq_contract=CONTRACT, stored_product_id=None)) is None
