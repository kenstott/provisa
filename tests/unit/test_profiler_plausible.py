# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2a5c90-e41b-4f68-92c3-b8f0a6e1d457
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A column's plausible type from its profile, name and tags (REQ-1934)."""

from __future__ import annotations

import pytest

from provisa.profiler.plausible import PLAUSIBLE_TYPES, infer
from provisa.profiler.statement import ColumnAggregates, ColumnSpec


def _text(
    name: str,
    shapes: list[tuple[str, int]],
    *,
    distinct: int,
    rows: int = 100,
    length_max: int = 20,
    values: list | None = None,
) -> ColumnAggregates:
    return ColumnAggregates(
        spec=ColumnSpec(name, "varchar", "text", name),
        non_null=rows,
        distinct=distinct,
        shapes=shapes,
        length_max=length_max,
        values=values or [],
    )


@pytest.mark.parametrize(
    "agg,expected",
    [
        (_text("contact", [("aaa@aaaaa.aaa", 90), ("aa@aaa.aa", 10)], distinct=100), "email"),
        (_text("site", [("aaaaa://aaa.aaaa.aaa", 95)], distinct=95), "url"),
        (_text("phone", [("(999) 999-9999", 100)], distinct=100), "phone"),
        (_text("zip", [("99999", 100)], distinct=60), "address_postal_code"),
        (_text("city", [("Aaaaaa", 100)], distinct=30), "address_city"),
        (_text("first_name", [("Aaaa", 100)], distinct=80), "person_name_first"),
        (_text("customer_name", [("Aaaa Aaaaa", 100)], distinct=95), "person_name_full"),
        (_text("order_ref", [("AAA-9999-99999", 100)], distinct=100), "identifier"),
        (_text("status", [("aaaaaa", 100)], distinct=4), "category"),
        (
            _text(
                "notes", [("Aaa aaaa aaa aaaa.", 50), ("Aa aaaa", 50)], distinct=100, length_max=400
            ),
            "free_text",
        ),
    ],
)
def test_text_columns(agg, expected):
    assert infer(agg, set(), 100).plausible_type == expected


def test_numeric_temporal_and_boolean_columns():
    ident = ColumnAggregates(
        ColumnSpec("customer_id", "integer", "numeric", "customer_id"),
        non_null=100,
        distinct=100,
        integers=100,
    )
    assert infer(ident, set(), 100).plausible_type == "identifier"
    amount = ColumnAggregates(
        ColumnSpec("amount", "double", "numeric", "amount"), non_null=100, distinct=80, integers=10
    )
    assert infer(amount, set(), 100).plausible_type == "numeric_distribution"
    flag = ColumnAggregates(
        ColumnSpec("flag", "integer", "numeric", "flag"),
        non_null=100,
        distinct=2,
        integers=100,
        values=[("0", 60), ("1", 40)],
    )
    assert infer(flag, set(), 100).plausible_type == "boolean"
    placed = ColumnAggregates(ColumnSpec("placed", "timestamp", "temporal", "placed"), non_null=10)
    assert infer(placed, set(), 100).plausible_type == "temporal"
    paid = ColumnAggregates(ColumnSpec("paid", "boolean", "boolean", "paid"), non_null=10)
    assert infer(paid, set(), 100).plausible_type == "boolean"


def test_tags_are_named_in_the_evidence_and_every_label_is_known():
    label = infer(_text("x", [("aaaa", 10)], distinct=90, rows=100), {"pii"}, 100)
    assert label.plausible_type in PLAUSIBLE_TYPES
    assert "tagged pii" in label.evidence


def test_an_empty_column_is_unknown():
    empty = ColumnAggregates(ColumnSpec("x", "varchar", "text", "x"), non_null=0)
    assert infer(empty, set(), 100).plausible_type == "unknown"
