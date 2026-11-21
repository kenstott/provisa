# Copyright (c) 2026 Kenneth Stott
# Canary: 38c43012-c2c2-42b1-9fd9-3dceb57e7a70
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2131 (amended 77da388aa6): a masked value keeps its column's type.

A column or output whose type is not text (numeric, boolean, date and time, binary) and is masked
by a constant mask reads as NULL; a text column keeps its constant mask.
"""

from __future__ import annotations

import pytest
import sqlglot.expressions as exp

from provisa.security.masking import (
    MaskingRule,
    MaskType,
    apply_mask_to_value,
    mask_expression,
    resolve,
)

_STARS = MaskingRule(mask_type=MaskType.constant, value="***")

_NON_TEXT = {
    "numeric": ["integer", "bigint", "smallint", "tinyint", "double", "real", "decimal(10,2)"],
    "boolean": ["boolean"],
    "date and time": ["date", "time", "timestamp", "timestamp with time zone"],
    "binary": ["varbinary", "bytea"],
}
_TEXT = ["varchar", "varchar(100)", "char(3)", "text", "character varying", "VARCHAR"]


def _family_types() -> list[tuple[str, str]]:
    return [(family, t) for family, types in _NON_TEXT.items() for t in types]


def _expr(rule: MaskingRule, data_type: str) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    return mask_expression(rule, exp.column("c", table="t0", quoted=True), data_type)


@pytest.mark.parametrize(("family", "data_type"), _family_types())
def test_a_constant_mask_on_a_non_text_column_reads_null(family: str, data_type: str) -> None:
    assert isinstance(_expr(_STARS, data_type), exp.Null), (family, data_type)


@pytest.mark.parametrize(("family", "data_type"), _family_types())
def test_a_constant_mask_on_a_non_text_value_reads_none(family: str, data_type: str) -> None:
    assert apply_mask_to_value(_STARS, 42, data_type) is None, (family, data_type)


@pytest.mark.parametrize("data_type", _TEXT)
def test_a_text_column_keeps_its_constant_mask(data_type: str) -> None:
    assert _expr(_STARS, data_type).sql(dialect="postgres") == "'***'"
    assert apply_mask_to_value(_STARS, "secret", data_type) == "***"


def test_a_numeric_constant_on_a_text_column_reads_as_text() -> None:
    zero = MaskingRule(mask_type=MaskType.constant, value=0)
    assert _expr(zero, "varchar").sql(dialect="postgres") == "'0'"
    assert apply_mask_to_value(zero, "secret", "varchar") == "0"


def test_a_reader_holding_no_entry_reads_a_float_output_as_null() -> None:
    """The complete mask of a reader no entry matches keeps the type too (REQ-2059)."""
    masks = [{"to": ["steward"], "mask": None}]
    rule = resolve(masks, ["analyst"], "double")
    assert rule is not None
    assert isinstance(_expr(rule, "double"), exp.Null)
