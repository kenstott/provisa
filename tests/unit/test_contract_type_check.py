# Copyright (c) 2026 Kenneth Stott
# Canary: 4e2a9c71-8b3d-4f05-a6e1-9d7c2b5f0e38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2158 (amended 2026-10-28, THE DECLARED LOGICAL TYPE IS PROMISED BY DEFAULT; kept by the
2026-11-27 amendment): every contract has one built-in promise check. It fails when a column's live
type no longer fits the logicalType the contract's current version declares for it (a date become a
string, a number become text), naming the column, the declared logicalType and the live type, and
passes on a change of width, length or precision within the same logical type."""

# Requirements: REQ-2158

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from provisa.core.models import OfferContract, OfferField, OfferTerms
from provisa.dq.contract_types import (
    CONTRACT_TYPE_CHECK,
    TypeMismatch,
    check_contract,
    check_types,
    fits,
    logical_type,
)

_AT = datetime(2026, 11, 27, tzinfo=UTC)


def _contract(*fields: tuple[str, str]) -> OfferContract:
    return OfferContract(
        version="1.0.0",
        terms=OfferTerms(fields=[OfferField(name=n, type=t) for n, t in fields]),
        created_at=_AT,
    )


@pytest.mark.parametrize(
    ("declared", "live"),
    [
        ("varchar(20)", "varchar(40)"),  # a longer string
        ("varchar(40)", "varchar(20)"),  # a shorter one: width is an accepted risk
        ("integer", "bigint"),  # a wider integer
        ("numeric(10,2)", "decimal(12,4)"),  # more precision
        ("numeric(12,4)", "double"),  # another number
        ("timestamp", "timestamp(3) with time zone"),
        ("date", "date"),
        ("text", "uuid"),
        ("jsonb", "map(varchar, varchar)"),
        ("text[]", "array(varchar)"),
        ("numeric", "integer"),  # every integer is a number
    ],
)
def test_a_live_type_within_the_declared_logical_type_passes(declared, live):
    result = check_types([("c", declared)], {"c": live})
    assert result.outcome == "pass"
    assert result.mismatches == []


@pytest.mark.parametrize(
    ("declared", "live", "logical"),
    [
        ("date", "varchar", "date"),  # a date become a string
        ("numeric(10,2)", "text", "number"),  # a number become text
        ("integer", "decimal(10,2)", "integer"),  # an integer that may now hold fractions
        ("timestamp", "date", "timestamp"),
        ("boolean", "integer", "boolean"),
        ("text", "varbinary", "string"),  # a live type no logical type names
    ],
)
def test_a_live_type_outside_the_declared_logical_type_fails_by_name(declared, live, logical):
    result = check_types([("placed_on", declared)], {"placed_on": live})
    assert result.outcome == "fail"
    assert result.mismatches == [TypeMismatch(column="placed_on", declared=logical, live=live)]
    assert result.mismatches[0].message == (
        f"column placed_on: declared logicalType {logical}, live type {live}"
    )


def test_a_column_gone_from_the_source_fails_as_absent():
    result = check_types([("id", "integer"), ("note", "text")], {"id": "integer"})
    assert result.outcome == "fail"
    assert result.mismatches == [TypeMismatch(column="note", declared="string", live=None)]
    assert result.mismatches[0].message == "column note: declared logicalType string, live absent"


def test_a_column_with_no_declared_logical_type_promises_none():
    """A type no ODCS logical type names is published by its physical name alone (no logicalType),
    so the check has nothing to hold it to."""
    assert check_types([("shape", "geometry")], {"shape": "varchar"}).outcome == "pass"


def test_every_failing_column_is_named_in_field_order():
    result = check_types(
        [("a", "date"), ("b", "integer"), ("c", "boolean")],
        {"a": "varchar", "b": "bigint", "c": "varchar"},
    )
    assert [m.column for m in result.mismatches] == ["a", "c"]


def test_a_contract_is_checked_against_its_current_versions_fields():
    contract = _contract(("order_id", "integer"), ("placed_on", "date"))
    result = check_contract(contract, {"order_id": "bigint", "placed_on": "varchar(10)"})
    assert result.name == CONTRACT_TYPE_CHECK
    assert result.outcome == "fail"
    assert result.mismatches == [
        TypeMismatch(column="placed_on", declared="date", live="varchar(10)")
    ]
    assert check_contract(contract, {"order_id": "bigint", "placed_on": "date"}).outcome == "pass"


def test_fits_reads_logical_types():
    assert logical_type("varchar(20)") == "string"
    assert fits("number", "integer")
    assert not fits("integer", "double")
    assert not fits("string", None)
