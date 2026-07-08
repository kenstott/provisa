# Copyright (c) 2026 Kenneth Stott
# Canary: 4b74f879-5356-4c0f-a4fa-6a2734b21cb3
#
# This source code is licensed under the Business Source License 1.1

"""pytest-bdd step implementations for REQ-921 - Promotion Coercion."""

from __future__ import annotations

import pytest
from pytest_bdd import given, when, then, scenarios

from provisa.api_source.promotions import generate_promotion_ddl, _PG_CAST_MAP
from provisa.api_source.models import PromotionConfig


scenarios("../features/REQ-921.feature")


@pytest.fixture
def shared_data() -> dict:
    return {}


# ---------------------------------------------------------------------------
# Given
# ---------------------------------------------------------------------------


@given("a promotion with a target_type that is in _PG_CAST_MAP")
def given_promotion_in_cast_map(shared_data: dict) -> None:
    # Pick the first mapped type that requires an actual cast suffix (not empty string).
    mapped_type = next(t for t, cast in _PG_CAST_MAP.items() if cast != "")
    promotion = PromotionConfig(
        jsonb_column="data",
        field="some.field",
        target_column="target_col",
        target_type=mapped_type,
    )
    shared_data["promotion_in_map"] = promotion
    shared_data["mapped_cast"] = _PG_CAST_MAP[mapped_type]


# ---------------------------------------------------------------------------
# When
# ---------------------------------------------------------------------------


@when("generate_promotion_ddl generates the coercion SQL")
def when_generate_ddl_mapped(shared_data: dict) -> None:
    promotion = shared_data["promotion_in_map"]
    stmts = generate_promotion_ddl("test_table", [promotion])
    assert len(stmts) == 1
    shared_data["ddl_mapped"] = stmts[0]


@when("a promotion with a target_type not in _PG_CAST_MAP")
def when_promotion_not_in_cast_map(shared_data: dict) -> None:
    unmapped_type = "uuid"
    assert unmapped_type not in _PG_CAST_MAP, (
        f"{unmapped_type!r} must not be in _PG_CAST_MAP for this test to be valid"
    )
    promotion = PromotionConfig(
        jsonb_column="payload",
        field="ref.id",
        target_column="ref_id",
        target_type=unmapped_type,
    )
    stmts = generate_promotion_ddl("test_table", [promotion])
    assert len(stmts) == 1
    shared_data["ddl_unmapped"] = stmts[0]
    shared_data["unmapped_type"] = unmapped_type


# ---------------------------------------------------------------------------
# Then
# ---------------------------------------------------------------------------


@then("the output includes an explicit CAST expression")
def then_output_has_cast(shared_data: dict) -> None:
    ddl = shared_data["ddl_mapped"]
    cast_suffix = shared_data["mapped_cast"]
    assert cast_suffix, "Expected a non-empty cast suffix for a mapped type"
    assert cast_suffix in ddl, f"Expected cast suffix {cast_suffix!r} in DDL statement:\n{ddl}"
    assert "::" in ddl, f"Expected '::' cast operator in DDL statement:\n{ddl}"


@then("the output omits the CAST and relies on JSONB native representation")
def then_output_omits_cast(shared_data: dict) -> None:
    ddl = shared_data["ddl_unmapped"]
    unmapped_type = shared_data["unmapped_type"]
    # The generated expression must not contain a cast back to the target type.
    # The only "::" allowed would be from any source column cast, but since
    # cast_source=False (default) there should be no "::" at all in the
    # generated expression portion.
    # Extract the GENERATED ALWAYS AS (...) clause to inspect just that part.
    start = ddl.index("GENERATED ALWAYS AS (") + len("GENERATED ALWAYS AS (")
    end = ddl.index(") STORED")
    generated_expr = ddl[start:end]
    assert f"::{unmapped_type}" not in generated_expr, (
        f"Unexpected cast to {unmapped_type!r} found in generated expression: {generated_expr!r}"
    )
    assert "::" not in generated_expr, (
        f"Unexpected '::' cast operator in generated expression for unmapped type: {generated_expr!r}"
    )
