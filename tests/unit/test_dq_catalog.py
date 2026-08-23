# Copyright (c) 2026 Kenneth Stott
# Canary: 3e7b18c4-92da-4f60-b5a1-6c04e9d72f83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What the contract builder's picker may offer, and how one check is written down (REQ-1443).

The picker holds no vocabulary of its own, so these are the tests that stand in for it: a check is
offered only where the checker can really run it, and a check the editors compose has to come out as
the same text a person would have typed by hand — otherwise a builder-made contract and a
hand-authored one diverge the moment they meet the parser.
"""

# Requirements: REQ-1443

from __future__ import annotations

import json

import pytest
import yaml

from provisa.dq.catalog import build_check_definition, check_catalog, checks_for_column
from provisa.dq.contract import ContractError, contract_checks


def _types(kinds):
    return [kind.check_type for kind in kinds]


def test_an_unknown_checker_raises_rather_than_offering_nothing():
    """An empty catalog renders as "this checker has no checks", which is a different claim."""
    with pytest.raises(ContractError, match="unknown data-quality checker"):
        check_catalog("deequ")


def test_a_numeric_column_is_offered_the_numeric_checks_and_not_the_text_ones():
    offered = _types(checks_for_column("great_expectations", "bigint"))
    assert "expect_column_mean_to_be_between" in offered
    assert "expect_column_values_to_match_regex" not in offered


def test_a_text_column_is_offered_the_text_checks_and_not_the_numeric_ones():
    offered = _types(checks_for_column("great_expectations", "text"))
    assert "expect_column_value_lengths_to_be_between" in offered
    assert "expect_column_mean_to_be_between" not in offered


def test_a_column_with_no_resolved_type_gets_only_the_type_agnostic_checks():
    """data_type is resolved at registration and never backfilled (REQ-846), so a null one means the
    column genuinely has no type — not that every check applies."""
    offered = _types(checks_for_column("great_expectations", None))
    assert "expect_column_values_to_not_be_null" in offered
    assert "expect_column_values_to_be_between" not in offered


def test_dataset_checks_are_never_offered_for_a_column():
    offered = _types(checks_for_column("soda", "bigint"))
    assert "row_count" not in offered
    assert "freshness" not in offered


def test_a_soda_check_builds_the_text_a_person_would_have_typed():
    definition = build_check_definition(
        "soda",
        "missing",
        column_name="customer",
        comparator="must_be_less_than",
        threshold_value=5,
        metric="percent",
        level="warn",
    )
    # The args alone: the key naming the check type is soda's envelope, restated by the contract
    # serializer (REQ-1443 clause 7), so it is not in the row the panel edits.
    assert yaml.safe_load(definition) == {
        "threshold": {"metric": "percent", "must_be_less_than": 5, "level": "warn"}
    }


def test_a_fail_level_is_left_unwritten_because_soda_already_defaults_to_it():
    """Emitting it would rewrite the threshold block of every hand-authored contract on first save."""
    definition = build_check_definition(
        "soda", "row_count", comparator="must_be_greater_than", threshold_value=0, level="fail"
    )
    assert yaml.safe_load(definition) == {"threshold": {"must_be_greater_than": 0}}


def test_a_gx_expectation_carries_its_column_in_kwargs():
    definition = build_check_definition(
        "great_expectations",
        "expect_column_values_to_be_in_set",
        column_name="tier",
        params={"value_set": ["gold", "silver"], "mostly": 0.99},
    )
    # The kwargs alone — GX's type/kwargs envelope is the serializer's, not the row's.
    assert json.loads(definition) == {
        "column": "tier",
        "value_set": ["gold", "silver"],
        "mostly": 0.99,
    }


def test_a_built_check_is_the_same_object_the_parser_reads_back():
    """The builder is a view of the raw text, so a built check has to survive the round trip that a
    typed one does — a definition only the builder understands is a second dialect."""
    definition = build_check_definition(
        "soda",
        "duplicate",
        column_name="order_id",
        comparator="must_be",
        threshold_value=0,
    )
    contract = yaml.safe_dump(
        {
            "dataset": "provisa/sales/orders",
            "columns": [
                {"name": "order_id", "checks": [{"duplicate": yaml.safe_load(definition)}]}
            ],
        }
    )
    assert [(c["column_name"], c["check_type"]) for c in contract_checks(contract, "soda")] == [
        ("order_id", "duplicate")
    ]


def test_gx_refuses_a_warn_because_an_expectation_has_no_such_outcome():
    with pytest.raises(ContractError, match="severity"):
        build_check_definition(
            "great_expectations",
            "expect_column_values_to_not_be_null",
            column_name="customer",
            level="warn",
        )


def test_gx_refuses_a_threshold_block_because_its_kwargs_are_the_bound():
    with pytest.raises(ContractError, match="carry no threshold block"):
        build_check_definition(
            "great_expectations",
            "expect_column_values_to_not_be_null",
            column_name="customer",
            comparator="must_be",
            threshold_value=0,
        )


def test_a_comparator_with_no_value_is_an_error_rather_than_a_check_that_never_fails():
    with pytest.raises(ContractError, match="no value to compare against"):
        build_check_definition("soda", "row_count", comparator="must_be_greater_than")


def test_a_value_with_no_comparator_is_an_error_too():
    with pytest.raises(ContractError, match="no comparator"):
        build_check_definition("soda", "row_count", threshold_value=10)


def test_a_column_scoped_check_that_names_no_column_is_refused():
    with pytest.raises(ContractError, match="names no column"):
        build_check_definition("soda", "missing")


def test_a_dataset_scoped_check_that_names_a_column_is_refused():
    with pytest.raises(ContractError, match="takes no column"):
        build_check_definition("soda", "row_count", column_name="customer")


def test_an_unknown_parameter_is_refused_rather_than_dropped():
    """Both parsers reject bodies they do not recognize, so a dropped key builds text that fails at
    scan time instead of at build time."""
    with pytest.raises(ContractError, match="has no parameter"):
        build_check_definition(
            "soda", "duplicate", column_name="order_id", params={"valid_values": ["a"]}
        )


def test_a_required_parameter_must_be_present():
    with pytest.raises(ContractError, match="requires 'function'"):
        build_check_definition("soda", "aggregate", column_name="amount")


def test_an_enum_parameter_is_held_to_its_choices():
    with pytest.raises(ContractError, match="must be one of"):
        build_check_definition(
            "soda", "aggregate", column_name="amount", params={"function": "median"}
        )


def test_a_check_the_picker_cannot_author_says_so_and_points_at_the_raw_editor():
    """failed_rows takes hand-written SQL, so it is the raw text's business — and still authorable."""
    with pytest.raises(ContractError, match="author it in the raw contract"):
        build_check_definition("soda", "failed_rows")
