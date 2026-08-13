# Copyright (c) 2026 Kenneth Stott
# Canary: 7e2b9c41-08d5-4a6f-9b3e-52c1af704d18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests: the data-quality contract IS the definition (REQ-1443).

Every assertion here is about reading the operator's authored artifact — no checker is installed or
invoked, which is the point of keeping the parse on Provisa's side of the subprocess boundary.
"""

from __future__ import annotations

import json

import pytest

from provisa.core.models import Table
from provisa.dq.contract import (
    CHECKERS,
    ContractError,
    build_contract,
    contract_checks,
    contract_dataset,
    dataset_parts,
    resolve_contract_target,
)

SODA_CONTRACT = """
dataset: provisa/sales/orders
columns:
  - name: id
    checks:
      - missing:
      - duplicate:
  - name: customer
    checks:
      - missing:
checks:
  - row_count:
      must_be_greater_than: 0
"""

GX_SUITE = json.dumps(
    {
        "name": "orders_suite",
        "meta": {"dataset": "provisa/sales/orders"},
        "expectations": [
            {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "customer"}},
            {"type": "expect_table_row_count_to_be_between", "kwargs": {"min_value": 1}},
        ],
    }
)


def _table(schema: str, name: str) -> Table:
    return Table(source_id="warehouse", domain_id="default", schema=schema, table=name, columns=[])


def test_soda_dataset_is_read_from_the_top_level_key():
    assert contract_dataset(SODA_CONTRACT, "soda") == "provisa/sales/orders"


def test_gx_dataset_is_read_from_meta():
    """A GX suite has no target in it — the batch is supplied at runtime — so the dataset lives in
    ``meta``, GX's own user-metadata slot, and the artifact stays a valid GX suite."""
    assert contract_dataset(GX_SUITE, "great_expectations") == "provisa/sales/orders"


def test_gx_suite_authored_as_json_parses_unchanged():
    """YAML is a superset of JSON, so the shape GX itself emits is accepted with no conversion."""
    assert contract_checks(GX_SUITE, "great_expectations")[0]["column_name"] == "customer"


def test_unknown_checker_is_rejected():
    with pytest.raises(ContractError, match="unknown data-quality checker"):
        contract_dataset(SODA_CONTRACT, "deequ")


def test_missing_dataset_is_an_error_not_an_inferred_default():
    """REQ-939: a contract that does not say what it scans is not a contract. Inferring the target
    from the table it is registered under is exactly the hand-declared lineage the design removes."""
    with pytest.raises(ContractError, match="names no dataset"):
        contract_dataset("columns: []\n", "soda")
    with pytest.raises(ContractError, match="names no dataset"):
        contract_dataset(json.dumps({"expectations": []}), "great_expectations")


@pytest.mark.parametrize("bad", ["sales/orders", "provisa/sales/orders/extra", "provisa//orders"])
def test_dataset_must_be_three_non_empty_parts(bad: str):
    with pytest.raises(ContractError, match="slash-separated"):
        contract_dataset(f"dataset: {bad}\n", "soda")


def test_unparseable_text_is_an_error():
    with pytest.raises(ContractError, match="not parseable"):
        contract_dataset("dataset: [unclosed\n", "soda")


def test_non_mapping_top_level_is_an_error():
    with pytest.raises(ContractError, match="must be a mapping"):
        contract_dataset("- just\n- a\n- list\n", "soda")


def test_dataset_parts_splits_into_source_schema_table():
    assert dataset_parts("provisa/sales/orders") == ("provisa", "sales", "orders")


def test_target_resolves_on_schema_and_table_not_the_leading_part():
    """The leading part names the pgwire ENDPOINT the checker connects through, not a Provisa source
    id, so the same governed table is reachable under whatever name the operator gave the endpoint."""
    target = _table("sales", "orders")
    tables = [_table("sales", "customers"), target, _table("hr", "orders")]
    assert resolve_contract_target("anything/sales/orders", tables) is target


def test_target_outside_the_governed_estate_is_rejected():
    with pytest.raises(ContractError, match="resolves to no governed table"):
        resolve_contract_target("provisa/sales/orders", [_table("sales", "customers")])


def test_ambiguous_target_is_rejected_rather_than_picked():
    with pytest.raises(ContractError, match="ambiguous"):
        resolve_contract_target(
            "provisa/sales/orders", [_table("sales", "orders"), _table("sales", "orders")]
        )


def test_soda_checks_flatten_dataset_and_column_levels():
    checks = contract_checks(SODA_CONTRACT, "soda")
    assert [(c["column_name"], c["check_type"]) for c in checks] == [
        ("", "row_count"),
        ("id", "missing"),
        ("id", "duplicate"),
        ("customer", "missing"),
    ]


def test_soda_check_type_is_the_single_key():
    """Soda v4 rejects a check with any other number of keys, so a builder that emitted a ``type:``
    field would produce a contract the checker refuses to run."""
    with pytest.raises(ContractError, match="exactly one key"):
        contract_checks("dataset: a/b/c\nchecks:\n  - {missing: null, duplicate: null}\n", "soda")


def test_gx_checks_carry_type_and_column():
    checks = contract_checks(GX_SUITE, "great_expectations")
    assert [(c["column_name"], c["check_type"]) for c in checks] == [
        ("customer", "expect_column_values_to_not_be_null"),
        ("", "expect_table_row_count_to_be_between"),
    ]


def test_gx_expectation_without_a_type_is_rejected():
    suite = json.dumps({"meta": {"dataset": "a/b/c"}, "expectations": [{"kwargs": {}}]})
    with pytest.raises(ContractError, match="missing its required 'type'"):
        contract_checks(suite, "great_expectations")


def test_both_shipped_checkers_are_registered():
    assert CHECKERS == {"soda", "great_expectations"}


def test_a_soda_contract_round_trips_through_the_builder():
    """The panel parses raw text into rows and serializes rows back; a check that did not survive the
    round trip would be a hand edit silently discarded by the form (REQ-1443 clause 7)."""
    rows = contract_checks(SODA_CONTRACT, "soda")
    rebuilt = build_contract("soda", "provisa/sales/orders", rows)
    assert contract_checks(rebuilt, "soda") == rows
    assert contract_dataset(rebuilt, "soda") == "provisa/sales/orders"


def test_a_gx_suite_round_trips_through_the_builder():
    rows = contract_checks(GX_SUITE, "great_expectations")
    rebuilt = build_contract("great_expectations", "provisa/sales/orders", rows)
    assert contract_checks(rebuilt, "great_expectations") == rows
    assert contract_dataset(rebuilt, "great_expectations") == "provisa/sales/orders"


def test_a_built_soda_check_keeps_its_threshold_body():
    """The threshold is the check's own body, carried verbatim rather than summarized — a builder
    that kept only the type would silently loosen every threshold it round-tripped."""
    rows = contract_checks(SODA_CONTRACT, "soda")
    rebuilt = build_contract("soda", "provisa/sales/orders", rows)
    assert "must_be_greater_than: 0" in rebuilt


def test_building_against_a_two_part_dataset_is_rejected():
    """A two-part identifier cannot name a governed (schema, table), so it is rejected at build
    rather than at the scan that would resolve it somewhere else."""
    with pytest.raises(ContractError, match="exactly 3 non-empty"):
        build_contract("soda", "provisa/orders", contract_checks(SODA_CONTRACT, "soda"))


def test_building_for_an_unknown_checker_is_rejected():
    with pytest.raises(ContractError, match="unknown data-quality checker"):
        build_contract("dbt", "provisa/sales/orders", [])


def test_a_check_row_with_no_definition_text_is_rejected():
    """The row's definition IS the authored check; an empty one would serialize a check whose body
    the operator never wrote."""
    with pytest.raises(ContractError, match="carries no definition text"):
        build_contract(
            "soda", "provisa/sales/orders", [{"column_name": "id", "check_type": "missing"}]
        )
