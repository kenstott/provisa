# Copyright (c) 2026 Kenneth Stott
# Canary: 0f2d6b84-57ac-4e19-8c3b-71a04d5e9cb2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests: registering a checker table turns its contract into a registration (REQ-1443).

The operator declares a contract and a governance intent; everything else about the table — its
columns, its watermark, its promotions — is derived here, because the envelope belongs to the checker
rather than to whoever wrote the config.
"""

from __future__ import annotations

import pytest

from provisa.core.config_loader import _validate_dq_contracts
from provisa.core.models import Column, ProvisaConfig, Source, SourceType, Table
from provisa.dq.results import DQ_PROMOTIONS, DQ_WATERMARK_COLUMN, RESULT_FIELDS

CONTRACT = """
dataset: provisa/sales/orders
columns:
  - name: customer
    checks:
      - missing:
"""


def _checker_source(source_type: SourceType = SourceType.soda) -> Source:
    return Source(
        id="dq",
        type=source_type,
        mapping={
            "host": "localhost",
            "port": 5439,
            "database": "provisa",
            "user": "provisa",
            "password": "provisa",
        },
    )


def _results_table(contract: str | None = CONTRACT, visible_to=("analyst",)) -> Table:
    return Table(
        source_id="dq",
        domain_id="default",
        schema="quality",
        table="orders_scans",
        dq_contract=contract,
        columns=[Column(name="placeholder", data_type="varchar", visible_to=list(visible_to))],
    )


def _target_table() -> Table:
    return Table(
        source_id="warehouse",
        domain_id="default",
        schema="sales",
        table="orders",
        columns=[Column(name="id", data_type="bigint", visible_to=["analyst"])],
    )


def _config(*tables: Table, source: Source | None = None) -> ProvisaConfig:
    return ProvisaConfig(
        sources=[source or _checker_source(), Source(id="warehouse", type=SourceType.postgresql)],
        domains=[],
        tables=list(tables),
        roles=[],
    )


def test_declared_columns_are_replaced_by_the_shipped_results_schema():
    """The envelope is the CHECKER's, so a hand-written column list could only disagree with what
    lands. The declared column is read for its governance and then discarded."""
    results = _results_table()
    _validate_dq_contracts(_config(results, _target_table()))
    assert tuple(c.name for c in results.columns) == RESULT_FIELDS


def test_the_operators_governance_survives_the_replacement():
    results = _results_table(visible_to=("steward", "analyst"))
    _validate_dq_contracts(_config(results, _target_table()))
    assert all(c.visible_to == ["steward", "analyst"] for c in results.columns)


def test_scan_time_becomes_the_watermark_so_scans_append():
    """probe_type=watermark implies APPEND (REQ-982) — that is what makes the table a scan history
    without any history subsystem."""
    results = _results_table()
    _validate_dq_contracts(_config(results, _target_table()))
    assert results.watermark_column == DQ_WATERMARK_COLUMN


def test_shipped_promotions_are_seeded():
    results = _results_table()
    _validate_dq_contracts(_config(results, _target_table()))
    assert [p["target_column"] for p in results.promotions] == [
        p["target_column"] for p in DQ_PROMOTIONS
    ]


def test_an_operators_own_promotion_is_kept_and_not_duplicated():
    """The shipped set is a default, not a replacement — and re-seeding a target column the operator
    already declared would register the same generated column twice."""
    results = _results_table()
    results.promotions = [
        {
            "jsonb_column": "diagnostics",
            "field": "missing_count",
            "target_column": "missing_count",
            "target_type": "bigint",
        },
        dict(DQ_PROMOTIONS[0]),
    ]
    _validate_dq_contracts(_config(results, _target_table()))
    targets = [p["target_column"] for p in results.promotions]
    assert targets == ["missing_count", "freshness_max_timestamp", "dataset_rows_tested"]


def test_great_expectations_is_registered_the_same_way():
    """Two source types, one shape: the licence tier differs, the registration does not."""
    results = _results_table(
        contract='{"meta": {"dataset": "provisa/sales/orders"}, "expectations": []}'
    )
    config = _config(
        results, _target_table(), source=_checker_source(SourceType.great_expectations)
    )
    _validate_dq_contracts(config)
    assert tuple(c.name for c in results.columns) == RESULT_FIELDS


def test_a_contract_on_a_non_checker_source_is_rejected():
    """Its rows would come from wherever that source's loader fetched them, which the results schema
    does not describe."""
    table = _target_table()
    table.dq_contract = CONTRACT
    with pytest.raises(ValueError, match="only valid on a data-quality checker source"):
        _validate_dq_contracts(_config(table))


def test_a_checker_table_without_a_contract_is_rejected():
    with pytest.raises(ValueError, match="must carry a dq_contract"):
        _validate_dq_contracts(_config(_results_table(contract=None), _target_table()))


def test_a_checker_table_with_no_declared_column_is_rejected():
    """The columns ship, but the governance does not — visible_to has no default to fall back on."""
    results = _results_table()
    results.columns = []
    with pytest.raises(ValueError, match="carry visible_to"):
        _validate_dq_contracts(_config(results, _target_table()))


def test_disagreeing_visible_to_is_rejected_rather_than_merged():
    """One results row cannot be visible to different roles column by column when every column comes
    out of the same scan."""
    results = _results_table()
    results.columns = [
        Column(name="a", data_type="varchar", visible_to=["analyst"]),
        Column(name="b", data_type="varchar", visible_to=["steward"]),
    ]
    with pytest.raises(ValueError, match="must share one visible_to"):
        _validate_dq_contracts(_config(results, _target_table()))


def test_a_contract_naming_an_ungoverned_table_is_rejected():
    """A checker may only observe what Provisa governs (REQ-967); rows about anything else would
    describe a table with no lineage, no governance and no RLS."""
    with pytest.raises(ValueError, match="resolves to no governed table"):
        _validate_dq_contracts(_config(_results_table()))


def test_an_unparseable_contract_names_the_table_it_broke_on():
    results = _results_table(contract="dataset: [unclosed\n")
    with pytest.raises(ValueError, match="'orders_scans': .*not parseable"):
        _validate_dq_contracts(_config(results, _target_table()))


def test_a_contract_pointed_at_its_own_results_table_is_rejected():
    """A contract observes a governed table, not the scan history it produces — the scan would
    otherwise be reading rows it is in the middle of writing."""
    results = _results_table(contract="dataset: provisa/quality/orders_scans\n")
    with pytest.raises(ValueError, match="resolves to the results table itself"):
        _validate_dq_contracts(_config(results, _target_table()))
