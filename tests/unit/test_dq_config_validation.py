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
from provisa.dq.registration import check_contract_target
from provisa.dq.results import DQ_PROMOTIONS, DQ_WATERMARK_COLUMN, RESULT_FIELDS
from tests.helpers import dq_contexts

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
        cache_ttl=3600,
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


def test_scan_time_becomes_the_watermark():
    """Every row of one scan carries its scan_time, the history's time column."""
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


def test_a_checker_table_declaring_its_own_product_is_rejected():
    """REQ-1443 clause 10: membership is inherited from the scanned table, so a declared one could
    only agree with the derivation or contradict it."""
    table = _results_table()
    table.product_id = "orders-product"
    with pytest.raises(ValueError, match="cannot declare product_id"):
        _validate_dq_contracts(_config(table, _target_table()))


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
    describe a table with no lineage, no governance and no RLS. Checked against the compiled
    contexts — the pgwire names the dataset carries — once the schema build has produced them."""
    contexts = dq_contexts((1, "warehouse", "sales", "sales", "customers"))
    with pytest.raises(ValueError, match="'orders_scans': .*resolves to no governed table"):
        check_contract_target(_results_table(), "provisa/sales/orders", contexts)


def test_an_unparseable_contract_names_the_table_it_broke_on():
    results = _results_table(contract="dataset: [unclosed\n")
    with pytest.raises(ValueError, match="'orders_scans': .*not parseable"):
        _validate_dq_contracts(_config(results, _target_table()))


def test_a_contract_pointed_at_its_own_results_table_is_rejected():
    """A contract observes a governed table, not the scan history it produces — the scan would
    otherwise be reading rows it is in the middle of writing."""
    results = _results_table(contract="dataset: provisa/default/orders_scans\n")
    contexts = dq_contexts((1, "dq", "default", "quality", "orders_scans"))
    with pytest.raises(ValueError, match="resolves to the results table itself"):
        check_contract_target(results, "provisa/default/orders_scans", contexts)


def test_the_table_is_registered_under_its_result_relation_with_the_name_as_alias():
    """REQ-1443 (amended 2026-10-12): the relation lives in the org's control plane beside its own
    tables, so it is named <name>_<source>_dq and the name given is what SQL and GraphQL publish."""
    results = _results_table()
    _validate_dq_contracts(_config(results, _target_table()))
    assert results.table_name == "orders_scans_dq_dq"
    assert results.alias == "orders_scans"


def test_a_table_already_under_its_relation_name_keeps_it():
    """An update carries the registered name; deriving it again must not suffix it twice."""
    results = _results_table()
    results.table_name = "orders_scans_dq_dq"
    results.alias = "orders_scans"
    _validate_dq_contracts(_config(results, _target_table()))
    assert (results.table_name, results.alias) == ("orders_scans_dq_dq", "orders_scans")


@pytest.mark.parametrize("ttl", [None, 0])
def test_a_checker_table_without_a_scan_interval_is_refused(ttl):
    """It scans every cache_ttl seconds, so it must state one."""
    results = _results_table()
    results.cache_ttl = ttl
    with pytest.raises(ValueError, match="scans every cache_ttl seconds"):
        _validate_dq_contracts(_config(results, _target_table()))


@pytest.mark.parametrize("checker", ["soda", "great_expectations"])
def test_a_checker_whose_password_the_deployment_has_not_set_loads_as_written(checker, monkeypatch):
    """A checker source made in the Sources form names its password by reference; the scan
    resolves it, so a deployment without the variable still loads its org's model."""
    from provisa.core.config_loader import parse_config_dict

    monkeypatch.delenv("PROVISA_DQ_PASSWORD", raising=False)
    mapping = {
        "host": "localhost",
        "port": 5439,
        "database": "provisa",
        "user": "org_admin",
        "password": "${env:PROVISA_DQ_PASSWORD}",
    }
    config = parse_config_dict(
        {
            "sources": [{"id": "dq", "type": checker, "mapping": mapping}],
            "domains": [],
            "tables": [],
            "roles": [],
        }
    )
    assert config.sources[0].mapping == mapping
    assert config.written.sources[0].mapping == mapping
