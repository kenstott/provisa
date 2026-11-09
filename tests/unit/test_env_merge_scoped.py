# Copyright (c) 2026 Kenneth Stott
# Canary: 68ab5264-81ad-4a64-92e5-f56e4adb101c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a merge makes of two models, whole and scoped (REQ-1490, REQ-1506).

The half of a merge that decides WHAT the target becomes is pure: two trees in, one tree out. That
is what is under test here. The half that writes it is the one apply every load goes through, and
is covered against real schemas in ``tests/integration/test_env_merge.py``.
"""

# Requirements: REQ-1490, REQ-1491, REQ-1506

from __future__ import annotations

import pytest

from provisa.core.env_merge import MergeSelectionError, _chosen, merged_tree, requirements

SOURCE_FILE = "sources/warehouse.yaml"
DOMAIN = "sales/domain.yaml"
ORDER = "sales/tables/Order.yaml"
CUSTOMER = "sales/tables/Customer.yaml"
METRIC = "metrics/revenue.yaml"


def _order(**extra) -> dict:
    return {
        "source_id": "warehouse",
        "table_name": "orders",
        "alias": "Order",
        "relationships": [{"id": "orders_customer", "target": CUSTOMER}],
        **extra,
    }


def _source() -> dict[str, dict]:
    return {
        SOURCE_FILE: {"type": "postgres"},
        DOMAIN: {"description": "revenue"},
        ORDER: _order(description="every order"),
        CUSTOMER: {"source_id": "warehouse", "table_name": "customers", "alias": "Customer"},
        METRIC: {"expression": "SUM(Order.total)"},
    }


def _merged(target: dict, selection: list[str] | None, removals: bool = False):
    source = _source()
    chosen, pulled_in = _chosen(selection, source, target, removals)
    return merged_tree(source, target, chosen, removals), pulled_in


class TestAWholeMerge:
    def test_every_object_the_source_has_the_target_gets(self):
        merged, pulled_in = _merged({}, None)
        assert merged == _source() and pulled_in == []

    def test_an_object_only_the_target_has_is_left_alone(self):
        mine = {"finance/domain.yaml": {"description": "ledgers"}}
        merged, _ = _merged(dict(mine), None)
        assert merged["finance/domain.yaml"] == mine["finance/domain.yaml"]

    def test_removals_are_a_separate_decision(self):
        mine = {"finance/domain.yaml": {"description": "ledgers"}}
        merged, _ = _merged(dict(mine), None, removals=True)
        assert "finance/domain.yaml" not in merged

    def test_a_source_row_is_never_removed_by_a_merge(self):
        # REQ-1491: its binding is the target's own, and the source not naming it is no evidence.
        mine = {"sources/local.yaml": {"type": "postgres"}}
        merged, _ = _merged(dict(mine), None, removals=True)
        assert "sources/local.yaml" in merged


class TestAScopedMerge:
    def test_only_what_was_selected_is_carried(self):
        target = {**_source(), ORDER: _order(description="old"), METRIC: {"expression": "SUM(1)"}}
        merged, pulled_in = _merged(target, [METRIC])
        assert merged[METRIC] == _source()[METRIC]
        assert merged[ORDER]["description"] == "old"  # the other change stayed behind
        assert pulled_in == []

    def test_what_the_selection_requires_and_the_target_lacks_is_pulled_in_and_named(self):
        # A metric brought without the table it reads, and that table without its source, its
        # domain and the table on the other side of its relationship, would land inconsistent.
        merged, pulled_in = _merged({}, [METRIC])
        assert pulled_in == sorted([ORDER, CUSTOMER, SOURCE_FILE, DOMAIN])
        assert set(merged) == set(_source())

    def test_what_the_target_already_holds_is_not_carried_for_the_selections_sake(self):
        target = {**_source(), CUSTOMER: {**_source()[CUSTOMER], "description": "ours"}}
        del target[METRIC]
        merged, pulled_in = _merged(target, [METRIC])
        assert pulled_in == []
        assert merged[CUSTOMER]["description"] == "ours"

    def test_a_selection_naming_nothing_either_side_holds_is_refused_by_name(self):
        with pytest.raises(MergeSelectionError, match="metrics/nope.yaml"):
            _merged({}, ["metrics/nope.yaml"])

    def test_selecting_what_the_source_lacks_is_a_removal_and_must_be_asked_for(self):
        target = {"metrics/old.yaml": {"expression": "SUM(1)"}}
        with pytest.raises(MergeSelectionError, match="removal"):
            _merged(dict(target), ["metrics/old.yaml"])
        merged, _ = _merged(dict(target), ["metrics/old.yaml"], removals=True)
        assert "metrics/old.yaml" not in merged

    def test_removals_reach_only_what_was_selected(self):
        target = {"metrics/old.yaml": {"expression": "SUM(1)"}, "metrics/keep.yaml": {"x": 1}}
        merged, _ = _merged(dict(target), ["metrics/old.yaml"], removals=True)
        assert "metrics/keep.yaml" in merged


class TestWhatAnObjectRequires:
    def test_a_table_requires_its_domain_source_and_the_tables_it_points_at(self):
        body = _order(
            glossary=[{"column_name": "total", "term": "terms/revenue.yaml"}],
            meta_links=[{"target": "meta/tables/Catalog.yaml"}],
        )
        body["relationships"].append(
            {"id": "m2m", "target": CUSTOMER, "via": "sales/tables/Link.yaml"}
        )
        assert requirements(ORDER, body, {}) == {
            DOMAIN,
            SOURCE_FILE,
            CUSTOMER,
            "sales/tables/Link.yaml",
            "terms/revenue.yaml",
            "meta/tables/Catalog.yaml",
        }

    def test_a_data_product_requires_its_domain_and_its_members(self):
        # REQ-1634: the product owns its membership, so it stands on its member tables and
        # commands; a member names no product.
        body = {
            "domain_id": "sales",
            "tables": [{"table": ORDER}, {"table": CUSTOMER}],
            "commands": [{"command_name": "refund"}],
        }
        assert requirements("data-products/orders.yaml", body, {}) == {
            DOMAIN,
            ORDER,
            CUSTOMER,
            "commands/refund.yaml",
        }

    def test_a_table_in_no_domain_requires_the_root_domain(self):
        assert "domain.yaml" in requirements("tables/Loose.yaml", {"source_id": "w"}, {})

    def test_a_metric_requires_the_tables_its_expression_reads(self):
        assert requirements(METRIC, _source()[METRIC], _source()) == {ORDER}

    def test_a_data_product_requires_its_domain(self):
        assert requirements("data-products/orders.yaml", {"domain_id": "sales"}, {}) == {DOMAIN}
