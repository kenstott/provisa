# Copyright (c) 2026 Kenneth Stott
# Canary: 6a1d3e98-4f27-4b5c-8e90-2c7b5d1f4a63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2122: the pick list's order. The asset the table is linked to is preselected, else the one
asset for the same physical table; the rest follow by name similarity. Nothing is matched by a
model, and nothing is preselected on a guess."""

from __future__ import annotations

import pytest

from provisa.api.metadata_export.reader import (
    CatalogAsset,
    CatalogReaderNotAvailable,
    catalog_reader,
    rank,
    readable_providers,
)
from provisa.core.models import CatalogConnection


def _asset(ref: str, *physical: str) -> CatalogAsset:
    return CatalogAsset(ref=ref, name=".".join(physical), physical=physical)


ORDERS = _asset("o", "prod_pg", "sales_db", "public", "orders")
ORDERS_ARCHIVE = _asset("oa", "prod_pg", "sales_db", "archive", "orders")
ORDER_LINES = _asset("ol", "prod_pg", "sales_db", "public", "order_lines")
CUSTOMERS = _asset("c", "prod_pg", "sales_db", "public", "customers")


def test_the_asset_for_the_same_physical_table_is_preselected():
    ranked = rank(
        [CUSTOMERS, ORDER_LINES, ORDERS],
        name="orders",
        physical=("sales_db", "public", "orders"),
        linked_ref=None,
    )
    assert ranked.preselected == ORDERS
    assert [a.ref for a in ranked.candidates] == ["o", "ol", "c"]


def test_the_physical_match_ignores_case_and_a_shorter_address():
    ranked = rank(
        [CUSTOMERS, ORDERS], name="orders", physical=("PUBLIC", "Orders"), linked_ref=None
    )
    assert ranked.preselected == ORDERS


def test_a_table_name_alone_is_not_the_same_physical_table():
    # Two schemas hold an orders table: the schema decides, and a bare name preselects nothing.
    ranked = rank([ORDERS, ORDERS_ARCHIVE], name="orders", physical=("orders",), linked_ref=None)
    assert ranked.preselected is None


def test_two_assets_for_the_same_address_preselect_neither():
    twin = _asset("o2", "other_pg", "sales_db", "public", "orders")
    ranked = rank(
        [ORDERS, twin], name="orders", physical=("sales_db", "public", "orders"), linked_ref=None
    )
    assert ranked.preselected is None
    assert {a.ref for a in ranked.candidates} == {"o", "o2"}


def test_the_linked_asset_is_preselected_over_the_physical_match():
    ranked = rank(
        [ORDERS, ORDERS_ARCHIVE],
        name="orders",
        physical=("sales_db", "public", "orders"),
        linked_ref="oa",
    )
    assert ranked.preselected == ORDERS_ARCHIVE
    assert ranked.candidates[0] == ORDERS_ARCHIVE


def test_the_rest_follow_by_name_similarity():
    ranked = rank(
        [CUSTOMERS, ORDER_LINES], name="orders", physical=("x", "orders"), linked_ref=None
    )
    assert [a.ref for a in ranked.candidates] == ["ol", "c"]


def test_a_catalog_with_no_table_assets_has_no_reader():
    assert "openmetadata" in readable_providers()
    lineage = CatalogConnection(
        id="lineage", provider="openlineage", endpoint="http://marquez", domains=["sales"]
    )
    with pytest.raises(CatalogReaderNotAvailable, match="holds no table assets"):
        catalog_reader(lineage)
