# Copyright (c) 2026 Kenneth Stott
# Canary: 0dc5b7b8-9a6e-4a8b-975c-39643e25efd3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A view declared under ``views:`` is a member of a data product as a table is (``product_id``),
and the quality demo's product holds gold, silver and the checks."""

# Requirements: REQ-1634

from __future__ import annotations

from pathlib import Path

from provisa.core.config_loader import views_as_tables

_QUALITY = Path(__file__).resolve().parents[2] / "demo" / "named" / "quality" / "config.yaml"


def test_a_view_names_its_data_product():
    raw = views_as_tables({"views": [{"id": "daily", "sql": "SELECT 1", "product_id": "sales"}]})
    [entry] = raw["tables"]
    assert entry["product_id"] == "sales"


def test_the_quality_demos_product_holds_gold_silver_and_the_checks():
    import yaml

    raw = views_as_tables(yaml.safe_load(_QUALITY.read_text()))
    [product] = raw["data_products"]
    members = sorted(t["table"] for t in raw["tables"] if t.get("product_id") == product["id"])
    assert members == ["view_dq_results", "view_gold", "view_silver"]
    assert product["domain_id"] == "sales"
