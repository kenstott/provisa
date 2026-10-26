# Copyright (c) 2026 Kenneth Stott
# Canary: 8b7959da-7156-4116-b7d6-00677af614e0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The regions sandbox tier serves every demo it lists, each by a node of its visitor region
(REQ-2068).

Its config declares every listed demo's regions, with a store each; the UI server names a node for
each listed demo's visitor region; and a bundle build's seeding node is given the tier's config of
the demo's own regions alone, so building retail asks for no ledger store and banking for no eu
one.
"""

# Requirements: REQ-2068

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import yaml

_REPO = Path(__file__).resolve().parents[2]
_TIER = yaml.safe_load((_REPO / "config" / "provisa-sandbox-regions.yaml").read_text())
_LISTED = [
    d
    for d in (_REPO / "packaging/linux/demo-stores/regions-tier-bundles")
    .read_text()
    .strip()
    .split(",")
    if d
]


def _builder():
    spec = importlib.util.spec_from_file_location(
        "demo_bundle", _REPO / "scripts" / "demo_bundle.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _regions(demo: str) -> dict:
    return yaml.safe_load((_REPO / "demo/named" / demo / "demo.yaml").read_text())["regions"]


def _ui_env() -> str:
    return (_REPO / "packaging/linux/first-launch.sh").read_text()


def test_the_tier_lists_retail():
    # banking joins once its bundle builds (its setup on a node per region).
    assert sorted(_LISTED) == ["retail"]


def test_the_tier_declares_every_listed_demos_regions_with_a_store_each():
    declared = {r["id"] for r in _TIER["platform"]["regions"]}
    regions = {r["id"]: r for r in _TIER["regions"]}
    stores = {s["id"] for s in _TIER["stores"]}
    for demo in _LISTED:
        r = _regions(demo)
        for region in [r["visitor"], *r["headless"]]:
            assert region in declared and region in regions, (demo, region)
            assert regions[region]["replicas"] in stores and regions[region]["engine"] in stores


def test_each_listed_demo_has_a_node_of_its_visitor_region():
    text = _ui_env()
    demos = dict(
        pair.split("=")
        for pair in re.search(r'PROVISA_SANDBOX_REGIONS_DEMOS: "([^"]+)"', text).group(1).split(",")
    )
    nodes = re.search(r"PROVISA_SANDBOX_REGIONS_NODES:-([^}]+)}", text).group(1)
    for demo in _LISTED:
        visitor = _regions(demo)["visitor"]
        assert demos[demo] == visitor, demo
        assert f"{visitor}=https://" in nodes, demo


def test_a_seeding_node_is_given_the_tiers_config_of_its_demos_regions_alone():
    builder = _builder()
    tier = builder._tier_for(["ledger", "warehouse"])
    assert [r["id"] for r in tier["platform"]["regions"]] == ["ledger", "warehouse"]
    assert [r["id"] for r in tier["regions"]] == ["ledger", "warehouse"]
    assert {s["id"] for s in tier["stores"]} == {
        "ledger_engine",
        "warehouse_engine",
        "ledger_data",
        "warehouse_data",
        "cache",
    }
    assert "PROVISA_DEMO_EU" not in yaml.safe_dump(tier)
