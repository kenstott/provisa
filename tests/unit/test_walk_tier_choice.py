# Copyright (c) 2026 Kenneth Stott
# Canary: fc433fcd-84f6-4bd9-a32d-88180c9aa5df
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2006, REQ-2068: the visitor walk serves each published demo on the tier the cloud serves it
on -- a demo whose demo.yaml names regions on a regions tier, every other demo on the plain
sandbox tier. Retail names two regions; walked on the plain tier, its template could never be
provisioned (its configuration reads each region's address and stores from the regions tier's
environment)."""

from __future__ import annotations

from pathlib import Path

import yaml

from tests.integration.regions_tier import serves_on_regions_tier

_NAMED = Path(__file__).resolve().parents[2] / "demo" / "named"


def test_a_demo_that_names_regions_is_served_on_a_regions_tier():
    assert serves_on_regions_tier(_NAMED / "retail")


def test_a_demo_that_names_no_regions_is_served_on_the_plain_tier():
    plain = [
        spec.parent
        for spec in sorted(_NAMED.glob("*/demo.yaml"))
        if yaml.safe_load(spec.read_text()).get("published") is True
        and not yaml.safe_load(spec.read_text()).get("regions")
    ]
    assert plain  # the published demos are mostly single-region
    assert not any(serves_on_regions_tier(directory) for directory in plain)


def test_the_choice_is_read_from_the_demo_yaml(tmp_path):
    (tmp_path / "demo.yaml").write_text(yaml.safe_dump({"regions": {"visitor": "us"}}))
    assert serves_on_regions_tier(tmp_path)
    (tmp_path / "demo.yaml").write_text(yaml.safe_dump({"published": True}))
    assert not serves_on_regions_tier(tmp_path)
