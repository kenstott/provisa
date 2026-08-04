# Copyright (c) 2026 Kenneth Stott
# Canary: 73082cd8-1c3e-436f-b12f-9070c549fe7c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tier entitlement tests (REQ-1053, REQ-1066, REQ-1073)."""

from __future__ import annotations

import pytest

from provisa.control_plane.entitlements import (
    EntitlementError,
    Feature,
    Tier,
    UnknownTierError,
    min_tier,
    parse_tier,
    require_feature,
    tier_allows,
)
from provisa.control_plane.models import Org
from provisa.control_plane.store import ControlPlaneStore


def _store_with(tier: str) -> ControlPlaneStore:
    store = ControlPlaneStore()
    store.register_org(
        Org(
            id="o1",
            name="Acme",
            data_plane_id="dp1",
            created_at="2026-01-01T00:00:00+00:00",
            tier=tier,
        )
    )
    return store


def test_premium_org_is_entitled_to_metadata_egress():  # REQ-1073
    require_feature(_store_with("premium"), "o1", Feature.METADATA_EGRESS)


@pytest.mark.parametrize("tier", ["free", "standard"])
def test_below_premium_is_denied_metadata_egress(tier):  # REQ-1073
    with pytest.raises(EntitlementError) as exc:
        require_feature(_store_with(tier), "o1", Feature.METADATA_EGRESS)
    assert exc.value.org_id == "o1"
    assert exc.value.feature is Feature.METADATA_EGRESS
    assert exc.value.required is Tier.PREMIUM
    assert "o1" in str(exc.value)
    assert "metadata_egress" in str(exc.value)


def test_unknown_tier_is_denied_not_defaulted():  # REQ-1073
    with pytest.raises(UnknownTierError) as exc:
        require_feature(_store_with("platinum"), "o1", Feature.METADATA_EGRESS)
    assert exc.value.tier == "platinum"
    assert "free" in str(exc.value)


def test_unregistered_org_raises_keyerror():  # REQ-1073
    with pytest.raises(KeyError):
        require_feature(ControlPlaneStore(), "nonexistent", Feature.METADATA_EGRESS)


def test_parse_tier_round_trips_every_tier():  # REQ-1053
    for tier in Tier:
        assert parse_tier("o1", tier.value) is tier


def test_tier_allows_is_ordered():  # REQ-1066
    assert not tier_allows(Tier.FREE, Feature.METADATA_EGRESS)
    assert not tier_allows(Tier.STANDARD, Feature.METADATA_EGRESS)
    assert tier_allows(Tier.PREMIUM, Feature.METADATA_EGRESS)


def test_min_tier_is_the_single_definition():  # REQ-1053
    # The pricing page (REQ-1053) and the runtime gate (REQ-1073) read this one mapping.
    assert min_tier(Feature.METADATA_EGRESS) is Tier.PREMIUM


def test_every_feature_declares_a_minimum_tier():  # REQ-1066
    for feature in Feature:
        assert isinstance(min_tier(feature), Tier)
