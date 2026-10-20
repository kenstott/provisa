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

import types

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


@pytest.fixture
def tiered(control_plane, monkeypatch):
    """Set an org's tier where it is stored: its row in the platform registry (REQ-1066)."""
    from sqlalchemy.dialects.postgresql import insert

    from provisa.core.schema_admin import orgs

    admin_db = control_plane.platform
    monkeypatch.setattr(
        "provisa.api.app.state", types.SimpleNamespace(admin_db=admin_db), raising=False
    )

    async def _set(tier: str | None, org_id: str = "o1") -> None:
        async with admin_db.acquire() as conn:
            await conn.execute_core(
                insert(orgs)
                .values(id=org_id, name=org_id, tier=tier)
                .on_conflict_do_update(index_elements=[orgs.c.id], set_={"tier": tier})
            )

    return _set


@pytest.mark.asyncio
async def test_premium_org_is_entitled_to_metadata_export(tiered):  # REQ-1073
    await tiered("premium")
    await require_feature("o1", Feature.METADATA_EXPORT)


@pytest.mark.asyncio
@pytest.mark.parametrize("tier", ["free", "standard"])
async def test_below_premium_is_denied_metadata_export(tiered, tier):  # REQ-1073
    await tiered(tier)
    with pytest.raises(EntitlementError) as exc:
        await require_feature("o1", Feature.METADATA_EXPORT)
    assert exc.value.org_id == "o1"
    assert exc.value.feature is Feature.METADATA_EXPORT
    assert exc.value.required is Tier.PREMIUM
    assert "o1" in str(exc.value)
    assert "metadata_export" in str(exc.value)


@pytest.mark.asyncio
async def test_an_org_with_no_tier_set_is_entitled_to_nothing(tiered):  # REQ-1066
    await tiered(None)
    with pytest.raises(EntitlementError) as exc:
        await require_feature("o1", Feature.METADATA_EXPORT)
    assert exc.value.tier is None
    assert "has no tier set" in str(exc.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("tier", ["free", "standard", "premium"])
async def test_demo_mode_exempts_every_tier(tiered, tier, monkeypatch):  # REQ-1073
    await tiered(tier)
    monkeypatch.setenv("PROVISA_DEMO", "1")
    await require_feature("o1", Feature.METADATA_EXPORT)


@pytest.mark.asyncio
async def test_demo_mode_exempts_even_unregistered_orgs(tiered, monkeypatch):  # REQ-1073
    monkeypatch.setenv("PROVISA_DEMO", "1")
    await require_feature("ghost", Feature.METADATA_EXPORT)


@pytest.mark.asyncio
async def test_unknown_tier_is_denied_not_defaulted(tiered, monkeypatch):  # REQ-1073
    # The column refuses a tier outside the vocabulary; one read from elsewhere is refused too.
    async def _platinum(_org_id):
        return "platinum"

    monkeypatch.setattr("provisa.core.org_tier.read_org_tier", _platinum)
    with pytest.raises(UnknownTierError) as exc:
        await require_feature("o1", Feature.METADATA_EXPORT)
    assert exc.value.tier == "platinum"
    assert "free" in str(exc.value)


@pytest.mark.asyncio
async def test_unregistered_org_raises_keyerror(tiered):  # REQ-1073
    with pytest.raises(KeyError):
        await require_feature("nonexistent", Feature.METADATA_EXPORT)


@pytest.mark.asyncio
async def test_the_stored_tier_refuses_a_value_outside_the_vocabulary(tiered):  # REQ-1066
    from sqlalchemy.exc import IntegrityError

    with pytest.raises(IntegrityError):
        await tiered("platinum")


def test_parse_tier_round_trips_every_tier():  # REQ-1053
    for tier in Tier:
        assert parse_tier("o1", tier.value) is tier


def test_tier_allows_is_ordered():  # REQ-1066
    assert not tier_allows(Tier.FREE, Feature.METADATA_EXPORT)
    assert not tier_allows(Tier.STANDARD, Feature.METADATA_EXPORT)
    assert tier_allows(Tier.PREMIUM, Feature.METADATA_EXPORT)


def test_min_tier_is_the_single_definition():  # REQ-1053
    # The pricing page (REQ-1053) and the runtime gate (REQ-1073) read this one mapping.
    assert min_tier(Feature.METADATA_EXPORT) is Tier.PREMIUM


def test_every_feature_declares_a_minimum_tier():  # REQ-1066
    for feature in Feature:
        assert isinstance(min_tier(feature), Tier)
