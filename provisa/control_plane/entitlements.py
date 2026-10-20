# Copyright (c) 2026 Kenneth Stott
# Canary: a92ffc1b-de8d-48f7-8e26-71a3e4a5584a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Per-org tier entitlements (REQ-1053, REQ-1066, REQ-1073).

The tier vocabulary is free/standard/premium (REQ-1053). A feature declares the LOWEST
tier that may use it; ``require_feature`` denies everything below.

Denial is the failure mode for every uncertainty here — an org whose tier string is not in
``Tier`` raises rather than resolving to a permissive default. REQ-1073 is a MUST
constraint, so a tier that cannot be identified cannot be entitled.
"""

# Requirements: REQ-1053, REQ-1066, REQ-1073

from __future__ import annotations

from enum import Enum

from provisa.core.demo import is_demo


class Tier(str, Enum):  # REQ-1053
    """Subscription tiers, ordered free < standard < premium."""

    FREE = "free"
    STANDARD = "standard"
    PREMIUM = "premium"


_TIER_RANK: dict[Tier, int] = {Tier.FREE: 0, Tier.STANDARD: 1, Tier.PREMIUM: 2}


class Feature(str, Enum):  # REQ-1066
    """Tier-gated feature keys. Call sites reference these, never bare strings."""

    METADATA_EXPORT = "metadata_export"


# REQ-1073: metadata export is an enterprise/premium entitlement, extending REQ-1066.
_MIN_TIER: dict[Feature, Tier] = {
    Feature.METADATA_EXPORT: Tier.PREMIUM,
}


class EntitlementError(RuntimeError):  # REQ-1073
    """Raised when an org's tier does not entitle it to a feature."""

    def __init__(self, org_id: str, feature: Feature, tier: str | None, required: Tier) -> None:
        held = f"is on tier {tier!r}" if tier is not None else "has no tier set"
        super().__init__(
            f"org {org_id!r} {held}; feature {feature.value!r} requires "
            f"tier {required.value!r} or higher"
        )
        self.org_id = org_id
        self.feature = feature
        self.tier = tier
        self.required = required


class UnknownTierError(RuntimeError):  # REQ-1073
    """Raised when an org carries a tier string outside the ``Tier`` vocabulary."""

    def __init__(self, org_id: str, tier: str) -> None:
        super().__init__(
            f"org {org_id!r} has unrecognised tier {tier!r}; "
            f"valid tiers are {[t.value for t in Tier]}"
        )
        self.org_id = org_id
        self.tier = tier


def parse_tier(org_id: str, tier: str) -> Tier:  # REQ-1053
    """Resolve a stored tier string to a ``Tier``, raising on anything unrecognised."""
    try:
        return Tier(tier)
    except ValueError as exc:
        raise UnknownTierError(org_id, tier) from exc


def tier_allows(tier: Tier, feature: Feature) -> bool:  # REQ-1066
    """True when ``tier`` is at or above the minimum tier for ``feature``."""
    return _TIER_RANK[tier] >= _TIER_RANK[_MIN_TIER[feature]]


def min_tier(feature: Feature) -> Tier:  # REQ-1053
    """The lowest tier entitled to ``feature`` — the same definition the pricing page reads."""
    return _MIN_TIER[feature]


async def require_feature(org_id: str, feature: Feature) -> None:  # REQ-1073, REQ-1066
    """Raise unless ``org_id`` is entitled to ``feature`` by the tier stored with it.

    The tier is the org's own row in the platform registry (``provisa.core.org_tier``, REQ-1066
    amended 2026-10-20). An org with no tier set is entitled to no tier-gated feature, and an org
    with no row raises ``KeyError``: neither is given a tier it was never set.

    Demo mode (``--demo`` → PROVISA_DEMO) is exempt by design: the demo showcases every
    feature, so tier gates are not enforced there.
    """
    if is_demo():
        return
    from provisa.core.org_tier import read_org_tier

    stored = await read_org_tier(org_id)
    if stored is None:
        raise EntitlementError(org_id, feature, None, _MIN_TIER[feature])
    tier = parse_tier(org_id, stored)
    if not tier_allows(tier, feature):
        raise EntitlementError(org_id, feature, stored, _MIN_TIER[feature])
