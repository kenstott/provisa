# Copyright (c) 2026 Kenneth Stott
# Canary: 8f8ec523-0921-4866-889d-9a3f38256e46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Control plane data models for REQ-073 hosted SaaS deployment.

REQ-1355: org == tenant. ``Org`` here is the routing record of the registry — the org id is the
same slug that keys ``orgs`` in the platform plane, not a second identifier.
"""

# Requirements: REQ-073, REQ-1355

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class DataPlane:  # REQ-073, REQ-1355
    id: str
    org_id: str
    endpoint: str
    region: str
    active: bool


@dataclass
class Org:  # REQ-073, REQ-1355, REQ-1053
    id: str
    name: str
    data_plane_id: str
    created_at: str
    # REQ-1053: subscription tier, one of provisa.control_plane.entitlements.Tier. Required —
    # every tier-gated feature (REQ-1066/1073) resolves its entitlement from this field, and a
    # default here would entitle an org nobody priced.
    tier: str
