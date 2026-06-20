# Copyright (c) 2026 Kenneth Stott
# Canary: 8f8ec523-0921-4866-889d-9a3f38256e46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Control plane data models for REQ-073 hosted SaaS deployment."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class DataPlane:
    id: str
    tenant_id: str
    endpoint: str
    region: str
    active: bool


@dataclass
class Tenant:
    id: str
    name: str
    data_plane_id: str
    created_at: str
