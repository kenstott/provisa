# Copyright (c) 2026 Kenneth Stott
# Canary: 8f8ec523-0921-4866-889d-9a3f38256e46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""In-memory control plane store for REQ-073."""

# Requirements: REQ-073, REQ-1355

from __future__ import annotations

from provisa.control_plane.models import DataPlane, Org


class ControlPlaneStore:  # REQ-073
    """In-memory store for orgs and data planes. V1: no DB persistence."""

    def __init__(self) -> None:
        self._orgs: dict[str, Org] = {}
        self._data_planes: dict[str, DataPlane] = {}

    def register_org(self, org: Org) -> None:
        self._orgs[org.id] = org

    def get_org(self, org_id: str) -> Org:  # REQ-592
        return self._orgs[org_id]

    def register_data_plane(self, dp: DataPlane) -> None:  # REQ-506
        self._data_planes[dp.id] = dp

    def get_data_plane(self, dp_id: str) -> DataPlane:  # REQ-506
        return self._data_planes[dp_id]

    def route_query(self, org_id: str) -> DataPlane:  # REQ-506
        org = self._orgs[org_id]
        dp = self._data_planes[org.data_plane_id]
        if not dp.active:
            raise ValueError(f"DataPlane {dp.id!r} for org {org_id!r} is not active")
        return dp

    def list_orgs(self) -> list[Org]:  # REQ-592
        return list(self._orgs.values())

    def list_data_planes(self) -> list[DataPlane]:  # REQ-506
        return list(self._data_planes.values())
