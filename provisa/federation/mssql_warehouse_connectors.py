# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Fabric / Synapse object/lake ATTACH connectors — zero-copy external links via ``OPENROWSET``.

Microsoft Fabric Warehouse and Azure Synapse read object/lake data on OneLake / ADLS IN PLACE via
``OPENROWSET(BULK '<url>', FORMAT='PARQUET'|'CSV'|'DELTA')``. The runtime exposes each as a view over
OPENROWSET (SCAN — no landing). The connector's ``engine`` is set per instance so the same shapes
serve both the ``fabric`` and ``synapse`` engines. Access uses the caller's Azure AD identity
(OneLake), or an ADLS credential the source carries; nothing is guessed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from provisa.federation.connector_base import Capability, Connector, Mechanism

if TYPE_CHECKING:
    from provisa.core.models import Source

# source_type → OPENROWSET FORMAT. Native BULK formats both warehouses read directly.
_OPENROWSET_FORMAT = {
    "parquet": "PARQUET",
    "csv": "CSV",
    "delta_lake": "DELTA",
}

# Fabric-only: OneLake virtualizes an Iceberg table (shortcut-provisioned, or already in OneLake) as
# Delta metadata, so the SQL endpoint reads it via OPENROWSET FORMAT='DELTA'. Synapse serverless has
# no OneLake virtualization, so Iceberg is NOT attachable there — it lands as a REPLICA instead.
_FABRIC_ONLY_FORMAT = {
    "iceberg": "DELTA",
}


def _format_for(source_type: str) -> str:
    if source_type in _OPENROWSET_FORMAT:
        return _OPENROWSET_FORMAT[source_type]
    return _FABRIC_ONLY_FORMAT[source_type]  # only reached for fabric-only types (iceberg → DELTA)


class _OpenrowsetLinkConnector(Connector):
    """A Fabric/Synapse external link over OneLake/ADLS via ``OPENROWSET`` (ATTACH_R → SCAN)."""

    mechanism = Mechanism.SCAN  # OPENROWSET reads the object/lake in place — no copy (REQ-951)

    def __init__(self, engine: str, source_type: str, key: str) -> None:
        self.engine = engine
        self.source_type = source_type
        self.key = key

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True, write=False)

    def details(self, source: "Source") -> dict:
        return {
            "format": _format_for(self.source_type),
            "location": getattr(source, "path", None),  # https OneLake / abfss ADLS URL
        }


def openrowset_link_connectors(engine: str) -> list[Connector]:
    """The Fabric/Synapse object/lake external-link connectors for ``engine`` (``fabric``|``synapse``).
    Fabric additionally attaches Iceberg (OneLake Delta virtualization); Synapse cannot, so it lands."""
    types = list(_OPENROWSET_FORMAT)
    if engine == "fabric":
        types += list(_FABRIC_ONLY_FORMAT)
    return [_OpenrowsetLinkConnector(engine, st, f"{engine}_{st}_link") for st in types]
