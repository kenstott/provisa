# Copyright (c) 2026 Kenneth Stott
# Canary: 3f1b8d47-6c02-4ae9-9d15-7b204ec8a361
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Atlan adapter (REQ-1069).

Atlan is built on Atlas, and its ingestion API keeps the Atlas entity envelope — so this is a
subclass over the same mapping rather than a second implementation. Three things differ, and
they are the whole of this module:

* the routes are mounted under ``/api/meta`` instead of ``/api/atlas``;
* an Atlan entity is typed by Atlan's own type set (``Table``, ``Column``, ``Database``,
  ``Connection``) rather than by Atlas's RDBMS types;
* an Atlan asset must name the ``connectionQualifiedName`` it belongs to, and its
  qualifiedName is rooted at that connection rather than suffixed with a cluster name.

Governance signals stay Atlas classifications, which Atlan calls tags but transports
identically (REQ-1071).
"""

# Requirements: REQ-1068, REQ-1069, REQ-1070, REQ-1071

from __future__ import annotations

from typing import TYPE_CHECKING

from provisa.api.metadata_export.atlas import AtlasExport, to_entities
from provisa.api.metadata_export.registry import register_provider

if TYPE_CHECKING:
    from provisa.api.metadata_export.atlas import AtlasEntity
    from provisa.api.metadata_export.model import MetadataSnapshot
    from provisa.api.metadata_export.provider import PublishResult

# Atlas type name -> Atlan type name. Atlan rejects an entity whose typeName is not in its own
# type set, and it does not ship the Atlas RDBMS types.
TYPE_MAP = {
    "rdbms_instance": "Connection",
    "rdbms_db": "Database",
    "rdbms_table": "Table",
    "rdbms_column": "Column",
    "Process": "Process",
}

# Every Atlan asset declares which source system it came from. Provisa is the governed access
# path, so that is what the assets are attributed to rather than the federated source.
CONNECTOR_NAME = "provisa"


@register_provider
class AtlanExport(AtlasExport):  # REQ-1069
    """Publish a snapshot to Atlan over its Atlas-shaped ingestion API."""

    provider_name = "atlan"

    entity_bulk_path = "/api/meta/entity/bulk"
    typedefs_path = "/api/meta/types/typedefs"
    classificationdef_path = "/api/meta/types/classificationdef/name"
    health_path = "/api/meta/types/typedefs/headers"

    def _connection_qn(self, snapshot: MetadataSnapshot) -> str:
        """Atlan's root address for everything this org publishes.

        Atlan's convention is ``<tenant>/<connector>/<name>``; the org id is what separates one
        Provisa tenant's assets from another's inside a shared Atlan workspace.
        """
        return f"default/{CONNECTOR_NAME}/{snapshot.org_id}"

    def _atlan_entities(self, snapshot: MetadataSnapshot) -> list[AtlasEntity]:
        connection_qn = self._connection_qn(snapshot)
        entities = to_entities(snapshot)
        for entity in entities:
            # A type Atlan has no equivalent for would be published under a name its API
            # rejects. Mapping is total, so an unmapped type is a wiring fault and raises here
            # rather than reaching the wire.
            entity.type_name = TYPE_MAP[entity.type_name]
            entity.attributes["connectorName"] = CONNECTOR_NAME
            if entity.kind != "instance":
                entity.attributes["connectionQualifiedName"] = connection_qn
        return entities

    async def publish(self, snapshot: MetadataSnapshot) -> PublishResult:
        return await self._publish_entities(self._atlan_entities(snapshot), snapshot)
