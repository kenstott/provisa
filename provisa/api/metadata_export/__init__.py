# Copyright (c) 2026 Kenneth Stott
# Canary: 8066ccbb-a4cd-42f6-9cb0-13ac1153014d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Metadata export — publishing Provisa's governance metadata to external catalogs.

REQ-1068: outbound only. Provisa is the upstream source of truth; no module under this
package reads an external catalog back into the governed configuration. The absence of any
ingest entry point is the constraint, and ``tests/unit/test_metadata_export_outbound.py``
enforces it.
"""

# Requirements: REQ-1068

from provisa.api.metadata_export.builder import build_snapshot
from provisa.api.metadata_export.model import MetadataSnapshot
from provisa.api.metadata_export.provider import (
    AssetError,
    MetadataExport,
    MetadataExportNotConfiguredError,
    PublishResult,
)
from provisa.api.metadata_export.registry import (
    metadata_export,
    register_provider,
    registered_providers,
)

# Importing the adapters is what registers them: ``metadata_export()`` resolves a configured
# name against the registry, and a provider module nobody imported is a provider the factory
# would reject as unknown (REQ-1069).
from provisa.api.metadata_export.atlan import AtlanExport  # noqa: E402
from provisa.api.metadata_export.atlas import AtlasExport  # noqa: E402
from provisa.api.metadata_export.collibra import CollibraExport  # noqa: E402
from provisa.api.metadata_export.datahub import DataHubExport  # noqa: E402
from provisa.api.metadata_export.openlineage import OpenLineageExport  # noqa: E402
from provisa.api.metadata_export.openmetadata import OpenMetadataExport  # noqa: E402

__all__ = [
    "AssetError",
    "AtlanExport",
    "AtlasExport",
    "CollibraExport",
    "DataHubExport",
    "OpenLineageExport",
    "OpenMetadataExport",
    "MetadataExport",
    "MetadataExportNotConfiguredError",
    "MetadataSnapshot",
    "PublishResult",
    "build_snapshot",
    "metadata_export",
    "register_provider",
    "registered_providers",
]
