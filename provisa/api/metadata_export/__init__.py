# Copyright (c) 2026 Kenneth Stott
# Canary: 8066ccbb-a4cd-42f6-9cb0-13ac1153014d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Metadata egress — publishing Provisa's governance metadata to external catalogs.

REQ-1068: outbound only. Provisa is the upstream source of truth; no module under this
package reads an external catalog back into the governed configuration. The absence of any
ingest entry point is the constraint, and ``tests/unit/test_metadata_egress_outbound.py``
enforces it.
"""

# Requirements: REQ-1068

from provisa.api.metadata_egress.builder import build_snapshot
from provisa.api.metadata_egress.model import MetadataSnapshot
from provisa.api.metadata_egress.provider import (
    AssetError,
    MetadataEgress,
    MetadataEgressNotConfiguredError,
    PublishResult,
)
from provisa.api.metadata_egress.registry import (
    metadata_egress,
    register_provider,
    registered_providers,
)

# Importing the adapters is what registers them: ``metadata_egress()`` resolves a configured
# name against the registry, and a provider module nobody imported is a provider the factory
# would reject as unknown (REQ-1069).
from provisa.api.metadata_egress.atlan import AtlanEgress  # noqa: E402
from provisa.api.metadata_egress.atlas import AtlasEgress  # noqa: E402
from provisa.api.metadata_egress.collibra import CollibraEgress  # noqa: E402
from provisa.api.metadata_egress.datahub import DataHubEgress  # noqa: E402
from provisa.api.metadata_egress.openlineage import OpenLineageEgress  # noqa: E402
from provisa.api.metadata_egress.openmetadata import OpenMetadataEgress  # noqa: E402

__all__ = [
    "AssetError",
    "AtlanEgress",
    "AtlasEgress",
    "CollibraEgress",
    "DataHubEgress",
    "OpenLineageEgress",
    "OpenMetadataEgress",
    "MetadataEgress",
    "MetadataEgressNotConfiguredError",
    "MetadataSnapshot",
    "PublishResult",
    "build_snapshot",
    "metadata_egress",
    "register_provider",
    "registered_providers",
]
