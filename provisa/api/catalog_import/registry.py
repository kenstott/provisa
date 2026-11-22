# Copyright (c) 2026 Kenneth Stott
# Canary: 516ebce6-e124-4bad-9b82-d78354416cb5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Reader resolution for catalog import (REQ-2247).

A configured catalog target resolves to its adapter through the metadata export registry, the one
place adapters are named; an adapter that does not implement ``CatalogReader`` is refused by name,
never answered with an empty glossary.
"""

# Requirements: REQ-2247

from __future__ import annotations

from typing import TYPE_CHECKING

from provisa.api.catalog_import.reader import CatalogReader, GlossaryListingUnsupported
from provisa.api.metadata_export.registry import metadata_export

if TYPE_CHECKING:
    from provisa.core.models import MetadataExportTarget


def catalog_reader(target: MetadataExportTarget) -> CatalogReader:
    """The glossary reader for one configured catalog target.

    An unknown provider is refused as ``metadata_export`` refuses it; a known one whose catalog
    Provisa does not read terms from raises ``GlossaryListingUnsupported`` naming it.
    """
    adapter = metadata_export(target)
    if not isinstance(adapter, CatalogReader):
        raise GlossaryListingUnsupported(target.provider)
    return adapter
