# Copyright (c) 2026 Kenneth Stott
# Canary: 8eb77570-b420-46ca-865a-97acc508eb19
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Catalog import: reading an external catalog's business glossary into Provisa (REQ-2247).

The inbound counterpart of ``provisa.api.metadata_export``, kept as its own port so that one stays
outbound-only (REQ-1068). A catalog adapter that can read its glossary implements
``CatalogReader`` beside ``MetadataExport``; ``catalog_reader`` resolves a configured target to it.
"""

# Requirements: REQ-2247

from provisa.api.catalog_import.reader import (
    CatalogReader,
    CatalogTerm,
    GlossaryListingUnsupported,
)
from provisa.api.catalog_import.registry import catalog_reader

__all__ = [
    "CatalogReader",
    "CatalogTerm",
    "GlossaryListingUnsupported",
    "catalog_reader",
]
