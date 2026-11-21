# Copyright (c) 2026 Kenneth Stott
# Canary: 3258bda8-e63b-4e74-a2f6-8e88b1fa5ad3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The CatalogReader port (REQ-2247, TERMS SOURCED FROM CATALOGS).

Inbound: ``list_glossary`` reads a catalog's whole business glossary, so a term present only in the
catalog can become a draft Provisa term. Separate from the outbound ``MetadataExport`` port
(REQ-1068); an adapter that does both implements both.
"""

# Requirements: REQ-2247

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


class GlossaryListingUnsupported(NotImplementedError):  # REQ-2247
    """Raised when a catalog provider that cannot read its glossary is asked to list it."""


@dataclass(frozen=True)
class CatalogTerm:  # REQ-2247
    """One term of a catalog's business glossary, as the catalog holds it.

    ``ref`` is the catalog's own id for the term (an OpenMetadata entity id, a DataHub URN), kept
    as the provenance of a term made from it. ``broader`` names the terms it is a kind of in the
    catalog: OpenMetadata's parent term, DataHub's IsA (Inherits) terms. ``definition`` is None
    when the catalog holds none.
    """

    name: str
    definition: str | None
    broader: tuple[str, ...]
    ref: str


class CatalogReader(ABC):  # REQ-2247
    """Abstract base for reading an external catalog's business glossary."""

    # Stable provider identifier, the same name the adapter is configured under.
    provider_name: str

    @abstractmethod
    async def list_glossary(self) -> list[CatalogTerm]:
        """Every term of the catalog's business glossary, all pages read."""
        ...
