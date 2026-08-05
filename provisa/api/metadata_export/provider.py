# Copyright (c) 2026 Kenneth Stott
# Canary: 2016ca22-f3fb-4425-b57a-9e32067610c7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The MetadataExport port (REQ-1068).

Mirrors the provider pattern already in the tree: an abstract base with a stable
``provider_name`` (``provisa/auth/models.py`` ``AuthProvider``) resolved by a factory that
refuses an unknown name at construction (``provisa/core/mail.py`` ``email_sender``).

Outbound only. The port declares ``publish`` and ``health`` and nothing that reads.
"""

# Requirements: REQ-1068, REQ-1069

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.api.metadata_export.model import AssetRef, MetadataSnapshot
    from provisa.core.models import MetadataExportConfig


class MetadataExportNotConfiguredError(RuntimeError):  # REQ-1068
    """Raised when export is asked for but no usable provider is configured."""


@dataclass(frozen=True)
class AssetRefStub:  # REQ-1068
    """Addresses a target-side object that is not a Provisa asset (a tag, a service, a route).

    Satisfies the ``AssetError.asset`` contract, so a failure creating a target-side structure
    is reported the same way a failed table is instead of being dropped for lack of a ref.
    """

    name: str

    def fqn(self, separator: str = ".") -> str:
        """Match ``AssetRef.fqn``. There is one part, so the separator never applies."""
        del separator
        return self.name


@dataclass
class AssetError:  # REQ-1068
    """One asset the target catalog refused, and why."""

    asset: AssetRef | AssetRefStub
    message: str


@dataclass
class PublishResult:  # REQ-1068
    """Outcome of one publish.

    Per-asset failures are RETURNED, not swallowed: a catalog that rejects 40 of 200 columns
    has to surface as a partial publish in the admin view, and a caller that treats
    ``errors`` as empty when it is not has a visible bug rather than a quiet data gap.
    """

    provider_name: str
    published: dict[str, int] = field(default_factory=dict)
    errors: list[AssetError] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors

    def total_published(self) -> int:
        return sum(self.published.values())


class MetadataExport(ABC):  # REQ-1068
    """Abstract base for outbound metadata publication to an external catalog."""

    # Stable provider identifier ("openlineage", "openmetadata", "atlas", …). Set by each
    # concrete provider and matched against ``metadata_export.provider`` in config; a
    # provider that leaves it unset is a wiring fault, caught by the registry.
    provider_name: str

    def __init__(self, config: MetadataExportConfig) -> None:
        self._config = config

    @abstractmethod
    async def publish(self, snapshot: MetadataSnapshot) -> PublishResult:
        """Push ``snapshot`` to the external catalog."""
        ...

    @abstractmethod
    async def health(self) -> None:
        """Verify the target is reachable and the credentials are accepted.

        Returns nothing and raises on any failure — the admin UI reports the exception
        message, so a health check that returned a boolean would discard the diagnosis.
        """
        ...
