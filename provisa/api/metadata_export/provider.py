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
    # REQ-1389: vendor-side identities captured from this publish —
    # ``{semantic_uri: (vendor_ref, physical_key)}``, where ``vendor_ref`` is the catalog's
    # own id for the asset (guid / entity UUID / asset UUID / dataset URN) and
    # ``physical_key`` is the vendor-side name-key it was published under. The publish path
    # persists these so the NEXT publish can rebind a physically re-addressed asset to the
    # same catalog entity instead of trusting the vendor's name-keyed upsert.
    bindings: dict[str, tuple[str, str]] = field(default_factory=dict)

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
        # REQ-1389: bindings captured by prior publishes, keyed by the canonical Provisa URN.
        # Loaded by the publish path before ``publish``; a provider that can rebind reads
        # them to re-address the SAME catalog entity when the vendor-side name-key changed.
        self._bindings: dict[str, tuple[str, str]] = {}
        # REQ-1912: where the engine's store publishes a view of each replica-served table, keyed
        # by the table's registered identity (source, schema, table) -> (schema, view). Handed in
        # by the publish path for a provider that declares ``needs_export_views``; a provider
        # never works the org or the replica-served decision out for itself.
        self._export_views: dict[tuple[str, str, str], tuple[str, str]] | None = None

    @property
    def stored_bindings(self) -> dict[str, tuple[str, str]]:
        """The vendor bindings captured by earlier publishes (REQ-1389).

        A property rather than a method: the port stays outbound-only — ``publish`` and
        ``health`` are its whole callable surface — and this is state the publish path
        loads in, not an operation on the catalog.
        """
        return self._bindings

    @stored_bindings.setter
    def stored_bindings(self, bindings: dict[str, tuple[str, str]]) -> None:
        self._bindings = bindings

    #: Whether the publish path must hand this provider ``export_views`` before ``publish``: true
    #: for a provider that shares or annotates objects inside the engine's own store (Snowflake
    #: Horizon), where a replica-served table is published through its export view.
    needs_export_views = False

    @property
    def export_views(self) -> dict[tuple[str, str, str], tuple[str, str]]:
        """The export view of each replica-served table, as the publish path handed it in
        (REQ-1912). Reading it before it was handed in is a wiring fault and raises: an absent
        map is never read as "no table is replica-served"."""
        if self._export_views is None:
            raise RuntimeError(
                f"metadata export provider {self.provider_name!r} was not given the export view "
                "addresses of the org it publishes (MetadataExport.export_views)"
            )
        return self._export_views

    @export_views.setter
    def export_views(self, views: dict[tuple[str, str, str], tuple[str, str]]) -> None:
        self._export_views = views

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
