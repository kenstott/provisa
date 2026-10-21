# Copyright (c) 2026 Kenneth Stott
# Canary: 7b2e9d40-5c18-4a63-9f07-e3d61a8c4b25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The CatalogReader port: reading a connected catalog's assets back (REQ-2122).

The export (``provider.MetadataExport``) writes to a catalog; a reader reads the same catalog
through the same connection, for a steward who takes a table's description from it, refreshes it,
imports it or locks the table to it. A reader searches the catalog's table assets and reads one
asset's descriptive metadata: its description, its columns' descriptions, its owners, tags and
glossary terms. It never reads governance.

Matching is a human pick. :func:`rank` preselects the asset already linked to the table, else the
asset for the same physical table, and orders the rest by name similarity. Nothing is matched by a
model.
"""

# Requirements: REQ-2122

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.core.models import CatalogConnection


class CatalogReaderNotAvailable(RuntimeError):  # REQ-2122
    """Raised for a catalog whose provider has no reader: it holds no table assets to read."""


@dataclass(frozen=True)
class CatalogAsset:  # REQ-2122
    """One table asset of a catalog, as a pick list shows it.

    ref: the catalog's own id for the asset (an OpenMetadata UUID, an Atlas guid, a DataHub URN).
    name: its fully qualified name as the catalog shows it.
    physical: the physical table the catalog says the asset is, outermost first and lower-cased
        (``("sales_db", "public", "orders")``): what :func:`rank` compares with the table's own.
    description: the asset's description, as the pick list previews it.
    url: where a steward opens the asset in the catalog, when the catalog has a page for it.
    """

    ref: str
    name: str
    physical: tuple[str, ...]
    description: str = ""
    url: str | None = None


@dataclass(frozen=True)
class AssetMetadata:  # REQ-2122
    """The descriptive metadata of one catalog asset, as the catalog holds it now.

    columns: each column's description, by the column's name in the catalog.
    column_tags / column_terms: each column's tags and glossary terms, by the column's name.
    owners: who the catalog names as the asset's owners. Shown, never stored.
    """

    asset: CatalogAsset
    description: str
    columns: dict[str, str] = field(default_factory=dict)
    owners: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()
    terms: tuple[str, ...] = ()
    column_tags: dict[str, tuple[str, ...]] = field(default_factory=dict)
    column_terms: dict[str, tuple[str, ...]] = field(default_factory=dict)


class CatalogReader(ABC):  # REQ-2122
    """Reads one connected catalog's table assets."""

    provider_name: str

    def __init__(self, catalog: CatalogConnection) -> None:
        self._catalog = catalog

    @abstractmethod
    async def search(self, text: str, limit: int) -> list[CatalogAsset]:
        """The catalog's table assets matching ``text``, as many as ``limit``."""
        ...

    @abstractmethod
    async def read(self, ref: str) -> AssetMetadata:
        """The asset ``ref``'s descriptive metadata as the catalog holds it now."""
        ...

    def asset_url(self, ref: str, name: str) -> str | None:
        """Where a steward opens the asset ``ref`` (named ``name``) in the catalog, or None when
        the catalog has no page for it."""
        del ref, name
        return None


_READERS: dict[str, type[CatalogReader]] = {}


def register_reader[T: type[CatalogReader]](cls: T) -> T:
    """Class decorator registering a reader under its ``provider_name``, the export's name for
    the same catalog. A reader that sets none, or a second reader for one provider, fails at
    import."""
    name = getattr(cls, "provider_name", None)
    if not name:
        raise ValueError(f"{cls.__name__} does not set provider_name")
    if name in _READERS and _READERS[name] is not cls:
        raise ValueError(
            f"catalog reader {name!r} is already registered to {_READERS[name].__name__}"
        )
    _READERS[name] = cls
    return cls


def readable_providers() -> list[str]:
    """The providers whose catalogs can be read."""
    return sorted(_READERS)


def catalog_reader(catalog: CatalogConnection) -> CatalogReader:
    """The reader for ``catalog``. A provider with none (OpenLineage, which keeps lineage events
    rather than table assets) is refused by name."""
    try:
        reader = _READERS[catalog.provider]
    except KeyError:
        raise CatalogReaderNotAvailable(
            f"catalog {catalog.id!r} ({catalog.provider}) holds no table assets to read"
        ) from None
    return reader(catalog)


@dataclass(frozen=True)
class Ranked:  # REQ-2122
    """A pick list for one table from one catalog: the preselected asset, if any, then the rest
    in the order they are offered."""

    preselected: CatalogAsset | None
    candidates: list[CatalogAsset]


def _same_physical(asset: CatalogAsset, physical: tuple[str, ...]) -> bool:
    """Whether the asset names the same physical table: its trailing parts are the table's own
    (the catalog may name more of the address, a service or a database, or fewer)."""
    theirs = tuple(p.lower() for p in asset.physical)
    ours = tuple(p.lower() for p in physical)
    width = min(len(theirs), len(ours))
    # The table and its schema at least: a bare table name is not the same physical table.
    return width >= 2 and theirs[-width:] == ours[-width:]


def rank(
    candidates: list[CatalogAsset],
    *,
    name: str,
    physical: tuple[str, ...],
    linked_ref: str | None,
) -> Ranked:
    """Order ``candidates`` for the table called ``name`` at ``physical``.

    The asset the table is linked to is preselected; else the one asset for the same physical
    table, when there is exactly one; the rest follow by name similarity, most similar first.
    """
    by_ref = {c.ref: c for c in candidates}
    preselected = by_ref.get(linked_ref) if linked_ref is not None else None
    if preselected is None:
        same = [c for c in candidates if _same_physical(c, physical)]
        preselected = same[0] if len(same) == 1 else None

    def _score(asset: CatalogAsset) -> float:
        leaf = asset.physical[-1] if asset.physical else asset.name
        return SequenceMatcher(None, name.lower(), leaf.lower()).ratio()

    rest = sorted(
        (c for c in candidates if preselected is None or c.ref != preselected.ref),
        key=lambda c: (-_score(c), c.name),
    )
    return Ranked(preselected=preselected, candidates=([preselected] if preselected else []) + rest)
