# Copyright (c) 2026 Kenneth Stott
# Canary: bcbb8305-30ca-4967-99dd-6df1d46dfa30
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Regions in the model (REQ-1921, REQ-1922): data residency.

The PLATFORM declares the physical regions its nodes run in (``platform.regions``: an id and the
address its nodes answer at). An ORG is a collection of regions: its model selects the platform
regions it uses and declares, for each, the stores it keeps there — the engine, the replica and
view storage, the cache, the state store and the request record (``regions``, naming entries of
``stores``). A source, or a table (overriding its source's), may name one of the org's regions:
its data is replicated and read only there, and the org's other regions read it from that
region's replica.

With no platform regions there is one implicit region (:data:`DEFAULT_REGION`) and nothing in a
model may name a region.
"""

# Requirements: REQ-1921, REQ-1922

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from provisa.core.models import ProvisaConfig

REGION_ID_PATTERN = r"^[a-z][a-z0-9]{1,39}$"

# REQ-1922 amendment (2026-10-03, "an org is a collection of regions"): a platform that declares
# no regions runs as ONE implicit region, the way single-tenant runs as the implicit "default" org.
# It is an internal value only — never shown, never configured, never accepted from a model.
DEFAULT_REGION = "default"

# A region's stores, in the order they are named.
STORE_ROLES = ("engine", "replicas", "views", "cache", "state", "record")


class PlatformRegion(BaseModel):
    """A physical region the platform runs nodes in."""

    id: str = Field(pattern=REGION_ID_PATTERN)
    address: str  # where the region's nodes answer (shown to a node started without --region)


class PlatformConfig(BaseModel):
    """What the platform declares, for every org."""

    regions: list[PlatformRegion] = Field(default_factory=list)


class StoreConfig(BaseModel):
    """A store an org's regions name by id: a connection URL (``${secret:...}`` resolved)."""

    id: str
    url: str


class OrgRegion(BaseModel):
    """One platform region an org selects, with the store it keeps each kind of data in there."""

    id: str = Field(pattern=REGION_ID_PATTERN)
    engine: str
    replicas: str
    views: str
    cache: str
    state: str
    record: str


def _is_embedded_duckdb(url: str) -> bool:
    return url.split(":", 1)[0].split("+", 1)[0].lower() == "duckdb"


def validate_regions(config: "ProvisaConfig") -> None:
    """Refuse a model whose regions do not hold together, naming what is wrong (REQ-1922)."""
    platform = [r.id for r in config.platform.regions]
    if len(set(platform)) != len(platform):
        raise ValueError(f"the platform declares a region twice ({', '.join(platform)})")
    named = _named_regions(config)
    if not platform:
        if config.regions or config.stores or named:
            raise ValueError(
                "the platform declares no regions, so the model may not select or name one"
            )
        return
    if not config.regions:
        listed = ", ".join(f"{r.id} ({r.address})" for r in config.platform.regions)
        raise ValueError(f"the org selects none of the platform's regions: {listed}")
    selected = [r.id for r in config.regions]
    if len(set(selected)) != len(selected):
        raise ValueError(f"the org selects a region twice ({', '.join(selected)})")
    stores = {s.id: s for s in config.stores}
    if len(stores) != len(config.stores):
        raise ValueError("the org declares a store id twice")
    for region in config.regions:
        if region.id not in platform:
            raise ValueError(
                f"region {region.id!r} is not one of the platform's ({', '.join(platform)})"
            )
        for role in STORE_ROLES:
            store = getattr(region, role)
            if store not in stores:
                raise ValueError(
                    f"region {region.id!r} {role} store {store!r} is not declared in stores"
                )
    for what, region in named:
        if region not in selected:
            raise ValueError(
                f"{what} names region {region!r}, which the org does not select "
                f"({', '.join(selected)})"
            )
    # A table that names a region is read from that region's replica by the org's other regions:
    # its replica store must be one their engines can attach.
    if len(selected) > 1:
        by_id = {r.id: r for r in config.regions}
        for region in sorted({r for _, r in named}):
            store = by_id[region].replicas
            if _is_embedded_duckdb(stores[store].url):
                raise ValueError(
                    f"region {region!r} replicas store {store!r} is an embedded DuckDB file, "
                    "which the org's other regions cannot read"
                )


def _named_regions(config: "ProvisaConfig") -> list[tuple[str, str]]:
    """``(what, region)`` for every source and table that names a region."""
    out = [(f"source {s.id}", s.region) for s in config.sources if s.region is not None]
    out += [
        (f"table {t.source_id}/{t.schema_name}.{t.table_name}", t.region)
        for t in config.tables
        if t.region is not None
    ]
    return out


def table_region(source_region: str | None, table_region_: str | None) -> str | None:
    """The region a table's data lives in: its own, else its source's; None for no region."""
    return table_region_ if table_region_ is not None else source_region
