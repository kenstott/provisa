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
``stores``). A table may name one of the org's regions: its data is replicated and read only
there, and the org's other regions read it from that region's replica. A table naming none may be
copied in every region. A source may name one too, but only as the region the admin form starts
a new table of it in: it decides nothing about where data lives (REQ-1921, "a table carries its
own region; a source's region is its default").

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
    # REQ-1922: the engine kind of a store a region names as its engine (an engine-builder key).
    # Required there and nowhere else: a URL does not identify an engine kind.
    kind: str | None = None


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
        for role in config.roles:
            require_residency_grant(role.id, role.capabilities, role.residency_values, None)
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
        require_engine_kind(region.id, stores[region.engine])
        require_one_materialize_store(region)
    for what, region in named:
        if region not in selected:
            raise ValueError(
                f"{what} names region {region!r}, which the org does not select "
                f"({', '.join(selected)})"
            )
    for role in config.roles:
        require_residency_grant(role.id, role.capabilities, role.residency_values, selected)
    # A table's region is where its data lives; a source's is only a form default.
    for what, region in _named_table_regions(config):
        require_readable_elsewhere(what, region, config.regions, stores)


def require_residency_grant(
    role_id: str,
    capabilities: "list[str]",
    values: "list[str]",
    selected: "list[str] | None",
) -> None:
    """Refuse a role's data_residency grant that does not hold together (REQ-1921): the right
    exists only when the platform declares regions (``selected`` None: it declares none), a grant
    lists only the org's regions and "no region", and values are listed only with the right."""
    from provisa.security.residency import NO_REGION

    holds = "data_residency" in capabilities
    if selected is None:
        if holds or values:
            raise ValueError(
                f"role {role_id!r} holds data_residency, which exists only when the platform "
                "declares regions"
            )
        return
    if values and not holds:
        raise ValueError(
            f"role {role_id!r} lists residency values but does not hold data_residency"
        )
    unknown = sorted(set(values) - set(selected) - {NO_REGION})
    if unknown:
        raise ValueError(
            f"role {role_id!r} data_residency grant names {', '.join(unknown)}, which the org "
            f"does not select ({', '.join(selected)}, or {NO_REGION})"
        )


def require_readable_elsewhere(
    what: str, region: str, regions: "list[OrgRegion]", stores: "dict[str, StoreConfig]"
) -> None:
    """Refuse ``what`` keeping its data in ``region`` when the org's other regions cannot read it
    there (REQ-1922): they read it from that region's replica, in place, so that region's
    replicas store must be a PostgreSQL server store and every other region's engine one that
    reads another region's store (``engine_kinds.REGION_READERS``). Loaded and saved alike."""
    from provisa.core.engine_kinds import REGION_READERS

    if len(regions) < 2:
        return
    home = next(r for r in regions if r.id == region)  # the caller refused an unselected one
    store = stores[home.replicas]
    if _is_embedded_duckdb(store.url):
        raise ValueError(
            f"region {region!r} replicas store {store.id!r} is an embedded DuckDB file, which "
            f"the org's other regions cannot read; {what} keeps its data there"
        )
    if not _is_postgresql(store.url):
        raise ValueError(
            f"region {region!r} replicas store {store.id!r} is not a PostgreSQL store, the one "
            f"kind the org's other regions read in place; {what} keeps its data there"
        )
    for other in regions:
        kind = stores[other.engine].kind
        if other.id != region and kind not in REGION_READERS:
            raise ValueError(
                f"region {other.id!r} runs the {kind} engine, which cannot read another region's "
                f"replicas, and {what} keeps its data in region {region!r}"
            )


def _is_postgresql(url: str) -> bool:
    return url.split(":", 1)[0].split("+", 1)[0].lower() in ("postgresql", "postgres")


def require_one_materialize_store(region: "OrgRegion") -> None:
    """Refuse a region whose replicas and views name different stores. MAINTAINER (REQ-1922):
    one store for both for now — every engine attaches one materialize store, which keeps the
    two in schemas of their own (REQ-1912). The model keeps both fields."""
    if region.replicas != region.views:
        raise ValueError(
            f"region {region.id!r} names replicas store {region.replicas!r} and views store "
            f"{region.views!r}; a region keeps its replicas and its views in one store"
        )


def require_engine_kind(region: str, store: StoreConfig) -> None:
    """Refuse a region's engine store that names no engine kind, or one no engine is built for."""
    from provisa.core.engine_kinds import ENGINE_KINDS as kinds

    if store.kind is None:
        raise ValueError(
            f"region {region!r} engine store {store.id!r} names no engine kind; give it one of "
            f"{', '.join(sorted(kinds))}"
        )
    if store.kind not in kinds:
        raise ValueError(
            f"region {region!r} engine store {store.id!r} names engine kind {store.kind!r}, which "
            f"is not one of {', '.join(sorted(kinds))}"
        )


def _named_regions(config: "ProvisaConfig") -> list[tuple[str, str]]:
    """``(what, region)`` for every source and table that names a region."""
    out = [(f"source {s.id}", s.region) for s in config.sources if s.region is not None]
    return out + _named_table_regions(config)


def _named_table_regions(config: "ProvisaConfig") -> list[tuple[str, str]]:
    """``(what, region)`` for every table that names a region."""
    return [
        (f"table {t.source_id}/{t.schema_name}.{t.table_name}", t.region)
        for t in config.tables
        if t.region is not None
    ]
