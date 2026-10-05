# Copyright (c) 2026 Kenneth Stott
# Canary: 809d0493-8e83-4b65-a387-ab6f04cac80a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Repository for an org's REGIONS and the STORES they name (REQ-1921, REQ-1922).

An org selects the platform regions it uses (``org_regions``) and names, for each kind of data,
the store it is kept in there (``stores``: a connection URL, kept as a secret reference). A source
or table that names a region must name one the org selects: :func:`require_selected` is the check
the source and table writes make, against the same rows a config load writes."""

# Requirements: REQ-1921, REQ-1922

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from provisa.core import model_change
from provisa.core.regions import OrgRegion, StoreConfig
from provisa.core.repositories.origin import require as require_origin
from provisa.core.repositories.origin import take_over
from provisa.core.schema_org import org_regions, stores

if TYPE_CHECKING:
    from provisa.core.database import Connection


class RegionNotSelected(ValueError):
    """A source or table names a region its org does not select."""

    def __init__(self, what: str, region: str, selected: list[str]) -> None:
        self.what, self.region, self.selected = what, region, selected
        if selected:
            why = f"which the org does not select ({', '.join(selected)})"
        else:
            why = "but the org selects no region (the platform declares none, or none is chosen)"
        super().__init__(f"{what} names region {region!r}, {why}")


class OrgNotInRegion(LookupError):
    """A node is asked to serve an org in a region the org does not select."""

    def __init__(self, org_id: str, region: str, selected: list[str]) -> None:
        self.org_id, self.region, self.selected = org_id, region, selected
        super().__init__(
            f"org {org_id!r} does not select region {region!r}, which this node serves "
            f"(it selects {', '.join(selected) if selected else 'none'})"
        )


async def upsert_store(conn: "Connection", store: StoreConfig, *, origin: str) -> None:
    """Create a store, or replace its URL."""
    model_change.name("upsert", "store", store.id)  # REQ-1524
    require_origin(origin)
    # The save refuses what the load refuses: the data the org keeps in a region stays readable
    # by its other regions with this store as it now is.
    declared = {s.id: s for s in await list_stores(conn)}
    await _require_named_readable(conn, await list_regions(conn), {**declared, store.id: store})
    await conn.upsert(
        stores,
        {"id": store.id, "url": store.url, "kind": store.kind, "origin": origin},
        index_elements=["id"],
        update_columns=["url", "kind"],
    )
    await take_over(
        conn, stores, (stores.c.id == store.id,), kind="store", ident=store.id, origin=origin
    )


async def upsert_region(conn: "Connection", region: OrgRegion, *, origin: str) -> None:
    """Select a platform region for the org, or replace the stores it names there."""
    model_change.name("upsert", "region", region.id)  # REQ-1524
    require_origin(origin)
    # The save refuses what the load refuses (provisa/core/regions.py): every store the region
    # names is declared, and its engine store names an engine kind.
    from provisa.core.regions import (
        STORE_ROLES,
        require_engine_kind,
        require_one_materialize_store,
        require_reachable_engine,
    )
    from provisa.core import process_region

    declared = {s.id: s for s in await list_stores(conn)}
    for role in STORE_ROLES:
        if getattr(region, role) not in declared:
            raise ValueError(
                f"region {region.id!r} {role} store {getattr(region, role)!r} is not declared "
                "in stores"
            )
    require_engine_kind(region.id, declared[region.engine])
    require_reachable_engine(
        region.id, declared[region.engine], len(process_region.platform_regions())
    )
    require_one_materialize_store(region)
    regions = [r for r in await list_regions(conn) if r.id != region.id]
    await _require_named_readable(conn, [*regions, region], declared)
    values = region.model_dump()
    await conn.upsert(
        org_regions,
        {**values, "origin": origin},
        index_elements=["id"],
        update_columns=[k for k in values if k != "id"],
    )
    await take_over(
        conn,
        org_regions,
        (org_regions.c.id == region.id,),
        kind="region",
        ident=region.id,
        origin=origin,
    )


async def list_regions(conn: "Connection") -> list[OrgRegion]:
    """The regions the org selects, by id."""
    rows = await conn.execute_core(select(org_regions).order_by(org_regions.c.id))
    return [
        OrgRegion.model_validate({k: v for k, v in r._mapping.items() if k != "origin"})
        for r in rows.fetchall()
    ]


async def list_stores(conn: "Connection") -> list[StoreConfig]:
    """The stores the org's regions name, by id."""
    rows = await conn.execute_core(
        select(stores.c.id, stores.c.url, stores.c.kind).order_by(stores.c.id)
    )
    return [StoreConfig(id=r.id, url=r.url, kind=r.kind) for r in rows.fetchall()]


async def require_selected(conn: "Connection", what: str, region: str | None) -> None:
    """Refuse ``what`` (a source or table) naming a region its org does not select (REQ-1921).
    None — no region — is always allowed."""
    if region is None:
        return
    selected = [r.id for r in await list_regions(conn)]
    if region not in selected:
        raise RegionNotSelected(what, region, selected)


async def require_table_region(conn: "Connection", what: str, region: str | None) -> None:
    """Refuse a table naming a region its org does not select, or one the org's other regions
    cannot read it in (REQ-1921, REQ-1922): a table's region is where its data lives."""
    await require_selected(conn, what, region)
    if region is None:
        return
    from provisa.core.regions import require_readable_elsewhere

    declared = {s.id: s for s in await list_stores(conn)}
    require_readable_elsewhere(what, region, await list_regions(conn), declared)


async def _require_named_readable(
    conn: "Connection", regions: list[OrgRegion], declared: dict[str, StoreConfig]
) -> None:
    """Refuse a change to the org's regions or stores that leaves a table naming a region its
    other regions cannot read it in (REQ-1922; ``regions.require_readable_elsewhere``). A
    source's region is only a form default: no data lives there."""
    from provisa.core.regions import require_readable_elsewhere
    from provisa.core.schema_org import registered_tables

    if len(regions) < 2:
        return
    t = registered_tables
    named = await conn.execute_core(
        select(t.c.source_id, t.c.schema_name, t.c.table_name, t.c.region).where(
            t.c.region.isnot(None)
        )
    )
    out = [
        (f"table {r.source_id}/{r.schema_name}.{r.table_name}", r.region) for r in named.fetchall()
    ]
    selected = {r.id for r in regions}
    for what, region in out:
        if region in selected:  # a region no longer selected is held by the integrity guard
            require_readable_elsewhere(what, region, regions, declared)


async def require_serves_here(conn: "Connection", org_id: str, node_region: str) -> None:
    """Refuse serving ``org_id`` on a node in ``node_region`` when the org does not select that
    region (REQ-1922: a node serves its region of every org that selects it). The one implicit
    region (no platform regions) serves every org."""
    from provisa.core.regions import DEFAULT_REGION

    if node_region == DEFAULT_REGION:
        return
    selected = [r.id for r in await list_regions(conn)]
    if node_region not in selected:
        raise OrgNotInRegion(org_id, node_region, selected)


async def set_table_region(conn: "Connection", table_id: int, region: str | None) -> str:
    """Set (or, None, remove) the region a registered table's data lives in (REQ-1921) — the one
    place it changes after registration. Refused as a save of the table is. Returns the table's
    name."""
    from sqlalchemy import update

    from provisa.core.schema_org import registered_tables as t

    row = (
        await conn.execute_core(
            select(t.c.source_id, t.c.schema_name, t.c.table_name).where(t.c.id == table_id)
        )
    ).fetchone()
    if row is None:
        raise LookupError(f"table {table_id} is not registered")
    model_change.name("update", "table region", row.table_name)  # REQ-1524
    await require_table_region(
        conn, f"table {row.source_id}/{row.schema_name}.{row.table_name}", region
    )
    await conn.execute_core(update(t).where(t.c.id == table_id).values(region=region))
    return row.table_name


async def set_source_region(conn: "Connection", source_id: str, region: str | None) -> None:
    """Set (or, None, remove) the region the admin form starts a source's new tables in
    (REQ-1921). It moves no existing table: each carries its own."""
    from sqlalchemy import update

    from provisa.core.schema_org import sources

    model_change.name("update", "source region", source_id)  # REQ-1524
    await require_selected(conn, f"source {source_id}", region)
    done = await conn.execute_core(
        update(sources).where(sources.c.id == source_id).values(region=region)
    )
    if done.rowcount == 0:
        raise LookupError(f"source {source_id!r} is not registered")
