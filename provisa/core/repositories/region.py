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


async def upsert_store(conn: "Connection", store: StoreConfig) -> None:
    """Create a store, or replace its URL."""
    model_change.name("upsert", "store", store.id)  # REQ-1524
    await conn.upsert(
        stores,
        {"id": store.id, "url": store.url, "kind": store.kind},
        index_elements=["id"],
        update_columns=["url", "kind"],
    )


async def upsert_region(conn: "Connection", region: OrgRegion) -> None:
    """Select a platform region for the org, or replace the stores it names there."""
    model_change.name("upsert", "region", region.id)  # REQ-1524
    # The save refuses what the load refuses (provisa/core/regions.py): every store the region
    # names is declared, and its engine store names an engine kind.
    from provisa.core.regions import (
        STORE_ROLES,
        require_engine_kind,
        require_one_materialize_store,
    )

    declared = {s.id: s for s in await list_stores(conn)}
    for role in STORE_ROLES:
        if getattr(region, role) not in declared:
            raise ValueError(
                f"region {region.id!r} {role} store {getattr(region, role)!r} is not declared "
                "in stores"
            )
    require_engine_kind(region.id, declared[region.engine])
    require_one_materialize_store(region)
    values = region.model_dump()
    await conn.upsert(
        org_regions,
        values,
        index_elements=["id"],
        update_columns=[k for k in values if k != "id"],
    )


async def list_regions(conn: "Connection") -> list[OrgRegion]:
    """The regions the org selects, by id."""
    rows = await conn.execute_core(select(org_regions).order_by(org_regions.c.id))
    return [OrgRegion.model_validate(dict(r._mapping)) for r in rows.fetchall()]


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
