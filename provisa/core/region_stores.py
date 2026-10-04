# Copyright (c) 2026 Kenneth Stott
# Canary: b0092b74-3b17-4ab6-bad5-903f18407c35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Binding an org's control-plane stores in this node's region (REQ-1921, REQ-1922).

An org's MODEL store is the control plane's tenant database, shared by every region the org
selects. In each region it selects, the org names the store its operating STATE is kept in and
the store its RECORD is kept in (``org_regions.state`` / ``.record``, store ids into ``stores``).
A node serves its own region of the org, so it opens the org's state and record handles over
those two stores. An org that does not select the node's region is refused here, by name.

The one implicit region (the platform declares none) keeps all three in the control plane's
tenant database: there is no region model to name another store.
"""

# Requirements: REQ-1921, REQ-1922

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from provisa.core.database import Database, OrgStores
    from provisa.core.model_change import ModelPlane
    from provisa.core.regions import OrgRegion, StoreConfig


class StoreNotDeclared(LookupError):
    """A region names a store id the org does not declare. Load and save refuse this
    (``provisa/core/regions.py``); meeting it here means the model store was written around them."""

    def __init__(self, org_id: str, region: str, role: str, store_id: str) -> None:
        super().__init__(
            f"org {org_id!r} region {region!r} keeps its {role} in store {store_id!r}, which the "
            "org does not declare"
        )


# One engine per store URL in this process: orgs that keep their state in one database share its
# pool, the same reason the tenant engine is shared (REQ-1316).
_engines: dict[str, "Engine"] = {}
_engines_lock = threading.Lock()


def _engine_for(url: str, *, pool_size: int, max_overflow: int) -> "Engine":
    from provisa.core.database import create_engine_from_url

    with _engines_lock:
        engine = _engines.get(url)
        if engine is None:
            engine = create_engine_from_url(url, pool_size=pool_size, max_overflow=max_overflow)
            _engines[url] = engine
        return engine


def open_org_stores(
    search_path: str, model: "ModelPlane", *, model_engine: "Engine"
) -> "OrgStores":
    """The org's handles as its runtime is built, before its model is loaded.

    With no platform regions all three are bound here. With regions only the model handle is:
    which stores keep this region's state and record is in the model, so those two handles are
    bound by :func:`bind_region_stores` once it is loaded, and until then they are None — a read
    of the region's state before the model names its store fails at once rather than reaching the
    control plane's database in its place."""
    from provisa.core import process_region
    from provisa.core.database import Database, OrgStores, org_store_handles
    from provisa.core.regions import DEFAULT_REGION

    if process_region.region() == DEFAULT_REGION:
        # REQ-1922 amendment: with no platform regions there is one implicit region, and its
        # state and record are kept in the control plane's tenant database with the model.
        return org_store_handles(
            search_path,
            model,
            model_engine=model_engine,
            state_engine=model_engine,
            record_engine=model_engine,
        )
    return OrgStores(
        Database(
            model_engine, name="org-model", search_path=search_path, model=model, holds="model"
        ),
        None,
        None,
    )


async def bind_region_stores(
    org_id: str,
    env: str | None,
    stores: "OrgStores",
    *,
    pool_size: int,
    max_overflow: int,
    schema_sql: str,
    initialise: bool,
) -> "OrgStores":
    """Bind the org's state and record handles to the stores its model names for this node's
    region, laying out the org's schema in each. A no-op with no platform regions (both were bound
    by :func:`open_org_stores`)."""
    from provisa.audit.query_log import init_audit_schema
    from provisa.core import process_region
    from provisa.core.database import Database, OrgStores
    from provisa.core.db import init_schema
    from provisa.core.regions import DEFAULT_REGION
    from provisa.core.repositories.region import OrgNotInRegion, list_regions, list_stores
    from provisa.core.secrets import resolve_secrets

    region = process_region.region()
    if region == DEFAULT_REGION:
        return stores
    model_db = stores.model_db
    async with model_db.acquire() as conn:
        selected = await list_regions(conn)
        declared = {s.id: s.url for s in await list_stores(conn)}
    here = next((r for r in selected if r.id == region), None)
    if here is None:
        raise OrgNotInRegion(org_id, region, [r.id for r in selected])

    def _engine(role: str, store_id: str) -> "Engine":
        if store_id not in declared:
            raise StoreNotDeclared(org_id, region, role, store_id)
        url = resolve_secrets(declared[store_id])
        return _engine_for(url, pool_size=pool_size, max_overflow=max_overflow)

    search_path = model_db.search_path
    assert search_path is not None  # an org handle is always scoped to its schema
    tenant_db = Database(
        _engine("state", here.state), name="org-state", search_path=search_path, holds="state"
    )
    record_db = Database(
        _engine("record", here.record), name="org-record", search_path=search_path, holds="record"
    )
    # The org's schema is laid out whole in each store; each handle reads and writes only its
    # own side's tables of it (provisa/core/store_sides.py). A worker whose launch already laid
    # it out (REQ-1900, ``initialise=False``) runs no DDL.
    # The layout is written through a handle that holds no side: laying a schema out seeds rows
    # of its own (the built-in roles and domains), which no side's handle may write.
    if initialise:
        for db in {id(d.engine): d for d in (tenant_db, record_db)}.values():
            layout = Database(db.engine, name="org-layout", search_path=search_path)
            await init_schema(layout, schema_sql, org_id=org_id, env=env)
            await init_audit_schema(layout, org_id=org_id, env=env)
    return OrgStores(model_db, tenant_db, record_db)


class RegionLane(NamedTuple):
    """What an org's region names for its engine lane: the engine's kind, and either the
    coordinator endpoint (a Trino kind) or the DSN it is addressed by (every other kind); and the
    store its replicas and views are written into (one store, ``require_one_materialize_store``)."""

    kind: str
    endpoint: tuple[str, int] | None
    url: str | None
    materialize_url: str
    cache_url: str  # the org's response cache and Hot counts in this region


_ENDPOINT_KINDS = frozenset({"trino", "trino-byo"})


class RegionLaneConflict(RuntimeError):
    """An org in a region deployment whose admin-plane row also sets an engine or a store: the
    region in its model decides those, so there are two answers and neither is taken."""

    def __init__(self, org_id: str, region: str, fields: list[str]) -> None:
        super().__init__(
            f"org {org_id!r} keeps its engine and stores in region {region!r} of its model, and "
            f"its organisation row also sets {', '.join(fields)}; clear those to serve it here"
        )


def region_lane(
    org_id: str, regions: "list[OrgRegion]", stores: "list[StoreConfig]"
) -> RegionLane | None:
    """The engine and materialize store the org's model names for this node's region; None with
    no platform regions.

    ``regions`` and ``stores`` are the org's, from its model store or from the config file."""
    from sqlalchemy import make_url

    from provisa.core import process_region
    from provisa.core.regions import (
        DEFAULT_REGION,
        require_engine_kind,
        require_one_materialize_store,
    )
    from provisa.core.repositories.region import OrgNotInRegion
    from provisa.core.secrets import resolve_secrets

    region = process_region.region()
    if region == DEFAULT_REGION:
        return None
    here = next((r for r in regions if r.id == region), None)
    if here is None:
        raise OrgNotInRegion(org_id, region, [r.id for r in regions])
    declared = {s.id: s for s in stores}
    for role, store_id in (
        ("engine", here.engine),
        ("replicas", here.replicas),
        ("cache", here.cache),
    ):
        if store_id not in declared:
            raise StoreNotDeclared(org_id, region, role, store_id)
    require_one_materialize_store(here)
    materialize_url = resolve_secrets(declared[here.replicas].url)
    cache_url = resolve_secrets(declared[here.cache].url)
    store = declared[here.engine]
    require_engine_kind(region, store)
    assert store.kind is not None  # require_engine_kind refuses a store without one
    url = resolve_secrets(store.url)
    if store.kind in _ENDPOINT_KINDS:
        parsed = make_url(url)
        if parsed.host is None or parsed.port is None:
            raise ValueError(
                f"region {region!r} engine store {store.id!r} is a {store.kind} coordinator and "
                "needs a host and a port in its URL"
            )
        return RegionLane(store.kind, (parsed.host, parsed.port), None, materialize_url, cache_url)
    return RegionLane(store.kind, None, url, materialize_url, cache_url)


def refuse_lane_conflict(
    org_id: str,
    *,
    engine_kind: str | None,
    engine_url: str | None,
    external_engine: tuple[str, int] | None,
    storage_url: str | None,
) -> None:
    """Refuse an org whose admin-plane row sets an engine or a store when its region decides."""
    from provisa.core import process_region

    fields = [
        name
        for name, value in (
            ("engine_kind", engine_kind),
            ("engine_url", engine_url),
            ("external_engine", external_engine),
            ("storage_url", storage_url),
        )
        if value is not None
    ]
    if fields:
        raise RegionLaneConflict(org_id, process_region.region(), fields)


class ForeignRegion(NamedTuple):
    """Another region of the org, as a node of this region reads it (REQ-1922): the store its
    replicas are kept in (a table that names it is read from there) and its state store, where
    whether that replica is built is recorded. Read only."""

    id: str
    replicas_url: str
    state_db: "Database"


class HomeRegionUnavailable(RuntimeError):
    """A table that names another region is read from that region's replica, never live: refused
    while that replica is not built or that region's stores cannot be reached."""

    code = "query.home_region_unavailable"

    def __init__(self, table: str, region: str, why: str) -> None:
        self.table, self.region = table, region
        self.params = {"table": table, "region": region}
        super().__init__(
            f"table {table!r} is kept in region {region!r} and is read only from its replica "
            f"there, which {why}"
        )


async def bind_foreign_regions(
    org_id: str, model_db: "Database", *, pool_size: int, max_overflow: int
) -> "dict[str, ForeignRegion]":
    """The org's regions other than this node's, by id. Empty with no platform regions."""
    from provisa.core import process_region
    from provisa.core.database import Database
    from provisa.core.regions import DEFAULT_REGION
    from provisa.core.repositories.region import list_regions, list_stores
    from provisa.core.secrets import resolve_secrets

    here = process_region.region()
    if here == DEFAULT_REGION:
        return {}
    async with model_db.acquire() as conn:
        selected = await list_regions(conn)
        declared = {s.id: s.url for s in await list_stores(conn)}
    out: dict[str, ForeignRegion] = {}
    search_path = model_db.search_path
    assert search_path is not None  # an org handle is always scoped to its schema
    for region in selected:
        if region.id == here:
            continue
        for role, store_id in (("replicas", region.replicas), ("state", region.state)):
            if store_id not in declared:
                raise StoreNotDeclared(org_id, region.id, role, store_id)
        state_engine = _engine_for(
            resolve_secrets(declared[region.state]), pool_size=pool_size, max_overflow=max_overflow
        )
        out[region.id] = ForeignRegion(
            region.id,
            resolve_secrets(declared[region.replicas]),
            Database(
                state_engine, name=f"org-state-{region.id}", search_path=search_path, holds="state"
            ),
        )
    return out
