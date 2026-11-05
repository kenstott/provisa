# Copyright (c) 2026 Kenneth Stott
# Canary: 2f6b8c41-93d7-4e0a-b5c2-7a1e9d3f6b08
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Regions, edited in the UI (REQ-1922, amended 2026-11-05).

Two surfaces. ``/admin/platform/regions`` is the platform's regions -- each an id and the address
its nodes answer at -- edited by a platform administrator. They are a deployment key a node reads
before it opens any store, so they are written to the config file and take effect on restart.

``/admin/regions`` is the acting org's stores and the platform regions it selects with the store
it keeps each kind of data in there. A save sends the org's whole set: it is validated as a
configuration is (``regions.validate_regions``), against the platform this node runs on and the
org's sources, tables and roles, and refused naming the field before anything is written. A
store's URL is kept exactly as entered -- a ``${env:...}`` reference stays one -- and a password
in it never comes back (REQ-1575).
"""

# Requirements: REQ-1921, REQ-1922

from __future__ import annotations

import copy
import re
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Request
from pydantic import BaseModel, ValidationError

from provisa.api.admin._config_io import config_path, read_config, write_config
from provisa.api.admin._platform_guard import require_deployment_settings, require_org_settings
from provisa.api.admin.secret_redaction import redact_url_password, restore_url_password
from provisa.api.errors import ApiError
from provisa.core.regions import OrgRegion, PlatformConfig, PlatformRegion, StoreConfig

if TYPE_CHECKING:
    from provisa.core.database import Connection, Database

router = APIRouter(tags=["admin"])


# -- the platform's regions ----------------------------------------------------------------------


class PlatformRegionBody(BaseModel):
    id: str
    address: str


class PlatformRegionsBody(BaseModel):
    regions: list[PlatformRegionBody]


def _written_platform_regions(cfg: dict) -> list[dict]:
    return list((cfg.get("platform") or {}).get("regions") or [])


@router.get("/admin/platform/regions")
async def get_platform_regions(request: Request) -> dict:
    """The platform's regions as the config file writes them (a reference stays one)."""
    require_deployment_settings(request)
    return {
        "regions": [
            {"id": r.get("id"), "address": r.get("address")}
            for r in _written_platform_regions(read_config())
        ],
        "restart_required": True,
    }


@router.put("/admin/platform/regions")
async def set_platform_regions(request: Request, body: PlatformRegionsBody) -> dict:
    """Replace the platform's regions. Written to the config file; applied on restart."""
    require_deployment_settings(request)
    for at, region in enumerate(body.regions):
        try:
            PlatformRegion(id=region.id, address=region.address)
        except ValidationError as refused:
            raise _refusal(
                f"platform region {region.id!r}: {_first_error(refused)}", f"regions.{at}.id"
            )
        if not region.address.strip():
            raise _refusal(
                f"platform region {region.id!r} names no address", f"regions.{at}.address"
            )
    ids = [r.id for r in body.regions]
    if len(set(ids)) != len(ids):
        raise _refusal(f"the platform declares a region twice ({', '.join(ids)})", "regions")
    cfg = copy.deepcopy(read_config())
    kept = {r.get("id"): r for r in _written_platform_regions(cfg)}
    regions = []
    for region in body.regions:
        entry: dict[str, Any] = {"id": region.id, "address": region.address}
        # What this page does not edit -- the region's data addresses -- stays as written.
        if kept.get(region.id, {}).get("data_addresses"):
            entry["data_addresses"] = kept[region.id]["data_addresses"]
        regions.append(entry)
    platform = dict(cfg.get("platform") or {})
    platform["regions"] = regions
    cfg["platform"] = platform
    write_config(config_path(), cfg)
    return {
        "regions": [{"id": r["id"], "address": r["address"]} for r in regions],
        "restart_required": True,
    }


# -- the org's stores and regions -----------------------------------------------------------------


class OrgRegionsBody(BaseModel):
    # Validated one by one below, so a refusal names the item and field it is about.
    stores: list[dict[str, Any]]
    regions: list[dict[str, Any]]


def _model_db() -> "Database":
    from provisa.api.app import state

    if state.model_db is None:
        raise ApiError(503, "regions.database_not_connected", "Database not connected")
    return state.model_db


def _platform() -> PlatformConfig:
    """The platform this node runs on: the regions it bound at launch, as written."""
    from provisa.api.app import state

    return state.config.written.platform


def _store_view(store: StoreConfig) -> dict:
    return {"id": store.id, "url": redact_url_password(store.url), "kind": store.kind}


@router.get("/admin/regions")
async def get_org_regions(request: Request) -> dict:
    """The platform's regions, and the org's stores and the regions it selects."""
    require_org_settings(request)
    from provisa.core.engine_kinds import ENGINE_KINDS
    from provisa.core.regions import STORE_ROLES
    from provisa.core.repositories import region as region_repo

    async with _model_db().acquire() as conn:
        stores = await region_repo.list_stores(conn)
        regions = await region_repo.list_regions(conn)
    return {
        "platform_regions": [{"id": r.id, "address": r.address} for r in _platform().regions],
        "stores": [_store_view(s) for s in stores],
        "regions": [r.model_dump() for r in regions],
        "engine_kinds": sorted(ENGINE_KINDS),
        "store_roles": list(STORE_ROLES),
    }


@router.put("/admin/regions")
async def set_org_regions(request: Request, body: OrgRegionsBody) -> dict:
    """Replace the org's stores and regions with the set sent, or refuse it naming the field."""
    require_org_settings(request)
    from provisa.core.regions import validate_regions
    from provisa.core.repositories import region as region_repo

    stores = [_parsed(StoreConfig, "stores", at, raw) for at, raw in enumerate(body.stores)]
    regions = [_parsed(OrgRegion, "regions", at, raw) for at, raw in enumerate(body.regions)]
    async with _model_db().acquire() as conn:
        async with conn.transaction():
            held = {s.id: s for s in await region_repo.list_stores(conn)}
            # The form was handed each URL without its password: the same address sent back keeps it.
            stores = [
                s.model_copy(
                    update={
                        "url": restore_url_password(s.url, getattr(held.get(s.id), "url", None))
                    }
                )
                for s in stores
            ]
            try:
                validate_regions(await _as_config(conn, stores, regions))
            except ValueError as refused:
                raise _refusal(str(refused), _field_of(str(refused))) from None
            selected = {r.id for r in regions}
            declared = {s.id for s in stores}
            try:
                for store in stores:
                    await region_repo.upsert_store(conn, store)
                for gone in [
                    r.id for r in await region_repo.list_regions(conn) if r.id not in selected
                ]:
                    await region_repo.delete_region(conn, gone)
                for region in regions:
                    await region_repo.upsert_region(conn, region)
                for gone in [s for s in held if s not in declared]:
                    await region_repo.delete_store(conn, gone)
            except region_repo.RegionDeleteRefused as refused:
                field = "regions" if refused.kind == "region" else "stores"
                raise _refusal(str(refused), f"{field}.{refused.key}.id") from None
            except ValueError as refused:
                raise _refusal(str(refused), _field_of(str(refused))) from None
            stores_now = await region_repo.list_stores(conn)
            regions_now = await region_repo.list_regions(conn)
    return {
        "stores": [_store_view(s) for s in stores_now],
        "regions": [r.model_dump() for r in regions_now],
    }


def _parsed(model: type[StoreConfig] | type[OrgRegion], kind: str, at: int, raw: dict) -> Any:
    try:
        return model.model_validate(raw)
    except ValidationError as refused:
        error = refused.errors()[0]
        name = raw.get("id") if isinstance(raw.get("id"), str) and raw.get("id") else str(at)
        attr = ".".join(str(p) for p in error["loc"])
        what = "store" if kind == "stores" else "region"
        raise _refusal(f"{what} {name!r} {attr}: {error['msg']}", f"{kind}.{name}.{attr}") from None


async def _as_config(
    conn: "Connection", stores: list[StoreConfig], regions: list[OrgRegion]
) -> Any:
    """The org's model as ``validate_regions`` reads a configuration: the platform this node runs
    on, the stores and regions sent, and what the store holds that names a region -- its sources,
    its tables and its roles' residency grants -- as a configuration load gives them."""
    from sqlalchemy import select

    from provisa.core.db import STANDARD_ROLES
    from provisa.core.models import ProvisaConfig
    from provisa.core.schema_org import registered_tables, roles, sources

    source_rows = await conn.execute_core(select(sources.c.id, sources.c.region))
    t = registered_tables
    table_rows = await conn.execute_core(
        select(t.c.source_id, t.c.schema_name, t.c.table_name, t.c.region)
    )
    role_rows = await conn.execute_core(
        select(roles.c.id, roles.c.capabilities, roles.c.residency_values)
    )
    return ProvisaConfig.model_construct(
        platform=_platform(),
        stores=stores,
        regions=regions,
        sources=[SimpleNamespace(id=r.id, region=r.region) for r in source_rows.fetchall()],
        tables=[
            SimpleNamespace(
                source_id=r.source_id,
                schema_name=r.schema_name,
                table_name=r.table_name,
                region=r.region,
            )
            for r in table_rows.fetchall()
        ],
        roles=[
            SimpleNamespace(
                id=r.id,
                capabilities=list(r.capabilities or []),
                residency_values=list(r.residency_values or []),
            )
            for r in role_rows.fetchall()
            if r.id not in STANDARD_ROLES
        ],
    )


def _refusal(message: str, field: str | None) -> ApiError:
    return ApiError(422, "regions.refused", message, field=field)


def _first_error(refused: ValidationError) -> str:
    error = refused.errors()[0]
    return f"{'.'.join(str(p) for p in error['loc'])}: {error['msg']}"


# Which field of the set sent a refusal from ``validate_regions`` is about, by the words it names
# it in -- so the page opens the item and marks the field. The message names it either way.
_ROLE = r"(engine|replicas|views|cache|state|record)"
_FIELDS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(rf"^region '(\w+)' {_ROLE} store '[^']*' is not declared"), "regions.{0}.{1}"),
    (re.compile(r"^region '(\w+)' is not one of the platform's"), "regions.{0}.id"),
    (
        re.compile(r"^region '\w+' engine store '([^']*)' names (?:no )?engine kind"),
        "stores.{0}.kind",
    ),
    (re.compile(r"^region '\w+' engine store '([^']*)' is an embedded"), "stores.{0}.url"),
    (re.compile(r"^region '\w+' replicas store '([^']*)' is"), "stores.{0}.url"),
    (re.compile(r"^region '(\w+)' names replicas store"), "regions.{0}.views"),
    (re.compile(r"^region '(\w+)' runs the"), "regions.{0}.engine"),
    (re.compile(r"names region '(\w+)', which the org does not select"), "regions.{0}.id"),
    (re.compile(r"^the org selects (?:none|a region twice)"), "regions"),
    (re.compile(r"^the org declares a store id twice"), "stores"),
)


def _field_of(message: str) -> str | None:
    for pattern, field in _FIELDS:
        found = pattern.search(message)
        if found:
            return field.format(*found.groups())
    return None
