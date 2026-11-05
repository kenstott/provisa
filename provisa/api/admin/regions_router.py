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

The platform administrator edits regions on the Platform page. ``/admin/platform/regions`` is the
platform's regions -- each an id, a description, the purposes it serves and its data addresses
(REQ-1916) -- and the grant every new organization receives. They are a deployment key a node
reads before it opens any store, so they are written to the config file and take effect on
restart. A region's purposes explain why it exists; they change no routing, policy or validation.

``/admin/platform/orgs/{org_id}/regions`` is one org's grant: the platform regions it is granted
and the store it keeps each kind of data in there. A save sends the org's whole set: it is
validated as a configuration is (``regions.validate_regions``), against the platform this node
runs on and the org's sources, tables and roles, and refused naming the field before anything is
written. A store's URL is kept exactly as entered -- a ``${env:...}`` reference stays one -- and a
password in it never comes back (REQ-1575). A node binds an org's region lane when it builds the
org's runtime and does not rebuild it on a change here, so a change applies when nodes restart.

Removing a platform region that orgs are granted, or a grant an org holds, is refused listing the
orgs it affects unless the save confirms it.

``/admin/regions`` is the acting org's regions and stores, for its administrator to read: what
the platform granted it. The org administrator does not change them.

In a demo sandbox the platform's regions are shown read-only, to explain regions; every write
here is refused there.
"""

# Requirements: REQ-1916, REQ-1921, REQ-1922

from __future__ import annotations

import copy
import re
from contextlib import AbstractAsyncContextManager
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict, ValidationError

from provisa.api.admin._config_io import config_path, read_config, write_config
from provisa.api.admin._platform_guard import require_deployment_settings, require_org_settings
from provisa.api.admin.secret_redaction import redact_url_password, restore_url_password
from provisa.api.errors import ApiError
from provisa.core.regions import (
    PURPOSES,
    OrgRegion,
    PlatformConfig,
    PlatformRegion,
    RegionGrant,
    StoreConfig,
)

if TYPE_CHECKING:
    from provisa.core.database import Connection, Database

router = APIRouter(tags=["admin"])


# -- who may read and write ------------------------------------------------------------------------


def _in_demo_sandbox() -> bool:
    """Whether the request is a demo sandbox's (``org_ids.is_demo_sandbox``)."""
    from provisa.core.org_ids import is_demo_sandbox
    from provisa.core.request_context import current_org

    org = current_org.get()
    return org is not None and is_demo_sandbox(org)


def _refuse_in_sandbox() -> None:
    if _in_demo_sandbox():
        raise ApiError(
            403,
            "regions.sandbox_read_only",
            "regions are shown in a demo sandbox to explain them and cannot be changed there",
        )


def _require_platform_write(request: Request) -> None:
    _refuse_in_sandbox()
    require_deployment_settings(request)


# -- the platform's regions ----------------------------------------------------------------------


class PlatformRegionBody(BaseModel):
    # A field the platform region does not have -- the address it once had -- is refused.
    model_config = ConfigDict(extra="forbid")

    id: str
    description: str
    purposes: list[str]
    data_addresses: list[str] = []


class PlatformRegionsBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    regions: list[PlatformRegionBody]
    # Validated one by one below, so a refusal names the item and field it is about.
    default_grant: dict[str, list[dict[str, Any]]] = {"stores": [], "regions": []}
    # Removing a region orgs are granted is refused listing them unless this confirms it.
    confirm_removal: bool = False


def _written_platform(cfg: dict) -> dict:
    return dict(cfg.get("platform") or {})


def _region_view(region: dict) -> dict:
    return {
        "id": region.get("id"),
        "description": region.get("description"),
        "purposes": list(region.get("purposes") or []),
        "data_addresses": list(region.get("data_addresses") or []),
    }


def _written_grant_view(grant: dict) -> dict:
    return {
        "stores": [
            {
                "id": s.get("id"),
                "url": redact_url_password(s.get("url") or ""),
                "kind": s.get("kind"),
            }
            for s in grant.get("stores") or []
        ],
        "regions": list(grant.get("regions") or []),
    }


def _choices() -> dict:
    from provisa.core.engine_kinds import ENGINE_KINDS
    from provisa.core.regions import STORE_ROLES

    return {
        "purposes": list(PURPOSES),
        "engine_kinds": sorted(ENGINE_KINDS),
        "store_roles": list(STORE_ROLES),
    }


@router.get("/admin/platform/regions")
async def get_platform_regions(request: Request) -> dict:
    """The platform's regions and default grant as the config file writes them (a reference
    stays one). A demo sandbox reads them too, read-only."""
    read_only = _in_demo_sandbox()
    if not read_only:
        require_deployment_settings(request)
    platform = _written_platform(read_config())
    return {
        "regions": [_region_view(r) for r in platform.get("regions") or []],
        "default_grant": _written_grant_view(platform.get("default_grant") or {}),
        **_choices(),
        "read_only": read_only,
        "restart_required": True,
    }


@router.put("/admin/platform/regions")
async def set_platform_regions(request: Request, body: PlatformRegionsBody) -> dict:
    """Replace the platform's regions and default grant. Written to the config file; applied on
    restart."""
    _require_platform_write(request)
    regions = []
    for at, region in enumerate(body.regions):
        try:
            regions.append(PlatformRegion.model_validate(region.model_dump()))
        except ValidationError as refused:
            error = refused.errors()[0]
            attr = str(error["loc"][0]) if error["loc"] else "id"
            raise _refusal(
                f"platform region {region.id!r} {attr}: {error['msg']}", f"regions.{at}.{attr}"
            ) from None
    ids = [r.id for r in regions]
    if len(set(ids)) != len(ids):
        raise _refusal(f"the platform declares a region twice ({', '.join(ids)})", "regions")
    cfg = copy.deepcopy(read_config())
    platform = _written_platform(cfg)
    written_grant = platform.get("default_grant") or {}
    grant = _grant(body.default_grant, written_grant.get("stores") or [], "default_grant.")
    try:
        PlatformConfig(regions=regions, default_grant=grant)
    except ValidationError as refused:
        error = refused.errors()[0]
        message = str((error.get("ctx") or {}).get("error", error["msg"]))
        raise _refusal(message, _field_of(message, "default_grant.")) from None
    removed = sorted({r.get("id") for r in platform.get("regions") or []} - set(ids))
    if removed and not body.confirm_removal:
        affected = await _orgs_granted(removed)
        if affected:
            raise _affected(
                f"removing region {', '.join(removed)} affects the orgs granted it: "
                f"{', '.join(affected)}",
                affected,
                removed,
            )
    platform["regions"] = [r.model_dump(exclude_defaults=True) for r in regions]
    if grant.regions or grant.stores:
        platform["default_grant"] = grant.model_dump(exclude_none=True)
    else:
        platform.pop("default_grant", None)
    cfg["platform"] = platform
    write_config(config_path(), cfg)
    return {
        "regions": [_region_view(r) for r in platform["regions"]],
        "default_grant": _written_grant_view(platform.get("default_grant") or {}),
        **_choices(),
        "read_only": False,
        "restart_required": True,
    }


# -- an org's grant ---------------------------------------------------------------------------------


class GrantBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # Validated one by one below, so a refusal names the item and field it is about.
    stores: list[dict[str, Any]]
    regions: list[dict[str, Any]]
    # Removing a region the org is granted is refused naming the org unless this confirms it.
    confirm_removal: bool = False


def _org_model_store(org_id: str) -> AbstractAsyncContextManager["Database"]:
    """The model store of ``org_id``'s production environment, where its grant is kept."""
    from provisa.api.app import org_model_store
    from provisa.core.environments import PROD

    return org_model_store(org_id, PROD)


def _acting_model_db() -> "Database":
    from provisa.api.app import state

    if state.model_db is None:
        raise ApiError(503, "regions.database_not_connected", "Database not connected")
    return state.model_db


async def _org_ids() -> list[str]:
    """Every organization the platform grants regions to: the platform plane's orgs, but the
    sandbox tier's, which take their regions from their demo's configuration."""
    from sqlalchemy import select

    from provisa.api.app import state
    from provisa.core.org_ids import is_sandbox_tier_org
    from provisa.core.schema_admin import orgs

    assert state.admin_db is not None, "the platform plane holds the organizations"
    async with state.admin_db.acquire() as conn:
        rows = (await conn.execute_core(select(orgs.c.id).order_by(orgs.c.id))).fetchall()
    return [r.id for r in rows if not is_sandbox_tier_org(r.id)]


async def _granted(org_id: str) -> list[str]:
    from provisa.core.repositories import region as region_repo

    async with _org_model_store(org_id) as db:
        async with db.acquire() as conn:
            return [r.id for r in await region_repo.list_regions(conn)]


async def _orgs_granted(regions: list[str]) -> list[str]:
    """The orgs granted any of ``regions``."""
    return [o for o in await _org_ids() if set(await _granted(o)) & set(regions)]


def _platform() -> PlatformConfig:
    """The platform this node runs on: the regions it bound at launch, as written."""
    from provisa.api.app import state

    return state.config.written.platform


def _store_view(store: StoreConfig) -> dict:
    return {"id": store.id, "url": redact_url_password(store.url), "kind": store.kind}


def _platform_region_views() -> list[dict]:
    return [
        {"id": r.id, "description": r.description, "purposes": list(r.purposes)}
        for r in _platform().regions
    ]


async def _grant_view(db: "Database") -> dict:
    from provisa.core.repositories import region as region_repo

    async with db.acquire() as conn:
        stores = await region_repo.list_stores(conn)
        regions = await region_repo.list_regions(conn)
    return {
        "stores": [_store_view(s) for s in stores],
        "regions": [r.model_dump() for r in regions],
    }


@router.get("/admin/platform/orgs")
async def get_org_grants(request: Request) -> dict:
    """Every organization and the regions it is granted, for the platform administrator."""
    require_deployment_settings(request)
    return {"orgs": [{"id": o, "regions": await _granted(o)} for o in await _org_ids()]}


@router.get("/admin/platform/orgs/{org_id}/regions")
async def get_org_grant(org_id: str, request: Request) -> dict:
    """One org's grant: its regions and the stores they name."""
    require_deployment_settings(request)
    await _require_org(org_id)
    async with _org_model_store(org_id) as db:
        held = await _grant_view(db)
    return {"org_id": org_id, "platform_regions": _platform_region_views(), **held, **_choices()}


@router.put("/admin/platform/orgs/{org_id}/regions")
async def set_org_grant(org_id: str, request: Request, body: GrantBody) -> dict:
    """Replace the org's grant with the set sent, or refuse it naming the field."""
    _require_platform_write(request)
    await _require_org(org_id)
    from provisa.core.regions import validate_regions
    from provisa.core.repositories import region as region_repo

    stores = [_parsed(StoreConfig, "stores", at, raw) for at, raw in enumerate(body.stores)]
    regions = [_parsed(OrgRegion, "regions", at, raw) for at, raw in enumerate(body.regions)]
    async with _org_model_store(org_id) as db:
        async with db.acquire() as conn:
            async with conn.transaction():
                held = {s.id: s for s in await region_repo.list_stores(conn)}
                # The form was handed each URL without its password: the same address sent back
                # keeps it.
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
                removed = sorted(
                    r.id for r in await region_repo.list_regions(conn) if r.id not in selected
                )
                if removed and not body.confirm_removal:
                    raise _affected(
                        f"removing region {', '.join(removed)} from org {org_id!r} affects it",
                        [org_id],
                        removed,
                    )
                declared = {s.id for s in stores}
                try:
                    for store in stores:
                        await region_repo.upsert_store(conn, store)
                    for gone in removed:
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
        saved = await _grant_view(db)
    return {"org_id": org_id, "platform_regions": _platform_region_views(), **saved, **_choices()}


async def _require_org(org_id: str) -> None:
    if org_id not in await _org_ids():
        raise ApiError(404, "regions.unknown_org", f"no organization {org_id!r}", org=org_id)


# -- the acting org's regions, read-only ---------------------------------------------------------


@router.get("/admin/regions")
async def get_org_regions(request: Request) -> dict:
    """The platform's regions, and the regions and stores the platform granted the acting org."""
    require_org_settings(request)
    return {
        "platform_regions": _platform_region_views(),
        **await _grant_view(_acting_model_db()),
        **_choices(),
    }


def _grant(raw: dict[str, list[dict[str, Any]]], held: list[dict], prefix: str) -> RegionGrant:
    """The grant sent, each item parsed so a refusal names it; a store URL sent back without its
    password keeps the one ``held``."""
    passwords = {s.get("id"): s.get("url") for s in held}
    stores = [
        _parsed(StoreConfig, "stores", at, item, prefix)
        for at, item in enumerate(raw.get("stores") or [])
    ]
    return RegionGrant(
        stores=[
            s.model_copy(update={"url": restore_url_password(s.url, passwords.get(s.id))})
            for s in stores
        ],
        regions=[
            _parsed(OrgRegion, "regions", at, item, prefix)
            for at, item in enumerate(raw.get("regions") or [])
        ],
    )


def _affected(message: str, orgs: list[str], regions: list[str]) -> ApiError:
    return ApiError(409, "regions.affects_orgs", message, orgs=orgs, regions=regions)


def _parsed(
    model: type[StoreConfig] | type[OrgRegion], kind: str, at: int, raw: dict, prefix: str = ""
) -> Any:
    try:
        return model.model_validate(raw)
    except ValidationError as refused:
        error = refused.errors()[0]
        name = raw.get("id") if isinstance(raw.get("id"), str) and raw.get("id") else str(at)
        attr = ".".join(str(p) for p in error["loc"])
        what = "store" if kind == "stores" else "region"
        raise _refusal(
            f"{what} {name!r} {attr}: {error['msg']}", f"{prefix}{kind}.{name}.{attr}"
        ) from None


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


def _field_of(message: str, prefix: str = "") -> str | None:
    for pattern, field in _FIELDS:
        found = pattern.search(message)
        if found:
            return prefix + field.format(*found.groups())
    return None
