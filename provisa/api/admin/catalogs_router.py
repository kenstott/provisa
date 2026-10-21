# Copyright (c) 2026 Kenneth Stott
# Canary: 8c4f2e71-3a9b-4d05-b6e2-1f7a9c3d5e80
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Catalogs panel's surface (REQ-2122).

Each catalog an org works with is connected once here: its provider, endpoint, credentials and the
domains that work with it. The metadata export names a catalog for each target, and the
description pick list, Refresh from catalog, the import and the lock read through the same
connection.

Credentials are write-only through this surface, as on the export's (REQ-1074): the read reports
whether each is set, a credential left out of a write keeps the one held, and one sent empty clears
it. The connection test is the provider's own health check, the one the export runs.
"""

# Requirements: REQ-2122

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from pydantic import ValidationError

from provisa.api.admin._guards import require_active_org_id
from provisa.api.admin._platform_guard import require_org_settings
from provisa.api.errors import ApiError
from provisa.api.metadata_export import metadata_export
from provisa.api.metadata_export.registry import registered_providers
from provisa.core.models import CatalogExport, CatalogsConfig, MetadataExportConfig

router = APIRouter()

CONFIG_KEY = "catalogs"

# A catalog's credential fields, which the read reports as set/not-set and never returns.
SECRET_FIELDS = (
    "api_key",
    "token",
    "entra_client_secret",
    "google_service_account_json",
    "aws_access_key_id",
    "aws_secret_access_key",
)

# Everything else of a catalog the panel edits, returned as held.
PLAIN_FIELDS = (
    "id",
    "provider",
    "endpoint",
    "auth_mode",
    "username",
    "entra_tenant_id",
    "entra_client_id",
    "gcp_project",
    "gcp_location",
    "aws_region",
    "datazone_domain_id",
    "timeout_seconds",
    "domains",
    "import_cron",
)


def _model_db():
    """The org's model store, where its settings are kept, or a 503."""
    from provisa.api.app import state

    if state.model_db is None:
        raise ApiError(503, "catalogs.database_not_connected", "Database not connected")
    return state.model_db


async def stored_settings() -> dict[str, Any]:
    """The org's settings as resolved: the deployment's with the org's own over them."""
    from provisa.core.org_settings import resolve_org_config

    return await resolve_org_config(_model_db())


def catalogs_config(settings: dict[str, Any]) -> CatalogsConfig:
    """The org's catalogs, or a 400 naming what is wrong with them."""
    try:
        return CatalogsConfig(catalogs=list(settings.get(CONFIG_KEY) or []))
    except ValidationError as exc:
        raise ApiError(400, "catalogs.invalid", str(exc)) from exc


def _view(catalog: Any) -> dict[str, Any]:
    return {
        **{name: getattr(catalog, name) for name in PLAIN_FIELDS},
        **{
            f"{name}_set": bool(getattr(catalog, name).get_secret_value()) for name in SECRET_FIELDS
        },
    }


def _merged(stored: list[dict[str, Any]], sent: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The catalogs as sent, each credential left out of the body kept from the stored catalog of
    the same id. A catalog left out of the body is removed."""
    by_id = {c.get("id"): c for c in stored}
    merged = []
    for catalog in sent:
        prior = by_id.get(catalog.get("id"), {})
        merged.append(
            {
                **{name: catalog[name] for name in PLAIN_FIELDS if name in catalog},
                **{
                    name: catalog[name] if name in catalog else prior.get(name, "")
                    for name in SECRET_FIELDS
                },
            }
        )
    return merged


async def _domain_ids() -> set[str]:
    from provisa.core.repositories import domain as domain_repo

    async with _model_db().acquire() as conn:
        return {d["id"] for d in await domain_repo.list_all(conn)}


@router.get("/admin/catalogs")
async def get_catalogs(request: Request) -> dict:  # REQ-2122
    """The org's catalogs, credentials reported as set/not-set, and the providers it may pick."""
    require_org_settings(request)
    require_active_org_id(request)
    config = catalogs_config(await stored_settings())
    from provisa.api.metadata_export.reader import readable_providers

    return {
        "providers": sorted(registered_providers()),
        # REQ-2122: the providers whose catalogs can be read back (imported from, picked from).
        "readable": readable_providers(),
        "catalogs": [_view(c) for c in config.catalogs],
    }


@router.put("/admin/catalogs")
async def set_catalogs(request: Request) -> dict:  # REQ-2122
    """Keep the org's catalogs as sent. Refused by name: a catalog with no domain or a domain the
    org does not have, and the removal of a catalog the metadata export still names."""
    require_org_settings(request)
    require_active_org_id(request)
    from provisa.core.org_settings import write_org_overrides

    body = await request.json()
    settings = await stored_settings()
    merged = _merged(list(settings.get(CONFIG_KEY) or []), list(body.get("catalogs") or []))
    config = catalogs_config({CONFIG_KEY: merged})
    known = await _domain_ids()
    for catalog in config.catalogs:
        unknown = sorted(set(catalog.domains) - known)
        if unknown:
            raise ApiError(
                400,
                "catalogs.unknown_domain",
                f"catalog {catalog.id!r} is assigned to domains the organization does not have: "
                f"{', '.join(unknown)}",
                catalog=catalog.id,
                domains=", ".join(unknown),
            )
    export = MetadataExportConfig(**dict(settings.get("metadata_export") or {}))
    named = {c.id for c in config.catalogs}
    exported = sorted(t.catalog for t in export.targets if t.catalog not in named)
    if exported:
        raise ApiError(
            400,
            "catalogs.exported",
            f"the metadata export publishes to {', '.join(exported)}; remove it from the export "
            "first",
            catalogs=", ".join(exported),
        )
    identity = getattr(request.state, "identity", None)
    await write_org_overrides(
        _model_db(), {CONFIG_KEY: merged}, updated_by=getattr(identity, "user_id", "anonymous")
    )
    # Re-arm the scheduled imports against what was just saved, so a changed schedule takes
    # effect now rather than at the next restart.
    from provisa.api.app import state

    scheduler = getattr(state, "_scheduler", None)
    if scheduler is not None:
        from provisa.api.metadata_export.catalog_import import register_org_jobs

        await register_org_jobs(scheduler, require_active_org_id(request))
    return {"success": True, "catalogs": [c.id for c in config.catalogs]}


@router.post("/admin/catalogs/{catalog_id}/health")
async def check_catalog(request: Request, catalog_id: str) -> dict:  # REQ-2122
    """Test one catalog's connection with its provider's health check, the export's own."""
    require_org_settings(request)
    require_active_org_id(request)
    settings = await stored_settings()
    catalog = catalogs_config(settings).by_id().get(catalog_id)
    if catalog is None:
        raise ApiError(404, "catalogs.not_found", f"There is no catalog {catalog_id!r}")
    export = MetadataExportConfig(**dict(settings.get("metadata_export") or {}))
    settings_for = next(
        (t for t in export.targets if t.catalog == catalog_id), CatalogExport(catalog=catalog_id)
    )
    try:
        await metadata_export(catalog.export_target(settings_for)).health()
    except Exception as exc:  # noqa: BLE001 - allow-blind-except: the message IS the answer
        # The provider raises whatever its transport raised; the admin needs that text to tell a
        # wrong address from a refused credential, so it is reported rather than classified.
        return {
            "id": catalog_id,
            "provider": catalog.provider,
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
        }
    return {"id": catalog_id, "provider": catalog.provider, "ok": True}


def _uri_org_id(request: Request) -> str | None:
    """The org a table's semantic URI names: the org under multitenancy, none otherwise."""
    from provisa.api.app import state

    return require_active_org_id(request) if state.multitenancy else None


def _readable(catalog_id: str, settings: dict[str, Any]) -> Any:
    from provisa.api.metadata_export.reader import readable_providers

    catalog = catalogs_config(settings).by_id().get(catalog_id)
    if catalog is None:
        raise ApiError(404, "catalogs.not_found", f"There is no catalog {catalog_id!r}")
    if catalog.provider not in readable_providers():
        raise ApiError(
            400,
            "catalog_link.unreadable",
            f"catalog {catalog_id!r} ({catalog.provider}) holds no table assets to read",
            catalog=catalog_id,
        )
    return catalog


@router.get("/admin/catalogs/{catalog_id}/import")
async def review_import(request: Request, catalog_id: str) -> dict:  # REQ-2122
    """The catalog's Import review: each linked table of its domains with what its asset holds
    differently from Provisa's values, and each table not linked to it."""
    from provisa.api.admin.capabilities import require_capability_request
    from provisa.api.metadata_export import catalog_import

    require_capability_request(request, "table_registration")
    catalog = _readable(catalog_id, await stored_settings())
    async with _model_db().acquire() as conn:
        return await catalog_import.review(conn, catalog, _uri_org_id(request))


@router.post("/admin/catalogs/{catalog_id}/import")
async def accept_import(request: Request, catalog_id: str) -> dict:  # REQ-2122
    """Accept the catalog's changes for the tables named in ``table_ids``: all at once, or one by
    one."""
    from provisa.api.admin.capabilities import require_capability_request
    from provisa.api.admin.schema_helpers import _rebuild_schemas
    from provisa.api.metadata_export import catalog_import, linking

    require_capability_request(request, "table_registration")
    body = await request.json()
    table_ids = [int(t) for t in body.get("table_ids") or []]
    catalog = _readable(catalog_id, await stored_settings())
    async with _model_db().acquire() as conn:
        try:
            async with conn.transaction():
                applied = await catalog_import.accept(
                    conn, catalog, table_ids, _uri_org_id(request)
                )
        except linking.LinkRefused as refused:
            raise ApiError(
                refused.status, refused.code, str(refused), None, **refused.params
            ) from None
    await _rebuild_schemas()
    return {"success": True, "applied": {str(k): v for k, v in applied.items()}}
