# Copyright (c) 2026 Kenneth Stott
# Canary: 6f2c9a41-70de-4b53-9c8a-1de3b0a77c25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Interactive Hasura v2 / DDN import for the acting org's semantic layer (REQ-1483).

The CLIs (``python -m provisa.hasura_v2``, ``python -m provisa.ddn``) convert a project directory
to a config file someone then loads. These two endpoints are the same conversion, driven from the
admin UI, split so nothing lands in the org until an administrator has read what the conversion
produced:

  POST /admin/import/hasura/preview — convert an upload, return the YAML, warnings and a summary
  POST /admin/import/hasura/apply   — load a previewed config into the acting org

Preview never touches the tenant database. Apply takes the YAML the administrator approved, not a
server-side stash of the preview, so what is applied is exactly what was reviewed and edited.
"""

from __future__ import annotations

import base64
import binascii
import logging
from typing import Any

import yaml
from fastapi import APIRouter, Request
from pydantic import BaseModel

from provisa.api.admin._platform_guard import require_org_settings
from provisa.api.errors import ApiError
from provisa.core.models import ProvisaConfig
from provisa.import_shared.upload import DDN, HASURA_V2, UploadError, staged_upload
from provisa.import_shared.warnings import WarningCollector

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/import/hasura", tags=["admin", "import"])


class ImportPreviewRequest(BaseModel):
    filename: str
    # The upload is base64 in a JSON body rather than multipart: the archive is binary and every
    # other admin surface speaks JSON, so the whole admin API keeps one content type.
    content_b64: str
    flavor: str = "auto"  # "auto" | "hasura_v2" | "ddn"
    domain_map: dict[str, str] = {}
    source_overrides: dict[str, Any] = {}


class ImportSummary(BaseModel):
    """What the conversion produced, for the approval step."""

    sources: int
    domains: int
    tables: int
    columns: int
    roles: int
    relationships: int
    rls_rules: int
    source_ids: list[str]
    domain_ids: list[str]
    role_ids: list[str]


class ImportWarningOut(BaseModel):
    category: str
    message: str
    source_path: str = ""


class ImportPreviewResponse(BaseModel):
    flavor: str
    config_yaml: str
    warnings: list[ImportWarningOut]
    summary: ImportSummary


class ImportApplyRequest(BaseModel):
    config_yaml: str
    # replace=False merges the import into what the org already has; replace=True is the full
    # replace semantics load_config implements — everything absent from this config is deleted.
    replace: bool = False


class ImportApplyResponse(BaseModel):
    summary: ImportSummary
    replace: bool


def _decode(content_b64: str) -> bytes:
    try:
        return base64.b64decode(content_b64, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ApiError(
            400, "import.bad_encoding", f"uploaded content is not valid base64: {exc}"
        ) from exc


def _summarize(config: ProvisaConfig) -> ImportSummary:
    columns = sum(len(t.columns) for t in config.tables)
    relationships = len(config.relationships)
    rls_rules = len(config.rls_rules)
    return ImportSummary(
        sources=len(config.sources),
        domains=len(config.domains),
        tables=len(config.tables),
        columns=columns,
        roles=len(config.roles),
        relationships=relationships,
        rls_rules=rls_rules,
        source_ids=[s.id for s in config.sources],
        domain_ids=[d.id for d in config.domains],
        role_ids=[r.id for r in config.roles],
    )


def _convert(req: ImportPreviewRequest) -> tuple[str, ProvisaConfig, WarningCollector]:
    """Run the same parser+mapper pair the matching CLI runs, over the staged upload."""
    collector = WarningCollector()
    data = _decode(req.content_b64)
    with staged_upload(req.filename, data, req.flavor) as staged:
        if staged.flavor == DDN:
            from provisa.ddn.mapper import convert_hml
            from provisa.ddn.parser import parse_hml_dir

            assert staged.root is not None, "a DDN upload always stages to a directory"
            metadata = parse_hml_dir(staged.root, collector)
            config = convert_hml(
                metadata,
                collector=collector,
                domain_map=req.domain_map,
                source_overrides=req.source_overrides,
            )
            return DDN, config, collector

        from provisa.hasura_v2.mapper import convert_metadata
        from provisa.hasura_v2.parser import parse_metadata_dir, parse_metadata_document

        if staged.document is not None:
            v2_metadata = parse_metadata_document(staged.document, collector)
        else:
            assert staged.root is not None, "a v2 upload stages to a directory or a document"
            v2_metadata = parse_metadata_dir(staged.root, collector)
        config = convert_metadata(
            v2_metadata,
            collector=collector,
            domain_map=req.domain_map,
            source_overrides=req.source_overrides,
        )
        return HASURA_V2, config, collector


@router.post("/preview", response_model=ImportPreviewResponse)
async def preview_import(req: ImportPreviewRequest, request: Request) -> ImportPreviewResponse:
    """Convert an upload and return the config for review. Writes nothing."""
    require_org_settings(request)  # REQ-1483: an org owns its own semantic layer
    try:
        flavor, config, collector = _convert(req)
    except UploadError as exc:
        raise ApiError(400, "import.bad_upload", str(exc)) from exc
    except ValueError as exc:
        # A malformed metadata document is the administrator's input, not a server fault.
        raise ApiError(400, "import.conversion_failed", f"conversion failed: {exc}") from exc

    data = config.model_dump(by_alias=True, exclude_none=True, mode="json")
    return ImportPreviewResponse(
        flavor=flavor,
        config_yaml=yaml.dump(data, default_flow_style=False, sort_keys=False),
        warnings=[
            ImportWarningOut(category=w.category, message=w.message, source_path=w.source_path)
            for w in collector.warnings
        ],
        summary=_summarize(config),
    )


@router.post("/apply", response_model=ImportApplyResponse)
async def apply_import(req: ImportApplyRequest, request: Request) -> ImportApplyResponse:
    """Load the approved config into the acting org, then rebuild its schemas.

    This is the settled config→org sequence (the one org creation runs), not a second loader:
    catalog names first so sources register under the org's own engine catalogs (REQ-1266), then
    load_config, source pools/enums, PK resolution, schema rebuild.
    """
    require_org_settings(request)  # REQ-1483
    from provisa.api.app import _rebuild_schemas, state
    from provisa.api.app_loaders import _build_source_pools_and_enums, _populate_source_catalog_names
    from provisa.api.startup_seed import _resolve_pk_from_sources
    from provisa.core.config_loader import load_config, parse_config_dict

    try:
        raw = yaml.safe_load(req.config_yaml)
    except yaml.YAMLError as exc:
        raise ApiError(400, "import.bad_yaml", f"config is not valid YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise ApiError(400, "import.bad_yaml", "config must be a YAML mapping")
    try:
        config = parse_config_dict(raw)
    except ValueError as exc:
        raise ApiError(400, "import.invalid_config", f"config is not valid: {exc}") from exc

    tenant_db = state.tenant_db
    if tenant_db is None:
        raise ApiError(
            409, "import.no_active_org", "no org is bound to this request; sign in to an org first"
        )

    _populate_source_catalog_names(config)
    async with tenant_db.acquire() as conn:
        await load_config(
            config,
            conn,
            state.federation_engine,
            replace=req.replace,
            catalog_names=state.source_catalogs,
        )
    await _build_source_pools_and_enums(config)
    await _resolve_pk_from_sources()
    await _rebuild_schemas()

    log.info(
        "hasura import applied: %d sources, %d tables, %d roles (replace=%s)",
        len(config.sources),
        len(config.tables),
        len(config.roles),
        req.replace,
    )
    return ImportApplyResponse(summary=_summarize(config), replace=req.replace)
