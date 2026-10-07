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


# REQ-1687: the source kinds whose connection the administrator supplies as an override.
_CONNECTION_KINDS = {"postgresql", "mysql"}


class DiscoveredSource(BaseModel):  # REQ-1687
    """A converted source whose connection the export could not know: what the conversion guessed,
    for the administrator to correct before converting again. Never carries a password."""

    id: str
    type: str
    host: str
    port: int
    database: str
    username: str


class ImportPreviewResponse(BaseModel):
    flavor: str
    config_yaml: str
    warnings: list[ImportWarningOut]
    summary: ImportSummary
    # The schema (v2) or subgraph (DDN) names the upload carries — and, for v2, the remote schema
    # names, which map to a domain the same way (REQ-1681) — so the UI can offer one mapping row per
    # name instead of asking the administrator to type them from memory.
    discovered_domains: list[str]
    # REQ-1687: the SQL sources the upload carries, with the connection the conversion guessed.
    discovered_sources: list[DiscoveredSource] = []


class ImportApplyRequest(BaseModel):
    # An import is always a merge into what the org already has (REQ-1919): what the imported
    # model does not mention is not the import's to remove.
    config_yaml: str


class ImportApplyResponse(BaseModel):
    summary: ImportSummary


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


def _convert(req: ImportPreviewRequest) -> tuple[str, ProvisaConfig, WarningCollector, list[str]]:
    """Run the same parser+mapper pair the matching CLI runs, over the staged upload.

    The fourth element is what the upload itself names — DDN subgraphs, v2 schemas — before any
    mapping is applied. It is the left-hand side of the domain map, which the administrator cannot
    know until the file has been parsed.
    """
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
            return DDN, config, collector, sorted(metadata.subgraphs)

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
        schemas = sorted({t.schema_name for s in v2_metadata.sources for t in s.tables})
        # REQ-1681/REQ-1687: a remote schema's name is a domain-map key too.
        return (
            HASURA_V2,
            config,
            collector,
            schemas + sorted(rs.name for rs in v2_metadata.remote_schemas),
        )


@router.post("/preview", response_model=ImportPreviewResponse)
async def preview_import(req: ImportPreviewRequest, request: Request) -> ImportPreviewResponse:
    """Convert an upload and return the config for review. Writes nothing."""
    require_org_settings(request)  # REQ-1483: an org owns its own semantic layer
    try:
        flavor, config, collector, discovered = _convert(req)
    except UploadError as exc:
        raise ApiError(400, "import.bad_upload", str(exc)) from exc
    except ValueError as exc:
        # A malformed metadata document is the administrator's input, not a server fault.
        raise ApiError(400, "import.conversion_failed", f"conversion failed: {exc}") from exc

    # REQ-1691: preview is design time — type the columns from the sources the overrides reach.
    from provisa.api.admin.import_typing import type_imported_columns

    await type_imported_columns(config, collector)

    data = config.model_dump(by_alias=True, exclude_none=True, mode="json")
    return ImportPreviewResponse(
        flavor=flavor,
        config_yaml=yaml.dump(data, default_flow_style=False, sort_keys=False),
        warnings=[
            ImportWarningOut(category=w.category, message=w.message, source_path=w.source_path)
            for w in collector.warnings
        ],
        summary=_summarize(config),
        discovered_domains=discovered,
        discovered_sources=[
            DiscoveredSource(
                id=s.id,
                type=s.type.value,
                host=s.host or "",
                port=int(s.port or 0),
                database=s.database or "",
                username=s.username or "",
            )
            for s in config.sources
            if s.type.value in _CONNECTION_KINDS
        ],
    )


async def _require_residency_of_import(request: Request, conn, config: ProvisaConfig) -> None:
    """Refuse an import that sets, changes or removes a source's or a table's region beyond the
    caller's data_residency grant (REQ-1921) — the same rule as an edit in the admin, judged
    against what the org holds now (an object it does not hold yet is created)."""
    from sqlalchemy import select

    from provisa.api.admin.capabilities import require_residency_change_request
    from provisa.core.schema_org import registered_tables, sources
    from provisa.security.residency import CREATED, ResidencyRefused

    held_sources = {
        r.id: r.region for r in (await conn.execute_core(select(sources.c.id, sources.c.region)))
    }
    t = registered_tables
    held_tables = {
        (r.source_id, r.schema_name, r.table_name): r.region
        for r in await conn.execute_core(
            select(t.c.source_id, t.c.schema_name, t.c.table_name, t.c.region)
        )
    }
    try:
        for s in config.sources:
            before = held_sources[s.id] if s.id in held_sources else CREATED
            require_residency_change_request(request, f"source {s.id}", before, s.region)
        for tbl in config.tables:
            key = (tbl.source_id, tbl.schema_name, tbl.table_name)
            before = held_tables[key] if key in held_tables else CREATED
            require_residency_change_request(
                request,
                f"table {tbl.source_id}/{tbl.schema_name}.{tbl.table_name}",
                before,
                tbl.region,
            )
    except ResidencyRefused as refused:
        raise ApiError(
            403,
            refused.code,
            str(refused),
            object=refused.params["object"],
            value=refused.params["value"],
        ) from refused


@router.post("/apply", response_model=ImportApplyResponse)
async def apply_import(req: ImportApplyRequest, request: Request) -> ImportApplyResponse:
    """Apply the approved config to the acting org as an explicit one-time seed: it adds and
    updates what the config declares and removes nothing (REQ-1919). The settled config→org
    sequence (``app_loaders.apply_configuration``), not a second loader."""
    require_org_settings(request)  # REQ-1483
    from provisa.api.app import state
    from provisa.api.app_loaders import apply_configuration
    from provisa.core.config_loader import parse_config_dict

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

    if state.model_db is None:
        raise ApiError(
            409, "import.no_active_org", "no org is bound to this request; sign in to an org first"
        )

    from provisa.api.admin.capabilities import has_capability_request
    from provisa.security.sensitive import SENSITIVE_DATA, config_changes, refusal

    if not has_capability_request(request, SENSITIVE_DATA):  # REQ-1943
        async with state.model_db.acquire() as conn:
            changes = await config_changes(conn, config)
        if changes:
            raise ApiError(403, "import.sensitive_data_required", refusal(changes))

    async with state.model_db.acquire() as conn:
        await _require_residency_of_import(request, conn, config)  # REQ-1921
    await apply_configuration(config)

    log.info(
        "hasura import applied: %d sources, %d tables, %d roles",
        len(config.sources),
        len(config.tables),
        len(config.roles),
    )
    return ImportApplyResponse(summary=_summarize(config))
