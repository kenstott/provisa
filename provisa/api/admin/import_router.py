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

from provisa.api.admin.capabilities import has_capability_request
from provisa.api.errors import ApiError
from provisa.core.models import ProvisaConfig
from provisa.security.rights import Capability
from provisa.import_shared.upload import DDN, HASURA_V2, UploadError, staged_upload
from provisa.import_shared.warnings import WarningCollector, in_report_order

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/import/hasura", tags=["admin", "import"])

# REQ-1327: an import writes objects of several layers, so it asks, for each kind of object the
# config declares, the right that object's own admin edit asks -- never one right for all of
# them. One entry per model section an apply writes (store_config.MODEL_SECTIONS), plus the
# naming rules held inside the ``naming`` block.
_C = Capability
SECTION_RIGHTS: dict[str, tuple[str, ...]] = {
    # The platform's: where the org's data may live and in which stores (require_deployment_settings).
    "stores": (_C.PLATFORM_SETTINGS.value, _C.CROSS_ORG.value),
    "regions": (_C.PLATFORM_SETTINGS.value, _C.CROSS_ORG.value),
    "sources": (_C.SOURCE_REGISTRATION.value,),
    "kafka_sources": (_C.SOURCE_REGISTRATION.value,),
    "connectors": (_C.SOURCE_REGISTRATION.value,),  # REQ-2221
    "domains": (_C.ORG_SETTINGS.value,),
    "roles": (_C.USER_MANAGEMENT.value,),
    "data_products": (_C.DATA_PRODUCT_RW.value,),
    "glossary_terms": (_C.GLOSSARY_RW.value,),
    "calendars": (_C.TABLE_REGISTRATION.value,),
    "tables": (_C.TABLE_REGISTRATION.value,),
    "relationships": (_C.CREATE_RELATIONSHIP.value,),
    "metrics": (_C.TABLE_REGISTRATION.value,),
    "tags": (_C.TABLE_REGISTRATION.value,),
    "tag_assignments": (_C.TABLE_REGISTRATION.value,),
    "rls_rules": (_C.MASKING_CONFIG.value,),
    "functions": (_C.TABLE_REGISTRATION.value,),
    "webhooks": (_C.TABLE_REGISTRATION.value,),
    "scheduled_triggers": (_C.ORG_SETTINGS.value,),
    "naming": (_C.TABLE_REGISTRATION.value,),
}

#: The rights that open the Import page and its preview: any one of the org's object rights.
#: The platform's pair is not among them -- the platform holds no data capability -- and the UI's
#: route and nav entry name this same list (navGroups.ts IMPORT_RIGHTS).
IMPORT_RIGHTS: tuple[str, ...] = tuple(
    sorted(
        {r for rights in SECTION_RIGHTS.values() for r in rights}
        - {
            _C.PLATFORM_SETTINGS.value,
            _C.CROSS_ORG.value,
        }
    )
)


def _declared_sections(config: ProvisaConfig) -> list[str]:
    """The sections of ``config`` that declare at least one object, in SECTION_RIGHTS order."""
    return [
        section
        for section in SECTION_RIGHTS
        if (config.naming.rules if section == "naming" else getattr(config, section))
    ]


def _require_an_import_right(request: Request) -> None:
    if any(has_capability_request(request, right) for right in IMPORT_RIGHTS):
        return
    raise ApiError(
        403,
        "import.right_required",
        f"importing needs one of {', '.join(IMPORT_RIGHTS)}",
        rights=", ".join(IMPORT_RIGHTS),
    )


def _require_section_rights(request: Request, config: ProvisaConfig) -> None:
    """Refuse an apply declaring an object whose own right the caller lacks, naming each."""
    declared = _declared_sections(config)
    missing = sorted(
        {r for s in declared for r in SECTION_RIGHTS[s] if not has_capability_request(request, r)}
    )
    if not missing:
        return
    lacking = [s for s in declared if any(r in missing for r in SECTION_RIGHTS[s])]
    raise ApiError(
        403,
        "import.rights_required",
        f"this import declares {', '.join(lacking)}, which need {', '.join(missing)}",
        sections=", ".join(lacking),
        rights=", ".join(missing),
    )


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


class ImportReportItemOut(BaseModel):  # REQ-417
    """What one object of the upload became: what it is carried as, or the Provisa feature it
    was mapped to and why."""

    category: str
    item: str
    detail: str


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
    # The report: what converts as it is, what a Provisa feature does better (each saying why),
    # and, as warnings, what has no Provisa equivalent and is redone by hand (each saying what).
    carried: list[ImportReportItemOut] = []
    mapped: list[ImportReportItemOut] = []
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
    # REQ-2222: run the whole model build, report what it would change, and save nothing.
    dry_run: bool = False


class ImportSectionChanges(BaseModel):  # REQ-2222
    add: list[str]
    change: list[dict[str, Any]]  # {item, fields}: field names only, a value may be a credential
    remove: list[str]


class ImportDryRunReport(BaseModel):  # REQ-2222
    """What an apply of this config would do, read from the store before the rollback."""

    changes: bool
    unchanged: int
    sections: dict[str, ImportSectionChanges]
    unreachable_change_feeds: list[str]
    # What an apply does beyond the model build that a dry run does not exercise.
    not_run: list[str]


class ImportApplyResponse(BaseModel):
    summary: ImportSummary
    dry_run: ImportDryRunReport | None = None  # REQ-2222: set by a dry run, which saved nothing


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
    _require_an_import_right(request)  # REQ-1327: preview writes nothing
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
        carried=[
            ImportReportItemOut(category=i.category, item=i.item, detail=i.detail)
            for i in in_report_order(collector.carried)
        ],
        mapped=[
            ImportReportItemOut(category=i.category, item=i.item, detail=i.detail)
            for i in in_report_order(collector.mapped)
        ],
        warnings=[
            ImportWarningOut(category=w.category, message=w.message, source_path=w.source_path)
            for w in in_report_order(collector.warnings)
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


async def _dry_run_report(config: ProvisaConfig) -> ImportDryRunReport:  # REQ-2222
    """The dry run's report, or the model's refusal by name: a build's refusals are ValueErrors
    (a contract's is its own type), and a constraint the store holds names itself."""
    from sqlalchemy.exc import IntegrityError

    from provisa.api.apply_dry_run import NOT_RUN, dry_run_configuration
    from provisa.dq.contract import ContractError

    try:
        outcome = await dry_run_configuration(config)
    except (ValueError, ContractError) as refused:
        raise ApiError(
            400, "import.refused", str(refused), refusal=type(refused).__name__
        ) from refused
    except IntegrityError as refused:
        # The driver's own message: the statement's parameters, which may be credentials, are not.
        raise ApiError(
            400, "import.refused", str(refused.orig), refusal=type(refused).__name__
        ) from refused
    return ImportDryRunReport(
        **outcome.changes,
        unreachable_change_feeds=outcome.unreachable_change_feeds,
        not_run=list(NOT_RUN),
    )


@router.post("/apply", response_model=ImportApplyResponse)
async def apply_import(req: ImportApplyRequest, request: Request) -> ImportApplyResponse:
    """Apply the approved config to the acting org as an explicit one-time seed: it adds and
    updates what the config declares and removes nothing (REQ-1919). The settled config→org
    sequence (``app_loaders.apply_configuration``), not a second loader. With ``dry_run`` the
    same rights are asked and the same build runs, in a transaction rolled back (REQ-2222)."""
    _require_an_import_right(request)  # REQ-1327
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
    except KeyError as exc:
        # A reference the deployment cannot resolve, such as a handler URL's environment variable
        # the report named to set before applying (REQ-417): the administrator's to supply.
        raise ApiError(400, "import.invalid_config", f"config is not valid: {exc.args[0]}") from exc

    if state.model_db is None:
        raise ApiError(
            409, "import.no_active_org", "no org is bound to this request; sign in to an org first"
        )

    _require_section_rights(request, config)  # REQ-1327: each object's own right
    from provisa.security.sensitive import SENSITIVE_DATA, config_changes, refusal

    if not has_capability_request(request, SENSITIVE_DATA):  # REQ-1943
        async with state.model_db.acquire() as conn:
            changes = await config_changes(conn, config)
        if changes:
            raise ApiError(403, "import.sensitive_data_required", refusal(changes))

    async with state.model_db.acquire() as conn:
        await _require_residency_of_import(request, conn, config)  # REQ-1921
    if req.dry_run:  # REQ-2222
        return ImportApplyResponse(
            summary=_summarize(config), dry_run=await _dry_run_report(config)
        )
    await apply_configuration(config)

    log.info(
        "hasura import applied: %d sources, %d tables, %d roles",
        len(config.sources),
        len(config.tables),
        len(config.roles),
    )
    return ImportApplyResponse(summary=_summarize(config))
