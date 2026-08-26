# Copyright (c) 2026 Kenneth Stott
# Canary: 7a2e5b90-4c1d-4f68-93a7-0e5b2c81d47f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The model as a downloadable workbook (REQ-1592).

``GET /admin/report.xlsx`` — one sheet per object type, built from the metadata-export projection
so what a reviewer reads is what the platform actually serves. Download only: there is no POST
twin, because a spreadsheet accepted back as configuration would reintroduce every ambiguity the
config format removes, and a stale workbook uploaded months later would silently revert the model.

NARROWED, NOT REFUSED. A sheet the caller holds no right to read is OMITTED rather than 403-ing the
whole download, and the ``domains`` query parameter narrows exactly as it does on the surfaces the
sheets mirror. The leading Report sheet names the org, the environment, the domains covered, the
generation timestamp and the omitted sheets, so a partial workbook can never pass as a complete one.
"""

# Requirements: REQ-1592

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from fastapi import APIRouter, Query, Request, Response

from provisa.api.admin._guards import require_active_org_id
from provisa.api.admin._platform_guard import has_right
import provisa.api.metadata_export.workbook as wb  # noqa: PLR0402
from provisa.security.rights import Capability

if TYPE_CHECKING:
    from provisa.api.metadata_export.model import MetadataSnapshot
    from provisa.core.models import ProvisaConfig

router = APIRouter()

# The right that lets a caller READ each sheet. These are the same rights the corresponding admin
# surfaces are gated on — the Sources view on source_registration, the Tables view on
# table_registration (glossary_router's /ref does the same for a read-only lookup), and so on —
# because the workbook shows those surfaces' contents and must not be a way around their gates.
SHEET_RIGHTS: dict[str, str] = {
    wb.SOURCES: Capability.SOURCE_REGISTRATION.value,
    wb.TABLES: Capability.TABLE_REGISTRATION.value,
    wb.COLUMNS: Capability.TABLE_REGISTRATION.value,
    wb.RELATIONSHIPS: Capability.CREATE_RELATIONSHIP.value,
    wb.VIEWS: Capability.CREATE_VIEW.value,
    wb.METRICS: Capability.CREATE_VIEW.value,
    wb.MATERIALIZED_VIEWS: Capability.CREATE_VIEW.value,
    wb.GLOSSARY: Capability.GLOSSARY_READ.value,
}

XLSX_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def _view_scope(request: Request, selected: "list[str] | None") -> frozenset[str] | None:
    """The caller's domain authority INTERSECTED with the requested domains (REQ-1591, REQ-1592).

    Identical rule to the glossary surface this mirrors: a selection never widens authority, and
    an absent parameter leaves authority alone. Repeated ``domains=`` parameters rather than one
    comma-joined string, because the no-domain domain's id IS the empty string.
    """
    from provisa.api.admin.capabilities import allowed_domains_request

    allowed = allowed_domains_request(request)
    if selected is None:
        return allowed
    chosen = frozenset(selected)
    return chosen if allowed is None else chosen & allowed


def _domains_label(scope: frozenset[str] | None) -> str:
    """How the Report sheet states the coverage. ``None`` is unlimited, which is an answer."""
    if scope is None:
        return "all"
    if not scope:
        return "none"
    return ", ".join(sorted("(no domain)" if domain == "" else domain for domain in scope))


def _narrow(snapshot: "MetadataSnapshot", scope: frozenset[str] | None) -> "MetadataSnapshot":
    """Drop every asset outside ``scope``, keeping the snapshot internally consistent.

    Tables carry the domain; everything else follows the tables it addresses. Sources are kept only
    when a kept table uses them — a narrowed workbook that still listed the whole source inventory
    would leak the shape of the domains it was narrowed away from. Glossary terms follow REQ-1591's
    ANY rule over the domains of their refs; a term with no refs is unscoped and is kept.
    """
    from dataclasses import replace

    if scope is None:
        return snapshot
    tables = [table for table in snapshot.tables if (table.domain_id or "") in scope]
    kept = {table.ref for table in tables}
    kept_sources = {table.source_id for table in tables}
    # A term ref addresses a COLUMN — (source, schema, table, column) — so its table is its parts
    # minus the last one; the domain index is keyed that way rather than by AssetRef identity.
    domain_of = {table.ref.parts: (table.domain_id or "") for table in snapshot.tables}
    terms = []
    for term in snapshot.glossary_terms:
        in_scope = tuple(
            ref
            for ref in term.refs
            if ref.parts[:-1] in domain_of and domain_of[ref.parts[:-1]] in scope
        )
        # A kept term carries only the refs inside the scope: printing its out-of-scope refs would
        # name the very tables the narrowing withheld.
        if not term.refs:
            terms.append(term)
        elif in_scope:
            terms.append(replace(term, refs=in_scope))
    return replace(
        snapshot,
        sources=[source for source in snapshot.sources if source.id in kept_sources],
        domains=[domain for domain in snapshot.domains if domain.id in scope],
        tables=tables,
        relationships=[edge for edge in snapshot.relationships if edge.source in kept],
        glossary_terms=terms,
    )


async def _hydrate(org_id: str) -> tuple["MetadataSnapshot", "ProvisaConfig"]:
    """The governed projection AND the config it was projected from, for one org.

    The same sequence :func:`provisa.api.metadata_export.publishing.publish_snapshot` uses, because
    a report that projected the model differently from the publisher would show a reviewer
    something the platform does not serve. The config comes back alongside the snapshot because
    views, metrics and materialized views have no snapshot asset (see the workbook module).
    """
    from provisa.api.app import state
    from provisa.api.metadata_export.builder import build_snapshot
    from provisa.api.metadata_export.dq_outcomes import read_latest_outcomes
    from provisa.api.metadata_export.publishing import GOVERNED_DIALECT, _model_for_export
    from provisa.api.org_runtime import reset_current_org, set_current_org
    from provisa.core.repositories import glossary as glossary_repo

    token = set_current_org(org_id)
    try:
        model = await _model_for_export()
        tenant_db = state.tenant_db
        assert tenant_db is not None  # the admin surface is only mounted with a tenant plane
        async with tenant_db.acquire() as conn:
            glossary = await glossary_repo.export_graph(conn)
        dq_outcomes = await read_latest_outcomes(model)
        snapshot = build_snapshot(
            model,
            org_id=org_id,
            dialect=GOVERNED_DIALECT,
            glossary=glossary,
            dq_outcomes=dq_outcomes,
            # The report is a steward's view of the whole registered model, not a catalog publish:
            # an unmarked table is one of the things a review exists to find, so the Data Product
            # flag rides along as a column on the Tables sheet instead of deciding what appears.
            data_products_only=False,
        )
    finally:
        reset_current_org(token)
    return snapshot, model


@router.get("/admin/report.xlsx", tags=["admin"])
async def model_report(
    request: Request,
    domains: "list[str] | None" = Query(default=None),
) -> Response:
    """The workbook. One way only — there is deliberately no upload counterpart."""
    from provisa.api.org_runtime import active_env

    org_id = require_active_org_id(request)
    included = tuple(name for name in wb.OBJECT_SHEETS if has_right(request, SHEET_RIGHTS[name]))
    omitted = tuple(name for name in wb.OBJECT_SHEETS if name not in included)
    scope = _view_scope(request, domains)
    snapshot, config = await _hydrate(org_id)
    rows = wb.sheet_rows(_narrow(snapshot, scope), config, org_id=org_id)
    header = wb.ReportHeader(
        org_id=org_id,
        environment=active_env(),
        domains=_domains_label(scope),
        generated_at=datetime.now(UTC),
        included=included,
        omitted=omitted,
    )
    payload = wb.build_workbook(header, rows)
    return Response(
        content=payload,
        media_type=XLSX_MEDIA_TYPE,
        headers={"content-disposition": f'attachment; filename="provisa-model-{org_id}.xlsx"'},
    )
