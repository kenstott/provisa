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
from typing import TYPE_CHECKING, Any

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
    wb.ONTOLOGY: Capability.GLOSSARY_READ.value,
    wb.DATA_PRODUCTS: Capability.DATA_PRODUCT_READ.value,
    wb.DATA_CONTRACTS: Capability.DATA_PRODUCT_READ.value,
    wb.DATA_QUALITY: Capability.DQ_READ.value,
    wb.DATA_PROFILING: Capability.DQ_READ.value,
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
        data_products=[product for product in snapshot.data_products if product.domain_id in scope],
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
    from provisa.core.request_context import reset_current_org, set_current_org
    from provisa.core.repositories import glossary as glossary_repo

    token = set_current_org(org_id)
    try:
        model = await _model_for_export()
        model_db = state.model_db
        assert model_db is not None  # the admin surface is only mounted with a tenant plane
        async with model_db.acquire() as conn:
            glossary = await glossary_repo.export_graph(conn)
            # REQ-2163: the Ontology sheet states each term's axioms.
            glossary["axioms"] = await glossary_repo.axioms(conn)
        dq_outcomes = await read_latest_outcomes(model)
        snapshot = build_snapshot(
            model,
            org_id=org_id,
            uri_org_id=org_id if state.multitenancy else None,
            dialect=GOVERNED_DIALECT,
            glossary=glossary,
            dq_outcomes=dq_outcomes,
            contexts=state.contexts,
            # The report is a steward's view of the whole registered model, not a catalog publish:
            # an unmarked table is one of the things a review exists to find, so the Data Product
            # flag rides along as a column on the Tables sheet instead of deciding what appears.
            data_products_only=False,
        )
    finally:
        reset_current_org(token)
    return snapshot, model


async def _as_served(
    request: Request, snapshot: "MetadataSnapshot", config: "ProvisaConfig"
) -> tuple["MetadataSnapshot", "ProvisaConfig"]:
    """The workbook's model as its caller reads it (REQ-2152): the columns its acting role is
    served and the tables holding one (``scope.for_role``), and the metrics granted to it. The
    right that opens the workbook does not widen what it shows."""
    import types

    from provisa.api.admin.catalog_scope import catalog_scope
    from provisa.api.app import state
    from provisa.api.flight.catalog import role_sees_metric
    from provisa.api.metadata_export.publishing import snapshot_reader
    from provisa.api.metadata_export.scope import for_role

    caller = catalog_scope(types.SimpleNamespace(context={"request": request}))
    if caller.whole:
        return snapshot, config
    # Each table as the catalog answers it: a table no build compiled yet (saved since) by its
    # stored grants, as read_rights.served_columns_of judges it.
    from sqlalchemy import select

    from provisa.api.admin.read_rights import served_columns_of
    from provisa.core.schema_org import registered_tables as rt

    assert state.model_db is not None  # the org's model store is bound with its runtime
    async with state.model_db.acquire() as conn:
        ids = [r.id for r in (await conn.execute_core(select(rt.c.id))).fetchall()]
        served = {i: await served_columns_of(conn, caller, i) for i in ids}
    narrowed = for_role(
        snapshot,
        await snapshot_reader(state, lambda i: served.get(i, set())),
        lambda domain_id: caller.lists_domain(domain_id, False),
    )
    narrowed = _masked_for(_naming_only_the_served(narrowed, caller.reach), caller.role)
    role = caller.role
    metrics = [m for m in config.metrics if role is not None and role_sees_metric(state, role, m)]
    return narrowed, config.model_copy(update={"metrics": metrics})


def _naming_only_the_served(
    snapshot: "MetadataSnapshot", reach: frozenset[str] | None
) -> "MetadataSnapshot":
    """Every sheet names only what its caller is served (REQ-1592, 2026-10-27). ``snapshot``
    holds the caller's served tables and columns already; a relationship on a column the caller
    is not served goes with it, a Class lists only the served tables it is a union of, a term
    outside the caller's domains (``reach``) is absent -- one with none is shown to every reader
    (REQ-2163, readable_term) -- and an edge is shown only when both its terms are."""
    from dataclasses import replace

    from provisa.core.glossary import readable_term

    served = {table.ref: {column.name for column in table.columns} for table in snapshot.tables}
    relationships = [
        edge
        for edge in snapshot.relationships
        if edge.source_column in served.get(edge.source, set())
        and (edge.target is None or edge.target_column in served.get(edge.target, set()))
    ]
    terms = [
        replace(term, tables=tuple(ref for ref in term.tables if ref in served))
        for term in snapshot.glossary_terms
        if readable_term(reach, frozenset(term.domains))
    ]
    shown = {term.term_id for term in terms}
    return replace(
        snapshot,
        relationships=relationships,
        glossary_terms=terms,
        glossary_edges=[
            edge
            for edge in snapshot.glossary_edges
            if edge.from_term_id in shown and edge.to_term_id in shown
        ],
    )


def _masked_for(snapshot: "MetadataSnapshot", role: str | None) -> "MetadataSnapshot":
    """The snapshot with each check's metric on a column masked to ``role`` blank (REQ-1592,
    2026-10-27): a statistic of a masked column would reveal what its mask hides. The check's
    verdict stays."""
    from dataclasses import replace

    from provisa.api.metadata_export.model import GovernanceSignal

    masked = {
        tag.asset
        for tag in snapshot.governance_tags
        if tag.signal is GovernanceSignal.MASKED and role in tag.restricted_roles
    }
    if not any(check.asset in masked for check in snapshot.assertions):
        return snapshot
    return replace(
        snapshot,
        assertions=[
            replace(check, outcome=replace(check.outcome, metric_value=None))
            if check.asset in masked
            else check
            for check in snapshot.assertions
        ],
    )


async def _product_inputs(request: Request, snapshot: "MetadataSnapshot") -> dict[str, list[str]]:
    """Each product's nearest physical inputs (REQ-2126) as its caller's lineage gives them --
    product_lineage_for, the answer the product's panel reads: a registered input by its
    relation, one a member reads straight from a source by that source's object."""
    import types

    from provisa.api.admin import lineage_router
    from provisa.api.admin.catalog_scope import catalog_scope
    from provisa.api.app import state

    caller = catalog_scope(types.SimpleNamespace(context={"request": request}))
    inputs: dict[str, list[str]] = {}
    for product in snapshot.data_products:
        found = await lineage_router.product_lineage_for(state, product.id, caller.role)
        assert found is not None  # a product the snapshot holds exists
        inputs[product.id] = [
            f"{i['source_id']}:{i['source_schema']}.{i['source_table']}"
            if i["kind"] == "source"
            else i["relation"]
            for i in found["inputs"]
        ]
    return inputs


async def _run_columns(conn: Any, table_name: str, table_id: int, run_id: str) -> list[dict]:
    """The columns rows profile run ``run_id`` of table ``table_id`` wrote."""
    from sqlalchemy import select

    from provisa.profiler.history import relation

    columns = await relation(conn, table_name, table_id, "columns")
    result = await conn.execute_core(select(columns).where(columns.c.run_id == run_id))
    return [dict(row._mapping) for row in result.fetchall()]


async def _profiles(
    request: Request, snapshot: "MetadataSnapshot"
) -> dict[tuple[str, ...], list[dict]]:
    """Each served profiled table's latest measured run's columns rows, as the caller's role may
    see them (REQ-1592, 2026-10-27): the View Profile Runs rule, governance.safe_run, so a column
    masked to the role or tagged pii shows its shape only and none of its figures."""
    import types

    from sqlalchemy import select

    from provisa.api.admin.catalog_scope import catalog_scope
    from provisa.api.app import state
    from provisa.core.schema_org import registered_tables as rt
    from provisa.profiler import governance, history

    caller = catalog_scope(types.SimpleNamespace(context={"request": request}))
    served = {table.ref.parts for table in snapshot.tables}
    profiles: dict[tuple[str, ...], list[dict]] = {}
    assert state.model_db is not None  # the org's model store is bound with its runtime
    async with state.model_db.acquire() as conn:
        profiled = (
            await conn.execute_core(
                select(rt.c.id, rt.c.source_id, rt.c.schema_name, rt.c.table_name).where(
                    rt.c.profiler_source_id.is_not(None)
                )
            )
        ).fetchall()
        for table in profiled:
            parts = (table.source_id, table.schema_name, table.table_name)
            if parts not in served:
                continue
            latest = await history.previous_runs(
                conn, table.table_name, table.id, datetime.now(UTC), 1
            )
            if not latest:
                continue  # a member not run yet has no figures
            assert caller.role is not None  # a caller served a table acts as a role
            rows = await _run_columns(conn, table.table_name, table.id, latest[0][0])
            rules = await governance.column_rules(conn, state, table.id)
            roles = frozenset(r.strip() for r in caller.role.split(",") if r.strip())
            profiles[parts] = governance.safe_run({"columns": rows}, rules, roles)["columns"]
    return profiles


async def _withheld_definitions(
    request: Request, config: "ProvisaConfig", org_id: str
) -> frozenset[str]:
    """The views (by table URI) whose definition the caller is not shown (REQ-2152, ruling Q7):
    a definition names the columns of every table it reads, so it is shown only to a caller served
    every column of each -- the rule the audited statement text follows. A definition whose tables
    cannot be told is withheld."""
    import types

    from provisa.api.admin.catalog_scope import catalog_scope
    from provisa.api.admin.read_rights import serves_every_column
    from provisa.api.app import state
    from provisa.api.metadata_export.refs import table_uri
    from provisa.compiler.metric_expand import metric_reference_tables
    from provisa.core.repositories.integrity import names_in_sql

    caller = catalog_scope(types.SimpleNamespace(context={"request": request}))
    if caller.whole:
        return frozenset()
    ids: dict[str, list[int]] = {}
    for t in state.tables:
        ids.setdefault(t["table_name"], []).append(t["id"])
        if t.get("alias"):
            ids.setdefault(t["alias"], []).append(t["id"])
    metrics = {m.name: m for m in config.metrics}
    withheld: set[str] = set()
    assert state.model_db is not None  # the org's model store is bound with its runtime
    async with state.model_db.acquire() as conn:
        for table in config.tables:
            if table.view_sql is not None:
                try:
                    names = names_in_sql(table.view_sql)[0]
                except ValueError:
                    withheld.add(table_uri(org_id, table))
                    continue
            elif table.view_metrics is not None:
                # A metric the caller is not granted is one whose tables cannot be told.
                if any(metric not in metrics for metric in table.view_metrics.metrics):
                    withheld.add(table_uri(org_id, table))
                    continue
                names = {
                    name
                    for metric in table.view_metrics.metrics
                    if metric in metrics
                    for name in metric_reference_tables(metric, metrics[metric].expression)
                }
            else:
                continue
            bare = [name.split(".")[-1] for name in names]
            readable = all(name in ids for name in bare) and all(
                [await serves_every_column(conn, caller, i) for name in bare for i in ids[name]]
            )
            if not readable:
                withheld.add(table_uri(org_id, table))
    return frozenset(withheld)


@router.get("/admin/report.xlsx", tags=["admin"])
async def model_report(
    request: Request,
    domains: "list[str] | None" = Query(default=None),
) -> Response:
    """The workbook. One way only — there is deliberately no upload counterpart."""
    from provisa.core.request_context import active_env

    org_id = require_active_org_id(request)
    included = tuple(name for name in wb.OBJECT_SHEETS if has_right(request, SHEET_RIGHTS[name]))
    omitted = tuple(name for name in wb.OBJECT_SHEETS if name not in included)
    scope = _view_scope(request, domains)
    snapshot, config = await _hydrate(org_id)
    snapshot, config = await _as_served(request, _narrow(snapshot, scope), config)
    rows = wb.sheet_rows(
        snapshot,
        config,
        org_id=org_id,
        withheld_definitions=await _withheld_definitions(request, config, org_id),
        profiles=await _profiles(request, snapshot),
        inputs=await _product_inputs(request, snapshot),
    )
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
