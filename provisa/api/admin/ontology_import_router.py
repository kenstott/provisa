# Copyright (c) 2026 Kenneth Stott
# Canary: e3c91b58-27d4-4f6a-b0e1-95a4c8d7f206
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Import an ontology into the glossary as an alignment: the admin API for it (REQ-2237).

A steward uploads an Apache Ossie ontology, an RDF/OWL file, or a whole release of one as a zip
archive of its module files, in the domains they work in. It is
read at once (provisa/ontology_import): its classes, properties, value types and concepts are
kept as items, each with the alignments proposed between it and the glossary's terms the steward
may read, or another item of the file. The steward edits or excludes items, approves or rejects
each proposal, adds alignments of their own, and applies the import: every pending item becomes a
draft term carrying its definition and IRI, or the term it was approved as the same entity as,
and every approved alignment becomes an edge. Nothing is approved for them.

Curation's rules hold throughout (glossary_router.py): every endpoint takes ``glossary_rw``; an
import is seen only by those who reach one of its domains; terms are created only in domains the
caller curates; a term the import would change, the caller must be allowed to change.
"""

# Requirements: REQ-2237

from __future__ import annotations

import base64
import binascii
import json
from typing import TYPE_CHECKING, Any, cast

from fastapi import APIRouter, Request
from fastapi.responses import Response

from provisa.api.admin import glossary_router as glossary
from provisa.api.admin._guards import require_active_org_id
from provisa.api.errors import ApiError
from provisa.core.repositories import glossary as glossary_repo
from provisa.ontology_import import align, apply, parse, store

if TYPE_CHECKING:
    from provisa.core.database import Connection, Database

router = APIRouter(prefix="/admin/glossary/ontology-imports", tags=["admin", "glossary"])

#: The largest ontology file an import reads.
MAX_BYTES = 32 * 1024 * 1024


def _pool() -> Database:
    from provisa.api.app import state

    if state.model_db is None:
        raise ApiError(503, "ontology_import.database_not_connected", "Database not connected")
    return state.model_db


def _refused(exc: Exception) -> ApiError:
    """A refusal of the import's, or a glossary refusal (the repository raises ValueError)."""
    if isinstance(exc, apply.ApplyRefused):
        return ApiError(
            exc.status, "ontology_import.apply_refused", str(exc), problems=exc.problems
        )
    status = exc.status if isinstance(exc, apply.ImportRefused) else 400
    code = "ontology_import.not_found" if status == 404 else "ontology_import.refused"
    return ApiError(status, code, str(exc))


def _iso(value: Any) -> str | None:
    return None if value is None else value.isoformat()


def _readable(request: Request, domains: Any) -> bool:
    from provisa.core.glossary import readable_term

    return readable_term(glossary._authority(request), frozenset(domains))


def _import_view(i: store.ImportRow, counts: dict[str, dict[str, int]] | None = None) -> dict:
    return {
        "id": i.id,
        "title": i.title,
        "file_name": i.file_name,
        "format": i.format,
        "domains": i.domains,
        "summary": i.summary,
        "imported_by": i.imported_by,
        "imported_at": _iso(i.imported_at),
        "applied_by": i.applied_by,
        "applied_at": _iso(i.applied_at),
        "counts": counts or {"items": {}, "alignments": {}},
    }


async def _readable_terms(conn: Connection, request: Request) -> list[dict[str, Any]]:
    """The glossary terms the caller may read."""
    return await glossary_repo.list_terms(
        cast("Connection", conn), domains=glossary._authority(request)
    )


def _role(term: dict[str, Any]) -> str:
    if term["table_count"]:
        return parse.CLASS
    return parse.PROPERTY if term["ref_count"] else parse.CONCEPT


def _term_view(term_id: int | None, terms: dict[int, dict[str, Any]]) -> dict[str, Any] | None:
    """A term as the review shows it; one outside the caller's domains is never named
    (REQ-1591)."""
    if term_id is None:
        return None
    term = terms.get(term_id)
    if term is None:
        return {"outside_domains": True}
    return {
        "term_id": term_id,
        "name": term["name"],
        "definition": term["definition"],
        "role": _role(term),
        "status": term["status"],
        "grounding": term["grounding"],
        "iri": term["iri"],
    }


def _alignment_view(
    a: store.AlignmentRow, terms: dict[int, dict[str, Any]], names: dict[int, str]
) -> dict[str, Any]:
    return {
        "id": a.id,
        "item_id": a.item_id,
        "rel_type": a.rel_type,
        "item_is_source": a.item_is_source,
        "basis": a.basis,
        "status": a.status,
        "use_definition": a.use_definition,
        "decided_by": a.decided_by,
        "decided_at": _iso(a.decided_at),
        "term": _term_view(a.term_id, terms),
        "other_item": None
        if a.other_item_id is None
        else {"item_id": a.other_item_id, "name": names.get(a.other_item_id)},
    }


async def _review(conn: Connection, request: Request, found: store.ImportRow) -> dict[str, Any]:
    """The import with every item and the alignments proposed for it."""
    items = await store.items(conn, found.id)
    alignments = await store.alignments(conn, found.id)
    terms = {t["id"]: t for t in await _readable_terms(conn, request)}
    names = {i.id: i.name for i in items}
    by_item: dict[int, list[dict[str, Any]]] = {}
    for a in alignments:
        by_item.setdefault(a.item_id, []).append(_alignment_view(a, terms, names))
    counts = (await store.counts(conn)).get(found.id)
    return {
        "import": _import_view(found, counts),
        "items": [
            {
                "id": i.id,
                "name": i.name,
                "kind": i.kind,
                "definition": i.definition,
                "iri": i.iri,
                "keys": i.payload["keys"],
                "labels": i.payload["labels"],
                "statements": i.payload.get("statements", []),
                "status": i.status,
                "term": _term_view(i.term_id, terms),
                "alignments": by_item.get(i.id, []),
            }
            for i in items
        ],
    }


async def _held(conn: Connection, import_id: int, request: Request) -> store.ImportRow:
    found = await store.import_(conn, import_id)
    if found is None or not _readable(request, found.domains):
        raise ApiError(404, "ontology_import.not_found", f"there is no ontology import {import_id}")
    return found


async def _require_curatable(conn: Connection, term_ids: Any, request: Request) -> None:
    for term_id in sorted(term_ids):
        refusal = await glossary._curation_refusal(cast("Connection", conn), term_id, request)
        if refusal is not None:
            raise refusal


def _content(body: dict[str, Any]) -> tuple[str, bytes]:
    name = body.get("file_name")
    if not isinstance(name, str) or not name.strip():
        raise apply.ImportRefused("an ontology file is named by its file name")
    encoded = body.get("content_b64")
    if not isinstance(encoded, str) or not encoded:
        raise apply.ImportRefused("an ontology file's content is sent as content_b64")
    try:
        data = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError):
        raise apply.ImportRefused("content_b64 is not base64") from None
    if len(data) > MAX_BYTES:
        raise apply.ImportRefused(f"{name!r} is larger than {MAX_BYTES // (1024 * 1024)} MB")
    return name.strip(), data


@router.post("")
async def import_ontology(request: Request) -> dict:
    """Read an ontology file (``file_name``, ``content_b64``, and ``format`` when its name does
    not say: ossie, turtle, xml, nt or json-ld) in the steward's working ``domains``, and propose
    its alignments with the glossary."""
    glossary._require_glossary_rw(request)
    require_active_org_id(request)
    body = await request.json()
    unknown = sorted(set(body) - {"file_name", "content_b64", "format", "domains"})
    if unknown:
        raise ApiError(
            400, "ontology_import.refused", f"an import's {unknown[0]!r} is not set here"
        )
    declared = glossary._declared_domains(request, body.get("domains"), current=set())
    try:
        file_name, data = _content(body)
        fmt = body.get("format")
        if fmt is not None and not isinstance(fmt, str):
            raise apply.ImportRefused(f"an ontology's format is one of {', '.join(parse.FORMATS)}")
        parsed = parse.parse(file_name, data, fmt)
    except (apply.ImportRefused, parse.ImportRefused) as exc:
        raise _refused(exc) from None
    async with _pool().acquire() as conn:
        terms = [
            align.Term(t["id"], t["name"], _role(t), t["iri"])
            for t in await _readable_terms(conn, request)
            if t["status"] not in ("deprecated", "retired")
        ]
        proposals = align.propose(parsed.items, parsed.edges, terms)
        async with conn.transaction():
            import_id = await store.create(
                conn,
                parsed,
                proposals,
                file_name=file_name,
                domains=sorted(declared),
                by=glossary._caller_id(request),
            )
        found = await store.import_(conn, import_id)
        assert found is not None
        return await _review(conn, request, found)


@router.get("")
async def list_imports(request: Request) -> dict:
    """The imports the caller may see, newest first, with their items and alignments counted by
    status."""
    glossary._require_glossary_rw(request)
    require_active_org_id(request)
    async with _pool().acquire() as conn:
        found = await store.imports(conn)
        counts = await store.counts(conn)
    return {
        "imports": [
            _import_view(i, counts.get(i.id)) for i in found if _readable(request, i.domains)
        ]
    }


@router.get("/{import_id}")
async def get_import(import_id: int, request: Request) -> dict:
    """The import, its items and the alignments proposed for each."""
    glossary._require_glossary_rw(request)
    require_active_org_id(request)
    async with _pool().acquire() as conn:
        return await _review(conn, request, await _held(conn, import_id, request))


@router.delete("/{import_id}")
async def discard_import(import_id: int, request: Request) -> dict:
    """Discard the import and its proposals. The terms it made and the edges it wrote stay."""
    glossary._require_glossary_rw(request)
    require_active_org_id(request)
    async with _pool().acquire() as conn:
        await _held(conn, import_id, request)
        await store.remove(conn, import_id)
    return {"id": import_id, "discarded": True}


@router.get("/{import_id}/export")
async def export_import(import_id: int, request: Request, format: str = "turtle") -> Response:
    """The ontology the import brought in, written back out as RDF (``format``: turtle, xml, nt
    or json-ld): the terms it applied that the caller may read, with what each keeps of the
    ontology and the edges between them (provisa.ontology_import.export)."""
    from provisa.ontology_import import export

    glossary._require_glossary_rw(request)
    require_active_org_id(request)
    if format not in export.FORMATS:
        raise ApiError(
            400,
            "ontology_import.refused",
            f"an ontology is exported as one of {', '.join(export.FORMATS)}",
        )
    async with _pool().acquire() as conn:
        await _held(conn, import_id, request)
        applied = {i.term_id for i in await store.items(conn, import_id) if i.term_id is not None}
        terms = [t for t in await _readable_terms(conn, request) if t["id"] in applied]
        ids = [t["id"] for t in terms]
        edges = [e for e in await glossary_repo.term_edges(cast("Connection", conn)) if e[0] in ids]
        statements = await glossary_repo.term_statements(cast("Connection", conn), ids)
    exported = export.graph_of(terms, edges, statements)
    return Response(
        content=export.serialize(exported, format),
        media_type=export.MEDIA_TYPES[format],
        headers={"X-Provisa-Left-Out": json.dumps(exported.left_out, sort_keys=True)},
    )


@router.patch("/items/{item_id}")
async def edit_item(item_id: int, request: Request) -> dict:
    """Edit a pending item's ``name`` or ``definition``, or exclude it (``excluded``)."""
    glossary._require_glossary_rw(request)
    require_active_org_id(request)
    body = await request.json()
    try:
        async with _pool().acquire() as conn:
            found = await apply.held_item(conn, item_id)
            imported = await _held(conn, found.import_id, request)
            name, definition, status = apply.edited(found, body)
            await store.edit_item(conn, item_id, name=name, definition=definition, status=status)
            return await _review(conn, request, imported)
    except apply.ImportRefused as exc:
        raise _refused(exc) from None


@router.post("/{import_id}/alignments")
async def add_alignment(import_id: int, request: Request) -> dict:
    """Align an item as the steward chooses: ``item_id`` with ``term_id`` or ``other_item_id``,
    of ``rel_type`` (SAME_AS or an edge type), read from the item unless ``item_is_source`` is
    false. Added approved: adding it is the steward's decision."""
    glossary._require_glossary_rw(request)
    require_active_org_id(request)
    body = await request.json()
    try:
        async with _pool().acquire() as conn:
            imported = await _held(conn, import_id, request)
            term_id = body.get("term_id")
            if isinstance(term_id, int) and not isinstance(term_id, bool):
                await _require_curatable(conn, [term_id], request)
            alignment_id = await apply.add(conn, imported, body, glossary._caller_id(request))
            added = await store.alignment(conn, alignment_id)
            assert added is not None
            terms = {t["id"]: t for t in await _readable_terms(conn, request)}
            names = {i.id: i.name for i in await store.items(conn, import_id)}
            return _alignment_view(added, terms, names)
    except apply.ImportRefused as exc:
        raise _refused(exc) from None


async def _decide(
    conn: Connection, alignment_id: int, request: Request, status: str, use_definition: Any
) -> store.AlignmentRow:
    found = await apply.held_alignment(conn, alignment_id)
    await _held(conn, found.import_id, request)
    if status == store.APPROVED and found.term_id is not None:
        await _require_curatable(conn, [found.term_id], request)
    await apply.decide(
        conn, found, status, use_definition=use_definition, by=glossary._caller_id(request)
    )
    decided = await store.alignment(conn, alignment_id)
    assert decided is not None
    return decided


@router.post("/alignments/{alignment_id}/approve")
async def approve_alignment(alignment_id: int, request: Request) -> dict:
    """Approve a proposed alignment; ``use_definition`` gives a term aligned as the same entity
    the item's definition in place of its own."""
    glossary._require_glossary_rw(request)
    require_active_org_id(request)
    body = await request.json()
    try:
        async with _pool().acquire() as conn:
            decided = await _decide(
                conn, alignment_id, request, store.APPROVED, body.get("use_definition", False)
            )
            return {"id": decided.id, "status": decided.status}
    except apply.ImportRefused as exc:
        raise _refused(exc) from None


@router.post("/alignments/{alignment_id}/reject")
async def reject_alignment(alignment_id: int, request: Request) -> dict:
    glossary._require_glossary_rw(request)
    require_active_org_id(request)
    try:
        async with _pool().acquire() as conn:
            decided = await _decide(conn, alignment_id, request, store.REJECTED, False)
            return {"id": decided.id, "status": decided.status}
    except apply.ImportRefused as exc:
        raise _refused(exc) from None


@router.post("/{import_id}/approve")
async def bulk_approve(import_id: int, request: Request) -> dict:
    """Approve several of the import's alignments at once (``alignment_ids``). Each is decided on
    its own; one that is refused is named with why, and the others stand."""
    glossary._require_glossary_rw(request)
    require_active_org_id(request)
    body = await request.json()
    ids = body.get("alignment_ids")
    if not isinstance(ids, list) or not all(
        isinstance(i, int) and not isinstance(i, bool) for i in ids
    ):
        raise ApiError(400, "ontology_import.refused", "alignment_ids is a list of alignment ids")
    approved: list[int] = []
    refused: list[dict[str, Any]] = []
    async with _pool().acquire() as conn:
        await _held(conn, import_id, request)
        for alignment_id in ids:
            try:
                found = await apply.held_alignment(conn, alignment_id)
                if found.import_id != import_id:
                    raise apply.NotFound(f"import {import_id} holds no alignment {alignment_id}")
                await _decide(conn, alignment_id, request, store.APPROVED, False)
                approved.append(alignment_id)
            except (apply.ImportRefused, ApiError) as exc:
                message = exc.detail if isinstance(exc, ApiError) else str(exc)
                refused.append({"id": alignment_id, "message": message})
    return {"approved": approved, "refused": refused}


@router.post("/{import_id}/apply")
async def apply_import(import_id: int, request: Request) -> dict:
    """Apply the import: its pending items become draft terms (or the terms approved as the same
    entity take their IRIs), and its approved alignments become edges. All or nothing: what stops
    it is refused together (``problems``), each naming what to do."""
    glossary._require_glossary_rw(request)
    org_id = require_active_org_id(request)
    try:
        async with _pool().acquire() as conn:
            imported = await _held(conn, import_id, request)
            domains = glossary._declared_domains(request, imported.domains, current=set())
            await _require_curatable(
                conn, apply.touched_terms(await store.alignments(conn, import_id)), request
            )
            async with conn.transaction():
                done = await apply.apply(conn, imported, domains, glossary._caller_id(request))
    except (apply.ImportRefused, ValueError) as exc:
        raise _refused(exc) from None
    await glossary._notify(org_id, "ontology imported")
    return {
        "id": import_id,
        "created": done.created,
        "aligned": done.aligned,
        "edges": done.edges,
        "waiting": done.waiting,
    }
