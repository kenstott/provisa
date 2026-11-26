# Copyright (c) 2026 Kenneth Stott
# Canary: c52057d7-f5ad-4a87-8ef0-0493ad93e238
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Library page's API (REQ-2185, A TOP-LEVEL LIBRARY; ADD NOW, INDEX IN THE BACKGROUND).

Everyone browses what they may read: the list and a document's detail and citations ask
``glossary_read`` and show only documents in the caller's domains. Changing the library is curated
with the glossary -- term finding adds its documents as proposed -- so it asks the glossary's
curation right, ``glossary_rw`` (REQ-1327: each surface its own right): adding by file or URL,
keeping or discarding a proposed one, changing its domains, refreshing a URL document, uploading a
new version of a file document, removing one and retrying a failed one. A term's panel reads the
documents the term cites, and a curator who may change the term adds and removes them there.

Adding is one step: the document exists at once, and a background job reads and indexes it and then
looks for its terms. Its index status (indexing with progress, ready, failed with the reason) is
a property of the document. A file travels as base64 in a JSON body, as every admin upload does
(import_router.py).

The glossary's in-service terms are embedded as they change (TERMS ARE EMBEDDED AS THEY CHANGE):
every save that brings a term into service, renames it, redefines it or takes it out of service
calls :func:`embed_terms`, which brings the org's term embeddings up to date in the background.
"""

# Requirements: REQ-2185

from __future__ import annotations

import base64
import binascii
from collections.abc import Collection, Coroutine
from dataclasses import asdict
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Request

from provisa.api.admin._guards import require_active_org_id
from provisa.api.admin.capabilities import (
    acting_role_of_request,
    allowed_domains_request,
    require_capability_request,
)
from provisa.api.errors import ApiError
from provisa.core.connection_loop import spawn_background
from provisa.reference_library import citations, library, store, terms
from provisa.security.rights import Capability

if TYPE_CHECKING:
    from provisa.core.database import Connection, Database

router = APIRouter(prefix="/admin/reference-library", tags=["admin", "reference-library"])


def _pool() -> Database:
    from provisa.api.app import state

    if state.model_db is None:
        raise ApiError(503, "reference_library.database_not_connected", "Database not connected")
    return state.model_db


def _guard(request: Request) -> str:
    require_capability_request(request, Capability.GLOSSARY_RW.value)
    return require_active_org_id(request)


def _read_scope(request: Request) -> frozenset[str] | None:
    """The domains whose documents this caller may read, ``None`` for every domain."""
    require_capability_request(request, Capability.GLOSSARY_READ.value)
    require_active_org_id(request)
    return allowed_domains_request(request)


def _may_read(scope: frozenset[str] | None, domains: Collection[str]) -> bool:
    from provisa.reference_library.retrieve import readable

    return readable(scope, frozenset(domains))


async def _readable_terms(
    conn: Connection, scope: frozenset[str] | None, term_ids: Collection[int]
) -> set[int]:
    """Those of ``term_ids`` a caller reading ``scope`` may read."""
    from provisa.core.glossary import readable_term
    from provisa.core.repositories.glossary import term_domains

    held = await term_domains(conn)
    return {t for t in term_ids if readable_term(scope, frozenset(held.get(t, set())))}


def _acting_role(request: Request) -> str:
    return acting_role_of_request(request)


def _launch(work: Coroutine[Any, Any, None]) -> None:
    """Run a document's job on a background worker: it outlives its request
    (provisa.core.connection_loop)."""
    spawn_background(work, name="library-index")


def _refused(exc: library.LibraryRefused) -> ApiError:
    params: dict[str, Any] = {}
    if isinstance(exc, library.AlreadyInLibrary):
        params["document_id"] = exc.document_id
    return ApiError(exc.status, exc.code, str(exc), **params)


async def _embedder() -> library.Embedder:
    from provisa.api.app import state
    from provisa.reference_library.retrieve import org_embedder

    try:
        return await org_embedder(state)
    except ValueError as exc:
        raise ApiError(409, "reference_library.no_vector_model", str(exc)) from None


async def _current_model() -> str | None:
    """The org's vector model; None when the org has none enabled (nothing is then compared)."""
    try:
        return (await _embedder()).model
    except ApiError as exc:
        if exc.code == "reference_library.no_vector_model":
            return None
        raise


def _iso(value: Any) -> str | None:
    return None if value is None else value.isoformat()


def _document(d: store.DocumentRow, current_model: str | None = None) -> dict[str, Any]:
    return {
        "needs_reembedding": (
            d.index_status == store.READY
            and current_model is not None
            and d.embedding_model != current_model
        ),
        "index_status": d.index_status,
        "index_error": d.index_error,
        "index_step": d.index_step,
        "index_done": d.index_done,
        "index_total": d.index_total,
        "terms_note": d.terms_note,
        "id": d.id,
        "title": d.title,
        "source_kind": d.source_kind,
        "source": d.source,
        "media_type": d.media_type,
        "status": d.status,
        "domains": list(d.domains),
        "embedding_model": d.embedding_model,
        "passages": d.passages,
        "added_by": d.added_by,
        "added_at": _iso(d.added_at),
        "refreshed_at": _iso(d.refreshed_at),
        "replaced_at": _iso(d.replaced_at),
    }


async def _domains(conn: Connection, raw: Any) -> list[str]:
    from provisa.core.repositories import domain as domain_repo

    known = {d["id"] for d in await domain_repo.list_all(conn)}
    return library.check_domains(raw, known)


def _file(body: dict) -> library.Content:
    encoded = body.get("content_b64")
    if not isinstance(encoded, str) or not encoded:
        raise library.LibraryRefused("a file document's content is sent as content_b64")
    try:
        data = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError):
        raise library.LibraryRefused("content_b64 is not base64") from None
    return library.file_content(body.get("file_name"), data)


def _user_id(request: Request) -> str | None:
    identity = getattr(request.state, "identity", None)
    return getattr(identity, "user_id", None) if identity is not None else None


@router.get("/documents")
async def list_documents(request: Request) -> dict:
    scope = _read_scope(request)
    model = await _current_model()
    async with _pool().acquire() as conn:
        return {
            "embedding_model": model,
            "documents": [
                _document(d, model)
                for d in await store.documents(conn)
                if _may_read(scope, d.domains)
            ],
        }


async def _read_document(
    conn: Connection, document_id: int, scope: frozenset[str] | None
) -> store.DocumentRow:
    """The document, if this caller may read it: one they may not is one that is not there."""
    found = await store.document(conn, document_id)
    if found is None or not _may_read(scope, found.domains):
        raise _refused(library.DocumentNotFound(document_id))
    return found


@router.get("/documents/{document_id}")
async def get_document(document_id: int, request: Request) -> dict:
    scope = _read_scope(request)
    async with _pool().acquire() as conn:
        found = await _read_document(conn, document_id, scope)
        passages = await store.passages(conn, document_id)
    return {
        **_document(found, await _current_model()),
        "passage_list": [
            {"ordinal": o, "section": s, "page": p, "text": t} for o, s, p, t in passages
        ],
    }


@router.get("/documents/{document_id}/cited-by")
async def cited_by(
    document_id: int,
    request: Request,
    section_heading: str | None = None,
    section_page: int | None = None,
) -> dict:
    """What cites the document, among what the caller may read: each citation with its state, a
    quote the document no longer holds flagged as drifted. REQ-2208: and the tags whose definition
    cites it and the tag assignments that cite it, each citation current, drifted or broken; with
    ``section_heading`` or ``section_page``, only what cites that section of it."""
    scope = _read_scope(request)
    async with _pool().acquire() as conn:
        await _read_document(conn, document_id, scope)
        citing = await terms.cited_by(conn, document_id)
        readable = await _readable_terms(conn, scope, {c.term_id for c in citing})
        by_tags = await _cited_by_tags(conn, request, document_id, section_heading, section_page)
    shown = [c for c in citing if c.term_id in readable]
    return {
        "terms": [asdict(c) for c in shown],
        "drifted": sum(c.state == terms.DRIFTED for c in shown),
        **by_tags,
    }


def _citation(c: citations.Citation) -> dict:
    return {
        "section_heading": c.section_heading,
        "section_page": c.section_page,
        "text_sha256": c.text_sha256,
        "cited_by": c.cited_by,
        "cited_at": c.cited_at.isoformat(),
        "state": c.state,
        "needs_review": c.needs_review,
    }


async def _cited_by_tags(
    conn: Connection,
    request: Request,
    document_id: int,
    section_heading: str | None,
    section_page: int | None,
) -> dict:  # REQ-2208
    """The tags whose definition cites the document (or the section), and the tag assignments
    citing it that the caller may read, through the tag assignments' one read gate."""
    from provisa.api.admin.schema_query import readable_tag_assignments
    from provisa.core.repositories import tag as tag_repo

    found = await citations.citing(conn, document_id, section_heading, section_page)
    by_assignment = {
        (c.tag_id, c.object_key) for c in found if c.citer_kind == citations.TAG_ASSIGNMENT
    }
    held = [
        *await tag_repo.list_assignments(conn),
        *await tag_repo.list_library_assignments(conn),
    ]
    rows = [r for r in held if (r["base_tag_id"], r["object_key"]) in by_assignment]
    if rows:
        rows = await readable_tag_assignments(request, conn, rows)
    assignments = []
    for r in rows:
        for c in found:
            if (c.citer_kind, c.tag_id, c.object_key) == (
                citations.TAG_ASSIGNMENT,
                r["base_tag_id"],
                r["object_key"],
            ):
                assignments.append(
                    {
                        "tag_id": r["tag_id"],
                        "object_type": r["object_type"],
                        "object_key": r["object_key"],
                        **_citation(c),
                    }
                )
    return {
        "tags": [
            {"tag_id": c.tag_id, **_citation(c)} for c in found if c.citer_kind == citations.TAG
        ],
        "tag_assignments": assignments,
    }


def reviews_due(cited: dict) -> int:  # REQ-2259, REQ-2208
    """How many review items a document's citations hold (``cited_by``'s answer): each term
    citation whose words the document no longer holds, and each tag or tag assignment citation
    whose section changed or is gone."""
    return int(cited["drifted"]) + sum(
        bool(c["needs_review"]) for c in [*cited["tags"], *cited["tag_assignments"]]
    )


@router.get("/reviews-due")
async def documents_reviews_due(request: Request) -> dict:  # REQ-2259, REQ-2208
    """Context Health's documents card: each document the caller reads whose citations hold a
    review item (:func:`reviews_due`), with how many, among the citations the caller reads."""
    scope = _read_scope(request)
    async with _pool().acquire() as conn:
        readable = [d for d in await store.documents(conn) if _may_read(scope, d.domains)]
    due = []
    for d in readable:
        count = reviews_due(await cited_by(d.id, request))
        if count:
            due.append({"id": d.id, "title": d.title, "reviews": count})
    return {"documents": due}


# -- a term's documents -------------------------------------------------------------------------


async def _held_term(conn: Connection, term_id: int, request: Request) -> None:
    """The term, if this caller may read it: one outside their domains is refused, one that is
    not there is not found (the glossary's own gate, glossary_router.get_term)."""
    from provisa.api.admin import glossary_router as glossary
    from provisa.core.repositories import glossary as glossary_repo

    if await glossary_repo.get_term(conn, term_id) is None:
        raise ApiError(404, "glossary.term_not_found", f"term {term_id} not found")
    await glossary._require_term_readable(conn, term_id, request)


async def _curatable_term(conn: Connection, term_id: int, request: Request) -> None:
    """The term, if this caller may change it: the curation right the review queue asks
    (glossary_rw), and the term's own curation gate (glossary_router._curation_refusal)."""
    from provisa.api.admin import glossary_router as glossary

    await _held_term(conn, term_id, request)
    refusal = await glossary._curation_refusal(conn, term_id, request)
    if refusal is not None:
        raise refusal


def _term_citation(c: terms.TermCitation) -> dict:
    return {
        "key": c.key,
        "origin": c.origin,
        "status": c.status,
        "document_id": c.document_id,
        "document_title": c.document_title,
        "section_heading": c.section_heading,
        "section_page": c.section_page,
        "state": c.state,
        "needs_review": c.state != citations.CURRENT,
        "quote": c.quote,
        "ordinal": c.ordinal,
        "draft_id": c.draft_id,
        "cite_key": c.cite_key,
        "cited_by": c.cited_by,
    }


@router.get("/terms/{term_id}/citations")
async def term_citations(term_id: int, request: Request) -> dict:  # REQ-2208, REQ-2179
    """The library documents the term cites, among those the caller may read: the passages its
    accepted drafts were found in, the sections picked for it, and the passages of drafts awaiting
    review that propose merging into it (``proposed``), each with its state. A scan's citation is
    shown only where its scan is readable too, as the review queue shows it."""
    from provisa.core.glossary import readable_term

    scope = _read_scope(request)
    async with _pool().acquire() as conn:
        await _held_term(conn, term_id, request)
        found = await terms.term_citations(conn, term_id)
        readable = {d.id for d in await store.documents(conn) if _may_read(scope, d.domains)}
    shown = [
        c
        for c in found
        if (c.document_id is None or c.document_id in readable)
        and (c.origin != terms.SCAN or readable_term(scope, frozenset(c.domains)))
    ]
    return {"citations": [_term_citation(c) for c in shown]}


def _place(body: dict) -> citations.Place:
    """The document, and the section of it by heading or by page, a body names."""
    document_id = body.get("document_id")
    heading = body.get("section_heading")
    page = body.get("section_page")
    if not isinstance(document_id, int) or isinstance(document_id, bool):
        raise ApiError(400, citations.CitationRefused.code, "document_id is a document's id")
    if heading is not None and not isinstance(heading, str):
        raise ApiError(400, citations.CitationRefused.code, "section_heading is text")
    if page is not None and (not isinstance(page, int) or isinstance(page, bool)):
        raise ApiError(400, citations.CitationRefused.code, "section_page is a page number")
    if heading is not None and page is not None:
        raise ApiError(
            400,
            citations.CitationRefused.code,
            "a section is named by its heading or, when it has none, by its page",
        )
    return citations.Place(document_id, heading, page)


@router.post("/terms/{term_id}/citations")
async def cite_for_term(term_id: int, body: dict, request: Request) -> dict:  # REQ-2208
    """The term cites a library document, or a section of it, the caller may read; citing it
    again records its text as it reads now, which confirms a drifted citation. Asks the curation
    right of the review queue and the term's own curation gate."""
    scope = _read_scope(request)
    _guard(request)
    place = _place(body)
    try:
        async with _pool().acquire() as conn:
            async with conn.transaction():
                await _curatable_term(conn, term_id, request)
                await _read_document(conn, place.document_id, scope)
                await citations.cite(
                    conn,
                    citations.TERM,
                    None,
                    citations.term_key(term_id),
                    place,
                    _user_id(request),
                )
            found = await terms.term_citations(conn, term_id)
    except library.LibraryRefused as exc:
        raise _refused(exc) from None
    [cited] = [c for c in found if c.origin == terms.PICKED and c.cite_key == place.key()]
    return _term_citation(cited)


@router.delete("/terms/{term_id}/citations")
async def uncite_for_term(  # REQ-2208, REQ-2179
    term_id: int,
    request: Request,
    cite_key: str | None = None,
    draft_id: int | None = None,
    quote: str | None = None,
) -> dict:
    """Take a citation off the term: a picked one by its ``cite_key``, or a scan's by its
    ``draft_id`` and ``quote`` (the passage leaves the term's accepted draft). A draft awaiting
    review is rejected through the review queue, not here."""
    _read_scope(request)
    _guard(request)
    if (cite_key is None) == (draft_id is None) or (draft_id is not None and quote is None):
        raise ApiError(
            400,
            citations.CitationRefused.code,
            "name a picked citation by cite_key, or a scan's by draft_id and quote",
        )
    async with _pool().acquire() as conn:
        async with conn.transaction():
            await _curatable_term(conn, term_id, request)
            if cite_key is not None:
                gone = await citations.uncite_key(
                    conn, citations.TERM, None, citations.term_key(term_id), cite_key
                )
            else:
                assert draft_id is not None and quote is not None
                gone = await terms.drop_passage(conn, term_id, draft_id, quote)
    if not gone:
        raise ApiError(
            404, "reference_library.citation_not_found", f"term {term_id} cites no such place"
        )
    return {"removed": True}


async def _job_embedder() -> library.Embedder:
    """The org's vector model for a background job: an org without one fails the job by name."""
    try:
        return await _embedder()
    except ApiError as exc:
        raise library.LibraryRefused(str(exc.detail)) from None


async def run_index(
    pool: Database, document_id: int, org_id: str, role: str, started_by: str | None
) -> None:
    """A document's background job: read it, index it and, once it is ready, look for its terms."""
    from provisa.api.admin import term_finding_router as finding

    document = await library.index_pending(pool, document_id, _job_embedder, org_id)
    if document is not None:
        await finding.find_terms_in(pool, document, role, started_by)


def _launch_id() -> str:
    from provisa.api.admin import term_finding_router as finding

    return finding.this_launch()


@router.post("/documents")
async def add_document(body: dict, request: Request) -> dict:
    """Add a document by ``url``, or by ``file_name`` with ``content_b64``; an admin's document
    is kept. It exists at once, indexing; a background job reads it and looks for its terms."""
    org_id = _guard(request)
    pool = _pool()
    try:
        if body.get("url") is not None and body.get("content_b64") is not None:
            raise library.LibraryRefused("a document is added by a URL or a file, not both")
        async with pool.acquire() as conn:
            domains = await _domains(conn, body.get("domains"))
            if body.get("url") is not None:
                kind, source, content = store.URL, library.check_url(body["url"]), None
            else:
                content = _file(body)
                kind, source = store.FILE, content.name
            document_id = await library.add_pending(
                conn,
                source_kind=kind,
                source=source,
                content=content,
                domains=domains,
                status=store.KEPT,
                added_by=_user_id(request),
                launch=_launch_id(),
                title=body.get("title"),
            )
            found = await store.document(conn, document_id)
    except library.LibraryRefused as exc:
        raise _refused(exc) from None
    assert found is not None
    _launch(run_index(pool, document_id, org_id, _acting_role(request), _user_id(request)))
    return _document(found, await _current_model())


@router.post("/documents/{document_id}/retry")
async def retry_document(document_id: int, request: Request) -> dict:
    """Run a failed document's job again; a file is read from the bytes held for it."""
    org_id = _guard(request)
    pool = _pool()
    try:
        async with pool.acquire() as conn:
            await library.retry(conn, document_id, _launch_id())
            found = await store.document(conn, document_id)
    except library.LibraryRefused as exc:
        raise _refused(exc) from None
    assert found is not None
    _launch(run_index(pool, document_id, org_id, _acting_role(request), _user_id(request)))
    return _document(found, await _current_model())


@router.patch("/documents/{document_id}")
async def change_document(document_id: int, body: dict, request: Request) -> dict:
    """Change a document's ``title`` or ``domains``, or keep a proposed one (``status: kept``)."""
    _guard(request)
    unknown = sorted(set(body) - {"title", "domains", "status"})
    try:
        if unknown:
            raise library.LibraryRefused(f"a document's {unknown[0]!r} is not changed here")
        if "status" in body and body["status"] != store.KEPT:
            raise library.LibraryRefused(
                "a proposed document is kept here; discarding it removes it"
            )
        async with _pool().acquire() as conn:
            async with conn.transaction():
                if "title" in body:
                    await library.rename(conn, document_id, body["title"])
                if "domains" in body:
                    await library.set_domains(
                        conn, document_id, await _domains(conn, body["domains"])
                    )
                if "status" in body:
                    await library.keep(conn, document_id)
            found = await store.document(conn, document_id)
            if found is None:
                raise library.DocumentNotFound(document_id)
    except library.LibraryRefused as exc:
        raise _refused(exc) from None
    return _document(found, await _current_model())


@router.post("/documents/{document_id}/refresh")
async def refresh_document(document_id: int, request: Request) -> dict:
    """Fetch a URL document again and replace its content."""
    org_id = _guard(request)
    try:
        async with _pool().acquire() as conn:
            await library.refresh(conn, document_id, await _embedder(), org_id)
            found = await store.document(conn, document_id)
    except library.LibraryRefused as exc:
        raise _refused(exc) from None
    assert found is not None
    return _document(found, await _current_model())


@router.post("/documents/{document_id}/reembed")
async def reembed_document(document_id: int, request: Request) -> dict:
    """Re-embed a document's stored passages with the org's current vector model."""
    _guard(request)
    try:
        embedder = await _embedder()
        async with _pool().acquire() as conn:
            await library.reembed(conn, document_id, embedder)
            found = await store.document(conn, document_id)
    except library.LibraryRefused as exc:
        raise _refused(exc) from None
    assert found is not None
    return _document(found, embedder.model)


@router.post("/reembed")
async def reembed_documents(request: Request) -> dict:
    """Re-embed every document embedded by an earlier model; each failed one is named."""
    _guard(request)
    embedder = await _embedder()
    async with _pool().acquire() as conn:
        outcome = await library.reembed_all(conn, embedder)
    return {
        "embedding_model": embedder.model,
        "reembedded": outcome.reembedded,
        "failed": [
            {"document_id": f.document_id, "title": f.title, "reason": f.reason}
            for f in outcome.failed
        ],
    }


@router.post("/documents/{document_id}/version")
async def upload_version(document_id: int, body: dict, request: Request) -> dict:
    """Replace a file document's content with an uploaded file (``file_name``, ``content_b64``)."""
    _guard(request)
    try:
        content = _file(body)
        async with _pool().acquire() as conn:
            await library.new_version(conn, document_id, content, await _embedder())
            found = await store.document(conn, document_id)
    except library.LibraryRefused as exc:
        raise _refused(exc) from None
    assert found is not None
    return _document(found, await _current_model())


@router.delete("/documents/{document_id}")
async def remove_document(document_id: int, request: Request) -> dict:
    """Remove a document, or discard a proposed one."""
    _guard(request)
    try:
        async with _pool().acquire() as conn:
            await library.remove(conn, document_id)
    except library.LibraryRefused as exc:
        raise _refused(exc) from None
    return {"removed": document_id}


async def embed_terms(org_id: str) -> None:
    """Bring the embeddings of ``org_id``'s in-service glossary terms up to date in the background
    of a save (REQ-2185, TERMS ARE EMBEDDED AS THEY CHANGE): a term that entered service or whose
    name or definition changed is embedded by the org's vector model, and a term that left service
    loses its embedding (terms.refresh). Every path that changes a term's service state, name or
    definition calls this, after its save. An org with no vector model schedules nothing. The work
    runs on a background worker (connection_loop.spawn_background), which logs a failure with its
    traceback under its name; the save it follows has already been answered."""
    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org(org_id)
    try:
        if await _current_model() is None:
            return
        spawn_background(_embed_terms(_pool()), name=f"glossary term embeddings of {org_id}")
    finally:
        reset_current_org(token)


async def _embed_terms(pool: Database) -> None:
    embedder = await _embedder()
    async with pool.acquire() as conn:
        await terms.refresh(conn, await terms.in_service(conn), embedder)
