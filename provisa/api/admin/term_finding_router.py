# Copyright (c) 2026 Kenneth Stott
# Canary: 5a3f8c21-7b6e-4d09-91a4-e0c2d7b58f16
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Find terms in a document: the glossary's API for it (REQ-2179, REQ-2185).

A steward picks a reference library document they may read, or gives a new one as a file or an
http(s) URL, which joins the library as a proposed document in the domains the steward is working
in (a document already in the library is not added again: the scan reads the library's copy).
The scan runs in the background with the org's AI model, held to the role's LLM budget, and its
candidates land as drafts in the review queue, where the steward edits, accepts, merges or
rejects each, or accepts several at once. Scanning a document again is a new scan whose drafts
show the candidates the glossary does not hold yet and those that match or duplicate its terms
and synonyms; nothing is re-reviewed automatically.

Curation's rules hold throughout (glossary_router.py): the queue takes ``glossary_rw``; a term is
added only in domains the caller curates; a draft merges only into a term the caller may change.
"""

# Requirements: REQ-2179, REQ-2185

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, cast

from fastapi import APIRouter, Query, Request

from provisa.api.admin import glossary_router as glossary
from provisa.api.admin._guards import require_active_org_id
from provisa.api.errors import ApiError
from provisa.core.repositories import glossary as glossary_repo
from provisa.reference_library import library, store
from provisa.term_finding import queue, review, scan

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from provisa.core.database import Connection, Database

log = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/glossary/term-finding", tags=["admin", "glossary"])

#: The scans running now, held so none is collected before it finishes.
_RUNNING: set[asyncio.Task] = set()


def _pool() -> Database:
    from provisa.api.app import state

    if state.model_db is None:
        raise ApiError(503, "term_finding.database_not_connected", "Database not connected")
    return state.model_db


def _readable(request: Request, domains: Any) -> bool:
    from provisa.core.glossary import readable_term

    return readable_term(glossary._authority(request), frozenset(domains))


def _iso(value: Any) -> str | None:
    return None if value is None else value.isoformat()


def _document(d: store.DocumentRow) -> dict[str, Any]:
    return {
        "id": d.id,
        "title": d.title,
        "source_kind": d.source_kind,
        "source": d.source,
        "status": d.status,
        "domains": list(d.domains),
        "passages": d.passages,
        "added_at": _iso(d.added_at),
        "refreshed_at": _iso(d.refreshed_at),
        "replaced_at": _iso(d.replaced_at),
    }


def _scan(s: queue.ScanRow, counts: dict[str, int] | None = None) -> dict[str, Any]:
    return {
        "id": s.id,
        "document_id": s.document_id,
        "document_title": s.document_title,
        "document_source": s.document_source,
        # REQ-2185: a removed document's citations stay, marked as citing a removed document.
        "document_removed": s.document_id is None,
        "domains": s.domains,
        "status": s.status,
        "error": s.error,
        "summary": s.summary,
        "started_by": s.started_by,
        "started_at": _iso(s.started_at),
        "finished_at": _iso(s.finished_at),
        "counts": counts or {},
    }


def _refused(exc: library.LibraryRefused | review.ReviewRefused) -> ApiError:
    if isinstance(exc, library.LibraryRefused):
        params: dict[str, Any] = {}
        if isinstance(exc, library.AlreadyInLibrary):
            params["document_id"] = exc.document_id
        return ApiError(exc.status, exc.code, str(exc), **params)
    code = "term_finding.not_found" if exc.status == 404 else "term_finding.refused"
    return ApiError(exc.status, code, str(exc))


# -- choosing the document ----------------------------------------------------------------------


@router.get("/documents")
async def list_documents(request: Request) -> dict:
    """The library documents the caller may read, proposed and kept, to pick one to scan."""
    glossary._require_glossary_rw(request)
    require_active_org_id(request)
    async with _pool().acquire() as conn:
        found = await store.documents(conn)
    return {"documents": [_document(d) for d in found if _readable(request, d.domains)]}


async def _library_domains(conn: Connection, declared: set[str]) -> list[str]:
    """The steward's working domains as a library document's. In a single-domain deployment a
    steward declares none and the document is the whole org's, which is the same thing there."""
    from provisa.core.repositories import domain as domain_repo

    known = {d["id"] for d in await domain_repo.list_all(conn)}
    return library.check_domains(sorted(declared) or [library.WHOLE_ORG], known)


async def _new_document(
    conn: Connection, body: dict, domains: list[str], org_id: str, request: Request
) -> int:
    """The document ``body`` gives by ``url`` or file, added to the library as proposed -- or the
    library's copy when it is already there."""
    from provisa.api.admin import reference_library_router as library_api

    if body.get("url") is not None:
        url = library.check_url(body["url"])
        existing = await store.find_url(conn, url)
        if existing is not None:
            return existing
        kind, content = store.URL, await library.fetch(url, org_id)
    else:
        kind, content = store.FILE, library_api._file(body)
    try:
        return await library.add(
            conn,
            content,
            source_kind=kind,
            domains=domains,
            status=store.PROPOSED,
            embedder=await library_api._embedder(),
            added_by=glossary._caller_id(request),
        )
    except library.AlreadyInLibrary as exc:
        return exc.document_id


# -- scanning -----------------------------------------------------------------------------------


async def _model(request: Request) -> Callable[[str, int], Awaitable[str]]:
    from provisa.api.admin.capabilities import acting_role_of_request
    from provisa.api.app import state
    from provisa.term_finding import model

    return await model.for_role(state, acting_role_of_request(request))


async def run_scan(
    pool: Database,
    scan_id: int,
    document_id: int,
    title: str,
    call: Callable[[str, int], Awaitable[str]],
) -> None:
    """Scan the document and queue its candidates; a scan that cannot finish is marked failed,
    naming why."""
    try:
        async with pool.acquire() as conn:
            rows = await store.passages(conn, document_id)
        result = await scan.run(title, [scan.Passage(*r) for r in rows], call)
        async with pool.acquire() as conn:
            terms = [
                {"id": t["id"], "name": t["name"]}
                for t in await glossary_repo.list_terms(cast("Connection", conn))
            ]
            edges = await glossary_repo.term_edges(cast("Connection", conn))
            scan.match(
                result.candidates,
                terms,
                [(f, t) for f, t, rel in edges if rel == review.SYNONYM_OF],
            )
            summary = {
                "passages": result.passages,
                "mentions": result.mentions,
                "candidates": len(result.candidates),
                "calls": result.calls,
                "left_out": result.left_out.as_dict(),
            }
            async with conn.transaction():
                await queue.finish_scan(conn, scan_id, result.candidates, summary)
    except Exception as exc:  # the scan is marked failed with what stopped it, never left running
        log.exception("term-finding scan %s failed", scan_id)
        async with pool.acquire() as conn:
            await queue.fail_scan(conn, scan_id, str(exc) or type(exc).__name__)


def _launch(work: Awaitable[None]) -> None:
    task = asyncio.ensure_future(work)
    _RUNNING.add(task)
    task.add_done_callback(_RUNNING.discard)


@router.post("/scans")
async def start_scan(request: Request) -> dict:
    """Scan a library document (``document_id``) or a new one (``url``, or ``file_name`` with
    ``content_b64``), in the steward's working ``domains``."""
    glossary._require_glossary_rw(request)
    org_id = require_active_org_id(request)
    body = await request.json()
    given = [k for k in ("document_id", "url", "content_b64") if body.get(k) is not None]
    if len(given) != 1:
        raise ApiError(
            400,
            "term_finding.one_document",
            "a scan reads one document: a library document, a URL or a file",
        )
    declared = glossary._declared_domains(request, body.get("domains"), current=set())
    pool = _pool()
    try:
        async with pool.acquire() as conn:
            if body.get("document_id") is not None:
                document_id = body["document_id"]
                if not isinstance(document_id, int) or isinstance(document_id, bool):
                    raise library.LibraryRefused("document_id is a library document's id")
            else:
                document_id = await _new_document(
                    conn, body, await _library_domains(conn, declared), org_id, request
                )
            document = await store.document(conn, document_id)
            if document is None:
                raise library.DocumentNotFound(document_id)
            if not _readable(request, document.domains):
                raise ApiError(
                    403,
                    "auth.domain_denied",
                    f"{document.title!r} is in the reference library, outside your domains",
                )
            call = await _model(request)
            scan_id = await queue.start_scan(
                conn,
                document_id=document.id,
                document_title=document.title,
                document_source=document.source,
                domains=sorted(declared),
                started_by=glossary._caller_id(request),
            )
            started = await queue.scan(conn, scan_id)
    except (library.LibraryRefused, review.ReviewRefused) as exc:
        raise _refused(exc) from None
    _launch(run_scan(pool, scan_id, document.id, document.title, call))
    assert started is not None
    return {"scan": _scan(started), "document": _document(document)}


@router.get("/scans")
async def list_scans(request: Request, document_id: int | None = Query(None)) -> dict:
    """The scans in the caller's domains (of ``document_id``), newest first, each with its
    drafts counted by status."""
    glossary._require_glossary_rw(request)
    require_active_org_id(request)
    async with _pool().acquire() as conn:
        found = await queue.scans(conn, document_id=document_id)
        counts = await queue.draft_counts(conn)
    return {"scans": [_scan(s, counts.get(s.id)) for s in found if _readable(request, s.domains)]}


async def _scan_held(conn: Connection, scan_id: int, request: Request) -> queue.ScanRow:
    found = await queue.scan(conn, scan_id)
    if found is None or not _readable(request, found.domains):
        raise ApiError(404, "term_finding.not_found", f"there is no scan {scan_id}")
    return found


def _draft_view(d: queue.DraftRow, names: dict[int, str]) -> dict[str, Any]:
    """The draft as the queue shows it. ``names`` are the glossary terms the caller may read; a
    term outside them is shown as outside the caller's domains, never by name (REQ-1591)."""

    def term(term_id: int | None) -> dict[str, Any] | None:
        if term_id is None:
            return None
        if term_id not in names:
            return {"outside_domains": True}
        return {"term_id": term_id, "name": names[term_id]}

    return {
        "id": d.id,
        "scan_id": d.scan_id,
        "name": d.name,
        "category": d.category,
        "sense": d.sense,
        "definition": d.definition,
        "definition_kind": d.definition_kind,
        "proposal": d.proposal,
        "match": term(d.match_term_id),
        "match_kind": d.match_kind,
        "status": d.status,
        "term": term(d.term_id),
        "decided_by": d.decided_by,
        "decided_at": _iso(d.decided_at),
        "passages": d.payload["passages"],
        "variants": d.payload["variants"],
        "conflicts": d.payload["conflicts"],
        "homonyms": d.payload["homonyms"],
        "matches": [{**(term(m["term_id"]) or {}), "via": m["via"]} for m in d.payload["matches"]],
    }


async def _term_names(conn: Connection, request: Request) -> dict[int, str]:
    """The glossary terms the caller may read, by id."""
    return {
        t["id"]: t["name"]
        for t in await glossary_repo.list_terms(
            cast("Connection", conn), domains=glossary._authority(request)
        )
    }


@router.get("/scans/{scan_id}")
async def get_scan(scan_id: int, request: Request) -> dict:
    """The scan and its drafts, each with its source passages, variants, conflicting
    explanations and the glossary terms it matches."""
    glossary._require_glossary_rw(request)
    require_active_org_id(request)
    async with _pool().acquire() as conn:
        found = await _scan_held(conn, scan_id, request)
        drafts = await queue.drafts(conn, scan_id)
        names = await _term_names(conn, request)
        counts = (await queue.draft_counts(conn)).get(scan_id)
    return {"scan": _scan(found, counts), "drafts": [_draft_view(d, names) for d in drafts]}


# -- deciding on drafts -------------------------------------------------------------------------


async def _draft_held(
    conn: Connection, draft_id: int, request: Request
) -> tuple[queue.DraftRow, queue.ScanRow]:
    found = await review.held(conn, draft_id)
    return found, await _scan_held(conn, found.scan_id, request)


async def _require_curatable(conn: Connection, term_ids: Any, request: Request) -> None:
    for term_id in term_ids:
        refusal = await glossary._curation_refusal(cast("Connection", conn), term_id, request)
        if refusal is not None:
            raise refusal


async def _accept(conn: Connection, draft_id: int, request: Request) -> dict[str, Any]:
    found, held_scan = await _draft_held(conn, draft_id, request)
    domains = glossary._declared_domains(request, held_scan.domains, current=set())
    existing = await review.existing_terms(conn, review.kept_synonyms(found))
    await _require_curatable(conn, existing.values(), request)
    term_id = await review.accept(conn, found, domains, glossary._caller_id(request))
    return {"id": draft_id, "status": queue.ACCEPTED, "term_id": term_id}


async def _merge(
    conn: Connection, draft_id: int, request: Request, target: Any, use_definition: Any
) -> dict[str, Any]:
    found, held_scan = await _draft_held(conn, draft_id, request)
    target_id = found.match_term_id if target is None else target
    if not isinstance(target_id, int) or isinstance(target_id, bool):
        raise review.ReviewRefused(
            f"draft {draft_id} ({found.name!r}) matches no glossary term: name the term to "
            "merge it into"
        )
    if not isinstance(use_definition, bool):
        raise review.ReviewRefused("use_definition is true or false")
    term = await glossary_repo.get_term(cast("Connection", conn), target_id)
    if term is None:
        raise review.ReviewRefused(f"the glossary holds no term {target_id}")
    domains = glossary._declared_domains(request, held_scan.domains, current=set())
    existing = await review.existing_terms(conn, review.merge_names(found, term["name"]))
    await _require_curatable(conn, [target_id, *existing.values()], request)
    await review.merge(
        conn,
        found,
        target_id,
        use_definition=use_definition,
        domains=domains,
        by=glossary._caller_id(request),
    )
    return {"id": draft_id, "status": queue.MERGED, "term_id": target_id}


@router.patch("/drafts/{draft_id}")
async def edit_draft(draft_id: int, request: Request) -> dict:
    """Edit a draft's ``name``, ``definition`` or ``synonyms`` before deciding on it."""
    glossary._require_glossary_rw(request)
    require_active_org_id(request)
    body = await request.json()
    try:
        async with _pool().acquire() as conn:
            found, _ = await _draft_held(conn, draft_id, request)
            name, definition, payload = review.edited(found, body)
            await queue.edit(conn, draft_id, name=name, definition=definition, payload=payload)
            changed = await queue.draft(conn, draft_id)
            assert changed is not None
            return _draft_view(changed, await _term_names(conn, request))
    except review.ReviewRefused as exc:
        raise _refused(exc) from None


@router.post("/drafts/{draft_id}/accept")
async def accept_draft(draft_id: int, request: Request) -> dict:
    """Add the draft's term to the glossary, with the synonyms it keeps."""
    glossary._require_glossary_rw(request)
    org_id = require_active_org_id(request)
    try:
        async with _pool().acquire() as conn:
            async with conn.transaction():
                decided = await _accept(conn, draft_id, request)
    except (review.ReviewRefused, ValueError) as exc:
        raise _refused(_review_error(exc)) from None
    await glossary._notify(org_id, "ontology term accepted from a document")
    return decided


@router.post("/drafts/{draft_id}/merge")
async def merge_draft(draft_id: int, request: Request) -> dict:
    """Merge the draft into a glossary term: the one it matches, or ``term_id``. Its name and kept
    synonyms become synonyms of the term; ``use_definition`` gives the term its definition."""
    glossary._require_glossary_rw(request)
    org_id = require_active_org_id(request)
    body = await request.json()
    try:
        async with _pool().acquire() as conn:
            async with conn.transaction():
                decided = await _merge(
                    conn,
                    draft_id,
                    request,
                    body.get("term_id"),
                    body.get("use_definition", False),
                )
    except (review.ReviewRefused, ValueError) as exc:
        raise _refused(_review_error(exc)) from None
    await glossary._notify(org_id, "ontology term merged from a document")
    return decided


@router.post("/drafts/{draft_id}/reject")
async def reject_draft(draft_id: int, request: Request) -> dict:
    glossary._require_glossary_rw(request)
    require_active_org_id(request)
    try:
        async with _pool().acquire() as conn:
            found, _ = await _draft_held(conn, draft_id, request)
            await review.reject(conn, found, glossary._caller_id(request))
    except review.ReviewRefused as exc:
        raise _refused(exc) from None
    return {"id": draft_id, "status": queue.REJECTED}


def _review_error(exc: Exception) -> review.ReviewRefused:
    """A glossary refusal (provisa.core.repositories.glossary raises ValueError) as the review's."""
    return exc if isinstance(exc, review.ReviewRefused) else review.ReviewRefused(str(exc))


@router.post("/scans/{scan_id}/accept")
async def bulk_accept(scan_id: int, request: Request) -> dict:
    """Decide on several of the scan's drafts at once (``draft_ids``) as each proposes: a new term
    accepted, a match merged into the term it matches. Each is decided on its own; one that is
    refused is named with why, and the others stand."""
    glossary._require_glossary_rw(request)
    org_id = require_active_org_id(request)
    body = await request.json()
    ids = body.get("draft_ids")
    if not isinstance(ids, list) or not all(
        isinstance(i, int) and not isinstance(i, bool) for i in ids
    ):
        raise ApiError(400, "term_finding.refused", "draft_ids is a list of draft ids")
    decided: list[dict[str, Any]] = []
    refused: list[dict[str, Any]] = []
    async with _pool().acquire() as conn:
        await _scan_held(conn, scan_id, request)
        for draft_id in ids:
            try:
                async with conn.transaction():
                    found = await review.held(conn, draft_id)
                    if found.scan_id != scan_id:
                        raise review.DraftNotFound(draft_id)
                    if found.proposal == queue.MERGE:
                        decided.append(await _merge(conn, draft_id, request, None, False))
                    else:
                        decided.append(await _accept(conn, draft_id, request))
            except (review.ReviewRefused, ValueError, ApiError) as exc:
                message = exc.detail if isinstance(exc, ApiError) else str(exc)
                refused.append({"id": draft_id, "message": message})
    if decided:
        await glossary._notify(org_id, "ontology terms accepted from a document")
    return {"decided": decided, "refused": refused}
