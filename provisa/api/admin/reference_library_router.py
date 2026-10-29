# Copyright (c) 2026 Kenneth Stott
# Canary: c52057d7-f5ad-4a87-8ef0-0493ad93e238
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Admin Reference library page's API (REQ-2185, MAINTAINED IN ADMIN).

An admin lists the library's documents (title, source, status, domains, when each was added and
last refreshed or replaced), adds one by file or URL, keeps or discards a proposed one, changes
its domains, refreshes a URL document, uploads a new version of a file document, and removes one.
The library is the org's, so the page asks ``org_settings``. A file travels as base64 in a JSON
body, as every admin upload does (import_router.py).
"""

# Requirements: REQ-2185

from __future__ import annotations

import base64
import binascii
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Request

from provisa.api.admin._guards import require_active_org_id
from provisa.api.admin._platform_guard import require_org_settings
from provisa.api.errors import ApiError
from provisa.reference_library import library, store

if TYPE_CHECKING:
    from provisa.core.database import Connection, Database

router = APIRouter(prefix="/admin/reference-library", tags=["admin", "reference-library"])


def _pool() -> Database:
    from provisa.api.app import state

    if state.model_db is None:
        raise ApiError(503, "reference_library.database_not_connected", "Database not connected")
    return state.model_db


def _guard(request: Request) -> str:
    require_org_settings(request)
    return require_active_org_id(request)


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


def _iso(value: Any) -> str | None:
    return None if value is None else value.isoformat()


def _document(d: store.DocumentRow) -> dict[str, Any]:
    return {
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
    _guard(request)
    async with _pool().acquire() as conn:
        return {"documents": [_document(d) for d in await store.documents(conn)]}


@router.get("/documents/{document_id}")
async def get_document(document_id: int, request: Request) -> dict:
    _guard(request)
    async with _pool().acquire() as conn:
        found = await store.document(conn, document_id)
        if found is None:
            raise _refused(library.DocumentNotFound(document_id))
        passages = await store.passages(conn, document_id)
    return {
        **_document(found),
        "passage_list": [
            {"ordinal": o, "section": s, "page": p, "text": t} for o, s, p, t in passages
        ],
    }


@router.post("/documents")
async def add_document(body: dict, request: Request) -> dict:
    """Add a document by ``url``, or by ``file_name`` with ``content_b64``; an admin's document
    is kept."""
    org_id = _guard(request)
    try:
        if body.get("url") is not None and body.get("content_b64") is not None:
            raise library.LibraryRefused("a document is added by a URL or a file, not both")
        async with _pool().acquire() as conn:
            domains = await _domains(conn, body.get("domains"))
            if body.get("url") is not None:
                url = library.check_url(body["url"])
                existing = await store.find_url(conn, url)
                if existing is not None:
                    raise library.AlreadyInLibrary(existing, url)
                kind, content = store.URL, await library.fetch(url, org_id)
            else:
                kind, content = store.FILE, _file(body)
            document_id = await library.add(
                conn,
                content,
                source_kind=kind,
                domains=domains,
                status=store.KEPT,
                embedder=await _embedder(),
                added_by=_user_id(request),
                title=body.get("title"),
            )
            found = await store.document(conn, document_id)
    except library.LibraryRefused as exc:
        raise _refused(exc) from None
    assert found is not None
    return _document(found)


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
    return _document(found)


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
    return _document(found)


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
    return _document(found)


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
