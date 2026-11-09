# Copyright (c) 2026 Kenneth Stott
# Canary: a0cd8e61-5db4-4049-8d52-2ea4c772a09f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Adding, refreshing, replacing and removing library documents (REQ-2185).

A document is added by a file or an http(s) URL, with domains (a list of domain ids, or ``*`` for
the whole org) and a status: an admin's document is kept; one the ontology adds when a steward
finds terms in it (REQ-2179) is proposed until someone keeps or discards it. A document already in
the library -- the same URL, or the same content -- is not added again (:class:`AlreadyInLibrary`
names it).

Refresh fetches a URL document again and replaces its content; a new version replaces a file
document's content with the uploaded file. Either keeps the document's place, status and domains,
and earlier content is not kept. Nothing is re-reviewed when content changes (REQ-2185, RESCANNED
ON DEMAND).

The content is read into sections (extract.py), cut into passages (passages.py) and each passage
embedded by the org's vector model (:class:`Embedder`) before anything is written, so a document
that cannot be read or embedded leaves the library as it was.

From the Library page a document is added in one step (ADD NOW, INDEX IN THE BACKGROUND):
:func:`add_pending` makes it exist at once and :func:`index_pending`, in a background job, reads and
indexes it, reporting its index status on its row: indexing, with its progress where known; ready;
or failed, with the reason and a retry. A document is used, and can be cited, once it is ready
(:func:`require_ready`). Term finding's own add (:func:`add`) still reads first: its scan needs the
passages at once.
"""

# Requirements: REQ-2185

from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import Awaitable, Callable, Collection, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from provisa.reference_library import extract, store
from provisa.reference_library.passages import cut

if TYPE_CHECKING:
    from provisa.core.database import Connection, Database

log = logging.getLogger(__name__)

#: The whole org, as a glossary term's ``*`` (provisa.core.glossary.ENTERPRISE_DOMAIN).
WHOLE_ORG = "*"
#: The largest document the library takes, in bytes.
MAX_BYTES = 20 * 1024 * 1024
#: How long a URL document's fetch may take, in seconds.
FETCH_TIMEOUT = 30.0
#: How many passages are embedded in one call to the vector model.
EMBED_BATCH = 64


class LibraryRefused(ValueError):
    """A library change refused, said naming why; ``code`` and ``status`` for the API."""

    code = "reference_library.invalid"
    status = 400


class DocumentNotFound(LibraryRefused):
    code = "reference_library.not_found"
    status = 404

    def __init__(self, document_id: int) -> None:
        super().__init__(f"the reference library holds no document {document_id}")


class AlreadyInLibrary(LibraryRefused):
    code = "reference_library.already_in_library"
    status = 409

    def __init__(self, document_id: int, what: str) -> None:
        super().__init__(f"{what} is already in the reference library, as document {document_id}")
        self.document_id = document_id


class FetchFailed(LibraryRefused):
    code = "reference_library.fetch_failed"
    status = 502


class EmbeddingFailed(LibraryRefused):
    """The vector model could not embed a document's passages; the document is as it was."""

    code = "reference_library.embed_failed"
    status = 502


class NotReady(LibraryRefused):
    """A document used before it is ready, said naming what it is waiting on."""

    code = "reference_library.not_ready"
    status = 409


class NotFailed(LibraryRefused):
    code = "reference_library.not_failed"
    status = 409

    def __init__(self, document_id: int) -> None:
        super().__init__(f"document {document_id} has not failed: there is nothing to retry")


@dataclass(frozen=True)
class Embedder:
    """The org's vector model: its id and the call that embeds texts with it."""

    model: str
    embed: Callable[[list[str]], Awaitable[list[list[float]]]]


@dataclass(frozen=True)
class Content:
    """A document's bytes as received, with the media type and charset they were read as."""

    data: bytes
    media_type: str
    charset: str | None
    name: str


def check_domains(raw: Any, known: Collection[str]) -> list[str]:
    """``raw`` as a document's domains: a non-empty list of the org's domain ids, or ``["*"]``."""
    if not isinstance(raw, list) or not raw or not all(isinstance(d, str) and d for d in raw):
        raise LibraryRefused('domains is a list of domain ids, or ["*"] for the whole org')
    domains = sorted(set(raw))
    if WHOLE_ORG in domains:
        if len(domains) > 1:
            raise LibraryRefused(
                f"{WHOLE_ORG!r} is the whole org and cannot be combined with a domain"
            )
        return domains
    unknown = [d for d in domains if d not in known]
    if unknown:
        raise LibraryRefused(f"the org has no domain {unknown[0]!r}")
    return domains


def check_url(url: Any) -> str:
    if (
        not isinstance(url, str)
        or urlsplit(url).scheme not in ("http", "https")
        or not urlsplit(url).netloc
    ):
        raise LibraryRefused("a document's URL is an http(s) address")
    return url


def file_content(name: Any, data: bytes) -> Content:
    if not isinstance(name, str) or not name.strip():
        raise LibraryRefused("a file document is named by its file name")
    if len(data) > MAX_BYTES:
        raise LibraryRefused(f"{name!r} is larger than {MAX_BYTES // (1024 * 1024)} MB")
    return Content(data, _readable(lambda: extract.media_type_of_name(name)), None, name)


def _readable(read: Callable[[], Any]) -> Any:
    try:
        return read()
    except extract.UnreadableDocument as exc:
        raise LibraryRefused(str(exc)) from None


async def fetch(url: str, org_id: str | None) -> Content:
    """The document ``url`` serves now, each hop held to the org's egress rule (REQ-2006)."""
    import httpx

    from provisa.security import sandbox_egress

    try:
        async with httpx.AsyncClient(
            timeout=FETCH_TIMEOUT,
            follow_redirects=True,
            event_hooks=sandbox_egress.http_event_hooks(org_id),
        ) as client:
            async with client.stream("GET", url) as response:
                if response.status_code >= 400:
                    raise FetchFailed(f"{url} answered {response.status_code}")
                media_type, charset = _readable(
                    lambda: extract.media_type_of_response(
                        response.headers.get("content-type"), url
                    )
                )
                data = b""
                async for chunk in response.aiter_bytes():
                    data += chunk
                    if len(data) > MAX_BYTES:
                        raise LibraryRefused(
                            f"{url} serves more than {MAX_BYTES // (1024 * 1024)} MB"
                        )
    except httpx.HTTPError as exc:
        raise FetchFailed(f"{url} could not be fetched: {exc}") from None
    except sandbox_egress.PrivateDestinationRefused as exc:
        raise LibraryRefused(str(exc)) from None
    return Content(data, media_type, charset, url)


@dataclass(frozen=True)
class _Read:
    title: str | None
    sha256: str
    passages: list[store.StoredPassage]


#: What a job is told after each batch of passages it embeds: how many are done, of how many.
Progress = Callable[[int, int], Awaitable[None]]


async def _embed_texts(
    texts: Sequence[str], embedder: Embedder, progress: Progress | None = None
) -> list[list[float]]:
    vectors: list[list[float]] = []
    for start in range(0, len(texts), EMBED_BATCH):
        batch = list(texts[start : start + EMBED_BATCH])
        embedded = await embedder.embed(batch)
        if len(embedded) != len(batch):
            raise LibraryRefused(
                f"the vector model {embedder.model!r} returned {len(embedded)} embeddings "
                f"for {len(batch)} passages"
            )
        vectors.extend(embedded)
        if progress is not None:
            await progress(len(vectors), len(texts))
    return vectors


async def _read(content: Content, embedder: Embedder, progress: Progress | None = None) -> _Read:
    document = _readable(
        lambda: extract.read(content.data, content.media_type, content.name, content.charset)
    )
    cuts = cut(document)
    vectors = await _embed_texts([p.text for p in cuts], embedder, progress)
    return _Read(
        title=document.title,
        sha256=hashlib.sha256(content.data).hexdigest(),
        passages=[
            store.StoredPassage(p.ordinal, p.section, p.page, p.text, v)
            for p, v in zip(cuts, vectors, strict=True)
        ],
    )


def name_title(name: str) -> str:
    """A document's title before it has been read: the last part of its file name or address."""
    path = urlsplit(name).path if "://" in name else name
    return path.rstrip("/").rsplit("/", 1)[-1] or name


def _given_title(given: Any) -> str | None:
    if given is not None and not isinstance(given, str):
        raise LibraryRefused("a document's title is text")
    return given.strip() if given and given.strip() else None


def _title(given: Any, read: _Read, content: Content) -> str:
    return _given_title(given) or read.title or name_title(content.name)


async def add(
    conn: Connection,
    content: Content,
    *,
    source_kind: str,
    domains: Sequence[str],
    status: str,
    embedder: Embedder,
    added_by: str | None,
    title: Any = None,
) -> int:
    """Add a document; its id. ``domains`` are checked (:func:`check_domains`) by the caller."""
    if status not in store.STATUSES:
        raise LibraryRefused(f"a document's status is proposed or kept, not {status!r}")
    if source_kind == store.URL:
        existing = await store.find_url(conn, content.name)
        if existing is not None:
            raise AlreadyInLibrary(existing, content.name)
    read = await _read(content, embedder)
    existing = await store.find_content(conn, read.sha256)
    if existing is not None:
        raise AlreadyInLibrary(existing, f"the content of {content.name!r}")
    async with conn.transaction():
        return await store.insert_document(
            conn,
            title=_title(title, read, content),
            source_kind=source_kind,
            source=content.name,
            media_type=content.media_type,
            content_sha256=read.sha256,
            status=status,
            embedding_model=embedder.model,
            added_by=added_by,
            domains=domains,
            passages=read.passages,
        )


async def add_pending(
    conn: Connection,
    *,
    source_kind: str,
    source: str,
    content: Content | None,
    domains: Sequence[str],
    status: str,
    added_by: str | None,
    launch: str,
    title: Any = None,
) -> int:
    """Add a document that exists at once and is read in the background (:func:`index_pending`);
    its id. ``content`` is a file's; a URL is fetched by the job. What can be refused without
    reading it is refused here: a URL or a file already in the library."""
    named = _given_title(title) or name_title(source)
    if source_kind == store.URL:
        existing = await store.find_url(conn, source)
        if existing is not None:
            raise AlreadyInLibrary(existing, source)
        digest = media_type = ""
        pending = None
    else:
        assert content is not None
        digest = hashlib.sha256(content.data).hexdigest()
        existing = await store.find_content(conn, digest)
        if existing is not None:
            raise AlreadyInLibrary(existing, f"the content of {content.name!r}")
        media_type, pending = content.media_type, content.data
    async with conn.transaction():
        return await store.insert_pending(
            conn,
            title=named,
            source_kind=source_kind,
            source=source,
            media_type=media_type,
            content_sha256=digest,
            pending=pending,
            status=status,
            added_by=added_by,
            domains=domains,
            launch=launch,
        )


class _IndexingEnded(Exception):
    """The document stopped indexing while its job ran: it was removed or given up on."""


#: What a job that a server's shutdown cut off says.
SHUTDOWN = "the server stopped while the document was being indexed; retry it"


async def index_pending(
    pool: Database,
    document_id: int,
    embedder: Callable[[], Awaitable[Embedder]],
    org_id: str | None,
) -> store.DocumentRow | None:
    """Read and index a document added by :func:`add_pending`; the document once it is ready, or
    None when it did not get there. Whatever stops it is written on the document, by name, for a
    retry: the job never leaves a document indexing. A document removed meanwhile just ends it."""

    async def report(step: str, done: int | None = None, total: int | None = None) -> None:
        async with pool.acquire() as conn:
            if not await store.record_index_progress(
                conn, document_id, step=step, done=done, total=total
            ):
                raise _IndexingEnded

    async def embedded(done: int, total: int) -> None:
        await report("indexing", done, total)

    try:
        await report("reading")
        async with pool.acquire() as conn:
            held = await _held(conn, document_id)
            pending = await store.pending_content(conn, document_id)
        if held.source_kind == store.URL:
            content = await fetch(held.source, org_id)
        elif pending is None:
            raise LibraryRefused(f"{held.source!r} is no longer held: add the file again")
        else:
            content = Content(pending, held.media_type, None, held.source)
        model = await embedder()
        read = await _read(content, model, embedded)
        async with pool.acquire() as conn:
            other = await store.find_content(conn, read.sha256)
            if other is not None and other != document_id:
                raise AlreadyInLibrary(other, f"the content of {content.name!r}")
            read_title = read.title if held.title == name_title(held.source) else None
            async with conn.transaction():
                finished = await store.finish_indexing(
                    conn,
                    document_id,
                    title=read_title or held.title,
                    media_type=content.media_type,
                    content_sha256=read.sha256,
                    embedding_model=model.model,
                    passages=read.passages,
                )
            return await store.document(conn, document_id) if finished else None
    except (_IndexingEnded, DocumentNotFound):
        log.info("indexing of library document %s ended: it is no longer indexing", document_id)
        return None
    except asyncio.CancelledError:  # the process is shutting down (shutdown_background)
        async with pool.acquire() as conn:
            await store.fail_indexing(conn, document_id, SHUTDOWN)
        raise
    except Exception as exc:  # the reason is written on the document, for a retry
        log.warning("indexing of library document %s failed: %s", document_id, exc)
        async with pool.acquire() as conn:
            await store.fail_indexing(conn, document_id, str(exc) or type(exc).__name__)
        return None


async def _held(conn: Connection, document_id: int) -> store.DocumentRow:
    found = await store.document(conn, document_id)
    if found is None:
        raise DocumentNotFound(document_id)
    return found


def require_ready(document: store.DocumentRow) -> None:
    """Refuse a document that is not ready, saying what it is waiting on (citations and assistants
    use a document once it is ready)."""
    if document.index_status == store.INDEXING:
        raise NotReady(
            f"{document.title!r} is still being indexed: it can be used once it is ready"
        )
    if document.index_status == store.FAILED:
        raise NotReady(f"{document.title!r} could not be indexed: {document.index_error}")


async def retry(conn: Connection, document_id: int, launch: str) -> None:
    """Put a failed document back to indexing, for its job to run again in ``launch``."""
    await _held(conn, document_id)
    if not await store.begin_retry(conn, document_id, launch):
        raise NotFailed(document_id)


async def _ready(conn: Connection, document_id: int) -> store.DocumentRow:
    held = await _held(conn, document_id)
    require_ready(held)
    return held


async def refresh(
    conn: Connection, document_id: int, embedder: Embedder, org_id: str | None
) -> None:
    """Fetch a URL document again and replace its content with what the URL serves now."""
    held = await _ready(conn, document_id)
    if held.source_kind != store.URL:
        raise LibraryRefused(
            f"document {document_id} was uploaded as a file: upload a new version of it instead"
        )
    content = await fetch(held.source, org_id)
    read = await _read(content, embedder)
    async with conn.transaction():
        await store.replace_content(
            conn,
            document_id,
            media_type=content.media_type,
            content_sha256=read.sha256,
            embedding_model=embedder.model,
            passages=read.passages,
            source=None,
            refreshed=True,
        )


async def new_version(
    conn: Connection, document_id: int, content: Content, embedder: Embedder
) -> None:
    """Replace a file document's content with an uploaded file, which names it from now on."""
    held = await _ready(conn, document_id)
    if held.source_kind != store.FILE:
        raise LibraryRefused(
            f"document {document_id} was added from its URL: refresh it to fetch it again instead"
        )
    read = await _read(content, embedder)
    other = await store.find_content(conn, read.sha256)
    if other is not None and other != document_id:
        raise AlreadyInLibrary(other, f"the content of {content.name!r}")
    async with conn.transaction():
        await store.replace_content(
            conn,
            document_id,
            media_type=content.media_type,
            content_sha256=read.sha256,
            embedding_model=embedder.model,
            passages=read.passages,
            source=content.name,
            refreshed=False,
        )


@dataclass(frozen=True)
class FailedDocument:
    """A document whose re-embedding failed, and why; it is as it was."""

    document_id: int
    title: str
    reason: str


@dataclass(frozen=True)
class ReembedOutcome:
    reembedded: list[int]
    failed: list[FailedDocument]


async def needing_re_embedding(conn: Connection, embedder: Embedder) -> list[store.DocumentRow]:
    """The documents embedded by a model other than the org's current one."""
    return [
        d for d in await store.documents(conn, ready=True) if d.embedding_model != embedder.model
    ]


async def reembed(conn: Connection, document_id: int, embedder: Embedder) -> None:
    """Re-embed a document's stored passage text with the org's current model. The file is not read
    again and the URL is not fetched; nothing but the embeddings (and the model they name) changes.
    A document the model cannot embed is left as it was (:class:`EmbeddingFailed`)."""
    held = await _ready(conn, document_id)
    texts = [text for _, _, _, text in await store.passages(conn, document_id)]
    try:
        vectors = await _embed_texts(texts, embedder)
    except Exception as exc:
        raise EmbeddingFailed(
            f"document {document_id} ({held.title!r}) could not be re-embedded with "
            f"{embedder.model!r}: {exc}"
        ) from exc
    async with conn.transaction():
        await store.replace_vectors(
            conn, document_id, embedding_model=embedder.model, vectors=vectors
        )


async def reembed_all(conn: Connection, embedder: Embedder) -> ReembedOutcome:
    """Re-embed every document embedded by an earlier model. A document that fails is reported by
    name with its reason and left as it was; the others are still re-embedded."""
    done: list[int] = []
    failed: list[FailedDocument] = []
    for held in sorted(await needing_re_embedding(conn, embedder), key=lambda d: d.id):
        try:
            await reembed(conn, held.id, embedder)
        except EmbeddingFailed as exc:
            failed.append(FailedDocument(held.id, held.title, str(exc.__cause__ or exc)))
        else:
            done.append(held.id)
    return ReembedOutcome(done, failed)


async def keep(conn: Connection, document_id: int) -> None:
    """Keep a proposed document."""
    held = await _held(conn, document_id)
    if held.status != store.PROPOSED:
        raise LibraryRefused(f"document {document_id} is already kept")
    await store.set_status(conn, document_id, store.KEPT)


async def set_domains(conn: Connection, document_id: int, domains: Sequence[str]) -> None:
    """Change a document's domains; ``domains`` are checked by the caller."""
    await _held(conn, document_id)
    await store.set_domains(conn, document_id, domains)


async def rename(conn: Connection, document_id: int, title: Any) -> None:
    if not isinstance(title, str) or not title.strip():
        raise LibraryRefused("a document's title is text")
    await _held(conn, document_id)
    await store.set_title(conn, document_id, title.strip())


async def remove(conn: Connection, document_id: int) -> None:
    """Remove a document (or discard a proposed one): no assistant draws on it from now on."""
    if not await store.remove(conn, document_id):
        raise DocumentNotFound(document_id)
