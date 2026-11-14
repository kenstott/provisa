# Copyright (c) 2026 Kenneth Stott
# Canary: 7b3e19c4-5a2d-4f86-9e0b-c41d7f2a8e63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Citations of reference library documents and their sections (REQ-2208).

One shape for whatever cites the library (``reference_citations``): a tag's definition cites the
section that defines it, each assignment of a tag cites the documents or sections that trace or
confirm it, and a data quality check kept from a regulation cites the section it came from
(REQ-2245); a security item will cite under a kind of its own. A citation names a document and,
optionally, a section of it as a section tag does: by its heading, or by its page when it has none
(a PDF's pages). The section's text is what the library's extraction read: the passages under that
heading, or on that page, in order; the whole document's when no section is named.

Each citation keeps a hash of that text when it was made, and the document's title and source, so
it outlives the document. A new version of a document replaces its content in place
(store.replace_content: a refresh or an upload keeps the document's id, place and domains), so the
citation keeps naming it; its state is read against the passages held now:

- :data:`CURRENT`: the section is there and its text is unchanged;
- :data:`DRIFTED`: the section is there and its text changed: its citer is flagged for review;
- :data:`BROKEN`: the section is gone from the document, or the document was removed: flagged too.

Citing a section again records its text as it is now, which is how a reviewer confirms a drifted
citation.
"""

# Requirements: REQ-2208, REQ-2245

from __future__ import annotations

import hashlib
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import ColumnElement, and_, case, delete, func, literal, or_, select
from sqlalchemy.dialects.postgresql import aggregate_order_by

from provisa.core.schema_org import reference_citations as citations_t
from provisa.core.schema_org import reference_passages as passages_t
from provisa.reference_library import library, store

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: A tag's definition cites the section that defines it.
TAG = "tag"
#: An assignment of a tag to an object cites what traces or confirms it.
TAG_ASSIGNMENT = "tag_assignment"
#: A data quality check kept from a regulation's section cites that section; its tag id is NULL and
#: its object key names the check (provisa.dq.regulation.check_object_key).
DQ_CHECK = "dq_check"
CITER_KINDS = (TAG, TAG_ASSIGNMENT, DQ_CHECK)

#: The cited section is there with the text it had when cited.
CURRENT = "current"
#: The cited section's text changed: its citer is flagged for review.
DRIFTED = "drifted"
#: The cited section, or the document, is gone: its citer is flagged for review.
BROKEN = "broken"


class CitationRefused(library.LibraryRefused):
    """A citation that cannot be made, said naming why."""

    code = "reference_library.citation_invalid"


@dataclass(frozen=True)
class Place:
    """A document, or a section of it named by its heading or, when it has none, its page."""

    document_id: int
    section_heading: str | None = None
    section_page: int | None = None

    def key(self) -> str:
        """The citation's name of the place, as a section tag's object key names a section."""
        if self.section_heading is not None:
            return f"section:{self.document_id}:heading:{self.section_heading}"
        if self.section_page is not None:
            return f"section:{self.document_id}:page:{self.section_page}"
        return f"document:{self.document_id}"


@dataclass(frozen=True)
class Citation:
    """A citation with its state read against the document as it is held now."""

    citer_kind: str
    tag_id: str | None
    object_key: str
    document_id: int | None
    document_title: str
    document_source: str
    section_heading: str | None
    section_page: int | None
    text_sha256: str
    cited_by: str | None
    cited_at: datetime
    state: str

    @property
    def needs_review(self) -> bool:
        return self.state != CURRENT


def text_hash(text: str) -> str:
    """The hash a citation keeps of a section's text, however its whitespace is laid out, stated
    over a Python string: :func:`section_sha256` states the same over the stored passages, and is
    what a citation records and is read against."""
    return hashlib.sha256(" ".join(text.split()).encode()).hexdigest()


def section_sha256(
    document_id: ColumnElement, heading: ColumnElement, page: ColumnElement
) -> ColumnElement:
    """The hash of the text of the section named by ``heading`` or ``page`` (the whole document
    when neither), from the document's passages in order, its whitespace laid out as one space;
    NULL when the document holds no such section. One SQL expression, so a citation is recorded
    and read by the same statement wherever it is read: here, and in the tags report's relation
    (provisa.api._meta_views)."""
    p = passages_t.alias("cited_passage")
    named = or_(
        and_(heading.is_not(None), p.c.section == heading),
        and_(heading.is_(None), page.is_not(None), p.c.page == page),
        and_(heading.is_(None), page.is_(None)),
    )
    joined = func.string_agg(p.c.text, aggregate_order_by(literal(" "), p.c.ordinal))
    laid_out = func.btrim(func.regexp_replace(joined, r"\s+", " ", "g"))
    digest = func.encode(func.sha256(func.convert_to(laid_out, "UTF8")), "hex")
    return select(digest).where(p.c.document_id == document_id, named).scalar_subquery()


def citation_state(c) -> ColumnElement:  # noqa: ANN001 - a reference_citations table or alias
    """A citation's state against its document's passages as held now: :data:`BROKEN` when the
    document or the section is gone, :data:`DRIFTED` when the section's text changed since it was
    cited, else :data:`CURRENT`."""
    held = section_sha256(c.c.document_id, c.c.section_heading, c.c.section_page)
    return case(
        (c.c.document_id.is_(None), BROKEN),
        (held.is_(None), BROKEN),
        (held != c.c.text_sha256, DRIFTED),
        else_=CURRENT,
    )


def citation_states():
    """Every citation with its state (column ``state``): the one reading of a citation's state."""
    return select(citations_t, citation_state(citations_t).label("state"))


async def cite(
    conn: Connection,
    citer_kind: str,
    tag_id: str | None,
    object_key: str,
    place: Place,
    cited_by: str | None,
) -> Citation:
    """Record that the citer cites ``place``, keeping the section's text hash and the document's
    title and source as they are now; citing it again records them again (a review's
    confirmation). The document must be ready, and hold the section."""
    document = await store.document(conn, place.document_id)
    if document is None:
        raise library.DocumentNotFound(place.document_id)
    try:
        library.require_ready(document)
    except library.LibraryRefused as exc:
        raise CitationRefused(str(exc)) from None
    held = (
        await conn.execute_core(
            select(
                section_sha256(
                    literal(place.document_id),
                    literal(place.section_heading, type_=passages_t.c.section.type),
                    literal(place.section_page, type_=passages_t.c.page.type),
                )
            )
        )
    ).scalar_one()
    if held is None:
        named = (
            f"heading {place.section_heading!r}"
            if place.section_heading is not None
            else f"page {place.section_page}"
        )
        raise CitationRefused(f"{document.title!r} has no section at {named}")
    values = {
        "document_id": place.document_id,
        "document_title": document.title,
        "document_source": document.source,
        "section_heading": place.section_heading,
        "section_page": place.section_page,
        "text_sha256": held,
        "cited_by": cited_by,
    }
    await conn.upsert(
        citations_t,
        {
            "citer_kind": citer_kind,
            "tag_id": tag_id,
            "object_key": object_key,
            "cite_key": place.key(),
            **values,
        },
        index_elements=["citer_kind", "tag_id", "object_key", "cite_key"],
        update_columns=list(values),
        set_extra={"cited_at": func.now()},
    )
    found = await of_citers(conn, citer_kind, [tag_id] if tag_id is not None else None)
    return next(c for c in found.get((tag_id, object_key), []) if citation_key(c) == place.key())


def citation_key(citation: Citation) -> str:
    """The name of the place a held citation cites, as :meth:`Place.key` names it."""
    document_id = citation.document_id if citation.document_id is not None else -1
    return Place(document_id, citation.section_heading, citation.section_page).key()


async def uncite(
    conn: Connection, citer_kind: str, tag_id: str | None, object_key: str, place: Place
) -> bool:
    """Take the citer's citation of ``place`` off; False when it cites no such place."""
    return await uncite_key(conn, citer_kind, tag_id, object_key, place.key())


async def uncite_key(
    conn: Connection, citer_kind: str, tag_id: str | None, object_key: str, cite_key: str
) -> bool:
    """Take the citer's citation named ``cite_key`` off; False when it cites no such place."""
    result = await conn.execute_core(
        delete(citations_t).where(
            citations_t.c.citer_kind == citer_kind,
            citations_t.c.tag_id == tag_id,
            citations_t.c.object_key == object_key,
            citations_t.c.cite_key == cite_key,
        )
    )
    return (result.rowcount or 0) > 0


async def uncite_all(
    conn: Connection, citer_kind: str, tag_id: str | None, object_key: str
) -> None:
    """Every citation the citer makes taken off: the citer itself is gone."""
    await conn.execute_core(
        delete(citations_t).where(
            citations_t.c.citer_kind == citer_kind,
            citations_t.c.tag_id == tag_id,
            citations_t.c.object_key == object_key,
        )
    )


def _with_state(rows: Sequence) -> list[Citation]:
    """Each citation row of :func:`citation_states`, its state read by that statement."""
    return [
        Citation(
            citer_kind=r.citer_kind,
            tag_id=r.tag_id,
            object_key=r.object_key,
            document_id=r.document_id,
            document_title=r.document_title,
            document_source=r.document_source,
            section_heading=r.section_heading,
            section_page=r.section_page,
            text_sha256=r.text_sha256,
            cited_by=r.cited_by,
            cited_at=r.cited_at,
            state=r.state,
        )
        for r in rows
    ]


_ORDER = (
    citations_t.c.tag_id,
    citations_t.c.object_key,
    citations_t.c.document_title,
    citations_t.c.section_page,
    citations_t.c.section_heading,
)


async def of_citers(
    conn: Connection, citer_kind: str, tag_ids: Collection[str] | None = None
) -> dict[tuple[str | None, str], list[Citation]]:
    """Every citation of citers of ``citer_kind`` (of ``tag_ids`` only, when given), each with its
    state, keyed by its citer: (tag id, object key)."""
    where = [citations_t.c.citer_kind == citer_kind]
    if tag_ids is not None:
        where.append(citations_t.c.tag_id.in_(sorted(tag_ids)))
    rows = (
        await conn.execute_core(citation_states().where(and_(*where)).order_by(*_ORDER))
    ).fetchall()
    out: dict[tuple[str | None, str], list[Citation]] = {}
    for citation in _with_state(rows):
        out.setdefault((citation.tag_id, citation.object_key), []).append(citation)
    return out


async def citing(
    conn: Connection,
    document_id: int,
    section_heading: str | None = None,
    section_page: int | None = None,
) -> list[Citation]:
    """What cites the document, each citation with its state; with a heading or a page, only what
    cites that section."""
    where = [citations_t.c.document_id == document_id]
    if section_heading is not None:
        where.append(citations_t.c.section_heading == section_heading)
    if section_page is not None:
        where.append(citations_t.c.section_page == section_page)
    rows = (
        await conn.execute_core(
            citation_states().where(and_(*where)).order_by(citations_t.c.citer_kind, *_ORDER)
        )
    ).fetchall()
    return _with_state(rows)
