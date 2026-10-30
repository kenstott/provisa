# Copyright (c) 2026 Kenneth Stott
# Canary: 1d9c6b38-e2f7-4a05-9c81-5b3e07a2f6d4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The steward's decisions on a draft (REQ-2179): edit, accept, merge, reject.

Accepting a draft adds its term to the glossary (a Concept, in the scan's domains) with its
definition, and each variant kept as a synonym becomes a term SYNONYM_OF it. Merging a draft into
a glossary term makes the draft's name and its kept variants synonyms of that term, and gives the
term the draft's definition only when the steward says so. A synonym already in the glossary is
linked, not added again. Who may make each change is the router's question (the glossary's
curation rules, REQ-1591/REQ-1592), asked before any of these run.
"""

# Requirements: REQ-2179

from __future__ import annotations

from collections.abc import Collection, Iterable
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.core.repositories import glossary as glossary_repo
from provisa.core.schema_org import glossary_terms
from provisa.term_finding import queue
from provisa.term_finding.scan import normal

if TYPE_CHECKING:
    from provisa.core.database import Connection

SYNONYM_OF = "SYNONYM_OF"


class ReviewRefused(ValueError):
    """A decision refused, said naming why; ``status`` for the API."""

    status = 400


class DraftNotFound(ReviewRefused):
    status = 404

    def __init__(self, draft_id: int) -> None:
        super().__init__(f"the review queue holds no draft {draft_id}")


async def held(conn: Connection, draft_id: int) -> queue.DraftRow:
    """The draft, still waiting on a decision."""
    found = await queue.draft(conn, draft_id)
    if found is None:
        raise DraftNotFound(draft_id)
    if found.status != queue.DRAFT:
        raise ReviewRefused(f"draft {draft_id} ({found.name!r}) is already {found.status}")
    return found


def kept_synonyms(found: queue.DraftRow) -> list[str]:
    """The variants the draft proposes as synonyms, as term names."""
    return list(
        dict.fromkeys(v["text"].strip().lower() for v in found.payload["variants"] if v["keep"])
    )


def merge_names(found: queue.DraftRow, target_name: str) -> list[str]:
    """The names a merge into ``target_name`` makes its synonyms: the draft's own name and its
    kept variants, less any that name the target itself."""
    target = normal(target_name)
    return [n for n in dict.fromkeys([found.name, *kept_synonyms(found)]) if normal(n) != target]


async def existing_terms(conn: Connection, names: Iterable[str]) -> dict[str, int]:
    """The glossary terms named ``names``, by name."""
    wanted = sorted({n.strip().lower() for n in names})
    if not wanted:
        return {}
    rows = (
        await conn.execute_core(
            select(glossary_terms.c.name, glossary_terms.c.id).where(
                glossary_terms.c.name.in_(wanted)
            )
        )
    ).fetchall()
    return {name: int(term_id) for name, term_id in rows}


async def _link_synonym(
    conn: Connection, name: str, term_id: int, domains: Collection[str]
) -> None:
    held_terms = await existing_terms(conn, [name])
    synonym = held_terms.get(name)
    if synonym == term_id:
        return
    if synonym is None:
        synonym = await glossary_repo.create_abstract_term(conn, name, domains=set(domains))
    await glossary_repo.add_edge(conn, synonym, term_id, SYNONYM_OF)


def edited(found: queue.DraftRow, body: dict[str, Any]) -> tuple[str, str | None, dict]:
    """The draft's name, definition and payload with the steward's edits: ``name``,
    ``definition`` (null clears it), ``synonyms`` (the variant texts proposed as synonyms; a
    text the scan did not find is added as another name)."""
    unknown = sorted(set(body) - {"name", "definition", "synonyms"})
    if unknown:
        raise ReviewRefused(f"a draft's {unknown[0]!r} is not edited here")
    name = found.name
    if "name" in body:
        if not isinstance(body["name"], str) or not body["name"].strip():
            raise ReviewRefused("a draft's name is text")
        name = " ".join(body["name"].split()).lower()
    definition = found.definition
    if "definition" in body:
        raw = body["definition"]
        if raw is not None and not isinstance(raw, str):
            raise ReviewRefused("a draft's definition is text")
        definition = raw.strip() if raw and raw.strip() else None
    payload = dict(found.payload)
    if "synonyms" in body:
        raw = body["synonyms"]
        if not isinstance(raw, list) or not all(isinstance(s, str) and s.strip() for s in raw):
            raise ReviewRefused("a draft's synonyms are a list of names")
        keep = {" ".join(s.split()).lower() for s in raw}
        variants = [{**v, "keep": v["text"].lower() in keep} for v in payload["variants"]]
        held_texts = {v["text"].lower() for v in variants}
        variants += [
            {"text": s, "kind": "alternative name", "keep": True} for s in sorted(keep - held_texts)
        ]
        payload["variants"] = variants
    return name, definition, payload


async def accept(
    conn: Connection, found: queue.DraftRow, domains: Collection[str], by: str | None
) -> int:
    """Add the draft's term to the glossary with its kept synonyms; the new term's id."""
    if (await existing_terms(conn, [found.name])).get(found.name) is not None:
        raise ReviewRefused(
            f"the glossary already holds a term {found.name!r}: merge the draft into it, or "
            "rename the draft"
        )
    term_id = await glossary_repo.create_abstract_term(
        conn, found.name, definition=found.definition, domains=set(domains)
    )
    for name in kept_synonyms(found):
        if name != found.name:
            await _link_synonym(conn, name, term_id, domains)
    await queue.decide(conn, found.id, queue.ACCEPTED, term_id=term_id, by=by)
    return term_id


async def merge(
    conn: Connection,
    found: queue.DraftRow,
    target_id: int,
    *,
    use_definition: bool,
    domains: Collection[str],
    by: str | None,
) -> None:
    """Merge the draft into the glossary term ``target_id``."""
    target = await glossary_repo.get_term(conn, target_id)
    if target is None:
        raise ReviewRefused(f"the glossary holds no term {target_id}")
    for name in merge_names(found, target["name"]):
        await _link_synonym(conn, name, target_id, domains)
    if use_definition:
        if found.definition is None:
            raise ReviewRefused(f"draft {found.id} ({found.name!r}) has no definition to use")
        await glossary_repo.set_definition(conn, target_id, found.definition)
    await queue.decide(conn, found.id, queue.MERGED, term_id=target_id, by=by)


async def reject(conn: Connection, found: queue.DraftRow, by: str | None) -> None:
    await queue.decide(conn, found.id, queue.REJECTED, term_id=None, by=by)
