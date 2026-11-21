# Copyright (c) 2026 Kenneth Stott
# Canary: 6a0f4d72-b9e3-4c81-8f25-d3e70b6a1c49
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A steward's decisions on an import, and applying it to the glossary (REQ-2237).

The steward edits an item (its ``name`` and ``definition``) or excludes it, approves or rejects
each proposed alignment, and adds alignments of their own. Applying the import then, for every
item still pending:

- an item approved as the SAME_AS a term is that term: the term takes the item's IRI, and its
  definition when the term has none or the steward chose the item's (``use_definition``);
- every other item becomes a draft Concept in the import's domains, carrying its definition and
  IRI, and grounds only through the alignments approved for it.

Either way the term keeps what the ontology stated about the item that a term does not model
(its restrictions, characteristics, individuals, annotations: provisa.ontology_import.kept).

Every approved alignment that is not SAME_AS is written as the glossary edge it names. One whose
other end is an excluded item is left waiting. Nothing is approved here: a term the import makes
is a draft, and an alignment is written only when a steward approved it. An import is applied as
a whole or not at all: what stops it (a name or an IRI another term already holds, two terms
approved as the same entity) is refused together, each naming what to do. Applying again writes
what was approved since. Who may change which terms is the router's question, asked first.
"""

# Requirements: REQ-2237

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.core.repositories import glossary as glossary_repo
from provisa.core.schema_org import glossary_terms
from provisa.ontology_import import store
from provisa.ontology_import.align import ALIGNMENT_TYPES, SAME_AS, STEWARD

if TYPE_CHECKING:
    from provisa.core.database import Connection


class ImportRefused(ValueError):
    """A decision refused, said naming why; ``status`` for the API."""

    status = 400


class NotFound(ImportRefused):
    status = 404


class ApplyRefused(ImportRefused):
    """An apply refused for every reason in ``problems``."""

    status = 409

    def __init__(self, problems: list[str]) -> None:
        super().__init__("; ".join(problems))
        self.problems = problems


async def held_item(conn: Connection, item_id: int) -> store.ItemRow:
    found = await store.item(conn, item_id)
    if found is None:
        raise NotFound(f"no import holds an item {item_id}")
    return found


async def held_alignment(conn: Connection, alignment_id: int) -> store.AlignmentRow:
    found = await store.alignment(conn, alignment_id)
    if found is None:
        raise NotFound(f"no import holds an alignment {alignment_id}")
    return found


def edited(found: store.ItemRow, body: dict[str, Any]) -> tuple[str, str | None, str]:
    """The item's name, definition and status with the steward's edits: ``name``,
    ``definition`` (null clears it) and ``excluded``. An applied item is the glossary's now."""
    unknown = sorted(set(body) - {"name", "definition", "excluded"})
    if unknown:
        raise ImportRefused(f"an imported item's {unknown[0]!r} is not edited here")
    if found.status == store.APPLIED:
        raise ImportRefused(f"{found.name!r} is in the glossary now: edit its term instead")
    name = found.name
    if "name" in body:
        if not isinstance(body["name"], str) or not body["name"].strip():
            raise ImportRefused("an imported item's name is text")
        name = " ".join(body["name"].split()).lower()
    definition = found.definition
    if "definition" in body:
        raw = body["definition"]
        if raw is not None and not isinstance(raw, str):
            raise ImportRefused("an imported item's definition is text")
        definition = raw.strip() if raw and raw.strip() else None
    status = found.status
    if "excluded" in body:
        if not isinstance(body["excluded"], bool):
            raise ImportRefused("excluded is true or false")
        status = store.EXCLUDED if body["excluded"] else store.PENDING
    return name, definition, status


async def decide(
    conn: Connection,
    found: store.AlignmentRow,
    status: str,
    *,
    use_definition: Any,
    by: str | None,
) -> None:
    """Approve or reject the alignment; ``use_definition`` gives a SAME_AS term the item's
    definition in place of its own. A written alignment is an edge of the glossary now."""
    if found.status == store.WRITTEN:
        raise ImportRefused(f"alignment {found.id} is in the glossary now: edit the term instead")
    if not isinstance(use_definition, bool):
        raise ImportRefused("use_definition is true or false")
    if use_definition and found.rel_type != SAME_AS:
        raise ImportRefused("only a term aligned as the same entity takes the item's definition")
    if status == store.APPROVED and found.rel_type == SAME_AS:
        item = await held_item(conn, found.item_id)
        if item.status == store.APPLIED:
            raise ImportRefused(
                f"{item.name!r} is in the glossary already, as a term of its own: relate it to "
                "the term instead"
            )
    await store.decide(conn, found.id, status, use_definition=use_definition, by=by)


async def add(
    conn: Connection, imported: store.ImportRow, body: dict[str, Any], by: str | None
) -> int:
    """Add the steward's own alignment: ``item_id`` with ``term_id`` or ``other_item_id``, of
    ``rel_type``, read from the item unless ``item_is_source`` is false."""
    unknown = sorted(
        set(body)
        - {"item_id", "term_id", "other_item_id", "rel_type", "item_is_source", "use_definition"}
    )
    if unknown:
        raise ImportRefused(f"an alignment's {unknown[0]!r} is not set here")

    def an_id(key: str) -> int | None:
        value = body.get(key)
        if value is not None and (not isinstance(value, int) or isinstance(value, bool)):
            raise ImportRefused(f"{key} is an id")
        return value

    item_id, term_id, other_item_id = an_id("item_id"), an_id("term_id"), an_id("other_item_id")
    if item_id is None:
        raise ImportRefused("an alignment names the imported item it aligns")
    if (term_id is None) == (other_item_id is None):
        raise ImportRefused("an alignment aligns the item with one term or one other item")
    rel_type = body.get("rel_type")
    if rel_type not in ALIGNMENT_TYPES:
        raise ImportRefused(f"rel_type is one of {', '.join(ALIGNMENT_TYPES)}")
    item_is_source = body.get("item_is_source", True)
    use_definition = body.get("use_definition", False)
    if not isinstance(item_is_source, bool) or not isinstance(use_definition, bool):
        raise ImportRefused("item_is_source and use_definition are true or false")
    if rel_type == SAME_AS and term_id is None:
        raise ImportRefused("an item is the same entity as a term, never as another item")
    if use_definition and rel_type != SAME_AS:
        raise ImportRefused("only a term aligned as the same entity takes the item's definition")
    item = await held_item(conn, item_id)
    if item.import_id != imported.id:
        raise NotFound(f"import {imported.id} holds no item {item_id}")
    if other_item_id is not None:
        other = await held_item(conn, other_item_id)
        if other.import_id != imported.id or other.id == item.id:
            raise ImportRefused(f"import {imported.id} holds no other item {other_item_id}")
    if term_id is not None and await glossary_repo.get_term(conn, term_id) is None:
        raise NotFound(f"the glossary holds no term {term_id}")
    if rel_type == SAME_AS and item.status == store.APPLIED:
        raise ImportRefused(f"{item.name!r} is in the glossary already, as a term of its own")
    return await store.add_alignment(
        conn,
        import_id=imported.id,
        item_id=item_id,
        term_id=term_id,
        other_item_id=other_item_id,
        rel_type=rel_type,
        item_is_source=item_is_source,
        basis=STEWARD,
        use_definition=use_definition,
        by=by,
    )


def touched_terms(alignments: Collection[store.AlignmentRow]) -> set[int]:
    """The existing terms applying would change: every approved alignment's term."""
    return {a.term_id for a in alignments if a.status == store.APPROVED and a.term_id is not None}


@dataclass
class Applied:
    created: list[dict[str, Any]] = field(default_factory=list)
    aligned: list[dict[str, Any]] = field(default_factory=list)
    edges: list[dict[str, Any]] = field(default_factory=list)
    waiting: list[dict[str, Any]] = field(default_factory=list)


async def _terms(conn: Connection) -> list[Any]:
    return list(
        (
            await conn.execute_core(
                select(
                    glossary_terms.c.id,
                    glossary_terms.c.name,
                    glossary_terms.c.iri,
                    glossary_terms.c.definition,
                    glossary_terms.c.calculation_method,
                )
            )
        ).fetchall()
    )


def _problems(
    pending: list[store.ItemRow], same: dict[int, list[store.AlignmentRow]], terms: list[Any]
) -> list[str]:
    by_id = {t.id: t for t in terms}
    by_name = {t.name: t for t in terms}
    by_iri = {t.iri: t for t in terms if t.iri}
    problems: list[str] = []
    for item in pending:
        chosen = same.get(item.id, [])
        if len(chosen) > 1:
            names = ", ".join(repr(by_id[a.term_id].name) for a in chosen if a.term_id in by_id)
            problems.append(
                f"{item.name!r} is approved as the same entity as {names}: reject all but one"
            )
            continue
        if chosen:
            term = by_id.get(chosen[0].term_id)
            if term is None:
                problems.append(
                    f"{item.name!r} is approved as the same entity as a term no longer in the "
                    "glossary"
                )
            elif item.iri and term.iri and term.iri != item.iri:
                problems.append(
                    f"{item.name!r} is approved as the same entity as {term.name!r}, which "
                    f"already carries the IRI {term.iri}"
                )
            elif item.iri and item.iri in by_iri and by_iri[item.iri].id != term.id:
                problems.append(
                    f"{item.name!r} is approved as the same entity as {term.name!r}, but its "
                    f"IRI is carried by {by_iri[item.iri].name!r}"
                )
            continue
        if item.name in by_name:
            problems.append(
                f"the glossary already holds a term {item.name!r}: approve aligning the item "
                "with it as the same entity, rename the item, or exclude it"
            )
        elif item.iri and item.iri in by_iri:
            problems.append(
                f"{by_iri[item.iri].name!r} already carries the IRI of {item.name!r}: approve "
                "aligning the item with it as the same entity, or exclude it"
            )
    return problems


def _statements(item: store.ItemRow) -> list[dict[str, Any]]:
    """What the ontology stated about the item that a term does not model: kept on its term."""
    return list(item.payload.get("statements", []))


async def apply(
    conn: Connection, imported: store.ImportRow, domains: Collection[str], by: str | None
) -> Applied:
    """Apply the import, as the module says; what it created, aligned, wrote and left waiting."""
    items = await store.items(conn, imported.id)
    approved = [a for a in await store.alignments(conn, imported.id) if a.status == store.APPROVED]
    pending = [i for i in items if i.status == store.PENDING]
    same: dict[int, list[store.AlignmentRow]] = {}
    for a in approved:
        if a.rel_type == SAME_AS:
            same.setdefault(a.item_id, []).append(a)
    terms = await _terms(conn)
    problems = _problems(pending, same, terms)
    if problems:
        raise ApplyRefused(problems)
    by_id = {t.id: t for t in terms}
    done = Applied()
    resolved = {i.id: i.term_id for i in items if i.status == store.APPLIED and i.term_id}
    for item in pending:
        if item.id in same:
            (chosen,) = same[item.id]
            assert chosen.term_id is not None
            term = by_id[chosen.term_id]
            if item.iri and not term.iri:
                await glossary_repo.set_iri(conn, term.id, item.iri)
            if item.definition and (chosen.use_definition or not (term.definition or "").strip()):
                await glossary_repo.set_definition(conn, term.id, item.definition)
            # REQ-2243: the method goes with the definition, by the same choice.
            method = item.payload["calculation_method"]
            if method and (chosen.use_definition or not (term.calculation_method or "").strip()):
                await glossary_repo.set_calculation_method(conn, term.id, method)
            await store.written(conn, chosen.id)
            resolved[item.id] = term.id
            await glossary_repo.keep_statements(conn, term.id, _statements(item))
            done.aligned.append({"item_id": item.id, "term_id": term.id, "name": term.name})
        else:
            term_id = await glossary_repo.create_abstract_term(
                conn, item.name, definition=item.definition, domains=set(domains)
            )
            if item.iri:
                await glossary_repo.set_iri(conn, term_id, item.iri)
            if item.payload["calculation_method"]:  # REQ-2243
                await glossary_repo.set_calculation_method(
                    conn, term_id, item.payload["calculation_method"]
                )
            await glossary_repo.keep_statements(conn, term_id, _statements(item))
            resolved[item.id] = term_id
            done.created.append({"item_id": item.id, "term_id": term_id, "name": item.name})
        await store.item_applied(conn, item.id, resolved[item.id])
    for a in approved:
        if a.rel_type == SAME_AS:
            continue
        own = resolved.get(a.item_id)
        other = a.term_id if a.term_id is not None else resolved.get(a.other_item_id or 0)
        if own is None or other is None:
            done.waiting.append({"alignment_id": a.id, "reason": "an end of it is excluded"})
            continue
        source, target = (own, other) if a.item_is_source else (other, own)
        if source != target:
            await glossary_repo.add_edge(conn, source, target, a.rel_type)
            done.edges.append(
                {"alignment_id": a.id, "from": source, "rel_type": a.rel_type, "to": target}
            )
        await store.written(conn, a.id)
    await store.applied(conn, imported.id, by)
    return done
