# Copyright (c) 2026 Kenneth Stott
# Canary: 02f81c1f-4fd7-41f6-946d-b26f4fca4cce
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tags a column carries through its glossary term (REQ-2145).

pii is on columns, and on a Property term. Only a term that is defined and in service -- live, or
deprecated -- carries a tag; a draft (undefined) or retired term governs nothing. Every column whose
ref is on such a term tagged pii carries pii, sensitive included (REQ-1943). Nothing is copied: the
column's tags are read as its own assignments plus its term's, here, so a column moved onto the
term carries the tag at once and a column moved off it no longer does.

Each effective tag says where it comes from: ``{"via": "column", "reason": ...}`` for an
assignment on the column itself, ``{"via": "term", "term": <name>, "term_id": ...}`` for one on
its term. A column can carry a tag both ways; it then has both.

A move a person makes of a column's ref (:func:`tag_changes_of_moves`) is refused when a column
loses a restricting tag it carried through its term, unless the caller confirms each lost tag or
carries it to the term the column moves to. A move the model makes by derivation -- sync's
re-derivation of a draft term's refs, key refs re-homed when a table is grounded or ungrounded --
changes no inherited tag by construction: a draft governs nothing, and a key ref only ever moves
between Classes, which carry no tag. A column's own tags never change on a move.
"""

# Requirements: REQ-2145

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable

from sqlalchemy import func, select

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: The tags a term may carry (REQ-2145: pii only, for now).
TERM_TAGS = ("pii",)

#: The tags whose loss on a move must be confirmed: the restricting ones (pii, now).
RESTRICTING_TAGS = ("pii",)


def governs(glossary_terms: Any) -> Any:
    """The terms whose tags their columns carry: defined, and not retired."""
    return (func.coalesce(func.trim(glossary_terms.c.definition), "") != "") & (
        glossary_terms.c.retired.is_(False)
    )


def inherited_column_tags(tag_assignments: Any, term_refs: Any, glossary_terms: Any) -> Any:
    """Each (table_id, column_name, tag_id, base_tag_id, term_id) a column carries through its
    term: a tag on a term that governs, once for every column ref the term holds. The tables are
    passed so another environment's schema can be addressed."""
    ta, refs, terms = tag_assignments, term_refs, glossary_terms
    return (
        select(
            refs.c.table_id,
            refs.c.column_name,
            ta.c.tag_id,
            ta.c.base_tag_id,
            ta.c.term_id,
        )
        .select_from(
            ta.join(refs, refs.c.term_id == ta.c.term_id).join(terms, terms.c.id == ta.c.term_id)
        )
        .where(ta.c.object_type == "term", governs(terms))
    )


async def effective_column_tags(
    conn: "Connection", table_ids: "Iterable[int] | None" = None
) -> dict[tuple[int, str], list[dict]]:
    """Every column's tags, by (table_id, column_name), each with its provenance: the column's own
    assignments and its term's (see the module docstring), in one statement. ``table_ids``
    narrows it to those tables."""
    from sqlalchemy import literal, union_all

    from provisa.core.schema_org import glossary_term_refs, glossary_terms, tag_assignments as ta

    direct = select(
        ta.c.table_id,
        ta.c.column_name,
        ta.c.tag_id,
        ta.c.base_tag_id,
        ta.c.reason,
        literal(None).label("term_id"),
        literal(None).label("term"),
    ).where(ta.c.object_type == "column")
    inherited = inherited_column_tags(ta, glossary_term_refs, glossary_terms).subquery()
    through = select(
        inherited.c.table_id,
        inherited.c.column_name,
        inherited.c.tag_id,
        inherited.c.base_tag_id,
        literal(None).label("reason"),
        inherited.c.term_id,
        glossary_terms.c.name.label("term"),
    ).join(glossary_terms, glossary_terms.c.id == inherited.c.term_id)
    if table_ids is not None:
        ids = sorted(set(table_ids))
        direct = direct.where(ta.c.table_id.in_(ids))
        through = through.where(inherited.c.table_id.in_(ids))
    out: dict[tuple[int, str], list[dict]] = {}
    for r in (await conn.execute_core(union_all(direct, through))).fetchall():
        tag = {"tag_id": r.tag_id, "base_tag_id": r.base_tag_id}
        if r.term_id is None:
            tag |= {"via": "column", "reason": r.reason}
        else:
            tag |= {"via": "term", "term": r.term, "term_id": r.term_id}
        out.setdefault((r.table_id, r.column_name), []).append(tag)
    return out


async def column_tag_ids(conn: "Connection", table_id: int) -> dict[str, set[str]]:
    """The base ids of the tags each column of table ``table_id`` carries, its term's included."""
    out: dict[str, set[str]] = {}
    for (_, column), tags in (await effective_column_tags(conn, {table_id})).items():
        out.setdefault(column, set()).update(t["base_tag_id"] for t in tags)
    return out


async def inherited_from(
    conn: "Connection", table_id: int, column_name: str, base_tag_id: str
) -> dict | None:
    """The term column ``table_id.column_name`` carries ``base_tag_id`` through, as
    ``{"term_id", "term"}``, or None when it does not carry it through a term."""
    for tag in (await effective_column_tags(conn, {table_id})).get((table_id, column_name), []):
        if tag["via"] == "term" and tag["base_tag_id"] == base_tag_id:
            return {"term_id": tag["term_id"], "term": tag["term"]}
    return None


# ---------------------------------------------------------------------------
# Tagging a term
# ---------------------------------------------------------------------------


class TermTagRefused(ValueError):
    """A tag a term may not take, with the code and parameters the surfaces answer with."""

    def __init__(self, code: str, message: str, **params: Any) -> None:
        super().__init__(message)
        self.code = code
        self.params = params


async def refuse_term_tag(conn: "Connection", term_id: int, tag_id: str) -> None:
    """Raise :class:`TermTagRefused` unless term ``term_id`` may take tag ``tag_id``: a tag that
    goes on a term (pii), on a term that is defined and in service, and a Property."""
    from provisa.core.models import base_tag_id
    from provisa.core.repositories import glossary as glossary_repo
    from provisa.core.schema_org import glossary_terms

    if base_tag_id(tag_id) not in TERM_TAGS:
        raise TermTagRefused(
            "tag.not_on_terms",
            f"Tag {tag_id!r} does not go on a term; only {', '.join(TERM_TAGS)} does",
            tag=tag_id,
        )
    row = (
        await conn.execute_core(
            select(
                glossary_terms.c.name, glossary_terms.c.definition, glossary_terms.c.retired
            ).where(glossary_terms.c.id == term_id)
        )
    ).fetchone()
    if row is None:
        raise TermTagRefused("glossary.term_not_found", f"term {term_id} not found", term=term_id)
    if row.retired or not (row.definition or "").strip():
        raise TermTagRefused("tag.term_not_live", f"Define '{row.name}' to mark it", term=row.name)
    role = await glossary_repo.term_role(conn, term_id)
    if role != "property":
        raise TermTagRefused(
            "tag.term_not_property",
            f"Tag {tag_id!r} goes on a term only when it is a Property, one that means columns; "
            f"{row.name!r} is a {role}",
            tag=tag_id,
            term=row.name,
            role=role,
        )


# ---------------------------------------------------------------------------
# Moves a person makes of column refs
# ---------------------------------------------------------------------------


class TagLossRefused(ValueError):
    """A move refused because columns would lose restricting tags they carry through their term,
    unconfirmed (409 tag.move_loses). ``columns``: each moved column's change, as
    ``{column, table, loses, gains}``."""

    code = "tag.move_loses"

    def __init__(self, columns: list[dict]) -> None:
        named = "; ".join(
            f"{c['table']}.{c['column']} loses {', '.join(c['loses'])}"
            for c in columns
            if c["loses"]
        )
        super().__init__(
            f"This move takes tags off columns: {named}. Confirm the loss of each tag "
            "(confirm_tag_loss), or carry the tags to the term the columns move to (carry_tags)."
        )
        self.columns = columns


async def inherited_snapshot(
    conn: "Connection", table_ids: "Iterable[int] | None" = None
) -> dict[tuple[int, str], dict]:
    """Each tagged column's tags, by (table_id, column_name): ``{"tags": {base ids it carries
    through its term}, "carried": {every base id it carries}, "terms": {term_id: name}}`` -- the
    before of a move, for :func:`tag_changes_of_moves`."""
    out: dict[tuple[int, str], dict] = {}
    for key, tags in (await effective_column_tags(conn, table_ids)).items():
        through = [t for t in tags if t["via"] == "term"]
        out[key] = {
            "tags": {t["base_tag_id"] for t in through},
            "carried": {t["base_tag_id"] for t in tags},
            "terms": {t["term_id"]: t["term"] for t in through},
        }
    return out


async def tag_changes_of_moves(
    conn: "Connection",
    before: dict[tuple[int, str], dict],
    table_ids: "Iterable[int] | None" = None,
    *,
    confirm_tag_loss: "Iterable[str]" = (),
    carry_tags: bool = False,
) -> list[dict]:
    """THE check of a move a person makes of column refs -- a ref move, a claim, retire's move of a
    term's columns, a relationship saved or deleted that re-homes key refs. Call it after the move,
    in the move's transaction, with ``before`` (:func:`inherited_snapshot` of the same tables taken
    before the move); a refusal raised here undoes the move with the transaction.

    Returns each column whose inherited tags changed, as ``{column, table, loses, gains}`` -- a
    column that keeps a tag through its own assignment is listed, its provenance having changed,
    and needs no confirming. A column that stops carrying a restricting tag (``RESTRICTING_TAGS``)
    altogether refuses the move
    (:class:`TagLossRefused`) unless ``confirm_tag_loss`` names every tag lost; with
    ``carry_tags`` the lost tags are put on the term each column moved to instead -- refused when
    that term may not take them (:class:`TermTagRefused`) -- so nothing is lost."""
    from provisa.core.repositories import glossary as glossary_repo
    from provisa.core.schema_org import registered_tables

    async def changes() -> list[dict]:
        after = await inherited_snapshot(conn, table_ids)
        out = []
        for key in sorted(set(before) | set(after)):
            had = before.get(key, {}).get("tags", set())
            has = after.get(key, {}).get("tags", set())
            if had != has:
                gone = before.get(key, {}).get("carried", set()) - after.get(key, {}).get(
                    "carried", set()
                )
                out.append(
                    {
                        "table_id": key[0],
                        "column": key[1],
                        "loses": sorted(had - has),
                        "gains": sorted(has - had),
                        "gone": sorted(gone),
                    }
                )
        return out

    found = await changes()
    lost = {t for c in found for t in c["gone"] if t in RESTRICTING_TAGS}
    if lost and carry_tags:
        from provisa.core.models import TagAssignment
        from provisa.core.repositories import tag as tag_repo

        reasons = {}
        for held in before.values():
            for term_id in held["terms"]:
                for t in await glossary_repo.term_tags(conn, term_id):
                    reasons.setdefault(t["tag_id"], t["reason"])
        for c in found:
            moved_to = await glossary_repo.get_term_by_ref(conn, c["table_id"], c["column"])
            for tag_id in c["gone"]:
                if tag_id not in RESTRICTING_TAGS:
                    continue
                if moved_to is None:
                    raise TermTagRefused(
                        "tag.term_not_property",
                        f"Column {c['column']} moves to no term to carry {tag_id!r} to",
                        tag=tag_id,
                        column=c["column"],
                    )
                await refuse_term_tag(conn, moved_to["id"], tag_id)
                await tag_repo.assign(
                    conn,
                    TagAssignment(
                        tag_id=tag_id,
                        object_type="term",
                        term_id=moved_to["id"],
                        reason=reasons.get(tag_id),
                    ),
                )
        found = await changes()
        lost = {t for c in found for t in c["gone"] if t in RESTRICTING_TAGS}
    names = {}
    if found:
        rows = (
            await conn.execute_core(
                select(
                    registered_tables.c.id,
                    registered_tables.c.source_id,
                    registered_tables.c.schema_name,
                    registered_tables.c.table_name,
                ).where(registered_tables.c.id.in_(sorted({c["table_id"] for c in found})))
            )
        ).fetchall()
        names = {r.id: f"{r.source_id}.{r.schema_name}.{r.table_name}" for r in rows}
    summary = [
        {
            "column": c["column"],
            "table": names.get(c["table_id"], str(c["table_id"])),
            "loses": c["loses"],
            "gains": c["gains"],
        }
        for c in found
    ]
    if lost - set(confirm_tag_loss):
        raise TagLossRefused(summary)
    return summary
