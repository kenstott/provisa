# Copyright (c) 2026 Kenneth Stott
# Canary: 21fe6831-1a92-4290-a21a-ec4ddd5d5d5b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tag registry + assignment repository, via SQLAlchemy Core (dialect-portable).

One org-level registry (REQ-1373); assignments attach tags to sources, tables,
columns, and relationships (REQ-1377). The three system tags are code-defined
intrinsics (models.SYSTEM_TAGS) — present in every install, never stored — so the
tags table holds user tags only, and reads synthesize the system tags in front of
it. System-tag immutability is enforced at the mutation layer.
"""

# Requirements: REQ-1373, REQ-1375, REQ-1377

import datetime as _dt

from provisa.core import model_change
from typing import TYPE_CHECKING

from sqlalchemy import delete as _delete, func, select

from provisa.core.models import (
    LIBRARY_TAG_OBJECT_TYPES,
    DERIVED_TAG_IDS,
    DERIVED_TAGS,
    SYSTEM_TAG_IDS,
    RowRuleRef,
    SYSTEM_TAGS,
    Tag,
    TagAssignment,
    TagParamValue,
    base_tag_id,
)
from provisa.core.repositories.integrity import Dependent, ObjectRef, guard, remove_parts
from provisa.reference_library import citations
from provisa.core.schema_org import (
    glossary_terms,
    reference_document_tags,
    registered_tables,
    rls_rules,
    tag_assignments,
    tag_param_values,
    tags,
)

if TYPE_CHECKING:
    from provisa.core.database import Connection


def _code_tag_row(tag: Tag) -> dict:
    return {
        "id": tag.id,
        "description": tag.description,
        "applies_to": list(tag.applies_to),
        "is_system": True,
        "derived": tag.derived,
        "reason_policy": tag.reason_policy,
        "expires_policy": tag.expires_policy,
        "param_policy": tag.param_policy,
        "sensitive": tag.sensitive,
        "requires_citation": tag.requires_citation,
        "required": tag.required,
    }


def _user_tag_row(row) -> dict:
    # The tags table holds user tags only, so nothing in it is ever derived (REQ-1443) — the
    # column does not exist and the flag is supplied here rather than defaulted at every reader.
    return {**dict(row._mapping), "derived": False}


async def upsert(conn: "Connection", tag: Tag) -> None:  # REQ-1373, REQ-1919
    """Create the tag, or replace its definition."""
    model_change.name("upsert", "tag", tag.id)  # REQ-1524
    await conn.upsert(
        tags,
        {
            "id": tag.id,
            "description": tag.description,
            "applies_to": tag.applies_to,
            "is_system": tag.is_system,
            "reason_policy": tag.reason_policy,
            "expires_policy": tag.expires_policy,
            "param_policy": tag.param_policy,
            "sensitive": tag.sensitive,
            "requires_citation": tag.requires_citation,  # REQ-2208
            "required": tag.required,  # REQ-2208 (THE TAGS REPORT)
        },
        index_elements=["id"],
        update_columns=[
            "description",
            "applies_to",
            "reason_policy",
            "expires_policy",
            "param_policy",
            "sensitive",
            "requires_citation",
            "required",
        ],
    )


async def get(conn: "Connection", tag_id: str) -> dict | None:
    """Look up the registry tag *tag_id* names, accepting an assigned parameterized id.

    REQ-1467: callers hold ids in assigned form ("entity:customer"), and the registry is
    keyed by the base id — resolving here keeps every caller from having to split first.
    """
    base = base_tag_id(tag_id)
    for code_tag in SYSTEM_TAGS + DERIVED_TAGS:
        if code_tag.id == base:
            return _code_tag_row(code_tag)
    result = await conn.execute_core(select(tags).where(tags.c.id == base))
    row = result.fetchone()
    return _user_tag_row(row) if row is not None else None


async def list_all(conn: "Connection") -> list[dict]:
    result = await conn.execute_core(select(tags).order_by(tags.c.id))
    reserved = SYSTEM_TAG_IDS + DERIVED_TAG_IDS
    user_rows = [_user_tag_row(r) for r in result.fetchall() if r._mapping["id"] not in reserved]
    return [_code_tag_row(t) for t in SYSTEM_TAGS + DERIVED_TAGS] + user_rows


class TagDeleteRefused(Exception):  # REQ-1384
    """A tag that may not be deleted because objects still refer to it; ``dependents`` lists
    them. Today that is a row filter attached to the tag."""

    def __init__(self, tag_id: str, dependents: list[Dependent]) -> None:
        self.tag_id = tag_id
        self.dependents = dependents
        # What a refusal says before naming its dependents (REQ-2152: the API layer names only
        # those its caller may read).
        self.lead = f"Tag {tag_id!r} is still referred to by"
        named = ", ".join(f"{d.ref.kind} {d.name or d.ref.id}" for d in dependents)
        super().__init__(f"Tag {tag_id!r} is still referred to by: {named}")


async def delete(conn: "Connection", tag_id: str) -> bool:  # REQ-1918, REQ-1384
    """Delete a user tag: THE delete, for every surface. False when there is no such tag.

    A tag takes its assignments and its parameter values with it (REQ-1918): they are its
    parts, removed here because no foreign key carries them — system tags are code-defined with
    no row to reference. A row filter attached to the tag blocks it (REQ-1384,
    :class:`TagDeleteRefused`): removing the filter with the tag would widen what the filter's
    role reads. How many objects lose the tag is :func:`assignment_count`, for the confirmation
    shown before a tag that carries a policy is deleted. One transaction.
    """
    # The base id, not tag_id: a parameterized tag's assignments are stored in "{tag}:{value}"
    # form, so matching on tag_id would leave every one of them orphaned (REQ-1467).
    model_change.name("delete", "tag", tag_id)  # REQ-1524
    base = base_tag_id(tag_id)
    ref = ObjectRef("tag", base)
    async with conn.transaction():
        found = await conn.execute_core(select(tags.c.id).where(tags.c.id == base))
        if found.fetchone() is None:
            return False
        blocking = await guard(conn, ref)
        if blocking:
            raise TagDeleteRefused(base, blocking)
        await remove_parts(conn, ref)
        await conn.execute_core(_delete(tags).where(tags.c.id == base))
    return True


async def assignment_count(conn: "Connection", tag_id: str) -> int:
    """How many objects carry this tag, counting every parameter value (REQ-1467), the library's
    documents and sections included (REQ-2208)."""
    count = 0
    for held_in in (tag_assignments, reference_document_tags):
        result = await conn.execute_core(
            select(held_in.c.id).where(held_in.c.base_tag_id == base_tag_id(tag_id))
        )
        count += len(result.fetchall())
    return count


def _held_in(object_type: str):  # REQ-2208
    """The table that holds a tag on an object of ``object_type``: the library's for a document or
    a section, the environment's own as the documents are; tag_assignments, with the model, for
    every other object. A tag follows its object's environment class."""
    return reference_document_tags if object_type in LIBRARY_TAG_OBJECT_TYPES else tag_assignments


def _target(assignment: TagAssignment) -> dict:
    """The columns that name the tagged object, in the table that holds the tag."""
    if assignment.object_type in LIBRARY_TAG_OBJECT_TYPES:  # REQ-2208
        return {
            "document_id": assignment.document_id,
            "section_heading": assignment.section_heading,
            "section_page": assignment.section_page,
        }
    return {
        "source_id": assignment.source_id,
        "table_id": assignment.table_id,
        "column_name": assignment.column_name,
        "relationship_id": assignment.relationship_id,
        "command_name": assignment.command_name,
        "term_id": assignment.term_id,  # REQ-2145
        "product_id": assignment.product_id,  # REQ-1634
        "rls_rule_id": assignment.rls_rule_id,  # REQ-2208
        "role_id": assignment.role_id,
        "metric_name": assignment.metric_name,
    }


async def assign(  # REQ-1377, REQ-1919
    conn: "Connection", assignment: TagAssignment
) -> None:
    """Put the tag on the object, or change that assignment. REQ-2094: its why is stamped with
    who decided it, when and on what evidence when the tag or its why changes."""
    model_change.name("assign", "tag", assignment.tag_id)  # REQ-1524
    why = await _stamped_why(conn, assignment)
    await conn.upsert(
        _held_in(assignment.object_type),
        {
            "tag_id": assignment.tag_id,
            "base_tag_id": assignment.base_tag_id(),
            "object_type": assignment.object_type,
            **_target(assignment),
            "object_key": assignment.object_key(),
            "reason": assignment.reason,
            "expires_on": assignment.expires_on,
            "purpose": why.get("purpose"),
            "bases": why.get("bases"),
            "review_on": why.get("review_on"),
            "decided_by": why.get("decided_by"),
            "decided_at": _dt.datetime.fromisoformat(why["decided_at"])
            if why.get("decided_at")
            else None,
            "evidence": why.get("evidence"),
        },
        index_elements=["base_tag_id", "object_key"],
        # tag_id updates: re-assigning entity:employee where entity:customer sat is a
        # correction of the parameter, which is the only way to change one (REQ-1467).
        update_columns=[
            "tag_id",
            "object_type",
            "reason",
            "expires_on",
            "purpose",
            "bases",
            "review_on",
            "decided_by",
            "decided_at",
            "evidence",
        ],
    )


async def _stamped_why(conn: "Connection", assignment: TagAssignment) -> dict:
    """The assignment's why to store (REQ-2094): its reason (the ``reason`` column), and the given
    purpose, bases and review-by date -- the stored ones when the save does not restate them --
    with the decider, time and evidence moved when the tag or its why changes."""
    from provisa.core.schema_org import registered_tables as rt
    from provisa.judgments.evidence import evidence
    from provisa.judgments.why import given_part, parse, stamp_for

    held_in = _held_in(assignment.object_type)
    held = (
        await conn.execute_core(
            select(held_in).where(
                (held_in.c.base_tag_id == assignment.base_tag_id())
                & (held_in.c.object_key == assignment.object_key())
            )
        )
    ).fetchone()
    previous = None
    if held is not None:
        h = held._mapping
        previous = {
            "reason": h["reason"],
            "purpose": h["purpose"],
            "bases": h["bases"],
            "review_on": h["review_on"],
            "decided_by": h["decided_by"],
            "decided_at": h["decided_at"].isoformat() if h["decided_at"] else None,
            "evidence": h["evidence"],
        }
    where = f"tag {assignment.tag_id} on {assignment.object_key()}"
    given = given_part(previous) if assignment.why is None else parse(assignment.why, where)
    given = {k: v for k, v in given.items() if k != "reason"}
    if assignment.reason:
        given["reason"] = assignment.reason

    async def _evidence() -> dict:
        if assignment.table_id is None:
            return {}
        reg = (await conn.execute_core(select(rt).where(rt.c.id == assignment.table_id))).fetchone()
        if reg is None:
            return {}
        return await evidence(
            conn,
            None,
            dict(reg._mapping),
            assignment.column_name if assignment.object_type == "column" else None,
        )

    return await stamp_for(
        conn,
        previous,
        given,
        changed=held is None or held._mapping["tag_id"] != assignment.tag_id,
        where=where,
        evidence=_evidence,
    )


async def unassign(
    conn: "Connection", tag_id: str, object_key: str, object_type: str | None = None
) -> bool:
    """Take the tag off the object keyed ``object_key``. ``object_type`` says which table holds
    it (:func:`_held_in`); left out, the model's."""
    # Matched on the base id so removing "the entity tag" from a column succeeds whether the
    # caller names the parameter or not; (base_tag_id, object_key) is unique, so it is exact.
    model_change.name("unassign", "tag", tag_id)  # REQ-1524
    held_in = _held_in(object_type or "")
    result = await conn.execute_core(
        _delete(held_in).where(
            (held_in.c.base_tag_id == base_tag_id(tag_id)) & (held_in.c.object_key == object_key)
        )
    )
    removed = (result.rowcount or 0) > 0
    if removed:  # REQ-2208: its citations go with the assignment
        await citations.uncite_all(conn, citations.TAG_ASSIGNMENT, base_tag_id(tag_id), object_key)
    return removed


# ---------------------------------------------------------------------------
# Citations of the reference library (REQ-2208)
# ---------------------------------------------------------------------------


class CitationTargetNotFound(LookupError):  # REQ-2208
    """The tag, or the assignment, a citation is to be made on or taken off is not there."""


async def _require_tag(conn: "Connection", tag_id: str) -> str:
    if await get(conn, tag_id) is None:
        raise CitationTargetNotFound(f"Tag {tag_id!r} not found")
    return base_tag_id(tag_id)


async def _require_assignment(conn: "Connection", assignment: TagAssignment) -> tuple[str, str]:
    held_in = _held_in(assignment.object_type)
    key = (assignment.base_tag_id(), assignment.object_key())
    found = await conn.execute_core(
        select(held_in.c.id).where(
            (held_in.c.base_tag_id == key[0]) & (held_in.c.object_key == key[1])
        )
    )
    if found.fetchone() is None:
        raise CitationTargetNotFound(f"Tag {assignment.tag_id!r} is not assigned to {key[1]}")
    return key


async def cite_definition(  # REQ-2208
    conn: "Connection", tag_id: str, place: "citations.Place", cited_by: str | None
) -> "citations.Citation":
    """The tag's definition cites ``place`` (the section that defines it); a system tag's too."""
    model_change.name("cite", "tag", tag_id)  # REQ-1524
    base = await _require_tag(conn, tag_id)
    return await citations.cite(conn, citations.TAG, base, "", place, cited_by)


async def uncite_definition(  # REQ-2208
    conn: "Connection", tag_id: str, place: "citations.Place"
) -> bool:
    model_change.name("uncite", "tag", tag_id)  # REQ-1524
    base = await _require_tag(conn, tag_id)
    return await citations.uncite(conn, citations.TAG, base, "", place)


async def cite_assignment(  # REQ-2208
    conn: "Connection", assignment: TagAssignment, place: "citations.Place", cited_by: str | None
) -> "citations.Citation":
    """The assignment cites ``place``: what traces or confirms the tag on that object."""
    model_change.name("cite", "tag", assignment.tag_id)  # REQ-1524
    base, key = await _require_assignment(conn, assignment)
    return await citations.cite(conn, citations.TAG_ASSIGNMENT, base, key, place, cited_by)


async def uncite_assignment(  # REQ-2208
    conn: "Connection", assignment: TagAssignment, place: "citations.Place"
) -> bool:
    model_change.name("uncite", "tag", assignment.tag_id)  # REQ-1524
    base, key = await _require_assignment(conn, assignment)
    return await citations.uncite(conn, citations.TAG_ASSIGNMENT, base, key, place)


async def definition_citations(  # REQ-2208
    conn: "Connection",
) -> "dict[str, list[citations.Citation]]":
    """Each tag's definition citations, by base tag id, each with its state."""
    found = await citations.of_citers(conn, citations.TAG)
    return {tag: cited for (tag, _key), cited in found.items() if tag is not None}


async def assignment_citations(  # REQ-2208
    conn: "Connection",
) -> "dict[tuple[str, str], list[citations.Citation]]":
    """Each assignment's citations, by (base tag id, object key), each with its state."""
    found = await citations.of_citers(conn, citations.TAG_ASSIGNMENT)
    return {(tag, key): cited for (tag, key), cited in found.items() if tag is not None}


def citation_gap(tag_row: dict | None, cited: "list[citations.Citation]") -> bool:  # REQ-2208
    """Whether an assignment of the tag lacks the citation its tag requires: a gap, reported
    and never refused."""
    return bool(tag_row and tag_row.get("requires_citation")) and not cited


# ---------------------------------------------------------------------------
# Parameter values (REQ-1467)
# ---------------------------------------------------------------------------


async def list_param_values(conn: "Connection", tag_id: str) -> list[dict]:
    result = await conn.execute_core(
        select(tag_param_values)
        .where(tag_param_values.c.tag_id == base_tag_id(tag_id))
        .order_by(tag_param_values.c.value)
    )
    return [dict(r._mapping) for r in result.fetchall()]


async def list_all_param_values(conn: "Connection") -> list[dict]:
    result = await conn.execute_core(
        select(tag_param_values).order_by(tag_param_values.c.tag_id, tag_param_values.c.value)
    )
    return [dict(r._mapping) for r in result.fetchall()]


async def upsert_param_value(conn: "Connection", param: TagParamValue) -> None:
    await conn.upsert(
        tag_param_values,
        {
            "tag_id": base_tag_id(param.tag_id),
            "value": param.value,
            "description": param.description,
        },
        index_elements=["tag_id", "value"],
        update_columns=["description"],
    )


async def param_value_assignment_count(conn: "Connection", tag_id: str, value: str) -> int:
    """Assignments carrying exactly this parameter value — the delete guard (REQ-1467)."""
    base = base_tag_id(tag_id)
    count = 0
    for held_in in (tag_assignments, reference_document_tags):  # REQ-2208: the library's too
        result = await conn.execute_core(
            select(held_in.c.id).where(held_in.c.tag_id == f"{base}:{value}")
        )
        count += len(result.fetchall())
    return count


async def delete_param_value(conn: "Connection", tag_id: str, value: str) -> bool:
    result = await conn.execute_core(
        _delete(tag_param_values).where(
            (tag_param_values.c.tag_id == base_tag_id(tag_id)) & (tag_param_values.c.value == value)
        )
    )
    return (result.rowcount or 0) > 0


async def list_assignments(conn: "Connection") -> list[dict]:
    """All assignments, with the qualified table address joined in for table/column targets, the
    term's name for a term (REQ-2145) and the rule's role, target and rights for a row rule
    (REQ-2208).

    ``table_ref`` ("source.schema.table"), ``term`` and ``rls_rule`` are the config-vocabulary
    identities the export builder resolves; DB serials never leave the repository layer.
    """
    rule_table = registered_tables.alias("rule_table")
    result = await conn.execute_core(
        select(
            tag_assignments,
            registered_tables.c.source_id.label("ref_source_id"),
            registered_tables.c.schema_name.label("ref_schema_name"),
            registered_tables.c.table_name.label("ref_table_name"),
            glossary_terms.c.name.label("term"),
            rls_rules.c.role_id.label("rule_role_id"),
            rls_rules.c.domain_id.label("rule_domain_id"),
            rls_rules.c.action_name.label("rule_action_name"),
            rls_rules.c.tag_id.label("rule_tag_id"),
            rls_rules.c.rights.label("rule_rights"),
            func.coalesce(rule_table.c.alias, rule_table.c.table_name).label("rule_table"),
        )
        .join(
            registered_tables,
            tag_assignments.c.table_id == registered_tables.c.id,
            isouter=True,
        )
        .join(glossary_terms, tag_assignments.c.term_id == glossary_terms.c.id, isouter=True)
        .join(rls_rules, tag_assignments.c.rls_rule_id == rls_rules.c.id, isouter=True)
        .join(rule_table, rls_rules.c.table_id == rule_table.c.id, isouter=True)
        .order_by(tag_assignments.c.tag_id, tag_assignments.c.object_key)
    )
    rows = []
    for r in result.fetchall():
        row = dict(r._mapping)
        src = row.pop("ref_source_id", None)
        schema = row.pop("ref_schema_name", None)
        table = row.pop("ref_table_name", None)
        row["table_ref"] = f"{src}.{schema}.{table}" if src is not None else None
        row["rls_rule"] = _rule_ref(row)  # REQ-2208
        row["why"] = why_of(row)  # REQ-2094
        rows.append(row)
    return rows


async def list_library_assignments(conn: "Connection") -> list[dict]:  # REQ-2208
    """The tags on reference library documents and their sections, kept with the library, as
    :func:`list_assignments` answers rows: every other object's column None."""
    result = await conn.execute_core(
        select(reference_document_tags).order_by(
            reference_document_tags.c.tag_id, reference_document_tags.c.object_key
        )
    )
    model = (
        "source_id",
        "table_id",
        "column_name",
        "relationship_id",
        "command_name",
        "product_id",
        "term_id",
        "term",
        "table_ref",
        "rls_rule_id",
        "rls_rule",
        "role_id",
        "metric_name",
    )
    rows = []
    for r in result.fetchall():
        row = {**dict.fromkeys(model), **dict(r._mapping)}
        row["why"] = why_of(row)
        rows.append(row)
    return rows


_RULE_COLUMNS = (
    "rule_role_id",
    "rule_domain_id",
    "rule_action_name",
    "rule_tag_id",
    "rule_rights",
    "rule_table",
)


def _rule_ref(row: dict) -> dict | None:
    """The row rule an assignment row is on, as a configuration names it (models.RowRuleRef), with
    the joined rule columns taken off the row; None when the assignment is not on a rule."""
    rule = {k: row.pop(k, None) for k in _RULE_COLUMNS}
    if row.get("rls_rule_id") is None or rule["rule_role_id"] is None:
        return None
    ref = {
        "role_id": rule["rule_role_id"],
        "table_id": rule["rule_table"],
        "domain_id": rule["rule_domain_id"],
        "action_name": rule["rule_action_name"],
        "tag_id": rule["rule_tag_id"],
        "rights": list(rule["rule_rights"]),
    }
    return {k: v for k, v in ref.items() if v is not None}


async def resolve_rls_rule_id(conn: "Connection", ref: RowRuleRef) -> int | None:  # REQ-2208
    """The serial of the row rule ``ref`` names in THIS schema, or None when there is none: the
    role's rule on the target holding exactly those rights (a right is in at most one of a role's
    rules on a target, REQ-2188)."""
    from provisa.core.repositories import table as table_repo

    if ref.action_name:
        target = rls_rules.c.action_name == ref.action_name
    elif ref.tag_id:
        target = rls_rules.c.tag_id == ref.tag_id
    elif ref.domain_id:
        target = rls_rules.c.domain_id == ref.domain_id
    elif ref.table_id:
        table = await table_repo.find_by_table_name(conn, ref.table_id)
        if table is None:
            return None
        target = rls_rules.c.table_id == table["id"]
    else:
        raise ValueError(f"row rule of {ref.role_id!r} names no target")
    rights = "".join(r for r in ("R", "U", "D") if r in ref.rights)
    row = (
        await conn.execute_core(
            select(rls_rules.c.id).where(
                target, rls_rules.c.role_id == ref.role_id, rls_rules.c.rights == rights
            )
        )
    ).fetchone()
    return row.id if row is not None else None


def why_of(row: dict) -> dict | None:
    """An assignment row's why beyond its ``reason`` (REQ-2094), or None when nothing is
    recorded."""
    decided_at = row.get("decided_at")
    why = {
        "purpose": row.get("purpose"),
        "bases": row.get("bases") or None,
        "review_on": row.get("review_on"),
        "decided_by": row.get("decided_by"),
        "decided_at": decided_at.isoformat() if decided_at else None,
        "evidence": row.get("evidence") or None,
    }
    why = {k: v for k, v in why.items() if v is not None}
    return why or None


async def resolve_table_id(
    conn: "Connection", source_id: str, schema_name: str, table_name: str
) -> int | None:
    result = await conn.execute_core(
        select(registered_tables.c.id).where(
            (registered_tables.c.source_id == source_id)
            & (registered_tables.c.schema_name == schema_name)
            & (registered_tables.c.table_name == table_name)
        )
    )
    row = result.fetchone()
    return row.id if row is not None else None
