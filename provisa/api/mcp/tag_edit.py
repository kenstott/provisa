# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The tag edits of the tag assignment form and the tag editor, as agent tools.

An object is named as the tags report and read_screen name it: its kind and key
(``column:12:email``, ``section:7:heading:Retention``). Each edit is the admin mutation the forms
send -- assignTag, unassignTag, citeTagAssignment, unciteTagAssignment, citeTagDefinition,
unciteTagDefinition -- run as the caller (:func:`run`, over MCP, the way
:func:`provisa.api.mcp.table_edit.save_table` runs updateTable), or handed to the person's browser
as that same mutation for them to confirm (:func:`as_admin_mutation`, in Polly's chat). Either way
the mutation's own checks decide, and its own result comes back.
"""

# Requirements: REQ-2208

from __future__ import annotations

import types
from typing import Any

# The schema's own converter: the GraphQL argument and input field names strawberry publishes.
from strawberry.utils.str_converters import to_camel_case

# Each kind's key after ``<kind>:``: the input fields it names, in order, and how each is read.
# The last field takes the rest of the key, so a column name or a source id may hold a colon.
_KEYS: dict[str, tuple[tuple[str, type], ...]] = {
    "source": (("source_id", str),),
    "table": (("table_id", int),),
    "column": (("table_id", int), ("column_name", str)),
    "relationship": (("relationship_id", str),),
    "command": (("command_name", str),),
    "product": (("product_id", str),),
    "term": (("term_id", int),),
    "rls_rule": (("rls_rule_id", int),),
    "role": (("role_id", str),),
    "metric": (("metric_name", str),),
    "document": (("document_id", int),),
}

KINDS: tuple[str, ...] = (*_KEYS, "section")

# The tools, each with the mutation it is and what that mutation takes.
ASSIGNMENT_TOOLS = ("assign_tag", "unassign_tag", "cite_tag_assignment", "uncite_tag_assignment")
DEFINITION_TOOLS = ("cite_tag_definition", "uncite_tag_definition")
TOOLS = (*ASSIGNMENT_TOOLS, *DEFINITION_TOOLS)

_RESULT = "{ success message code params }"
_DOCUMENTS = {
    "assign_tag": (
        "mutation AssignTag($input: TagAssignmentInput!, $citations: [LibraryCitationInput!]) "
        f"{{ assignTag(input: $input, citations: $citations) {_RESULT} }}"
    ),
    "unassign_tag": (
        f"mutation UnassignTag($input: TagAssignmentInput!) {{ unassignTag(input: $input) {_RESULT} }}"
    ),
    "cite_tag_assignment": (
        "mutation CiteTagAssignment($assignment: TagAssignmentInput!, "
        "$citation: LibraryCitationInput!) "
        f"{{ citeTagAssignment(assignment: $assignment, citation: $citation) {_RESULT} }}"
    ),
    "uncite_tag_assignment": (
        "mutation UnciteTagAssignment($assignment: TagAssignmentInput!, "
        "$citation: LibraryCitationInput!) "
        f"{{ unciteTagAssignment(assignment: $assignment, citation: $citation) {_RESULT} }}"
    ),
    "cite_tag_definition": (
        "mutation CiteTagDefinition($tagId: String!, $citation: LibraryCitationInput!) "
        f"{{ citeTagDefinition(tagId: $tagId, citation: $citation) {_RESULT} }}"
    ),
    "uncite_tag_definition": (
        "mutation UnciteTagDefinition($tagId: String!, $citation: LibraryCitationInput!) "
        f"{{ unciteTagDefinition(tagId: $tagId, citation: $citation) {_RESULT} }}"
    ),
}


def _refused(kind: str, key: str) -> ValueError:
    return ValueError(
        f"object_key {key!r} names no {kind}: give it as read_screen's tags report it, "
        f"<kind>:<id> (column:<table id>:<column>, section:<document id>:heading:<heading> or "
        f"section:<document id>:page:<page>)"
    )


def target(kind: str, key: str) -> dict[str, Any]:
    """The TagAssignmentInput fields that name the object ``key`` of ``kind``."""
    prefix = f"{kind}:"
    if kind not in KINDS or not key.startswith(prefix):
        raise _refused(kind, key)
    rest = key[len(prefix) :]
    if kind == "section":
        document, _, place = rest.partition(":")
        how, _, value = place.partition(":")
        if not document.isdigit() or how not in ("heading", "page") or not value:
            raise _refused(kind, key)
        if how == "page" and not value.isdigit():
            raise _refused(kind, key)
        named: dict[str, Any] = {"document_id": int(document)}
        named["section_heading" if how == "heading" else "section_page"] = (
            value if how == "heading" else int(value)
        )
        return named
    fields = _KEYS[kind]
    parts = rest.split(":", len(fields) - 1)
    if len(parts) != len(fields) or not all(parts):
        raise _refused(kind, key)
    named = {}
    for (field, kind_of), part in zip(fields, parts, strict=True):
        if kind_of is int and not part.isdigit():
            raise _refused(kind, key)
        named[field] = kind_of(part)
    return named


def _assignment(
    kind: str, key: str, tag_id: str, reason: str | None = None, expires_on: str | None = None
) -> dict[str, Any]:
    """The TagAssignmentInput's fields, snake_case, the unset ones left out."""
    fields: dict[str, Any] = {"tag_id": tag_id, "object_type": kind, **target(kind, key)}
    if reason is not None:
        fields["reason"] = reason
    if expires_on is not None:
        fields["expires_on"] = expires_on
    return fields


_CITATION_FIELDS = ("document_id", "section_heading", "section_page")


def _citation(given: dict) -> dict[str, Any]:
    unknown = set(given) - set(_CITATION_FIELDS)
    if unknown or "document_id" not in given:
        raise ValueError(
            f"a citation is {{document_id, section_heading?, section_page?}}; got {sorted(given)}"
        )
    return {f: given[f] for f in _CITATION_FIELDS if given.get(f) is not None}


def _camel_keys(fields: dict[str, Any]) -> dict[str, Any]:
    return {to_camel_case(k): v for k, v in fields.items()}


class _Edit(types.SimpleNamespace):
    """One tool call read: the mutation's arguments, snake_case, and what it does in a sentence."""


def _read(name: str, args: dict) -> _Edit:
    if name not in TOOLS:
        raise ValueError(f"{name!r} is not a tag edit")
    citation = _citation(args["citation"]) if "citation" in args else None
    if name in DEFINITION_TOOLS:
        assert citation is not None
        verb = "cites" if name == "cite_tag_definition" else "no longer cites"
        return _Edit(
            arguments={"tag_id": args["tag_id"], "citation": citation},
            summary=f"Tag {args['tag_id']}'s definition {verb} {_place(citation)}.",
        )
    kind, key, tag_id = args["object_kind"], args["object_key"], args["tag_id"]
    if name == "assign_tag":
        assignment = _assignment(kind, key, tag_id, args.get("reason"), args.get("expires_on"))
        arguments: dict[str, Any] = {"input": assignment}
        if args.get("citations") is not None:
            arguments["citations"] = [_citation(c) for c in args["citations"]]
        return _Edit(arguments=arguments, summary=f"Put tag {tag_id} on {key}.")
    assignment = _assignment(kind, key, tag_id)
    if name == "unassign_tag":
        return _Edit(arguments={"input": assignment}, summary=f"Take tag {tag_id} off {key}.")
    assert citation is not None
    verb = "cites" if name == "cite_tag_assignment" else "no longer cites"
    return _Edit(
        arguments={"assignment": assignment, "citation": citation},
        summary=f"Tag {tag_id} on {key} {verb} {_place(citation)}.",
    )


def _place(citation: dict[str, Any]) -> str:
    document = f"library document {citation['document_id']}"
    if "section_heading" in citation:
        return f"{document}, section {citation['section_heading']!r}"
    if "section_page" in citation:
        return f"{document}, page {citation['section_page']}"
    return document


def as_admin_mutation(name: str, args: dict) -> dict[str, Any]:
    """The admin_mutation the edit is: the form's own mutation, its variables and a summary the
    person reads before they confirm it."""
    edit = _read(name, args)
    variables: dict[str, Any] = {}
    for arg, value in edit.arguments.items():
        if isinstance(value, list):
            variables[to_camel_case(arg)] = [_camel_keys(v) for v in value]
        elif isinstance(value, dict):
            variables[to_camel_case(arg)] = _camel_keys(value)
        else:
            variables[to_camel_case(arg)] = value
    return {"mutation": _DOCUMENTS[name], "variables": variables, "summary": edit.summary}


async def run(request: Any, name: str, args: dict) -> dict[str, Any]:
    """Run the edit as the caller through the admin mutation; its result, unchanged."""
    from provisa.api.admin.schema_mutation import Mutation
    from provisa.api.admin.types import LibraryCitationInput, TagAssignmentInput

    edit = _read(name, args)
    built: dict[str, Any] = {}
    for arg, value in edit.arguments.items():
        if arg in ("input", "assignment"):
            built[arg] = TagAssignmentInput(**value)
        elif arg == "citation":
            built[arg] = LibraryCitationInput(**value)
        elif arg == "citations":
            built[arg] = [LibraryCitationInput(**c) for c in value]
        else:
            built[arg] = value
    info = types.SimpleNamespace(context={"request": request})
    # pyright mistypes strawberry.mutation-decorated methods' call signature (see table_edit).
    mutation = getattr(Mutation(), name)
    result = await mutation(info, **built)
    return {
        "success": result.success,
        "message": result.message,
        "code": result.code,
        "params": result.params,
    }
