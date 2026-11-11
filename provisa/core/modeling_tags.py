# Copyright (c) 2026 Kenneth Stott
# Canary: 7e6c6226-7ecd-42f5-adb3-69f7b2dbe51c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1320: shared formatting for the star/vault modeling-role tag.

Every catalog surface (GraphQL introspection, pg_description, and the Flight/MCP
catalog description) appends the same "[fact]" / "[dimension, scd2]" suffix so
callers see the star shape regardless of which surface they query.

REQ-1415: the suffix also lists the tag ids assigned to the object, so a reader of a catalog
sees ``[dimension, entity:customer, natural_language]`` inline. The suffix is a HUMAN-FACING
MIRROR. It is not the machine contract and nothing derives behavior by parsing it. To keep it
unambiguous without per-surface escaping, a tag id and a tag parameter value are restricted to a
slug charset where they are created (:func:`is_tag_slug`), so neither can hold a comma or bracket.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence

#: What a user tag id and a tag parameter value may be made of (REQ-1415).
TAG_SLUG_CHARSET = "a-z, 0-9, _ and -"

# REQ-2238: the tag name the landing reconcile sets on a column to its grounded term
# (``PROVISA_GOVERNANCE.TERM``). A steward tag of this name would be overwritten by the term value,
# so the name is reserved. The reconcile and the tag definition validation both read it here.
RESERVED_TERM_TAG = "term"


def is_reserved_tag_name(name: str) -> bool:
    """Whether ``name`` is the reserved term tag; compared case-insensitively, as Snowflake
    compares tag names."""
    return name.casefold() == RESERVED_TERM_TAG


_TAG_SLUG = re.compile(r"^[a-z0-9_-]+$")

TagsByTable = dict[int, list[str]]
TagsByColumn = dict[tuple[int, str], list[str]]


def is_tag_slug(value: str) -> bool:
    """Whether ``value`` can be a tag id or a tag parameter value (REQ-1415)."""
    return bool(_TAG_SLUG.match(value))


def append_modeling_tag(
    description: str | None,
    role: str | None,
    history: str | None,
    tags: Sequence[str] = (),
) -> str:
    """Append the bracketed modeling-role and tag suffix to a description.

    Returns the suffix alone (no leading space) when description is empty/None, and the
    description unchanged (or "") when there is neither a role nor a tag.
    """
    parts: list[str] = []
    if role:
        parts.append(role)
        if history:
            parts.append(history)
    parts.extend(tags)
    if not parts:
        return description or ""
    suffix = f"[{', '.join(parts)}]"
    return f"{description} {suffix}" if description else suffix


def column_comment(description: str | None, role: str | None, tags: Sequence[str]) -> str:
    """A column's description with its suffix (REQ-1415): the table's modeling role and the
    column's own tags. A column with no tag keeps its description as written."""
    if not tags:
        return description or ""
    return append_modeling_tag(description, role, None, tags)


def tags_by_object(
    assignments: Iterable[tuple[str, str, int | None, str | None]],
) -> tuple[TagsByTable, TagsByColumn]:
    """Index tag assignments, each ``(tag_id, object_type, table_id, column_name)``, by the table
    or column they are on. Ids are as assigned (a parameterized one reads ``entity:customer``)
    and sorted, so every surface lists them in one order. Assignments on other object types
    (source, relationship, command, product) have no catalog description to ride on."""
    by_table: TagsByTable = {}
    by_column: TagsByColumn = {}
    for tag_id, object_type, table_id, column_name in assignments:
        if table_id is None:
            continue
        if object_type == "table":
            by_table.setdefault(table_id, []).append(tag_id)
        elif object_type == "column" and column_name:
            by_column.setdefault((table_id, column_name), []).append(tag_id)
    for ids in (*by_table.values(), *by_column.values()):
        ids.sort()
    return by_table, by_column


def attach_tags(
    tables: Iterable[dict], assignments: Iterable[tuple[str, str, int | None, str | None]]
) -> None:
    """Put each registered table row's and column row's tag ids on it as ``tags`` (REQ-1415),
    which is what the GraphQL and pgwire catalog builders read."""
    by_table, by_column = tags_by_object(assignments)
    for table in tables:
        table["tags"] = by_table.get(table["id"], [])
        for column in table.get("columns") or []:
            column["tags"] = by_column.get((table["id"], column.get("column_name")), [])
