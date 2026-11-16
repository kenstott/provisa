# Copyright (c) 2026 Kenneth Stott
# Canary: 3c1f0e7b-8d2a-4b59-9e64-71a5c2d8f0b3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What one discovery request sends: per table its context, per compared column its context, and
the instructions around them (REQ-413).

A table's context is its name, description, tags, role of fact or dimension, and the names of all
its columns, those not compared included. A compared column's context is its name, description,
classes, tags and sample values, never its type; a column with a sensitive class sends its class
and no sample values. The prompt states that a shared pii tag alone is weak evidence.

A table that requires a parameter is never read (REQ-1986): its context names its required
parameters with their types and says its values were not read, and its columns send the class
``not_read`` and no sample values.
"""

# Requirements: REQ-413, REQ-1986

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

from provisa.discovery.columns import NOT_READ, ColumnFacts, TableFacts


def table_context(table: TableFacts) -> dict[str, Any]:
    """A table's context. ``id`` is how an answer names the table."""
    out: dict[str, Any] = {
        "id": table.table_id,
        "name": table.table_name,
        "description": table.description,
        "tags": list(table.tags),
        "role": table.role,
        "columns": [c.name for c in table.columns],
    }
    if table.reduced:
        out["required_parameters"] = [
            {"column": c.name, "parameter": c.required_parameter, "type": c.data_type}
            for c in table.columns
            if c.required_parameter is not None
        ]
        out["values"] = NOT_READ
    return out


def column_context(column: ColumnFacts) -> dict[str, Any]:
    """A compared column's context: a sensitive column's class stands in for its values."""
    out: dict[str, Any] = {
        "name": column.name,
        "description": column.description,
        "classes": list(column.classes),
        "tags": list(column.tags),
    }
    if not column.sensitive and NOT_READ not in column.classes:
        out["sample_values"] = list(column.samples)
    return out


def _dump(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def block(table: TableFacts, columns: Sequence[ColumnFacts]) -> dict[str, Any]:
    """One table of a window: its context and the contexts of its columns compared there."""
    return {"table": table_context(table), "compared_columns": [column_context(c) for c in columns]}


def render_window(blocks: Sequence[dict[str, Any]]) -> str:
    """A window as the request sends it."""
    return _dump(list(blocks))


def block_weight(table: TableFacts, columns: Sequence[ColumnFacts]) -> int:
    """A table's weight in a window: the length in characters of what is sent for it."""
    return len(_dump(block(table, columns)))


def window_weight(block_weights: Sequence[int]) -> int:
    """The length of a rendered window holding blocks of these weights: brackets and commas."""
    return sum(block_weights) + 2 + max(0, len(block_weights) - 1)


_SHAPE = (
    "{"
    '"source_table_id":<int>,"source_column":"<name>",'
    '"target_table_id":<int>,"target_column":"<name>",'
    '"cardinality":"many-to-one"|"one-to-many",'
    '"confidence":<0.0-1.0>,"reasoning":"<brief>","suggested_name":"<kebab-case business concept>"'
    "}"
)


_NOT_READ = (
    "A table with required_parameters is read only when each is given a value, so none of "
    "its values was read: its columns have the class 'not_read' and no sample values, and "
    "are judged on their names, descriptions and tags alone. A required parameter is a "
    "value a caller passes to read the table, and often names another table's key.\n"
)


def instructions(
    group: str, at_most: int, *, either_direction: bool, not_read: bool = False
) -> str:
    """The text around the windows. ``either_direction``: the two windows are drawn from one set
    of tables, so a relationship may start in either. ``not_read``: a table of the windows
    requires a parameter and was never read, and the text says how to judge it."""
    if either_direction:
        direction = (
            "Source and target are the same set of tables: a relationship may start in either "
            "window and end in either, two different columns of one table included."
        )
    else:
        direction = (
            "A relationship starts at a column of the SOURCE window (the referring column) and "
            "ends at a column of the TARGET window."
        )
    return (
        "You are a database relationship analyst. Find columns of the SOURCE window that refer "
        "to columns of the TARGET window, as a foreign key refers to the key it names: "
        "many-to-one, the referring column the many side.\n"
        f"Every compared column here is of the type group '{group}'. Each table lists the "
        "names of all its columns; only its compared_columns may be proposed. A column's classes "
        "say what its values were recognised as; 'unknown' is a column none was recognised for. "
        "A column with a sensitive class shows its class and no sample values.\n"
        f"{_NOT_READ if not_read else ''}"
        "A shared pii tag alone is weak evidence of a relationship.\n"
        f"{direction}\n"
        f"Propose at most {at_most} relationships, the strongest first.\n"
        "Return ONLY a JSON array, no other text, each element:\n"
        f"{_SHAPE}\n"
        "suggested_name names the business concept the join means, not its tables or columns.\n"
    )


def prompt(
    group: str,
    at_most: int,
    source: Sequence[dict[str, Any]],
    target: Sequence[dict[str, Any]] | None,
    *,
    either_direction: bool,
) -> str:
    """One request: the instructions, the source window and the target window. ``target`` None:
    the request compares one window with itself. ``either_direction``: source and target are one
    set of tables (the target is the same as the source)."""
    not_read = any(b["table"].get("values") == NOT_READ for b in [*source, *(target or [])])
    parts = [instructions(group, at_most, either_direction=either_direction, not_read=not_read)]
    if target is None:
        parts.append("## SOURCE and TARGET window\n" + render_window(source))
    else:
        parts.append("## SOURCE window\n" + render_window(source))
        parts.append("## TARGET window\n" + render_window(target))
    return "\n".join(parts)
