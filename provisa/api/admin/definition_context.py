# Copyright (c) 2026 Kenneth Stott
# Canary: 7d1c4e92-3a6b-4f58-9e21-b5c8a0f3d617
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What the definition assistant is told of a term's bound columns (REQ-1387, amended 2026-11-18):
each column's description and type, its table's name or alias and description, and a few distinct
sample values read as the curator through the governed pipeline.

The values are read as relationship discovery reads a role's values
(:func:`provisa.discovery.gather._published_as`, :func:`provisa.discovery.gather._governed_as`):
the curator's rules, masks and row rules apply, and a column or table the curator cannot read has
none. A column whose classes are sensitive (discovery's rule: the steward's decided class, else
the latest profile's matches, else the table classified now) has none, and a table that requires a
parameter (REQ-1986) is never read.
"""

# Requirements: REQ-1387, REQ-2253

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.discovery.columns import classes_of
from provisa.discovery.context import SAMPLE_VALUES, sample_text
from provisa.discovery.gather import (
    _classified_now,
    _decided,
    _governed_as,
    _profiled_classes,
    _published_as,
    _quote,
    _reading,
    _sensitive,
    tables_never_read,
)

if TYPE_CHECKING:
    from provisa.core.database import Connection


@dataclass(frozen=True)
class BoundColumn:
    """One bound column as the definition assistant is told of it."""

    table: str
    table_description: str | None
    column: str
    description: str | None
    data_type: str
    samples: tuple[str, ...]


async def column_samples(
    state: Any,
    conn: "Connection",
    role: str,
    table_id: int,
    table_name: str,
    columns: Sequence[str],
    per_column: int = SAMPLE_VALUES,
) -> dict[str, tuple[str, ...]]:
    """Up to ``per_column`` distinct non-null values of each of ``columns`` (registered names) of
    one table, each read through the governed pipeline as ``role``. A sensitive column, a column
    ``role`` cannot read, and every column of a table ``role`` cannot read or that requires a
    parameter have none."""
    if not columns or table_id in await tables_never_read(state, conn, [table_id]):
        return {}
    published = _published_as(state, role, table_id)
    if published is None:
        return {}
    relation, exposed = published
    matched = await _profiled_classes(conn, table_id, table_name)
    if matched is None:
        matched = await _classified_now(state, table_id, table_name)
    decided = await _decided(conn, [table_id])
    out: dict[str, tuple[str, ...]] = {}
    for name in columns:
        if name not in exposed:
            continue
        if _sensitive(classes_of(matched.get(name, set()), decided.get((table_id, name), set()))):
            continue
        col = _quote(exposed[name])
        rows = await _reading(
            table_name,
            _governed_as(
                f"SELECT DISTINCT {col} FROM {relation} WHERE {col} IS NOT NULL LIMIT {per_column}",
                role,
            ),
        )
        held: list[str] = []
        for (value,) in rows:
            text = sample_text(value)
            if text not in held:
                held.append(text)
        out[name] = tuple(held)
    return out


async def bound_columns(
    state: Any, conn: "Connection", role: str, refs: Sequence[dict]
) -> list[BoundColumn]:
    """Each of ``refs`` (a term's refs already narrowed to the selected domains) with its
    registered description and type, its table's and the values ``role`` reads of it."""
    from provisa.core.schema_org import registered_tables as rt
    from provisa.core.schema_org import table_columns as tc

    table_ids = sorted({int(r["table_id"]) for r in refs})
    if not table_ids:
        return []
    tables = {
        int(r.id): r
        for r in (
            await conn.execute_core(
                select(rt.c.id, rt.c.table_name, rt.c.alias, rt.c.description).where(
                    rt.c.id.in_(table_ids)
                )
            )
        ).fetchall()
    }
    columns = {
        (int(r.table_id), str(r.column_name)): r
        for r in (
            await conn.execute_core(
                select(tc.c.table_id, tc.c.column_name, tc.c.description, tc.c.data_type).where(
                    tc.c.table_id.in_(table_ids)
                )
            )
        ).fetchall()
    }
    wanted: dict[int, list[str]] = {}
    for r in refs:
        wanted.setdefault(int(r["table_id"]), []).append(str(r["column_name"]))
    samples: dict[int, dict[str, tuple[str, ...]]] = {}
    for tid, names in wanted.items():
        samples[tid] = await column_samples(
            state, conn, role, tid, str(tables[tid].table_name), names
        )
    out: list[BoundColumn] = []
    for r in refs:
        tid, name = int(r["table_id"]), str(r["column_name"])
        table = tables[tid]
        column = columns[(tid, name)]
        out.append(
            BoundColumn(
                table=str(table.alias or table.table_name),
                table_description=table.description or None,
                column=name,
                description=column.description or None,
                data_type=str(column.data_type),
                samples=samples[tid].get(name, ()),
            )
        )
    return out


async def term_definitions(conn: "Connection", term_ids: Sequence[int]) -> dict[int, str]:
    """The definitions of those of ``term_ids`` that have one."""
    from provisa.core.schema_org import glossary_terms as gt

    if not term_ids:
        return {}
    rows = (
        await conn.execute_core(
            select(gt.c.id, gt.c.definition).where(gt.c.id.in_(sorted(set(term_ids))))
        )
    ).fetchall()
    return {int(r.id): str(r.definition) for r in rows if r.definition and r.definition.strip()}
