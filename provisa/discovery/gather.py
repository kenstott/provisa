# Copyright (c) 2026 Kenneth Stott
# Canary: 9a2f61c8-4e0d-4b7a-a3c5-2d8e7f1b6c94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a discovery run reads before it asks anything (REQ-413): the tables of its source and
target sets, each with its context, and each column with its type groups, classes, tags and
sample values.

A column's classes come from the table's latest profile run and the steward's decisions on it
(REQ-2147); a table never profiled is classified during the run with the profiler's
classification sample (:func:`provisa.profiler.run.classify_table`), and those classes are used
for the run and not recorded. System domains are never included.
"""

# Requirements: REQ-413, REQ-2147

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.compiler.naming import source_to_catalog
from provisa.discovery.collector import _validate_ident
from provisa.discovery.columns import (
    ColumnFacts,
    TableFacts,
    classes_of,
    type_groups,
)
from provisa.discovery.direction import Edge, Known, Sampled
from provisa.federation.execution_auth import system_auth

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: REQ-2147 (the new profiler's default, provisa-ui profilerMapping classification_sample): the
#: non-null values per column a never-profiled table is classified from.
VALUES_PER_COLUMN = 500
#: The profiler's default sample budget (a profiler source's sample_above_cells, absent by
#: default: the whole table is read), for a never-profiled table's classification.
SAMPLE_ABOVE_CELLS: int | None = None
#: Rows read for a column's sample values, and the distinct values sent of them: what discovery
#: has read and sent per table since REQ-018 (collect_metadata samples 20 rows, the prompt sent 5).
CONTEXT_SAMPLE_ROWS = 20
CONTEXT_SAMPLE_VALUES = 5
#: The bounded sample of a column read for repeated values when only it can choose a direction.
UNIQUENESS_SAMPLE_ROWS = 500


def _system_domains() -> frozenset[str]:
    from provisa.security.rights import SYSTEM_DOMAINS

    return SYSTEM_DOMAINS


async def scope_table_ids(
    conn: "Connection", domains: Sequence[str], table_ids: Sequence[int] | None
) -> list[int]:
    """The tables of one set: those named, else every table of the chosen domains; never a table
    of a system domain (meta, ontology, ops) whatever was asked, nor a draft table (REQ-1921: a
    draft is read nowhere)."""
    from provisa.core.schema_org import registered_tables as rt

    system = _system_domains()
    query = select(rt.c.id).where(rt.c.draft.is_(False), rt.c.domain_id.notin_(sorted(system)))
    if table_ids is not None:
        query = query.where(rt.c.id.in_(sorted(set(table_ids))))
    else:
        query = query.where(rt.c.domain_id.in_(sorted(set(domains) - system)))
    rows = (await conn.execute_core(query.order_by(rt.c.id))).fetchall()
    return [int(r[0]) for r in rows]


async def _table_tags(conn: "Connection", ids: list[int]) -> dict[int, list[str]]:
    from provisa.core.schema_org import tag_assignments as ta

    rows = (
        await conn.execute_core(
            select(ta.c.table_id, ta.c.tag_id).where(
                ta.c.object_type == "table", ta.c.table_id.in_(ids)
            )
        )
    ).fetchall()
    out: dict[int, list[str]] = {}
    for r in rows:
        out.setdefault(int(r.table_id), []).append(str(r.tag_id))
    return out


async def _decided(conn: "Connection", ids: list[int]) -> dict[tuple[int, str], set[str]]:
    """The steward's accepted classes, by (table id, registered column name)."""
    from provisa.core.schema_org import profiler_classifications as pc

    rows = (
        await conn.execute_core(
            select(pc.c.table_id, pc.c.physical_column, pc.c.kind).where(
                pc.c.table_id.in_(ids), pc.c.status == "accepted"
            )
        )
    ).fetchall()
    out: dict[tuple[int, str], set[str]] = {}
    for r in rows:
        out.setdefault((int(r.table_id), str(r.physical_column)), set()).add(str(r.kind))
    return out


async def _profiled_classes(
    conn: "Connection", table_id: int, table_name: str
) -> dict[str, set[str]] | None:
    """The kinds the table's latest measured profile run matched, by registered column name;
    None when the table has never been profiled."""
    from provisa.profiler.declared import measured
    from provisa.profiler.history import relation

    runs = await relation(conn, table_name, table_id, "runs")
    latest = (
        await conn.execute_core(
            select(runs.c.run_id)
            .where(runs.c.status == "succeeded", measured(runs))
            .order_by(runs.c.run_time.desc())
            .limit(1)
        )
    ).fetchone()
    if latest is None:
        return None
    found = await relation(conn, table_name, table_id, "classifications")
    rows = (
        await conn.execute_core(
            select(found.c.physical_column, found.c.kind).where(found.c.run_id == latest[0])
        )
    ).fetchall()
    out: dict[str, set[str]] = {}
    for r in rows:
        out.setdefault(str(r.physical_column), set()).add(str(r.kind))
    return out


async def _classified_now(state: Any, table_id: int, table_name: str) -> dict[str, set[str]]:
    """A never-profiled table classified from the profiler's classification sample, recording
    nothing (REQ-2147)."""
    from provisa.profiler.run import classify_table

    judged = await classify_table(
        state,
        table_id=table_id,
        table_name=table_name,
        values_per_column=VALUES_PER_COLUMN,
        sample_above_cells=SAMPLE_ABOVE_CELLS,
    )
    return {j.physical: {m.kind.id for m in j.matches} for j in judged}


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _address(table: dict) -> str:
    catalog = _validate_ident(source_to_catalog(table["source_id"]))
    return (
        f"{catalog}.{_validate_ident(table['schema_name'])}.{_validate_ident(table['table_name'])}"
    )


async def _engine_rows(engine: Any, sql: str) -> list:
    result = await engine.execute_engine(
        engine.address_replicas(sql), authorization=system_auth("discovery")
    )
    return list(result.rows)


async def _samples(engine: Any, table: dict, columns: list[str]) -> dict[str, tuple[str, ...]]:
    """Up to :data:`CONTEXT_SAMPLE_VALUES` distinct non-null values of each column, from the
    first :data:`CONTEXT_SAMPLE_ROWS` rows read."""
    if not columns:
        return {}
    cols = ", ".join(_quote(c) for c in columns)
    rows = await _engine_rows(
        engine, f"SELECT {cols} FROM {_address(table)} LIMIT {CONTEXT_SAMPLE_ROWS}"
    )
    out: dict[str, list[str]] = {c: [] for c in columns}
    for row in rows:
        for name, value in zip(columns, row, strict=True):
            text = None if value is None else str(value)
            held = out[name]
            if text is not None and text not in held and len(held) < CONTEXT_SAMPLE_VALUES:
                held.append(text)
    return {c: tuple(v) for c, v in out.items()}


def _sensitive(classes: tuple[str, ...]) -> bool:
    from provisa.profiler.classify import kind_of

    return any(kind_of(c).sensitive for c in classes if c != "unknown")


async def table_facts(
    state: Any,
    conn: "Connection",
    ids: Sequence[int],
    on_table: Callable[[int, int], Awaitable[None]] | None = None,
) -> list[TableFacts]:
    """Every table of ``ids`` with its context and its columns' facts; ``on_table(read, total)``
    is told after each table is read."""
    from provisa.core.schema_org import registered_tables as rt
    from provisa.core.schema_org import table_columns as tc
    from provisa.core.term_tags import effective_column_tags

    id_list = sorted(set(ids))
    if not id_list:
        return []
    tables = [
        dict(r._mapping)
        for r in (
            await conn.execute_core(
                select(
                    rt.c.id,
                    rt.c.source_id,
                    rt.c.domain_id,
                    rt.c.schema_name,
                    rt.c.table_name,
                    rt.c.description,
                    rt.c.modeling_role,
                ).where(rt.c.id.in_(id_list))
            )
        ).fetchall()
    ]
    columns: dict[int, list[dict]] = {}
    for r in (
        await conn.execute_core(
            select(
                tc.c.table_id,
                tc.c.column_name,
                tc.c.description,
                tc.c.data_type,
                tc.c.subtype,
                tc.c.array_depth,
                tc.c.is_primary_key,
            )
            .where(tc.c.table_id.in_(id_list))
            .order_by(tc.c.table_id, tc.c.id)
        )
    ).fetchall():
        columns.setdefault(int(r.table_id), []).append(dict(r._mapping))
    table_tags = await _table_tags(conn, id_list)
    column_tags = await effective_column_tags(conn, id_list)
    decided = await _decided(conn, id_list)

    out: list[TableFacts] = []
    for t in tables:
        tid = int(t["id"])
        matched = await _profiled_classes(conn, tid, t["table_name"])
        if matched is None:
            matched = await _classified_now(state, tid, t["table_name"])
        prepared = []
        for c in columns.get(tid, []):
            name = str(c["column_name"])
            groups = type_groups(
                str(c["data_type"] or ""), subtype=c["subtype"], array_depth=int(c["array_depth"])
            )
            classes = classes_of(matched.get(name, set()), decided.get((tid, name), set()))
            prepared.append((c, name, groups, classes, _sensitive(classes)))
        sampled = [name for _, name, groups, _, sensitive in prepared if groups and not sensitive]
        samples = await _samples(state.federation_engine, t, sampled)
        facts = tuple(
            ColumnFacts(
                name=name,
                data_type=str(c["data_type"] or ""),
                description=c["description"],
                tags=tuple(sorted({tag["tag_id"] for tag in column_tags.get((tid, name), [])})),
                classes=classes,
                sensitive=sensitive,
                samples=samples.get(name, ()),
                is_primary_key=bool(c["is_primary_key"]),
                groups=groups,
            )
            for c, name, groups, classes, sensitive in prepared
        )
        out.append(
            TableFacts(
                table_id=tid,
                domain_id=str(t["domain_id"]),
                schema_name=str(t["schema_name"]),
                table_name=str(t["table_name"]),
                description=t["description"],
                tags=tuple(sorted(table_tags.get(tid, []))),
                role=t["modeling_role"],
                columns=facts,
            )
        )
        if on_table is not None:
            await on_table(len(out), len(tables))
    return out


async def known(conn: "Connection") -> Known:
    """Existing relationships, pending and rejected suggestions."""
    from provisa.core.schema_org import relationship_candidates as rc
    from provisa.core.schema_org import relationships as rel

    def edge(r: Any) -> Edge:
        return (int(r[0]), str(r[1]), int(r[2]), str(r[3]))

    existing = (
        await conn.execute_core(
            select(
                rel.c.source_table_id,
                rel.c.source_column,
                rel.c.target_table_id,
                rel.c.target_column,
            )
        )
    ).fetchall()
    candidates = (
        await conn.execute_core(
            select(
                rc.c.source_table_id,
                rc.c.source_column,
                rc.c.target_table_id,
                rc.c.target_column,
                rc.c.status,
            )
        )
    ).fetchall()
    return Known(
        existing={edge(r) for r in existing if r[2] is not None and r[3] is not None},
        pending={edge(r) for r in candidates if r[4] == "suggested"},
        rejected={edge(r) for r in candidates if r[4] == "rejected"},
        relationships_ending={int(r[2]) for r in existing if r[2] is not None},
    )


def uniqueness_reader(state: Any, rows: dict[int, dict]):
    """A reader of a column's bounded sample for repeated values (REQ-413), ``rows`` the
    registered rows of the run's tables by id."""

    async def read(table: TableFacts, column: str) -> Sampled:
        col = _quote(column)
        values = await _engine_rows(
            state.federation_engine,
            f"SELECT {col} FROM {_address(rows[table.table_id])} WHERE {col} IS NOT NULL "
            f"LIMIT {UNIQUENESS_SAMPLE_ROWS}",
        )
        seen = [r[0] for r in values]
        return Sampled(rows=len(seen), repeats=len(set(seen)) != len(seen))

    return read


async def registered_rows(conn: "Connection", ids: Sequence[int]) -> dict[int, dict]:
    from provisa.core.schema_org import registered_tables as rt

    rows = (
        await conn.execute_core(
            select(
                rt.c.id, rt.c.source_id, rt.c.domain_id, rt.c.schema_name, rt.c.table_name
            ).where(rt.c.id.in_(sorted(set(ids))))
        )
    ).fetchall()
    return {int(r.id): dict(r._mapping) for r in rows}
