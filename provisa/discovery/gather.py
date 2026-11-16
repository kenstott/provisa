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

A table's values are read as the profiler reads them: through the governed pipeline as the org
admin, by the name the table is published under, so a view, an API table and a file are read the
one way. A table that cannot be read stops the run, named (:class:`TableUnreadable`).

A table that requires a parameter (REQ-1986) cannot give a row count and cannot be profiled. It
is known from its registration (:func:`provisa.core.parameters.required_parameters` over its
registered columns), before any read, and is never read: no profile is looked up for it, it is
not classified and not sampled. It takes part on what is registered of it: its names and
descriptions, its columns and their types, its tags, and its required parameters and their types.
A parameterized table kept for good (REQ-2031) is the exception: its rows are stored, the asked
value an ordinary stored column (REQ-1986), and a read of it is answered from them whole. The
model says so (:func:`provisa.api.data.materialization.frozen_parameter_filters`, the pipeline's
own rule), and discovery reads it as it reads any stored table.

:func:`registered_facts` is what the model store alone says of the tables, nothing read: the
request count before Run is computed from it. :func:`table_facts` adds what a run reads.
"""

# Requirements: REQ-413, REQ-1986, REQ-2147

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import replace
from typing import TYPE_CHECKING, Any, TypeVar

from sqlalchemy import select

from provisa.core.parameters import is_parameter, is_required, parameter_name, required_parameters
from provisa.discovery.columns import (
    NOT_READ,
    UNKNOWN,
    ColumnFacts,
    TableFacts,
    classes_of,
    type_groups,
)
from provisa.discovery.context import SAMPLE_VALUES, sample_text
from provisa.discovery.direction import Edge, Known, Sampled

if TYPE_CHECKING:
    from provisa.core.database import Connection

_T = TypeVar("_T")

#: REQ-2147 (the new profiler's default, provisa-ui profilerMapping classification_sample): the
#: non-null values per column a never-profiled table is classified from.
VALUES_PER_COLUMN = 500
#: The profiler's default sample budget (a profiler source's sample_above_cells, absent by
#: default: the whole table is read), for a never-profiled table's classification.
SAMPLE_ABOVE_CELLS: int | None = None
#: Rows read for a column's sample values, and the distinct values sent of them: what discovery
#: has read and sent per table since REQ-018 (collect_metadata samples 20 rows, the prompt sent 5).
CONTEXT_SAMPLE_ROWS = 20
CONTEXT_SAMPLE_VALUES = SAMPLE_VALUES
#: The bounded sample of a column read for repeated values when only it can choose a direction.
UNIQUENESS_SAMPLE_ROWS = 500


class TableUnreadable(Exception):
    """A table of the run's sets could not be read: the table and why."""

    def __init__(self, table: str, reason: str) -> None:
        self.table = table
        self.reason = reason
        super().__init__(f"table {table!r} cannot be read: {reason}")


async def _reading(table_name: str, read: Awaitable[_T]) -> _T:
    """What one read of ``table_name`` answers. Whatever refuses or fails the read (its source
    has no connection, is not bound in this environment, a row cap, a parameter its registration
    does not record as required) is the table's, raised naming it: the one place a read's failure
    is caught, so none reaches the caller unnamed."""
    try:
        return await read
    except Exception as exc:  # noqa: BLE001 - whatever stopped the read, reported by table
        raise TableUnreadable(table_name, str(exc) or type(exc).__name__) from exc


def _published(state: Any, table_id: int, table_name: str) -> tuple[str, dict[str, str]]:
    """The table as the org admin reads it: its quoted ``domain.table`` name, and each registered
    column it can read by the name that column is published under."""
    from provisa.profiler.run import ProfileError, resolve_target
    from provisa.profiler.statement import qualified

    try:
        target = resolve_target(state, table_id, table_name, {})
    except ProfileError as exc:
        raise TableUnreadable(table_name, str(exc)) from exc
    return qualified(target.pgwire_name), {c.physical: c.name for c in target.columns}


async def _rows(table_name: str, sql: str) -> list[tuple]:
    """The rows of one governed read of a table as the org admin (the profiler's read)."""
    from provisa.profiler import run as profiler

    return (await _reading(table_name, profiler._governed(sql)))[1]


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


async def _terms(conn: "Connection", ids: list[int]) -> dict[tuple[int, str], str]:
    """The glossary term bound to each column, by (table id, registered column name)."""
    from provisa.core.schema_org import glossary_term_refs as refs
    from provisa.core.schema_org import glossary_terms as terms

    rows = (
        await conn.execute_core(
            select(refs.c.table_id, refs.c.column_name, terms.c.name)
            .join(terms, terms.c.id == refs.c.term_id)
            .where(refs.c.table_id.in_(ids))
        )
    ).fetchall()
    return {(int(r.table_id), str(r.column_name)): str(r.name) for r in rows}


def _read_from_stored_rows(state: Any, ids: Sequence[int]) -> set[int]:
    """Those of ``ids`` that are parameterized and kept for good (REQ-2031): their rows are
    stored and a read of one is answered from them whole, its parameters ordinary stored columns
    (REQ-1986). Said by the model, as the pipeline reads it; no read is tried."""
    from provisa.api.data.materialization import frozen_parameter_filters

    return {i for i in ids if frozen_parameter_filters(state, [i])}


async def tables_never_read(state: Any, conn: "Connection", ids: Sequence[int]) -> set[int]:
    """Those of ``ids`` discovery never reads (REQ-1986): each requires a parameter, from its
    registered columns, and its rows are not kept for good. Their suggestions were found on
    reduced information."""
    from provisa.core.schema_org import table_columns as tc

    rows = (
        await conn.execute_core(
            select(
                tc.c.table_id,
                tc.c.column_name,
                tc.c.native_filter_type,
                tc.c.native_filter_required,
                tc.c.required_by_steward,
            ).where(tc.c.table_id.in_(sorted(set(ids))))
        )
    ).fetchall()
    requiring = {int(r.table_id) for r in rows if required_parameters([dict(r._mapping)])}
    return requiring - _read_from_stored_rows(state, sorted(requiring))


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
    from provisa.profiler import run as profiler

    judged = await _reading(
        table_name,
        profiler.classify_table(
            state,
            table_id=table_id,
            table_name=table_name,
            values_per_column=VALUES_PER_COLUMN,
            sample_above_cells=SAMPLE_ABOVE_CELLS,
        ),
    )
    return {j.physical: {m.kind.id for m in j.matches} for j in judged}


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


async def _samples(
    state: Any, table_id: int, table_name: str, columns: list[str]
) -> dict[str, tuple[str, ...]]:
    """Up to :data:`CONTEXT_SAMPLE_VALUES` distinct non-null values of each column, from the
    first :data:`CONTEXT_SAMPLE_ROWS` rows read. A column the org admin's relation does not hold
    has none."""
    if not columns:
        return {}
    relation, exposed = _published(state, table_id, table_name)
    columns = [c for c in columns if c in exposed]
    if not columns:
        return {}
    cols = ", ".join(_quote(exposed[c]) for c in columns)
    rows = await _rows(table_name, f"SELECT {cols} FROM {relation} LIMIT {CONTEXT_SAMPLE_ROWS}")
    out: dict[str, list[str]] = {c: [] for c in columns}
    for row in rows:
        for name, value in zip(columns, row, strict=True):
            text = None if value is None else sample_text(value)
            held = out[name]
            if text is not None and text not in held and len(held) < CONTEXT_SAMPLE_VALUES:
                held.append(text)
    return {c: tuple(v) for c, v in out.items()}


def _sensitive(classes: tuple[str, ...]) -> bool:
    from provisa.profiler.classify import kind_of

    return any(kind_of(c).sensitive for c in classes if c != "unknown")


async def registered_facts(state: Any, conn: "Connection", ids: Sequence[int]) -> list[TableFacts]:
    """Every table of ``ids`` as the model store gives it, and nothing else: no table is read, no
    profile looked up, no source asked. A table that is never read (it requires a parameter and
    is not kept for good) is whole here, its columns of the class :data:`NOT_READ`. Any other
    table's columns have the class unknown and no sample values: a run reads those."""
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
                tc.c.native_filter_type,
                tc.c.native_filter_required,
                tc.c.required_by_steward,
            )
            .where(tc.c.table_id.in_(id_list))
            .order_by(tc.c.table_id, tc.c.id)
        )
    ).fetchall():
        columns.setdefault(int(r.table_id), []).append(dict(r._mapping))
    table_tags = await _table_tags(conn, id_list)
    column_tags = await effective_column_tags(conn, id_list)
    terms = await _terms(conn, id_list)
    # REQ-1986: a table that requires a parameter gives no row count and no profile; it is known
    # from its registration, here. One kept for good is read from its stored rows (REQ-2031).
    requiring = [t for t in id_list if required_parameters(columns.get(t, []))]
    never_read = set(requiring) - _read_from_stored_rows(state, requiring)

    out: list[TableFacts] = []
    for t in tables:
        tid = int(t["id"])
        reduced = tid in never_read
        facts = tuple(
            ColumnFacts(
                name=str(c["column_name"]),
                data_type=str(c["data_type"] or ""),
                description=c["description"],
                tags=tuple(
                    sorted({tag["tag_id"] for tag in column_tags.get((tid, c["column_name"]), [])})
                ),
                classes=(NOT_READ,) if reduced else (UNKNOWN,),
                sensitive=False,
                samples=(),
                is_primary_key=bool(c["is_primary_key"]),
                groups=type_groups(
                    str(c["data_type"] or ""),
                    subtype=c["subtype"],
                    array_depth=int(c["array_depth"]),
                ),
                required_parameter=(
                    parameter_name(c) if reduced and is_parameter(c) and is_required(c) else None
                ),
                term=terms.get((tid, str(c["column_name"]))),
            )
            for c in columns.get(tid, [])
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
    return out


async def table_facts(
    state: Any,
    conn: "Connection",
    ids: Sequence[int],
    on_table: Callable[[int, int], Awaitable[None]] | None = None,
) -> list[TableFacts]:
    """Every table of ``ids`` with its context and its columns' facts, as a run reads them: what
    is registered (:func:`registered_facts`), and for each table that is read, its columns'
    classes and sample values. ``on_table(read, total)`` is told after each table is read."""
    registered = await registered_facts(state, conn, ids)
    decided = await _decided(conn, [t.table_id for t in registered])
    out: list[TableFacts] = []
    for table in registered:
        # A table never read, and one with no registered column, has nothing to classify or
        # sample.
        if not table.reduced and table.columns:
            tid = table.table_id
            matched = await _profiled_classes(conn, tid, table.table_name)
            if matched is None:
                matched = await _classified_now(state, tid, table.table_name)
            classed = []
            for c in table.columns:
                classes = classes_of(matched.get(c.name, set()), decided.get((tid, c.name), set()))
                classed.append(replace(c, classes=classes, sensitive=_sensitive(classes)))
            sampled = [c.name for c in classed if c.groups and not c.sensitive]
            samples = await _samples(state, tid, table.table_name, sampled)
            table = replace(
                table, columns=tuple(replace(c, samples=samples.get(c.name, ())) for c in classed)
            )
        out.append(table)
        if on_table is not None:
            await on_table(len(out), len(registered))
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


def uniqueness_reader(state: Any):
    """A reader of a column's bounded sample for repeated values (REQ-413). A column the org
    admin's relation does not hold has a sample of no rows, as an empty table's column has. A
    table that requires a parameter is never read (REQ-1986): the direction rules do not ask for
    its sample, and asking is refused."""

    async def read(table: TableFacts, column: str) -> Sampled:
        if table.reduced:
            raise TableUnreadable(table.table_name, "it requires a parameter and is never read")
        relation, exposed = _published(state, table.table_id, table.table_name)
        if column not in exposed:
            return Sampled(rows=0, repeats=False)
        col = _quote(exposed[column])
        values = await _rows(
            table.table_name,
            f"SELECT {col} FROM {relation} WHERE {col} IS NOT NULL LIMIT {UNIQUENESS_SAMPLE_ROWS}",
        )
        seen = [r[0] for r in values]
        return Sampled(rows=len(seen), repeats=len(set(seen)) != len(seen))

    return read
