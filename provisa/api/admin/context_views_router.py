# Copyright (c) 2026 Kenneth Stott
# Canary: c54dd29e-89c1-4525-aac6-082ce93900ae
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The read-only Tags and Classification pages of the Context group (REQ-2259).

* ``GET /admin/context/tags`` -- per tag, the tables and columns it is applied to, what
  depends on it: the row filters keyed on it and the classification kinds whose acceptance
  assigns it.
* ``GET /admin/context/classification`` -- each classification kind and the columns it was
  detected on (an accepted decision).

Both are gated on ``glossary_read``. Only the tables and columns the caller reads are named
(:func:`provisa.api.admin.read_rights.reader_for_request`, REQ-2152); one it does not is counted
and never named. Editing stays where it is: the tags in Admin, the decisions in the table editor.
"""

# Requirements: REQ-2259

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any, cast

from fastapi import APIRouter, Request
from sqlalchemy import func, select

from provisa.api.admin._guards import require_active_org_id
from provisa.api.admin._platform_guard import _require_right
from provisa.security.rights import Capability

if TYPE_CHECKING:
    from provisa.core.database import Connection, Database

router = APIRouter(prefix="/admin/context", tags=["admin", "context"])

# The tag an accepted sensitive classification assigns (profiler.classifications.accept_effects).
CLASSIFICATION_TAG = "pii"

ColumnReadable = Callable[[int, "str | None"], bool]


async def _pool() -> "Database":
    from provisa.api.app import state

    assert state.model_db is not None
    return state.model_db


def _require_glossary_read(request: Request) -> None:
    _require_right(request, Capability.GLOSSARY_READ.value)


def placed(rows: Iterable[dict], readable: ColumnReadable) -> tuple[list[dict], int]:  # REQ-2259
    """The ``rows`` (each with ``table_id``, ``table_ref`` and ``column_name``; a column of None
    is the table itself) the caller reads, named, and the number it does not, counted."""
    named: list[dict] = []
    hidden = 0
    for r in rows:
        if readable(r["table_id"], r["column_name"]):
            named.append(
                {"tableId": r["table_id"], "tableRef": r["table_ref"], "column": r["column_name"]}
            )
        else:
            hidden += 1
    return named, hidden


def tag_usages(
    tag_ids: Iterable[str],
    assignments: Iterable[dict],
    row_filters: dict[str, int],
    sensitive_kinds: list[str],
    readable: ColumnReadable,
) -> list[dict]:  # REQ-2259
    """Per tag, where it is applied on tables and columns (named when readable, else counted)
    and what depends on it: the number of row filters keyed on it and the classification kinds
    whose acceptance assigns it. ``assignments`` are table and column assignments with
    ``base_tag_id``."""
    by_tag: dict[str, list[dict]] = {}
    for a in assignments:
        by_tag.setdefault(a["base_tag_id"], []).append(a)
    out = []
    for tag_id in tag_ids:
        named, hidden = placed(by_tag.get(tag_id, []), readable)
        out.append(
            {
                "id": tag_id,
                "usages": named,
                "hiddenUsages": hidden,
                "rowFilters": row_filters.get(tag_id, 0),
                "classificationKinds": sensitive_kinds if tag_id == CLASSIFICATION_TAG else [],
            }
        )
    return out


def kind_detections(
    kinds: Iterable[Any],
    disabled: frozenset[str],
    accepted: Iterable[dict],
    readable: ColumnReadable,
) -> list[dict]:  # REQ-2259
    """Each classification kind, with the columns it was detected on (``accepted``: rows with
    ``kind``, ``table_id``, ``table_ref`` and ``column_name``), named when readable, else counted."""
    by_kind: dict[str, list[dict]] = {}
    for a in accepted:
        by_kind.setdefault(a["kind"], []).append(a)
    out = []
    for k in kinds:
        named, hidden = placed(by_kind.get(k.id, []), readable)
        out.append(
            {
                "id": k.id,
                "sensitive": k.sensitive,
                "threshold": k.threshold,
                "maskPreset": k.mask_preset,
                "enabled": k.id not in disabled,
                "detections": named,
                "hiddenDetections": hidden,
            }
        )
    return out


def _table_ref(r: Any) -> str:
    return f"{r.source_id}.{r.schema_name}.{r.table_name}"


@router.get("/tags")
async def tags_in_context(request: Request) -> dict:
    _require_glossary_read(request)
    require_active_org_id(request)
    from provisa.api.admin.read_rights import reader_for_request
    from provisa.core.repositories import tag as tag_repo
    from provisa.core.schema_org import rls_rules
    from provisa.profiler.classify import catalogue

    reader = reader_for_request(request)
    pool = await _pool()
    async with pool.acquire() as conn:
        _conn = cast("Connection", conn)
        tags = await tag_repo.list_all(_conn)
        assignments = [
            a
            for a in await tag_repo.list_assignments(_conn)
            if a["object_type"] in ("table", "column") and a["table_id"] is not None
        ]
        filters = (
            await _conn.execute_core(
                select(rls_rules.c.tag_id, func.count())
                .where(rls_rules.c.tag_id.is_not(None))
                .group_by(rls_rules.c.tag_id)
            )
        ).fetchall()
    sensitive = [k.id for k in catalogue() if k.sensitive]
    return {
        "tags": tag_usages(
            [t["id"] for t in tags],
            assignments,
            {r[0]: r[1] for r in filters},
            sensitive,
            reader.column,
        )
    }


@router.get("/classification")
async def classification_in_context(request: Request) -> dict:
    _require_glossary_read(request)
    require_active_org_id(request)
    from provisa.api.admin.read_rights import reader_for_request
    from provisa.core.org_settings import read_org_overrides
    from provisa.core.schema_org import profiler_classifications as pc
    from provisa.core.schema_org import registered_tables as rt
    from provisa.profiler import classifications
    from provisa.profiler.classify import catalogue

    reader = reader_for_request(request)
    pool = await _pool()
    disabled = classifications.disabled_of(await read_org_overrides(pool))
    async with pool.acquire() as conn:
        rows = (
            await cast("Connection", conn).execute_core(
                select(
                    pc.c.kind,
                    pc.c.table_id,
                    pc.c.physical_column,
                    rt.c.source_id,
                    rt.c.schema_name,
                    rt.c.table_name,
                )
                .join(rt, rt.c.id == pc.c.table_id)
                .where(pc.c.status == "accepted", rt.c.retired.is_(False))
                .order_by(rt.c.table_name, pc.c.physical_column)
            )
        ).fetchall()
    accepted = [
        {
            "kind": r.kind,
            "table_id": r.table_id,
            "table_ref": _table_ref(r),
            "column_name": r.physical_column,
        }
        for r in rows
    ]
    return {"kinds": kind_detections(catalogue(), disabled, accepted, reader.column)}


def awaiting_by_table(
    suggested: Iterable[dict], decided: set[tuple[int, str, str]], readable: ColumnReadable
) -> tuple[list[dict], int]:  # REQ-2259
    """The classification detections awaiting a decision, per table: ``suggested`` rows (each
    with ``table_id``, ``table_ref``, ``column_name`` -- the physical column -- and ``kind``)
    from each table's latest profile run, less those ``decided`` since (``(table_id, column,
    kind)``). A detection on a column the caller reads is counted under its table, named; one on
    a column it does not is counted in the second value, its table never named."""
    named: dict[int, dict] = {}
    hidden = 0
    for r in suggested:
        if (r["table_id"], r["column_name"], r["kind"]) in decided:
            continue
        if not readable(r["table_id"], r["column_name"]):
            hidden += 1
            continue
        entry = named.setdefault(
            r["table_id"], {"tableId": r["table_id"], "tableRef": r["table_ref"], "awaiting": 0}
        )
        entry["awaiting"] += 1
    return sorted(named.values(), key=lambda e: e["tableRef"]), hidden


@router.get("/classification/awaiting")
async def classification_awaiting(request: Request) -> dict:
    """REQ-2259 (Context Health): the detections of each table's latest measured profile run that
    no one has accepted or rejected, counted per table the caller reads; a detection on a column
    it does not read is counted, never named. A table never profiled has no detections."""
    _require_glossary_read(request)
    require_active_org_id(request)
    from sqlalchemy import text

    from provisa.api.admin.read_rights import reader_for_request
    from provisa.core.schema_org import profiler_classifications as pc
    from provisa.core.schema_org import registered_tables as rt
    from provisa.profiler.declared import measured
    from provisa.profiler.schema import (
        is_result_table_name,
        parse_result_table_name,
        result_sa_table,
    )

    reader = reader_for_request(request)
    pool = await _pool()
    suggested: list[dict] = []
    async with pool.acquire() as conn:
        _conn = cast("Connection", conn)
        tables = {
            r.id: r
            for r in (
                await _conn.execute_core(
                    select(rt.c.id, rt.c.source_id, rt.c.schema_name, rt.c.table_name).where(
                        rt.c.retired.is_(False)
                    )
                )
            ).fetchall()
        }
        # The result relations that exist: a table a profiler has not run on has none.
        relations = (
            await _conn.execute_core(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'"
                )
            )
        ).fetchall()
        held = {parse_result_table_name(r[0]) for r in relations if is_result_table_name(r[0])}
        for table_id, row in tables.items():
            name = row.table_name
            if (name, table_id, "runs") not in held or (
                name,
                table_id,
                "classifications",
            ) not in held:
                continue
            runs = result_sa_table(name, table_id, "runs")
            latest = (
                await _conn.execute_core(
                    select(runs.c.run_id)
                    .where(runs.c.status == "succeeded", measured(runs))
                    .order_by(runs.c.run_time.desc())
                    .limit(1)
                )
            ).fetchone()
            if latest is None:
                continue
            found = result_sa_table(name, table_id, "classifications")
            for r in (
                await _conn.execute_core(
                    select(found.c.physical_column, found.c.kind).where(
                        found.c.run_id == latest[0], found.c.status == "suggested"
                    )
                )
            ).fetchall():
                suggested.append(
                    {
                        "table_id": table_id,
                        "table_ref": _table_ref(row),
                        "column_name": str(r.physical_column),
                        "kind": str(r.kind),
                    }
                )
        decided = {
            (r.table_id, r.physical_column, r.kind)
            for r in (
                await _conn.execute_core(select(pc.c.table_id, pc.c.physical_column, pc.c.kind))
            ).fetchall()
        }
    named, hidden = awaiting_by_table(suggested, decided, reader.column)
    return {"tables": named, "hidden": hidden}
