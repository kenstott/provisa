# Copyright (c) 2026 Kenneth Stott
# Canary: 6e2b9d41-7c3f-4a85-b0d6-1f9e8a2c5d73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin-save check that a ttl / ttl_probe change signal has a landing cache_ttl (REQ-1907).

A table the operator's settings say is replicated (materialize, row_materialize, or a resolved
replicate 0 / N > 0 / load_protected) with change_signal ttl / ttl_probe needs a cache_ttl on the
table or its source -- it is that signal's refresh clock (REQ-930); the global response-cache
default_ttl is never a landing clock. A table that lands only because the engine cannot reach its
source is judged on the read path instead. The no-TTL freshness signals need no cache_ttl. A source
save and a table save each re-judge every table of the source with the value being saved in place
of the stored one, since a table inherits its source's signal and cache_ttl. The same rule runs at
config load (config_loader._validate_landing_ttl) and on the read path
(role_ttl.require_landing_ttl).

The same pass refuses a table whose resolved settings pair load_protected with replicate -1
(never), which contradict each other (REQ-826)."""

# Requirements: REQ-1907, REQ-826

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import select

from provisa.api.admin.types import MutationResult
from provisa.core.models import BUILT_IN_SOURCE_IDS
from provisa.core.replicate import contradiction
from provisa.core.schema_org import registered_tables, sources
from provisa.federation.role_ttl import lands_from_config, missing_landing_ttl


@dataclass(frozen=True)
class SourceTtl:
    """A source's change signal, cache_ttl and replication settings as being saved."""

    change_signal: str
    cache_ttl: int | None
    replicate: int | None
    load_protected: bool


@dataclass(frozen=True)
class TableTtl:
    """A table's change signal, cache_ttl and replication settings as being saved (None =
    inherit)."""

    schema_name: str
    table_name: str
    change_signal: str | None
    cache_ttl: int | None
    materialize: bool
    row_materialize: bool
    replicate: int | None
    load_protected: bool | None


async def _as_saved(
    conn: Any,
    source_id: str,
    source: SourceTtl | None,
    table: TableTtl | None,
) -> tuple[SourceTtl, list[TableTtl]] | MutationResult:
    """The source and every table of it, with ``source`` / ``table`` (the values being saved) in
    place of the stored ones; a failing result when the source does not exist."""
    if source is None:
        res = await conn.execute_core(
            select(
                sources.c.change_signal,
                sources.c.cache_ttl,
                sources.c.replicate,
                sources.c.load_protected,
            ).where(sources.c.id == source_id)
        )
        row = res.fetchone()
        if row is None:
            return MutationResult(
                success=False,
                message=f"Source {source_id!r} not found",
                code="schema.source_not_found",
                params={"source": source_id},
            )
        source = SourceTtl(row.change_signal, row.cache_ttl, row.replicate, row.load_protected)
    res = await conn.execute_core(
        select(
            registered_tables.c.schema_name,
            registered_tables.c.table_name,
            registered_tables.c.change_signal,
            registered_tables.c.cache_ttl,
            registered_tables.c.materialize,
            registered_tables.c.row_materialize,
            registered_tables.c.replicate,
            registered_tables.c.load_protected,
        ).where(registered_tables.c.source_id == source_id)
    )
    tables = [TableTtl(*r) for r in res.fetchall()]
    if table is not None:
        key = (table.schema_name, table.table_name)
        tables = [t for t in tables if (t.schema_name, t.table_name) != key] + [table]
    return source, tables


def _contradiction(
    source_id: str, source: SourceTtl, tables: list[TableTtl]
) -> MutationResult | None:
    """The refusal for load_protected with replicate -1 (REQ-826): on the source itself, or on the
    first table whose resolved settings pair them."""
    refused = contradiction(source.replicate, source.load_protected)
    if refused is not None:
        return MutationResult(
            success=False,
            message=f"Source {source_id!r}: {refused}",
            code="schema.replicate_contradicts_load_protected",
            params={"source": source_id},
        )
    for t in tables:
        refused = contradiction(
            t.replicate if t.replicate is not None else source.replicate,
            bool(t.load_protected if t.load_protected is not None else source.load_protected),
        )
        if refused is not None:
            return MutationResult(
                success=False,
                message=f"Table {t.table_name!r} (source {source_id!r}): {refused}",
                code="schema.replicate_contradicts_load_protected",
                params={"table": t.table_name, "source": source_id},
            )
    return None


async def replicate_contradiction_refusal(
    conn: Any,
    source_id: str,
    *,
    source: SourceTtl | None = None,
    table: TableTtl | None = None,
) -> MutationResult | None:
    """A failing MutationResult when the save would pair load_protected with replicate -1 on the
    source or on a table of it (REQ-826), judged with the values being saved; None otherwise. For
    a save that changes load protection, whose own rule (REQ-1141) judges its refresh gates."""
    if source_id in BUILT_IN_SOURCE_IDS:
        return None
    saved = await _as_saved(conn, source_id, source, table)
    if isinstance(saved, MutationResult):
        return saved
    return _contradiction(source_id, *saved)


async def landing_ttl_refusal(
    conn: Any,
    source_id: str,
    *,
    source: SourceTtl | None = None,
    table: TableTtl | None = None,
) -> MutationResult | None:
    """A failing MutationResult naming the first table of ``source_id`` whose ttl / ttl_probe
    signal has no table or source cache_ttl, judged with ``source`` / ``table`` (the values being
    saved) in place of the stored ones; None when every table has a landing clock. Refuses the
    load_protected / replicate -1 pair first (REQ-826).

    A built-in source (provisa-admin, provisa-otel, __derived__) is Provisa's own catalog or MV
    store: its tables are never landed from an upstream by change_signal, so the rule has nothing
    to judge there."""
    if source_id in BUILT_IN_SOURCE_IDS:
        return None
    saved = await _as_saved(conn, source_id, source, table)
    if isinstance(saved, MutationResult):
        return saved
    source, tables = saved
    contradicted = _contradiction(source_id, source, tables)
    if contradicted is not None:
        return contradicted
    for t in tables:
        if not lands_from_config(
            materialize=t.materialize,
            row_materialize=t.row_materialize,
            table_replicate=t.replicate,
            source_replicate=source.replicate,
            table_load_protected=t.load_protected,
            source_load_protected=source.load_protected,
        ):
            continue
        err = missing_landing_ttl(
            t.change_signal, source.change_signal, t.cache_ttl, source.cache_ttl
        )
        if err is not None:
            return MutationResult(
                success=False,
                message=f"Table {t.table_name!r} (source {source_id!r}): {err}",
                code="schema.landing_ttl_required",
                params={"table": t.table_name, "source": source_id},
            )
    return None
