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
from provisa.federation.role_ttl import CLOCKED_SIGNALS, lands_from_config, missing_landing_ttl


@dataclass(frozen=True)
class SourceTtl:
    """A source's change signal, cache_ttl and replication settings as being saved."""

    change_signal: str
    cache_ttl: int | None
    replicate: int | None
    load_protected: bool
    #: REQ-860: the source's read-time freshness gate; its ``probe`` needs no cache_ttl.
    freshness_gate: bool


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
                sources.c.freshness_gate,
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
        source = SourceTtl(
            row.change_signal,
            row.cache_ttl,
            row.replicate,
            row.load_protected,
            row.freshness_gate,
        )
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


async def _stored_hints(conn: Any, source_id: str) -> dict:
    """The stored federation_hints of ``source_id``; empty for a source not stored yet."""
    row = (
        await conn.execute_core(select(sources.c.federation_hints).where(sources.c.id == source_id))
    ).fetchone()
    return {} if row is None else dict(row.federation_hints or {})


async def _stored_type(conn: Any, source_id: str) -> str | None:
    """The stored type of ``source_id``, or None for a source not stored yet."""
    row = (
        await conn.execute_core(select(sources.c.type).where(sources.c.id == source_id))
    ).fetchone()
    return None if row is None else row.type


async def row_materialize_save_refusal(
    conn: Any, source_id: str, table_name: str, row_materialize: bool, pk_columns: list[str]
) -> MutationResult | None:
    """A failing MutationResult when a table saved with row_materialize is of a source whose type
    has no row fetch by primary keys, or is refused it for a reason of its own, or -- for a type
    whose fetch is each table's own -- is a table without one (REQ-1865,
    ``core.source_registry.row_materialize_refusal``); None otherwise."""
    if not row_materialize:
        return None
    from provisa.core.source_registry import (
        ROW_FETCH_BY_KEYS_PER_TABLE,
        no_key_fetch_refusal,
        row_materialize_refusal,
    )

    source_type = await _stored_type(conn, source_id)
    own = None
    if source_type in ROW_FETCH_BY_KEYS_PER_TABLE:
        from provisa.api.admin._graphql_table_registration import stored_key_fetches
        from provisa.api.app import state
        from provisa.graphql_remote.by_keys import has_key_fetch

        fetches = await stored_key_fetches(state, source_id, table_name)
        own = has_key_fetch(fetches, pk_columns)
        if not own:
            return MutationResult(
                success=False,
                message=no_key_fetch_refusal(table_name),
                code="schema.row_materialize_no_key_fetch",
                params={"table": table_name},
            )
    refused = row_materialize_refusal(table_name, source_type, table_fetches_by_keys=own)
    return None if refused is None else MutationResult(success=False, message=refused)


def _engine_reads_in_place(source_type: str | None) -> bool:
    """Whether the bound engine reads a source of ``source_type`` live in place (an attach or a
    scan) rather than serving it from a replica (federation.strategy.engine_attaches). A source not
    stored yet has no tables to judge, so it is read as in place."""
    from provisa.api.app import state
    from provisa.federation.strategy import engine_attaches

    if source_type is None:
        return True
    return engine_attaches(state.federation_engine, source_type)


def kind_lands(source_type: str) -> bool:
    """Whether every table of a source of ``source_type`` is served from a replica here: the
    bound engine cannot read the kind in place, and the kind owns a replica at all. Such a
    table needs a landing cache_ttl, its own or its source's, to be registered under a ttl
    signal -- the judgment :func:`landing_ttl_refusal` makes at the save."""
    from provisa.federation.replica_routing import NO_REPLICA_TYPES

    from provisa.core.source_registry import is_live_only

    return (
        source_type not in NO_REPLICA_TYPES
        and not is_live_only(source_type)  # REQ-2000
        and not _engine_reads_in_place(source_type)
    )


def _live_only_refusal(
    source_id: str, source_type: str | None, source: SourceTtl, tables: list[TableTtl]
) -> MutationResult | None:
    """REQ-2000: a live-only source's tables are read live, never replicated, so a replicate value
    on the source or a table, or a table's row_materialize, is refused; with none there is no
    landing clock to judge."""
    if source.replicate is not None:
        return _replicated_live_only(source_id, source_type, f"Source {source_id!r}")
    for t in tables:
        if t.replicate is not None or t.row_materialize:
            return _replicated_live_only(source_id, source_type, f"Table {t.table_name!r}")
    return None


def _replicated_live_only(source_id: str, source_type: str | None, what: str) -> MutationResult:
    return MutationResult(
        success=False,
        message=f"{what}: {source_type} is live-only: its tables are read live, never replicated",
        code="schema.live_only_replicated",
        params={"source": source_id, "type": source_type},
    )


def source_states_the_clock(source_type: str | None) -> bool:
    """Whether a source of ``source_type`` must state the landing cache_ttl itself, rather than
    leave it to each table. True for the mail kinds (every declared mail platform): their tables
    are one fixed set that always lands, read together by one read of the source, so the one
    clock they refresh by is the source's. The source form asks it of this function
    (``/admin/source-kinds/{kind}``); nothing else names those kinds."""
    from provisa.core import mail_platforms

    return source_type in mail_platforms.source_types()


async def landing_ttl_refusal(
    conn: Any,
    source_id: str,
    *,
    source: SourceTtl | None = None,
    table: TableTtl | None = None,
    source_type: str | None = None,
    federation_hints: dict | None = None,
) -> MutationResult | None:
    """A failing MutationResult naming the first table of ``source_id`` whose ttl / ttl_probe
    signal has no table or source cache_ttl, judged with ``source`` / ``table`` (the values being
    saved) in place of the stored ones; None when every table has a landing clock. A source whose
    kind states the clock itself (``source_states_the_clock``) is refused without one, tables or
    none; ``source_type`` is the type being saved, for a source not stored yet, and
    ``federation_hints`` the hints being saved (else the stored ones): a source that asks to be read
    in place (``reach: "scan"``, REQ-1867) is not landed by the engine. Refuses the
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
    # A table lands when the operator's settings say so, and also when the bound engine cannot
    # read its source in place — every table of such a source is served from a replica, so it
    # needs the same refresh clock (REQ-1907). Asked of the stored source: a source being created
    # has no tables yet.
    from provisa.federation.replica_routing import NO_REPLICA_TYPES

    # The type being saved (a source being created is not stored yet), else the stored one.
    source_type = source_type or await _stored_type(conn, source_id)
    if (
        source_states_the_clock(source_type)
        and source.change_signal in CLOCKED_SIGNALS
        and source.cache_ttl is None
    ):
        return MutationResult(
            success=False,
            message=(
                f"Source {source_id!r}: change_signal {source.change_signal!r} is TTL-based and "
                "every table of this source lands, so set the source's Cache TTL (Load "
                "Management and Timeliness)"
            ),
            code="schema.source_cache_ttl_required",
            params={"source": source_id, "signal": source.change_signal},
        )
    # A type that owns no replica (ingest, a profiler's results: REQ-1771,
    # REQ-1934) is read where it is written and never landed, so it has no landing clock to judge.
    if source_type in NO_REPLICA_TYPES:
        return None
    from provisa.core.source_registry import is_live_only

    if is_live_only(source_type):
        return _live_only_refusal(source_id, source_type, source, tables)  # REQ-2000
    hints = (
        federation_hints if federation_hints is not None else await _stored_hints(conn, source_id)
    )
    engine_lands = not (hints.get("reach") == "scan" or _engine_reads_in_place(source_type))
    for t in tables:
        if not engine_lands and not lands_from_config(
            materialize=t.materialize,
            row_materialize=t.row_materialize,
            table_replicate=t.replicate,
            source_replicate=source.replicate,
            table_load_protected=t.load_protected,
            source_load_protected=source.load_protected,
        ):
            continue
        err = missing_landing_ttl(
            t.change_signal,
            source.change_signal,
            t.cache_ttl,
            source.cache_ttl,
            gated=source.freshness_gate,
        )
        if err is not None:
            return MutationResult(
                success=False,
                message=f"Table {t.table_name!r} (source {source_id!r}): {err}",
                code="schema.landing_ttl_required",
                params={"table": t.table_name, "source": source_id},
            )
    return None
