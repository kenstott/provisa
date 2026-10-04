# Copyright (c) 2026 Kenneth Stott
# Canary: 3c2d9a71-6b08-4e75-8f12-3c7a0d4f9c11
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Delta-fetch incremental reload for MATERIALIZED replicas (REQ-874).

An incremental reload for datasets whose federation strategy is MATERIALIZED (REQ-826) —
the data is landed, so a full re-pull is the cost delta avoids. VIRTUAL and SCAN are
excluded (always fresh / read-in-place).

KEY SIMPLIFICATION — PROBE == DELTA for monotonic-cursor entries: the delta query IS the
freshness evaluation. Run it; a non-empty result means changed (apply the rows), empty means
fresh (no-op). No separate watermark/probe query.

The delta_query is ONE author-supplied, source-native query with two placeholders Provisa
SUBSTITUTES but never parses: ``$wm`` (bound to the stored cursor value) and ``{{fields}}``
(the table's registered selection set). The cursor field is implicit — the field ``$wm``
filters on — and after applying delta rows the stored cursor advances to max(cursor-field)
over the returned rows. This module is the UNIFORM part (field injection, the PROBE==DELTA
decision, cursor advance); the per-source-type authoring and native execution are elsewhere.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

    from provisa.federation.strategy import Strategy

_FIELDS_PLACEHOLDER = "{{fields}}"
_WM_PLACEHOLDER = "$wm"

# REQ-874: the source types a generated SQL delta (SELECT {fields} FROM t WHERE wm > $1 ORDER BY
# wm, run on the source's native pool) is defined for -- the RDBMS, Postgres/MySQL-wire-compatible,
# cloud-warehouse and SQL-OLAP types. A `delta` on any other source type (files, REST/GraphQL,
# document/graph/search stores, lakes, streams) is refused by name at config load and admin save;
# at build time a non-SQL source also falls back to a whole rebuild (delta_build_reason -> SKIP_STORE).
SQL_DELTA_SOURCE_TYPES = frozenset(
    {
        "postgresql", "mysql", "singlestore", "mariadb", "sqlserver", "oracle", "firebird",
        "duckdb", "motherduck", "saphana", "cockroachdb", "yugabytedb", "greenplum", "tidb",
        "snowflake", "bigquery", "databricks", "fabric", "synapse", "redshift", "trino",
        "clickhouse", "exasol",
    }
)  # fmt: skip


def delta_source_supported(source_type: str) -> bool:  # REQ-874
    """Whether a generated SQL delta is defined for this source type (SQL_DELTA_SOURCE_TYPES)."""
    return source_type in SQL_DELTA_SOURCE_TYPES


def delta_applies(strategy: Strategy) -> bool:  # REQ-874
    """Delta reload is defined only for MATERIALIZED entries (VIRTUAL/SCAN are excluded)."""
    from provisa.federation.strategy import Strategy as _S

    return strategy is _S.MATERIALIZED


def render_delta_fields(template: str, fields: Sequence[str], *, separator: str = ", ") -> str:
    """Substitute the ``{{fields}}`` placeholder with the registered selection set (REQ-874).

    Pure textual substitution — Provisa never parses the source-native filter. ``$wm`` is left
    intact to be bound natively to the stored cursor value at execution.
    """
    return template.replace(_FIELDS_PLACEHOLDER, separator.join(fields))


def has_wm_placeholder(template: str) -> bool:
    """A well-formed delta_query must carry the ``$wm`` cursor placeholder."""
    return _WM_PLACEHOLDER in template


def delta_is_fresh(rows: Sequence[object]) -> bool:  # REQ-874 PROBE == DELTA
    """The delta query IS the freshness check: empty result ⇒ fresh (no-op), non-empty ⇒ changed."""
    return len(rows) == 0


def advance_cursor(
    rows: Sequence[dict], cursor_field: str, current: object | None
) -> object | None:  # REQ-874
    """Advance the stored cursor to max(cursor-field) over the returned delta rows.

    Empty result keeps the current cursor. Cursor and monotonicity are the registrant's
    responsibility; Provisa does no dedup or boundary-inclusivity logic.
    """
    values = [row[cursor_field] for row in rows if cursor_field in row]
    if not values:
        return current
    return max(values)


# Why a delta reload falls back to a whole rebuild, by declared rule (REQ-874). Shown on the
# replica status as "whole rebuild: <reason>"; never a silent rebuild.
SKIP_NO_DELTA = "no_delta_declared"
SKIP_FIRST_BUILD = "first_build"  # no replica or no stored cursor yet
SKIP_DEFINITION = "definition_changed"  # columns/key/address changed, or reason model/definition
SKIP_OPERATOR = "operator_requested"  # an operator start-build
SKIP_REBUILD_EVERY = "rebuild_interval_elapsed"
SKIP_STORE = "store_cannot_apply_delta"


def delta_build_reason(
    *,
    has_delta: bool,
    has_cursor: bool,
    reason: str,
    definition_changed: bool,
    store_applies_delta: bool,
    rebuild_due: bool,
) -> str | None:
    """None when this build applies a delta; otherwise the declared ``delta_skipped`` code for
    why it falls back to a whole rebuild (REQ-874). The order is the rule's: no delta declared,
    then the cases that force a whole copy (first build, a definition change, an operator full
    build, the rebuild interval), then a store that cannot apply a delta. A delta read or apply that
    FAILS is a failed build, never routed here.

    ``REASON_MODEL`` is NOT a force-whole here: a model-declared replica carries ``requested_reason
    = "model"`` for its whole life, so once it HAS a completed build and a cursor (``has_cursor``,
    checked above), a model-driven refresh is exactly when a delta should apply -- the first build
    is already separated out as ``SKIP_FIRST_BUILD``. Only a real definition change
    (``REASON_DEFINITION`` / ``definition_changed``) or an operator full build forces a whole copy."""
    from provisa.federation import replica_state as rs

    if not has_delta:
        return SKIP_NO_DELTA
    if not has_cursor:
        return SKIP_FIRST_BUILD
    if definition_changed or reason == rs.REASON_DEFINITION:
        return SKIP_DEFINITION
    if reason == rs.REASON_OPERATOR:
        return SKIP_OPERATOR
    if rebuild_due:
        return SKIP_REBUILD_EVERY
    if not store_applies_delta:
        return SKIP_STORE
    return None


async def apply_sql_delta(
    state: Any,
    engine: Any,
    source: Any,
    table: Any,
    args: Any,
    address: Any,
    cursor: object | None,
    store_dsn: str,
) -> tuple[int, object | None]:
    """Read the delta from a SQL source and apply it to the replica by primary key (REQ-874).

    SQL-source scope: Provisa GENERATES ``SELECT {fields} FROM <source table> WHERE <wm> > $1
    ORDER BY <wm>`` from the table's ``watermark_column`` (the cursor field), binds the stored
    cursor as ``$1`` -- a parameter, never spliced -- and reads it on the source's native pool
    through the DIRECT terminal (``EngineRuntime.execute_native``). The returned rows are applied to
    the replica's store table in ONE transaction (``materialize_exec.apply_cdc``): a tombstoned row
    (``deletes == 'tombstone'`` and its ``tombstone_column`` truthy) deletes its key, every other
    row upserts on the registered primary key. Returns ``(rows_applied, advanced_cursor)``; an empty
    read is fresh -> ``(0, cursor)`` with no write. The cursor advances to the max watermark over
    the returned rows (``advance_cursor``)."""
    from provisa.federation.materialize_exec import apply_cdc, build_table
    from provisa.federation.store_writer import store_connection

    delta = table.delta
    wm = table.watermark_column
    if not wm:
        # Table._validate_delta already guarantees this; a defensive check, never a fallback.
        raise ValueError(f"table {table.table_name!r}: delta needs watermark_column (REQ-874)")
    pk_columns = list(args.pk_columns or [])
    if not pk_columns:
        raise ValueError(
            f"table {table.table_name!r}: delta apply needs a primary key to upsert/delete by "
            "(REQ-874)"
        )
    field_list = ", ".join(f'"{name}"' for name, _ in args.columns)
    src = f'"{table.schema_name}"."{table.table_name}"'
    sql = f'SELECT {field_list} FROM {src} WHERE "{wm}" > $1 ORDER BY "{wm}"'
    result = await engine.execute_native(state.source_pools, source.id, sql, [cursor])
    rows = [dict(zip(result.column_names, r, strict=False)) for r in result.rows]
    if delta_is_fresh(rows):
        return 0, cursor
    tombstone = delta.tombstone_column if delta.deletes == "tombstone" else None
    events = [
        SimpleNamespace(
            operation="delete" if tombstone and row.get(tombstone) else "upsert", row=row
        )
        for row in rows
    ]
    tbl = build_table(address.schema, address.table, args.columns, tuple(pk_columns))
    async with store_connection(store_dsn) as conn:
        await apply_cdc(conn, tbl, pk_columns, events)
    return len(rows), advance_cursor(rows, wm, cursor)


async def source_max_watermark(state: Any, engine: Any, source: Any, table: Any) -> object | None:
    """The current max of a delta table's watermark column at its SQL source (REQ-874). A whole
    rebuild sets the delta cursor to this so the next delta resumes from it instead of re-pulling
    everything. None when the table is empty (no rows yet)."""
    wm = table.watermark_column
    if not wm:
        raise ValueError(f"table {table.table_name!r}: delta needs watermark_column (REQ-874)")
    src = f'"{table.schema_name}"."{table.table_name}"'
    result = await engine.execute_native(
        state.source_pools, source.id, f'SELECT MAX("{wm}") FROM {src}', []
    )
    if not result.rows or result.rows[0][0] is None:
        return None
    return result.rows[0][0]
