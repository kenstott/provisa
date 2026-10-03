# Copyright (c) 2026 Kenneth Stott
# Canary: 5b8c2e1a-4f70-4d93-a2c6-0e9b7f1d3a58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Per-source input-version signal gathering for MV refresh (REQ-862).

At refresh time each source table is asked for the strongest point-in-time signal
it can offer, so the lineage trace records the actual data version consumed rather
than always degrading to the refresh wall-clock:

- Iceberg sources expose ``<table>$snapshots`` — the latest committed snapshot id is
  a precise, monotonic version (``iceberg_snapshot``).
- RDB sources declaring a ``watermark_column`` (REQ-260, discovered from
  ``provisa_admin.public.registered_tables``) yield ``MAX(<col>)`` (``watermark``).

Both are queried through the same the engine connection the refresh already uses (the
config DB is the ``provisa_admin`` catalog, the Iceberg store is its own catalog),
so gathering needs no extra plumbing. It is best-effort telemetry: a source that is
neither Iceberg nor watermarked simply contributes nothing, and any query error is
logged and skipped — signal gathering never fails a refresh. ``resolve_input_version``
then picks the strongest signal, or the refresh epoch when there are none.
"""

from __future__ import annotations

import logging

from typing import TYPE_CHECKING

from provisa.lineage import InputVersion

if TYPE_CHECKING:
    from provisa.mv.models import TableIdentity

log = logging.getLogger(__name__)

# Registry of source-table watermark columns (REQ-260) lives in the config DB, which
# the engine exposes as the provisa_admin catalog.
_WATERMARK_LOOKUP_SQL = (
    "SELECT source_id, schema_name, table_name, watermark_column "
    "FROM provisa_admin.public.registered_tables "
    "WHERE watermark_column IS NOT NULL"
)


async def _watermark_columns(engine) -> dict[tuple[str, str, str], str]:
    """Map each registered table's identity ``(source_id, schema, table)`` to its watermark
    column, from the config registry. {} on failure.

    A read of the control plane's own registry through the engine's ``provisa_admin`` catalog,
    by design: it is not a source table, it has no replica, and it is not addressed (REQ-1912)."""
    try:
        rows = (await engine.execute_engine(_WATERMARK_LOOKUP_SQL)).rows
        return {(row[0], row[1], row[2]): row[3] for row in rows if row[2] and row[3]}
    except Exception as exc:  # noqa: BLE001 — best-effort; missing registry is not fatal
        log.debug("watermark-column lookup unavailable: %s", exc)
        return {}


async def _iceberg_snapshot(engine, table: "TableIdentity") -> str | None:
    """Latest committed Iceberg snapshot id for ``table``, or None if not Iceberg.

    ``table`` is a registered table; the snapshot list is the SOURCE's own metadata table
    (``<table>$snapshots``), so it is read at the table's registered address on the engine, by
    design, never at a replica (a replica has no snapshots). A table served from its replica has
    no live attach to read it through and contributes no snapshot signal."""
    try:
        catalog, schema, name = await engine.registered_key(table)
        from provisa.federation.runtime import quoted_name

        rows = (
            await engine.execute_engine(
                f"SELECT snapshot_id FROM {quoted_name((catalog, schema, name + '$snapshots'))} "
                "ORDER BY committed_at DESC LIMIT 1"
            )
        ).rows
        row = rows[0] if rows else None
    except Exception as exc:  # noqa: BLE001 — non-Iceberg tables have no $snapshots
        log.debug("no iceberg snapshot for %s: %s", table.label, exc)
        return None
    return str(row[0]) if row and row[0] is not None else None


async def _table_watermark(engine, table: "TableIdentity", column: str) -> str | None:
    """``MAX(column)`` for ``table`` as an RDB watermark value, or None on failure.

    ``table`` is a registered table, read where the engine reads it (REQ-1912): the view
    built from it reads that same address, so this is the version of what the view will read."""
    try:
        rows = (
            await engine.execute_engine(
                f'SELECT MAX("{column}") FROM {await engine.read_ref(table)}'
            )
        ).rows
        row = rows[0] if rows else None
    except Exception as exc:  # noqa: BLE001 — column/table may be unqueryable here
        log.debug("no watermark for %s.%s: %s", table.label, column, exc)
        return None
    return str(row[0]) if row and row[0] is not None else None


def input_token(signals: list[InputVersion], source_tables: list) -> str | None:
    """A stable per-MV change token from per-source signals, or None (REQ-881).

    Usable ONLY when EVERY source produced a signal (len == len(source_tables)); a partial
    signal set returns None so the caller degrades to TTL and never skips a rebuild on
    incomplete change information (REQ-855 None-degrades-to-TTL).
    """
    if not source_tables or len(signals) != len(source_tables):
        return None
    return ";".join(sorted(f"{s.kind}:{s.value}" for s in signals))


async def gather_input_signals(engine, source_tables: list["TableIdentity"]) -> list[InputVersion]:
    """Gather the strongest available input-version signal per source table (REQ-862).

    Prefers an Iceberg snapshot id; falls back to an RDB watermark when the source
    declares a watermark column. Sources offering neither contribute nothing. Never
    raises — pass the result to ``resolve_input_version``. Reads vithe engine terminal.
    """
    watermarks = await _watermark_columns(engine)
    signals: list[InputVersion] = []
    for table in source_tables:
        snapshot = await _iceberg_snapshot(engine, table)
        if snapshot is not None:
            signals.append(InputVersion(snapshot, "iceberg_snapshot"))
            continue
        column = watermarks.get((table.source_id, table.schema_name, table.table_name))
        if column:
            value = await _table_watermark(engine, table, column)
            if value is not None:
                signals.append(InputVersion(value, "watermark"))
    return signals
