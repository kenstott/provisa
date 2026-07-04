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

Both are queried through the same Trino connection the refresh already uses (the
config DB is the ``provisa_admin`` catalog, the Iceberg store is its own catalog),
so gathering needs no extra plumbing. It is best-effort telemetry: a source that is
neither Iceberg nor watermarked simply contributes nothing, and any query error is
logged and skipped — signal gathering never fails a refresh. ``resolve_input_version``
then picks the strongest signal, or the refresh epoch when there are none.
"""

from __future__ import annotations

import logging

from provisa.lineage import InputVersion

log = logging.getLogger(__name__)

# Registry of source-table watermark columns (REQ-260) lives in the config DB, which
# Trino exposes as the provisa_admin catalog.
_WATERMARK_LOOKUP_SQL = (
    "SELECT table_name, watermark_column "
    "FROM provisa_admin.public.registered_tables "
    "WHERE watermark_column IS NOT NULL"
)


def _base_name(table: str) -> str:
    """Bare table name from a possibly catalog/schema-qualified, possibly quoted ref."""
    return table.split(".")[-1].strip('"')


def _watermark_columns(trino_conn) -> dict[str, str]:
    """Map ``table_name -> watermark_column`` from the config registry. {} on failure."""
    try:
        cursor = trino_conn.cursor()
        cursor.execute(_WATERMARK_LOOKUP_SQL)
        return {row[0]: row[1] for row in cursor.fetchall() if row[0] and row[1]}
    except Exception as exc:  # noqa: BLE001 — best-effort; missing registry is not fatal
        log.debug("watermark-column lookup unavailable: %s", exc)
        return {}


def _iceberg_snapshot(trino_conn, table: str) -> str | None:
    """Latest committed Iceberg snapshot id for ``table``, or None if not Iceberg."""
    try:
        cursor = trino_conn.cursor()
        cursor.execute(
            f'SELECT snapshot_id FROM "{_base_name(table)}$snapshots" '
            "ORDER BY committed_at DESC LIMIT 1"
        )
        row = cursor.fetchone()
    except Exception as exc:  # noqa: BLE001 — non-Iceberg tables have no $snapshots
        log.debug("no iceberg snapshot for %s: %s", table, exc)
        return None
    return str(row[0]) if row and row[0] is not None else None


def _table_watermark(trino_conn, table: str, column: str) -> str | None:
    """``MAX(column)`` for ``table`` as an RDB watermark value, or None on failure."""
    try:
        cursor = trino_conn.cursor()
        cursor.execute(f'SELECT MAX("{column}") FROM "{_base_name(table)}"')
        row = cursor.fetchone()
    except Exception as exc:  # noqa: BLE001 — column/table may be unqueryable here
        log.debug("no watermark for %s.%s: %s", table, column, exc)
        return None
    return str(row[0]) if row and row[0] is not None else None


def gather_input_signals(trino_conn, source_tables: list[str]) -> list[InputVersion]:
    """Gather the strongest available input-version signal per source table (REQ-862).

    Prefers an Iceberg snapshot id; falls back to an RDB watermark when the source
    declares a watermark column. Sources offering neither contribute nothing. Never
    raises — pass the result to ``resolve_input_version``.
    """
    watermarks = _watermark_columns(trino_conn)
    signals: list[InputVersion] = []
    for table in source_tables:
        snapshot = _iceberg_snapshot(trino_conn, table)
        if snapshot is not None:
            signals.append(InputVersion(snapshot, "iceberg_snapshot"))
            continue
        column = watermarks.get(_base_name(table))
        if column:
            value = _table_watermark(trino_conn, table, column)
            if value is not None:
                signals.append(InputVersion(value, "watermark"))
    return signals
