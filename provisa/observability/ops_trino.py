# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# complexity-gate: allow-ble=6 reason="best-effort Iceberg DDL; each except logs exc_info and is non-fatal (catalog-not-ready is retried), extracted verbatim from app.py"

"""Trino-specific provisioning of the ops (telemetry) domain (REQ-016).

Creates the Iceberg schema/tables/views in Trino for the ``otel.signals`` catalog
and evolves them idempotently. Only relevant when Trino is the bound federation
engine — native engines land telemetry in the dedicated ops store (see
``ops_schema``/``otlp2sql``), so this module is never imported on that path.
"""

from __future__ import annotations

import datetime
import logging

import trino

from provisa.observability.ops_schema import OPS_TABLES

# Postgres column types (ops_schema's source of truth) mapped to Trino/Iceberg types.
from provisa.compiler.type_map import OPS_PG_TO_PHYSICAL


def _ops_physical(pg_type: str) -> str:
    if pg_type not in OPS_PG_TO_PHYSICAL:
        raise ValueError(f"unmapped ops pg type: {pg_type}")
    return OPS_PG_TO_PHYSICAL[pg_type]


def seed_ops_trino(  # REQ-016
    trino_conn: trino.dbapi.Connection,
    ops_views: list[tuple[str, list[tuple[str, str, bool]], str]],
    snapshot_retention_hours: int | None = None,
) -> None:
    """Create Iceberg schema/tables/views in Trino for the ops domain (idempotent)."""
    _log = logging.getLogger(__name__)

    def _exec(ddl: str) -> None:
        cur = trino_conn.cursor()
        cur.execute(ddl)
        cur.fetchall()

    # Schema + physical tables — one exception aborts table creation (catalog not ready).
    _tables_ready = True
    try:
        _exec("CREATE SCHEMA IF NOT EXISTS otel.signals")
        for tbl_name, cols in OPS_TABLES.items():
            col_defs = [f'"{col_name}" {_ops_physical(pg_type)}' for col_name, pg_type, _ in cols]
            col_names_lower = {col_name.lower() for col_name, _, _ in cols}
            partition_cols = (
                ["'table_name'", "'_date'"] if "table_name" in col_names_lower else ["'_date'"]
            )
            _exec(
                f"CREATE TABLE IF NOT EXISTS otel.signals.{tbl_name} "
                f"({', '.join(col_defs)}) "
                f"WITH (partitioning = ARRAY[{', '.join(partition_cols)}], format = 'PARQUET')"
            )
    except Exception:
        _log.warning(
            "ops Iceberg DDL failed — will retry before next schema introspection", exc_info=True
        )
        _tables_ready = False

    if _tables_ready:
        # Column additions are non-fatal and isolated per table.
        for tbl_name, cols in OPS_TABLES.items():
            try:
                cur = trino_conn.cursor()
                cur.execute(f"SHOW COLUMNS FROM otel.signals.{tbl_name}")
                existing_cols = {row[0].lower() for row in cur.fetchall()}
                for col_name, pg_type, _ in cols:
                    if col_name.lower() not in existing_cols:
                        trino_type = _ops_physical(pg_type)
                        try:
                            _exec(
                                f'ALTER TABLE otel.signals.{tbl_name} ADD COLUMN "{col_name}" {trino_type}'
                            )
                            _log.info("ops Iceberg: added column %s.%s", tbl_name, col_name)
                        except Exception:
                            _log.warning(
                                "ops Iceberg: could not add column %s.%s",
                                tbl_name,
                                col_name,
                                exc_info=True,
                            )
            except Exception:
                _log.warning(
                    "ops Iceberg: could not inspect columns for %s", tbl_name, exc_info=True
                )

        # Evolve partition spec on existing tables to include table_name (non-destructive).
        for tbl_name in OPS_TABLES:
            try:
                _exec(f'ALTER TABLE otel.signals.{tbl_name} ADD PARTITION FIELD "table_name"')
            except trino.exceptions.Error:
                pass  # already present, unsupported, or Trino transient — best-effort, not fatal

        # Warm up Iceberg metadata: first query on a cold Iceberg table can take >60s;
        # running a zero-row scan here ensures metadata is loaded before user requests arrive.
        for tbl_name in OPS_TABLES:
            try:
                _exec(f"SELECT 1 FROM otel.signals.{tbl_name} LIMIT 0")
            except Exception:
                _log.warning(
                    "ops Iceberg: warm-up scan on %s failed (non-fatal)", tbl_name, exc_info=True
                )

    # Views — always attempted; independent of column-addition and table-creation failures.
    # If the initial DDL block failed because Trino wasn't ready, these will also fail (caught
    # individually). If tables already exist from a prior run, views are created/refreshed here.
    # Use DROP IF EXISTS + CREATE VIEW for broad Trino version compatibility
    # (CREATE OR REPLACE VIEW for Iceberg requires Trino 418+).
    for view_name, _, view_ddl in ops_views:
        try:
            _exec(f"DROP VIEW IF EXISTS otel.signals.{view_name}")
            clean_ddl = view_ddl.replace("CREATE OR REPLACE VIEW", "CREATE VIEW")
            _exec(clean_ddl)
        except Exception:
            _log.warning("ops view %s: create failed", view_name, exc_info=True)

    if not _tables_ready:
        return

    # Expire old Iceberg snapshots and orphan files when retention is configured.
    if snapshot_retention_hours is not None:
        threshold = (
            datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(hours=snapshot_retention_hours)
        ).strftime("%Y-%m-%d %H:%M:%S.000")
        for tbl_name in OPS_TABLES:
            for proc, arg in [
                ("expire_snapshots", f"retention_threshold => TIMESTAMP '{threshold}'"),
                ("remove_orphan_files", f"retention_threshold => TIMESTAMP '{threshold}'"),
            ]:
                try:
                    _exec(f"ALTER TABLE otel.signals.{tbl_name} EXECUTE {proc}({arg})")
                    _log.info(
                        "ops Iceberg: %s on %s (retention %dh)",
                        proc,
                        tbl_name,
                        snapshot_retention_hours,
                    )
                except Exception:
                    _log.warning(
                        "ops Iceberg: %s on %s failed (non-fatal)", proc, tbl_name, exc_info=True
                    )
