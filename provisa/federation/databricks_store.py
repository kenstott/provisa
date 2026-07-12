# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Databricks materialization-store write face (REQ-987).

Databricks is a self-only warehouse engine: materialized sources LAND into the warehouse itself,
through the engine's OWN connection (it cannot attach an external store). This module is the one place
that write happens — DDL (create schema/table) + a bulk multi-row ``INSERT`` (never a per-row loop),
so the Delta table lands columnar. ``change_signal`` selects replace (truncate + insert) vs append.

Row-oriented per-row INSERT is intentionally NOT a path here (REQ-987); the batch lands as one
multi-row ``INSERT … VALUES`` statement, which Databricks writes as a single columnar Delta commit.
A COPY-INTO-from-object-stage path (for very large batches) is a future addition behind this same
``land_databricks_native`` seam — callers never change.
"""

from __future__ import annotations

import json
from typing import Any

from provisa.core.change_signal import APPEND, select_landing_shape
from provisa.core.ir_types import to_ir

# Canonical IR name → Databricks/Delta SQL type. Delta has no unsigned/serial spellings; a landed
# replica carries the source's own key values, so integers/decimals map to their widest safe Delta
# type. JSON is stored as STRING (Databricks parses on read via from_json / : path access).
_IR_TO_DATABRICKS: dict[str, str] = {
    "smallint": "SMALLINT",
    "integer": "INT",
    "bigint": "BIGINT",
    "text": "STRING",
    "boolean": "BOOLEAN",
    "float": "FLOAT",
    "double": "DOUBLE",
    "numeric": "DECIMAL(38,9)",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "time": "STRING",  # Delta has no bare TIME type
    "uuid": "STRING",
    "bytea": "BINARY",
    "json": "STRING",
}


def _ddl_type(ir_type: str) -> str:
    """Databricks column type for a canonical IR type name — raises on an unknown type (never a
    silent widen), mirroring the store-DDL discipline of the other engines."""
    canonical = to_ir(ir_type)  # normalize any native/dialect spelling to the IR vocabulary
    sql_type = _IR_TO_DATABRICKS.get(canonical)
    if sql_type is None:
        raise ValueError(
            f"no Databricks type mapping for IR type {ir_type!r} (canonical {canonical!r})"
        )
    return sql_type


def _qualified(catalog: str, schema: str, table: str) -> str:
    return f"`{catalog}`.`{schema}`.`{table}`"


def _ensure_namespace(cur: Any, catalog: str, schema: str) -> None:
    """Create the Unity Catalog + schema that hold the landed replica (idempotent). A self-only
    Databricks engine lands each source into a UC catalog named for the source (matching the
    compiler's physical name ``source_id.schema.table``), so the governed query resolves natively."""
    cur.execute(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")


def _existing_columns(cur: Any, catalog: str, schema: str, table: str) -> list[str]:
    """The store table's current column names, or ``[]`` if it does not exist."""
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_catalog = ? AND table_schema = ? AND table_name = ? "
        "ORDER BY ordinal_position",
        [catalog, schema, table],
    )
    return [r[0] for r in cur.fetchall()]


def _create_ddl(catalog: str, schema: str, table: str, columns: list[tuple[str, str]]) -> str:
    cols = ", ".join(f"`{name}` {_ddl_type(ir_type)}" for name, ir_type in columns)
    return f"CREATE TABLE IF NOT EXISTS {_qualified(catalog, schema, table)} ({cols}) USING DELTA"


def reconcile_databricks_native(
    cur: Any,
    *,
    catalog: str,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
) -> str:
    """Converge the Databricks landing table to ``columns`` (DDL only, no data). Returns
    ``created`` | ``kept`` | ``recreated`` — an existing matching table survives; a drifted one is
    recreated (a config/schema change is authoritative; data re-lands on the next refresh)."""
    _ensure_namespace(cur, catalog, schema)
    have = _existing_columns(cur, catalog, schema, table)
    want = [name for name, _ in columns]
    if not have:
        cur.execute(_create_ddl(catalog, schema, table, columns))
        return "created"
    if have == want:
        return "kept"
    cur.execute(f"DROP TABLE IF EXISTS {_qualified(catalog, schema, table)}")
    cur.execute(_create_ddl(catalog, schema, table, columns))
    return "recreated"


def _coerce(value: Any, ir_type: str) -> Any:
    """JSON columns take the source's serialized text; a dict/list is re-serialized so the STRING
    column holds valid JSON text. Everything else passes through to the driver's bind."""
    if to_ir(ir_type) == "json" and value is not None and not isinstance(value, str):
        return json.dumps(value)
    return value


def land_databricks_native(
    cur: Any,
    *,
    catalog: str,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    rows: list[dict],
    change_signal: str = "ttl",
    watermark_column: str | None = None,
) -> str:
    """Land ``rows`` into the Databricks table through the engine's own cursor (REQ-987).

    Shape from ``change_signal`` (REQ-932): a poll signal with a watermark APPENDS the delta; every
    other batch REPLACES (``TRUNCATE`` + insert). Rows land as one bulk multi-row ``INSERT … VALUES``
    (a single columnar Delta commit), never a per-row loop. Returns the qualified name."""
    _ensure_namespace(cur, catalog, schema)
    cur.execute(_create_ddl(catalog, schema, table, columns))
    qualified = _qualified(catalog, schema, table)
    if select_landing_shape(change_signal, watermark_column) != APPEND:
        cur.execute(f"TRUNCATE TABLE {qualified}")
    if rows:
        colnames = [name for name, _ in columns]
        collist = ", ".join(f"`{cn}`" for cn in colnames)
        row_ph = "(" + ", ".join("?" * len(colnames)) + ")"
        placeholders = ", ".join([row_ph] * len(rows))
        params: list[Any] = []
        for r in rows:
            for name, ir_type in columns:
                params.append(_coerce(r.get(name), ir_type))
        cur.execute(f"INSERT INTO {qualified} ({collist}) VALUES {placeholders}", params)
    return qualified
