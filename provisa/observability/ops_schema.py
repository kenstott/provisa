# Copyright (c) 2026 Kenneth Stott
# Canary: adfd50dd-06b5-46a9-bf19-707d308bf063
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Shared ops-domain telemetry schema + SQL target resolution.

Single source of truth for the traces/metrics/logs tables that BOTH:
  - ``otlp2sql`` creates and writes to, and
  - the ops domain (``provisa-otel`` source) registers and queries.

Both resolve the same database via :func:`ops_db_url`, so telemetry lands
exactly where the ops domain reads it. ``OPS_TABLES`` is imported by
``provisa.api.app`` so the two never drift.
"""

from __future__ import annotations

import os

import sqlalchemy as sa

# Physical schema (namespace) the ops tables live in — matches otel.signals.* .
OPS_SCHEMA = "signals"

# (column, logical_type, is_primary_key). Logical types are mapped to SQLAlchemy
# in _TYPE_MAP below. This is the contract the whole ops domain depends on.
OPS_TABLES: dict[str, list[tuple[str, str, bool]]] = {
    "traces": [
        ("trace_id", "text", True),
        ("span_id", "text", False),
        ("parent_span_id", "text", False),
        ("span_name", "text", False),
        ("span_kind", "integer", False),
        ("service_name", "text", False),
        ("service_namespace", "text", False),
        ("timestamp", "timestamp", False),
        ("end_timestamp", "timestamp", False),
        ("duration", "bigint", False),
        ("status_code", "integer", False),
        ("status_message", "text", False),
        ("scope_name", "text", False),
        ("span_attributes", "text", False),
        ("resource_attributes", "text", False),
        # extracted from span_attributes at ingest (see TRACE_ATTR_COLS)
        ("table_name", "text", False),
        ("domain_id", "text", False),
        ("role_id", "text", False),
        ("query_text", "text", False),
        ("tenant_id", "text", False),
        ("_date", "date", False),
    ],
    "metrics": [
        ("timestamp", "timestamp", True),
        ("start_timestamp", "timestamp", False),
        ("metric_name", "text", False),
        ("metric_description", "text", False),
        ("metric_unit", "text", False),
        ("metric_type", "text", False),
        ("service_name", "text", False),
        ("service_namespace", "text", False),
        ("scope_name", "text", False),
        ("metric_attributes", "text", False),
        ("resource_attributes", "text", False),
        ("value", "float8", False),
        ("tenant_id", "text", False),
        ("_date", "date", False),
    ],
    "logs": [
        ("timestamp", "timestamp", True),
        ("observed_timestamp", "timestamp", False),
        ("trace_id", "text", False),
        ("span_id", "text", False),
        ("severity_number", "integer", False),
        ("severity_text", "text", False),
        ("body", "text", False),
        ("service_name", "text", False),
        ("service_namespace", "text", False),
        ("scope_name", "text", False),
        ("log_attributes", "text", False),
        ("resource_attributes", "text", False),
        ("tenant_id", "text", False),
        ("_date", "date", False),
    ],
}

# Span-attribute key -> trace column, extracted inline at ingest. Mirrors
# scheduler.jobs._TRACE_EXTRA_ATTRS so otlp2sql rows == the old compaction rows
# (tenant added; harmless when absent).
TRACE_ATTR_COLS: dict[str, str] = {
    "provisa.table": "table_name",
    "provisa.domain": "domain_id",
    "provisa.role": "role_id",
    "provisa.query_text": "query_text",
    "provisa.tenant": "tenant_id",
}

_TYPE_MAP = {
    "text": sa.Text,
    # REQ-1435: the OTLP wire carries every instant as an integer count of nanoseconds since the
    # epoch. Registered as bigint, every ops report rendered those instants as a 19-digit number,
    # which no reader can date and no grid can sort as time. The writers convert to a real instant
    # (otlp2sql builds a datetime; the compactor casts the parquet int64 to an arrow timestamp),
    # so the registered type is the instant, not its encoding.
    "timestamp": sa.DateTime,
    "integer": sa.Integer,
    "bigint": sa.BigInteger,
    "float8": sa.Float,
    "date": sa.Date,
    "boolean": sa.Boolean,
}


def build_metadata(schema: str | None = OPS_SCHEMA) -> tuple[sa.MetaData, dict[str, sa.Table]]:
    """SQLAlchemy MetaData + Tables for the ops schema (schema=None for engines
    without namespaces, e.g. sqlite).

    Telemetry tables are append-only: the ``is_key`` flag is a logical/partition
    hint, NOT a physical primary key — a real PK on a timestamp would (a)
    be wrong (many rows share a timestamp) and (b) make SQLAlchemy emit an
    autoincrement (BIGSERIAL) that DuckDB rejects.
    """
    md = sa.MetaData(schema=schema)
    tables: dict[str, sa.Table] = {}
    for name, cols in OPS_TABLES.items():
        tables[name] = sa.Table(
            name,
            md,
            *[sa.Column(col, _TYPE_MAP[typ]()) for col, typ, _key in cols],
        )
    return md, tables


def telemetry_dir() -> str:
    """Directory of the dedicated telemetry store. The single location that
    otlp2sql (duckdb file), otlp2parquet (parquet/ subdir), and the ops domain
    all point at. Override with ``PROVISA_TELEMETRY_DIR``."""
    d = os.environ.get("PROVISA_TELEMETRY_DIR")
    if not d:
        home = os.environ.get("PROVISA_HOME") or os.path.join(os.path.expanduser("~"), ".provisa")
        d = os.path.join(home, "telemetry")
    os.makedirs(d, exist_ok=True)
    return d


def telemetry_parquet_dir() -> str:
    """Where otlp2parquet lands parquet — a subdir of the same store, exposed to
    the ops domain as DuckDB views over the telemetry duckdb."""
    d = os.path.join(telemetry_dir(), "parquet")
    os.makedirs(d, exist_ok=True)
    return d


def _ops_config() -> dict:
    """The persisted platform config, for the ops-store URL fallback. Empty if unreadable
    (e.g. very early boot before a config file exists) — mirrors
    ``federation.engine._engine_config``."""
    try:
        from provisa.api.admin._config_io import read_config
    except ImportError:  # api layer not importable at very early boot (module-load ordering)
        return {}
    return read_config() or {}


def configured_ops_store_url() -> str | None:
    """The telemetry-store DSN: ``$PROVISA_OPS_DB_URL`` then the persisted
    ``ops_store_url`` config field. Mirrors
    ``federation.engine.configured_materialize_url``'s env-then-config precedence."""
    return os.environ.get("PROVISA_OPS_DB_URL") or _ops_config().get("ops_store_url")


def ops_db_url() -> str:
    """SQLAlchemy URL for the telemetry store — the single value shared by
    otlp2sql, the ops-domain source, and (via views) otlp2parquet output.

    Telemetry gets its OWN store, never the control-plane DB. Resolution order:
    ``$PROVISA_OPS_DB_URL`` / the persisted ``ops_store_url`` config field
    (:func:`configured_ops_store_url`), else the embedded default — a dedicated
    DuckDB under :func:`telemetry_dir`.
    """
    url = configured_ops_store_url()
    if url:
        return url
    return f"duckdb:///{os.path.join(telemetry_dir(), 'telemetry.duckdb')}"


def ensure_tables(engine: sa.Engine) -> dict[str, sa.Table]:
    """Create the ops schema (postgres) and the traces/metrics/logs tables,
    idempotently. Returns the Table objects for inserts."""
    use_schema = engine.dialect.name != "sqlite"
    schema = OPS_SCHEMA if use_schema else None
    md, tables = build_metadata(schema)
    with engine.begin() as conn:
        if use_schema:
            conn.execute(sa.schema.CreateSchema(OPS_SCHEMA, if_not_exists=True))
        md.create_all(conn)
    return tables
