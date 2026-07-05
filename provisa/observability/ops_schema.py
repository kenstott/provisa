# Copyright (c) 2026 Kenneth Stott
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
        ("timestamp", "bigint", False),
        ("end_timestamp", "bigint", False),
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
        ("timestamp", "bigint", True),
        ("start_timestamp", "bigint", False),
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
        ("timestamp", "bigint", True),
        ("observed_timestamp", "bigint", False),
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
    "integer": sa.Integer,
    "bigint": sa.BigInteger,
    "float8": sa.Float,
    "date": sa.Date,
    "boolean": sa.Boolean,
}


def build_metadata(schema: str | None = OPS_SCHEMA) -> tuple[sa.MetaData, dict[str, sa.Table]]:
    """SQLAlchemy MetaData + Tables for the ops schema (schema=None for engines
    without namespaces, e.g. sqlite)."""
    md = sa.MetaData(schema=schema)
    tables: dict[str, sa.Table] = {}
    for name, cols in OPS_TABLES.items():
        tables[name] = sa.Table(
            name,
            md,
            *[sa.Column(col, _TYPE_MAP[typ](), primary_key=key) for col, typ, key in cols],
        )
    return md, tables


def ops_db_url() -> str:
    """SQLAlchemy URL for the ops telemetry DB — the single value shared by
    otlp2sql and the ops-domain source registration.

    Override with ``PROVISA_OPS_DB_URL`` (e.g. ``duckdb:///~/.provisa/ops.duckdb``,
    ``sqlite:///~/.provisa/ops.sqlite``). Default: the platform postgres control
    plane — TCP, or a unix socket when ``PG_HOST`` is a path (bundled pgserver).
    """
    url = os.environ.get("PROVISA_OPS_DB_URL")
    if url:
        return url
    host = os.environ.get("PG_HOST", os.environ.get("POSTGRES_HOST", "postgres"))
    db = os.environ.get("PG_DATABASE", os.environ.get("PG_NAME", "provisa"))
    user = os.environ.get("PG_USER", "provisa")
    pw = os.environ.get("PG_PASSWORD", "provisa")
    if host.startswith("/"):  # unix socket (pgserver on the desktop)
        return f"postgresql+psycopg2://{user}:{pw}@/{db}?host={host}"
    port = os.environ.get("PG_PORT", "5432")
    return f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"


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
