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

import json
import os
from datetime import datetime, timezone

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

# REQ-1910: `traces` holds ONE row per request — the request span, which carries the request's
# transport, role, org, route, engine, sources, cache result, row count, status and stage
# durations as attributes — plus one row per piece of background work (a refresh, a scheduler
# job: a span with no parent). Every other span is detail: a child span a debug-detail request
# produced, kept in `trace_details` under the same trace_id, with the same columns, for a short
# retention (TRACE_DETAIL_RETENTION_HOURS).
OPS_TABLES["trace_details"] = list(OPS_TABLES["traces"])

# REQ-862: the span each MV refresh records its lineage on (mv.refresh._emit_column_lineage_span).
MV_REFRESH_LINEAGE_SPAN = "mv.refresh.column_lineage"

# REQ-862: the refresh lineage records, one row a refresh, read out of their spans in `traces`.
# Written in PostgreSQL and transpiled to each engine's SQL where the engine defines the view
# (ops_trino.seed_ops_trino, the native attach walk). The attributes are read from the text
# column as it is, never cast: Trino's CAST(varchar AS JSON) makes a JSON string rather than
# parsing it, and json_extract_scalar parses varchar itself. tenant_id carries the org rule
# (compiler.rls.ORG_SCOPED_SIGNALS_RULE) every provisa-otel table is read under.
MV_REFRESH_LINEAGE_VIEW = "mv_refresh_lineage"
MV_REFRESH_LINEAGE_SQL = f"""SELECT
  span_attributes ->> 'mv.id' AS mv_id,
  span_attributes ->> 'mv.target_table' AS target_table,
  span_attributes ->> 'lineage.definition_version' AS definition_version,
  span_attributes ->> 'lineage.input_version' AS input_version,
  span_attributes ->> 'lineage.input_version_kind' AS input_version_kind,
  trace_id,
  "timestamp" AS refreshed_at,
  span_attributes ->> 'lineage.column_derivations' AS column_derivations,
  tenant_id
FROM otel.signals.traces
WHERE span_name = '{MV_REFRESH_LINEAGE_SPAN}'"""
MV_REFRESH_LINEAGE_COLUMNS: list[tuple[str, str, bool]] = [
    ("mv_id", "text", False),
    ("target_table", "text", False),
    ("definition_version", "text", False),
    ("input_version", "text", False),
    ("input_version_kind", "text", False),
    ("trace_id", "text", True),
    ("refreshed_at", "timestamp", False),
    ("column_derivations", "text", False),
    ("tenant_id", "text", False),
]

# The views each engine with the ops signals defines in otel.signals over them, each
# (view_name, [(column, logical_type, is_primary_key)], PostgreSQL SELECT).
OPS_SIGNAL_VIEWS: list[tuple[str, list[tuple[str, str, bool]], str]] = [
    (MV_REFRESH_LINEAGE_VIEW, MV_REFRESH_LINEAGE_COLUMNS, MV_REFRESH_LINEAGE_SQL),
]

# The span attribute that marks a request span (set by provisa.otel_compat.request_span and the
# HTTP server-span hook). A request span whose caller sent a traceparent has a parent, in another
# process — this attribute, not the absence of a parent, is what says it is the request record.
REQUEST_SPAN_ATTR = "provisa.transport"

# How long a `trace_details` row is kept. $OTLP2SQL_DETAIL_RETENTION_HOURS overrides it.
TRACE_DETAIL_RETENTION_HOURS = 24

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

# REQ-1428: the instrumentation scopes of the catalog database's driver, whose statement spans no
# lane keeps. REQ-1910: the catalog database is instrumented through psycopg now; asyncpg stays
# listed for the objects written while it was the instrumented driver.
CATALOG_DRIVER_SCOPES = (
    "opentelemetry.instrumentation.asyncpg",
    "opentelemetry.instrumentation.psycopg",
)


def _ns_to_dt(ns: int) -> datetime | None:
    """OTLP instant (nanoseconds since the epoch) as a UTC datetime.

    REQ-1435: the ops tables register these columns as timestamps, so the row carries the instant
    itself rather than its integer encoding. Microsecond is the resolution every store in the lane
    keeps (Iceberg TIMESTAMP(6), Postgres/DuckDB TIMESTAMP), so the sub-microsecond digits are
    dropped here rather than silently at the first write.
    """
    return (
        datetime.fromtimestamp(ns / 1_000_000_000, timezone.utc).replace(tzinfo=None)
        if ns
        else None
    )


def trace_row(
    *,
    trace_id: str,
    span_id: str,
    parent_span_id: str | None,
    name: str,
    kind: int,
    attributes: dict,
    resource_attributes: dict,
    scope_name: str | None,
    start_ns: int,
    end_ns: int,
    status_code: int,
    status_message: str | None,
) -> dict:
    """One span as a ``traces`` row (OPS_TABLES["traces"]): the one mapping every lane that lands
    spans uses -- otlp2sql from the OTLP wire, the native lane from the SDK's finished span -- so
    a span is the same row whichever wrote it."""
    ts = _ns_to_dt(start_ns)
    row = {
        "trace_id": trace_id,
        "span_id": span_id,
        "parent_span_id": parent_span_id or None,
        "span_name": name,
        "span_kind": kind,
        "service_name": resource_attributes.get("service.name"),
        "service_namespace": resource_attributes.get("service.namespace"),
        "timestamp": ts,
        "end_timestamp": _ns_to_dt(end_ns),
        # Elapsed nanoseconds, per the catalog description. Taken from the integers, not the
        # rounded datetimes above.
        "duration": end_ns - start_ns if (start_ns and end_ns) else None,
        "status_code": status_code,
        "status_message": status_message or None,
        "scope_name": scope_name,
        "span_attributes": json.dumps(attributes),
        "resource_attributes": json.dumps(resource_attributes),
        "table_name": None,
        "domain_id": None,
        "role_id": None,
        "query_text": None,
        "tenant_id": None,
        "_date": ts.date() if ts else None,
    }
    for attr_key, col in TRACE_ATTR_COLS.items():
        if attr_key in attributes:
            row[col] = attributes[attr_key]
    return row


def is_trace_record(row: dict) -> bool:
    """REQ-1910: whether a span row is a record of its own -- a request span, or the root span of
    background work -- rather than detail under one."""
    return row["parent_span_id"] is None or REQUEST_SPAN_ATTR in json.loads(row["span_attributes"])


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
