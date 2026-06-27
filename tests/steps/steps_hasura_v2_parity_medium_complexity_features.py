# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""Step implementations for REQ-218 — Cursor-based pagination (Hasura v2 parity),
REQ-219 — SSE Subscriptions via PostgreSQL LISTEN/NOTIFY,
REQ-220 — Database event triggers firing webhooks on table changes,
REQ-221 — Enum table auto-detection (pg_enum introspection → GraphQL enum types), and
REQ-222 — REST endpoint auto-generation for each root query field.

Cursor-based pagination adds `first`, `after`, `last`, `before` arguments to
root query fields. Results are returned as `edges[{cursor, node}]` with a
`pageInfo` object containing `hasNextPage`, `hasPreviousPage`, `startCursor`,
and `endCursor`. Cursors are base64-encoded sort key values. This feature
coexists with existing offset/limit pagination.

SSE Subscriptions expose a `GET /data/subscribe/<table>` endpoint that streams
INSERT/UPDATE/DELETE events to clients using FastAPI StreamingResponse backed by
PostgreSQL LISTEN/NOTIFY via asyncpg.

DB Event Triggers (REQ-220): table INSERT/UPDATE/DELETE changes fire HTTP POST
webhooks. PostgreSQL trigger + pg_notify() -> asyncpg listener -> HTTP POST to
configured URL. Config per table with operation filter and retry policy.

Enum Auto-Detection (REQ-221): introspect pg_enum at schema build time, generate
GraphQL enum types for columns using PostgreSQL user-defined enums, and map those
columns to GraphQL enum types instead of String.

REST Auto-Generation (REQ-222): for each root query field, generate
GET /data/rest/<table> FastAPI endpoint. Map query args to URL query params
(?limit=10&where.id.eq=1). Reuses GraphQL compilation pipeline internally.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import queue
import threading
import time
from typing import AsyncIterator
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import parse_qsl, urlsplit

import pytest
from graphql import (
    GraphQLEnumType,
    GraphQLEnumValue,
    GraphQLInputObjectType,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLScalarType,
    GraphQLString,
    parse,
    validate,
)
from graphql.language.ast import (
    ArgumentNode,
    FieldNode,
    IntValueNode,
    ObjectValueNode,
)
from pytest_bdd import given, parsers, scenarios, then, when

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import TableMeta, build_context, compile_query

scenarios("REQ-218.feature")
scenarios("REQ-219.feature")
scenarios("REQ-220.feature")
scenarios("REQ-221.feature")
scenarios("REQ-222.feature")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    """Plain dict used to pass state between Given/When/Then steps."""
    return {}


# ---------------------------------------------------------------------------
# Helpers (cursor pagination)
# ---------------------------------------------------------------------------


def _col(name: str, data_type: str = "varchar(100)", nullable: bool = False) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _build_schema_with_cursor_pagination():
    """Build a governed schema for an orders table with cursor pagination support."""
    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
                {"column_name": "created_at", "visible_to": ["admin"]},
            ],
        },
    ]
    column_types = {
        1: [
            _col("id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("region", "varchar(50)"),
            _col("created_at", "timestamp"),
        ],
    }
    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types=column_types,
        naming_rules=[],
        role={"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
        source_types={"sales-pg": "postgresql"},
    )
    return generate_schema(si), build_context(si)


def _encode_cursor(sort_key_values: dict) -> str:
    """Encode sort key values as a base64 cursor string."""
    raw = json.dumps(sort_key_values, sort_keys=True)
    return base64.b64encode(raw.encode("utf-8")).decode("utf-8")


def _decode_cursor(cursor: str) -> dict:
    """Decode a base64 cursor string back to sort key values."""
    raw = base64.b64decode(cursor.encode("utf-8")).decode("utf-8")
    return json.loads(raw)


def _simulate_cursor_query(
    rows: list[dict],
    first: int | None = None,
    after: str | None = None,
    last: int | None = None,
    before: str | None = None,
    sort_key: str = "id",
) -> dict:
    """Simulate cursor-based pagination over an in-memory row list."""
    working = list(rows)

    if after is not None:
        after_val = _decode_cursor(after)[sort_key]
        working = [r for r in working if r[sort_key] > after_val]

    if before is not None:
        before_val = _decode_cursor(before)[sort_key]
        working = [r for r in working if r[sort_key] < before_val]

    has_previous_page = False
    has_next_page = False

    if first is not None and last is not None:
        raise ValueError("Cannot specify both first and last")

    if first is not None:
        has_next_page = len(working) > first
        working = working[:first]

    if last is not None:
        has_previous_page = len(working) > last
        working = working[-last:]

    edges = [
        {"cursor": _encode_cursor({sort_key: row[sort_key]}), "node": row}
        for row in working
    ]

    start_cursor = edges[0]["cursor"] if edges else None
    end_cursor = edges[-1]["cursor"] if edges else None

    page_info = {
        "hasNextPage": has_next_page,
        "hasPreviousPage": has_previous_page,
        "startCursor": start_cursor,
        "endCursor": end_cursor,
    }

    return {"edges": edges, "pageInfo": page_info}


def _build_connection_graphql_query(
    table: str,
    first: int | None = None,
    after: str | None = None,
    last: int | None = None,
    before: str | None = None,
) -> str:
    """Build a GraphQL connection query string for cursor-based pagination."""
    args_parts = []
    if first is not None:
        args_parts.append(f"first: {first}")
    if after is not None:
        args_parts.append(f'after: "{after}"')
    if last is not None:
        args_parts.append(f"last: {last}")
    if before is not None:
        args_parts.append(f'before: "{before}"')

    args = f"({', '.join(args_parts)})" if args_parts else ""

    return f"""
    query {{
        {table}_connection{args} {{
            edges {{
                cursor
                node {{
                    id
                    amount
                    region
                }}
            }}
            pageInfo {{
                hasNextPage
                hasPreviousPage
                startCursor
                endCursor
            }}
        }}
    }}
    """


# ---------------------------------------------------------------------------
# Helpers (SSE subscriptions)
# ---------------------------------------------------------------------------

_SSE_EVENT_TYPES = ("INSERT", "UPDATE", "DELETE")


def _format_sse_event(event_type: str, table: str, data: dict) -> str:
    """Format a server-sent event message per the SSE specification."""
    payload = json.dumps({"event": event_type, "table": table, "data": data})
    return f"event: {event_type}\ndata: {payload}\n\n"


def _parse_sse_events(raw: str) -> list[dict]:
    """Parse raw SSE text into a list of event dicts with 'event' and 'data' keys."""
    events = []
    current: dict = {}
    for line in raw.splitlines():
        line = line.rstrip()
        if line.startswith("event:"):
            current["event"] = line[len("event:"):].strip()
        elif line.startswith("data:"):
            current["data"] = json.loads(line[len("data:"):].strip())
        elif line == "" and current:
            events.append(current)
            current = {}
    if current:
        events.append(current)
    return events


async def _simulate_sse_subscription(
    table: str,
    db_events: list[dict],
) -> list[str]:
    """Simulate the SSE event stream for a table subscription."""
    chunks: list[str] = []
    for db_event in db_events:
        event_type = db_event["event_type"]
        row = db_event["row"]
        chunk = _format_sse_event(event_type, table, row)
        chunks.append(chunk)
        await asyncio.sleep(0)
    return chunks


async def _mock_asyncpg_listen_notify(
    table: str,
    notifications: list[dict],
) -> AsyncIterator[str]:
    """Mock asyncpg LISTEN/NOTIFY: yield SSE-formatted events for each notification."""
    channel = f"provisa_{table}_changes"
    for notification in notifications:
        event_type = notification["event_type"]
        row = notification["row"]
        payload = json.dumps({"event": event_type, "table": table, "data": row})
        sse_chunk = f"event: {event_type}\ndata: {payload}\n\n"
        yield sse_chunk
        await asyncio.sleep(0)


class MockAsyncpgConnection:
    """Minimal asyncpg connection mock supporting add_listener / remove_listener."""

    def __init__(self, notifications: list[dict], table: str) -> None:
        self._notifications = notifications
        self._table = table
        self._listeners: dict[str, list] = {}
        self._fired = False

    async def add_listener(self, channel: str, callback) -> None:
        """Register a callback for the channel."""
        self._listeners.setdefault(channel, []).append(callback)

    async def remove_listener(self, channel: str, callback) -> None:
        """Remove a callback for the channel."""
        callbacks = self._listeners.get(channel, [])
        if callback in callbacks:
            callbacks.remove(callback)

    async def fire_notifications(self) -> None:
        """Fire all registered listeners with the pending notifications."""
        channel = f"provisa_{self._table}_changes"
        for notification in self._notifications:
            payload = json.dumps(notification)
            for callback in self._listeners.get(channel, []):
                await callback(self, 12345, channel, payload)
            await asyncio.sleep(0)

    async def execute(self, sql: str) -> None:
        """No-op execute for LISTEN SQL commands."""
        pass

    async def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Helpers (DB event triggers — REQ-220)
# ---------------------------------------------------------------------------


class EventTriggerConfig:
    """Configuration for a database event trigger (REQ-220).

    Mirrors what would be stored in provisa's metadata / config store.
    """

    def __init__(
        self,
        table: str,
        webhook_url: str,
        operations: list[str],
        retry_policy: dict | None = None,
    ) -> None:
        self.table = table
        self.webhook_url = webhook_url
        # Normalise to upper-case
        self.operations = [op.upper() for op in operations]
        self.retry_policy = retry_policy or {
            "max_retries": 3,
            "interval_seconds": 10,
            "timeout_seconds": 60,
        }

    def matches_operation(self, operation: str) -> bool:
        """Return True if the given operation is covered by this trigger's filter."""
        return operation.upper() in self.operations


class MockWebhookServer:
    """Captures HTTP POST requests that would be sent to a webhook endpoint."""

    def __init__(self) -> None:
        self.received: list[dict] = []

    async def handle(self, request_payload: dict) -> dict:
        """Record the payload and return a synthetic 200 response body."""
        self.received.append(request_payload)
        return {"status": "ok", "received": True}


async def _fire_webhook(
    webhook_url: str,
    payload: dict,
    retry_policy: dict,
    http_client: "MockHttpClient",
) -> dict:
    """Fire an HTTP POST to webhook_url with payload, respecting retry_policy.

    This is the core of the asyncpg-listener -> HTTP POST pipeline for REQ-220.
    The `http_client` argument abstracts httpx.AsyncClient so we can inject a
    mock without live network access.

    Returns the final response dict from the webhook server.
    """
    max_retries: int = retry_policy.get("max_retries", 3)
    timeout_seconds: int = retry_policy.get("timeout_seconds", 60)

    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            response = await http_client.post(webhook_url, json=payload, timeout=timeout_seconds)
            if response["status_code"] < 500:
                return response
            # 5xx — treat as retriable
            last_exc = RuntimeError(
                f"Webhook returned {response['status_code']} on attempt {attempt + 1}"
            )
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
        if attempt < max_retries:
            # In tests we do not actually sleep; the retry logic is still exercised
            await asyncio.sleep(0)

    raise RuntimeError(
        f"Webhook {webhook_url} failed after {max_retries + 1} attempts"
    ) from last_exc


class MockHttpClient:
    """Minimal mock that records calls to .post() and returns configurable responses."""

    def __init__(self, responses: list[dict] | None = None) -> None:
        # Each entry: {"status_code": int, "body": dict}
        # If exhausted, defaults to 200.
        self._responses = list(responses or [])
        self.calls: list[dict] = []

    async def post(self, url: str, *, json: dict, timeout: int = 60) -> dict:
        """Record the call and return the next configured response."""
        self.calls.append({"url": url, "payload": json, "timeout": timeout})
        if self._responses:
            return self._responses.pop(0)
        return {"status_code": 200, "body": {"status": "ok"}}


class MockAsyncpgEventTriggerConnection:
    """Asyncpg connection mock that fires pg_notify-style payloads for event triggers."""

    def __init__(self, table: str, trigger_config: EventTriggerConfig) -> None:
        self._table = table
        self._trigger_config = trigger_config
        self._listeners: dict[str, list] = {}

    async def add_listener(self, channel: str, callback) -> None:
        self._listeners.setdefault(channel, []).append(callback)

    async def remove_listener(self, channel: str, callback) -> None:
        callbacks = self._listeners.get(channel, [])
        if callback in callbacks:
            callbacks.remove(callback)

    async def execute(self, sql: str) -> None:
        pass

    async def close(self) -> None:
        pass

    async def simulate_table_change(
        self,
        operation: str,
        old_row: dict | None,
        new_row: dict | None,
    ) -> None:
        """Simulate a PostgreSQL trigger firing pg_notify for a table change.

        The channel name follows the convention:
            provisa_event_trigger_<table>
        The payload is a JSON object with operation, table, old, new.
        """
        channel = f"provisa_event_trigger_{self._table}"
        payload = json.dumps(
            {
                "operation": operation.upper(),
                "table": self._table,
                "old": old_row,
                "new": new_row,
            }
        )
        for callback in self._listeners.get(channel, []):
            await callback(self, 12345, channel, payload)
        await asyncio.sleep(0)


async def _run_event_trigger_pipeline(
    conn: MockAsyncpgEventTriggerConnection,
    trigger_config: EventTriggerConfig,
    table_changes: list[dict],
    http_client: MockHttpClient,
) -> list[dict]:
    """Wire the asyncpg listener to the webhook dispatcher and process table_changes.

    This replicates the production flow:
      1. Register asyncpg LISTEN on the event-trigger channel.
      2. For each notification, check the operation filter.
      3. If the operation matches, fire an HTTP POST to the webhook URL.
      4. Apply retry policy on failure.

    Returns the list of webhook payloads that were actually dispatched.
    """
    dispatched: list[dict] = []
    channel = f"provisa_event_trigger_{trigger_config.table}"

    async def _on_notify(connection, pid: int, ch: str, raw_payload: str) -> None:
        notification = json.loads(raw_payload)
        operation = notification.get("operation", "")
        if not trigger_config.matches_operation(operation):
            return
        webhook_payload = {
            "trigger": {
                "table": trigger_config.table,
                "operation": operation,
            },
            "event": {
                "op": operation,
                "data": {
                    "old": notification.get("old"),
                    "new": notification.get("new"),
                },
            },
        }
        await _fire_webhook(
            trigger_config.webhook_url,
            webhook_payload,
            trigger_config.retry_policy,
            http_client,
        )
        dispatched.append(webhook_payload)

    await conn.add_listener(channel, _on_notify)

    # Simulate each table change arriving via pg_notify
    for change in table_changes:
        operation = change["operation"]
        old_row = change.get("old")
        new_row = change.get("new")
        await conn.simulate_table_change(operation, old_row, new_row)

    await conn.remove_listener(channel, _on_notify)
    return dispatched


# ---------------------------------------------------------------------------
# Helpers (enum auto-detection — REQ-221)
# ---------------------------------------------------------------------------


# Represents a row from pg_enum / information_schema.columns introspection.
# In the real compiler, ColumnMetadata carries data_type which for a PG enum
# column is the UDT name (e.g. "order_status").  The schema builder then looks
# up the pg_enum rows to learn the valid values.

class PgEnumDefinition:
    """Models a PostgreSQL user-defined enum type (from pg_enum / pg_type)."""

    def __init__(self, type_name: str, values: list[str]) -> None:
        self.type_name = type_name          # e.g. "order_status"
        self.values = list(values)          # e.g. ["pending", "processing", "shipped"]

    def to_graphql_enum_type(self) -> GraphQLEnumType:
        """Build a GraphQLEnumType from this PG enum definition."""
        # GraphQL enum value names must be valid identifiers and by convention
        # are UPPER_SNAKE_CASE, but we preserve the PG casing to keep parity
        # with Hasura v2 which uses the literal pg_enum label.
        enum_values = {v: GraphQLEnumValue(v) for v in self.values}
        # Type name is PascalCase of the UDT name
        gql_type_name = _pg_type_name_to_graphql(self.type_name)
        return GraphQLEnumType(gql_type_name, enum_values)


def _pg_type_name_to_graphql(pg_type_name: str) -> str:
    """Convert a snake_case PG type name to PascalCase for GraphQL."""
    return "".join(part.capitalize() for part in pg_type_name.split("_"))


def _build_enum_aware_schema(
    pg_enums: list[PgEnumDefinition],
    enum_columns: dict[str, str],  # column_name -> pg_enum_type_name
) -> tuple[GraphQLObjectType, dict[str, GraphQLEnumType]]:
    """Build a GraphQL object type that maps enum columns to GraphQL enum types.

    This simulates what the Provisa schema compiler does during schema build
    when it introspects pg_enum and finds columns whose data_type matches a
    known UDT enum name.

    Args:
        pg_enums:      List of PG enum type definitions (from pg_enum).
        enum_columns:  Mapping of column_name -> pg_enum_type_name for the
                       columns in the table that use PG enum types.

    Returns:
        (GraphQLObjectType for the table, dict of enum type name -> GraphQLEnumType)
    """
    # Build a map from pg type name -> GraphQLEnumType
    enum_type_map: dict[str, GraphQLEnumType] = {}
    for pg_enum in pg_enums:
        gql_enum = pg_enum.to_graphql_enum_type()
        enum_type_map[pg_enum.type_name] = gql_enum

    # Build fields for the object type
    fields: dict = {}

    # Non-enum columns (always present in our test table)
    from graphql import GraphQLField, GraphQLInt, GraphQLNonNull as GNN
    fields["id"] = GraphQLField(GraphQLNonNull(GraphQLInt))  # type: ignore[arg-type]

    for col_name, pg_type_name in enum_columns.items():
        if pg_type_name in enum_type_map:
            fields[col_name] = GraphQLField(enum_type_map[pg_type_name])
        else:
            # Fallback: unknown type -> String (should not happen in well-formed input)
            fields[col_name] = GraphQLField(GraphQLString)

    obj_type = GraphQLObjectType("Order", lambda: fields)
    return obj_type, enum_type_map


def _simulate_pg_enum_introspection(
    pg_enums: list[PgEnumDefinition],
    column_metadata: list[ColumnMetadata],
) -> dict[str, str]:
    """Simulate compiler introspection: identify which columns use PG enum types.

    In production this compares column data_type against the set of UDT names
    known from pg_enum.  Returns a mapping of column_name -> pg_enum_type_name
    for columns whose data_type matches a known PG enum UDT name.

    Args:
        pg_enums:         All enum definitions discovered via pg_enum.
        column_metadata:  Column metadata rows for the table being compiled.

    Returns:
        dict mapping column_name -> pg_enum_type_name for enum columns.
    """
    known_enum_type_names = {e.type_name for e in pg_enums}
    result: dict[str, str] = {}
    for col in column_metadata:
        # data_type for a PG enum column is the UDT name (e.g. "order_status")
        if col.data_type.lower() in known_enum_type_names:
            result[col.column_name] = col.data_type.lower()
    return result


def _build_schema_with_pg_enums(
    pg_enums: list[PgEnumDefinition],
    table_columns: list[ColumnMetadata],
) -> tuple[GraphQLObjectType, dict[str, GraphQLEnumType], dict[str, str]]:
    """Full pipeline: introspect -> detect enum columns -> build GraphQL types.

    Returns:
        (table_object_type, enum_type_map, detected_enum_columns)
    """
    detected_enum_columns = _simulate_pg_enum_introspection(pg_enums, table_columns)
    obj_type, enum_type_map = _build_enum_aware_schema(pg_enums, detected_enum_columns)
    return obj_type, enum_type_map, detected_enum_columns


# ---------------------------------------------------------------------------
# Helpers (REST endpoint auto-generation — REQ-222)
# ---------------------------------------------------------------------------


def _build_rest_schema():
    """Build a governed schema for orders table used by REST endpoint tests."""
    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
            ],
        },
    ]
    column_types = {
        1: [
            _col("id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("region", "varchar(50)"),
        ],
    }
    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types=column_types,
        naming_rules=[],
        role={"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
        source_types={"sales-pg": "postgresql"},
    )
    return generate_schema(si), build_context(si)


def _parse_rest_query_params(query_string: str) -> dict:
    """Parse URL query string into a structured parameter dict.

    Handles dotted notation for where filters:
      ?limit=10&where.id.eq=1  ->  {"limit": 10, "where": {"id": {"eq": 1}}}
    """
    params: dict = {}
    pairs = parse_qsl(query_string, keep_blank_values=True)
    for key, value in pairs:
        if key == "limit":
            params["limit"] = int(value)
        elif key == "offset":
            params["offset"] = int(value)
        elif key == "order_by":
            params.setdefault("order_by", []).append(value)
        elif key.startswith("where."):
            # where.id.eq=1  ->  where: {id: {eq: 1}}
            parts = key.split(".", 2)
            if len(parts) == 3:
                _, col_name, operator = parts
                where_entry = params.setdefault("where", {})
                where_entry.setdefault(col_name, {})[operator] = _coerce_value(value)
        else:
            params[key] = value
    return params


def _coerce_value(value: str):
    """Coerce a string query param value to int or float if possible."""
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value


def _build_graphql_query_from_rest_params(
    table: str,
    params: dict,
    columns: list[str],
) -> str:
    """Translate REST query params into a GraphQL query string.

    This mirrors what the auto-generated REST endpoint does internally:
    it translates URL query parameters into a GraphQL query and routes
    it through the GraphQL compilation pipeline.

    Args:
        table:   The table name (corresponds to the root query field name).
        params:  Parsed query parameters from the URL.
        columns: Column names to select (defaults to all visible columns).

    Returns:
        A valid GraphQL query string.
    """
    args_parts: list[str] = []

    if "limit" in params:
        args_parts.append(f"limit: {params['limit']}")

    if "offset" in params:
        args_parts.append(f"offset: {params['offset']}")

    if "where" in params:
        where_clause = _build_graphql_where_clause(params["where"])
        args_parts.append(f"where: {where_clause}")

    if "order_by" in params:
        order_items = ", ".join(
            f"{{{item}}}" for item in params["order_by"]
        )
        args_parts.append(f"order_by: [{order_items}]")

    args = f"({', '.join(args_parts)})" if args_parts else ""
    fields = "\n".join(f"            {col}" for col in columns)

    return f"""
    query {{
        {table}{args} {{
{fields}
        }}
    }}
    """


def _build_graphql_where_clause(where: dict) -> str:
    """Convert a parsed where dict into a GraphQL where clause string.

    Example:
        {"id": {"eq": 1}} -> '{id: {_eq: 1}}'
    """
    conditions: list[str] = []
    for col_name, operators in where.items():
        for op, value in operators.items():
            # Map REST operator names to GraphQL convention (_eq, _gt, etc.)
            gql_op = f"_{op}" if not op.startswith("_") else op
            if isinstance(value, str):
                conditions.append(f"{col_name}: {{{gql_op}: \"{value}\"}}")
            else:
                conditions.append(f"{col_name}: {{{gql_op}: {value}}}")
    return "{" + ", ".join(conditions) + "}"


def _simulate_rest_endpoint(
    table: str,
    query_string: str,
    available_rows: list[dict],
    schema,
    ctx,
) -> dict:
    """Simulate the full REST endpoint processing pipeline.

    Models what GET /data/rest/<table>?... does:
    1. Parse URL query params.
    2. Translate to GraphQL query string.
    3. Validate the query against the compiled schema.
    4. Execute (simulated) via compile_query to produce SQL.
    5. Apply the SQL-equivalent filters to in-memory data.
    6. Return structured result.
    """
    params = _parse
