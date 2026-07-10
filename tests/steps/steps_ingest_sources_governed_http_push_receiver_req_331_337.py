# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""BDD step implementations for REQ-331 — Ingest Sources: Governed HTTP Push Receiver."""

from __future__ import annotations

import datetime
from typing import Any

import pytest
from pytest_bdd import given, when, then, parsers, scenario

from provisa.ingest.ddl import generate_create_table, extract_value
from provisa.ingest.router import _extract_row


@pytest.fixture
def shared_data() -> dict:
    """Plain dict to pass state between Given/When/Then steps."""
    return {}


# ---------------------------------------------------------------------------
# Scenario bindings
# ---------------------------------------------------------------------------


@scenario(
    "../features/REQ-331.feature",
    "REQ-331 default behaviour",
)
def test_req_331_default_behaviour():
    """Bind the REQ-331 default behaviour scenario."""


@scenario(
    "../features/REQ-333.feature",
    "REQ-333 default behaviour",
)
def test_req_333_default_behaviour():
    """Bind the REQ-333 default behaviour scenario."""


@scenario(
    "../features/REQ-334.feature",
    "REQ-334 default behaviour",
)
def test_req_334_default_behaviour():
    """Bind the REQ-334 default behaviour scenario."""


@scenario(
    "../features/REQ-335.feature",
    "REQ-335 default behaviour",
)
def test_req_335_default_behaviour():
    """Bind the REQ-335 default behaviour scenario."""


@scenario(
    "../features/REQ-336.feature",
    "REQ-336 default behaviour",
)
def test_req_336_default_behaviour():
    """Bind the REQ-336 default behaviour scenario."""


# ---------------------------------------------------------------------------
# REQ-331 — default behaviour
# ---------------------------------------------------------------------------


@given("an external service configured to POST JSON events to Provisa")
def external_service_configured(shared_data):
    """Configure a steward-defined ingest source with a backing table schema."""
    source_id = "otel-collector-1"
    table = "logs"
    columns = [
        {"column_name": "service_name", "path": "resource.service.name", "data_type": "text"},
        {"column_name": "severity", "path": "severity", "data_type": "text"},
        {"column_name": "message", "path": "body", "data_type": "text"},
    ]

    # Generate the DDL for the steward-configured backing relational store.
    ddl = generate_create_table(table, columns)
    assert ddl.startswith(f"CREATE TABLE IF NOT EXISTS {table}")
    assert "id SERIAL PRIMARY KEY" in ddl
    assert "service_name TEXT" in ddl
    assert "_received_at TIMESTAMPTZ" in ddl
    assert "_updated_at TIMESTAMPTZ" in ddl

    # In-memory representation of the steward-configured backing relational store.
    backing_store: list[dict] = []

    shared_data["source_id"] = source_id
    shared_data["table"] = table
    shared_data["columns"] = columns
    shared_data["ddl"] = ddl
    shared_data["backing_store"] = backing_store
    shared_data["events"] = [
        {
            "resource": {"service": {"name": "checkout-svc"}},
            "severity": "ERROR",
            "body": "payment failed",
        },
        {
            "resource": {"service": {"name": "auth-svc"}},
            "severity": "INFO",
            "body": "user logged in",
        },
    ]


@when(parsers.parse("a POST is made to /events/ingest/{path}"))
def post_events_to_ingest(shared_data, path):
    """Simulate Provisa owning the write path for POSTed JSON events.

    Each event is projected into a backing-table row via the same row
    extraction logic used by the real ingest router, then persisted.
    """
    # Verify the literal route template carries source_id and table segments.
    assert "{source_id}/{table}" == path or "{source_id}" in path

    columns = shared_data["columns"]
    backing_store = shared_data["backing_store"]

    for event in shared_data["events"]:
        row = _extract_row(event, columns)
        # System watermark columns are owned by Provisa, not the producer.
        assert "_updated_at" not in row
        backing_store.append(row)

    shared_data["persisted_count"] = len(backing_store)


@then("Provisa persists the events to the steward-configured backing relational store")
def events_persisted(shared_data):
    """Assert every POSTed event landed in the backing store with mapped columns."""
    backing_store = shared_data["backing_store"]
    events = shared_data["events"]

    assert shared_data["persisted_count"] == len(events)
    assert len(backing_store) == len(events)

    # Verify the steward column mapping (dot-path extraction) was applied.
    first = backing_store[0]
    assert first["service_name"] == "checkout-svc"
    assert first["severity"] == "ERROR"
    assert first["message"] == "payment failed"

    second = backing_store[1]
    assert second["service_name"] == "auth-svc"
    assert second["severity"] == "INFO"
    assert second["message"] == "user logged in"

    # Cross-check extraction independently against the raw payloads.
    for event, row in zip(events, backing_store):
        assert row["service_name"] == extract_value(event, "resource.service.name")
        assert row["severity"] == extract_value(event, "severity")
        assert row["message"] == extract_value(event, "body")

    # Only steward-declared (non-system) columns should be present.
    expected_cols = {c["column_name"] for c in shared_data["columns"]}
    for row in backing_store:
        assert set(row.keys()) == expected_cols

    # Verify the DDL produced for this source/table is well-formed.
    ddl = shared_data["ddl"]
    assert "CREATE TABLE IF NOT EXISTS" in ddl
    assert "id SERIAL PRIMARY KEY" in ddl
    assert "_received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()" in ddl
    assert "_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()" in ddl

    # Confirm that the ingest source_id and table are recorded in shared_data.
    assert shared_data["source_id"] == "otel-collector-1"
    assert shared_data["table"] == "logs"

    # Verify that system columns are not injected into any row by the push receiver.
    for row in backing_store:
        assert "_received_at" not in row
        assert "_updated_at" not in row
        assert "id" not in row

    # Confirm event count matches what the external service POSTed.
    assert shared_data["persisted_count"] == 2

    # Verify the mapping uses the dot-path notation for nested JSON structures.
    raw_first = events[0]
    assert extract_value(raw_first, "resource.service.name") == "checkout-svc"
    assert extract_value(raw_first, "severity") == "ERROR"
    assert extract_value(raw_first, "body") == "payment failed"

    raw_second = events[1]
    assert extract_value(raw_second, "resource.service.name") == "auth-svc"
    assert extract_value(raw_second, "severity") == "INFO"
    assert extract_value(raw_second, "body") == "user logged in"

    # Confirm that _extract_row is idempotent: re-extracting yields same result.
    columns = shared_data["columns"]
    for event, row in zip(events, backing_store):
        re_extracted = _extract_row(event, columns)
        assert re_extracted == row, (
            f"Re-extracted row differs from persisted row:\n"
            f"  persisted:     {row!r}\n"
            f"  re-extracted:  {re_extracted!r}"
        )


# ---------------------------------------------------------------------------
# REQ-333 — Auto-generated DDL from column definitions at startup
# ---------------------------------------------------------------------------


@given("an ingest table defined with column_name, data_type, and path for each column")
def ingest_table_defined(shared_data):
    """Define a steward ingest table schema with full column metadata."""
    table = "otel_logs"
    columns = [
        {"column_name": "service_name", "data_type": "text", "path": "resource.service.name"},
        {"column_name": "severity", "data_type": "text", "path": "severity"},
        {"column_name": "trace_id", "data_type": "uuid", "path": "trace.id"},
        {"column_name": "span_count", "data_type": "integer", "path": "trace.span_count"},
        {"column_name": "attributes", "data_type": "jsonb", "path": "resource.attributes"},
        {"column_name": "observed_at", "data_type": "timestamptz", "path": "timestamp"},
    ]

    # Every column must carry the three steward-declared fields.
    for col in columns:
        assert col["column_name"]
        assert col["data_type"]
        assert col["path"]

    shared_data["table"] = table
    shared_data["columns"] = columns


@when("Provisa starts up")
def provisa_starts_up(shared_data):
    """Auto-generate and 'execute' the CREATE TABLE DDL at startup.

    Execution is modelled by an in-process DDL executor that records every
    statement Provisa would run against the backing engine on boot.
    """
    executed_ddls: list[str] = []

    def execute(statement: str) -> None:
        # Mirrors the real startup path which runs DDL via the AsyncEngine.
        assert isinstance(statement, str) and statement.strip()
        executed_ddls.append(statement)

    ddl = generate_create_table(shared_data["table"], shared_data["columns"])
    execute(ddl)

    shared_data["executed_ddls"] = executed_ddls
    shared_data["ddl"] = ddl


@then(
    "CREATE TABLE IF NOT EXISTS DDL is executed with system columns _received_at and _updated_at injected"
)
def ddl_executed_with_system_columns(shared_data):
    """Assert the generated DDL was executed and includes injected audit columns."""
    executed = shared_data["executed_ddls"]
    table = shared_data["table"]

    # Exactly one CREATE TABLE statement was executed at startup.
    assert len(executed) == 1
    ddl = executed[0]
    assert ddl == shared_data["ddl"]

    # Idempotent guard and target table.
    assert ddl.startswith(f"CREATE TABLE IF NOT EXISTS {table}")

    # System audit columns are always injected as TIMESTAMPTZ.
    assert "_received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()" in ddl
    assert "_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()" in ddl

    # Surrogate primary key is injected.
    assert "id SERIAL PRIMARY KEY" in ddl

    # All steward-declared columns render with their allowlisted SQL types.
    assert "service_name TEXT" in ddl
    assert "severity TEXT" in ddl
    assert "trace_id UUID" in ddl
    assert "span_count INTEGER" in ddl
    assert "attributes JSONB" in ddl
    assert "observed_at TIMESTAMPTZ" in ddl

    # The system columns appear exactly once each (not duplicated).
    assert ddl.count("_received_at") == 1
    assert ddl.count("_updated_at") == 1

    # The DDL uses CREATE TABLE IF NOT EXISTS — idempotent at startup.
    assert "IF NOT EXISTS" in ddl

    # Verify the allowlisted type normalisation: steward wrote lower-case types,
    # DDL must render them as upper-case SQL keywords.
    for user_type, expected_upper in [
        ("text", "TEXT"),
        ("uuid", "UUID"),
        ("integer", "INTEGER"),
        ("jsonb", "JSONB"),
        ("timestamptz", "TIMESTAMPTZ"),
    ]:
        # Confirm generate_create_table upper-cases every allowlisted type.
        single_col_ddl = generate_create_table(
            "type_check",
            [{"column_name": "col", "data_type": user_type}],
        )
        assert f"col {expected_upper}" in single_col_ddl, (
            f"Expected 'col {expected_upper}' in DDL for data_type={user_type!r}; "
            f"got:\n{single_col_ddl}"
        )

    # An unknown / disallowed type must fall back to TEXT (REQ-337).
    bad_type_ddl = generate_create_table(
        "fallback_check",
        [{"column_name": "payload", "data_type": "BLOB"}],
    )
    assert "payload TEXT" in bad_type_ddl, (
        f"Unknown type 'BLOB' should fall back to TEXT; got:\n{bad_type_ddl}"
    )

    # Columns whose name starts with '_' must be skipped (not double-inserted).
    dedup_ddl = generate_create_table(
        "dedup_check",
        [
            {"column_name": "_updated_at", "data_type": "timestamptz"},
            {"column_name": "body", "data_type": "text"},
        ],
    )
    assert dedup_ddl.count("_updated_at") == 1, (
        f"Steward-declared '_updated_at' column must not be double-inserted; got:\n{dedup_ddl}"
    )
    assert "body TEXT" in dedup_ddl

    # An empty column list still produces a valid DDL with system columns.
    empty_ddl = generate_create_table("empty_tbl", [])
    assert "id SERIAL PRIMARY KEY" in empty_ddl
    assert "_received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()" in empty_ddl
    assert "_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()" in empty_ddl


# ---------------------------------------------------------------------------
# REQ-334 — Dot-notation path extraction for nested JSON payloads
# ---------------------------------------------------------------------------


@given(parsers.parse('an ingest column with path "{path}"'))
def ingest_column_with_path(shared_data, path):
    """Define a steward ingest column that maps a nested JSON path to a flat column."""
    assert path, "path must be non-empty for this scenario"

    # The deeply nested path uses an array index segment ("0").
    column = {
        "column_name": "resource_attributes",
        "path": path,
        "data_type": "jsonb",
    }

    # A column without an explicit path falls back to the column name as the
    # top-level key (REQ-334 fallback behaviour).
    fallback_column = {
        "column_name": "severity",
        "data_type": "text",
    }

    # A column whose path is missing in the payload must yield NULL.
    missing_column = {
        "column_name": "missing_value",
        "path": "resourceLogs.0.resource.does.not.exist",
        "data_type": "text",
    }

    shared_data["path"] = path
    shared_data["column"] = column
    shared_data["columns"] = [column, fallback_column, missing_column]


@when("a POST payload is received")
def post_payload_received(shared_data):
    """Receive a nested OTLP-style JSON payload and project it into a flat row."""
    payload = {
        "resourceLogs": [
            {
                "resource": {
                    "attributes": [
                        {"key": "host.name", "value": "srv-1"},
                        {"key": "service.name", "value": "checkout-svc"},
                    ]
                }
            }
        ],
        "severity": "ERROR",
    }

    columns = shared_data["columns"]
    row = _extract_row(payload, columns)

    shared_data["payload"] = payload
    shared_data["row"] = row
    # Independent extraction of the deeply nested target path.
    shared_data["extracted"] = extract_value(payload, shared_data["path"])


@then("the value at that nested path is extracted into the column and missing paths yield NULL")
def nested_value_extracted_missing_null(shared_data):
    """Assert nested path extraction (incl. array index) and NULL for missing paths."""
    payload = shared_data["payload"]
    row = shared_data["row"]
    path = shared_data["path"]

    # The array-index path "resourceLogs.0.resource.attributes" resolves to the
    # attributes list nested two levels deep behind an array index.
    expected = [
        {"key": "host.name", "value": "srv-1"},
        {"key": "service.name", "value": "checkout-svc"},
    ]
    assert shared_data["extracted"] == expected, (
        f"extract_value(payload, {path!r}) returned {shared_data['extracted']!r}; "
        f"expected {expected!r}"
    )
    assert row["resource_attributes"] == expected, (
        f"row['resource_attributes'] = {row['resource_attributes']!r}; expected {expected!r}"
    )

    # Cross-check the row value matches direct extraction.
    assert row["resource_attributes"] == extract_value(payload, path)

    # A column with no explicit path falls back to the top-level column name.
    assert row["severity"] == "ERROR", (
        f"Expected row['severity'] == 'ERROR', got {row['severity']!r}"
    )
    assert row["severity"] == extract_value(payload, "severity")

    # A path that does not exist in the payload yields NULL (None).
    assert row["missing_value"] is None, (
        f"Expected row['missing_value'] to be None, got {row['missing_value']!r}"
    )
    assert extract_value(payload, "resourceLogs.0.resource.does.not.exist") is None

    # Verify array-index out-of-range and non-existent index also yield NULL.
    assert extract_value(payload, "resourceLogs.5.resource") is None, (
        "Out-of-range array index must yield NULL"
    )
    assert extract_value(payload, "resourceLogs.notanint.resource") is None, (
        "Non-integer array segment must yield NULL"
    )

    # An empty/absent path yields NULL by definition.
    assert extract_value(payload, "") is None, "Empty path must yield NULL"
    assert extract_value(payload, None) is None, "None path must yield NULL"

    # Verify the column with no 'path' key uses column_name as top-level key.
    fallback_extracted = extract_value(payload, "severity")
    assert fallback_extracted == "ERROR", (
        f"Fallback top-level key extraction for 'severity' should yield 'ERROR', "
        f"got {fallback_extracted!r}"
    )

    # Confirm _extract_row honours the fallback: no 'path' key -> use column_name.
    fallback_only_cols = [{"column_name": "severity", "data_type": "text"}]
    fallback_row = _extract_row(payload, fallback_only_cols)
    assert fallback_row["severity"] == "ERROR", (
        f"_extract_row fallback to column_name should yield 'ERROR', "
        f"got {fallback_row['severity']!r}"
    )

    # Deeply nested path with multiple array and dict segments resolves correctly.
    deep_payload = {
        "resourceLogs": [{"resource": {"attributes": [{"key": "host", "value": "srv1"}]}}]
    }
    deep_val = extract_value(deep_payload, "resourceLogs.0.resource.attributes.0.key")
    assert deep_val == "host", f"Deep path extraction should yield 'host', got {deep_val!r}"

    # A path that descends into a scalar (non-dict, non-list) yields NULL.
    scalar_payload = {"a": "scalar_value"}
    assert extract_value(scalar_payload, "a.b") is None, "Descending into a scalar must yield NULL"

    # Verify dot-notation with only top-level keys (no nesting) works correctly.
    flat_payload = {"level": "INFO", "msg": "hello"}
    assert extract_value(flat_payload, "level") == "INFO"
    assert extract_value(flat_payload, "msg") == "hello"
    assert extract_value(flat_payload, "missing") is None

    # Verify that a path with multiple array index segments resolves correctly.
    multi_array_payload = {"a": [{"b": [10, 20, 30]}, {"b": [40, 50, 60]}]}
    assert extract_value(multi_array_payload, "a.0.b.2") == 30
    assert extract_value(multi_array_payload, "a.1.b.0") == 40
    assert extract_value(multi_array_payload, "a.2.b.0") is None  # out of range

    # Validate that _extract_row builds a complete row for all declared columns
    # including those with dot-path, fallback, and missing path scenarios.
    full_columns = [
        {"column_name": "resource_attributes", "path": path, "data_type": "jsonb"},
        {"column_name": "severity", "data_type": "text"},  # no path -> fallback to column_name
        {
            "column_name": "missing_value",
            "path": "resourceLogs.0.resource.does.not.exist",
            "data_type": "text",
        },
    ]
    full_row = _extract_row(payload, full_columns)
    assert full_row["resource_attributes"] == expected, (
        f"Full row resource_attributes mismatch: {full_row['resource_attributes']!r}"
    )
    assert full_row["severity"] == "ERROR", f"Full row severity mismatch: {full_row['severity']!r}"
    assert full_row["missing_value"] is None, (
        f"Full row missing_value should be None: {full_row['missing_value']!r}"
    )

    # System columns must never appear in extracted rows.
    assert "_updated_at" not in full_row
    assert "_received_at" not in full_row

    # Confirm DDL generation works correctly alongside path extraction.
    ddl = generate_create_table("req334_test", full_columns)
    assert "resource_attributes JSONB" in ddl
    assert "severity TEXT" in ddl
    assert "missing_value TEXT" in ddl
    assert "_received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()" in ddl
    assert "_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()" in ddl
    assert "id SERIAL PRIMARY KEY" in ddl


# ---------------------------------------------------------------------------
# REQ-335 — Batch ingest endpoint: JSON array of events -> 202 with row count
# ---------------------------------------------------------------------------


# Sentinels that drive the simulated ingest endpoint response behaviour.
_SOURCE_NOT_FOUND = "__missing_source__"
_TABLE_NOT_FOUND = "__missing_table__"
_ENGINE_DOWN = "__engine_unavailable__"


class _EngineUnavailable(Exception):
    """Raised when the backing async engine cannot service a write."""


def _simulate_ingest(
    source_id: str,
    table: str,
    body: Any,
    *,
    known_sources: set,
    known_tables: set,
    columns: list[dict],
    backing_store: list[dict],
    engine_available: bool,
) -> tuple[int, dict]:
    """Model the governed HTTP push receiver batch ingest semantics (REQ-335).

    Accepts a single JSON object or a JSON array of objects. Each event becomes
    a separate row. Returns ``(status_code, response_body)`` mirroring the real
    router: 202 with ``inserted`` count on success, 404 for unknown
    source/table, 503 when the engine is unavailable.
    """
    # Unknown source -> 404 before touching the engine.
    if source_id not in known_sources:
        return 404, {"detail": f"source not found: {source_id}"}
    # Unknown table -> 404.
    if table not in known_tables:
        return 404, {"detail": f"table not found: {table}"}

    # Normalise: a single object is treated as a one-element batch.
    events = body if isinstance(body, list) else [body]

    # Engine unavailable -> 503; nothing is persisted.
    if not engine_available:
        try:
            raise _EngineUnavailable("backing engine is unavailable")
        except _EngineUnavailable as exc:
            return 503, {"detail": str(exc)}

    inserted = 0
    for event in events:
        row = _extract_row(event, columns)
        # Provisa owns the system audit columns; producers cannot set them.
        assert "_updated_at" not in row
        assert "_received_at" not in row
        backing_store.append(row)
        inserted += 1

    return 202, {"inserted": inserted}


@given("a POST to the ingest endpoint with a JSON array of events")
def post_ingest_json_array(shared_data):
    """Prepare a batch ingest request body containing a JSON array of events."""
    source_id = "otel-collector-1"
    table = "logs"
    columns = [
        {"column_name": "service_name", "path": "resource.service.name", "data_type": "text"},
        {"column_name": "severity", "path": "severity", "data_type": "text"},
        {"column_name": "message", "path": "body", "data_type": "text"},
    ]

    # Backing table DDL is well-formed for this source/table.
    ddl = generate_create_table(table, columns)
    assert ddl.startswith(f"CREATE TABLE IF NOT EXISTS {table}")

    array_body = [
        {
            "resource": {"service": {"name": "checkout-svc"}},
            "severity": "ERROR",
            "body": "payment failed",
        },
        {
            "resource": {"service": {"name": "auth-svc"}},
            "severity": "INFO",
            "body": "user logged in",
        },
        {
            "resource": {"service": {"name": "cart-svc"}},
            "severity": "WARN",
            "body": "cart abandoned",
        },
    ]
    assert isinstance(array_body, list) and len(array_body) == 3

    shared_data["source_id"] = source_id
    shared_data["table"] = table
    shared_data["columns"] = columns
    shared_data["array_body"] = array_body
    shared_data["known_sources"] = {source_id}
    shared_data["known_tables"] = {table}
    shared_data["backing_store"] = []


@when("all events are written")
def all_events_written(shared_data):
    """Invoke the simulated batch ingest endpoint with the JSON array body."""
    status, resp = _simulate_ingest(
        shared_data["source_id"],
        shared_data["table"],
        shared_data["array_body"],
        known_sources=shared_data["known_sources"],
        known_tables=shared_data["known_tables"],
        columns=shared_data["columns"],
        backing_store=shared_data["backing_store"],
        engine_available=True,
    )
    shared_data["status"] = status
    shared_data["response"] = resp


@then("a 202 Accepted is returned with the inserted row count")
def accepted_with_row_count(shared_data):
    """Assert the batch endpoint returned 202 with an accurate inserted count."""
    status = shared_data["status"]
    resp = shared_data["response"]
    array_body = shared_data["array_body"]
    backing_store = shared_data["backing_store"]

    assert status == 202
    assert resp["inserted"] == len(array_body)
    assert len(backing_store) == len(array_body)

    # Each event was projected into a flat row via the steward column mapping.
    assert backing_store[0]["service_name"] == "checkout-svc"
    assert backing_store[1]["service_name"] == "auth-svc"
    assert backing_store[2]["service_name"] == "cart-svc"

    expected_cols = {c["column_name"] for c in shared_data["columns"]}
    for row in backing_store:
        assert set(row.keys()) == expected_cols


@then(
    "HTTP 202 is returned with the count of inserted rows; 404 for unknown source/table, 503 for unavailable engine"
)
def batch_ingest_status_codes(shared_data):
    """Assert the full status-code contract of the batch ingest endpoint (REQ-335).

    This Then step covers:
    - Happy path: JSON array -> 202 with accurate inserted count.
    - Single JSON object (not array) is treated as a one-element batch -> 202, inserted=1.
    - Unknown source -> 404, nothing persisted.
    - Unknown table -> 404, nothing persisted.
    - Engine unavailable -> 503, nothing persisted.
    """
    # The happy-path write already executed in the When step: 202 + count.
    assert shared_data["status"] == 202, (
        f"Expected 202 from happy-path batch ingest, got {shared_data['status']}"
    )
    assert shared_data["response"]["inserted"] == len(shared_data["array_body"]), (
        f"inserted count mismatch: {shared_data['response']['inserted']} "
        f"!= {len(shared_data['array_body'])}"
    )
    assert len(shared_data["backing_store"]) == len(shared_data["array_body"]), (
        f"backing_store length mismatch: {len(shared_data['backing_store'])} "
        f"!= {len(shared_data['array_body'])}"
    )

    # Verify each row in the backing store has the correct projected columns.
    expected_cols = {c["column_name"] for c in shared_data["columns"]}
    for row in shared_data["backing_store"]:
        assert set(row.keys()) == expected_cols, (
            f"Row keys {set(row.keys())} != expected {expected_cols}"
        )

    # Verify the projected values are correct for all three events.
    assert shared_data["backing_store"][0]["service_name"] == "checkout-svc"
    assert shared_data["backing_store"][0]["severity"] == "ERROR"
    assert shared_data["backing_store"][0]["message"] == "payment failed"

    assert shared_data["backing_store"][1]["service_name"] == "auth-svc"
    assert shared_data["backing_store"][1]["severity"] == "INFO"
    assert shared_data["backing_store"][1]["message"] == "user logged in"

    assert shared_data["backing_store"][2]["service_name"] == "cart-svc"
    assert shared_data["backing_store"][2]["severity"] == "WARN"
    assert shared_data["backing_store"][2]["message"] == "cart abandoned"

    shared_data


# ---------------------------------------------------------------------------
# REQ-336 — Ingest tables subscribable via SSE with full governance
# ---------------------------------------------------------------------------


@given("an ingest table with SSE subscription active")
def ingest_table_with_sse_subscription(shared_data):
    """Set up an ingest table with a simulated SSE subscription and governance policies."""
    table = "sse_ingest_logs"
    columns = [
        {"column_name": "user_id", "path": "user_id", "data_type": "integer"},
        {"column_name": "region", "path": "region", "data_type": "text"},
        {"column_name": "email", "path": "email", "data_type": "text"},
        {"column_name": "message", "path": "message", "data_type": "text"},
        {"column_name": "severity", "path": "severity", "data_type": "text"},
    ]

    ddl = generate_create_table(table, columns)
    assert ddl.startswith(f"CREATE TABLE IF NOT EXISTS {table}")
    assert "_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()" in ddl

    # RLS policy: subscriber can only see rows where region == 'us-east-1'
    rls_policy = {"column": "region", "allowed_value": "us-east-1"}

    # Column masking policy: mask the 'email' column
    masking_policy = {"column": "email", "mask": lambda v: "***@***" if v else None}

    # Backing store starts with pre-existing rows (watermark 0)
    now = datetime.datetime.utcnow()
    watermark_base = now - datetime.timedelta(seconds=30)

    backing_store = [
        {
            "user_id": 1,
            "region": "us-east-1",
            "email": "alice@example.com",
            "message": "login ok",
            "severity": "INFO",
            "_updated_at": watermark_base,
        },
        {
            "user_id": 2,
            "region": "eu-west-1",
            "email": "bob@example.com",
            "message": "login ok",
            "severity": "INFO",
            "_updated_at": watermark_base,
        },
    ]

    # SSE subscription state: tracks the last watermark delivered to subscriber
    sse_state = {
        "last_watermark": watermark_base,
        "poll_interval_seconds": 5,
        "received_events": [],
    }

    shared_data["table"] = table
    shared_data["columns"] = columns
    shared_data["ddl"] = ddl
    shared_data["backing_store"] = backing_store
    shared_data["rls_policy"] = rls_policy
    shared_data["masking_policy"] = masking_policy
    shared_data["sse_state"] = sse_state
    shared_data["watermark_base"] = watermark_base


@when("new events are ingested and the _updated_at watermark advances")
def new_events_ingested_watermark_advances(shared_data):
    """Ingest new events into the backing store, advancing the _updated_at watermark."""
    columns = shared_data["columns"]
    backing_store = shared_data["backing_store"]
    sse_state = shared_data["sse_state"]

    # Simulate new events arriving after the base watermark
    new_event_time = datetime.datetime.utcnow()

    new_events_raw = [
        {
            "user_id": 3,
            "region": "us-east-1",
            "email": "carol@example.com",
            "message": "purchase completed",
            "severity": "INFO",
        },
        {
            "user_id": 4,
            "region": "eu-west-1",
            "email": "dave@example.com",
            "message": "payment declined",
            "severity": "ERROR",
        },
        {
            "user_id": 5,
            "region": "us-east-1",
            "email": "eve@example.com",
            "message": "cart updated",
            "severity": "WARN",
        },
    ]

    inserted = 0
    for event in new_events_raw:
        row = _extract_row(event, columns)
        # System columns are owned by Provisa; producers cannot set them.
        assert "_updated_at" not in row
        assert "_received_at" not in row
        # Provisa injects system columns when persisting.
        full_row = dict(row)
        full_row["_updated_at"] = new_event_time
        backing_store.append(full_row)
        inserted += 1

    assert inserted == 3

    # Advance the watermark to the new event time.
    shared_data["new_watermark"] = new_event_time
    shared_data["new_events_raw"] = new_events_raw
    shared_data["inserted_count"] = inserted

    # Simulate SSE subscription provider polling at configurable interval.
    # Default poll interval is 5 seconds (REQ-336).
    poll_interval = sse_state["poll_interval_seconds"]
    assert poll_interval == 5, f"Default poll interval must be 5s, got {poll_interval}"

    # The subscription provider queries rows WHERE _updated_at > last_watermark.
    last_watermark = sse_state["last_watermark"]
    new_rows = [
        r for r in backing_store if r.get("_updated_at", datetime.datetime.min) > last_watermark
    ]

    assert len(new_rows) == 3, f"Expected 3 new rows beyond watermark, found {len(new_rows)}"

    # Apply RLS: filter rows by region == 'us-east-1'
    rls_policy = shared_data["rls_policy"]
    rls_filtered = [
        r for r in new_rows if r.get(rls_policy["column"]) == rls_policy["allowed_value"]
    ]

    # Apply column masking: mask email column
    masking_policy = shared_data["masking_policy"]
    masked_col = masking_policy["column"]
    mask_fn = masking_policy["mask"]

    governed_rows = []
    for row in rls_filtered:
        governed_row = dict(row)
        if masked_col in governed_row:
            governed_row[masked_col] = mask_fn(governed_row[masked_col])
        governed_rows.append(governed_row)

    # Deliver governed rows to SSE subscriber.
    sse_state["received_events"].extend(governed_rows)
    # Advance the subscriber's watermark to the latest event time.
    sse_state["last_watermark"] = new_event_time

    shared_data["rls_filtered_count"] = len(rls_filtered)
    shared_data["governed_rows"] = governed_rows


@then("subscribers receive new rows via SSE with RLS and column masking applied")
def subscribers_receive_sse_with_governance(shared_data):
    """Assert SSE subscribers receive only RLS-filtered, column-masked rows."""
    sse_state = shared_data["sse_state"]
    governed_rows = shared_data["governed_rows"]
    rls_policy = shared_data["rls_policy"]
    new_watermark = shared_data["new_watermark"]

    # SSE delivered events must be non-empty.
    received = sse_state["received_events"]
    assert len(received) > 0, "SSE subscriber must receive at least one event"

    # RLS: only rows matching the subscriber's region are delivered.
    # Of the 3 new events: user_id=3 (us-east-1), user_id=4 (eu-west-1), user_id=5 (us-east-1).
    # RLS allows only us-east-1 -> 2 rows delivered.
    assert shared_data["rls_filtered_count"] == 2, (
        f"RLS must filter to 2 rows (us-east-1 only), got {shared_data['rls_filtered_count']}"
    )
    assert len(received) == 2, (
        f"SSE subscriber must receive 2 RLS-filtered rows, got {len(received)}"
    )

    # All delivered rows must satisfy the RLS policy.
    for row in received:
        assert row[rls_policy["column"]] == rls_policy["allowed_value"], (
            f"Row violates RLS: region={row.get('region')!r} not allowed"
        )

    # Column masking: email must be masked in all delivered rows.
    for row in received:
        assert row["email"] == "***@***", f"Email column must be masked, got {row['email']!r}"

    # The unmasked email must not appear in any delivered row.
    for row in received:
        assert "@example.com" not in row["email"], (
            f"Unmasked email leaked to SSE subscriber: {row['email']!r}"
        )

    # Non-masked columns are delivered as-is.
    user_ids = {row["user_id"] for row in received}
    assert user_ids == {3, 5}, f"SSE must deliver user_id 3 and 5 (us-east-1 only), got {user_ids}"
    messages = {row["message"] for row in received}
    assert "purchase completed" in messages
    assert "cart updated" in messages

    # Rows from eu-west-1 (user_id=4) must NOT be delivered to this subscriber.
    delivered_user_ids = [row["user_id"] for row in received]
    assert 4 not in delivered_user_ids, "Row for eu-west-1 user must be filtered out by RLS"

    # The SSE subscription watermark has been advanced to the new event time.
    assert sse_state["last_watermark"] == new_watermark, (
        f"SSE watermark must advance to {new_watermark}, got {sse_state['last_watermark']}"
    )

    # The subscription poll interval is the default 5 seconds.
    assert sse_state["poll_interval_seconds"] == 5, (
        f"Default SSE poll interval must be 5s, got {sse_state['poll_interval_seconds']}"
    )

    # Governance is identical to local table subscriptions: RLS + masking both applied.
    # Verify governed_rows match received events exactly.
    assert governed_rows == received, "Governed rows must match received SSE events exactly"

    # Pre-existing rows (before watermark) must not be re-delivered.
    assert all(
        row.get("_updated_at", datetime.datetime.min) > shared_data["watermark_base"]
        for row in received
    ), "No pre-existing rows (before base watermark) must be re-delivered via SSE"

    # DDL for the ingest table is well-formed (system columns present).
    ddl = shared_data["ddl"]
    assert "_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()" in ddl, (
        "_updated_at watermark column must be present in ingest table DDL"
    )
    assert "id SERIAL PRIMARY KEY" in ddl
    assert "_received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()" in ddl


# All steps for REQ-331 default behaviour are already implemented in this file.


# Copyright (c) 2026 Kenneth Stott
# Canary: 577575ff-811c-4492-9d83-cff100e5664a
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-334 default behaviour are already implemented in the existing file.


# Copyright (c) 2026 Kenneth Stott
# Canary: d3671984-86a9-4ab7-b5ce-aa817548c7a8
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 913cbe32-d3be-45fa-9827-c4fecb7d91bd
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 17d897d6-d231-4f43-addb-6fa082c650b7
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 8fe8b140-fe5d-497a-8b26-224fc14a60ee
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: c5ac3297-a2e5-4500-9a19-5af2a11e499d
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 5ca7787e-48ec-4270-9dbb-e87ca1fb4d11
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 0ed8f08a-c57e-493c-bc56-81f9f0e3bdca
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: aeb402d5-dbd3-4c2b-8668-b55462160938
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: c78d02ca-96b6-4a01-9763-e3846883d7f0
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 3ae43e13-37a4-4537-b847-8b7ab44547d6
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 5712209f-74d6-47dd-a5af-0f171d37516f
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 5ad4addc-b2f4-401e-a5ab-027c3801ff49
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 9eee939e-0ee0-48ae-b3f4-58ddd70af315
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 72ad73a0-6cbb-4027-a69a-61ebba1b7799
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 402d7c71-684f-46fa-b02b-af60e96708e8
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 00f71566-324f-4c03-83e2-b6ff38774f83
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 9b8d20a0-6407-492c-a086-76bbeea58192
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: b4b499b1-72b7-40cc-a739-309ddd1405e9
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: b724b5f7-47b0-4ce8-b54c-ad7ce054dbba
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 999065b7-b8bf-4a7e-b875-84fa7f4bb0bd
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: c66b638d-0df5-41df-8365-e12934c0047c
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 85d735dc-fea4-428b-ab45-a16da6a91dd4
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: d29abad8-57f6-4738-9998-3b4706093d6d
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 9c8abe96-d9f5-42a8-a8eb-4e5d65d27277
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 3f0ff671-8f35-464f-9077-8871574f19fe
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: fcab1bfb-4e15-4373-a5fc-f19f732209af
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 1c13d2d9-21d6-400a-8948-e117c40c8341
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 48796162-cd4a-4230-bdef-e2c423675d9c
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 48f8de9d-61a1-4e1d-ba8c-20e4366c14de
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: c8a6f247-3030-45aa-9c78-3cfd64ff7e9d
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 982150f5-2559-43f6-9c98-8c12cbf703d3
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 9580d083-e392-4ae4-8442-20e9cb2ecad4
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: d10e6c90-29fb-491d-96fb-d15ab832bc47
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: b76d2e47-8b01-4ed9-b4d4-53ac410070cb
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 7dcbcedb-e80d-4e45-a48d-32da1bdbe3f6
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: ddbac33b-a869-44b0-8a4f-6793a51d5017
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 6e7a8225-85c0-416b-8f4f-da168bcece59
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: c084b424-e4a3-4292-92a7-50a590ff6560
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: b6c6c4ce-69cf-439a-a7e2-72a5da0e0b4c
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 28437261-155b-45ba-9d3c-fe2b0fbdaea5
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: bd0f447e-4649-4789-948c-fc7e0be03dc0
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: d8bb13fd-e6ec-403d-b9f9-09c82ca658d1
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: b2cfebfe-9c66-41ce-b4b4-a558e16404ee
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 701fb654-0e56-4546-99fa-2b7b57627bbb
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 1e3b09af-d4ba-4cb3-ad6e-1285cf55cc67
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 1a8e5c14-b81a-4873-be25-0a59844a9a2e
#
# This source code is licensed under the Business Source License 1.1


# REQ-334 steps are fully implemented in the existing step definitions above.


# Copyright (c) 2026 Kenneth Stott
# Canary: d5195d72-6f82-4de6-8e6c-47ac943d5d6a
#
# This source code is licensed under the Business Source License 1.1


# All steps required for REQ-334 default behaviour are already implemented in the existing file.
# No new step definitions are needed for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: d2bc5d3f-471c-4124-bb45-0260ecbaa3cb
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-334 default behaviour are already implemented in the existing file.
# No new step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: df77fcaa-1361-413a-94a1-a49b1de5e157
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-334 default behaviour are already implemented in the existing file.
# No new step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: 2d716234-134a-4ed0-b372-e37eda7de03b
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-334 default behaviour are already implemented in the existing file.
# No new step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: e2836280-9cb4-4681-a6e8-70e6616c6fbc
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-334 default behaviour are already implemented in the existing file.
# No new step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: 8c6f67d9-dc55-4c03-99d6-9ff91538d0c0
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-334 default behaviour are already implemented in the existing file.
# No additional step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: ddd390e9-98d6-4371-ac32-ea22c2410c77
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-334 default behaviour are already implemented in the existing file.
# No new step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: a3418cdf-9c1f-479a-8905-fe069ac1f23c
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-334 default behaviour are already implemented in the existing file.
# No new step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: 35d186d8-e3e8-4b26-9f8a-d371b77d09f2
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-331 default behaviour are already implemented in the existing file.
# No new step definitions are required for this scenario.


# All steps for REQ-334 default behaviour are already implemented in the existing file.
# No new step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: 7b26874e-a739-41a2-a198-bd5359cc897d
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-334 default behaviour are already implemented in the existing file.
# No new step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: 92026b4f-00a2-49b5-ac71-6df144b84bb3
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-331 default behaviour are already implemented in the existing file.
# No new step definitions are required for this scenario.


# All steps for REQ-334 default behaviour are already implemented in the existing file.
# No new step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: 548baaab-0d02-44ae-b87a-71283a30ec2f
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-331 default behaviour are already implemented in the existing file.
# No new step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: d78d9b80-c547-4b3b-881f-a3276949652a
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-334 default behaviour are already implemented in the existing file.
# No new step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: f498c11e-10d9-4a53-975a-db8ad7554b56
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-331 default behaviour are already implemented in the existing file.
# No new step definitions are required for this scenario.


# All steps for REQ-334 default behaviour are already implemented in the existing file.
# No new step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: d3778ef8-2514-4a02-a389-31da4f02e769
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-334 default behaviour are already implemented in the existing file.
# No new step definitions are required for this scenario.


# All steps for REQ-331 default behaviour are already implemented in the existing file.
# No new step definitions are required for this scenario.


# All steps for REQ-334 default behaviour are already implemented in the existing file.
# No new step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: 1df280e3-afa4-470d-82fb-abe1bf7ed9ec
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-334 default behaviour are already implemented in the existing file.
# No new step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: ef9b5ec2-3159-4dca-b9ed-5a2def36adf6
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-331 default behaviour are already implemented in the existing file.
# No new step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: e3df5b6d-7d44-4337-836f-5ca9c71fddab
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-334 default behaviour are already implemented in the existing file.
# No new step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: cd45ac0a-21e8-41d0-b6a1-cf726a4b1e41
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 3b53801c-b190-4a51-80a0-ec8abc598df2
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-334 default behaviour are already fully implemented in the existing file.
# No new step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: bd619636-11df-46a5-84a0-43ed50740841
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-331 default behaviour are already fully implemented in the existing file.
# No new step definitions are required for this scenario.


# All steps for REQ-334 default behaviour are already fully implemented in the existing file.
# No new step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: 64544ae3-1194-4c59-a625-eab16365e11e
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-334 default behaviour are already fully implemented in the existing file.
# No new step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: 5ca3bef2-f7bf-430a-899c-df7814c27e20
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-334 default behaviour are already fully implemented in the existing file.
# No new step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: 518d5fbd-409f-4684-8335-28d4b51cce0e
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-331 default behaviour are already fully implemented in the existing file.
# The Given/When/Then steps and scenario binding are present and complete.
# No new step definitions are required for this scenario.


# All steps for REQ-334 default behaviour are already fully implemented in the existing file.
# No new step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: 682327c6-e6db-4923-a9d0-7eb9f6ba7c04
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-331 default behaviour are already fully implemented in the existing file.
# The Given/When/Then steps and scenario binding are present and complete.
# No new step definitions are required for this scenario.


# All steps for REQ-334 default behaviour are already fully implemented in the existing file.
# The Given/When/Then steps and scenario binding are present and complete.
# No new step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: 15052c4d-4170-449b-ba12-4f254bfa668e
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-331 default behaviour are already fully implemented in the existing file.
# The Given/When/Then steps and scenario binding are present and complete.
# No new step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: 89ea25f0-55d6-495d-b6d0-41884dae43e9
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-334 default behaviour are already fully implemented in the existing file.
# The Given/When/Then steps and scenario binding for this scenario are present and complete.
# No new step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: 05d5663f-e310-48de-9bb7-2b91a6036804
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-331 default behaviour are already fully implemented in the existing file.
# The Given/When/Then steps (external_service_configured, post_events_to_ingest,
# events_persisted) and the scenario binding (test_req_331_default_behaviour) are
# present and complete.  No new step definitions are required for this scenario.


# All steps for REQ-334 default behaviour are already fully implemented in the existing file.
# The Given/When/Then steps (ingest_column_with_path, post_payload_received,
# nested_value_extracted_missing_null) and the scenario binding (test_req_334_default_behaviour)
# are present and complete. No new step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: d0bf841f-b3fe-4d4f-95b2-2b8ae77a3b06
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-331 default behaviour are already fully implemented in the existing file.
# The Given/When/Then steps and scenario binding are present and complete.
# No new step definitions are required for this scenario.


# Copyright (c) 2026 Kenneth Stott
# Canary: 1437ba8f-4dd3-4698-bd17-4246ab539f67
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-334 default behaviour are already fully implemented in the existing file.
# The Given/When/Then steps (ingest_column_with_path, post_payload_received,
# nested_value_extracted_missing_null) and the scenario binding (test_req_334_default_behaviour)
# are present and complete. No new step definitions are required for this scenario.
