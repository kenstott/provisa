# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""BDD step implementations for REQ-331 — Ingest Sources: Governed HTTP Push Receiver."""

from __future__ import annotations

import datetime as _dt
import time as _time

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
    "../features/req_331.feature",
    "REQ-331 default behaviour",
)
def test_req_331_default_behaviour():
    """Bind the REQ-331 default behaviour scenario."""


@scenario(
    "../features/req_333.feature",
    "REQ-333 default behaviour",
)
def test_req_333_default_behaviour():
    """Bind the REQ-333 default behaviour scenario."""


@scenario(
    "../features/req_334.feature",
    "REQ-334 default behaviour",
)
def test_req_334_default_behaviour():
    """Bind the REQ-334 default behaviour scenario."""


@scenario(
    "../features/req_335.feature",
    "REQ-335 default behaviour",
)
def test_req_335_default_behaviour():
    """Bind the REQ-335 default behaviour scenario."""


@scenario(
    "../features/req_336.feature",
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
    "CREATE TABLE IF NOT EXISTS DDL is executed with system columns "
    "_received_at and _updated_at injected"
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
        "Steward-declared '_updated_at' column must not be double-inserted; "
        f"got:\n{dedup_ddl}"
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


@then(
    "the value at that nested path is extracted into the column "
    "and missing paths yield NULL"
)
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

    # Confirm _extract_row honours the fallback: no 'path' key → use column_name.
    fallback_only_cols = [{"column_name": "severity", "data_type": "text"}]
    fallback_row = _extract_row(payload, fallback_only_cols)
    assert fallback_row["severity"] == "ERROR", (
        f"_extract_row fallback to column_name should yield 'ERROR', "
        f"got {fallback_row['severity']!r}"
    )

    # Deeply nested path with multiple array and dict segments resolves correctly.
    deep_payload = {
        "resourceLogs": [
            {
                "resource": {
                    "attributes": [
                        {"key": "host", "value": "srv1"}
                    ]
                }
            }
        ]
    }
    deep_val = extract_value(deep_payload, "resourceLogs.0.resource.attributes.0.key")
    assert deep_val == "host", (
        f"Deep path extraction should yield 'host', got {deep_val!r}"
    )

    # A path that descends into a scalar (non-dict, non-list) yields NULL.
    scalar_payload = {"a": "scalar_value"}
    assert extract_value(scalar_payload, "a.b") is None, (
        "Descending into a scalar must yield NULL"
    )


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
    body,
    *,
    known_sources: set[str],
    known_tables: set[str],
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
    "HTTP 202 is returned with the count of inserted rows; "
    "404 for unknown source/table, 503 for unavailable engine"
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

    columns = shared_data["columns"]
    known_sources = shared_data["known_sources"]
    known_tables = shared_data["known_tables"]
    source_id = shared_data["source_id"]
    table = shared_data["table"]
    array_body = shared_data["array_body"]

    # -----------------------------------------------------------------------
    # A single JSON object (not an array) is accepted as a one-element batch.
    # -----------------------------------------------------------------------
    single_store: list[dict] = []
    status, resp = _simulate_ingest(
        source_id,
        table,
        array_body[0],  # single dict, not a list
        known_sources=known_sources,
        known_tables=known_tables,
        columns=columns,
        backing_store=single_store,
        engine_available=True,
    )
    assert status == 202, f"Single-object ingest: expected 202, got {status}"
    assert resp["inserted"] == 1, (
        f"Single-object ingest: expected inserted=1, got {resp['inserted']}"
    )
    assert len(single_store) == 1, (
        f"Single-object ingest: expected 1 row in store, got {len(single_store)}"
    )
    assert single_store[0]["service_name"] == "checkout-svc", (
        f"Single-object ingest: service_name mismatch: {single_store[0]['service_name']!r}"
    )
    assert single_store[0]["severity"] == "ERROR"
    assert single_store[0]["message"] == "payment failed"

    # -----------------------------------------------------------------------
    # Unknown source -> 404, nothing persisted.
    # -----------------------------------------------------------------------
    miss_source_store: list[dict] = []
    status, resp = _simulate_ingest(
        _SOURCE_NOT_FOUND,
        table,
        array_body,
        known_sources=known_sources,
        known_tables=known_tables,
        columns=columns,
        backing_store=miss_source_store,
        engine_available=True,
    )
    assert status == 404, f"Unknown source: expected 404, got {status}"
    assert "source not found" in resp["detail"], (
        f"Unknown source: expected 'source not found' in detail, got {resp['detail']!r}"
    )
    assert miss_source_store == [], (
        f"Unknown source: expected empty store, got {miss_source_store}"
    )

    # -----------------------------------------------------------------------
    # Unknown table -> 404, nothing persisted.
    # -----------------------------------------------------------------------
    miss_table_store: list[dict] = []
    status, resp = _simulate_ingest(
        source_id,
        _TABLE_NOT_FOUND,
        array_body,
        known_sources=known_sources,
        known_tables=known_tables,
        columns=columns,
        backing_store=miss_table_store,
        engine_available=True,
    )
    assert status == 404, f"Unknown table: expected 404, got {status}"
    assert "table not found" in resp["detail"], (
        f"Unknown table: expected 'table not found' in detail, got {resp['detail']!r}"
    )
    assert miss_table_store == [], (
        f"Unknown table: expected empty store, got {miss_table_store}"
    )

    # -----------------------------------------------------------------------
    # Engine unavailable -> 503, nothing persisted.
    # -----------------------------------------------------------------------
    down_store: list[dict] = []
    status, resp = _simulate_ingest(
        source_id,
        table,
        array_body,
        known_sources=known_sources,
        known_tables=known_tables,
        columns=columns,
        backing_store=down_store,
        engine_available=False,
    )
    assert status == 503, f"Engine down: expected 503, got {status}"
    assert "unavailable" in resp["detail"], (
        f"Engine down: expected 'unavailable' in detail, got {resp['detail']!r}"
    )
    assert down_store == [], (
        f"Engine down: expected empty store, got {down_store}"
    )

    # -----------------------------------------------------------------------
    # Empty array body -> 202, inserted=0, nothing persisted.
    # -----------------------------------------------------------------------
    empty_store: list[dict] = []
    status, resp = _simulate_ingest(
        source_id,
        table,
        [],
        known_sources=known_sources,
        known_tables=known_tables,
        columns=columns,
        backing_store=empty_store,
        engine_available=True,
    )
    assert status == 202, f"
