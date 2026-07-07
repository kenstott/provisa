# Copyright (c) 2026 Kenneth Stott
# Canary: cf536be2-c029-4e39-a374-758f257f42f7
#
# This source code is licensed under the Business Source License 1.1

"""pytest-bdd step implementations for REQ-914: Pipeline Stage Tracing."""

from __future__ import annotations

from typing import Any

import pytest
from pytest_bdd import given, scenarios, then, when

scenarios("../features/req_914_pipeline_stage_tracing.feature")


@pytest.fixture
def shared_data() -> dict[str, Any]:
    return {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_recording_span():
    """Return a live SDK span that records events, inside an active context."""
    otel_sdk = pytest.importorskip("opentelemetry.sdk.trace")
    from opentelemetry.sdk.trace import TracerProvider

    provider = TracerProvider()
    tracer = provider.get_tracer("test.req914")
    return tracer, provider


# ---------------------------------------------------------------------------
# Given
# ---------------------------------------------------------------------------


@given("the query pipeline processes a request")
def given_pipeline_processes_request(shared_data):
    """Set up a representative SQL that contains literals (PII + RLS predicate values)."""
    shared_data["sql"] = (
        "SELECT ssn, name FROM customers WHERE region = 'us-east' AND account_status = 'active'"
    )
    shared_data["stage"] = "govern.in"
    shared_data["span_events"] = []


# ---------------------------------------------------------------------------
# When
# ---------------------------------------------------------------------------


@when("PROVISA_TRACE_SQL is unset or off")
def when_trace_sql_off(monkeypatch, shared_data):
    monkeypatch.delenv("PROVISA_TRACE_SQL", raising=False)
    monkeypatch.delenv("PROVISA_TRACE_AST", raising=False)

    pytest.importorskip("opentelemetry.sdk.trace")
    from opentelemetry.sdk.trace import TracerProvider

    provider = TracerProvider()
    tracer = provider.get_tracer("test.req914.off")

    from provisa.observability.stage_trace import trace_stage

    recorded_events: list = []
    with tracer.start_as_current_span("test-off") as span:
        trace_stage(shared_data["stage"], shared_data["sql"])
        recorded_events.extend(getattr(span, "events", []))

    shared_data["span_events_off"] = recorded_events


@when("PROVISA_TRACE_SQL is redacted")
def when_trace_sql_redacted(monkeypatch, shared_data):
    monkeypatch.setenv("PROVISA_TRACE_SQL", "redacted")
    monkeypatch.delenv("PROVISA_TRACE_AST", raising=False)

    pytest.importorskip("opentelemetry.sdk.trace")
    from opentelemetry.sdk.trace import TracerProvider

    provider = TracerProvider()
    tracer = provider.get_tracer("test.req914.redacted")

    from provisa.observability.stage_trace import trace_stage

    stages = ["govern.in", "govern.rls", "govern.mask", "govern.out"]
    # Use SQL with realistic literals including a fake SSN and an RLS predicate value
    sql_with_pii = (
        "SELECT ssn, name FROM customers "
        "WHERE region = 'us-east' AND ssn = '111-22-3333' AND account_id = 42"
    )
    shared_data["sql_with_pii"] = sql_with_pii

    recorded_events: list = []
    with tracer.start_as_current_span("test-redacted") as span:
        for stage in stages:
            trace_stage(stage, sql_with_pii)
        recorded_events.extend(getattr(span, "events", []))

    shared_data["span_events_redacted"] = recorded_events


@when("the SQL is unparseable in redacted mode")
def when_sql_unparseable_redacted(monkeypatch, shared_data):
    monkeypatch.setenv("PROVISA_TRACE_SQL", "redacted")
    monkeypatch.delenv("PROVISA_TRACE_AST", raising=False)

    pytest.importorskip("opentelemetry.sdk.trace")
    from opentelemetry.sdk.trace import TracerProvider

    provider = TracerProvider()
    tracer = provider.get_tracer("test.req914.unparseable")

    from provisa.observability.stage_trace import trace_stage

    bad_sql = "this is ;;; completely unparseable !@#$% SQL {{{{ }}}"
    shared_data["bad_sql"] = bad_sql

    request_completed = False
    recorded_events: list = []
    exception_raised: Exception | None = None

    try:
        with tracer.start_as_current_span("test-unparseable") as span:
            trace_stage("govern.in", bad_sql)
            request_completed = True
            recorded_events.extend(getattr(span, "events", []))
    except Exception as exc:  # noqa: BLE001
        exception_raised = exc

    shared_data["request_completed_after_bad_sql"] = request_completed
    shared_data["exception_from_trace"] = exception_raised
    shared_data["span_events_unparseable"] = recorded_events


# ---------------------------------------------------------------------------
# Then
# ---------------------------------------------------------------------------


@then("stage span events carry the stage name but no SQL text")
def then_events_carry_stage_but_no_sql(shared_data):
    events = shared_data["span_events_off"]
    # There must be at least one event emitted for the stage.
    assert len(events) >= 1, "Expected at least one span event in off mode"

    sql = shared_data["sql"]
    # Extract all literal substrings from the SQL that we know are there.
    literals_to_check = ["us-east", "active", "SELECT ssn"]

    for event in events:
        attrs = event.attributes or {}
        # Stage name must be present.
        stage_val = attrs.get("pipeline.stage")
        assert stage_val is not None, f"pipeline.stage attribute missing on event {event.name}"

        # No SQL text attribute must be present.
        for key in ("sql", "sql.redacted", "sql.full"):
            assert key not in attrs, (
                f"In off mode, attribute '{key}' must not be present on event; found: {attrs}"
            )

        # Confirm no literal values leaked into any attribute value.
        for literal in literals_to_check:
            for attr_val in attrs.values():
                assert literal not in str(attr_val), (
                    f"Literal '{literal}' leaked into span attribute in off mode: {attr_val}"
                )


@then("each stage event carries SQL with every literal replaced by a placeholder")
def then_each_event_has_redacted_sql(shared_data):
    events = shared_data["span_events_redacted"]
    assert len(events) >= 1, "Expected span events in redacted mode"

    for event in events:
        attrs = event.attributes or {}
        # In redacted mode, if sql.redact_error is set the redaction failed - that is tested
        # separately. Skip events that recorded an error (they won't have sql.redacted).
        if "sql.redact_error" in attrs:
            continue

        assert "sql.redacted" in attrs, (
            f"Expected 'sql.redacted' attribute on event '{event.name}'; got keys: {list(attrs.keys())}"
        )

        redacted_val = attrs["sql.redacted"]
        # The redacted SQL must contain a placeholder marker - sqlglot uses %s for Placeholder.
        assert "%s" in redacted_val or "?" in redacted_val or "PLACEHOLDER" in redacted_val.upper(), (
            f"Redacted SQL does not appear to contain a placeholder: {redacted_val}"
        )


@then("no literal value (e.g. an RLS predicate value or SSN) appears in any attribute")
def then_no_literal_in_attributes(shared_data):
    events = shared_data["span_events_redacted"]
    assert len(events) >= 1, "Expected span events in redacted mode"

    # Known literals present in sql_with_pii
    forbidden_literals = ["us-east", "111-22-3333", "42"]

    for event in events:
        attrs = event.attributes or {}
        for attr_key, attr_val in attrs.items():
            for literal in forbidden_literals:
                # The error message might reference the raw SQL - but trace_stage only records
                # the error type + message, not the full SQL, so this is still safe. In our
                # implementation the redact_error attribute may contain parser context but not
                # the original literal values.
                assert literal not in str(attr_val), (
                    f"Literal '{literal}' found in span attribute '{attr_key}': {attr_val}"
                )


@then("the failure is recorded as a span attribute and the request is not aborted")
def then_failure_recorded_as_attribute_not_aborted(shared_data):
    # The request must have completed without raising.
    assert shared_data["exception_from_trace"] is None, (
        f"trace_stage raised an exception when SQL was unparseable: "
        f"{shared_data['exception_from_trace']}"
    )
    assert shared_data["request_completed_after_bad_sql"] is True, (
        "Request did not complete after unparseable SQL was passed to trace_stage"
    )

    events = shared_data["span_events_unparseable"]
    assert len(events) >= 1, "Expected at least one span event even for unparseable SQL"

    # At least one event must carry a sql.redact_error attribute.
    error_recorded = False
    for event in events:
        attrs = event.attributes or {}
        if "sql.redact_error" in attrs:
            error_recorded = True
            error_val = attrs["sql.redact_error"]
            assert len(error_val) > 0, "sql.redact_error attribute is empty"
            # Must reference the exception type name (e.g. ParseError, SqlglotError)
            assert "Error" in error_val or "error" in error_val.lower(), (
                f"sql.redact_error does not look like an error message: {error_val}"
            )
            break

    assert error_recorded, (
        "Expected a 'sql.redact_error' attribute on a span event for unparseable SQL; "
        f"events seen: {[e.name for e in events]}, "
        f"attrs seen: {[dict(e.attributes or {}) for e in events]}"
    )
