# Copyright (c) 2026 Kenneth Stott
# Canary: 11f4cdd1-6570-46d4-9978-01b7de371241
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Stage-trace observability tests (REQ-914)."""

import pytest

from provisa.observability.stage_trace import redact_ast, redact_sql, trace_stage


class TestRedactSql:
    def test_string_literal_is_blanked(self):
        out = redact_sql("SELECT ssn FROM customers WHERE region = 'us-east'")
        assert "us-east" not in out
        assert "region" in out  # structure preserved
        assert "%s" in out  # literal replaced by placeholder

    def test_numeric_literal_is_blanked(self):
        out = redact_sql("SELECT id FROM orders WHERE total > 1000000")
        assert "1000000" not in out
        assert "total" in out

    def test_pii_value_never_survives_redaction(self):
        out = redact_sql("SELECT * FROM customers WHERE ssn = '111-11-1111'")
        assert "111-11-1111" not in out

    def test_column_and_table_identifiers_preserved(self):
        out = redact_sql('SELECT "email" FROM "public"."customers" WHERE "id" = 42')
        assert "email" in out
        assert "customers" in out
        assert "42" not in out


class TestRedactAst:
    def test_ast_repr_carries_structure_but_no_literal_values(self):
        out = redact_ast("SELECT ssn FROM customers WHERE ssn = '111-11-1111' AND age > 65")
        assert "111-11-1111" not in out
        assert "65" not in out
        assert "Column" in out  # structural node names preserved
        assert "Placeholder" in out  # literals blanked


class TestTraceStage:
    def test_trace_stage_is_noop_without_active_span(self):
        # No OTel span in this unit context — must not raise.
        trace_stage("govern.in", "SELECT 1")

    def test_trace_stage_never_raises_on_unparseable_sql(self, monkeypatch):
        # redacted mode + junk SQL: the redact error is surfaced on the span, not raised.
        monkeypatch.setenv("PROVISA_TRACE_SQL", "redacted")
        trace_stage("govern.in", "this is not sql ;;;")

    def test_off_mode_captures_no_sql_text(self, monkeypatch):
        # Default off: even with an active recording span, no literal text is attached.
        pytest.importorskip("opentelemetry")
        monkeypatch.delenv("PROVISA_TRACE_SQL", raising=False)
        from opentelemetry.sdk.trace import TracerProvider

        provider = TracerProvider()
        tracer = provider.get_tracer(__name__)
        with tracer.start_as_current_span("t") as span:
            trace_stage("govern.in", "SELECT ssn FROM customers WHERE region = 'us-east'")
        events = getattr(span, "events", [])
        for ev in events:
            for v in ev.attributes.values():
                assert "us-east" not in str(v)
