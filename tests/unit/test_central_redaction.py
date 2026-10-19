# Copyright (c) 2026 Kenneth Stott
# Canary: 95a6875f-9410-483f-8410-11e8813d1ef3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Central redaction (REQ-2087): one rule list -- built-ins, the deployment's, each org's, and the
automatic rule over the org's secrets -- applied to spans, logs, stdout, the audit trail and every
error a client is sent."""

# Requirements: REQ-2087

from __future__ import annotations

import asyncio
import json
import logging
import os

import grpc
import pytest

from provisa.security import redaction

SECRET = "s3cr3t-warehouse-pw"


@pytest.fixture(autouse=True)
def _fresh_rules():
    redaction.reset()
    yield
    redaction.reset()


def _resolve(table: dict[str, str]):
    def resolve(reference: str) -> str:
        if reference not in table:
            raise KeyError(reference)
        return table[reference]

    return resolve


# --- the built-ins ------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "rule, text",
    [
        ("card_number", "card 4111 1111 1111 1111 on file"),
        ("us_ssn", "ssn 123-45-6789"),
        ("bearer_token", "Authorization: Bearer abc.DEF-123_xyz"),
        ("provisa_access_token", "token provisa_pat_ABCDEFGHIJKLMNOPQRSTUV"),
        ("aws_access_key_id", "key AKIAIOSFODNN7EXAMPLE"),
        ("aws_secret_access_key", "aws_secret_access_key=" + "a" * 40),
        ("gcp_api_key", "AIza" + "B" * 35),
        ("azure_storage_key", "AccountKey=" + "C" * 86 + "=="),
        ("github_token", "ghp_" + "D" * 36),
        ("slack_token", "xoxb-1234567890-abcdef"),
        (
            "private_key",
            "-----BEGIN RSA PRIVATE KEY-----\nMIIE\n-----END RSA PRIVATE KEY-----",
        ),
    ],
)
def test_each_builtin_redacts_its_shape(rule, text):
    out = redaction.redact(text)
    assert f"[REDACTED:{rule}]" in out


def test_a_card_number_must_pass_the_luhn_check():
    assert "[REDACTED:card_number]" in redaction.redact("4111111111111111")
    assert redaction.redact("4111111111111112") == "4111111111111112"


def test_redact_value_walks_dicts_lists_and_tuples():
    value = {"a": ["ssn 123-45-6789", ("x", 7)], "b": 1}
    assert redaction.redact_value(value) == {
        "a": ["ssn [REDACTED:us_ssn]", ("x", 7)],
        "b": 1,
    }


# --- rules as written ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, why",
    [
        ([{"name": "bad name", "pattern": "x"}], "a name is"),
        ([{"name": "us_ssn", "pattern": "x"}], "already used"),
        ([{"name": "a", "pattern": "x"}, {"name": "a", "pattern": "y"}], "already used"),
        ([{"name": "a"}], "either a pattern or a reference"),
        ([{"name": "a", "pattern": "x", "reference": "${env:X}"}], "either a pattern"),
        ([{"name": "a", "pattern": "("}], "does not compile"),
        ([{"name": "a", "reference": "plain"}], "${secret:NAME} or ${env:NAME}"),
    ],
)
def test_a_rule_is_refused_by_name_for_what_is_wrong(raw, why):
    with pytest.raises(redaction.RuleInvalid) as caught:
        redaction.parse_rules(raw, "org")
    assert why in caught.value.why


def test_a_deployment_pattern_applies_in_every_org():
    rules = redaction.parse_rules([{"name": "employee_id", "pattern": r"EMP-\d{6}"}], "deployment")
    redaction.set_deployment_rules(rules, _resolve({}))
    assert redaction.redact("EMP-123456") == "[REDACTED:employee_id]"
    redaction.set_org("acme", [], {}, _resolve({}))
    assert redaction.redact("EMP-123456", "acme") == "[REDACTED:employee_id]"


def test_a_reference_rule_matches_the_value_it_resolves_to():
    rules = redaction.parse_rules([{"name": "ops", "reference": "${env:OPS}"}], "deployment")
    redaction.set_deployment_rules(rules, _resolve({"${env:OPS}": "ops-value-1234"}))
    assert redaction.redact("got ops-value-1234 back") == "got [REDACTED:ops] back"


def test_a_reference_that_does_not_resolve_is_named_unresolved(caplog):
    rules = redaction.parse_rules([{"name": "ops", "reference": "${env:MISSING}"}], "deployment")
    with caplog.at_level(logging.ERROR, logger="provisa.security.redaction"):
        redaction.set_deployment_rules(rules, _resolve({}))
    assert "ops" in caplog.text
    redaction.set_org("acme", [], {}, _resolve({}))
    assert redaction.described("acme")["unresolved"] == ["ops"]


def test_an_orgs_rules_apply_to_that_org_and_to_what_leaves_the_process():
    acme = redaction.parse_rules([{"name": "badge", "pattern": r"B-\d{4}"}], "org")
    redaction.set_org("acme", acme, {}, _resolve({}))
    redaction.set_org("other", [], {}, _resolve({}))
    assert redaction.redact("B-1234", "acme") == "[REDACTED:badge]"
    assert redaction.redact("B-1234", "other") == "B-1234"
    # Telemetry and errors carry no org: every org's rules apply together.
    assert redaction.redact("B-1234") == "[REDACTED:badge]"


# --- the automatic rule -------------------------------------------------------------------------


def test_the_orgs_secrets_are_redacted_and_shown_by_name_only():
    redaction.set_org("acme", [], {"warehouse_pw": SECRET}, _resolve({}))
    assert redaction.redact(f"login failed for {SECRET}") == (
        "login failed for [REDACTED:auto:warehouse_pw]"
    )
    described = redaction.described("acme")
    assert described["automatic"] == ["warehouse_pw"]
    assert SECRET not in json.dumps(described)


def test_a_value_shorter_than_the_minimum_is_not_matched():
    redaction.set_org("acme", [], {"pin": "abc"}, _resolve({}))
    assert redaction.redact("abc") == "abc"


def test_a_user_value_is_matched_from_its_first_resolution():
    assert redaction.redact("personal-token-99") == "personal-token-99"
    redaction.note_user_value("gh_token", "personal-token-99")
    assert redaction.redact("x personal-token-99 y") == "x [REDACTED:auto:user:gh_token] y"
    assert redaction.described("acme")["resolved_at_use"] == 1


def test_resolve_secrets_notes_the_user_and_secret_values_it_resolves(monkeypatch):
    from provisa.core import secrets

    class _Provider:
        def resolve(self, reference):
            return "org-secret-value-77"

        def resolve_user(self, reference):
            return "user-secret-value-88"

    monkeypatch.setattr(secrets, "_provider_for", lambda name: _Provider())
    secrets.resolve_secrets("${secret:wh}")
    secrets.resolve_secrets("${user:gh}")
    assert redaction.redact("org-secret-value-77") == "[REDACTED:auto:wh]"
    assert redaction.redact("user-secret-value-88") == "[REDACTED:auto:user:gh]"


# --- spans, logs, stdout ------------------------------------------------------------------------


def _span(**overrides):
    from opentelemetry.sdk.trace import Event, ReadableSpan
    from opentelemetry.trace import Status, StatusCode

    fields = {
        "name": f"query {SECRET}",
        "attributes": {"db.statement": f"SELECT '{SECRET}'", "n": 3},
        "events": [Event("exception", {"exception.message": f"bad {SECRET}"}, 1)],
        "status": Status(StatusCode.ERROR, f"failed with {SECRET}"),
    }
    fields.update(overrides)
    return ReadableSpan(**fields)


def test_a_span_is_handed_on_as_a_redacted_copy():
    from provisa.observability.redaction_otel import redacted_span

    redaction.set_org("acme", [], {"pw": SECRET}, _resolve({}))
    original = _span()
    copy = redacted_span(original)
    dumped = json.dumps(
        {
            "name": copy.name,
            "attributes": dict(copy.attributes),
            "events": [dict(e.attributes) for e in copy.events],
            "status": copy.status.description,
        }
    )
    assert SECRET not in dumped and "[REDACTED:auto:pw]" in dumped
    assert copy.attributes["n"] == 3
    assert SECRET in original.name  # the ended span is not changed; channels get the copy


def test_the_span_switch_gives_every_channel_the_redacted_copy():
    from provisa.api import otel_setup

    redaction.set_org("acme", [], {"pw": SECRET}, _resolve({}))
    seen: list = []

    class _Channel:
        def on_start(self, span, parent_context=None):
            pass

        def on_end(self, span):
            seen.append(span.name)

    exports = otel_setup._SpanExports()
    exports.set("a", _Channel())
    exports.set("b", _Channel())
    otel_setup._span_switch(exports).on_end(_span())
    assert seen == ["query [REDACTED:auto:pw]"] * 2


def test_a_log_record_is_redacted_before_it_is_exported():
    from opentelemetry._logs import LogRecord
    from opentelemetry.sdk._logs import ReadWriteLogRecord

    from provisa.observability.redaction_otel import log_record_processor

    redaction.set_org("acme", [], {"pw": SECRET}, _resolve({}))
    record = LogRecord(body=f"connect {SECRET}", attributes={"detail": SECRET})
    log_record_processor().on_emit(ReadWriteLogRecord(log_record=record))
    assert record.body == "connect [REDACTED:auto:pw]"
    assert record.attributes["detail"] == "[REDACTED:auto:pw]"


def test_stdout_is_redacted_by_the_filter_on_every_handler():
    import io

    from provisa.observability.redaction_otel import install_on_every_handler

    redaction.set_org("acme", [], {"pw": SECRET}, _resolve({}))
    stream = io.StringIO()
    logger = logging.getLogger("provisa.test_central_redaction")
    handler = logging.StreamHandler(stream)
    logger.addHandler(handler)
    try:
        install_on_every_handler()
        logger.error("could not connect with %s", SECRET)
        try:
            raise RuntimeError(f"bad {SECRET}")
        except RuntimeError:
            logger.exception("failed")
    finally:
        logger.removeHandler(handler)
    out = stream.getvalue()
    assert SECRET not in out
    assert "could not connect with [REDACTED:auto:pw]" in out
    assert "RuntimeError: bad [REDACTED:auto:pw]" in out


def test_an_endpoint_change_keeps_the_internal_channels_filter(monkeypatch):
    from provisa.api import otel_setup

    calls: list = []
    real = otel_setup._make_filtering_exporter

    def _spy(inner, redact_sql, attrs):
        calls.append((redact_sql, list(attrs)))
        return real(inner, redact_sql, attrs)

    monkeypatch.setattr(otel_setup, "_make_filtering_exporter", _spy)
    monkeypatch.setattr(otel_setup, "_internal_redaction", (True, ["user.email"]))
    try:
        otel_setup.attach_otlp_exporters(
            os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"], "provisa", "http/protobuf"
        )
    finally:
        otel_setup.shutdown_otel()
    assert (True, ["user.email"]) in calls


# --- the audit trail ----------------------------------------------------------------------------


def test_audit_text_is_redacted_before_it_is_hashed_and_encrypted():
    import hashlib
    from datetime import datetime, timezone

    from provisa.audit.writer import AuditRecord

    redaction.set_org("acme", [], {"pw": SECRET}, _resolve({}))

    class _Plain:
        def encrypt(self, data: bytes) -> bytes:
            return data

    record = AuditRecord(
        record_db=None,
        tenant_id="acme",
        user_id="alice",
        role_id="analyst",
        query_text=f"SELECT '{SECRET}'",
        table_ids=(),
        source="pgwire",
        status_code=200,
        duration_ms=1,
        logged_at=datetime.now(timezone.utc),
        trace_id=None,
        encryption=_Plain(),  # type: ignore[arg-type]
        model_stamp=1,
        model_env="prod",
        enforced={},
        route_reason=None,
        sources=(),
        data_age=None,
        region="default",
        time_travel=None,
        tables=[],
    )
    row = record.row(None)
    text = "SELECT '[REDACTED:auto:pw]'"
    assert row["query_text_enc"] == text.encode()
    assert row["query_hash"] == hashlib.sha256(text.encode()).hexdigest()


# --- errors a client is sent --------------------------------------------------------------------


def test_an_http_error_body_is_redacted():
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse, PlainTextResponse
    from fastapi.testclient import TestClient

    from provisa.api.error_redaction import ErrorRedactionMiddleware

    redaction.set_org("acme", [], {"pw": SECRET}, _resolve({}))
    app = FastAPI()

    @app.get("/bad")
    def bad():
        return JSONResponse({"detail": f"auth failed: {SECRET}"}, status_code=400)

    @app.get("/ok")
    def ok():
        return PlainTextResponse(SECRET)

    app.add_middleware(ErrorRedactionMiddleware)
    client = TestClient(app)
    bad_resp = client.get("/bad")
    assert bad_resp.status_code == 400
    assert bad_resp.json() == {"detail": "auth failed: [REDACTED:auto:pw]"}
    assert int(bad_resp.headers["content-length"]) == len(bad_resp.content)
    # A success body is the data asked for; the middleware leaves it alone.
    assert client.get("/ok").text == SECRET


def test_a_pgwire_error_message_is_redacted():
    from provisa.pgwire.server import ProvisaHandler

    redaction.set_org("acme", [], {"pw": SECRET}, _resolve({}))
    written: list[bytes] = []

    class _W:
        def write(self, data):
            written.append(bytes(data))

        def flush(self):
            pass

    handler = ProvisaHandler.__new__(ProvisaHandler)
    handler.wfile = _W()
    handler._send_pg_error("ERROR", "XX000", f"source said {SECRET}")
    blob = b"".join(written)
    assert SECRET.encode() not in blob and b"[REDACTED:auto:pw]" in blob


def test_a_grpc_handler_error_is_redacted():
    from provisa.grpc.redaction import _unary

    redaction.set_org("acme", [], {"pw": SECRET}, _resolve({}))
    aborted: list = []

    class _Abort(Exception):
        pass

    class _Ctx:
        def abort(self, code, details):
            aborted.append((code, details))
            raise _Abort()

    def _raises(request, ctx):
        raise RuntimeError(f"upstream {SECRET}")

    def _aborts(request, ctx):
        ctx.abort(grpc.StatusCode.INVALID_ARGUMENT, f"bad {SECRET}")

    with pytest.raises(_Abort):
        _unary(_raises)(None, _Ctx())
    with pytest.raises(_Abort):
        _unary(_aborts)(None, _Ctx())
    assert aborted == [
        (grpc.StatusCode.UNKNOWN, "Exception calling application: upstream [REDACTED:auto:pw]"),
        (grpc.StatusCode.INVALID_ARGUMENT, "bad [REDACTED:auto:pw]"),
    ]


def test_a_flight_error_is_redacted_and_keeps_its_type():
    import pyarrow.flight as flight

    from provisa.api.flight.redaction import redact_errors

    redaction.set_org("acme", [], {"pw": SECRET}, _resolve({}))

    @redact_errors
    class _Server:
        def do_action(self, context, action):
            raise flight.FlightUnauthenticatedError(f"token {SECRET}")

        def get_schema(self, context, descriptor):
            raise ValueError(f"no table {SECRET}")

        def list_actions(self, context):
            raise ValueError("nothing to hide")

    with pytest.raises(flight.FlightUnauthenticatedError) as unauth:
        _Server().do_action(None, None)
    assert SECRET not in str(unauth.value) and "[REDACTED:auto:pw]" in str(unauth.value)
    with pytest.raises(flight.FlightServerError) as other:
        _Server().get_schema(None, None)
    assert SECRET not in str(other.value)
    with pytest.raises(ValueError, match="nothing to hide"):
        _Server().list_actions(None)


def test_a_stream_failure_frame_is_redacted():
    from provisa.api.data.stream_end import failure_body

    redaction.set_org("acme", [], {"pw": SECRET}, _resolve({}))
    body = failure_body(RuntimeError(f"source said {SECRET}"))
    assert SECRET not in json.dumps(body)


def test_the_admin_trail_detail_is_redacted(monkeypatch):
    from provisa.core import org_membership

    redaction.set_org("acme", [], {"pw": SECRET}, _resolve({}))
    written: list = []

    class _Conn:
        async def execute_core(self, statement):
            written.append(statement.compile().params)

    class _Acquire:
        async def __aenter__(self):
            return _Conn()

        async def __aexit__(self, *exc):
            return False

    class _Db:
        def acquire(self):
            return _Acquire()

    asyncio.run(
        org_membership.record_admin_action(
            _Db(), action="x", actor_id="a", subject_id="s", detail={"why": SECRET}
        )
    )
    assert written[0]["detail"] == {"why": "[REDACTED:auto:pw]"}


def test_an_mcp_tool_error_is_redacted():
    from provisa.api.mcp.server import _redacted_error

    redaction.set_org("acme", [], {"pw": SECRET}, _resolve({}))
    said = _redacted_error(ValueError(f"source said {SECRET}"))
    assert said is not None and str(said) == "source said [REDACTED:auto:pw]"
    assert _redacted_error(ValueError("nothing to hide")) is None


def test_the_rules_page_refuses_a_bad_rule_by_name():
    from provisa.api.admin.redaction_router import RuleBody, RulesBody, _parsed
    from provisa.api.errors import ApiError

    with pytest.raises(ApiError) as refused:
        _parsed(RulesBody(rules=[RuleBody(name="x", pattern="(")]), "org")
    assert refused.value.status_code == 400
    assert "does not compile" in str(refused.value.detail)
    assert _parsed(RulesBody(rules=[RuleBody(name="x", pattern="a+")]), "org") == [
        {"name": "x", "pattern": "a+", "reference": None}
    ]
