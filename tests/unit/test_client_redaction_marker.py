# Copyright (c) 2026 Kenneth Stott
# Canary: ba21ae4b-1a03-4d0b-9458-d82169a99662
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The marker a client is sent names no secret (REQ-2087, amended 2026-11-16).

Text sent to a client carries a bare ``[REDACTED]``: no rule's name, no variable's. The log line
and the audit record of the same event keep the marker that names the rule. One function makes
the client's form (``redaction.client_redact``), and no surface builds a marker of its own."""

# Requirements: REQ-2087

from __future__ import annotations

import ast
import io
import json
import logging
from collections.abc import Callable
from pathlib import Path

import grpc
import pytest

from provisa.core.secrets import resolve_secrets
from provisa.security import redaction

#: A source's password, given by the environment: registered as ``auto:env:<VARIABLE>``.
VARIABLE = "J1426_WAREHOUSE_PASSWORD"
SECRET = "s3cr3t-warehouse-pw"
#: The org's own rule, and a value it matches.
RULE = "employee_id"
EMPLOYEE = "EMP-123456"
EVENT = f"login failed for {SECRET} of {EMPLOYEE}"
BARE = "login failed for [REDACTED] of [REDACTED]"
NAMED = f"login failed for [REDACTED:auto:env:{VARIABLE}] of [REDACTED:{RULE}]"

PROVISA = Path(__file__).resolve().parents[2] / "provisa"


@pytest.fixture(autouse=True)
def _rules(monkeypatch: pytest.MonkeyPatch):
    redaction.reset()
    monkeypatch.setenv(VARIABLE, SECRET)
    assert resolve_secrets(f"${{env:{VARIABLE}}}", credential=True) == SECRET
    redaction.set_deployment_rules(
        redaction.parse_rules([{"name": RULE, "pattern": r"EMP-\d{6}"}], "deployment"),
        lambda reference: reference,
    )
    yield
    redaction.reset()


# --- what each surface sends for the event ------------------------------------------------------


def _http_json() -> str:
    return _http(json_body=True)


def _http_text() -> str:
    return _http(json_body=False)


def _http(*, json_body: bool) -> str:
    """Every HTTP front (REST, GraphQL, SPARQL, Cypher over HTTP, the admin API the UI reads):
    one middleware answers each error body."""
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse, PlainTextResponse
    from fastapi.testclient import TestClient

    from provisa.api.error_redaction import ErrorRedactionMiddleware

    app = FastAPI()

    @app.get("/bad")
    def bad():
        if json_body:
            return JSONResponse({"detail": EVENT}, status_code=400)
        return PlainTextResponse(EVENT, status_code=502)

    app.add_middleware(ErrorRedactionMiddleware)
    answered = TestClient(app).get("/bad")
    return answered.json()["detail"] if json_body else answered.text


def _pgwire() -> str:
    from provisa.pgwire.server import ProvisaHandler

    written: list[bytes] = []

    class _W:
        def write(self, data):
            written.append(bytes(data))

        def flush(self):
            pass

    handler = ProvisaHandler.__new__(ProvisaHandler)
    handler.wfile = _W()
    handler._send_pg_error("ERROR", "XX000", EVENT)
    blob = b"".join(written)
    start = blob.index(b"Mlogin failed") + 1
    return blob[start : blob.index(b"\x00", start)].decode()


class _Aborted(Exception):
    pass


class _GrpcContext:
    def __init__(self) -> None:
        self.details = ""

    def abort(self, code, details):
        self.details = details
        raise _Aborted()


def _grpc_abort() -> str:
    from provisa.grpc.redaction import _unary

    def handler(request, ctx):
        ctx.abort(grpc.StatusCode.INVALID_ARGUMENT, EVENT)

    ctx = _GrpcContext()
    with pytest.raises(_Aborted):
        _unary(handler)(None, ctx)
    return ctx.details


def _grpc_raise() -> str:
    from provisa.grpc.redaction import _streaming

    def handler(request, ctx):
        raise RuntimeError(EVENT)
        yield  # a response stream

    ctx = _GrpcContext()
    with pytest.raises(_Aborted):
        list(_streaming(handler)(None, ctx))
    return ctx.details.removeprefix("Exception calling application: ")


def _flight() -> str:
    import pyarrow.flight as flight

    from provisa.api.flight.redaction import redact_errors

    @redact_errors
    class _Server:
        def do_get(self, context, ticket):
            raise flight.FlightServerError(EVENT)

    with pytest.raises(flight.FlightServerError) as raised:
        _Server().do_get(None, None)
    return str(raised.value)


def _flight_other() -> str:
    import pyarrow.flight as flight

    from provisa.api.flight.redaction import redact_errors

    @redact_errors
    class _Server:
        def get_schema(self, context, descriptor):
            raise ValueError(EVENT)

    with pytest.raises(flight.FlightServerError) as raised:
        _Server().get_schema(None, None)
    return str(raised.value)


def _mcp() -> str:
    from provisa.api.mcp.server import _redacted_error

    said = _redacted_error(ValueError(EVENT))
    assert said is not None
    return str(said)


class _BoltWriter:
    def __init__(self) -> None:
        self.buf = io.BytesIO()

    def write(self, data: bytes) -> None:
        self.buf.write(data)


def _bolt() -> str:
    from provisa.bolt.session import BoltSession

    writer = _BoltWriter()
    session = BoltSession(writer, (5, 4))  # type: ignore[arg-type]
    session.send_failure("Neo.ClientError.Statement.SyntaxError", EVENT)
    blob = writer.buf.getvalue()
    start = blob.index(b"login failed")
    # The message is the FAILURE's last field: the chunk's end marker follows it.
    return blob[start : blob.index(b"\x00\x00", start)].decode()


def _table_stream() -> str:
    from provisa.api.data.stream_end import table_stream_error
    from provisa.api.errors import ApiError

    frame = table_stream_error(ApiError(503, "data.source_failed", EVENT), "sub1", "orders")
    return json.loads(frame.split("data: ", 1)[1])["detail"]


def _graphql_stream() -> str:
    from provisa.api.data.stream_end import graphql_stream_error
    from provisa.api.errors import ApiError

    frame = graphql_stream_error(ApiError(503, "data.source_failed", EVENT), "sub1", "orders")
    return json.loads(frame.split("data: ", 1)[1])["errors"][0]["message"]


SURFACES: dict[str, Callable[[], str]] = {
    "http-json": _http_json,
    "http-text": _http_text,
    "pgwire": _pgwire,
    "grpc-abort": _grpc_abort,
    "grpc-raise": _grpc_raise,
    "flight": _flight,
    "flight-other-error": _flight_other,
    "mcp": _mcp,
    "bolt": _bolt,
    "table-subscription": _table_stream,
    "graphql-subscription": _graphql_stream,
}


# --- what the records keep for the same event ---------------------------------------------------


def _log_line() -> str:
    from provisa.observability.redaction_otel import install_on_every_handler

    stream = io.StringIO()
    logger = logging.getLogger("provisa.test_client_redaction_marker")
    handler = logging.StreamHandler(stream)
    logger.addHandler(handler)
    try:
        install_on_every_handler()
        logger.error("%s", EVENT)
    finally:
        logger.removeHandler(handler)
    return stream.getvalue().strip()


def _audit_text() -> str:
    from datetime import datetime, timezone

    from provisa.audit.writer import AuditRecord

    class _Plain:
        def encrypt(self, data: bytes) -> bytes:
            return data

    record = AuditRecord(
        record_db=None,
        tenant_id="acme",
        user_id="alice",
        role_id="analyst",
        query_text=EVENT,
        table_ids=(),
        source="pgwire",
        status_code=500,
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
    return record.row(None)["query_text_enc"].decode()


@pytest.mark.parametrize("surface", sorted(SURFACES))
def test_a_client_is_sent_the_bare_marker_and_the_records_keep_the_named_one(surface: str):
    told = SURFACES[surface]()
    assert told == BARE
    for named in (SECRET, EMPLOYEE, VARIABLE, RULE, "REDACTED:", "auto:"):
        assert named not in told
    # The same event, as Provisa records it.
    assert _log_line() == NAMED
    assert _audit_text() == NAMED


def test_the_admin_trail_and_a_span_keep_the_named_marker():
    from opentelemetry.sdk.trace import ReadableSpan

    from provisa.observability.redaction_otel import redacted_span

    assert redacted_span(ReadableSpan(name=EVENT)).name == NAMED
    assert redaction.redact_value({"why": [EVENT]}) == {"why": [NAMED]}


def test_the_client_form_matches_what_the_named_form_matches():
    """Matching is the same for both forms, anywhere in the text; only the marker differs."""
    text = f"relation {SECRET}.orders, {SECRET}_slot, non{SECRET} and card 4111 1111 1111 1111"
    assert redaction.redact(text) == (
        f"relation [REDACTED:auto:env:{VARIABLE}].orders, [REDACTED:auto:env:{VARIABLE}]_slot, "
        f"non[REDACTED:auto:env:{VARIABLE}] and card [REDACTED:card_number]"
    )
    assert redaction.client_redact(text) == (
        "relation [REDACTED].orders, [REDACTED]_slot, non[REDACTED] and card [REDACTED]"
    )
    # A card number that fails its check is no match in either form.
    assert redaction.client_redact("4111 1111 1111 1112") == "4111 1111 1111 1112"
    assert redaction.client_redact_value({"a": [SECRET, ("x", 7)]}) == {
        "a": [redaction.CLIENT_MARKER, ("x", 7)]
    }


def test_a_named_marker_already_in_the_text_reaches_a_client_bare():
    """An error built from text a record already redacted (a wrapped failure) names no rule."""
    assert redaction.client_redact(f"the model failed: {redaction.redact(EVENT)}") == (
        f"the model failed: {BARE}"
    )


def test_a_prompt_and_a_text_sent_to_a_model_carry_the_bare_marker():
    """What a model is sent returns to the caller in its answer: it names no rule either."""
    from provisa.codesource.llm_service import _redacted_messages

    assert _redacted_messages([{"role": "user", "content": EVENT}], "acme") == [
        {"role": "user", "content": BARE}
    ]


# --- the walk: no surface builds a marker, and only the records use the named form --------------

#: The functions that write Provisa's own records -- a log, a span, the audit trail -- and so use
#: the form that names the rule. Any other use of it is a client surface sending the named form.
RECORDS = {
    ("observability/redaction_otel.py", "_attributes"),
    ("observability/redaction_otel.py", "redacted_span"),
    ("observability/redaction_otel.py", "on_emit"),
    ("observability/redaction_otel.py", "filter"),
    ("audit/writer.py", "row"),
    ("audit/query_log.py", "log_query"),
    ("core/org_membership.py", "record_admin_action"),
    ("codesource/service_ops.py", "_keys_redacted"),
    ("codesource/service_ops.py", "audit"),
}
_NAMED_FORM = {"redact", "redact_value"}
_REDACTION = "provisa.security.redaction"


class _NamedUses(ast.NodeVisitor):
    """Each function of a module that uses the named form: imported by name from the redaction
    module, or read off the module (``redaction.redact``)."""

    def __init__(self) -> None:
        self.stack: list[str] = []
        self.found: set[str] = set()
        self.module_aliases: set[str] = set()
        self.name_aliases: set[str] = set()

    def _here(self) -> str:
        return self.stack[-1] if self.stack else "<module>"

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.stack.append(node.name)
        self.generic_visit(node)
        self.stack.pop()

    visit_AsyncFunctionDef = visit_FunctionDef  # type: ignore[assignment]

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        for alias in node.names:
            if node.module == _REDACTION and alias.name in _NAMED_FORM:
                self.name_aliases.add(alias.asname or alias.name)
                self.found.add(self._here())
            if node.module == "provisa.security" and alias.name == "redaction":
                self.module_aliases.add(alias.asname or alias.name)

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            if alias.name == _REDACTION:
                self.module_aliases.add(alias.asname or alias.name)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if node.attr in _NAMED_FORM and ast.unparse(node.value) in self.module_aliases:
            self.found.add(self._here())
        self.generic_visit(node)


def _modules() -> list[Path]:
    return sorted(
        p for p in PROVISA.rglob("*.py") if p.name != "redaction.py" or p.parent.name != "security"
    )


def test_only_the_records_use_the_form_that_names_the_rule():
    found: set[tuple[str, str]] = set()
    for path in _modules():
        source = path.read_text()
        if "redact" not in source:
            continue
        uses = _NamedUses()
        uses.visit(ast.parse(source))
        found |= {(path.relative_to(PROVISA).as_posix(), fn) for fn in uses.found}
    sends_named = sorted(found - RECORDS)
    assert not sends_named, (
        "these use redaction.redact / redact_value, the form that names the rule, outside "
        "Provisa's own records; text sent to a client (or a model) goes through "
        f"redaction.client_redact or engine_error_names.client_text: {sends_named}"
    )
    assert not RECORDS - found, f"listed as a record writer, and no longer one: {RECORDS - found}"


def test_no_surface_builds_a_marker_of_its_own():
    built = [
        path.relative_to(PROVISA).as_posix()
        for path in _modules()
        if "[REDACTED" in path.read_text()
    ]
    assert built == [], (
        f"a redaction marker is written only by provisa/security/redaction.py: {built}"
    )


def test_every_client_surface_answers_through_the_one_function():
    """Each transport's error path reads the one client text, which redacts with the one
    function for the client's form."""
    for surface in (
        "api/error_redaction.py",
        "pgwire/server.py",
        "grpc/redaction.py",
        "api/flight/redaction.py",
        "api/mcp/server.py",
        "bolt/session.py",
        "bolt/server.py",
    ):
        assert "client_text(" in (PROVISA / surface).read_text(), surface
    assert "client_redact(" in (PROVISA / "api/engine_error_names.py").read_text()
    assert "client_redact_value(" in (PROVISA / "api/data/stream_end.py").read_text()
