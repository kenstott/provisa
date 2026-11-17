# Copyright (c) 2026 Kenneth Stott
# Canary: 8d3c1f6a-47b2-4e95-a0d8-2c5e9b71f04a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An org's redaction rules (REQ-2087) apply only to that org's text, cannot stall the process,
and cannot read the server's environment."""

# Requirements: REQ-2087

from __future__ import annotations

import asyncio
import io
import logging
import random
import re
import threading
import time
from types import SimpleNamespace

import pytest

from provisa.core.request_context import reset_current_org, set_current_org
from provisa.security import redaction
from provisa.security.bounded_pattern import (
    STEP_LIMIT,
    PatternRefused,
    StepLimitExceeded,
    compile_bounded,
)


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


def _org(org_id: str, rules: list[dict]) -> None:
    redaction.set_org(org_id, redaction.parse_rules(rules, "org"), {}, _resolve({}))


def _logged_as(org_id: str | None, message: str) -> str:
    """``message`` as a handler with the redacting filter writes it, by a writer bound to
    ``org_id``."""
    from provisa.observability.redaction_otel import install_on_every_handler

    stream = io.StringIO()
    logger = logging.getLogger("provisa.test_org_redaction_scoped")
    handler = logging.StreamHandler(stream)
    logger.addHandler(handler)
    token = set_current_org(org_id) if org_id is not None else None
    try:
        install_on_every_handler()
        logger.error("%s", message)
    finally:
        if token is not None:
            reset_current_org(token)
        logger.removeHandler(handler)
    return stream.getvalue().strip()


# --- scope ---------------------------------------------------------------------------------------


def test_org_as_pattern_does_not_touch_org_bs_log_line():
    _org("org-a", [{"name": "badge", "pattern": r"B-\d{4}"}])
    _org("org-b", [])
    assert _logged_as("org-b", "badge B-1234 seen") == "badge B-1234 seen"
    assert _logged_as("org-a", "badge B-1234 seen") == "badge [REDACTED:badge] seen"
    # A record no org produced: the deployment's rules only.
    assert _logged_as(None, "badge B-1234 seen") == "badge B-1234 seen"


def test_org_as_pattern_does_not_touch_org_bs_span_or_client_text():
    from opentelemetry.sdk.trace import ReadableSpan

    from provisa.observability.redaction_otel import redacted_span
    from provisa.otel_compat import ORG_ATTRIBUTE

    _org("org-a", [{"name": "badge", "pattern": r"B-\d{4}"}])
    _org("org-b", [])

    def span(org: str) -> ReadableSpan:
        return ReadableSpan(name="q B-1234", attributes={ORG_ATTRIBUTE: org, "s": "B-1234"})

    assert redacted_span(span("org-b")).name == "q B-1234"
    assert redacted_span(span("org-b")).attributes["s"] == "B-1234"
    assert redacted_span(span("org-a")).name == "q [REDACTED:badge]"
    assert redaction.client_redact("B-1234", "org-b") == "B-1234"
    assert redaction.client_redact("B-1234", "org-a") == "[REDACTED]"


def test_the_deployments_rules_apply_to_every_org_and_to_records_of_none():
    rules = redaction.parse_rules([{"name": "emp", "pattern": r"EMP-\d{6}"}], "deployment")
    redaction.set_deployment_rules(rules, _resolve({}))
    _org("org-b", [])
    assert _logged_as("org-b", "EMP-123456") == "[REDACTED:emp]"
    assert _logged_as(None, "EMP-123456") == "[REDACTED:emp]"


def test_org_as_reference_value_does_not_touch_org_bs_text():
    rules = redaction.parse_rules([{"name": "word", "reference": "${secret:W}"}], "org")
    redaction.set_org("org-a", rules, {}, _resolve({"${secret:W}": "SELECT"}))
    _org("org-b", [])
    assert redaction.redact("SELECT 1", "org-b") == "SELECT 1"
    assert redaction.redact("SELECT 1", "org-a") == "[REDACTED:word] 1"


def test_a_secret_resolved_in_org_as_request_is_matched_in_as_text_and_in_records_of_none():
    token = set_current_org("org-a")
    try:
        redaction.note_secret_value("wh", "secret-of-a-1234")
    finally:
        reset_current_org(token)
    _org("org-a", [])
    _org("org-b", [])
    assert redaction.redact("secret-of-a-1234", "org-a") == "[REDACTED:auto:wh]"
    assert redaction.redact("secret-of-a-1234") == "[REDACTED:auto:wh]"
    assert redaction.redact("secret-of-a-1234", "org-b") == "secret-of-a-1234"


# --- catastrophic patterns -----------------------------------------------------------------------


@pytest.mark.parametrize(
    "pattern, code",
    [
        (r"(a+)+$", "redaction.pattern_backtracks"),
        (r"(\w+\s?)*$", "redaction.pattern_backtracks"),
        (r"(ab|ac)*x", "redaction.pattern_backtracks"),
        (r"(x|)*y", "redaction.pattern_backtracks"),
        (r"(a)\1", "redaction.pattern_unsupported"),
        (r"(?<=a)b", "redaction.pattern_unsupported"),
        (r"a{1,9000}", "redaction.pattern_too_large"),
    ],
)
def test_a_pattern_that_can_stall_is_refused_by_name_at_save(pattern, code):
    with pytest.raises(redaction.RuleInvalid) as caught:
        redaction.parse_rules([{"name": "bad", "pattern": pattern}], "org")
    assert caught.value.rule == "bad"
    assert caught.value.code == code


def test_the_deployments_own_pattern_keeps_the_full_syntax():
    rules = redaction.parse_rules([{"name": "look", "pattern": r"(?<=id=)\d+"}], "deployment")
    assert rules[0].pattern == r"(?<=id=)\d+"


def test_a_slow_pattern_is_cut_off_at_use_without_stalling_others(caplog):
    # Accepted at save (no nested or overlapping repeat), but it rescans the rest of the text after
    # every match: quadratic in the text's length.
    _org("org-a", [{"name": "slow", "pattern": r"a(.*b)?"}])
    _org("org-b", [{"name": "badge", "pattern": r"B-\d{4}"}])
    text = "a" * 5000
    seen: dict[str, str] = {}

    def a_logs() -> None:
        seen["a"] = redaction.redact(text, "org-a")

    with caplog.at_level(logging.ERROR, logger="provisa.security.redaction"):
        started = time.monotonic()
        worker = threading.Thread(target=a_logs)
        worker.start()
        assert redaction.redact("B-1234", "org-b") == "[REDACTED:badge]"
        worker.join(timeout=10)
        elapsed = time.monotonic() - started
    assert not worker.is_alive()
    assert elapsed < 5
    assert seen["a"] == "[REDACTED:slow]"  # withheld whole: what it would remove is unknown
    assert "'slow'" in caplog.text and "step limit" in caplog.text
    assert redaction.described("org-a")["exceeded"] == ["slow"]
    assert redaction.described("org-b")["exceeded"] == []


def test_the_step_limit_is_raised_by_name():
    with pytest.raises(StepLimitExceeded) as caught:
        compile_bounded(r"a(.*b)?").sub(lambda _s: "x", "a" * 5000)
    assert caught.value.steps == STEP_LIMIT


def test_a_catastrophic_shape_is_linear_on_the_matcher():
    # (a|b)*c passes the save check; on re a long run without c is linear too, but this asserts
    # the matcher itself never backtracks on a text that defeats a backtracking engine's guess.
    pattern = compile_bounded(r"(?:a|b)*c")
    started = time.monotonic()
    assert pattern.sub(lambda _s: "x", "ab" * 20000) == "ab" * 20000
    assert time.monotonic() - started < 5


_PATTERNS = [
    r"EMP-\d{6}",
    r"(?i)acct[ _-]?\d{4,8}",
    r"\bfoo\b",
    r"\Bx",
    r"x*",
    r"a|ab",
    r"ab|a",
    r"a.*?b",
    r"a.*b",
    r"^\w+",
    r"(?m)^\d+$",
    r"[^\s]+@[a-z]+\.com",
    r"colou?r",
    r"\d{2,3}",
    r"(ab)+",
    r"(?s)<.*?>",
    r"[a-f0-9]{8}",
    r"(?i)[A-Z]{3}",
    r"\$\d+(\.\d\d)?",
    r"a{0,3}",
    r"a{2,3}?",
    r"(?:cat|dog)s?",
    r"\s+",
    r"$",
    r"\Z",
]


@pytest.mark.parametrize("pattern", _PATTERNS)
def test_the_matcher_finds_what_re_finds(pattern):
    alphabet = "ab xyEMP-0123456789\nfoo.@comcatdogs<>$CoLoR"
    rnd = random.Random(pattern)
    bounded, compiled = compile_bounded(pattern), re.compile(pattern)
    for _ in range(200):
        text = "".join(rnd.choice(alphabet) for _ in range(rnd.randint(0, 30)))
        assert bounded.sub(lambda s: f"<{s}>", text) == compiled.sub(
            lambda m: f"<{m.group(0)}>", text
        ), (pattern, text)


def test_a_pattern_the_matcher_cannot_run_is_refused_by_the_matcher():
    with pytest.raises(PatternRefused):
        compile_bounded(r"(?P<x>a)(?P=x)")


# --- server references ---------------------------------------------------------------------------


def test_a_held_orgs_env_reference_is_refused_by_name_at_save():
    with pytest.raises(redaction.RuleInvalid) as caught:
        redaction.parse_rules(
            [{"name": "ops", "reference": "${env:DATABASE_URL}"}],
            "org",
            server_references=False,
        )
    assert caught.value.rule == "ops"
    assert caught.value.code == "redaction.server_reference"
    # A held org's own vault is its own.
    redaction.parse_rules(
        [{"name": "pw", "reference": "${secret:PW}"}], "org", server_references=False
    )


def test_a_held_orgs_stored_env_reference_is_never_resolved(caplog):
    resolved: list[str] = []

    def resolve(reference: str) -> str:
        resolved.append(reference)
        return "server-env-value"

    rules = redaction.parse_rules([{"name": "ops", "reference": "${env:DATABASE_URL}"}], "org")
    with caplog.at_level(logging.ERROR, logger="provisa.security.redaction"):
        redaction.set_org("held-org", rules, {}, resolve, server_references=False)
    assert resolved == []
    assert "'ops'" in caplog.text
    assert redaction.described("held-org")["unresolved"] == ["ops"]
    assert redaction.redact("server-env-value", "held-org") == "server-env-value"


def test_the_save_path_refuses_a_held_orgs_env_reference(monkeypatch):
    from provisa.api.admin import redaction_router
    from provisa.api.errors import ApiError

    monkeypatch.setattr(redaction_router, "require_org_settings", lambda _r: None)
    monkeypatch.setattr(redaction_router, "require_active_org_id", lambda _r: "held-org")
    monkeypatch.setattr(redaction_router, "held", lambda org: org == "held-org")
    body = redaction_router.RulesBody(
        rules=[redaction_router.RuleBody(name="ops", reference="${env:DATABASE_URL}")]
    )
    with pytest.raises(ApiError) as caught:
        asyncio.run(redaction_router.put_org_redaction(body, SimpleNamespace()))  # type: ignore[arg-type]
    assert caught.value.status_code == 400
    assert caught.value.code == "redaction.server_reference"
    assert caught.value.params["rule"] == "ops"


def test_the_save_path_refuses_a_catastrophic_pattern_by_name(monkeypatch):
    from provisa.api.admin import redaction_router
    from provisa.api.errors import ApiError

    monkeypatch.setattr(redaction_router, "require_org_settings", lambda _r: None)
    monkeypatch.setattr(redaction_router, "require_active_org_id", lambda _r: "org-a")
    monkeypatch.setattr(redaction_router, "held", lambda _org: False)
    body = redaction_router.RulesBody(
        rules=[redaction_router.RuleBody(name="bad", pattern="(a+)+$")]
    )
    with pytest.raises(ApiError) as caught:
        asyncio.run(redaction_router.put_org_redaction(body, SimpleNamespace()))  # type: ignore[arg-type]
    assert caught.value.code == "redaction.pattern_backtracks"
    assert caught.value.params["rule"] == "bad"
