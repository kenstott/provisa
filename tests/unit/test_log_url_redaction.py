# Copyright (c) 2026 Kenneth Stott
# Canary: 270d2847-7551-48da-bb8f-2b90b44148b5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1575: no log line carries a credential embedded in a URL.

A connection URL -- Redis, PostgreSQL, Trino, an S3 endpoint, a webhook -- can hold a password in
its userinfo or a token in its query. Every logging call that formats a URL routes it through
``redact_url_password`` first; the guard below keeps a new call from formatting one raw.
"""

from __future__ import annotations

import ast
import logging
import re
from pathlib import Path

import pytest

from provisa.api.admin.secret_redaction import redact_url_password

SECRET = "s3cr3t-pw-value"


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        (f"redis://user:{SECRET}@cache.internal:6379/0", "redis://user@cache.internal:6379/0"),
        (f"redis://:{SECRET}@cache.internal:6379/0", "redis://@cache.internal:6379/0"),
        (f"rediss://default:{SECRET}@cache.internal/0", "rediss://default@cache.internal/0"),
        (
            f"postgresql://app:{SECRET}@db.internal:5432/provisa?sslmode=require",
            "postgresql://app@db.internal:5432/provisa?sslmode=require",
        ),
        (f"https://hooks.example.com/in?token={SECRET}", "https://hooks.example.com/in"),
        (
            f"https://hooks.example.com/in?a=1&password={SECRET}&b=2",
            "https://hooks.example.com/in?a=1&b=2",
        ),
        (f"https://api.example.com/v1?key={SECRET}", "https://api.example.com/v1"),
        (f"https://api.example.com/v1?api_key={SECRET}#frag", "https://api.example.com/v1#frag"),
        ("redis://cache.internal:6379/0", "redis://cache.internal:6379/0"),
        (
            "https://example.com/path?keyspace=a&page=2",
            "https://example.com/path?keyspace=a&page=2",
        ),
        ("http://localhost:3000/?tour=1", "http://localhost:3000/?tour=1"),
    ],
)
def test_redact_url_password_cases(url: str, expected: str) -> None:
    assert redact_url_password(url) == expected


def test_apq_cache_log_line_carries_no_redis_password(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from provisa.api import app_startup
    from provisa.api.app import state

    monkeypatch.setattr(state, "redis_url", f"redis://default:{SECRET}@cache.internal:6379/0")
    monkeypatch.setattr(state, "apq_ttl", 60)
    monkeypatch.setattr(state, "apq_cache", None)
    log = logging.getLogger("test.apq.redaction")
    with caplog.at_level(logging.INFO, logger=log.name):
        app_startup._start_apq_cache(log)

    lines = [r.getMessage() for r in caplog.records if r.name == log.name]
    assert any(line.startswith("APQ cache initialized") for line in lines), lines
    assert all(SECRET not in line for line in lines), lines
    assert any("redis://default@cache.internal:6379/0" in line for line in lines), lines


# --- guard: no logging call formats a URL-named value without the redaction helper -----------

_PROVISA = Path(__file__).resolve().parents[2] / "provisa"
_LOG_METHODS = {"debug", "info", "warning", "warn", "error", "exception", "critical", "log"}
_URL_TOKENS = {"url", "urls", "uri", "uris", "dsn", "dsns"}
_URL_SUFFIX = re.compile(r"(url|uri|dsn)s?$", re.IGNORECASE)
#: Attributes of a parsed URL that hold no credential: reading one of them is not a URL.
_SAFE_ATTRS = {"path", "hostname", "scheme", "port"}
_HELPER = "redact_url_password"
#: (path relative to provisa/, line) of a logging call allowed to format a URL-named value raw.
#: This list only shrinks.
_ALLOWED: set[tuple[str, int]] = set()


def _url_named(name: str) -> bool:
    return bool(_URL_TOKENS & set(name.lower().split("_"))) or bool(_URL_SUFFIX.search(name))


def _receiver(func: ast.Attribute) -> str:
    value = func.value
    if isinstance(value, ast.Name):
        return value.id
    if isinstance(value, ast.Attribute):
        return value.attr
    if isinstance(value, ast.Call):
        inner = value.func
        return inner.attr if isinstance(inner, ast.Attribute) else getattr(inner, "id", "")
    return ""


def _is_log_call(call: ast.Call) -> bool:
    func = call.func
    if isinstance(func, ast.Name):
        return func.id == "print"
    return (
        isinstance(func, ast.Attribute)
        and func.attr in _LOG_METHODS
        and "log" in _receiver(func).lower()
    )


def _raw_urls(node: ast.AST) -> list[str]:
    found: list[str] = []

    def walk(n: ast.AST) -> None:
        if isinstance(n, ast.Call):
            callee = n.func
            name = callee.attr if isinstance(callee, ast.Attribute) else getattr(callee, "id", "")
            if name == _HELPER:
                return
        if isinstance(n, ast.Attribute) and n.attr in _SAFE_ATTRS:
            return
        if isinstance(n, ast.Name) and _url_named(n.id):
            found.append(n.id)
            return
        if isinstance(n, ast.Attribute) and _url_named(n.attr):
            found.append(ast.unparse(n))
            return
        if (
            isinstance(n, ast.Subscript)
            and isinstance(n.slice, ast.Constant)
            and isinstance(n.slice.value, str)
            and _url_named(n.slice.value)
        ):
            found.append(ast.unparse(n))
            return
        for child in ast.iter_child_nodes(n):
            walk(child)

    walk(node)
    return found


def _offenders() -> dict[tuple[str, int], list[str]]:
    out: dict[tuple[str, int], list[str]] = {}
    for path in sorted(_PROVISA.rglob("*.py")):
        rel = path.relative_to(_PROVISA).as_posix()
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"), filename=str(path))):
            if not (isinstance(node, ast.Call) and _is_log_call(node)):
                continue
            formatted = [*node.args] + [
                k.value for k in node.keywords if k.arg not in {"exc_info", "stack_info", "extra"}
            ]
            raw = [u for arg in formatted for u in _raw_urls(arg)]
            if raw:
                out[(rel, node.lineno)] = raw
    return out


def test_no_log_call_formats_a_url_without_redaction() -> None:
    offenders = {k: v for k, v in _offenders().items() if k not in _ALLOWED}
    assert not offenders, "\n".join(
        f"provisa/{rel}:{line}: {names} -- wrap in {_HELPER}()"
        for (rel, line), names in sorted(offenders.items())
    )


def test_allow_list_has_no_stale_entries() -> None:
    assert _ALLOWED <= set(_offenders()), sorted(_ALLOWED - set(_offenders()))


def test_guard_catches_a_raw_url_and_passes_a_redacted_one() -> None:
    raw = ast.parse('log.info("at %s", self.redis_url)', mode="eval").body
    wrapped = ast.parse('log.info("at %s", redact_url_password(self.redis_url))', mode="eval").body
    assert isinstance(raw, ast.Call) and isinstance(wrapped, ast.Call)
    assert _is_log_call(raw) and _raw_urls(raw) == ["self.redis_url"]
    assert _raw_urls(wrapped) == []
