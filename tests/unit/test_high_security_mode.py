# Copyright (c) 2026 Kenneth Stott
# Canary: 8fbf492f-f521-4f91-93ef-9e4909052f90
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""High-security mode gating unit tests (REQ-693) + @encrypted SDL marking (REQ-692)."""

from __future__ import annotations

from types import SimpleNamespace

from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from provisa.api.data.encrypted_directive import (
    ENCRYPTED_DIRECTIVE_SDL,
    annotate_encrypted_sdl,
    collect_encrypted_fields,
)
from provisa.core.models import SecurityConfig
from provisa.security.high_security import (
    HighSecurityMiddleware,
    high_security_reject,
    is_high_security,
    pgwire_start_allowed,
)


# -- config model -----------------------------------------------------------------------------


def test_security_config_defaults_standard():
    assert SecurityConfig().high is False
    assert SecurityConfig(mode="high").high is True
    assert SecurityConfig(mode="HIGH").high is True


# -- pgwire off (REQ-693) ---------------------------------------------------------------------


def test_pgwire_not_started_in_high_mode():
    high = SimpleNamespace(security_high=True)
    std = SimpleNamespace(security_high=False)
    assert pgwire_start_allowed(std, 5439) is True
    assert pgwire_start_allowed(high, 5439) is False  # refused
    assert pgwire_start_allowed(std, 0) is False  # no port configured


def test_is_high_security_reads_state():
    assert is_high_security(SimpleNamespace(security_high=True)) is True
    assert is_high_security(SimpleNamespace()) is False


# -- data-endpoint 403 / KMS gate (REQ-693) ---------------------------------------------------


class _Headers:
    def __init__(self, d):
        self._d = {k.lower(): v for k, v in d.items()}

    def get(self, k, default=None):
        return self._d.get(k.lower(), default)


def test_reject_data_endpoint_without_kms():
    assert high_security_reject("/data/sql", _Headers({})) is not None
    assert high_security_reject("/data/graphql", _Headers({})) is not None
    assert high_security_reject("/data/rest/pets", _Headers({})) is not None


def test_allow_data_endpoint_with_kms_key():
    assert high_security_reject("/data/sql", _Headers({"X-Provisa-KMS-Key": "arn"})) is None


def test_metadata_endpoints_reachable():
    # Clients must fetch the SDL to learn @encrypted fields before connecting.
    assert high_security_reject("/data/sdl", _Headers({})) is None
    assert high_security_reject("/data/introspection", _Headers({})) is None
    assert high_security_reject("/health", _Headers({})) is None


def _build_app(security_high: bool) -> TestClient:
    async def sql(request):
        return JSONResponse({"ok": True})

    async def sdl(request):
        return JSONResponse({"ok": True})

    app = Starlette(routes=[Route("/data/sql", sql, methods=["POST"]), Route("/data/sdl", sdl)])
    app.add_middleware(HighSecurityMiddleware, state=SimpleNamespace(security_high=security_high))
    return TestClient(app)


def test_middleware_403_for_data_without_kms_in_high_mode():
    client = _build_app(security_high=True)
    r = client.post("/data/sql", json={})
    assert r.status_code == 403
    assert "high-security" in r.json()["detail"]


def test_middleware_allows_data_with_kms_in_high_mode():
    client = _build_app(security_high=True)
    r = client.post("/data/sql", json={}, headers={"X-Provisa-KMS-Key": "arn:kms:key"})
    assert r.status_code == 200


def test_middleware_allows_sdl_metadata_in_high_mode():
    client = _build_app(security_high=True)
    assert client.get("/data/sdl").status_code == 200


def test_middleware_noop_in_standard_mode():
    client = _build_app(security_high=False)
    assert client.post("/data/sql", json={}).status_code == 200


# -- @encrypted directive marking (REQ-692) ---------------------------------------------------


def test_collect_encrypted_fields_from_config():
    col_enc = SimpleNamespace(name="ssn", alias=None, encrypted=True)
    col_alias = SimpleNamespace(name="email_raw", alias="email", encrypted=True)
    col_plain = SimpleNamespace(name="id", alias=None, encrypted=False)
    table = SimpleNamespace(columns=[col_enc, col_alias, col_plain])
    config = SimpleNamespace(tables=[table])
    assert collect_encrypted_fields(config) == {"ssn", "email"}


def test_annotate_encrypted_sdl_marks_fields():
    sdl = "type Employee {\n  id: Int\n  ssn: String\n  name: String\n}"
    out = annotate_encrypted_sdl(sdl, {"ssn"})
    assert "ssn: String @encrypted" in out
    assert "id: Int\n" in out  # untouched
    assert ENCRYPTED_DIRECTIVE_SDL in out


def test_annotate_encrypted_sdl_noop_without_fields():
    sdl = "type X { a: Int }"
    assert annotate_encrypted_sdl(sdl, set()) == sdl
