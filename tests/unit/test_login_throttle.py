# Copyright (c) 2026 Kenneth Stott
# Canary: 9a4e21b7-0f6c-4d3a-8b5e-c17d2f90a683
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1393 — login throttling and lockout across every credential-validating surface.

Two things are asserted here. The counter itself: n failures inside the window lock the subject
out, the lockout expires, a success clears it, and an infrastructure fault is not counted. And the
placement: because the counter sits at the credential-validation layer rather than on one protocol,
failures made over HTTP, pgwire and Bolt accumulate against the SAME subject — so an attacker
cannot reset their allowance by switching protocol. That cross-surface test is the point of the
whole design, and it fails the moment a surface authenticates without going through the layer.
"""

from __future__ import annotations

import asyncio
import base64
import threading
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

import bcrypt
import pytest

from provisa.auth.throttle import (
    LockedOut,
    LoginThrottle,
    configure_login_throttle,
    login_throttle,
    reset_login_throttle,
    subject_key,
    throttled,
)

_SECRET = "throttle-test-signing-key-at-least-32-bytes"
_USERNAME = "alice"
_PASSWORD = "s3cret"


# --- the counter -------------------------------------------------------------------------------


class _Clock:
    """A hand-wound clock, so a fifteen-minute lockout is testable in microseconds."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock() -> _Clock:
    return _Clock()


@pytest.fixture
def throttle(clock) -> LoginThrottle:
    return LoginThrottle(max_attempts=3, window_seconds=60, lockout_seconds=300, clock=clock)


def test_failures_below_the_limit_do_not_lock(throttle):
    for _ in range(2):
        throttle.record_failure("user:alice")
    throttle.check("user:alice")  # does not raise


def test_the_nth_failure_locks_the_subject_out(throttle):
    for _ in range(3):
        throttle.record_failure("user:alice")

    with pytest.raises(LockedOut) as exc_info:
        throttle.check("user:alice")
    assert exc_info.value.retry_after <= 300


def test_a_lockout_is_scoped_to_its_subject(throttle):
    """One account being guessed at must not deny service to every other account."""
    for _ in range(3):
        throttle.record_failure("user:alice")

    throttle.check("user:bob")  # does not raise


def test_failures_outside_the_window_do_not_accumulate(throttle, clock):
    """Two failures a day apart are a typo twice over, not an attack."""
    throttle.record_failure("user:alice")
    throttle.record_failure("user:alice")
    clock.advance(61)
    throttle.record_failure("user:alice")

    throttle.check("user:alice")  # does not raise


def test_the_lockout_expires(throttle, clock):
    for _ in range(3):
        throttle.record_failure("user:alice")
    clock.advance(301)

    throttle.check("user:alice")  # does not raise


def test_an_expired_lockout_restores_the_full_allowance(throttle, clock):
    """The failures that caused the lockout are cleared with it.

    Otherwise a subject would emerge from a lockout one attempt from the next one, and a user who
    mistyped their password three times in the morning could never log in again that hour.
    """
    for _ in range(3):
        throttle.record_failure("user:alice")
    clock.advance(301)
    throttle.check("user:alice")

    throttle.record_failure("user:alice")
    throttle.record_failure("user:alice")
    throttle.check("user:alice")  # does not raise — 2 of 3, not 5 of 3


def test_a_success_clears_the_history(throttle):
    throttle.record_failure("user:alice")
    throttle.record_failure("user:alice")
    throttle.record_success("user:alice")
    throttle.record_failure("user:alice")
    throttle.record_failure("user:alice")

    throttle.check("user:alice")  # does not raise


def test_the_counter_is_safe_under_concurrent_surfaces(clock):
    """pgwire authenticates on socketserver worker threads while the API loop authenticates too."""
    throttle = LoginThrottle(max_attempts=1000, window_seconds=60, lockout_seconds=300, clock=clock)
    threads = [
        threading.Thread(target=lambda: [throttle.record_failure("user:alice") for _ in range(50)])
        for _ in range(8)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(throttle._failures["user:alice"]) == 400


# --- the wrapper -------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_throttled_counts_a_rejected_credential_and_then_refuses_to_ask():
    configure_login_throttle(
        {"login_throttle": {"max_attempts": 2, "window_seconds": 60, "lockout_seconds": 300}}
    )
    calls = []

    async def validator(credential):
        calls.append(credential)
        raise ValueError("Invalid credentials")

    for _ in range(2):
        with pytest.raises(ValueError):
            await throttled(validator, "bad", principal=_USERNAME)

    with pytest.raises(LockedOut):
        await throttled(validator, "bad", principal=_USERNAME)
    # The third attempt never reached the validator — that is what a lockout has to mean, or the
    # brake still costs a bcrypt round per guess.
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_an_infrastructure_fault_is_not_counted_as_a_failed_login():
    """A JWKS outage or a dead control plane must not lock every user out of the deployment."""
    configure_login_throttle(
        {"login_throttle": {"max_attempts": 2, "window_seconds": 60, "lockout_seconds": 300}}
    )

    async def validator(credential):
        del credential
        raise ConnectionError("identity provider unreachable")

    for _ in range(5):
        with pytest.raises(ConnectionError):
            await throttled(validator, "token", principal=_USERNAME)

    login_throttle().check(subject_key(_USERNAME, "token"))  # does not raise


@pytest.mark.asyncio
async def test_a_bearer_only_surface_keys_on_the_credential_not_the_caller():
    """No principal on the wire, so one caller's bad token cannot lock out a different one."""
    configure_login_throttle(
        {"login_throttle": {"max_attempts": 2, "window_seconds": 60, "lockout_seconds": 300}}
    )

    async def validator(credential):
        del credential
        raise ValueError("Invalid token")

    for _ in range(2):
        with pytest.raises(ValueError):
            await throttled(validator, "token-a", principal=None)

    with pytest.raises(LockedOut):
        await throttled(validator, "token-a", principal=None)
    with pytest.raises(ValueError):
        await throttled(validator, "token-b", principal=None)


def test_reconfiguring_with_the_same_settings_keeps_the_counts():
    """build_auth_provider runs per connection on the wire surfaces.

    Rebuilding the throttle there would zero the counter on every reconnect, which is exactly what
    an attacker does between guesses.
    """
    config = {"login_throttle": {"max_attempts": 3, "window_seconds": 60, "lockout_seconds": 300}}
    first = configure_login_throttle(config)
    first.record_failure("user:alice")

    second = configure_login_throttle(dict(config))

    assert second is first
    assert len(second._failures["user:alice"]) == 1


def test_the_throttle_is_on_without_configuration():
    """REQ-1393: an opt-in brake protects only the deployments that did not need protecting."""
    reset_login_throttle()
    throttle = login_throttle()

    assert throttle.max_attempts > 0
    assert throttle.lockout_seconds > 0


# --- the placement: one lockout, every surface --------------------------------------------------


def _auth_config() -> dict:
    return {
        "provider": "simple",
        "allow_simple_auth": True,
        "jwt_secret": _SECRET,
        "simple": {
            "users": [
                {
                    "username": _USERNAME,
                    "password_hash": bcrypt.hashpw(
                        _PASSWORD.encode(), bcrypt.gensalt(rounds=4)
                    ).decode(),
                    "roles": ["analyst"],
                }
            ]
        },
        "role_mapping": [],
        "default_role": "analyst",
        "login_throttle": {"max_attempts": 3, "window_seconds": 60, "lockout_seconds": 300},
    }


def _state(auth_config: dict) -> SimpleNamespace:
    return SimpleNamespace(
        auth_config=auth_config,
        auth_middleware_active=True,
        multitenancy=False,
        admin_db=None,
    )


@pytest.fixture
def pgwire_loop():
    """pgwire authenticates on a worker thread and submits validators to the main loop."""
    import provisa.pgwire.server as pg_server

    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    previous = pg_server._loop
    pg_server._loop = loop
    try:
        yield loop
    finally:
        pg_server._loop = previous
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
        loop.close()


def _pgwire_attempt(auth_config: dict, password: str) -> list[tuple]:
    """One pgwire startup-packet attempt; returns the FATAL frames the handler wrote."""
    from provisa.pgwire.server import ProvisaHandler

    errors: list[tuple] = []
    handler = object.__new__(ProvisaHandler)
    handler._send_pg_error = lambda severity, sqlstate, message: errors.append(
        (severity, sqlstate, message)
    )
    handler.send_authentication_ok = lambda: None
    # A real handler's wfile is the CountingWriter that meters egress; a successful auth binds the
    # org onto it (REQ-1452), so the double has to carry it.
    handler.wfile = cast(Any, SimpleNamespace(bind_org=lambda _org_id: None))
    handler.handle_post_auth = lambda ctx: None  # noqa: ARG005 — signature match
    ctx = cast(
        Any, SimpleNamespace(params={"user": _USERNAME}, session=SimpleNamespace(org_id=None))
    )
    with patch("provisa.pgwire.server.state", _state(auth_config)):
        handler.handle_md5_password(ctx, password.encode("utf-8") + b"\x00")
    return errors


async def _bolt_attempt(auth_config: dict, password: str):
    from provisa.bolt.session import BoltSession

    return await BoltSession._authenticate(_state(auth_config), "basic", _USERNAME, password)


def _http_client(auth_config: dict):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from provisa.auth.middleware import AuthMiddleware
    from provisa.auth.wiring import build_auth_provider

    app = FastAPI()

    @app.get("/data/probe")
    async def probe():
        return {"ok": True}

    app.add_middleware(
        AuthMiddleware,
        provider=build_auth_provider(auth_config),
        mapping_rules=[],
        default_role="analyst",
        db_pool=None,
        admin_pool=None,
    )
    return TestClient(app)


def _basic_header(password: str) -> dict:
    return {
        "Authorization": "Basic " + base64.b64encode(f"{_USERNAME}:{password}".encode()).decode()
    }


def test_one_bad_password_per_surface_locks_the_account_on_all_of_them(pgwire_loop):
    """The design claim, asserted end to end.

    Three failures — one over HTTP, one over pgwire, one over Bolt — reach the limit that no single
    surface reached on its own, and the next attempt on ANY surface is refused as a lockout rather
    than as a wrong password. Were the counter per surface, each would still be on its first
    attempt here and every assertion below would read 401 / 28P01.
    """
    del pgwire_loop
    auth_config = _auth_config()
    client = _http_client(auth_config)

    assert client.get("/data/probe", headers=_basic_header("wrong")).status_code == 401
    assert _pgwire_attempt(auth_config, "wrong")[0][1] == "28P01"
    assert asyncio.run(_bolt_attempt(auth_config, "wrong")) is None

    locked = client.get("/data/probe", headers=_basic_header("wrong"))
    assert locked.status_code == 429
    assert int(locked.headers["Retry-After"]) > 0

    severity, sqlstate, message = _pgwire_attempt(auth_config, "wrong")[0]
    assert (severity, sqlstate) == ("FATAL", "28000")
    assert "too many failed authentication attempts" in message

    with pytest.raises(LockedOut):
        asyncio.run(_bolt_attempt(auth_config, "wrong"))


def test_the_lockout_refuses_the_correct_password_too(pgwire_loop):
    """A lockout that the right password walks through is not a lockout — an attacker who guesses
    it on attempt n+1 is admitted."""
    del pgwire_loop
    auth_config = _auth_config()
    client = _http_client(auth_config)

    for _ in range(3):
        client.get("/data/probe", headers=_basic_header("wrong"))

    assert client.get("/data/probe", headers=_basic_header(_PASSWORD)).status_code == 429
    assert _pgwire_attempt(auth_config, _PASSWORD)[0][1] == "28000"


def test_a_good_password_still_authenticates_under_the_throttle(pgwire_loop):
    """The brake must not be a wall: an unremarkable login is untouched."""
    del pgwire_loop
    auth_config = _auth_config()

    assert _pgwire_attempt(auth_config, _PASSWORD) == []
    identity = asyncio.run(_bolt_attempt(auth_config, _PASSWORD))
    assert identity is not None
    assert identity.user_id == _USERNAME


def test_the_login_route_counts_into_the_same_store():
    """/auth/login is on the middleware skip list, so it needs its own entry into the throttle —
    and it must be the same store, or the route is an unmetered guessing oracle."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    import provisa.auth.providers.simple as simple_mod
    from provisa.auth.wiring import build_auth_provider

    auth_config = _auth_config()
    provider = build_auth_provider(auth_config)
    app = FastAPI()
    app.include_router(simple_mod.router)

    from provisa.api.errors import ApiError

    @app.exception_handler(ApiError)
    async def _api_error(request, exc):  # noqa: ARG001 — FastAPI handler signature
        from fastapi.responses import JSONResponse

        return JSONResponse(status_code=exc.status_code, content={"detail": str(exc)})

    with patch.object(simple_mod, "_provider_instance", provider):
        client = TestClient(app)
        body = {"username": _USERNAME, "password": "wrong"}
        for _ in range(3):
            assert client.post("/auth/login", json=body).status_code == 401

        assert client.post("/auth/login", json=body).status_code == 429
        # The same account is now locked on a wire surface it never touched.
        assert (
            _http_client(auth_config)
            .get("/data/probe", headers=_basic_header(_PASSWORD))
            .status_code
            == 429
        )
