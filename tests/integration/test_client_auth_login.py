# Copyright (c) 2026 Kenneth Stott
# Canary: d39218da-e1b0-4ab6-8158-a120bd90fc20
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The clients' /auth/login exchange, against a real HTTP server.

REQ-124/REQ-AK5. The login response field is ``access_token``; both clients used to read
``token``, so the exchange yielded None on success and every "authenticated" connection
quietly sent no Authorization header at all. These tests run the real router over a real
socket so the field name is checked where it is actually read.
"""

from __future__ import annotations

import contextlib
import threading
import time

import bcrypt
import pytest
from fastapi import FastAPI

from tests.platform_plane import platform_db

pytestmark = [pytest.mark.integration, pytest.mark.e2e]

_PASSWORD = "pw-login-test"
_USERNAME = "analyst-user"
_JWT_SECRET = "test-secret-for-the-login-exchange-32b"  # >= 32 bytes: HS256 minimum


def _free_port() -> int:
    from tests.port_lease import lease_port

    return lease_port()


@pytest.fixture()
def login_server(monkeypatch):
    """A real uvicorn server serving /auth/login for a deployment configured with the simple
    provider, read from the app state the way a started deployment holds it."""
    import uvicorn

    from provisa.api.app import state
    from provisa.auth.login_router import router as login_router

    monkeypatch.setattr(
        state,
        "auth_config",
        {
            "provider": "simple",
            "allow_simple_auth": True,
            "jwt_secret": _JWT_SECRET,
            "simple": {
                "users": [
                    {
                        "username": _USERNAME,
                        "password_hash": bcrypt.hashpw(
                            _PASSWORD.encode("utf-8"), bcrypt.gensalt()
                        ).decode("utf-8"),
                        "roles": ["analyst"],
                    }
                ]
            },
        },
    )
    # The simple provider keeps its users' ids on the platform plane.
    monkeypatch.setattr(state, "admin_db", platform_db())
    app = FastAPI()
    app.include_router(login_router)

    port = _free_port()
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if server.started:
                break
            time.sleep(0.1)
        if not server.started:
            raise RuntimeError(f"login server did not start on {port} within 120s")
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=10)


def test_dbapi_login_returns_the_access_token(login_server):
    from provisa_client.dbapi import _auth_login

    token, _role = _auth_login(login_server, _USERNAME, _PASSWORD)

    assert token, "login must yield the access_token from the response body"


def test_adbc_login_returns_the_access_token(login_server):
    from provisa_client.adbc import _auth_login

    token, _role = _auth_login(login_server, _USERNAME, _PASSWORD)

    assert token, "login must yield the access_token from the response body"


def test_dbapi_connection_sends_the_bearer_header(login_server):
    from provisa_client import dbapi

    conn = dbapi.connect(login_server, username=_USERNAME, password=_PASSWORD)
    with contextlib.closing(conn):
        headers = conn._headers()

    assert headers["Authorization"].startswith("Bearer ")


def test_bad_password_yields_no_token(login_server):
    from provisa_client.dbapi import _auth_login

    token, _role = _auth_login(login_server, _USERNAME, "wrong-password")

    assert token is None
