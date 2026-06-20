# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-268/269/273 — client no longer forces a role or sends a connection mode.

The role is an optional, server-validated request (X-Provisa-Role), never a client-assumed
identity, and there is no `mode` parameter anywhere.
"""

import inspect

from provisa_client.dbapi import Connection, connect
from provisa_client.adbc import AdbcConnection, adbc_connect


# --- DB-API ---


def test_connect_has_no_mode_param():
    params = inspect.signature(connect).parameters
    assert "mode" not in params
    assert params["role"].default is None  # optional, not "admin"


def test_headers_send_x_provisa_role_when_set():
    conn = Connection(base_url="http://x", token="tok", role="analyst")
    h = conn._headers()
    assert h["X-Provisa-Role"] == "analyst"
    assert "X-Role" not in h  # the non-spec header is gone
    assert h["Authorization"] == "Bearer tok"


def test_headers_omit_role_when_unset():
    conn = Connection(base_url="http://x", token="tok", role=None)
    h = conn._headers()
    assert "X-Provisa-Role" not in h
    assert "X-Role" not in h


def test_connection_has_no_mode_attr():
    conn = Connection(base_url="http://x", token=None, role=None)
    assert not hasattr(conn, "_mode")


# --- ADBC / Flight ---


def test_adbc_connect_has_no_mode_param():
    params = inspect.signature(adbc_connect).parameters
    assert "mode" not in params
    assert params["role"].default is None


def test_adbc_ticket_drops_mode_and_omits_unset_role():
    import json

    conn = AdbcConnection(flight_client=object(), role=None, token="tok", base_url="http://x")
    from provisa_client.adbc import AdbcCursor

    ticket = AdbcCursor(connection=conn)._build_ticket("SELECT 1")
    data = json.loads(ticket.ticket)
    assert "mode" not in data
    assert "role" not in data  # omitted when unset
    assert data["token"] == "tok"


def test_adbc_ticket_includes_requested_role():
    import json

    from provisa_client.adbc import AdbcCursor

    conn = AdbcConnection(flight_client=object(), role="analyst", token=None, base_url="http://x")
    ticket = AdbcCursor(connection=conn)._build_ticket("SELECT 1")
    data = json.loads(ticket.ticket)
    assert data["role"] == "analyst"
    assert "mode" not in data


# --- SQLAlchemy dialect ---


def test_sqlalchemy_connect_args_drop_mode():
    from types import SimpleNamespace

    from provisa_client.sqlalchemy_dialect import ProvisaDialect

    url = SimpleNamespace(
        drivername="provisa",
        host="h",
        port=8001,
        username="u",
        password="p",
        query={"role": "analyst", "mode": "approved"},
    )
    _, opts = ProvisaDialect().create_connect_args(url)
    assert "mode" not in opts
    assert opts["role"] == "analyst"


def test_sqlalchemy_connect_args_omit_role_when_absent():
    from types import SimpleNamespace

    from provisa_client.sqlalchemy_dialect import ProvisaDialect

    url = SimpleNamespace(
        drivername="provisa",
        host="h",
        port=8001,
        username="u",
        password="p",
        query={},
    )
    _, opts = ProvisaDialect().create_connect_args(url)
    assert "role" not in opts
    assert "mode" not in opts


# --- Server: auth middleware role validation (REQ-273) ---

import pytest  # noqa: E402

from provisa.auth.middleware import AuthMiddleware  # noqa: E402
from provisa.auth.models import AuthIdentity  # noqa: E402


class _State:
    pass


class _URL:
    def __init__(self, path):
        self.path = path


class _Req:
    def __init__(self, headers):
        self.headers = headers
        self.url = _URL("/data/graphql")
        self.state = _State()


async def _next(_req):
    return "OK"


class _Provider:
    auth_scheme = "bearer"

    def __init__(self, roles):
        self._roles = roles

    async def validate_token(self, _token):
        return AuthIdentity(
            user_id="u", email=None, display_name="U", roles=self._roles, raw_claims={}
        )


@pytest.mark.asyncio
async def test_unsecured_honors_any_requested_role():
    # REQ-273 caveat: no auth provider → client-supplied role is taken at face value.
    mw = AuthMiddleware(app=None, provider=None)
    req = _Req({"x-provisa-role": "steward"})
    out = await mw.dispatch(req, _next)
    assert out == "OK"
    assert req.state.role == "steward"


@pytest.mark.asyncio
async def test_unsecured_defaults_admin_without_header():
    mw = AuthMiddleware(app=None, provider=None)
    req = _Req({})
    await mw.dispatch(req, _next)
    assert req.state.role == "admin"


@pytest.mark.asyncio
async def test_secured_honors_assigned_requested_role():
    mw = AuthMiddleware(app=None, provider=_Provider(["analyst", "viewer"]), default_role="analyst")
    req = _Req({"authorization": "Bearer tok", "x-provisa-role": "viewer"})
    out = await mw.dispatch(req, _next)
    assert out == "OK"
    assert req.state.role == "viewer"


@pytest.mark.asyncio
async def test_secured_rejects_unassigned_requested_role():
    mw = AuthMiddleware(app=None, provider=_Provider(["analyst", "viewer"]), default_role="analyst")
    req = _Req({"authorization": "Bearer tok", "x-provisa-role": "admin"})
    out = await mw.dispatch(req, _next)
    # a role the user does not hold is rejected, not honored
    assert getattr(out, "status_code", None) == 403
