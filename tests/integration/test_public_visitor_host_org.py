# Copyright (c) 2026 Kenneth Stott
# Canary: 535110f3-1a79-4db3-8512-91606475f312
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1058: a request with no credential is bound to the org its Host subdomain names.

Served over a real HTTP server, with the org registry in Postgres: the visitor of an existing
org's host is bound to that org as its public role; a host naming no org, or an org that does not
exist, is refused 401 as before; a credential on the org's host resolves as it always did.
"""

from __future__ import annotations

import base64
import os
import socket
import threading
import time

import bcrypt
import httpx
import pytest
from fastapi import FastAPI, Request
from sqlalchemy import create_engine, insert, text

from provisa.auth.middleware import AuthMiddleware
from provisa.auth.providers.basic import BasicAuthProvider
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import REGISTRY_TABLES, local_users, orgs
from provisa.core.schema_admin import metadata as admin_metadata
from provisa.security.rights import PUBLIC_ROLE

pytestmark = [pytest.mark.integration, pytest.mark.deployment_org("root")]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_URL = f"postgresql+psycopg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_ADMIN_SCHEMA = "test_req1058_admin"
_BASE = "provisa.org"
_ORG = "acme"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def server(monkeypatch):
    monkeypatch.setenv("VITE_ORG_BASE_DOMAIN", _BASE)
    sync = create_engine(_URL, pool_pre_ping=True)
    with sync.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {_ADMIN_SCHEMA}"))
        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        admin_metadata.create_all(conn, tables=REGISTRY_TABLES)
        conn.execute(insert(orgs).values(id="root", name="Root", created_by="system"))
        conn.execute(insert(orgs).values(id=_ORG, name="Acme", created_by="system"))
        conn.execute(
            insert(local_users).values(
                id="member",
                username="member",
                password_hash=bcrypt.hashpw(b"pw", bcrypt.gensalt()).decode(),
                email="member@example.com",
                display_name="member",
                is_active=True,
            )
        )
    admin_db = Database(create_engine_from_url(_URL), name="admin", search_path=_ADMIN_SCHEMA)

    app = FastAPI()
    app.add_middleware(
        AuthMiddleware,
        provider=BasicAuthProvider(admin_db),
        admin_pool=admin_db,
        multitenancy=True,
        default_org_id="root",
    )

    @app.get("/whoami")
    async def _whoami(request: Request):  # noqa: RUF029 — Starlette route must be async
        return {
            "user_id": request.state.identity.user_id,
            "role": request.state.role,
            "active_org_id": request.state.active_org_id,
        }

    import uvicorn

    port = _free_port()
    srv = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=srv.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and not srv.started:
        time.sleep(0.1)
    if not srv.started:
        raise RuntimeError("the server did not start within 60s")
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        srv.should_exit = True
        thread.join(timeout=10)
        admin_db.engine.dispose()
        with sync.begin() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        sync.dispose()


def _get(base: str, host: str, headers: dict[str, str] | None = None) -> httpx.Response:
    return httpx.get(f"{base}/whoami", headers={"host": host, **(headers or {})}, timeout=30)


def test_tokenless_request_on_an_org_host_is_that_orgs_public_visitor(server):
    resp = _get(server, f"{_ORG}.{_BASE}")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["active_org_id"] == _ORG
    assert body["role"] == PUBLIC_ROLE
    assert body["user_id"] != "anonymous"


def test_tokenless_request_on_a_host_naming_no_org_is_refused(server):
    assert _get(server, _BASE).status_code == 401
    assert _get(server, f"cloud.{_BASE}", {"x-org-provisa": _ORG}).status_code == 401


def test_tokenless_request_on_an_unknown_orgs_host_is_refused(server):
    assert _get(server, f"ghost.{_BASE}").status_code == 401


def test_a_credential_on_the_org_host_is_not_a_visitor(server):
    basic = "Basic " + base64.b64encode(b"member:pw").decode()
    resp = _get(server, f"{_ORG}.{_BASE}", {"Authorization": basic})
    # The member belongs to no org here, so the credential path refuses them by membership --
    # the visitor binding never applies to a request that presents a credential.
    assert resp.status_code == 403, resp.text
    assert "Not a member" in resp.text
