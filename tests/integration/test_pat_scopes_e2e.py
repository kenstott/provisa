# Copyright (c) 2026 Kenneth Stott
# Canary: 2f7b0c64-5a19-4e83-b6d2-9c40e1a7f358
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a personal access token's scopes are the roles it may use (REQ-1263, REQ-2076).

One real isolated server with an auth provider whose sign-in carries the user's roles. The user
signs in over HTTP, issues a token through the API for one of the roles they hold, and uses it
over HTTP and over pgwire: it acts as that role, and naming another role its owner holds is
refused on both. A token for a role the user does not hold, or for no role, is not issued.
"""

# Requirements: REQ-1263, REQ-2076

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import bcrypt
import httpx
import psycopg2
import pytest
import yaml

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_ORG = "pat_scopes_e2e"
_SCHEMA = "pat_scopes_e2e_src"
_USER = "pat-scopes-user"
_PASSWORD = "pat-scopes-user-password"
#: The roles the user's sign-in carries: what they hold.
_HELD = ["analyst", "org_admin"]


async def _pg(statements: list[str]) -> None:
    import asyncpg

    conn = await asyncpg.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        user=os.environ.get("PG_USER", "provisa"),
        password=os.environ.get("PG_PASSWORD", "provisa"),
        database=os.environ.get("PG_DATABASE", "provisa"),
        timeout=15,
    )
    try:
        for sql in statements:
            await conn.execute(sql)
    finally:
        await conn.close()


def _seed_source() -> None:
    asyncio.run(
        _pg(
            [
                f'DROP SCHEMA IF EXISTS "{_SCHEMA}" CASCADE',
                f'CREATE SCHEMA "{_SCHEMA}"',
                f'CREATE TABLE "{_SCHEMA}".orders (order_id integer PRIMARY KEY, region varchar)',
                f"INSERT INTO \"{_SCHEMA}\".orders VALUES (1, 'east')",
            ]
        )
    )


def _config(work: Path) -> Path:
    """One registered table both roles read, so each has a data surface to act on."""
    with open(_REPO / "tests/fixtures/sample_config.yaml") as f:
        base = yaml.safe_load(f)
    pg_source = next(s for s in base["sources"] if s["id"] == "sales-pg")
    cfg: dict = {"naming": base["naming"], "roles": base["roles"], "relationships": []}
    cfg["sources"] = [{**pg_source, "id": "pat-pg"}]
    cfg["domains"] = [{"id": "sales-analytics", "description": "token scopes e2e"}]
    cfg["tables"] = [
        {
            "source_id": "pat-pg",
            "domain_id": "sales-analytics",
            "schema": _SCHEMA,
            "table": "orders",
            "columns": [
                {"name": "order_id", "data_type": "integer", "visible_to": _HELD},
                {"name": "region", "data_type": "varchar", "visible_to": _HELD},
            ],
        }
    ]
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(cfg))
    return path


def _auth() -> dict:
    return {
        "provider": "simple",
        "allow_simple_auth": True,
        "jwt_secret": "pat-scopes-e2e-signing-secret-of-48-bytes-or-so!",
        "simple": {
            "users": [
                {
                    "username": _USER,
                    "password_hash": bcrypt.hashpw(_PASSWORD.encode(), bcrypt.gensalt()).decode(),
                    "roles": _HELD,
                }
            ]
        },
        "role_mapping": [],
        "default_role": "analyst",
    }


class _SignedIn:
    """The user's session against the server, and the tokens they issue."""

    def __init__(self, srv) -> None:
        self.srv = srv
        login = httpx.post(
            f"{srv.base_url}/auth/login",
            json={"username": _USER, "password": _PASSWORD},
            timeout=30,
        )
        login.raise_for_status()
        self.session = {"Authorization": f"Bearer {login.json()['access_token']}"}
        self._issued = 0

    def issue(self, scopes: list[str] | None) -> httpx.Response:
        self._issued += 1
        body: dict = {"name": f"e2e-{self._issued}"}
        if scopes is not None:
            body["scopes"] = scopes
        return httpx.post(
            f"{self.srv.base_url}/auth/tokens", json=body, headers=self.session, timeout=30
        )

    def token(self, scopes: list[str]) -> str:
        issued = self.issue(scopes)
        assert issued.status_code == 200, issued.text
        assert issued.json()["scopes"] == scopes
        return issued.json()["token"]

    def me(self, token: str, role: str | None = None) -> httpx.Response:
        headers = {"Authorization": f"Bearer {token}"}
        if role is not None:
            headers["X-Provisa-Role"] = role
        return httpx.get(f"{self.srv.base_url}/auth/me", headers=headers, timeout=30)

    def pg(self, token: str, role: str | None = None):
        return psycopg2.connect(
            host="127.0.0.1",
            port=self.srv.pgwire_port,
            dbname="provisa",
            user=_USER,
            password=token,
            connect_timeout=30,
            # psycopg2 leaves out a parameter given as None.
            options=None if role is None else f"-c provisa.role={role}",
        )


@pytest.fixture(scope="module")
def user(tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer, drop_org_schema

    work = tmp_path_factory.mktemp("pat_scopes")
    _seed_source()
    srv = IsolatedServer(
        _ORG,
        engine="duckdb",
        enable_pgwire=True,
        config=str(_config(work)),
        control_plane="postgres",
        auth=_auth(),
    )
    try:
        srv.start(timeout=240)
        yield _SignedIn(srv)
    finally:
        srv.stop_process()
        asyncio.run(drop_org_schema(_ORG))
        asyncio.run(_pg([f'DROP SCHEMA IF EXISTS "{_SCHEMA}" CASCADE']))


def test_a_token_is_not_issued_for_a_role_its_issuer_does_not_hold(user):
    refused = user.issue(["platform_admin"])
    assert refused.status_code == 403, refused.text
    assert refused.json()["code"] == "auth.token_scope_not_held"


@pytest.mark.parametrize("scopes", [None, []])
def test_a_token_is_not_issued_with_no_scopes(user, scopes):
    refused = user.issue(scopes)
    assert refused.status_code == 400, refused.text
    assert refused.json()["code"] == "auth.token_scopes_required"


def test_the_token_list_shows_each_tokens_scopes(user):
    user.token(["analyst"])
    listed = httpx.get(f"{user.srv.base_url}/auth/tokens", headers=user.session, timeout=30)
    assert listed.status_code == 200, listed.text
    assert ["analyst"] in [t["scopes"] for t in listed.json()]
    assert all("role_id" not in t for t in listed.json())


def test_over_http_the_token_acts_as_its_scope_and_no_other_role_its_owner_holds(user):
    token = user.token(["analyst"])
    acting = user.me(token)
    assert acting.status_code == 200, acting.text
    assert [a["role_id"] for a in acting.json()["assignments"]] == ["analyst"]
    assert user.me(token, "analyst").status_code == 200
    # The owner holds org_admin; the token does not.
    refused = user.me(token, "org_admin")
    assert refused.status_code == 403, refused.text
    assert refused.json() == {"detail": "not permitted"}


def test_over_pgwire_the_token_acts_as_its_scope_and_no_other_role_its_owner_holds(user):
    token = user.token(["analyst"])
    for named in (None, "analyst"):
        conn = user.pg(token, named)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                assert cur.fetchone() == (1,)
        finally:
            conn.close()
    with pytest.raises(psycopg2.OperationalError, match="not permitted"):
        user.pg(token, "org_admin").close()


def test_a_token_for_another_held_role_acts_as_that_role_not_the_default(user):
    """Naming no role, a token acts as its scope: never as the deployment's default role."""
    token = user.token(["org_admin"])
    acting = user.me(token)
    assert acting.status_code == 200, acting.text
    assert [a["role_id"] for a in acting.json()["assignments"]] == ["org_admin"]
    conn = user.pg(token)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)
    finally:
        conn.close()
    with pytest.raises(psycopg2.OperationalError, match="not permitted"):
        user.pg(token, "analyst").close()
