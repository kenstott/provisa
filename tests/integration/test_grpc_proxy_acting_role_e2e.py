# Copyright (c) 2026 Kenneth Stott
# Canary: 5a8f1e63-2b97-4c40-a7d5-9e0c3b6f8d12
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``POST /data/grpc/{Type}`` runs as the role the request runs as (REQ-273).

The acting role is the one the auth layer established from the signed-in identity (and an
``X-Provisa-Role`` it honoured). A ``role_id`` or ``role`` in the request body is not a second way
to pick a role: one that differs from the acting role is refused, as it is on ``/data/sql`` and
``/data/graphql``; one that agrees does nothing.

Lands on the TEST instance only: one real server, auth enforced, over a database the harness
creates."""

# Requirements: REQ-273, REQ-045

from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
from typing import Any

import bcrypt
import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_PASSWORD = "correct horse"


def _model() -> dict:
    def _table(domain: str, table: str, role: str) -> dict:
        return {
            "source_id": "sales-pg",
            "domain_id": domain,
            "schema": "public",
            "table": table,
            "columns": [
                {"name": "id", "data_type": "integer", "visible_to": [role]},
                {"name": "region", "data_type": "varchar", "visible_to": [role]},
            ],
        }

    def _user(name: str, role: str) -> dict:
        hashed = bcrypt.hashpw(_PASSWORD.encode(), bcrypt.gensalt()).decode()
        return {"username": name, "password_hash": hashed, "roles": [role]}

    reads = ["query_development", "full_results"]
    return {
        "auth": {
            "provider": "simple",
            "allow_simple_auth": True,
            "jwt_secret": "grpc-proxy-acting-role-test-signing-key",
            "default_role": "seller",
            "simple": {"users": [_user("sam", "seller"), _user("hana", "hr_reader")]},
        },
        "domains": [
            {"id": "sales", "description": "orders"},
            {"id": "hr", "description": "staff"},
        ],
        "tables": [_table("sales", "orders", "seller"), _table("hr", "staff", "hr_reader")],
        # org_admin is the reserved administrative role (REQ-1349): not declared.
        "roles": [
            {"id": "seller", "capabilities": reads, "domain_access": ["sales"]},
            {"id": "hr_reader", "capabilities": reads, "domain_access": ["hr"]},
        ],
    }


@pytest.fixture(scope="module")
def server():
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config=_model(),
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    own = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
    with own.connect() as conn:
        conn.execute(sa.text("CREATE TABLE public.staff (id integer PRIMARY KEY, region varchar)"))
        conn.execute(sa.text("INSERT INTO public.staff VALUES (7, 'north')"))
    own.dispose()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _call(
    boot,
    path: str,
    *,
    token: str | None = None,
    role: str | None = None,
    body: dict | None = None,
) -> tuple[int, Any]:
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    if role is not None:
        headers["X-Provisa-Role"] = role
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=None if body is None else json.dumps(body).encode(),
        headers=headers,
        method="GET" if body is None else "POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            status, raw = resp.status, resp.read().decode()
    except urllib.error.HTTPError as exc:
        status, raw = exc.code, exc.read().decode()
    try:
        return status, json.loads(raw)
    except json.JSONDecodeError:
        return status, raw


@pytest.fixture(scope="module")
def tokens(server) -> dict[str, str]:
    out = {}
    for user in ("sam", "hana"):
        status, body = _call(server, "/auth/login", body={"username": user, "password": _PASSWORD})
        assert status == 200, body
        out[user] = body["access_token"]
    return out


def _type(server, token: str, role: str, table: str) -> str:
    """The type the role's own proto exposes a plain Query RPC for ``table`` under."""
    status, proto = _call(server, f"/data/proto/{role}", token=token, role=role)
    assert status == 200, proto
    (name,) = [m for m in re.findall(r"rpc\s+Query(\w+)\s*\(", proto) if m.lower().endswith(table)]
    return name


@pytest.fixture(scope="module")
def staff(server, tokens) -> str:
    return _type(server, tokens["hana"], "hr_reader", "staff")


@pytest.fixture(scope="module")
def orders(server, tokens) -> str:
    return _type(server, tokens["sam"], "seller", "orders")


def test_a_caller_reads_its_own_roles_table(server, tokens, staff, orders):
    status, body = _call(
        server, f"/data/grpc/{staff}", token=tokens["hana"], role="hr_reader", body={"limit": 10}
    )
    assert status == 200 and [r["id"] for r in body] == [7], body
    # With no role named anywhere the request runs as the role the identity resolves to.
    status, body = _call(server, f"/data/grpc/{orders}", token=tokens["sam"], body={"limit": 10})
    assert status == 200 and body, body
    # A body role that agrees with the acting role does nothing.
    status, body = _call(
        server,
        f"/data/grpc/{orders}",
        token=tokens["sam"],
        body={"limit": 10, "role_id": "seller"},
    )
    assert status == 200 and body, body


@pytest.mark.parametrize("field", ["role_id", "role"])
def test_a_body_role_the_request_does_not_run_as_is_refused(server, tokens, staff, field):
    status, body = _call(
        server, f"/data/grpc/{staff}", token=tokens["sam"], body={"limit": 10, field: "hr_reader"}
    )
    assert status == 400, body
    assert body["code"] == "data.role_mismatch", body
    assert body["params"] == {"body_role": "hr_reader", "acting_role": "seller"}, body


def test_a_role_header_naming_a_role_not_held_is_refused(server, tokens, staff):
    status, body = _call(
        server, f"/data/grpc/{staff}", token=tokens["sam"], role="hr_reader", body={"limit": 10}
    )
    assert status == 403 and "not assigned" in str(body), body


def test_the_acting_role_reads_only_what_it_is_granted(server, tokens, staff):
    status, body = _call(server, f"/data/grpc/{staff}", token=tokens["sam"], body={"limit": 10})
    assert status != 200, body
