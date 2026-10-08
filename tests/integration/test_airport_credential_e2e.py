# Copyright (c) 2026 Kenneth Stott
# Canary: 9d4b1e73-5a28-4c96-b0f1-e7c3a6d2f485
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""With an auth provider, the airport bearer is a credential and the audit row names the user.

The real DuckDB ``airport`` extension attaches to a server that authenticates: a signed-in
user's session token reads what that user's role is served; a role NAME in the token's place is
refused (REQ-1263); and the statement's audit row carries the user's id beside the role it ran
as (REQ-074).

Lands on the TEST instance only: one real server over a database the harness creates."""

# Requirements: REQ-1120, REQ-1263, REQ-074, REQ-273

from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import time
import urllib.request

import bcrypt
import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_PASSWORD = "correct horse"

# Bounded subprocess: a hung airport handshake cannot hang the suite. Prints one JSON line.
_CLIENT = r"""
import json
import sys

import duckdb

conn = duckdb.connect()
conn.execute("INSTALL airport FROM community")
conn.execute("LOAD airport")
loc = f"grpc://localhost:{sys.argv[1]}"
conn.execute("CREATE SECRET airport_sec (TYPE airport, auth_token ?, scope ?)", [sys.argv[2], loc])
conn.execute(f"ATTACH '{loc}' AS provisa (TYPE AIRPORT)")
rel = conn.execute('SELECT id, region FROM provisa."sales"."orders" ORDER BY id')
print(json.dumps({"columns": [d[0] for d in rel.description], "rows": rel.fetchall()}, default=str))
"""


def _model() -> dict:
    hashed = bcrypt.hashpw(_PASSWORD.encode(), bcrypt.gensalt()).decode()
    return {
        "auth": {
            "provider": "simple",
            "allow_simple_auth": True,
            "jwt_secret": "airport-credential-test-signing-key",
            "default_role": "seller",
            "simple": {
                "users": [{"username": "sam", "password_hash": hashed, "roles": ["seller"]}]
            },
        },
        "domains": [{"id": "sales", "description": "orders"}],
        "tables": [
            {
                "source_id": "sales-pg",
                "domain_id": "sales",
                "schema": "public",
                "table": "orders",
                "columns": [
                    {"name": "id", "data_type": "integer", "visible_to": ["seller"]},
                    {"name": "region", "data_type": "varchar", "visible_to": ["seller"]},
                ],
            }
        ],
        # org_admin is the reserved administrative role (REQ-1349): not declared.
        "roles": [
            {
                "id": "seller",
                "capabilities": ["query_development", "full_results"],
                "domain_access": ["sales"],
            }
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
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


@pytest.fixture(scope="module")
def token(server) -> str:
    req = urllib.request.Request(
        f"http://127.0.0.1:{server.ports['http']}/auth/login",
        data=json.dumps({"username": "sam", "password": _PASSWORD}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())["access_token"]


def _attach_and_read(server, auth_token: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", _CLIENT, str(server.ports["airport"]), auth_token],
        capture_output=True,
        text=True,
        timeout=180,
    )


def _user_id(token: str) -> str:
    """The signed-in user's id: the session token's subject."""
    payload = token.split(".")[1]
    return json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))["sub"]


def _airport_audit_row(server) -> dict:
    """The newest audit row written for an airport statement (the writer inserts in batches)."""
    engine = sa.create_engine(server.url)
    try:
        with engine.connect() as conn:
            schema = conn.execute(
                sa.text(
                    "SELECT table_schema FROM information_schema.tables "
                    "WHERE table_name = 'query_audit_log' AND table_schema LIKE 'org%' LIMIT 1"
                )
            ).scalar_one()
        deadline = time.monotonic() + 60
        while True:
            with engine.connect() as conn:
                row = conn.execute(
                    sa.text(
                        f"SELECT user_id, role_id FROM {schema}.query_audit_log "
                        "WHERE source = 'airport' ORDER BY id DESC LIMIT 1"
                    )
                ).first()
            if row is not None:
                return dict(row._mapping)
            assert time.monotonic() < deadline, "no airport audit row was written"
            time.sleep(0.5)
    finally:
        engine.dispose()


def test_a_session_token_reads_what_its_users_role_is_served(server, token):
    result = _attach_and_read(server, token)
    assert result.returncode == 0, f"stdout={result.stdout}\nstderr={result.stderr}"
    out = json.loads(result.stdout.strip().splitlines()[-1])
    assert out["columns"] == ["id", "region"] and out["rows"], out


def test_the_audit_row_names_the_user_and_the_role_it_ran_as(server, token):
    result = _attach_and_read(server, token)
    assert result.returncode == 0, f"stdout={result.stdout}\nstderr={result.stderr}"
    row = _airport_audit_row(server)
    assert row == {"user_id": _user_id(token), "role_id": "seller"}, row
    assert row["user_id"] != "seller"


@pytest.mark.parametrize("named", ["seller", "org_admin", ""])
def test_a_role_name_in_the_tokens_place_is_refused(server, named):
    result = _attach_and_read(server, named)
    assert result.returncode != 0, result.stdout
    assert "credential" in result.stderr, result.stderr
