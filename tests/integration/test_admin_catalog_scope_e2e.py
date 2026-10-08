# Copyright (c) 2026 Kenneth Stott
# Canary: 6c2e8a40-1f93-4d57-b8a6-0d4f7c9e1b35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The admin catalog answers a caller by its rights and reach (REQ-1958).

One real server with auth enforced. An analyst-kind user (query rights only, one domain) asks the
admin GraphQL API for the catalog: it is answered the tables and columns its role is served --
the same set Arrow Flight's catalog lists for it -- with no grant lists or mask settings, the
domains its role reaches and the sources behind its tables. A table and a column it is not served
are not named.

Lands on the TEST instance only: one real server over a database the harness creates."""

# Requirements: REQ-1958, REQ-1134, REQ-128

from __future__ import annotations

import json
import os
import urllib.request

import bcrypt
import pyarrow.flight as flight
import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_PASSWORD = "correct horse"
_DDL = [
    "ALTER TABLE public.orders ADD COLUMN secret varchar",
    "CREATE TABLE public.staff (id integer PRIMARY KEY, region varchar, secret varchar)",
    """CREATE FUNCTION public.order_count() RETURNS TABLE(n bigint)
       LANGUAGE sql AS $$ SELECT count(*) FROM public.orders $$""",
    """CREATE FUNCTION public.staff_count() RETURNS TABLE(n bigint)
       LANGUAGE sql AS $$ SELECT count(*) FROM public.staff $$""",
]
_ORDERS, _STAFF = ("sales", "orders"), ("hr", "staff")
_ORDER_COUNT = ("commands", "sales", "order_count")
_STAFF_COUNT = ("commands", "hr", "staff_count")
_OURS = {_ORDERS, _STAFF, _ORDER_COUNT, _STAFF_COUNT}


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
                # Granted to neither role: in no role's catalog.
                {"name": "secret", "data_type": "varchar", "visible_to": ["org_admin"]},
            ],
        }

    def _user(name: str, roles: list[str]) -> dict:
        hashed = bcrypt.hashpw(_PASSWORD.encode(), bcrypt.gensalt()).decode()
        return {"username": name, "password_hash": hashed, "roles": roles}

    def _command(name: str, domain: str, role: str) -> dict:
        return {
            "name": name,
            "source_id": "sales-pg",
            "schema": "public",
            "function_name": name,
            "returns": "",
            "kind": "query",
            "domain_id": domain,
            "visible_to": [role],
            "arguments": [],
        }

    reads = ["query_development", "full_results"]
    return {
        "auth": {
            "provider": "simple",
            "allow_simple_auth": True,
            "jwt_secret": "grpc-proxy-acting-role-test-signing-key",
            "default_role": "seller",
            "simple": {
                "users": [
                    _user("sam", ["seller"]),
                    _user("hana", ["hr_reader"]),
                    _user("both", ["seller", "hr_reader"]),
                ]
            },
        },
        "domains": [
            {"id": "sales", "description": "orders"},
            {"id": "hr", "description": "staff"},
        ],
        "tables": [_table("sales", "orders", "seller"), _table("hr", "staff", "hr_reader")],
        "functions": [
            _command("order_count", "sales", "seller"),
            _command("staff_count", "hr", "hr_reader"),
        ],
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
        for ddl in _DDL:
            conn.execute(sa.text(ddl))
    own.dispose()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _login(boot, user: str) -> str:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}/auth/login",
        data=json.dumps({"username": user, "password": _PASSWORD}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())["access_token"]


_CATALOG = """{
  tables { tableName domainId viewSql
           columns { columnName visibleTo writableBy unmaskedTo maskType maskValue } }
  relationships { id }
  domains { id }
  sources { id host }
  metrics { name visibleTo }
}"""


def _admin(boot, token: str, role: str) -> dict:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}/admin/graphql",
        data=json.dumps({"query": _CATALOG}).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "X-Provisa-Role": role,
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        body = json.loads(resp.read())
    assert "errors" not in body, body
    return body["data"]


def _flight_catalog(boot, token: str, role: str) -> dict[tuple[str, str], list[str]]:
    """(domain, table) -> columns, as Arrow Flight lists the role's catalog."""
    options = flight.FlightCallOptions(
        headers=[(b"authorization", f"Bearer {token}".encode()), (b"x-provisa-role", role.encode())]
    )
    out = {}
    with flight.FlightClient(f"grpc://127.0.0.1:{boot.ports['flight']}") as client:
        for info in client.list_flights(b"", options):
            path = [p.decode() if isinstance(p, bytes) else p for p in info.descriptor.path]
            if len(path) == 2:
                out[(path[0], path[1])] = list(info.schema.names)
    return out


def test_an_analyst_is_answered_exactly_what_its_role_is_served(server):
    token = _login(server, "sam")
    data = _admin(server, token, "seller")
    listed = {
        (t["domainId"], t["tableName"]): [c["columnName"] for c in t["columns"]]
        for t in data["tables"]
    }
    # The same set, table for table and column for column, that the role's Flight catalog lists.
    assert listed == _flight_catalog(server, token, "seller")
    assert listed[_ORDERS] == ["id", "region"]  # not `secret`
    assert _STAFF not in listed


def test_no_grant_list_or_mask_setting_is_answered_without_view_governance(server):
    data = _admin(server, _login(server, "sam"), "seller")
    assert data["tables"], data
    for table in data["tables"]:
        assert table["viewSql"] is None, table["tableName"]
        for column in table["columns"]:
            assert column["visibleTo"] is None and column["writableBy"] is None, column
            assert column["unmaskedTo"] is None and column["maskType"] is None, column
            assert column["maskValue"] is None, column
    assert all(m["visibleTo"] is None for m in data["metrics"])


def test_the_reference_data_is_the_callers_reach(server):
    data = _admin(server, _login(server, "sam"), "seller")
    domains = {d["id"] for d in data["domains"]}
    assert "sales" in domains and "hr" not in domains, domains
    # Named, because the role is answered a table from it; its connection details are not.
    assert "sales-pg" in {s["id"] for s in data["sources"]}, data["sources"]
    assert all(s["host"] is None for s in data["sources"]), data["sources"]


def test_the_other_roles_user_is_answered_the_other_catalog(server):
    data = _admin(server, _login(server, "hana"), "hr_reader")
    listed = {(t["domainId"], t["tableName"]) for t in data["tables"]}
    assert _STAFF in listed and _ORDERS not in listed, listed
