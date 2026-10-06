# Copyright (c) 2026 Kenneth Stott
# Canary: 6f1c9b42-8e3d-4a75-b2c0-5d7e9a1f4c38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A request naming several roles is served the GraphQL schema of their meta-role (REQ-1620).

Two roles each see one domain's table. A request naming both (``X-Provisa-Role: a,b`` -- the UI's
"All") acts as their meta-role: its SDL and its introspection carry both roles' tables, and a
GraphQL query over both is answered.

Lands on the TEST instance only: one real server over a database the harness creates."""

# Requirements: REQ-1620, REQ-039

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request

import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_BOTH = "seller,hr_reader"


def _model() -> dict:
    def _table(domain: str, table: str, role: str) -> dict:
        return {
            "source_id": "sales-pg",
            "domain_id": domain,
            "schema": "public",
            "table": table,
            "columns": [
                {"name": "id", "data_type": "integer", "visible_to": [role]},
            ],
        }

    return {
        "domains": [
            {"id": "sales", "description": "orders"},
            {"id": "hr", "description": "staff"},
        ],
        "tables": [_table("sales", "orders", "seller"), _table("hr", "staff", "hr_reader")],
        # org_admin is the reserved administrative role (REQ-1349): not declared.
        "roles": [
            {"id": "seller", "capabilities": ["query_development"], "domain_access": ["sales"]},
            {"id": "hr_reader", "capabilities": ["query_development"], "domain_access": ["hr"]},
        ],
    }


@pytest.fixture(scope="module")
def server():
    boot = WorkerBoot(1, pg_host=_PG_HOST, pg_port=_PG_PORT, extra_config=_model())
    boot.create_database()
    own = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
    with own.connect() as conn:
        conn.execute(sa.text("CREATE TABLE public.staff (id integer PRIMARY KEY)"))
        conn.execute(sa.text("INSERT INTO public.staff VALUES (7)"))
    own.dispose()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _get(boot, path: str, role: str) -> tuple[int, str]:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}", headers={"X-Provisa-Role": role}
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()


def _graphql(boot, query: str, role: str) -> tuple[int, dict]:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}/data/graphql",
        data=json.dumps({"query": query}).encode(),
        headers={"Content-Type": "application/json", "X-Provisa-Role": role},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def _query_fields(boot, role: str) -> set[str]:
    """The root query fields the role's introspection lists."""
    status, body = _get(boot, "/data/introspection", role)
    assert status == 200, body
    schema = json.loads(body)
    data = schema.get("data", schema)["__schema"]
    query_type = data["queryType"]["name"]
    (root,) = [t for t in data["types"] if t["name"] == query_type]
    return {f["name"] for f in root["fields"]}


def test_the_role_sets_sdl_carries_both_roles_tables(server):
    seller, hr = _query_fields(server, "seller"), _query_fields(server, "hr_reader")
    orders = {f for f in seller if f.endswith("orders")}
    staff = {f for f in hr if f.endswith("staff")}
    assert orders and staff, (seller, hr)
    both = _query_fields(server, _BOTH)
    assert orders | staff <= both, both
    status, sdl = _get(server, "/data/sdl", _BOTH)
    assert status == 200, sdl
    assert all(name in sdl for name in orders | staff), sdl[:2000]


def test_a_graphql_query_over_both_roles_tables_is_answered(server):
    orders = next(f for f in _query_fields(server, "seller") if f.endswith("orders"))
    staff = next(f for f in _query_fields(server, "hr_reader") if f.endswith("staff"))
    status, body = _graphql(server, f"{{ {orders} {{ id }} {staff} {{ id }} }}", _BOTH)
    assert status == 200 and "errors" not in body, body
    assert {r["id"] for r in body["data"][staff]} == {7}
    assert body["data"][orders], body


def test_graphql_introspection_over_the_role_set_lists_both_roles_tables(server):
    """The GraphQL explorer reads the schema with an introspection query over /data/graphql."""
    seller = {f for f in _query_fields(server, "seller") if f.endswith("orders")}
    hr = {f for f in _query_fields(server, "hr_reader") if f.endswith("staff")}
    status, body = _graphql(server, "{ __schema { queryType { fields { name } } } }", _BOTH)
    assert status == 200 and "errors" not in body, body
    fields = {f["name"] for f in body["data"]["__schema"]["queryType"]["fields"]}
    assert seller | hr <= fields, fields


@pytest.mark.parametrize(("domain", "suffix"), [("sales", "orders"), ("hr", "staff")])
def test_the_role_sets_per_domain_introspection_covers_each_members_domain(server, domain, suffix):
    """The explorer pages read one domain's schema at a time (``/data/introspection?domain=``)."""
    status, body = _get(server, f"/data/introspection?domain={domain}", _BOTH)
    assert status == 200, body
    data = json.loads(body)
    schema = data.get("data", data)["__schema"]
    (root,) = [t for t in schema["types"] if t["name"] == schema["queryType"]["name"]]
    assert any(f["name"].endswith(suffix) for f in root["fields"]), root["fields"]


def test_the_explorer_compiles_a_query_over_both_roles_tables_as_the_role_set(server):
    """The explorer's tools tab compiles as the acting role -- under "Role: All", the set."""
    orders = next(f for f in _query_fields(server, "seller") if f.endswith("orders"))
    staff = next(f for f in _query_fields(server, "hr_reader") if f.endswith("staff"))
    query = f"{{ {orders} {{ id }} {staff} {{ id }} }}"
    document = (
        "mutation($input: CompileQueryInput!) { compileQuery(input: $input) { rootField sql } }"
    )
    req = urllib.request.Request(
        f"http://127.0.0.1:{server.ports['http']}/admin/graphql",
        data=json.dumps(
            {"query": document, "variables": {"input": {"query": query, "role": _BOTH}}}
        ).encode(),
        headers={"Content-Type": "application/json", "X-Provisa-Role": "org_admin"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        body = json.loads(resp.read())
    assert "errors" not in body, body
    assert {r["rootField"] for r in body["data"]["compileQuery"]} == {orders, staff}, body
