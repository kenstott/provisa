# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7e5d90-1c84-4f2a-9e61-a4d8c0f72b15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The gRPC Explorer's routes serve a set of held roles as their meta-role (REQ-1620, REQ-273).

Two roles each reach one domain: its table and its command. The Explorer under "Role: All" acts as
both, and names them where each route takes a role — in the path of ``/data/proto``,
``/data/grpc-commands``, ``/data/grpc-group-by-columns`` and ``/data/grpc-command``, and in
``X-Provisa-Role`` on ``/data/grpc/{Type}``. Each answers for the set's meta-role: the union of
what the two roles are served. A meta-role named directly is refused on every one of them.

Lands on the TEST instance only: one real server over a database the harness creates."""

# Requirements: REQ-1620, REQ-273, REQ-525, REQ-1156

from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_BOTH = "seller,hr_reader"
_META = "meta:hr_reader+seller"

_DDL = [
    "CREATE TABLE public.staff (id integer PRIMARY KEY, region varchar)",
    "INSERT INTO public.staff VALUES (7, 'north')",
    """CREATE FUNCTION public.order_count() RETURNS TABLE(n bigint)
       LANGUAGE sql AS $$ SELECT count(*) FROM public.orders $$""",
    """CREATE FUNCTION public.staff_count() RETURNS TABLE(n bigint)
       LANGUAGE sql AS $$ SELECT count(*) FROM public.staff $$""",
]


def _model() -> dict:
    def _table(domain: str, table: str, role: str) -> dict:
        return {
            "source_id": "sales-pg",
            "domain_id": domain,
            "schema": "public",
            "table": table,
            "enable_aggregates": True,
            "enable_group_by": True,
            "columns": [
                {"name": "id", "data_type": "integer", "visible_to": [role]},
                {"name": "region", "data_type": "varchar", "visible_to": [role]},
            ],
        }

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


def _call(boot, path: str, *, role: str | None = None, body: dict | None = None) -> tuple[int, Any]:
    """(status, parsed JSON or text) of one request; ``role`` is the X-Provisa-Role header."""
    headers = {"Content-Type": "application/json"}
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


def _proto(boot, roles: str, domains: str = "") -> str:
    path = f"/data/proto/{urllib.parse.quote(roles, safe='')}"
    if domains:
        path += f"?domains={urllib.parse.quote(domains, safe='')}"
    status, text = _call(boot, path, role=roles)
    assert status == 200, text
    return text


def _query_types(proto: str) -> set[str]:
    """The model's two tables a proto exposes a plain Query RPC for (every role is also served
    the catalog's own tables, which are not the subject here)."""
    return {
        m
        for m in re.findall(r"rpc\s+Query(\w+)\s*\(", proto)
        if m.lower().endswith(("orders", "staff"))
    }


def _type(boot, role: str, table: str) -> str:
    (name,) = [t for t in _query_types(_proto(boot, role)) if t.lower().endswith(table)]
    return name


def test_the_sets_proto_carries_both_roles_tables(server):
    orders, staff = _type(server, "seller", "orders"), _type(server, "hr_reader", "staff")
    assert _query_types(_proto(server, "seller")) == {orders}
    assert _query_types(_proto(server, "hr_reader")) == {staff}
    assert _query_types(_proto(server, _BOTH)) == {orders, staff}
    # The order the roles are named in does not matter.
    assert _proto(server, "hr_reader,seller") == _proto(server, _BOTH)


def test_the_sets_proto_narrowed_to_the_domains_its_roles_reach(server):
    """The Explorer names every checked domain — under "All", the domains of every active role."""
    orders, staff = _type(server, "seller", "orders"), _type(server, "hr_reader", "staff")
    assert _query_types(_proto(server, _BOTH, "sales,hr")) == {orders, staff}
    assert _query_types(_proto(server, _BOTH, "hr")) == {staff}
    # One role of the set does not reach the other's domain, and says so by name.
    status, body = _call(server, "/data/proto/seller?domains=sales,hr", role="seller")
    assert status == 403 and body["code"] == "data.domain_not_accessible", body


def test_the_sets_command_list_is_both_roles_commands(server):
    def _names(roles: str) -> set[str]:
        status, body = _call(
            server, f"/data/grpc-commands/{urllib.parse.quote(roles, safe='')}", role=roles
        )
        assert status == 200, body
        return {c["name"] for c in body}

    assert _names("seller") == {"order_count"}
    assert _names("hr_reader") == {"staff_count"}
    assert _names(_BOTH) == {"order_count", "staff_count"}


def test_the_set_runs_either_roles_command(server):
    path = f"/data/grpc-command/{urllib.parse.quote(_BOTH, safe='')}"
    for name in ("order_count", "staff_count"):
        status, body = _call(server, path, role=_BOTH, body={"name": name, "args_json": "{}"})
        assert status == 200, body
        assert body and all("n" in row for row in body), body
    # Alone, a role runs only its own.
    status, body = _call(
        server,
        "/data/grpc-command/seller",
        role="seller",
        body={"name": "staff_count", "args_json": "{}"},
    )
    assert status in (403, 404), body


def test_the_set_reads_either_roles_table_over_the_proxy(server):
    orders, staff = _type(server, "seller", "orders"), _type(server, "hr_reader", "staff")
    status, body = _call(server, f"/data/grpc/{staff}", role=_BOTH, body={"limit": 10})
    assert status == 200, body
    assert [r["id"] for r in body] == [7], body
    status, body = _call(server, f"/data/grpc/{orders}", role=_BOTH, body={"limit": 10})
    assert status == 200 and body, body
    # Alone, a role does not read the other's table.
    status, body = _call(server, f"/data/grpc/{staff}", role="seller", body={"limit": 10})
    assert status != 200, body


def test_the_sets_group_by_columns_are_listed_for_either_roles_table(server):
    staff = _type(server, "hr_reader", "staff")
    path = f"/data/grpc-group-by-columns/{urllib.parse.quote(_BOTH, safe='')}/{staff}GroupBy"
    status, body = _call(server, path, role=_BOTH)
    assert status == 200 and "region" in body, body
    request = {"by": ["region"], "funcs": ["count"]}
    status, body = _call(server, f"/data/grpc/{staff}GroupBy", role=_BOTH, body=request)
    assert status == 200, body
    assert [row["group_key"] for row in body] == [{"region": "north"}], body
    assert body[0]["aggregate"]["count"] == 1, body


def test_the_proxy_runs_as_the_role_the_request_runs_as_never_a_body_role(server):
    """``/data/grpc/{Type}`` is governed as the acting role (REQ-273); a body role that differs
    from it is refused, as it is on /data/sql and /data/graphql."""
    staff = _type(server, "hr_reader", "staff")
    status, body = _call(
        server, f"/data/grpc/{staff}", role="seller", body={"role_id": "hr_reader", "limit": 10}
    )
    assert status == 400 and body["code"] == "data.role_mismatch", body


@pytest.mark.parametrize(
    "path, body",
    [
        ("/data/proto/{role}", None),
        ("/data/proto/{role}?domains=sales", None),
        ("/data/grpc-commands/{role}", None),
        ("/data/grpc-group-by-columns/{role}/OrdersGroupBy", None),
        ("/data/grpc-command/{role}", {"name": "order_count", "args_json": "{}"}),
    ],
)
def test_a_meta_role_named_in_the_path_is_refused(server, path, body):
    # Made first, so the refusal is not merely "no such role yet".
    _proto(server, _BOTH)
    status, answer = _call(
        server, path.format(role=urllib.parse.quote(_META, safe="")), role=_BOTH, body=body
    )
    assert status == 403, answer
    assert answer["code"] == "auth.meta_role_named", answer
