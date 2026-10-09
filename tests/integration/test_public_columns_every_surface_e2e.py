# Copyright (c) 2026 Kenneth Stott
# Canary: 3a6d9e12-8c47-4f05-b1e3-6d2c0a7f5b94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A published column is served outside its domain on every surface (REQ-1959).

``hr.staff`` publishes ``id`` and ``name`` and keeps ``salary`` to its domain; ``hr.reviews``
publishes nothing. A role that reaches only ``sales`` reads the two published columns over SQL,
GraphQL, REST, the gRPC proxy and Arrow Flight; ``salary`` and ``reviews`` stay refused on each;
a role that reaches ``hr`` reads all of it, as before.

Lands on the TEST instance only: one real server over a database the harness creates."""

# Requirements: REQ-1959, REQ-039

from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
from typing import Any

import pyarrow.flight as flight
import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_ALL = ["org_admin", "seller", "hr_reader"]


def _model() -> dict:
    def _column(name: str, data_type: str, scope: str) -> dict:
        return {"name": name, "data_type": data_type, "visible_to": _ALL, "scope": scope}

    def _table(domain: str, table: str, columns: list[dict]) -> dict:
        return {
            "source_id": "sales-pg",
            "domain_id": domain,
            "schema": "public",
            "table": table,
            "columns": columns,
        }

    reads = ["query_development", "full_results"]
    return {
        "domains": [
            {"id": "sales", "description": "orders"},
            {"id": "hr", "description": "staff"},
        ],
        "tables": [
            _table(
                "sales",
                "orders",
                [_column("id", "integer", "domain"), _column("region", "varchar", "domain")],
            ),
            _table(
                "hr",
                "staff",
                [
                    _column("id", "integer", "public"),
                    _column("name", "varchar", "public"),
                    _column("salary", "integer", "domain"),
                ],
            ),
            _table("hr", "reviews", [_column("id", "integer", "domain")]),
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
        conn.execute(
            sa.text(
                "CREATE TABLE public.staff (id integer PRIMARY KEY, name varchar, salary integer)"
            )
        )
        conn.execute(sa.text("INSERT INTO public.staff VALUES (7, 'Ada', 100)"))
        conn.execute(sa.text("CREATE TABLE public.reviews (id integer PRIMARY KEY)"))
        conn.execute(sa.text("INSERT INTO public.reviews VALUES (1)"))
    own.dispose()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _call(boot, path: str, role: str, body: dict | None = None) -> tuple[int, Any]:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "X-Provisa-Role": role},
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


def _sql(boot, role: str, sql: str) -> tuple[int, Any]:
    status, body = _call(boot, "/data/sql", role, {"sql": sql})
    return status, body["data"]["sql"] if status == 200 else body


def _fields(boot, role: str) -> dict[str, set[str]]:
    """type name → field names of the role's GraphQL schema."""
    status, body = _call(boot, "/data/introspection", role)
    assert status == 200, body
    schema = body.get("data", body)["__schema"]
    return {t["name"]: {f["name"] for f in t["fields"] or []} for t in schema["types"]}


def _query_field(boot, role: str, table: str) -> str | None:
    status, body = _call(boot, "/data/introspection", role)
    assert status == 200, body
    schema = body.get("data", body)["__schema"]
    (root,) = [t for t in schema["types"] if t["name"] == schema["queryType"]["name"]]
    names = [f["name"] for f in root["fields"] if f["name"].lower().endswith(table)]
    return names[0] if names else None


def test_sql_reads_the_published_columns_and_nothing_else_of_the_domain(server):
    status, rows = _sql(server, "seller", "SELECT id, name FROM hr.staff")
    assert status == 200 and rows == [{"id": 7, "name": "Ada"}], rows
    status, body = _sql(server, "seller", "SELECT salary FROM hr.staff")
    assert status != 200, body
    status, body = _sql(server, "seller", "SELECT id FROM hr.reviews")
    assert status != 200, body
    # SELECT * is the published columns.
    status, rows = _sql(server, "seller", "SELECT * FROM hr.staff")
    assert status == 200 and rows == [{"id": 7, "name": "Ada"}], rows


def test_a_role_that_reaches_the_domain_reads_all_of_it_as_before(server):
    status, rows = _sql(server, "hr_reader", "SELECT id, name, salary FROM hr.staff")
    assert status == 200 and rows == [{"id": 7, "name": "Ada", "salary": 100}], rows
    status, rows = _sql(server, "hr_reader", "SELECT id FROM hr.reviews")
    assert status == 200 and rows == [{"id": 1}], rows
    # ...and is not served another domain's unpublished table.
    status, body = _sql(server, "hr_reader", "SELECT id FROM sales.orders")
    assert status != 200, body


def test_graphql_serves_the_published_table_and_columns(server):
    staff = _query_field(server, "seller", "staff")
    assert staff is not None, "a published table is a root field"
    assert _query_field(server, "seller", "reviews") is None
    status, body = _call(
        server, "/data/graphql", "seller", {"query": f"{{ {staff} {{ id name }} }}"}
    )
    assert status == 200 and "errors" not in body, body
    assert body["data"][staff] == [{"id": 7, "name": "Ada"}]
    status, body = _call(
        server, "/data/graphql", "seller", {"query": f"{{ {staff} {{ salary }} }}"}
    )
    assert status != 200 or "errors" in body, body


def test_rest_serves_the_published_columns(server):
    status, body = _call(server, "/data/rest/hr/staff", "seller")
    assert status == 200, body
    rows = body["data"] if isinstance(body, dict) and "data" in body else body
    # Every table's rows carry its two system fields beside its columns (``_name_``, the table's
    # address, and ``_domain_``; compiler/context.py virtual_columns) — a REST read with no
    # ``fields`` selects them with the rest. The role's DATA columns are the published two.
    assert rows == [{"id": 7, "name": "Ada", "_name_": "hr.staff", "_domain_": "hr"}], rows
    status, body = _call(server, "/data/rest/hr/reviews", "seller")
    assert status != 200, body


def test_the_grpc_proxy_serves_the_published_columns(server):
    status, proto = _call(server, "/data/proto/seller", "seller")
    assert status == 200, proto
    (staff,) = [
        m for m in re.findall(r"rpc\s+Query(\w+)\s*\(", proto) if m.lower().endswith("staff")
    ]
    assert not [
        m for m in re.findall(r"rpc\s+Query(\w+)\s*\(", proto) if m.lower().endswith("reviews")
    ]
    status, rows = _call(server, f"/data/grpc/{staff}", "seller", {"limit": 10})
    assert status == 200 and rows == [{"id": 7, "name": "Ada"}], rows


def test_flight_serves_the_published_columns(server):
    with flight.FlightClient(f"grpc://127.0.0.1:{server.ports['flight']}") as client:
        ticket = flight.Ticket(
            json.dumps({"query": "SELECT id, name FROM hr.staff", "role": "seller"}).encode()
        )
        assert client.do_get(ticket).read_all().to_pylist() == [{"id": 7, "name": "Ada"}]
        refused = flight.Ticket(
            json.dumps({"query": "SELECT salary FROM hr.staff", "role": "seller"}).encode()
        )
        with pytest.raises(flight.FlightError):
            client.do_get(refused).read_all()
