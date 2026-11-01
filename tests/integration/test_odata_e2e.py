# Copyright (c) 2026 Kenneth Stott
# Canary: 5307b24f-9cac-43d4-8ed7-706aee5f024b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The OData service on a real server (REQ-2193).

One isolated server with auth on, over a Postgres source this module creates: a client signs in with
HTTP Basic as Excel does, and each read goes through the real auth middleware, compiler, routing,
governance, source and audit log. Nothing is stood in for.
"""

# Requirements: REQ-2193

from __future__ import annotations

import asyncio
import os
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import bcrypt
import httpx
import pytest
import sqlalchemy as sa
import yaml

pytestmark = [pytest.mark.integration]

_ORG = "odata_e2e"
_SCHEMA = "odata_e2e_src"
_SOURCE = "odata-pg"
_CLERK = "odata_clerk"
_ANALYST_USER = ("odata-analyst", "odata-analyst-password")
_CLERK_USER = ("odata-clerk", "odata-clerk-password")
_ORDERS = 5005  # past one page of 5000
_FIRST_CUSTOMERS_ORDERS = 1500  # past one expanded list of 1000
_EDM = "{http://docs.oasis-open.org/odata/ns/edm}"
_ROOT = "/data/odata"
_BOTH = ["analyst", _CLERK]


async def _pg(*statements: str) -> None:
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
        for statement in statements:
            await conn.execute(statement)
    finally:
        await conn.close()


def _seed_source() -> None:
    asyncio.run(
        _pg(
            f'DROP SCHEMA IF EXISTS "{_SCHEMA}" CASCADE',
            f'CREATE SCHEMA "{_SCHEMA}"',
            f'CREATE TABLE "{_SCHEMA}".customers (id integer PRIMARY KEY, name varchar)',
            f"""INSERT INTO "{_SCHEMA}".customers VALUES (1, 'one'), (2, 'two'), (3, 'three')""",
            f'CREATE TABLE "{_SCHEMA}".orders (id integer PRIMARY KEY, customer_id integer, '
            "email varchar, amount numeric(10,2), margin double precision)",
            f"""INSERT INTO "{_SCHEMA}".orders
                SELECT i, CASE WHEN i <= {_FIRST_CUSTOMERS_ORDERS} THEN 1 ELSE 2 END,
                       'user' || i || '@real.example', i + 0.25, i / 2.0
                FROM generate_series(1, {_ORDERS}) AS i""",
            f'CREATE TABLE "{_SCHEMA}".salaries (id integer PRIMARY KEY, pay integer)',
            f'INSERT INTO "{_SCHEMA}".salaries VALUES (1, 100)',
        )
    )


def _column(name: str, data_type: str, roles: list[str], **extra) -> dict:
    return {"name": name, "data_type": data_type, "visible_to": roles, **extra}


def _table(name: str, columns: list[dict]) -> dict:
    return {
        "source_id": _SOURCE,
        "domain_id": "sales",
        "schema": _SCHEMA,
        "table": name,
        "columns": columns,
    }


def _config(work: Path) -> Path:
    masked = [
        {"to": ["analyst"], "mask": None},
        {"mask": {"type": "constant", "value": "***"}},
    ]
    config = {
        "sources": [
            {
                "id": _SOURCE,
                "type": "postgresql",
                "host": os.environ.get("PG_HOST", "localhost"),
                "port": int(os.environ.get("PG_PORT", "5432")),
                "database": os.environ.get("PG_DATABASE", "provisa"),
                "username": os.environ.get("PG_USER", "provisa"),
                "password": "${env:PG_PASSWORD}",
            }
        ],
        "domains": [{"id": "sales", "description": "OData e2e"}],
        "naming": {"domain_prefix": False},
        "tables": [
            _table(
                "customers",
                [
                    _column("id", "integer", _BOTH, is_primary_key=True),
                    _column("name", "varchar", _BOTH),
                ],
            ),
            _table(
                "orders",
                [
                    _column("id", "integer", _BOTH, is_primary_key=True),
                    _column("customer_id", "integer", _BOTH),
                    _column("email", "varchar", _BOTH, masks=masked),
                    _column("amount", "numeric(10,2)", _BOTH),
                    _column("margin", "double", ["analyst"]),
                ],
            ),
            _table(
                "salaries",
                [
                    _column("id", "integer", ["analyst"], is_primary_key=True),
                    _column("pay", "integer", ["analyst"]),
                ],
            ),
        ],
        "relationships": [
            {
                "id": "order-customer",
                "alias": "PLACED_BY",
                "cardinality": "many-to-one",
                "source_table_id": "orders",
                "source_column": "customer_id",
                "target_table_id": "customers",
                "target_column": "id",
            },
            {
                "id": "customer-orders",
                "alias": "PLACED",
                "cardinality": "one-to-many",
                "source_table_id": "customers",
                "source_column": "id",
                "target_table_id": "orders",
                "target_column": "customer_id",
            },
            {
                "id": "order-salary",
                "alias": "PAID",
                "cardinality": "one-to-one",
                "source_table_id": "orders",
                "source_column": "id",
                "target_table_id": "salaries",
                "target_column": "id",
            },
        ],
        "roles": [
            {
                "id": _CLERK,
                "description": "Reads one customer's orders, with email masked.",
                "capabilities": ["query_development"],
                "domain_access": ["*"],
            }
        ],
        "rls_rules": [
            {
                "description": "The clerk's customer.",
                "table_id": "orders",
                "role_id": _CLERK,
                "filter": "customer_id = 1",
            }
        ],
    }
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(config))
    return path


def _user(credential: tuple[str, str], role: str) -> dict:
    username, password = credential
    return {
        "username": username,
        "password_hash": bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode(),
        "roles": [role],
    }


def _auth() -> dict:
    return {
        "provider": "simple",
        "allow_simple_auth": True,
        "jwt_secret": "odata-e2e-signing-secret-0123456789abcdef",
        "simple": {"users": [_user(_ANALYST_USER, "analyst"), _user(_CLERK_USER, _CLERK)]},
        "role_mapping": [],
        "default_role": "analyst",
    }


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer, drop_org_schema

    _seed_source()
    srv = IsolatedServer(
        _ORG,
        engine="duckdb",
        config=str(_config(tmp_path_factory.mktemp("odata_e2e"))),
        control_plane="postgres",
        auth=_auth(),
    )
    try:
        srv.start(timeout=240)
        yield srv
    finally:
        srv.stop_process()
        asyncio.run(drop_org_schema(_ORG))
        asyncio.run(_pg(f'DROP SCHEMA IF EXISTS "{_SCHEMA}" CASCADE'))


@pytest.fixture
def analyst(server):
    with httpx.Client(base_url=server.base_url, auth=_ANALYST_USER, timeout=120) as client:
        yield client


@pytest.fixture
def clerk(server):
    with httpx.Client(base_url=server.base_url, auth=_CLERK_USER, timeout=120) as client:
        yield client


def _read(client: httpx.Client, path: str, **options: str) -> dict:
    params = {f"${name}": value for name, value in options.items()}
    resp = client.get(f"{_ROOT}/{path}", params=params)
    assert resp.status_code == 200, resp.text
    assert resp.headers["odata-version"] == "4.0"
    return resp.json()


def _metadata(client: httpx.Client) -> ET.Element:
    resp = client.get(f"{_ROOT}/$metadata")
    assert resp.status_code == 200, resp.text
    return ET.fromstring(resp.text)


def _names(root: ET.Element, tag: str) -> set[str | None]:
    return {e.get("Name") for e in root.iter(f"{_EDM}{tag}")}


# -- $metadata -------------------------------------------------------------------------------------


def test_metadata_is_the_roles_own(analyst, clerk):
    full = _metadata(analyst)
    assert {"customers", "orders", "salaries"} <= _names(full, "EntitySet")
    assert "margin" in _names(full, "Property")
    assert {"customer", "orders"} <= _names(full, "NavigationProperty")

    served = _metadata(clerk)
    assert {"customers", "orders"} <= _names(served, "EntitySet")
    assert "salaries" not in _names(served, "EntitySet")
    assert "margin" not in _names(served, "Property")


def test_the_service_document_lists_the_roles_sets(clerk):
    names = {entry["name"] for entry in _read(clerk, "")["value"]}
    assert {"customers", "orders"} <= names
    assert "salaries" not in names


# -- reads -----------------------------------------------------------------------------------------


def test_a_filtered_read(analyst):
    body = _read(analyst, "orders", filter="id lt 4", orderby="id desc", select="id,amount")
    assert body["value"] == [
        {"id": 3, "amount": 3.25},
        {"id": 2, "amount": 2.25},
        {"id": 1, "amount": 1.25},
    ]


def test_a_masked_column_and_a_row_rule(analyst, clerk):
    shown = _read(clerk, "orders", filter="id le 2", select="id,email", orderby="id")
    assert shown["value"] == [{"id": 1, "email": "***"}, {"id": 2, "email": "***"}]
    real = _read(analyst, "orders", filter="id eq 1", select="email")
    assert real["value"] == [{"email": "user1@real.example"}]
    assert _read(clerk, "orders", filter=f"id gt {_FIRST_CUSTOMERS_ORDERS}")["value"] == []
    assert clerk.get(f"{_ROOT}/orders/$count").text == str(_FIRST_CUSTOMERS_ORDERS)


def test_a_filter_on_a_masked_column_is_refused(clerk):
    resp = clerk.get(f"{_ROOT}/orders", params={"$filter": "email eq 'x'"})
    assert resp.status_code == 403, resp.text
    assert "email" in resp.json()["error"]["message"]


def test_a_column_the_role_cannot_see_is_as_for_no_column(clerk):
    resp = clerk.get(f"{_ROOT}/orders", params={"$select": "margin"})
    assert resp.status_code == 400, resp.text


def test_following_next_links_reads_every_row_once(analyst):
    body = _read(analyst, "orders", select="id", count="true")
    assert body["@odata.count"] == _ORDERS
    assert len(body["value"]) == 5000
    seen = [row["id"] for row in body["value"]]
    following = analyst.get(body["@odata.nextLink"])
    assert following.status_code == 200, following.text
    seen += [row["id"] for row in following.json()["value"]]
    assert "@odata.nextLink" not in following.json()
    assert seen == list(range(1, _ORDERS + 1))


# -- $expand ---------------------------------------------------------------------------------------


def test_a_to_many_expand_with_nested_options(analyst):
    body = _read(
        analyst,
        "customers",
        filter="id eq 1",
        select="name",
        expand="orders($filter=id le 3;$orderby=id desc;$select=id,amount;$top=2)",
    )
    # $top ends the list at 2, so it has no link; $filter leaves three rows and $top two of them.
    assert body["value"] == [
        {"name": "one", "orders": [{"id": 3, "amount": 3.25}, {"id": 2, "amount": 2.25}]}
    ]


def test_a_to_one_expand(analyst):
    body = _read(analyst, "orders", filter="id eq 1", select="id", expand="customer($select=name)")
    assert body["value"] == [{"id": 1, "customer": {"name": "one"}}]


def test_an_expand_keeps_the_related_tables_rules(clerk):
    body = _read(
        clerk,
        "customers",
        filter="id le 2",
        orderby="id",
        select="id",
        expand="orders($select=id,email;$orderby=id;$top=2)",
    )
    assert body["value"] == [
        {"id": 1, "orders": [{"id": 1, "email": "***"}, {"id": 2, "email": "***"}]},
        {"id": 2, "orders": []},  # another customer's orders are outside the row rule
    ]


def test_a_nested_filter_on_a_masked_column_is_refused(clerk):
    resp = clerk.get(f"{_ROOT}/customers", params={"$expand": "orders($filter=email eq 'x')"})
    assert resp.status_code == 403, resp.text


def test_a_relationship_into_a_hidden_table_is_as_for_no_name(analyst, clerk):
    assert _read(analyst, "orders", filter="id eq 1", select="id", expand="salary")["value"] == [
        {"id": 1, "salary": {"id": 1, "pay": 100}}
    ]
    resp = clerk.get(f"{_ROOT}/orders", params={"$expand": "salary"})
    assert resp.status_code == 400, resp.text


def test_a_bounded_list_links_the_rest_and_the_link_reads_it(analyst):
    body = _read(analyst, "customers", filter="id eq 1", select="id", expand="orders($select=id)")
    (customer,) = body["value"]
    assert len(customer["orders"]) == 1000
    seen = [order["id"] for order in customer["orders"]]
    url = customer["orders@odata.nextLink"]
    while url:
        resp = analyst.get(url)
        assert resp.status_code == 200, resp.text
        seen += [order["id"] for order in resp.json()["value"]]
        url = resp.json().get("@odata.nextLink")
    assert seen == list(range(1, _FIRST_CUSTOMERS_ORDERS + 1))


def test_a_navigation_read_keeps_the_rules(clerk):
    body = _read(clerk, "customers(1)/orders", select="id,email", orderby="id", top="1")
    assert body["value"] == [{"id": 1, "email": "***"}]


# -- authentication --------------------------------------------------------------------------------


def _assert_challenged(resp: httpx.Response) -> None:
    assert resp.status_code == 401, resp.text
    assert resp.headers.get_list("www-authenticate") == [
        'Basic realm="Provisa"',
        'Bearer realm="Provisa"',
    ]
    assert resp.headers["odata-version"] == "4.0"


def test_no_credential_is_a_401_with_a_challenge(server):
    _assert_challenged(httpx.get(f"{server.base_url}{_ROOT}/orders", timeout=120))
    _assert_challenged(httpx.get(f"{server.base_url}{_ROOT}/$metadata", timeout=120))


def test_a_wrong_password_is_a_401_with_a_challenge(server):
    resp = httpx.get(
        f"{server.base_url}{_ROOT}/orders", auth=(_ANALYST_USER[0], "not-the-password"), timeout=120
    )
    _assert_challenged(resp)


def test_a_bearer_token_reads(server):
    username, password = _CLERK_USER
    login = httpx.post(
        f"{server.base_url}/auth/login",
        json={"username": username, "password": password},
        timeout=120,
    )
    login.raise_for_status()
    resp = httpx.get(
        f"{server.base_url}{_ROOT}/orders",
        params={"$top": "1", "$select": "email"},
        headers={"Authorization": f"Bearer {login.json()['access_token']}"},
        timeout=120,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["value"] == [{"email": "***"}]


# -- audit -----------------------------------------------------------------------------------------


def _audited(role: str, read_table: int) -> dict:
    """The newest audit row of ``role`` that masked ``read_table``, waiting for the writer."""
    from provisa.core.database import create_engine_from_url

    engine = create_engine_from_url(os.environ["TENANT_DATABASE_URL"])
    query = sa.text(
        f"SELECT role_id, route, route_reason, sources, enforced FROM org_{_ORG}.query_audit_log "
        "WHERE role_id = :role ORDER BY id DESC LIMIT 20"
    )
    deadline = time.monotonic() + 60
    try:
        while True:
            with engine.connect() as conn:
                rows = [dict(r._mapping) for r in conn.execute(query, {"role": role})]
            for row in rows:
                if str(read_table) in (row["enforced"] or {}).get("masks", {}):
                    return row
            assert time.monotonic() < deadline, f"no audited read of {read_table}: {rows}"
            time.sleep(0.5)
    finally:
        engine.dispose()


def test_a_read_and_its_expansion_are_audited_with_what_was_enforced(clerk):
    _read(clerk, "customers", filter="id eq 1", expand="orders($select=email;$top=1)")
    from provisa.core.database import create_engine_from_url

    engine = create_engine_from_url(os.environ["TENANT_DATABASE_URL"])
    try:
        with engine.connect() as conn:
            orders_table = conn.execute(
                sa.text(f"SELECT id FROM org_{_ORG}.registered_tables WHERE table_name = 'orders'")
            ).scalar_one()
    finally:
        engine.dispose()

    row = _audited(_CLERK, orders_table)
    assert row["sources"] == [_SOURCE]
    assert row["route_reason"]
    enforced = row["enforced"]
    assert enforced["masks"][str(orders_table)] == {"email": "constant"}
    assert str(orders_table) in enforced["row_filters"]
