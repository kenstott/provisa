# Copyright (c) 2026 Kenneth Stott
# Canary: 8b3f6c19-4d72-4e05-9a1b-c6e2d0f7a385
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Arrow Flight lists its catalog to a credential, as its role (REQ-1263, REQ-127).

One real server with auth enforced. ``list_flights``, ``get_flight_info`` and ``get_schema``
carry their credential in the call's ``authorization`` header (and the role they ask for in
``x-provisa-role``); a catalog ticket carries its own. Without a credential each is refused. A
role is listed its own tables, columns and commands; a set of held roles their union; a role the
credential does not hold is refused.

Lands on the TEST instance only: one real server over a database the harness creates."""

# Requirements: REQ-1263, REQ-127, REQ-128, REQ-1156, REQ-1620

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


@pytest.fixture(scope="module")
def tokens(server) -> dict[str, str]:
    return {user: _login(server, user) for user in ("sam", "hana", "both")}


@pytest.fixture(scope="module")
def client(server):
    with flight.FlightClient(f"grpc://127.0.0.1:{server.ports['flight']}") as fc:
        yield fc


def _options(token: str | None = None, role: str | None = None) -> flight.FlightCallOptions:
    headers: list[tuple[bytes, bytes]] = []
    if token is not None:
        headers.append((b"authorization", f"Bearer {token}".encode()))
    if role is not None:
        headers.append((b"x-provisa-role", role.encode()))
    return flight.FlightCallOptions(headers=headers)


def _listed(client, options) -> set[tuple[str, ...]]:
    """The model's own tables and commands among what the call lists (every role is also
    listed the catalog's own tables, which are not the subject here)."""
    paths = {
        tuple(p.decode() if isinstance(p, bytes) else p for p in info.descriptor.path)
        for info in client.list_flights(b"", options)
    }
    return paths & _OURS


def _columns(client, options, path: tuple[str, ...]) -> list[str]:
    return client.get_schema(flight.FlightDescriptor.for_path(*path), options).schema.names


def _ticket(**body) -> flight.Ticket:
    return flight.Ticket(json.dumps(body).encode())


def _catalog(client, **body) -> list[dict]:
    return client.do_get(_ticket(**body)).read_all().to_pylist()


def test_every_catalog_call_without_a_credential_is_refused(client):
    descriptor = flight.FlightDescriptor.for_path(*_ORDERS)
    for call in (
        lambda: list(client.list_flights(b"", _options())),
        lambda: client.get_flight_info(descriptor, _options()),
        lambda: client.get_schema(descriptor, _options()),
        lambda: client.do_get(_ticket()).read_all(),
        lambda: list(client.list_flights(b"", _options(token="not-a-credential"))),
    ):
        with pytest.raises(flight.FlightUnauthenticatedError):
            call()


def test_a_role_is_listed_only_its_own_tables_columns_and_commands(client, tokens):
    sam = _options(tokens["sam"])
    assert _listed(client, sam) == {_ORDERS, _ORDER_COUNT}
    assert _columns(client, sam, _ORDERS) == ["id", "region"]
    info = client.get_flight_info(flight.FlightDescriptor.for_path(*_ORDERS), sam)
    assert info.schema.names == ["id", "region"]
    for path in (_STAFF, _STAFF_COUNT):
        with pytest.raises(flight.FlightServerError, match="not found"):
            client.get_flight_info(flight.FlightDescriptor.for_path(*path), sam)
    with pytest.raises(flight.FlightServerError, match="not found"):
        client.get_schema(flight.FlightDescriptor.for_path(*_STAFF), sam)

    hana = _options(tokens["hana"], role="hr_reader")
    assert _listed(client, hana) == {_STAFF, _STAFF_COUNT}
    assert _columns(client, hana, _STAFF) == ["id", "region"]


def test_a_catalog_ticket_lists_its_credentials_role(client, tokens):
    rows = _catalog(client, token=tokens["sam"])
    listed = {(r["schema_name"], r["table_name"]) for r in rows}
    assert _ORDERS in listed and _STAFF not in listed, listed
    columns = _catalog(client, token=tokens["sam"], domain="sales", table="orders")
    assert [c["column_name"] for c in columns] == ["id", "region"]
    with pytest.raises(flight.FlightServerError, match="not found"):
        _catalog(client, token=tokens["sam"], domain="hr", table="staff")


def test_a_held_set_is_listed_the_union(client, tokens):
    both = _options(tokens["both"], role="seller,hr_reader")
    assert _listed(client, both) == _OURS
    assert _columns(client, both, _ORDERS) == ["id", "region"]
    assert _columns(client, both, _STAFF) == ["id", "region"]
    rows = _catalog(client, token=tokens["both"], role="hr_reader,seller")
    assert {_ORDERS, _STAFF} <= {(r["schema_name"], r["table_name"]) for r in rows}
    # One of the held roles, named alone, is that role.
    assert _listed(client, _options(tokens["both"], role="hr_reader")) == {_STAFF, _STAFF_COUNT}


@pytest.mark.parametrize("role", ["hr_reader", "seller,hr_reader", "meta:hr_reader+seller"])
def test_a_role_the_credential_does_not_hold_is_refused(client, tokens, role):
    options = _options(tokens["sam"], role=role)
    descriptor = flight.FlightDescriptor.for_path(*_STAFF)
    for call in (
        lambda: list(client.list_flights(b"", options)),
        lambda: client.get_flight_info(descriptor, options),
        lambda: client.get_schema(descriptor, options),
        lambda: client.do_get(_ticket(token=tokens["sam"], role=role)).read_all(),
    ):
        with pytest.raises(flight.FlightUnauthorizedError):
            call()
