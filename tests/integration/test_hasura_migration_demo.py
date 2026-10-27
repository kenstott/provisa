# Copyright (c) 2026 Kenneth Stott
# Canary: 2de5c5f3-c3d9-4431-9332-af799f55be44
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The hasuramigration demo's Hasura app works the moment it is imported (REQ-417, REQ-2074).

The demo's metadata export (demo/named/hasuramigration/hasura/metadata) is zipped and imported
through the admin import surface against the demo's own database (db/init.sql) in the test
stack, applied with no edit, and the app's reshaped queries and mutations are then sent per role:
a customer reads and writes their own rows alone (the session value the token's x-hasura-user-id
claim carries; here the unsecured session header), support reads every order and updates the
statuses its permission allows, an analyst never sees a cancelled order or a customer's name.
The Salesforce half of the demo reads a live org and is the bundle build's, not this test's.
"""

# Requirements: REQ-417, REQ-1682, REQ-2074

from __future__ import annotations

import base64
import io
import os
import zipfile
from pathlib import Path

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]

DEMO = Path(__file__).resolve().parents[2] / "demo" / "named" / "hasuramigration"
DB = "hasura_outfitters"
# What the converted handler URLs read: the import names each to set (REQ-417).
HANDLER_ENV = {
    "PAYMENTS_BASE_URL": "https://payments.example",
    "REPORTS_BASE_URL": "https://reports.example",
    "FULFILMENT_WEBHOOK_URL": "https://fulfilment.example/order-placed",
}

ANA = {"x-provisa-role": "customer", "x-provisa-session-hasura-user-id": "1"}
SUPPORT = {"x-provisa-role": "support"}
ANALYST = {"x-provisa-role": "analyst"}


def _zipped_export() -> bytes:
    buf = io.BytesIO()
    root = DEMO / "hasura" / "metadata"
    with zipfile.ZipFile(buf, "w") as archive:
        for path in sorted(root.rglob("*")):
            if path.is_file():
                archive.write(path, Path("metadata") / path.relative_to(root))
    return buf.getvalue()


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def seeded_pg(docker_postgres):
    asyncpg = pytest.importorskip("asyncpg")
    host, port = docker_postgres["host"], docker_postgres["port"]
    user = os.environ.get("PG_USER", "provisa")
    password = os.environ.get("PG_PASSWORD", "provisa")
    admin = await asyncpg.connect(
        host=host, port=port, user=user, password=password, database="provisa"
    )
    try:
        if await admin.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", DB):
            await admin.execute(f'DROP DATABASE "{DB}" WITH (FORCE)')
        await admin.execute(f'CREATE DATABASE "{DB}"')
    finally:
        await admin.close()
    conn = await asyncpg.connect(host=host, port=port, user=user, password=password, database=DB)
    try:
        await conn.execute((DEMO / "db" / "init.sql").read_text())
    finally:
        await conn.close()
    return {"host": host, "port": port, "database": DB, "username": user, "password": password}


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client(seeded_pg):
    os.environ.setdefault("PG_PASSWORD", "provisa")
    os.environ.update(HANDLER_ENV)
    from provisa.api.app import create_app

    app = create_app()
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test", timeout=120) as c:
            yield c


async def _data(client, query, headers):
    resp = await client.post("/data/graphql", json={"query": query}, headers=headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert not body.get("errors"), body
    return body["data"]


async def _root(client, headers, kind: str, suffix: str) -> str:
    """The root field (query or mutation) whose name ends in suffix (its table's field, with
    whatever domain prefix the deployment's naming gives it)."""
    body = await _data(client, f"{{ __schema {{ {kind}Type {{ fields {{ name }} }} }} }}", headers)
    names = [f["name"] for f in body["__schema"][f"{kind}Type"]["fields"]]
    found = [n for n in names if n.split("__", 1)[-1] == suffix]
    assert len(found) == 1, f"{suffix} among {names}"
    return found[0]


class TestTheImportedAppWorks:
    async def test_the_export_imports_and_applies_unedited(self, client, seeded_pg):
        preview = await client.post(
            "/admin/import/hasura/preview",
            json={
                "filename": "outfitters.zip",
                "content_b64": base64.b64encode(_zipped_export()).decode(),
                "flavor": "hasura_v2",
                "domain_map": {"public": "store"},
                "source_overrides": {"outfitters": seeded_pg},
            },
        )
        assert preview.status_code == 200, preview.text
        body = preview.json()
        assert body["summary"]["tables"] == 4
        # The preview read search_products' signature from the database: a command now.
        assert any(c["item"] == "public.search_products" for c in body["carried"]), body["carried"]
        # Every column typed from the database: nothing about the source left to finish.
        assert not [w for w in body["warnings"] if w["category"] in ("sources", "tables")]
        applied = await client.post(
            "/admin/import/hasura/apply", json={"config_yaml": body["config_yaml"]}
        )
        assert applied.status_code == 200, applied.text

    async def test_the_customer_reads_her_own_orders_with_their_lines(self, client):
        orders = await _root(client, ANA, "query", "orders")
        data = await _data(
            client,
            f"{{ {orders}(order_by: [{{ placedAt: desc }}]) {{ id status totalCents "
            "items { quantity product { name } } } }",
            ANA,
        )
        rows = data[orders]
        assert sorted(r["id"] for r in rows) == [5, 10, 12, 23]
        assert all(r["items"] and r["items"][0]["product"]["name"] for r in rows)

    async def test_the_customer_reads_her_own_profile_under_its_custom_names(self, client):
        customers = await _root(client, ANA, "query", "customers")
        data = await _data(client, f"{{ {customers} {{ id givenName familyName email }} }}", ANA)
        assert data[customers] == [
            {
                "id": 1,
                "givenName": "Ana",
                "familyName": "Ruiz",
                "email": "ana.ruiz@grandhotels.example",
            }
        ]

    async def test_the_customer_renames_herself_and_nobody_else(self, client):
        update = await _root(client, ANA, "mutation", "updateCustomers")
        mine = await _data(
            client,
            f'mutation {{ {update}(set: {{givenName: "Anita"}}, where: {{id: {{eq: 1}}}}) '
            "{ affected_rows } }",
            ANA,
        )
        assert mine[update]["affected_rows"] == 1
        theirs = await _data(
            client,
            f'mutation {{ {update}(set: {{givenName: "X"}}, where: {{id: {{eq: 2}}}}) '
            "{ affected_rows } }",
            ANA,
        )
        assert theirs[update]["affected_rows"] == 0

    async def test_the_customer_places_an_order_that_is_hers(self, client):
        insert = await _root(client, ANA, "mutation", "insertOrders")
        placed = await _data(
            client,
            f'mutation {{ {insert}(input: {{shipTo: "12 Elm St"}}) {{ affected_rows }} }}',
            ANA,
        )
        assert placed[insert]["affected_rows"] == 1
        orders = await _root(client, SUPPORT, "query", "orders")
        data = await _data(
            client,
            f'{{ {orders}(where: {{shipTo: {{eq: "12 Elm St"}}}}) {{ customerId status }} }}',
            SUPPORT,
        )
        # The grant's presets: her own customer id from the session value, and status placed.
        assert {"customerId": 1, "status": "placed"} in data[orders]

    async def test_support_reads_every_order(self, client):
        orders = await _root(client, SUPPORT, "query", "orders")
        data = await _data(client, f"{{ {orders}(limit: 100) {{ id }} }}", SUPPORT)
        assert len(data[orders]) >= 40

    async def test_support_updates_only_the_statuses_its_permission_allows(self, client):
        update = await _root(client, SUPPORT, "mutation", "updateOrders")
        # Order 2 is placed: support may move it to packed.
        moved = await _data(
            client,
            f'mutation {{ {update}(set: {{status: "packed"}}, where: {{id: {{eq: 2}}}}) '
            "{ affected_rows } }",
            SUPPORT,
        )
        assert moved[update]["affected_rows"] == 1
        # Order 1 is shipped: outside the rows support may update.
        shipped = await _data(
            client,
            f'mutation {{ {update}(set: {{status: "placed"}}, where: {{id: {{eq: 1}}}}) '
            "{ affected_rows } }",
            SUPPORT,
        )
        assert shipped[update]["affected_rows"] == 0

    async def test_the_analyst_sees_no_cancelled_order_and_no_name(self, client):
        orders = await _root(client, ANALYST, "query", "orders")
        data = await _data(client, f"{{ {orders}(limit: 100) {{ status }} }}", ANALYST)
        assert data[orders] and all(r["status"] != "cancelled" for r in data[orders])
        customers = await _root(client, ANALYST, "query", "customers")
        resp = await client.post(
            "/data/graphql", json={"query": f"{{ {customers} {{ givenName }} }}"}, headers=ANALYST
        )
        # The document does not validate: its errors, no data (REQ-2167).
        body = resp.json()
        assert resp.status_code == 200 and "data" not in body, resp.text
        assert any("givenName" in e["message"] for e in body["errors"]), body
