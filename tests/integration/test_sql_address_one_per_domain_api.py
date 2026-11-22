# Copyright (c) 2026 Kenneth Stott
# Canary: befb3bff-c599-475d-9744-842b67059733
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: one SQL address per table in a domain, through the admin API (REQ-1933).

tests/unit/test_sql_address_one_per_domain.py proves the refusal in the table repository over a
throwaway control plane. This drives the path a steward takes: ``registerTable`` on the running
app's /admin/graphql, against the test stack's control plane. A second table whose address
(``domain.alias``, else ``domain.table``) another registered table in the domain already holds is
refused with ``schema.sql_address_taken``, naming the holder; the same alias in another domain is
its own address.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

_ALIAS = "shared_address"


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")

    from provisa.api.app import create_app

    app = create_app()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


async def _gql(client: AsyncClient, query: str) -> dict:
    resp = await client.post("/admin/graphql", json={"query": query})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "errors" not in body, body["errors"]
    return body["data"]


async def _register_view(client: AsyncClient, domain: str, table_name: str) -> dict:
    """A virtual view with explicit columns: registration needs no source to introspect."""
    data = await _gql(
        client,
        f"""
        mutation {{
            registerTable(input: {{
                sourceId: "__derived__",
                domainId: "{domain}",
                schemaName: "views",
                tableName: "{table_name}",
                alias: "{_ALIAS}",
                viewSql: "SELECT 1 AS n",
                # org_admin, the role these requests act as, reads the view: a table its caller
                # reads no column of is not listed to it (REQ-2152).
                columns: [{{ name: "n", visibleTo: ["public", "org_admin"] }}]
            }}) {{ success message code params }}
        }}
        """,
    )
    return data["registerTable"]


@dataclass(frozen=True)
class _Place:
    """The two domains a test registers in, and the suffix its table names carry."""

    domain: str
    other: str
    run: str

    def view(self, name: str) -> str:
        return f"{name}_{self.run}"


@pytest_asyncio.fixture(loop_scope="session")
async def domains(client):
    """Two domains of the test's own. REQ-2248: deleting a table retires it, keeping its row and
    its name, and a domain that still holds a table, retired or not, is kept (maintainer
    2026-11-15). So the teardown retires what the test registered and leaves the domains, and
    every name is the test's own: a later test never meets a name a retired table still holds."""
    run = uuid.uuid4().hex[:8]
    place = _Place(f"req1933-addresses-{run}", f"req1933-other-{run}", run)
    for domain in (place.domain, place.other):
        created = await _gql(
            client,
            f'mutation {{ createDomain(input: {{ id: "{domain}", description: "REQ-1933" }}) '
            "{ success message } }",
        )
        assert created["createDomain"]["success"], created
    yield place
    tables = await _gql(client, "{ tables { id domainId } }")
    for table in tables["tables"]:
        if table["domainId"] in (place.domain, place.other):
            deleted = await _gql(
                client, f"mutation {{ deleteTable(id: {table['id']}) {{ success message }} }}"
            )
            assert deleted["deleteTable"]["success"], deleted


async def test_a_second_table_at_a_taken_address_is_refused_naming_the_holder(client, domains):
    first, second = domains.view("first_view"), domains.view("second_view")
    holder = await _register_view(client, domains.domain, first)
    assert holder["success"], holder["message"]

    newcomer = await _register_view(client, domains.domain, second)

    assert newcomer["success"] is False
    assert newcomer["code"] == "schema.sql_address_taken"
    assert newcomer["params"]["domain"] == domains.domain
    assert newcomer["params"]["address"] == _ALIAS
    assert newcomer["params"]["holder"] == f"__derived__.views.{first}"
    # Named as its registration names it: a client's text has a physical address rewritten to
    # its model name, which for the holder is the address itself (engine_error_names.model_named).
    assert f"table '{first}' of source '__derived__' (schema 'views')" in newcomer["message"]
    tables = await _gql(client, "{ tables { tableName domainId } }")
    assert [t["tableName"] for t in tables["tables"] if t["domainId"] == domains.domain] == [first]


async def test_the_same_alias_in_another_domain_is_its_own_address(client, domains):
    first = await _register_view(client, domains.domain, domains.view("first_view"))
    assert first["success"], first["message"]

    elsewhere = await _register_view(client, domains.other, domains.view("second_view"))

    assert elsewhere["success"], elsewhere["message"]
