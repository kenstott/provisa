# Copyright (c) 2026 Kenneth Stott
# Canary: 4e7a2c19-8b3d-4f61-9c05-7d1e3a6b2f88
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A command saved from a remote GraphQL source's root query field, through the endpoint the
Commands form posts to, on an in-process app (REQ-1924, REQ-1986).

The form's "DB Function" with implementation "Source operation" on a remote GraphQL source posts
the picked root field (one that requires an argument) to ``POST /admin/actions/functions``. The
save answers a success, or a refusal by name; never an internal error.

The remote is a Strawberry schema answered through respx; the app runs on the local platform."""

# Requirements: REQ-1924, REQ-1986
from __future__ import annotations

import json
import uuid
from typing import Optional

import httpx
import pytest
import pytest_asyncio
import respx
import strawberry
from httpx import ASGITransport, AsyncClient

pytestmark = [
    pytest.mark.integration,
    pytest.mark.local_platform,
    pytest.mark.asyncio(loop_scope="session"),
]

_RUN = uuid.uuid4().hex[:8]
_SOURCE = f"gql-cmd-{_RUN}"
_DOMAIN = f"gql_cmd_{_RUN}"
_URL = f"https://gql-cmd-{_RUN}.example.com/graphql"


@strawberry.type
class Breed:
    name: str
    species: str


@strawberry.type
class Keeper:
    id: int
    name: str


_BREEDS = [Breed(name="beagle", species="dog"), Breed(name="tabby", species="cat")]


@strawberry.type
class Query:
    @strawberry.field(description="Every breed")
    def animal_breeds(self) -> list[Breed]:
        return _BREEDS

    @strawberry.field(description="Care requirements for one breed")
    def animal_breed(self, name: str) -> Optional[Breed]:
        return next((b for b in _BREEDS if b.name == name), None)

    @strawberry.field(description="One keeper")
    def keeper(self, id: int) -> Optional[Keeper]:
        return Keeper(id=id, name=f"keeper {id}")


_SCHEMA = strawberry.Schema(query=Query)


def _remote(request: httpx.Request) -> httpx.Response:
    body = json.loads(request.content)
    result = _SCHEMA.execute_sync(body["query"], variable_values=body.get("variables"))
    answer: dict = {"data": result.data}
    if result.errors:
        answer["errors"] = [{"message": e.message} for e in result.errors]
    return httpx.Response(200, json=answer)


@pytest_asyncio.fixture(loop_scope="session", scope="module")
async def client():
    import provisa.api.app as app_mod

    app = app_mod.create_app()
    async with app.router.lifespan_context(app):
        with respx.mock(assert_all_called=False) as mock:
            mock.post(_URL).mock(side_effect=_remote)
            mock.route().pass_through()
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test", timeout=300) as c:
                resp = await c.post(
                    "/admin/sources/graphql-remote",
                    json={
                        "source_id": _SOURCE,
                        "url": _URL,
                        "domain_id": _DOMAIN,
                        "cache_ttl": 300,
                    },
                )
                assert resp.status_code == 200, resp.text
                yield c
                # A derived table goes with its function (REQ-1986): the commands first.
                for name in (f"keeper_{_RUN}", f"breed2_{_RUN}"):
                    gone = await c.delete(f"/admin/actions/functions/{name}")
                    assert gone.status_code == 200, gone.text
                from tests.helpers import retire_tables_of_source

                await retire_tables_of_source(c, _SOURCE)


def _form(name: str, operation: str) -> dict:
    """What the Commands form posts for a "DB Function", implementation "Source operation"."""
    return {
        "name": name,
        "sourceId": _SOURCE,
        "schemaName": "graphql",
        "functionName": operation,
        "returns": "",
        "arguments": [],
        "visibleTo": [],
        "domainId": _DOMAIN,
        "kind": "mutation",
        "returnSchema": None,
        "outputColumns": [],
        "implKind": "source_operation",
        "binding": {},
        "materialize": False,
        "requiresApproval": False,
        "writesTable": None,
        "languageBindings": {},
    }


async def test_the_form_offers_the_read_it_saves(client):
    resp = await client.post(
        "/admin/graphql",
        json={
            "query": f'{{ availableFunctions(sourceId: "{_SOURCE}", schemaName: "graphql") '
            "{ name } }"
        },
    )
    assert resp.status_code == 200, resp.text
    names = {f["name"] for f in resp.json()["data"]["availableFunctions"]}
    assert {"animal_breed", "keeper"} <= names


async def test_a_read_whose_plain_list_is_unregistered_is_refused_by_name(client):
    resp = await client.post(
        "/admin/actions/functions", json=_form(f"breed_{_RUN}", "animal_breed")
    )
    assert resp.status_code == 422, resp.text
    assert resp.json()["code"] == "functions.plain_list_unregistered", resp.text


async def test_a_read_with_no_plain_list_saves(client):
    resp = await client.post("/admin/actions/functions", json=_form(f"keeper_{_RUN}", "keeper"))
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"success": True, "name": f"keeper_{_RUN}"}


async def test_a_read_whose_plain_list_is_registered_saves(client):
    reg = await client.post(
        "/admin/graphql",
        json={
            "query": f"""mutation {{ registerTable(input: {{
                sourceId: "{_SOURCE}", domainId: "{_DOMAIN}", schemaName: "graphql",
                tableName: "animal_breeds",
                columns: [{{ name: "name", visibleTo: ["org_admin"] }},
                          {{ name: "species", visibleTo: ["org_admin"] }}]
            }}) {{ success message code }} }}"""
        },
    )
    assert reg.status_code == 200, reg.text
    assert "errors" not in reg.json(), reg.text
    assert reg.json()["data"]["registerTable"]["success"], reg.text
    resp = await client.post(
        "/admin/actions/functions", json=_form(f"breed2_{_RUN}", "animal_breed")
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"success": True, "name": f"breed2_{_RUN}"}
