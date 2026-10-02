# Copyright (c) 2026 Kenneth Stott
# Canary: 58402b66-626d-4b8e-a020-647905c9e783
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A role is deleted the same way through GraphQL and REST, on each control plane (REQ-042, REQ-1677).

A served instance on a PostgreSQL control plane and one on a SQLite control plane are each asked,
through ``deleteRole`` and through ``DELETE /admin/roles/{id}``, to delete a seeded role, a role
others inherit from, and a role an administrator created. The answers and the rows left behind
are the same on all four combinations. Everything here lands on the TEST instance: dedicated
server processes on their own ports with their own control planes.
"""

# Requirements: REQ-042, REQ-1677, REQ-1297

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import httpx
import pytest

from tests.integration.isolated_server import IsolatedServer, drop_org_schema

pytestmark = [pytest.mark.integration]

_PG_ORG = "role_delete_pg"


@pytest.fixture(scope="module", params=["postgres", "sqlite"])
def server(request):
    if request.param == "postgres":
        srv = IsolatedServer(_PG_ORG, config="tests/fixtures/sample_config.yaml")
        srv.start()
        try:
            yield srv
        finally:
            srv.stop_process()
            asyncio.run(drop_org_schema(_PG_ORG))
        return
    pytest.importorskip("duckdb")
    store_dir = tempfile.TemporaryDirectory()
    srv = IsolatedServer(
        "role_delete_sqlite",
        engine="duckdb",
        config="tests/fixtures/duckdb_sqlite_config.yaml",
        control_plane="sqlite",
        materialize_store_url=f"duckdb:///{Path(store_dir.name) / 'materialize.duckdb'}",
    )
    srv.start()
    try:
        yield srv
    finally:
        srv.stop_process()
        store_dir.cleanup()


def _client(srv: IsolatedServer) -> httpx.Client:
    return httpx.Client(base_url=srv.base_url, timeout=srv.request_timeout + 15)


def _gql(c: httpx.Client, query: str) -> dict:
    resp = c.post("/admin/graphql", json={"query": query})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "errors" not in body, body
    return body["data"]


def _role_ids(c: httpx.Client) -> set[str]:
    return {r["id"] for r in _gql(c, "{ roles { id } }")["roles"]}


def _create_graphql(c: httpx.Client, role_id: str, parent: str | None = None) -> None:
    parent_arg = f', parentRoleId: "{parent}"' if parent else ""
    made = _gql(
        c,
        f"""mutation {{ createRole(input: {{
            id: "{role_id}", capabilities: ["query_development"], domainAccess: ["*"]{parent_arg}
        }}) {{ success message }} }}""",
    )["createRole"]
    assert made["success"] is True, made


def _create_rest(c: httpx.Client, role_id: str, parent: str | None = None) -> None:
    resp = c.post(
        "/admin/roles/",
        json={
            "id": role_id,
            "capabilities": ["query_development"],
            "domain_access": ["*"],
            "parent_role_id": parent,
        },
    )
    assert resp.status_code == 200, resp.text


def _delete_graphql(c: httpx.Client, role_id: str) -> dict:
    return _gql(c, f'mutation {{ deleteRole(id: "{role_id}") {{ success code message }} }}')[
        "deleteRole"
    ]


def _delete_rest(c: httpx.Client, role_id: str) -> httpx.Response:
    return c.delete(f"/admin/roles/{role_id}")


def test_a_seeded_role_is_refused_on_both_entry_points(server):
    with _client(server) as c:
        answer = _delete_graphql(c, "analyst")
        assert (answer["success"], answer["code"]) == (False, "schema.role_is_system"), answer
        resp = _delete_rest(c, "analyst")
        assert resp.status_code == 400, resp.text
        assert resp.json()["code"] == "roles.cannot_delete_system", resp.text
        assert "analyst" in _role_ids(c)


@pytest.mark.parametrize(
    "create", [_create_graphql, _create_rest], ids=["made-graphql", "made-rest"]
)
def test_a_parent_is_refused_on_both_entry_points_then_goes_after_its_heir(server, create):
    parent, heir = f"lead_{create.__name__}", f"member_{create.__name__}"
    with _client(server) as c:
        create(c, parent)
        create(c, heir, parent)

        answer = _delete_graphql(c, parent)
        assert (answer["success"], answer["code"]) == (False, "schema.role_has_dependents"), answer
        assert heir in answer["message"]
        resp = _delete_rest(c, parent)
        assert resp.status_code == 409, resp.text
        refusal = resp.json()
        assert refusal["code"] == "roles.has_dependents", refusal
        assert refusal["params"] == {
            "role": parent,
            "count": 1,
            "dependents": [{"kind": "role", "id": heir, "via": ["roles.parent_role_id"]}],
        }, refusal
        assert {parent, heir} <= _role_ids(c)

        # The heir through one entry point, the parent through the other.
        assert _delete_rest(c, heir).status_code == 200
        answer = _delete_graphql(c, parent)
        assert (answer["success"], answer["code"]) == (True, "schema.role_deleted"), answer
        assert not {parent, heir} & _role_ids(c)


@pytest.mark.parametrize(
    ("create", "delete_through_rest"),
    [(_create_graphql, True), (_create_rest, False)],
    ids=["made-graphql-deleted-rest", "made-rest-deleted-graphql"],
)
def test_a_created_role_is_deleted_through_the_other_entry_point(
    server, create, delete_through_rest
):
    role_id = f"loose_{create.__name__}"
    with _client(server) as c:
        create(c, role_id)
        assert role_id in _role_ids(c)
        if create is _create_graphql:  # createRole rebuilds, so the role is served at once
            served = c.get("/data/sdl", headers={"x-provisa-role": role_id})
            assert served.status_code == 200, served.text[:300]
        if delete_through_rest:
            resp = _delete_rest(c, role_id)
            assert resp.status_code == 200, resp.text
        else:
            answer = _delete_graphql(c, role_id)
            assert answer["success"] is True, answer
        assert role_id not in _role_ids(c)
        # The rebuild both paths run: the runtime no longer knows the role at all, so a request
        # naming it is refused before any schema is looked for.
        served = c.get("/data/sdl", headers={"x-provisa-role": role_id})
        assert served.status_code == 403, served.text[:300]
        assert f"unknown role {role_id!r}" in served.json()["detail"], served.text[:300]
        # Deleting it again finds nothing, on either path.
        assert _delete_rest(c, role_id).status_code == 404
        assert _delete_graphql(c, role_id)["code"] == "schema.role_not_found"
