# Copyright (c) 2026 Kenneth Stott
# Canary: 9e2c7a4b-1d6f-4b8e-a3c0-5f7d2b9e4a17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1679: an RLS rule on a tracked function filters the rows the function returns, end to end
through the real app — the admin GraphQL saves the rule against the function's contract, the data
GraphQL invokes the function, and the rows come back governed."""

import os

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]

ACTION = "random_python_set"
ROLE = "org_admin"


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")
    from provisa.api.app import create_app

    app = create_app()
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


async def _admin(client, query):
    resp = await client.post("/admin/graphql", json={"query": query})
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _data(client, query):
    resp = await client.post(
        "/data/graphql", json={"query": query}, headers={"x-provisa-role": ROLE}
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _field_name(client) -> str:
    body = await _data(client, "{ __schema { queryType { fields { name } } } }")
    names = [f["name"] for f in body["data"]["__schema"]["queryType"]["fields"]]
    # The field carries its domain prefix under the org's naming convention (ps__randomPythonSet).
    wanted = ACTION.replace("_", "")
    matches = [n for n in names if n.split("__", 1)[-1].lower() == wanted]
    assert matches, f"{ACTION} not exposed to {ROLE}: {names}"
    return matches[0]


class TestActionRls:
    async def test_rule_over_unknown_column_refused(self, client):
        res = await _admin(
            client,
            f'''mutation {{ upsertRlsRule(input: {{ actionName: "{ACTION}", roleId: "{ROLE}",
                filterExpr: "regon = 'east'" }}) {{ success message code }} }}''',
        )
        out = res["data"]["upsertRlsRule"]
        assert out["success"] is False
        assert out["code"] == "schema.rls_rule_invalid"
        assert "regon" in out["message"]

    async def test_rule_filters_the_functions_rows(self, client):
        res = await _admin(
            client,
            f'''mutation {{ upsertRlsRule(input: {{ actionName: "{ACTION}", roleId: "{ROLE}",
                filterExpr: "region = 'east'" }}) {{ success message code }} }}''',
        )
        assert res["data"]["upsertRlsRule"]["success"] is True, res
        listed = await _admin(client, "{ rlsRules { actionName roleId filterExpr } }")
        assert {"actionName": ACTION, "roleId": ROLE, "filterExpr": "region = 'east'"} in [
            {k: r[k] for k in ("actionName", "roleId", "filterExpr")}
            for r in listed["data"]["rlsRules"]
        ]
        # The rule is read into the runtime on rebuild, as a table rule is.
        await _admin(client, "mutation { rebuildSchemas { success } }")
        field = await _field_name(client)
        body = await _data(client, f"{{ {field}(rows: 40, seed: 7) {{ id region }} }}")
        assert not body.get("errors"), body
        rows = body["data"][field]
        assert rows, "seed 7 over 40 rows yields east rows"
        assert {r["region"] for r in rows} == {"east"}

        gone = await _admin(
            client,
            f'mutation {{ deleteRlsRule(roleId: "{ROLE}", actionName: "{ACTION}") {{ success }} }}',
        )
        assert gone["data"]["deleteRlsRule"]["success"] is True
