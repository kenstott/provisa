# Copyright (c) 2026 Kenneth Stott
# Canary: 3e8b1f62-5d07-4a9c-b2e4-7c1a9d0f6e35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A database source's routines are offered, never registered, on a real server (REQ-887).

A real isolated server (DuckDB engine) over the stack's Postgres, which it reaches directly
(``sales-pg``). The source's ``public`` schema holds ``get_customers_by_region``. The admin
``availableFunctions`` picker offers the routine with its kind; listing it registers nothing, and
a command exists once a steward saves one. (That catalog indexing registers none either is proven
in tests/unit/test_discovery_through_driver.py.)
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

pytestmark = [pytest.mark.integration]

_ORG = "routines_offered_e2e"
_ROUTINE = "get_customers_by_region"


@pytest.fixture(scope="module")
def server():
    from tests.integration.isolated_server import IsolatedServer, drop_org_schema

    srv = IsolatedServer(
        _ORG,
        engine="duckdb",
        config="tests/fixtures/sample_config.yaml",
        control_plane="postgres",
    )
    try:
        srv.start()
        yield srv
    finally:
        srv.stop_process()
        asyncio.run(drop_org_schema(_ORG))


def _client(srv) -> httpx.Client:
    return httpx.Client(base_url=srv.base_url, timeout=srv.request_timeout + 15)


def _registered(c: httpx.Client) -> set[str]:
    resp = c.get("/admin/actions")
    assert resp.status_code == 200, resp.text
    return {f["functionName"] for f in resp.json()["functions"]}


def test_the_schemas_routines_are_offered_and_none_is_registered(server):
    with _client(server) as c:
        assert _ROUTINE not in _registered(c)
        resp = c.post(
            "/admin/graphql",
            json={
                "query": '{ availableFunctions(sourceId: "sales-pg", schemaName: "public") '
                "{ name comment } }"
            },
        )
        assert resp.status_code == 200, resp.text
        payload = resp.json()
        assert not payload.get("errors"), payload
        offered = {f["name"]: f["comment"] for f in payload["data"]["availableFunctions"]}
        assert offered.get(_ROUTINE) == "query", offered
        assert _ROUTINE not in _registered(c)  # listing what is on offer registers nothing


def test_a_steward_registers_an_offered_routine(server):
    with _client(server) as c:
        resp = c.post(
            "/admin/actions/functions",
            json={
                "name": "customersByRegion",
                "sourceId": "sales-pg",
                "schemaName": "public",
                "functionName": _ROUTINE,
                "returns": "",
                "arguments": [{"name": "p_region", "type": "String"}],
                "visibleTo": ["org_admin"],
                "domainId": "sales-analytics",
                "kind": "query",
            },
        )
        assert resp.status_code == 200, resp.text
        assert _ROUTINE in _registered(c)
