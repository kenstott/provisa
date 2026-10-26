# Copyright (c) 2026 Kenneth Stott
# Canary: 6e1b9d04-3f7a-4c52-8a1e-5d0c7b2f9e68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1365, on a real server: resolve(uri, as_of) over GraphQL, REST and MCP.

One server, one keyed table with two rows. The keyed_reader role is restricted three ways: an RLS
rule leaves it one row, ``region`` is masked for it, and it has no ``full_results``. Each surface
dereferences the same semantic address and must answer what the role's own query would: the
masked value, nothing for the row RLS hides, and a refusal for a physical address. The audit
record of a resolve is the pipeline's own, so its provenance fields are asserted too.
"""

# Requirements: REQ-1365, REQ-1929

from __future__ import annotations

import asyncio
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_ADDRESS = "provisa://sales/tables/orders/pk={key}"
_AUDIT_WAIT_SECONDS = 30


@pytest.fixture(scope="module")
def server():
    base = _config(_PG_HOST, _PG_PORT, "unused")
    orders = base["tables"][0]
    for column in orders["columns"]:
        if column["name"] == "id":
            column["is_primary_key"] = True
        if column["name"] == "region":
            column["masks"] = [
                {"to": ["org_admin"], "mask": None},
                {"to": ["*"], "mask": {"type": "constant", "value": "MASKED"}},
            ]
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "tables": [orders],
            # REQ-2155: the restricted role is the test's own, a child of the standard analyst
            # (whose grants on the table it inherits). A rule on the standard analyst would hold
            # for every role that inherits from it, org_admin among them.
            "roles": [
                {
                    "id": "keyed_reader",
                    "description": "Test role.",
                    "parents": ["analyst"],
                    "capabilities": [],
                    "domain_access": ["*"],
                }
            ],
            "rls_rules": [
                {
                    "description": "Test rule.",
                    "table_id": "orders",
                    "role_id": "keyed_reader",
                    "filter": "id = 1",
                }
            ],
        },
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _http(boot, method: str, path: str, role: str, body: dict | None = None) -> tuple[int, dict]:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": role},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def _rest(boot, role: str, uri: str, **params: str) -> tuple[int, dict]:
    query = urllib.parse.urlencode({"uri": uri, **params})
    return _http(boot, "GET", f"/data/rest/resolve?{query}", role)


def _graphql(boot, role: str, uri: str) -> tuple[int, dict]:
    return _http(
        boot,
        "POST",
        "/data/graphql",
        role,
        {"query": "query($u: String!) { resolve(uri: $u) }", "variables": {"u": uri}},
    )


def _mcp(boot, role: str, uri: str) -> tuple[bool, str]:
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    async def _call() -> tuple[bool, str]:
        url = f"http://127.0.0.1:{boot.ports['mcp']}/mcp"
        async with streamablehttp_client(url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool("resolve", {"uri": uri, "role": role})
                return result.isError, "".join(getattr(c, "text", "") for c in result.content)

    return asyncio.run(_call())


# --- the governed value, the same on every surface ------------------------------------------------


def test_rest_returns_the_whole_governed_row(server):
    status, body = _rest(server, "org_admin", _ADDRESS.format(key=2))
    assert status == 200, body
    assert body["row"] == {"id": 2, "region": "west"}
    assert body["uri"] == _ADDRESS.format(key=2)


def test_rest_returns_one_column_in_either_address_form(server):
    for uri in (_ADDRESS.format(key=1) + "/region", _ADDRESS.format(key=1) + "#field:region"):
        status, body = _rest(server, "org_admin", uri)
        assert status == 200, body
        assert body["row"] == {"region": "east"}


def test_a_masked_column_resolves_to_its_mask(server):
    status, body = _rest(server, "keyed_reader", _ADDRESS.format(key=1) + "/region")
    assert status == 200, body
    assert body["row"] == {"region": "MASKED"}


def test_a_row_rls_hides_resolves_to_nothing(server):
    status, body = _rest(server, "keyed_reader", _ADDRESS.format(key=2))
    assert status == 200, body
    assert body["row"] is None
    # ... exactly what a key that matches no row answers.
    status, absent = _rest(server, "keyed_reader", _ADDRESS.format(key=999))
    assert status == 200, absent
    assert absent["row"] is None


def test_graphql_answers_the_same_as_rest(server):
    status, body = _graphql(server, "keyed_reader", _ADDRESS.format(key=1))
    assert status == 200, body
    assert body["data"]["resolve"]["row"] == {"id": 1, "region": "MASKED"}
    status, body = _graphql(server, "keyed_reader", _ADDRESS.format(key=2))
    assert status == 200, body
    assert body["data"]["resolve"]["row"] is None


def test_mcp_answers_the_same_as_rest(server):
    is_error, text = _mcp(server, "keyed_reader", _ADDRESS.format(key=1))
    assert not is_error, text
    assert json.loads(text)["row"] == {"id": 1, "region": "MASKED"}
    is_error, text = _mcp(server, "keyed_reader", _ADDRESS.format(key=2))
    assert not is_error, text
    assert json.loads(text)["row"] is None


# --- what is refused ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "uri", ["postgresql://db:5432/sales/public/orders", "sales-pg.public.orders", "s3://b/orders"]
)
def test_a_physical_address_is_refused_on_every_surface(server, uri):
    status, body = _rest(server, "org_admin", uri)
    assert status == 400, body
    assert "physical address is refused" in body["detail"]
    status, body = _graphql(server, "org_admin", uri)
    assert "physical address is refused" in json.dumps(body)
    is_error, text = _mcp(server, "org_admin", uri)
    assert is_error
    assert "physical address is refused" in text


def test_a_table_outside_the_model_is_not_found(server):
    status, body = _rest(server, "org_admin", "provisa://sales/tables/ledger/pk=1")
    assert status == 404, body
    assert "names nothing visible" in body["detail"]


def test_as_of_on_a_table_with_no_history_is_refused_by_name(server):
    status, body = _rest(server, "org_admin", _ADDRESS.format(key=1), as_of="2026-01-01T00:00:00")
    assert status == 400, body
    assert "keeps no history" in body["detail"]


# --- audited like any other query ------------------------------------------------------------------


def test_a_resolve_is_audited_with_the_pipelines_provenance(server):
    engine = sa.create_engine(server.url)
    try:
        with engine.connect() as conn:
            schema = conn.execute(
                sa.text(
                    "SELECT table_schema FROM information_schema.tables "
                    "WHERE table_name = 'query_audit_log'"
                )
            ).scalar_one()

            def _rows() -> int:
                return conn.execute(
                    sa.text(
                        f'SELECT count(*) FROM "{schema}".query_audit_log '
                        "WHERE role_id = 'keyed_reader' AND status_code = 200 AND row_count = 1 "
                        "AND route IS NOT NULL AND table_ids::text <> '[]'"
                    )
                ).scalar_one()

            before = _rows()
            status, body = _rest(server, "keyed_reader", _ADDRESS.format(key=1))
            assert status == 200, body
            deadline = time.monotonic() + _AUDIT_WAIT_SECONDS
            while _rows() == before and time.monotonic() < deadline:
                time.sleep(0.5)
            assert _rows() == before + 1
    finally:
        engine.dispose()
