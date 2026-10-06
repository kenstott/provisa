# Copyright (c) 2026 Kenneth Stott
# Canary: 6f2a9d41-b8c3-4e57-a1d0-93c7e5b2f816
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Polly's fakes tools through the MCP endpoint of a real server (REQ-1494, REQ-1857).

One server over PostgreSQL with ``orders(id, region, email)``. ``steward`` holds
table_registration, ``analyst`` does not. Over the Streamable-HTTP MCP endpoint: the analyst is
refused by name; the steward finds the table's id, declares a fake on ``email`` -- checked as the
table editor checks it and saved through the editor's own ``updateTable`` -- and reads it back; a
fake the check refuses comes back in the check's words and changes nothing."""

# Requirements: REQ-1494, REQ-1857, REQ-1934

from __future__ import annotations

import asyncio
import json
import os

import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_ROLES = ["org_admin", "analyst", "steward"]


@pytest.fixture(scope="module")
def server():
    base = _config(_PG_HOST, _PG_PORT, "unused")
    orders = base["tables"][0]
    orders["columns"] = [
        {"name": "id", "data_type": "integer", "visible_to": _ROLES, "is_primary_key": True},
        {"name": "region", "data_type": "varchar", "visible_to": _ROLES},
        {"name": "email", "data_type": "varchar", "visible_to": _ROLES},
    ]
    reads = ["query_development", "full_results"]
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "tables": [orders],
            # org_admin is reserved (REQ-1349): a config may not define it.
            "roles": [
                {"id": "analyst", "capabilities": reads, "domain_access": ["*"]},
                {
                    "id": "steward",
                    "capabilities": [*reads, "table_registration"],
                    "domain_access": ["*"],
                },
            ],
        },
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    try:
        engine = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            conn.execute(sa.text("ALTER TABLE public.orders ADD COLUMN email varchar"))
        engine.dispose()
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _call(boot, tool: str, args: dict) -> tuple[bool, str]:
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    async def _go() -> tuple[bool, str]:
        url = f"http://127.0.0.1:{boot.ports['mcp']}/mcp"
        async with streamablehttp_client(url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool(tool, args)
                return bool(result.isError), "".join(getattr(c, "text", "") for c in result.content)

    # Asserted by the caller: inside the client's task group a failure arrives wrapped in a group.
    return asyncio.run(_go())


def _ok(boot, tool: str, args: dict):
    is_error, text = _call(boot, tool, args)
    assert not is_error, text
    return json.loads(text)


def _orders_id(boot) -> int:
    found = _ok(boot, "find_table_id", {"domain": "sales", "table": "orders", "role": "steward"})
    found = found if isinstance(found, list) else [found]
    assert [f["table"] for f in found] == ["orders"], found
    return found[0]["id"]


def test_a_role_without_table_registration_is_refused_by_name(server):
    is_error, text = _call(
        server, "find_table_id", {"domain": "sales", "table": "orders", "role": "analyst"}
    )
    assert is_error and "Missing capability: 'table_registration'" in text, text


def test_a_fake_is_declared_through_the_editors_save_and_read_back(server):
    table_id = _orders_id(server)
    saved = _ok(
        server,
        "set_column_fake",
        {"table_id": table_id, "column": "email", "fake": "email()", "role": "steward"},
    )
    assert saved["fake"] == "email()", saved
    fakes = _ok(server, "get_table_fakes", {"table_id": table_id, "role": "steward"})
    by_column = {c["column"]: c for c in fakes["columns"]}
    assert by_column["email"]["fake"] == "email()", fakes
    assert by_column["region"]["fake"] is None, fakes


def test_a_refused_fake_comes_back_in_the_checks_words_and_changes_nothing(server):
    table_id = _orders_id(server)
    is_error, text = _call(
        server,
        "set_column_fake",
        {"table_id": table_id, "column": "region", "fake": "bool(2)", "role": "steward"},
    )
    assert is_error, text
    assert "bool" in text, text
    fakes = _ok(server, "get_table_fakes", {"table_id": table_id, "role": "steward"})
    assert {c["column"]: c["fake"] for c in fakes["columns"]}["region"] is None, fakes
