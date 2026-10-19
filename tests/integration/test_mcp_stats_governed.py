# Copyright (c) 2026 Kenneth Stott
# Canary: e52755d9-677a-408a-9f47-7496ab5b8ef1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The MCP stats tools are governed as run_sql is, on a real server (REQ-2090).

One server over PostgreSQL: ``orders(id, region, email, amount)`` with ``email`` masked except to
``org_admin`` and ``analyst`` reading the east rows only. A stats tool answers over what the role
reads: the analyst's n is the east rows', the email values it is told of are masked, and a model
reads at most ``mcp.stats_max_rows`` rows, saying so."""

# Requirements: REQ-2090

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
_ROLES = ["org_admin", "analyst"]
_ROWS = [
    (1, "east", 10.0),
    (2, "west", 30.0),
    (3, "east", 12.0),
    (4, "west", 33.0),
    (5, "east", 11.0),
    (6, "west", 29.0),
    (7, "north", 50.0),
]
_CAP = 4
_SQL = "SELECT id, region, email, amount FROM sales.orders"


@pytest.fixture(scope="module")
def server():
    base = _config(_PG_HOST, _PG_PORT, "unused")
    orders = base["tables"][0]
    orders["columns"] = [
        {"name": "id", "data_type": "integer", "visible_to": _ROLES, "is_primary_key": True},
        {"name": "region", "data_type": "varchar", "visible_to": _ROLES},
        {
            "name": "email",
            "data_type": "varchar",
            "visible_to": _ROLES,
            "masks": [
                {"to": ["org_admin"], "mask": None},
                {"to": ["*"], "mask": {"type": "constant", "value": "***"}},
            ],
        },
        {"name": "amount", "data_type": "double", "visible_to": _ROLES},
    ]
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "tables": [orders],
            "roles": [
                {
                    "id": "analyst",
                    "description": "Test role.",
                    "capabilities": ["query_development", "full_results"],
                    "domain_access": ["*"],
                }
            ],
            "rls_rules": [
                {
                    "description": "Test rule.",
                    "table_id": "orders",
                    "role_id": "analyst",
                    "filter": "region = 'east'",
                }
            ],
        },
        env={"PROVISA_REDIRECT_ENABLED": "false", "PROVISA_MCP_STATS_MAX_ROWS": str(_CAP)},
    )
    boot.create_database()
    try:
        engine = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            conn.execute(sa.text("ALTER TABLE public.orders ADD COLUMN email varchar"))
            conn.execute(sa.text("ALTER TABLE public.orders ADD COLUMN amount double precision"))
            conn.execute(sa.text("DELETE FROM public.orders"))
            for order_id, region, amount in _ROWS:
                conn.execute(
                    sa.text(
                        "INSERT INTO public.orders (id, region, email, amount) "
                        "VALUES (:i, :r, :e, :a)"
                    ),
                    {"i": order_id, "r": region, "e": f"u{order_id}@x", "a": amount},
                )
        engine.dispose()
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _call(boot, tool: str, args: dict) -> dict:
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    async def _run() -> tuple[bool, str]:
        url = f"http://127.0.0.1:{boot.ports['mcp']}/mcp"
        async with streamablehttp_client(url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool(tool, args)
                return bool(result.isError), "".join(getattr(c, "text", "") for c in result.content)

    # Asserted out here: inside the client's task group a failure arrives wrapped in a group.
    is_error, text = asyncio.run(_run())
    assert not is_error, text
    return json.loads(text)


def _column(answer: dict, name: str) -> dict:
    return next(c for c in answer["columns"] if c["column"] == name)


def test_a_row_rule_narrows_n(server):
    every = _call(server, "describe", {"sql": _SQL, "role": "org_admin"})
    east = _call(server, "describe", {"sql": _SQL, "role": "analyst"})
    assert every["n"] == 7 and east["n"] == 3
    assert _column(east, "amount")["mean"] == pytest.approx(11.0)


def test_a_masked_column_is_described_as_the_role_reads_it(server):
    east = _call(server, "describe", {"sql": _SQL, "role": "analyst"})
    email = _column(east, "email")
    assert email["top_values"] == [{"value": "***", "count": 3}] and email["distinct"] == 1
    admin = _call(server, "describe", {"sql": _SQL, "role": "org_admin"})
    assert _column(admin, "email")["distinct"] == 7


def test_a_comparison_counts_only_the_rows_the_role_reads(server):
    every = _call(
        server, "compare", {"sql": _SQL, "value": "amount", "group": "region", "role": "org_admin"}
    )
    assert every["method"] == "one-way ANOVA" and every["n"] == 7
    with pytest.raises(AssertionError, match="two or more groups"):
        _call(
            server,
            "compare",
            {"sql": _SQL, "value": "amount", "group": "region", "role": "analyst"},
        )


def test_a_model_reports_the_row_cap(server):
    answer = _call(
        server,
        "correlate",
        {"sql": _SQL + " ORDER BY id", "columns": ["id", "amount"], "role": "org_admin"},
    )
    assert answer["n"] == _CAP and answer["row_cap"] == _CAP and answer["truncated"] is True


def test_sql_the_role_may_not_run_is_refused_as_run_sql_refuses_it(server):
    with pytest.raises(AssertionError):
        _call(server, "describe", {"sql": "SELECT * FROM sales.nothing_here", "role": "analyst"})
