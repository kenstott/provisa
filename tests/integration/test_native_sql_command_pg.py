# Copyright (c) 2026 Kenneth Stott
# Canary: 3f9e2c71-5a4d-4b80-9c16-7d2e8a0b4f53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A source_query command holding native SQL, run on a PostgreSQL source (REQ-1988, amended
2026-11-13).

One real server over a PostgreSQL source holding ``orders(id PRIMARY KEY, region)``. Its commands
hold native PostgreSQL: several statements run in one transaction on the source, all committing
or none; a call's arguments are bound as values; the call answers the last statement's rows
against the declared output; and the call is governed as any command is. Writes are asserted on
the rows AT THE SOURCE.
"""

# Requirements: REQ-1988

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request

import pytest
import sqlalchemy as sa

from tests.helpers import config_write_grants
from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_SEED = [(1, "east"), (2, "west"), (3, "east")]
_ROLES = ["org_admin", "writer", "outsider"]
_ASSIGNED = ["org_admin", "writer"]

_ADD_PAIR = """
INSERT INTO public.orders (id, region) VALUES ({{a}}, {{r}});
INSERT INTO public.orders (id, region) VALUES ({{b}}, {{r}});
SELECT id, region FROM public.orders WHERE id IN ({{a}}, {{b}}) ORDER BY id
"""
_REGION_COUNT = (
    "SELECT region, count(*)::bigint AS n FROM public.orders WHERE region = {{r}} GROUP BY region"
)
_ORDER_COLUMNS = [{"name": "id", "type": "integer"}, {"name": "region", "type": "text"}]


def _command(name: str, kind: str, sql: str, arguments: list[dict], **extra) -> dict:
    return {
        "name": name,
        "source_id": "sales-pg",
        "schema": "public",
        "function_name": name,
        "returns": "",
        "kind": kind,
        "impl_kind": "source_query",
        "binding": {"sql": sql},
        "domain_id": "sales",
        "visible_to": _ASSIGNED,
        "arguments": arguments,
        **extra,
    }


_PAIR_ARGS = [
    {"name": "a", "type": "integer"},
    {"name": "b", "type": "integer"},
    {"name": "r", "type": "text"},
]


@pytest.fixture(scope="module")
def server():
    base = _config(_PG_HOST, _PG_PORT, "unused")
    orders = base["tables"][0]
    for column in orders["columns"]:
        column["visible_to"] = _ROLES
        if column["name"] == "id":
            column["is_primary_key"] = True
    orders["write_grants"] = config_write_grants(
        ["org_admin"], [c["name"] for c in orders["columns"]]
    )
    reads = ["query_development", "full_results"]
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "tables": [orders],
            "roles": [
                {
                    "id": "writer",
                    "description": "Test role.",
                    "capabilities": [*reads, "command_execution"],
                    "domain_access": ["*"],
                },
                {
                    "id": "outsider",
                    "description": "Test role.",
                    "capabilities": [*reads, "command_execution"],
                    "domain_access": ["*"],
                },
            ],
            "functions": [
                _command(
                    "add_pair",
                    "mutation",
                    _ADD_PAIR,
                    _PAIR_ARGS,
                    writes_table="public.orders",
                    output_columns=_ORDER_COLUMNS,
                ),
                _command(
                    "region_count",
                    "query",
                    _REGION_COUNT,
                    [{"name": "r", "type": "text"}],
                    output_columns=[
                        {"name": "region", "type": "text"},
                        {"name": "n", "type": "bigint"},
                    ],
                ),
                _command(
                    "region_total",
                    "query",
                    _REGION_COUNT,
                    [{"name": "r", "type": "text"}],
                    output_columns=[{"name": "total", "type": "bigint"}],
                ),
                _command(
                    "add_approved",
                    "mutation",
                    "INSERT INTO public.orders (id, region) VALUES ({{a}}, 'approved')",
                    [{"name": "a", "type": "integer"}],
                    writes_table="public.orders",
                    output_columns=[],
                    requires_approval=True,
                ),
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


@pytest.fixture
def source(server):
    engine = sa.create_engine(server.url, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        conn.execute(sa.text("DELETE FROM public.orders"))
        for order_id, region in _SEED:
            conn.execute(
                sa.text("INSERT INTO public.orders (id, region) VALUES (:i, :r)"),
                {"i": order_id, "r": region},
            )

    def rows() -> list[tuple[int, str]]:
        with engine.connect() as conn:
            return [
                tuple(r)
                for r in conn.execute(sa.text("SELECT id, region FROM public.orders ORDER BY id"))
            ]

    yield rows
    engine.dispose()


def _http(boot, role: str, path: str, body: dict) -> tuple[int, str, dict]:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": role},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status, resp.read().decode(), dict(resp.headers)
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode(), dict(exc.headers)


def _sql(boot, role: str, sql: str) -> tuple[bool, str]:
    status, body, _ = _http(boot, role, "/data/sql", {"sql": sql})
    return status == 200 and '"errors"' not in body, body


def _rows(body: str) -> list[dict]:
    return json.loads(body)["data"]["sql"]


def test_a_failing_second_write_leaves_neither_row(server, source):
    # Order 1 exists: the second INSERT violates the primary key, and the first is undone with it.
    ok, text = _sql(server, "writer", "SELECT * FROM add_pair(10, 1, 'north')")
    assert not ok, text
    assert source() == [(1, "east"), (2, "west"), (3, "east")]


def test_two_writes_that_succeed_leave_both_and_answer_the_last_statement(server, source):
    ok, text = _sql(server, "writer", "SELECT * FROM add_pair(10, 11, 'north')")
    assert ok, f"{text}\n{server.dump_stderr_debug()[-6000:]}"
    assert _rows(text) == [{"id": 10, "region": "north"}, {"id": 11, "region": "north"}]
    assert source() == [(1, "east"), (2, "west"), (3, "east"), (10, "north"), (11, "north")]


def test_a_value_holding_a_quote_and_a_semicolon_is_stored_as_data(server, source):
    value = "it's; DROP TABLE public.orders; --"
    quoted = value.replace("'", "''")
    ok, text = _sql(server, "writer", f"SELECT * FROM add_pair(12, 13, '{quoted}')")
    assert ok, text
    assert source()[-2:] == [(12, value), (13, value)]


@pytest.mark.usefixtures("source")
def test_a_reading_command_answers_its_declared_output(server):
    ok, text = _sql(server, "writer", "SELECT * FROM region_count('east')")
    assert ok, text
    assert _rows(text) == [{"region": "east", "n": 2}]


@pytest.mark.usefixtures("source")
def test_rows_off_the_declared_output_fail_the_call(server):
    ok, text = _sql(server, "writer", "SELECT * FROM region_total('east')")
    assert not ok, text
    assert "total" in text


def test_a_role_not_assigned_the_command_is_refused(server, source):
    ok, text = _sql(server, "outsider", "SELECT * FROM add_pair(20, 21, 'north')")
    assert not ok, text
    assert source() == [(1, "east"), (2, "west"), (3, "east")]


def test_a_command_needing_approval_with_no_hook_is_refused(server, source):
    ok, text = _sql(server, "writer", "SELECT * FROM add_approved(30)")
    assert not ok, text
    assert "needs approval" in text
    assert source() == [(1, "east"), (2, "west"), (3, "east")]


@pytest.mark.usefixtures("source")
def test_after_a_writing_call_the_held_rows_of_its_table_stop_being_served(server):
    query = {"query": "query @cached { s__orders { id region } }"}

    def read() -> tuple[str | None, list[dict]]:
        status, body, headers = _http(server, "writer", "/data/graphql", query)
        assert status == 200, body
        cache = {k.lower(): v for k, v in headers.items()}.get("x-provisa-cache")
        return cache, json.loads(body)["data"]["s__orders"]

    read()
    cache, held = read()
    assert cache == "HIT"
    ok, text = _sql(server, "writer", "SELECT * FROM add_pair(40, 41, 'south')")
    assert ok, text
    cache, rows = read()
    assert cache != "HIT"
    assert {"id": 40, "region": "south"} in rows and len(rows) == len(held) + 2
