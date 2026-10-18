# Copyright (c) 2026 Kenneth Stott
# Canary: b83e1f07-6c2d-4a95-91e4-d7a05c3f8b62
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A row filter on a tag, and one on a domain, decide the ROWS on every transport (REQ-1384).

One real server, one table of four orders in two regions, carrying the tag ``regional``:

* ``org_admin`` — no row filter: every order;
* ``tag_east`` — a filter on the TAG, ``region = 'east'``: the east orders;
* ``tag_and_table`` — its own table filter ``id > 1`` AND the tag's: the one east order past 1;
* ``domain_west`` — a filter on the table's DOMAIN, ``region = 'west'`` (REQ-402): the west
  orders. A domain filter on a role with no ancestors never reached the governance path.

Each transport is read as each role and the assertion is on the rows that came back. Taking
the tag off the table through the admin API stops the tag's filter at once, and putting it
back restores it: the role contexts are rebuilt by the assignment, not by an unrelated change.
Everything here lands on the TEST instance: a dedicated server on its own ports and database."""

# Requirements: REQ-1384, REQ-402

from __future__ import annotations

import os

import pytest
import sqlalchemy as sa

from tests.integration.test_row_filter_every_transport import _TRANSPORTS, _http, _ok
from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_NEW_ROLES = ["tag_east", "tag_and_table", "domain_west"]
_ALL = {1, 2, 3, 4}
_EAST = {1, 3}


@pytest.fixture(scope="module")
def server():
    base = _config(_PG_HOST, _PG_PORT, "unused")
    orders = base["tables"][0]
    # org_admin is the reserved administrative role every org has (REQ-1349): no config lists
    # it, and the columns are visible to it as to the roles the config defines.
    roles = ["org_admin"] + [r["id"] for r in base["roles"]] + _NEW_ROLES
    for column in orders["columns"]:
        column["visible_to"] = roles
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "tables": [orders],
            "roles": base["roles"]
            + [
                {
                    "id": r,
                    "description": "Test role.",
                    "capabilities": ["query_development"],
                    "domain_access": ["*"],
                }
                for r in _NEW_ROLES
            ],
            "tags": [{"id": "regional", "applies_to": ["table"]}],
            "tag_assignments": [
                {
                    "tag_id": "regional",
                    "object_type": "table",
                    "table_ref": "sales-pg.public.orders",
                }
            ],
            "rls_rules": [
                {
                    "description": "Test rule.",
                    "tag_id": "regional",
                    "role_id": "tag_east",
                    "filter": "region = 'east'",
                },
                {
                    "description": "Test rule.",
                    "tag_id": "regional",
                    "role_id": "tag_and_table",
                    "filter": "region = 'east'",
                },
                {
                    "description": "Test rule.",
                    "table_id": "orders",
                    "role_id": "tag_and_table",
                    "filter": "id > 1",
                },
                {
                    "description": "Test rule.",
                    "domain_id": "sales",
                    "role_id": "domain_west",
                    "filter": "region = 'west'",
                },
            ],
        },
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    try:
        own = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
        with own.connect() as conn:
            conn.execute(sa.text("INSERT INTO public.orders VALUES (3, 'east'), (4, 'west')"))
        own.dispose()
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _admin(boot, query: str) -> dict:
    out = _ok(*_http(boot, "org_admin", "POST", "/admin/graphql", {"query": query}))
    assert "errors" not in out, out
    return out["data"]


@pytest.mark.parametrize("transport", list(_TRANSPORTS))
def test_each_role_gets_exactly_its_filtered_rows(server, transport):
    read = _TRANSPORTS[transport]
    assert read(server, "org_admin") == _ALL
    assert read(server, "tag_east") == _EAST
    assert read(server, "tag_and_table") == {3}  # the table's rule AND the tag's
    assert read(server, "domain_west") == _ALL - _EAST


def test_taking_the_tag_off_the_table_stops_its_filter_and_putting_it_back_restores_it(server):
    read = _TRANSPORTS["sql_http"]
    table_id = next(
        t["id"]
        for t in _admin(server, "{ tables { id tableName } }")["tables"]
        if t["tableName"] == "orders"
    )
    assignment = f'{{tagId: "regional", objectType: "table", tableId: {table_id}}}'
    assert read(server, "tag_east") == _EAST
    try:
        removed = _admin(
            server, f"mutation {{ unassignTag(input: {assignment}) {{ success message }} }}"
        )["unassignTag"]
        assert removed["success"], removed["message"]
        assert read(server, "tag_east") == _ALL
        assert read(server, "tag_and_table") == {2, 3, 4}  # its own table rule still stands
    finally:
        restored = _admin(
            server, f"mutation {{ assignTag(input: {assignment}) {{ success message }} }}"
        )["assignTag"]
    assert restored["success"], restored["message"]
    assert read(server, "tag_east") == _EAST
    assert read(server, "tag_and_table") == {3}


def test_a_tag_that_carries_a_filter_is_refused_deletion(server):
    refused = _admin(server, 'mutation { deleteTag(id: "regional") { success code params } }')[
        "deleteTag"
    ]
    assert (refused["success"], refused["code"]) == (False, "schema.tag_has_dependents")
    assert {d["kind"] for d in refused["params"]["dependents"]} == {"row_filter"}
    assert _TRANSPORTS["sql_http"](server, "tag_east") == _EAST
