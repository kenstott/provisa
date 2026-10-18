# Copyright (c) 2026 Kenneth Stott
# Canary: 0cb840c4-f8bb-4ed5-9f5a-d25aab0e5475
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A grant on a business unit's domain reaches the domains under it, on every transport.

One real server. ``sales`` (the orders table) sits under ``retail``; ``finance`` (a depots
table) stands alone. Four roles, each granted ONE domain:

* ``bu_reader`` — ``retail``: reads ``sales.orders``, a table of a domain it never lists;
* ``bu_filtered`` — ``retail``, with a row filter on ``retail``: the filter is carried down the
  tree to the orders table;
* ``late_reader`` — ``finance``: reaches neither ``sales.orders`` nor an organization added
  under ``retail`` through the admin API, which ``bu_reader`` reaches without being edited;
* ``org_admin`` — every domain.

The table is still ``sales.orders`` and ``s__orders`` everywhere: the tree changes who reaches
a domain, never how a table is addressed. Everything here lands on the TEST instance: a
dedicated server on its own ports and database."""

# Requirements: REQ-1363

from __future__ import annotations

import json
import os

import pytest
import sqlalchemy as sa

from tests.integration.test_row_filter_every_transport import _TRANSPORTS, _http, _ok
from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_NEW_ROLES = {"bu_reader": "retail", "bu_filtered": "retail", "late_reader": "finance"}
_ALL = {1, 2, 3, 4}
_EAST = {1, 3}


@pytest.fixture(scope="module")
def server():
    base = _config(_PG_HOST, _PG_PORT, "unused")
    orders = base["tables"][0]
    # org_admin is every org's reserved role, never declared in the file (REQ-1349), so it is named
    # here beside the declared ones: a column is served only to the roles it lists.
    roles = ["org_admin", *(r["id"] for r in base["roles"]), *_NEW_ROLES]
    for column in orders["columns"]:
        column["visible_to"] = roles
    depots = {
        "source_id": "sales-pg",
        "domain_id": "finance",
        "schema": "public",
        "table": "depots",
        "columns": [{"name": "id", "data_type": "integer", "visible_to": roles}],
    }
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "domains": [
                {"id": "retail", "description": "business unit"},
                {"id": "sales", "description": "organization", "parent_domain_id": "retail"},
                {"id": "finance", "description": "another business unit"},
            ],
            "tables": [orders, depots],
            "roles": base["roles"]
            + [
                {
                    "id": role,
                    "description": "Test role.",
                    "capabilities": ["query_development"],
                    "domain_access": [domain],
                }
                for role, domain in _NEW_ROLES.items()
            ],
            "rls_rules": [
                {
                    "description": "Test rule.",
                    "domain_id": "retail",
                    "role_id": "bu_filtered",
                    "filter": "region = 'east'",
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
            conn.execute(sa.text("CREATE TABLE public.depots (id integer PRIMARY KEY)"))
            conn.execute(sa.text("INSERT INTO public.depots VALUES (10)"))
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


def _orders_over_sql(boot, role: str) -> tuple[int, str]:
    return _http(boot, role, "POST", "/data/sql", {"sql": "SELECT id FROM sales.orders"})


def _reads_no_order(boot, role: str) -> bool:
    """Whether ``role`` is refused the orders table: an error, never a row."""
    status, body = _orders_over_sql(boot, role)
    if status != 200:
        return True
    answered = json.loads(body)
    return bool(answered.get("errors")) and not (answered.get("data") or {}).get("sql")


@pytest.mark.parametrize("transport", list(_TRANSPORTS))
def test_a_business_unit_grant_reads_a_table_of_a_domain_under_it(server, transport):
    read = _TRANSPORTS[transport]
    assert read(server, "org_admin") == _ALL
    assert read(server, "bu_reader") == _ALL


def test_a_grant_beside_the_business_unit_reads_no_order(server):
    assert _reads_no_order(server, "late_reader"), _orders_over_sql(server, "late_reader")


@pytest.mark.parametrize("transport", list(_TRANSPORTS))
def test_a_row_filter_on_the_business_unit_covers_the_domains_under_it(server, transport):
    assert _TRANSPORTS[transport](server, "bu_filtered") == _EAST


def test_the_tree_is_reported_and_the_stored_grant_is_not_rewritten(server):
    data = _admin(server, "{ domains { id parentDomainId } roles { id domainAccess } }")
    parents = {d["id"]: d["parentDomainId"] for d in data["domains"]}
    assert (parents["sales"], parents["retail"], parents["finance"]) == ("retail", None, None)
    granted = {r["id"]: r["domainAccess"] for r in data["roles"]}
    assert granted["bu_reader"] == ["retail"]  # the grant that was made, not the flattened one


def test_a_domain_with_children_is_refused_deletion_naming_them(server):
    refused = _admin(server, 'mutation { deleteDomain(id: "retail") { success code params } }')[
        "deleteDomain"
    ]
    assert (refused["success"], refused["code"]) == (False, "schema.domain_has_dependents")
    assert ("domain", "sales") in {(d["kind"], d["id"]) for d in refused["params"]["dependents"]}


def test_an_unknown_parent_is_refused_by_name(server):
    refused = _admin(
        server,
        'mutation { createDomain(input: {id: "retail_latam", parentDomainId: "ghost"}) '
        "{ success code message } }",
    )["createDomain"]
    assert (refused["success"], refused["code"]) == (False, "schema.domain_parent_invalid")
    assert refused["message"] == "Parent domain 'ghost' not found"


def _domains_seen(boot, role: str) -> set[str]:
    out = _ok(*_http(boot, role, "POST", "/admin/graphql", {"query": "{ domains { id } }"}))
    assert "errors" not in out, out
    return {d["id"] for d in out["data"]["domains"]}


def test_an_organization_added_under_a_granted_unit_is_reached_without_editing_the_role(server):
    assert "retail_latam" not in _domains_seen(server, "bu_reader")
    added = _admin(
        server,
        'mutation { createDomain(input: {id: "retail_latam", description: "organization", '
        'parentDomainId: "retail"}) { success message } }',
    )["createDomain"]
    assert added["success"], added["message"]
    assert "retail_latam" in _domains_seen(server, "bu_reader")
    assert "retail_latam" not in _domains_seen(server, "late_reader")
    granted = {
        r["id"]: r["domainAccess"] for r in _admin(server, "{ roles { id domainAccess } }")["roles"]
    }
    assert granted["bu_reader"] == ["retail"]
