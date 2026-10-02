# Copyright (c) 2026 Kenneth Stott
# Canary: 830a2602-7dcd-42cc-9506-65474735936c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A domain is deleted only when nothing refers to it, on each control plane (REQ-1917).

A served instance on a PostgreSQL control plane and one on a SQLite control plane are each asked
to delete a domain that holds a registered table, and a domain created for the test that nothing
refers to. The answers and the rows left behind are the same on both: PostgreSQL does not
cascade the table away, and SQLite does not leave it pointing at nothing. Everything here lands
on the TEST instance: dedicated server processes on their own ports with their own control
planes.
"""

# Requirements: REQ-1917, REQ-1918, REQ-1919

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import httpx
import pytest

from tests.integration.isolated_server import IsolatedServer, drop_org_schema

pytestmark = [pytest.mark.integration]

_PG_ORG = "domain_delete_pg"


@pytest.fixture(scope="module", params=["postgres", "sqlite"])
def served(request):
    """The server, and a domain of its config that holds at least one registered table."""
    if request.param == "postgres":
        srv = IsolatedServer(_PG_ORG, config="tests/fixtures/sample_config.yaml")
        srv.start()
        try:
            yield srv, "sales-analytics"
        finally:
            srv.stop_process()
            asyncio.run(drop_org_schema(_PG_ORG))
        return
    pytest.importorskip("duckdb")
    store_dir = tempfile.TemporaryDirectory()
    srv = IsolatedServer(
        "domain_delete_sqlite",
        engine="duckdb",
        config="tests/fixtures/duckdb_sqlite_config.yaml",
        control_plane="sqlite",
        materialize_store_url=f"duckdb:///{Path(store_dir.name) / 'materialize.duckdb'}",
    )
    srv.start()
    try:
        yield srv, "shelter"
    finally:
        srv.stop_process()
        store_dir.cleanup()


def _gql(srv: IsolatedServer, query: str) -> dict:
    resp = httpx.post(
        f"{srv.base_url}/admin/graphql", json={"query": query}, timeout=srv.request_timeout + 15
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "errors" not in body, body
    return body["data"]


def _tables_in(srv: IsolatedServer, domain_id: str) -> set[int]:
    listed = _gql(srv, "{ tables { id domainId } }")["tables"]
    return {t["id"] for t in listed if t["domainId"] == domain_id}


def _domain_ids(srv: IsolatedServer) -> set[str]:
    return {d["id"] for d in _gql(srv, "{ domains { id } }")["domains"]}


def _delete(srv: IsolatedServer, domain_id: str) -> dict:
    return _gql(
        srv, f'mutation {{ deleteDomain(id: "{domain_id}") {{ success code message params }} }}'
    )["deleteDomain"]


def test_a_domain_holding_a_table_is_refused_naming_it_and_nothing_is_removed(served):
    srv, domain_id = served
    held = _tables_in(srv, domain_id)
    assert held, f"the fixture registers no table in {domain_id!r}"

    answer = _delete(srv, domain_id)
    assert (answer["success"], answer["code"]) == (False, "schema.domain_has_dependents"), answer
    assert answer["params"]["domain"] == domain_id
    named = {(d["kind"], d["id"]) for d in answer["params"]["dependents"]}
    assert {("table", table_id) for table_id in held} <= named, answer

    assert domain_id in _domain_ids(srv)
    assert _tables_in(srv, domain_id) == held


def test_a_domain_nothing_refers_to_is_deleted(served):
    srv, _ = served
    made = _gql(
        srv,
        'mutation { createDomain(input: {id: "scratch-1917", description: "x"}) { success } }',
    )["createDomain"]
    assert made["success"] is True
    assert "scratch-1917" in _domain_ids(srv)

    answer = _delete(srv, "scratch-1917")
    assert (answer["success"], answer["code"]) == (True, "schema.domain_deleted"), answer
    assert "scratch-1917" not in _domain_ids(srv)
    assert _delete(srv, "scratch-1917")["code"] == "schema.domain_not_found"
