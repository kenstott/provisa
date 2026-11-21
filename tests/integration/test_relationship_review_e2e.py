# Copyright (c) 2026 Kenneth Stott
# Canary: 4c8d1e73-0a95-4f26-b3e7-9d2f6a1c5b08
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: relationship review after registration on a real server (REQ-2262).

The ``test`` instance: an isolated server (DuckDB engine, its own control plane) over two SQLite
sources. The crm source holds customers and orders, already in the model; the shop source holds
regions and stores, the tables just registered, stores declaring a foreign key to regions. The
review is a run scoped to the shop tables against every table, either way round, asking declared
foreign keys and key names and types, no model: it proposes the declared key, stores.customer_id
to customers (a new table to an existing one, across sources) and orders.store_id to stores (an
existing table to a new one), and nothing between the two existing tables. Every candidate waits
for review; accepting one through the run's Apply creates its relationship.
"""

# Requirements: REQ-2262

from __future__ import annotations

import sqlite3
import time

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_ROLE = "org_admin"
_HEADERS = {"X-Provisa-Role": _ROLE}


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    home = tmp_path_factory.mktemp("relationship_review")
    crm = sqlite3.connect(home / "crm.sqlite")
    crm.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
    crm.executemany("INSERT INTO customers VALUES (?, ?)", [(i, f"c{i}") for i in range(1, 6)])
    crm.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, store_id INTEGER)"
    )
    crm.executemany(
        "INSERT INTO orders VALUES (?, ?, ?)", [(i, 1 + i % 5, 1 + i % 3) for i in range(1, 21)]
    )
    crm.commit()
    crm.close()
    shop = sqlite3.connect(home / "shop.sqlite")
    shop.execute("CREATE TABLE regions (id INTEGER PRIMARY KEY, label TEXT)")
    shop.executemany("INSERT INTO regions VALUES (?, ?)", [(1, "north"), (2, "south")])
    shop.execute(
        "CREATE TABLE stores (id INTEGER PRIMARY KEY, region_id INTEGER REFERENCES regions(id), "
        "customer_id INTEGER)"
    )
    shop.executemany("INSERT INTO stores VALUES (?, ?, ?)", [(1, 1, 1), (2, 2, 2), (3, 1, 3)])
    shop.commit()
    shop.close()
    readers = [_ROLE]

    def column(name: str, data_type: str, pk: bool = False) -> dict:
        return {"name": name, "data_type": data_type, "is_primary_key": pk, "visible_to": readers}

    def table(source: str, domain: str, name: str, columns: list[dict]) -> dict:
        return {
            "source_id": source,
            "domain_id": domain,
            "schema": "main",
            "table": name,
            "columns": columns,
        }

    config = {
        "auth": {"provider": "none"},
        "naming": {"domain_prefix": False, "rules": []},
        "domains": [{"id": "crm", "description": "CRM"}, {"id": "shop", "description": "Shop"}],
        "roles": [],
        "sources": [
            {"id": "crm-db", "type": "sqlite", "path": str(home / "crm.sqlite")},
            {"id": "shop-db", "type": "sqlite", "path": str(home / "shop.sqlite")},
        ],
        "tables": [
            table(
                "crm-db",
                "crm",
                "customers",
                [column("id", "integer", True), column("name", "varchar")],
            ),
            table(
                "crm-db",
                "crm",
                "orders",
                [
                    column("id", "integer", True),
                    column("customer_id", "integer"),
                    column("store_id", "integer"),
                ],
            ),
            table(
                "shop-db",
                "shop",
                "regions",
                [column("id", "integer", True), column("label", "varchar")],
            ),
            table(
                "shop-db",
                "shop",
                "stores",
                [
                    column("id", "integer", True),
                    column("region_id", "integer"),
                    column("customer_id", "integer"),
                ],
            ),
        ],
        "relationships": [],
    }
    path = home / "config.yaml"
    path.write_text(yaml.safe_dump(config))
    srv = IsolatedServer(
        "relationship_review", engine="duckdb", config=str(path), control_plane="own"
    )
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


def _ok(resp: httpx.Response) -> dict:
    assert resp.status_code == 200, resp.text
    return resp.json()


def _get(srv, path: str) -> dict:
    return _ok(httpx.get(f"{srv.base_url}{path}", headers=_HEADERS, timeout=60))


def _post(srv, path: str, body: dict | None = None) -> dict:
    return _ok(httpx.post(f"{srv.base_url}{path}", json=body, headers=_HEADERS, timeout=120))


def _table_ids(srv) -> dict[str, int]:
    body = _ok(
        httpx.post(
            f"{srv.base_url}/admin/graphql",
            json={"query": "{ tables { id tableName } }"},
            headers=_HEADERS,
            timeout=60,
        )
    )
    assert "errors" not in body, body
    return {t["tableName"]: int(t["id"]) for t in body["data"]["tables"]}


def _relationship_count(srv) -> int:
    body = _ok(
        httpx.post(
            f"{srv.base_url}/admin/graphql",
            json={"query": "{ relationships { id } }"},
            headers=_HEADERS,
            timeout=60,
        )
    )
    assert "errors" not in body, body
    return len(body["data"]["relationships"])


def test_the_review_after_registration_proposes_every_candidate_for_review(server):
    ids = _table_ids(server)
    scope = {
        "kinds": ["foreign_keys", "key_names"],
        "source_domains": ["shop"],
        "source_tables": [ids["regions"], ids["stores"]],
        "target_same_as_source": False,
        "target_domains": ["crm", "shop"],
        "target_tables": None,
        "either_way": True,
        "threshold": 0.7,
        "samples_per_column": 5,
    }
    run = _post(server, "/admin/discover/runs", scope)["run"]
    deadline = time.monotonic() + 180
    while run["status"] == "running":
        assert time.monotonic() < deadline, run
        time.sleep(1)
        run = _get(server, f"/admin/discover/runs/{run['id']}")["run"]
    assert run["status"] == "done", run
    assert run["model"] is None
    report = run["report"]
    assert report["foreign_keys"] == {"found": 1, "suggested": 1}
    # stores.region_id is named for regions too: one pair, shown once, as the declared key.
    assert report["key_names"] == {"found": 3, "suggested": 2}

    listed = _get(server, f"/admin/discover/runs/{run['id']}/proposals")["proposals"]
    shown = {
        (p["source_table_id"], p["source_column"], p["target_table_id"], p["target_column"]): p
        for p in listed
        if p["reason"] is None
    }
    assert set(shown) == {
        (ids["stores"], "region_id", ids["regions"], "id"),
        (ids["stores"], "customer_id", ids["customers"], "id"),
        (ids["orders"], "store_id", ids["stores"], "id"),
    }
    fk = shown[(ids["stores"], "region_id", ids["regions"], "id")]
    assert fk["kind"] == "foreign_keys"
    named = shown[(ids["orders"], "store_id", ids["stores"], "id")]
    assert named["kind"] == "key_names"
    assert named["cardinality"] == "many-to-one"
    assert named["reasoning"] == "name and type match the primary key stores.id"
    # Every candidate waits for review, the declared key included: none is a relationship yet.
    assert all(p["decidable"] for p in shown.values())
    before = _relationship_count(server)

    applied = _post(
        server,
        f"/admin/discover/runs/{run['id']}/apply",
        {"accept": [{"id": named["id"], "name": "order-store"}], "not_again": []},
    )
    assert applied["results"] == [
        {"id": named["id"], "action": "accept", "ok": True, "relationship_id": "order-store"}
    ]
    assert _relationship_count(server) == before + 1
