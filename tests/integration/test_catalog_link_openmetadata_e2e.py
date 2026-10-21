# Copyright (c) 2026 Kenneth Stott
# Canary: 9f3b6d21-4e80-4a57-b1c9-0d7e2a5f8c36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2122 e2e: a table takes its descriptions from a real OpenMetadata server, and the export
then updates the asset the steward picked.

OpenMetadata already holds the customer's own asset for the physical table behind sales.orders,
with a description, column descriptions, an owner, a tag and a glossary term, as its ingestion
would have left it. Through the production admin API, as the table editor calls it: the pick list
preselects that asset, the offer lays its metadata beside the table's own, the apply writes what
was accepted and links the table, the export publishes onto the picked asset instead of creating
another, and a table locked to the catalog keeps the catalog's description through an export.
Every assertion reads back from the catalog's API or Provisa's.
"""

# Requirements: REQ-2122

from __future__ import annotations

import asyncio
import base64
import os
from typing import Any

import httpx
import pytest

from tests.integration.metadata_export_server import (
    SCHEMA,
    SOURCE,
    published_server,
    save_targets,
)

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.requires_openmetadata,
    pytest.mark.asyncio(loop_scope="session"),
]

ORG = "catlink_openmetadata_e2e"
CATALOG = "openmetadata"
SERVICE = "customer_pg"
CATALOG_DESCRIPTION = "Every order a customer placed, one row per order"
AMOUNT_DESCRIPTION = "The order's total, tax included"
PROVISA_DESCRIPTION = "Orders placed"


def _base_url() -> str:
    return f"http://localhost:{os.environ['OPENMETADATA_PORT']}"


async def _token() -> str:
    """The bootstrap admin's JWT. OpenMetadata's login takes the password base64-encoded: the
    transport encoding the server requires, for the isolated test stack's own container."""
    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.post(
            f"{_base_url()}/api/v1/users/login",
            json={
                "email": "admin@open-metadata.org",
                "password": base64.b64encode(b"admin").decode(),
            },
        )
    response.raise_for_status()
    return response.json()["accessToken"]


class _OpenMetadata:
    def __init__(self, token: str) -> None:
        self._headers = {"Authorization": f"Bearer {token}"}

    async def call(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        async with httpx.AsyncClient(timeout=60) as client:
            return await client.request(
                method, f"{_base_url()}{path}", headers=self._headers, **kwargs
            )

    async def put(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        response = await self.call("PUT", path, json=body)
        assert response.status_code < 400, response.text
        return response.json()

    async def get(self, path: str, **params: Any) -> dict[str, Any]:
        response = await self.call("GET", path, params=params or None)
        assert response.status_code < 400, response.text
        return response.json()


async def _customer_asset(om: _OpenMetadata, database: str) -> dict[str, Any]:
    """The customer's own asset for the physical table behind sales.orders, as its ingestion
    leaves it: a service of its own, the real database and schema, descriptions, an owner, a tag
    of its own classification and a glossary term."""
    await om.put("/api/v1/services/databaseServices", {"name": SERVICE, "serviceType": "Postgres"})
    await om.put("/api/v1/databases", {"name": database, "service": SERVICE})
    await om.put("/api/v1/databaseSchemas", {"name": SCHEMA, "database": f"{SERVICE}.{database}"})
    await om.put("/api/v1/classifications", {"name": "Governance", "description": "Customer tags"})
    await om.put(
        "/api/v1/tags",
        {"name": "pii", "classification": "Governance", "description": "Personal data"},
    )
    await om.put("/api/v1/glossaries", {"name": "Business", "description": "Customer glossary"})
    await om.put(
        "/api/v1/glossaryTerms",
        {"name": "Patron", "glossary": "Business", "description": "Nobody in Provisa's glossary"},
    )
    admin = await om.get("/api/v1/users/name/admin")
    return await om.put(
        "/api/v1/tables",
        {
            "name": "orders",
            "databaseSchema": f"{SERVICE}.{database}.{SCHEMA}",
            "description": CATALOG_DESCRIPTION,
            "owners": [{"id": admin["id"], "type": "user"}],
            "columns": [
                {"name": "id", "dataType": "INT"},
                {"name": "customer_id", "dataType": "INT"},
                {"name": "AMOUNT", "dataType": "NUMERIC", "description": AMOUNT_DESCRIPTION},
                {
                    "name": "ssn",
                    "dataType": "VARCHAR",
                    "dataLength": 11,
                    "tags": [{"tagFQN": "Governance.pii", "source": "Classification"}],
                },
                {
                    "name": "region",
                    "dataType": "VARCHAR",
                    "dataLength": 8,
                    "tags": [{"tagFQN": "Business.Patron", "source": "Glossary"}],
                },
            ],
        },
    )


async def _om_patch(om: _OpenMetadata, ref: str, path: str, value: str) -> None:
    patched = await om.call(
        "PATCH",
        f"/api/v1/tables/{ref}",
        json=[{"op": "replace", "path": path, "value": value}],
        headers={**om._headers, "Content-Type": "application/json-patch+json"},
    )
    assert patched.status_code < 400, patched.text


async def _orders_id(client: httpx.AsyncClient) -> int:
    answer = await client.post("/admin/graphql", json={"query": "{ tables { id tableName } }"})
    assert answer.status_code == 200, answer.text
    (orders,) = [t for t in answer.json()["data"]["tables"] if t["tableName"] == "orders"]
    return int(orders["id"])


async def _orders(client: httpx.AsyncClient) -> dict[str, Any]:
    answer = await client.post(
        "/admin/graphql",
        json={"query": "{ tables { tableName description columns { columnName description } } }"},
    )
    assert answer.status_code == 200, answer.text
    (orders,) = [t for t in answer.json()["data"]["tables"] if t["tableName"] == "orders"]
    return orders


async def _candidates(client: httpx.AsyncClient, table_id: int, ref: str) -> dict[str, Any]:
    """The catalog's pick list, once OpenMetadata's search index holds the asset (it indexes a
    new entity asynchronously)."""
    for _ in range(60):
        answer = await client.get(f"/admin/tables/{table_id}/catalog/candidates")
        assert answer.status_code == 200, answer.text
        (listing,) = answer.json()["catalogs"]
        assert "error" not in listing, listing
        if any(a["ref"] == ref for a in listing["assets"]):
            return listing
        await asyncio.sleep(2)
    raise AssertionError(f"OpenMetadata's search never listed the asset {ref}")


async def _publish(client: httpx.AsyncClient) -> dict[str, Any]:
    published = await client.post("/admin/metadata-export/publish")
    assert published.status_code == 200, published.text
    (outcome,) = published.json()["targets"]
    assert outcome["ok"], outcome["errors"]
    return outcome


async def test_a_table_takes_its_descriptions_from_the_catalog_and_the_export_updates_the_pick():
    om = _OpenMetadata(await _token())
    database = os.environ["PG_DATABASE"]
    customer = await _customer_asset(om, database)
    async with published_server(ORG) as client:
        await save_targets(
            client,
            [
                {
                    "id": CATALOG,
                    "provider": "openmetadata",
                    "endpoint": _base_url(),
                    "auth_mode": "bearer",
                    "token": om._headers["Authorization"].removeprefix("Bearer "),
                    "timeout_seconds": 60,
                }
            ],
        )
        table_id = await _orders_id(client)

        # The pick list preselects the customer's asset for the same physical table.
        listing = await _candidates(client, table_id, customer["id"])
        assert listing["preselected"] == customer["id"]
        assert listing["linked"] is None

        # The offer lays the asset's metadata beside the table's own, each with its catalog.
        answer = await client.get(
            f"/admin/tables/{table_id}/catalog/{CATALOG}/offer", params={"ref": customer["id"]}
        )
        assert answer.status_code == 200, answer.text
        offer = answer.json()
        assert offer["description"] == {
            "current": PROVISA_DESCRIPTION,
            "catalog": CATALOG_DESCRIPTION,
        }
        matched = {c["column"]: c["catalog_column"] for c in offer["columns"]}
        assert matched["amount"] == "AMOUNT"  # matched by name, case aside
        assert offer["owners"] == ["admin"]
        by_column = {c["name"]: c for c in offer["catalog_columns"]}
        assert by_column["ssn"]["tags"] == [{"name": "Governance.pii", "tag": "pii"}]
        # A term Provisa's glossary does not have is listed and nothing is created.
        assert by_column["region"]["terms"] == [{"name": "Business.Patron", "term": None}]

        # Accept the column description and the tag, not the table's description.
        applied = await client.post(
            f"/admin/tables/{table_id}/catalog/{CATALOG}/apply",
            json={
                "ref": customer["id"],
                "description": False,
                "columns": {"amount": "AMOUNT"},
                "tags": [{"column": "ssn", "tag": "pii"}],
            },
        )
        assert applied.status_code == 200, applied.text
        orders = await _orders(client)
        assert orders["description"] == PROVISA_DESCRIPTION
        columns = {c["columnName"]: c["description"] for c in orders["columns"]}
        assert columns["amount"] == AMOUNT_DESCRIPTION
        held = await client.get(f"/admin/tables/{table_id}/catalog")
        (state,) = held.json()["catalogs"]
        assert state["link"]["ref"] == customer["id"]
        assert state["imported_at"] is not None
        assert state["locked"] is False

        # The export publishes onto the picked asset: its description becomes Provisa's, it names
        # the table's Provisa address, and no entity of the export's own is made for the table.
        await client.put(
            "/admin/metadata-export", json={"enabled": True, "targets": [{"catalog": CATALOG}]}
        )
        await _publish(client)
        picked = await om.get(f"/api/v1/tables/{customer['id']}", fields="columns,tags")
        assert picked["description"] == PROVISA_DESCRIPTION
        assert picked["sourceUrl"].startswith("provisa://")
        assert picked["sourceUrl"].endswith("/tables/orders")
        own = await om.call("GET", f"/api/v1/tables/name/{SOURCE}.default.{SCHEMA}.orders")
        assert own.status_code == 404, own.text

        # Locked to the catalog, the table keeps the catalog's description through an export.
        locked = await client.put(
            f"/admin/tables/{table_id}/catalog/lock", json={"catalog": CATALOG}
        )
        assert locked.status_code == 200, locked.text
        await _om_patch(om, customer["id"], "/description", "Edited in the catalog")
        await _publish(client)
        kept = await om.get(f"/api/v1/tables/{customer['id']}")
        assert kept["description"] == "Edited in the catalog"

        # Refresh from catalog reads the linked asset as it is now.
        refreshed = await client.get(f"/admin/tables/{table_id}/catalog/{CATALOG}/offer")
        assert refreshed.status_code == 200, refreshed.text
        assert refreshed.json()["description"]["catalog"] == "Edited in the catalog"
        assert refreshed.json()["locked"] is True
        applied = await client.post(
            f"/admin/tables/{table_id}/catalog/{CATALOG}/apply",
            json={"ref": customer["id"], "description": True},
        )
        assert applied.status_code == 200, applied.text
        assert (await _orders(client))["description"] == "Edited in the catalog"

        # The catalog's Import lists what changed for each linked table, and lists the tables
        # not linked to it without guessing an asset for them.
        await _om_patch(om, customer["id"], "/columns/2/description", "Total, tax and shipping")
        review = await client.get(f"/admin/catalogs/{CATALOG}/import")
        assert review.status_code == 200, review.text
        (linked,) = review.json()["linked"]
        assert linked["table_id"] == table_id
        assert [(c["field"], c["column"], c["catalog"]) for c in linked["changes"]] == [
            ("column", "amount", "Total, tax and shipping")
        ]
        assert {t["table"] for t in review.json()["not_linked"]} == {"customers", "order_totals"}
        accepted = await client.post(
            f"/admin/catalogs/{CATALOG}/import", json={"table_ids": [table_id]}
        )
        assert accepted.status_code == 200, accepted.text
        columns = {c["columnName"]: c["description"] for c in (await _orders(client))["columns"]}
        assert columns["amount"] == "Total, tax and shipping"

        # The scheduled import, on the existing scheduler, applies the catalog's metadata to the
        # table locked to it without review.
        held = (await client.get("/admin/catalogs")).json()["catalogs"]
        scheduled = [
            {
                **{k: v for k, v in c.items() if not k.endswith("_set")},
                "import_cron": "* * * * *",
            }
            for c in held
        ]
        saved = await client.put("/admin/catalogs", json={"catalogs": scheduled})
        assert saved.status_code == 200, saved.text
        await _om_patch(om, customer["id"], "/description", "Changed on the schedule")
        for _ in range(45):
            if (await _orders(client))["description"] == "Changed on the schedule":
                break
            await asyncio.sleep(4)
        else:
            raise AssertionError("the scheduled import never applied the catalog's description")


async def test_a_catalog_of_another_domain_is_not_the_tables():
    om = _OpenMetadata(await _token())
    async with published_server(ORG) as client:
        connected = await client.put(
            "/admin/catalogs",
            json={
                "catalogs": [
                    {
                        "id": "lab_catalog",
                        "provider": "openmetadata",
                        "endpoint": _base_url(),
                        "auth_mode": "bearer",
                        "token": om._headers["Authorization"].removeprefix("Bearer "),
                        "domains": ["lab"],
                    }
                ]
            },
        )
        assert connected.status_code == 200, connected.text
        table_id = await _orders_id(client)
        held = await client.get(f"/admin/tables/{table_id}/catalog")
        assert held.json()["catalogs"] == []
        refused = await client.get(
            f"/admin/tables/{table_id}/catalog/lab_catalog/offer", params={"ref": "x"}
        )
        assert refused.status_code == 400
        assert refused.json()["code"] == "catalog_link.not_the_domains"
