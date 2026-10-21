# Copyright (c) 2026 Kenneth Stott
# Canary: 7f1d4b38-2e96-4c50-a8e7-b3c6d9a0f215
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2122 e2e: a table takes its descriptions from a real Apache Atlas, and the export then
updates the entity the steward picked in place.

Atlas already holds the customer's own rdbms_table for the physical table behind sales.orders,
with a description, column descriptions, an owner and a classification. Through the production
admin API: the pick list preselects it, the apply writes what was accepted and links the table,
and the export writes the table's description into the entity's user description and its Provisa
address into its custom attributes, publishing no Provisa table entity for it; the view derived
from it loses its lineage to it, reported by name. A table locked to Atlas keeps Atlas's
description through an export. Every assertion reads back from Atlas or Provisa.
"""

# Requirements: REQ-2122

from __future__ import annotations

import asyncio
import base64
import os
from typing import Any

import httpx
import pytest

from provisa.api.metadata_export.atlas import CLUSTER
from tests.integration.metadata_export_server import (
    SCHEMA,
    SOURCE,
    published_server,
    save_targets,
)

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.requires_atlas,
    pytest.mark.asyncio(loop_scope="session"),
]

ORG = "catlink_atlas_e2e"
CATALOG = "atlas"
INSTANCE = "customer_pg"
CATALOG_DESCRIPTION = "Every order a customer placed, one row per order"
AMOUNT_DESCRIPTION = "The order's total, tax included"
PROVISA_DESCRIPTION = "Orders placed"


def _base_url() -> str:
    return f"http://localhost:{os.environ['ATLAS_PORT']}"


# The isolated test stack's Atlas container keeps its stock admin account.
_AUTH = {"Authorization": "Basic " + base64.b64encode(b"admin:admin").decode()}


async def _atlas(method: str, path: str, **kwargs: Any) -> httpx.Response:
    async with httpx.AsyncClient(timeout=60, headers=_AUTH) as client:
        return await client.request(method, f"{_base_url()}/api/atlas/v2{path}", **kwargs)


async def _ok(method: str, path: str, **kwargs: Any) -> dict[str, Any]:
    response = await _atlas(method, path, **kwargs)
    assert response.status_code < 400, response.text
    return response.json() if response.content else {}


async def _customer_table(database: str) -> str:
    """The customer's own rdbms_table, as an Atlas hook leaves it: instance, database, table and
    columns, a description, an owner and a classification of the customer's own."""
    await _atlas(
        "POST",
        "/types/typedefs",
        json={"classificationDefs": [{"name": "pii", "attributeDefs": []}]},
    )
    qn = f"{INSTANCE}.{database}.{SCHEMA}.orders@{INSTANCE}"
    columns = [
        ("id", ""),
        ("amount", AMOUNT_DESCRIPTION),
        ("ssn", ""),
    ]
    created = await _ok(
        "POST",
        "/entity/bulk",
        json={
            "entities": [
                {
                    "typeName": "rdbms_instance",
                    "guid": "-1",
                    "attributes": {
                        "qualifiedName": f"{INSTANCE}@{INSTANCE}",
                        "name": INSTANCE,
                        "rdbms_type": "postgresql",
                        "platform": "linux",
                        "hostname": "db.customer.example",
                        "port": "5432",
                    },
                },
                {
                    "typeName": "rdbms_db",
                    "guid": "-2",
                    "attributes": {
                        "qualifiedName": f"{INSTANCE}.{database}@{INSTANCE}",
                        "name": database,
                        "instance": {"guid": "-1", "typeName": "rdbms_instance"},
                    },
                },
                {
                    "typeName": "rdbms_table",
                    "guid": "-3",
                    "attributes": {
                        "qualifiedName": qn,
                        "name": "orders",
                        "description": CATALOG_DESCRIPTION,
                        "owner": "ann",
                        "db": {"guid": "-2", "typeName": "rdbms_db"},
                    },
                },
                *[
                    {
                        "typeName": "rdbms_column",
                        "guid": f"-{4 + i}",
                        "attributes": {
                            "qualifiedName": f"{INSTANCE}.{database}.{SCHEMA}.orders.{name}@{INSTANCE}",
                            "name": name,
                            "description": description,
                            "data_type": "text",
                            "table": {"guid": "-3", "typeName": "rdbms_table"},
                        },
                        **({"classifications": [{"typeName": "pii"}]} if name == "ssn" else {}),
                    }
                    for i, (name, description) in enumerate(columns)
                ],
            ]
        },
    )
    return created["guidAssignments"]["-3"]


async def _orders(client: httpx.AsyncClient) -> dict[str, Any]:
    answer = await client.post(
        "/admin/graphql",
        json={
            "query": "{ tables { id tableName description columns { columnName description } } }"
        },
    )
    assert answer.status_code == 200, answer.text
    (orders,) = [t for t in answer.json()["data"]["tables"] if t["tableName"] == "orders"]
    return orders


async def _candidates(client: httpx.AsyncClient, table_id: int, guid: str) -> dict[str, Any]:
    """The pick list, once Atlas's search index holds the entity (it indexes asynchronously)."""
    for _ in range(60):
        answer = await client.get(f"/admin/tables/{table_id}/catalog/candidates")
        assert answer.status_code == 200, answer.text
        (listing,) = answer.json()["catalogs"]
        assert "error" not in listing, listing
        if any(a["ref"] == guid for a in listing["assets"]):
            return listing
        await asyncio.sleep(2)
    raise AssertionError(f"Atlas's search never listed {guid}")


async def _publish(client: httpx.AsyncClient) -> dict[str, Any]:
    published = await client.post("/admin/metadata-export/publish")
    assert published.status_code == 200, published.text
    (outcome,) = published.json()["targets"]
    return outcome


async def test_a_table_takes_its_descriptions_from_atlas_and_the_export_updates_the_pick():
    guid = await _customer_table(os.environ["PG_DATABASE"])
    async with published_server(ORG) as client:
        await save_targets(
            client,
            [
                {
                    "id": CATALOG,
                    "provider": "atlas",
                    "endpoint": _base_url(),
                    "auth_mode": "basic",
                    "username": "admin",
                    "token": "admin",
                    "timeout_seconds": 60,
                }
            ],
        )
        table_id = int((await _orders(client))["id"])

        listing = await _candidates(client, table_id, guid)
        assert listing["preselected"] == guid

        offer = (
            await client.get(
                f"/admin/tables/{table_id}/catalog/{CATALOG}/offer", params={"ref": guid}
            )
        ).json()
        assert offer["description"]["catalog"] == CATALOG_DESCRIPTION
        assert offer["owners"] == ["ann"]
        by_column = {c["name"]: c for c in offer["catalog_columns"]}
        assert by_column["amount"]["description"] == AMOUNT_DESCRIPTION
        assert by_column["ssn"]["tags"] == [{"name": "pii", "tag": "pii"}]

        applied = await client.post(
            f"/admin/tables/{table_id}/catalog/{CATALOG}/apply",
            json={"ref": guid, "columns": {"amount": "amount"}},
        )
        assert applied.status_code == 200, applied.text
        columns = {c["columnName"]: c["description"] for c in (await _orders(client))["columns"]}
        assert columns["amount"] == AMOUNT_DESCRIPTION

        # The export updates the picked entity in place and publishes no table of its own for
        # it; the view's lineage to it cannot be expressed and is reported by name.
        await client.put(
            "/admin/metadata-export", json={"enabled": True, "targets": [{"catalog": CATALOG}]}
        )
        outcome = await _publish(client)
        assert any("picked asset" in e["message"] for e in outcome["errors"]), outcome
        picked = (await _ok("GET", f"/entity/guid/{guid}"))["entity"]
        assert picked["attributes"]["userDescription"] == PROVISA_DESCRIPTION
        assert picked["attributes"]["description"] == CATALOG_DESCRIPTION
        assert picked["customAttributes"]["provisaUri"].endswith("/tables/orders")
        own = await _atlas(
            "GET",
            "/entity/uniqueAttribute/type/provisa_table",
            params={"attr:qualifiedName": f"{SOURCE}.{SCHEMA}.orders@{CLUSTER}"},
        )
        assert own.status_code == 404, own.text

        # Locked to Atlas, the table keeps Atlas's description through an export.
        locked = await client.put(
            f"/admin/tables/{table_id}/catalog/lock", json={"catalog": CATALOG}
        )
        assert locked.status_code == 200, locked.text
        await _ok(
            "PUT",
            f"/entity/guid/{guid}",
            params={"name": "userDescription"},
            json="Edited in Atlas",
        )
        await _publish(client)
        kept = (await _ok("GET", f"/entity/guid/{guid}"))["entity"]
        assert kept["attributes"]["userDescription"] == "Edited in Atlas"
        refreshed = (await client.get(f"/admin/tables/{table_id}/catalog/{CATALOG}/offer")).json()
        assert refreshed["description"]["catalog"] == "Edited in Atlas"
