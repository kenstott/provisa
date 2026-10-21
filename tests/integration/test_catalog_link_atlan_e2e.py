# Copyright (c) 2026 Kenneth Stott
# Canary: 9a5e3c16-7b42-4d81-a0f9-c2d8e6b4a357
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2122 e2e: a table takes its descriptions from a real Atlan tenant, and the export then
updates the asset the steward picked in place.

The tenant holds the customer's own Table for sales.orders, made for the run (provisa_e2e_*) with
a description and a described column. Through the production admin API: the pick list finds it
in Atlan's index search, the offer reads it, the apply writes what was accepted and links the
table, and the export writes the table's description into the picked table's user description
and its Provisa address into its custom attributes, publishing no table of its own for it. The
run's table is removed after it.

Without the tenant's address and API token the test fails, never skips (REQ-2119).
"""

# Requirements: REQ-2122

from __future__ import annotations

import asyncio
import os
import uuid
from typing import Any

import httpx
import pytest

from tests.integration.metadata_export_server import published_server, save_targets

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_warehouse,
    pytest.mark.asyncio(loop_scope="session"),
]

ORG = "catlink_atlan_e2e"
CATALOG = "atlan"
TABLE_DESCRIPTION = "Every order a customer placed, as the data team keeps it"
AMOUNT_DESCRIPTION = "The order's total, tax included"
PROVISA_DESCRIPTION = "Orders placed"
_CREDENTIALS = ("PROVISA_E2E_ATLAN_ENDPOINT", "PROVISA_E2E_ATLAN_API_TOKEN")


def _credentials() -> dict[str, str]:
    missing = [name for name in _CREDENTIALS if not os.environ.get(name)]
    assert not missing, f"the Atlan link e2e needs {', '.join(missing)}"
    return {name: os.environ[name] for name in _CREDENTIALS}


async def _atlan(creds: dict[str, str], method: str, path: str, **kwargs: Any) -> Any:
    headers = {"Authorization": f"Bearer {creds['PROVISA_E2E_ATLAN_API_TOKEN']}"}
    async with httpx.AsyncClient(timeout=60, headers=headers) as client:
        response = await client.request(
            method, f"{creds['PROVISA_E2E_ATLAN_ENDPOINT'].rstrip('/')}/api/meta{path}", **kwargs
        )
    assert response.status_code < 400, response.text
    return response.json() if response.content else None


async def _customer_table(creds: dict[str, str], run: str) -> str:
    """The customer's own Table and its column, under a connection-shaped qualified name of the
    run's own; the table's guid."""
    prefix = f"default/postgres/provisa_e2e_{run}/sales/public"
    created = await _atlan(
        creds,
        "POST",
        "/entity/bulk",
        json={
            "entities": [
                {
                    "typeName": "Table",
                    "guid": "-1",
                    "attributes": {
                        "qualifiedName": f"{prefix}/orders",
                        "name": f"orders_{run}",
                        "description": TABLE_DESCRIPTION,
                        "connectionQualifiedName": f"default/postgres/provisa_e2e_{run}",
                    },
                },
                {
                    "typeName": "Column",
                    "guid": "-2",
                    "attributes": {
                        "qualifiedName": f"{prefix}/orders/amount",
                        "name": "amount",
                        "description": AMOUNT_DESCRIPTION,
                        "order": 1,
                        "table": {"guid": "-1", "typeName": "Table"},
                    },
                },
            ]
        },
    )
    return created["guidAssignments"]["-1"]


async def test_a_table_takes_its_descriptions_from_atlan_and_the_export_updates_the_pick():
    creds = _credentials()
    run = uuid.uuid4().hex[:8]
    guid = await _customer_table(creds, run)
    try:
        async with published_server(ORG) as client:
            await save_targets(
                client,
                [
                    {
                        "id": CATALOG,
                        "provider": "atlan",
                        "endpoint": creds["PROVISA_E2E_ATLAN_ENDPOINT"],
                        "auth_mode": "bearer",
                        "token": creds["PROVISA_E2E_ATLAN_API_TOKEN"],
                        "timeout_seconds": 120,
                    }
                ],
            )
            tables = (
                await client.post("/admin/graphql", json={"query": "{ tables { id tableName } }"})
            ).json()["data"]["tables"]
            (table_id,) = [int(t["id"]) for t in tables if t["tableName"] == "orders"]

            for _ in range(60):
                listing = (
                    await client.get(
                        f"/admin/tables/{table_id}/catalog/candidates",
                        params={"search": f"orders_{run}"},
                    )
                ).json()["catalogs"][0]
                assert "error" not in listing, listing
                if any(a["ref"] == guid for a in listing["assets"]):
                    break
                await asyncio.sleep(5)
            else:
                raise AssertionError("Atlan's search never listed the customer's table")

            offer = (
                await client.get(
                    f"/admin/tables/{table_id}/catalog/{CATALOG}/offer", params={"ref": guid}
                )
            ).json()
            assert offer["description"]["catalog"] == TABLE_DESCRIPTION
            by_column = {c["name"]: c["description"] for c in offer["catalog_columns"]}
            assert by_column["amount"] == AMOUNT_DESCRIPTION

            applied = await client.post(
                f"/admin/tables/{table_id}/catalog/{CATALOG}/apply",
                json={"ref": guid, "columns": {"amount": "amount"}},
            )
            assert applied.status_code == 200, applied.text

            published = await client.post("/admin/metadata-export/publish")
            assert published.status_code == 200, published.text

        picked = (await _atlan(creds, "GET", f"/entity/guid/{guid}"))["entity"]
        assert picked["attributes"]["userDescription"] == PROVISA_DESCRIPTION
        assert picked["customAttributes"]["provisaUri"].endswith("/tables/orders")
    finally:
        await _atlan(creds, "DELETE", f"/entity/guid/{guid}")
