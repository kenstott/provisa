# Copyright (c) 2026 Kenneth Stott
# Canary: 6c2a9f48-1d73-4e05-b8a6-3f5e0c9d2b71
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2122 e2e: a table takes its descriptions from a real Collibra, and the export then updates
the asset the steward picked.

The Collibra instance holds the customer's own Table asset for sales.orders, made for the run in
a community of the run's own, with a Description, a column with its Description, a tag and a
responsible user. Through the production admin API: the pick list finds it, the offer reads it,
the apply writes what was accepted and links the table, and the export writes the table's
description and its Provisa address onto the picked asset rather than importing a table of its
own. The run's community is removed after it.

Without the instance's address and token the test fails, never skips (REQ-2119).
"""

# Requirements: REQ-2122

from __future__ import annotations

import asyncio
import os
import uuid
from typing import Any

import httpx
import pytest

from provisa.api.metadata_export.collibra import URI_ATTRIBUTE
from tests.integration.metadata_export_server import published_server, save_targets

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_warehouse,
    pytest.mark.asyncio(loop_scope="session"),
]

ORG = "catlink_collibra_e2e"
CATALOG = "collibra"
TABLE_DESCRIPTION = "Every order a customer placed, as the governance office keeps it"
AMOUNT_DESCRIPTION = "The order's total, tax included"
PROVISA_DESCRIPTION = "Orders placed"
_CREDENTIALS = ("PROVISA_E2E_COLLIBRA_ENDPOINT", "PROVISA_E2E_COLLIBRA_TOKEN")


def _credentials() -> dict[str, str]:
    missing = [name for name in _CREDENTIALS if not os.environ.get(name)]
    assert not missing, f"the Collibra link e2e needs {', '.join(missing)}"
    return {name: os.environ[name] for name in _CREDENTIALS}


class _Collibra:
    def __init__(self, creds: dict[str, str]) -> None:
        self._base = creds["PROVISA_E2E_COLLIBRA_ENDPOINT"].rstrip("/")
        self._headers = {"Authorization": f"Bearer {creds['PROVISA_E2E_COLLIBRA_TOKEN']}"}

    async def call(self, method: str, path: str, **kwargs: Any) -> Any:
        async with httpx.AsyncClient(timeout=60, headers=self._headers) as client:
            response = await client.request(method, f"{self._base}/rest/2.0{path}", **kwargs)
        assert response.status_code < 400, response.text
        return response.json() if response.content else None

    async def type_id(self, kind: str, public_id: str) -> str:
        return (await self.call("GET", f"/{kind}/publicId/{public_id}"))["id"]


async def _customer_asset(c: _Collibra, run: str) -> tuple[str, str, str]:
    """The customer's own Table asset and its column, in a community and domain of the run's own;
    the community's, the table's and the column's ids."""
    community = await c.call("POST", "/communities", json={"name": f"provisa_e2e_{run}"})
    domain = await c.call(
        "POST",
        "/domains",
        json={
            "name": "Physical Data Dictionary",
            "communityId": community["id"],
            "typeId": await c.type_id("domainTypes", "PhysicalDataDictionary"),
        },
    )
    table = await c.call(
        "POST",
        "/assets",
        json={
            "name": f"customer_pg>sales>public>orders_{run}",
            "domainId": domain["id"],
            "typeId": await c.type_id("assetTypes", "Table"),
        },
    )
    column = await c.call(
        "POST",
        "/assets",
        json={
            "name": f"customer_pg>sales>public>orders_{run}>amount",
            "domainId": domain["id"],
            "typeId": await c.type_id("assetTypes", "Column"),
        },
    )
    described = await c.type_id("attributeTypes", "Description")
    for asset, text in ((table, TABLE_DESCRIPTION), (column, AMOUNT_DESCRIPTION)):
        await c.call(
            "POST",
            "/attributes",
            json={"assetId": asset["id"], "typeId": described, "value": text},
        )
    await c.call(
        "POST",
        "/relations",
        json={
            "sourceId": column["id"],
            "targetId": table["id"],
            "typeId": await c.type_id("relationTypes", "ColumnIsPartOfTable"),
        },
    )
    await c.call("POST", f"/assets/{table['id']}/tags", json={"tagNames": ["pii"]})
    return community["id"], table["id"], column["id"]


async def test_a_table_takes_its_descriptions_from_collibra_and_the_export_updates_the_pick():
    creds = _credentials()
    c = _Collibra(creds)
    run = uuid.uuid4().hex[:8]
    community, table_ref, _column = await _customer_asset(c, run)
    try:
        async with published_server(ORG) as client:
            await save_targets(
                client,
                [
                    {
                        "id": CATALOG,
                        "provider": "collibra",
                        "endpoint": creds["PROVISA_E2E_COLLIBRA_ENDPOINT"],
                        "auth_mode": "bearer",
                        "token": creds["PROVISA_E2E_COLLIBRA_TOKEN"],
                        "timeout_seconds": 120,
                    }
                ],
            )
            tables = (
                await client.post("/admin/graphql", json={"query": "{ tables { id tableName } }"})
            ).json()["data"]["tables"]
            (table_id,) = [int(t["id"]) for t in tables if t["tableName"] == "orders"]

            for _ in range(30):
                listing = (
                    await client.get(
                        f"/admin/tables/{table_id}/catalog/candidates",
                        params={"search": f"orders_{run}"},
                    )
                ).json()["catalogs"][0]
                assert "error" not in listing, listing
                if any(a["ref"] == table_ref for a in listing["assets"]):
                    break
                await asyncio.sleep(5)
            else:
                raise AssertionError("Collibra never listed the customer's table")

            offer = (
                await client.get(
                    f"/admin/tables/{table_id}/catalog/{CATALOG}/offer",
                    params={"ref": table_ref},
                )
            ).json()
            assert offer["description"]["catalog"] == TABLE_DESCRIPTION
            assert offer["tags"] == [{"name": "pii", "tag": "pii"}]
            by_column = {col["name"]: col["description"] for col in offer["catalog_columns"]}
            assert by_column["amount"] == AMOUNT_DESCRIPTION

            applied = await client.post(
                f"/admin/tables/{table_id}/catalog/{CATALOG}/apply",
                json={"ref": table_ref, "columns": {"amount": "amount"}},
            )
            assert applied.status_code == 200, applied.text

            published = await client.post("/admin/metadata-export/publish")
            assert published.status_code == 200, published.text
            (outcome,) = published.json()["targets"]
            assert outcome["ok"], outcome["errors"]

        attributes = await c.call("GET", "/attributes", params={"assetId": table_ref})
        values = {a["type"]["name"]: a["value"] for a in attributes["results"] if a.get("type")}
        assert values["Description"] == PROVISA_DESCRIPTION
        assert values[URI_ATTRIBUTE].endswith("/tables/orders")
    finally:
        await c.call("DELETE", f"/communities/{community}")
