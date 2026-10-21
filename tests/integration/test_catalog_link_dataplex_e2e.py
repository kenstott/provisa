# Copyright (c) 2026 Kenneth Stott
# Canary: 2c9e5a71-4b38-4f06-9d12-e8a7b3c0f594
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2122 e2e: a table takes its descriptions from the Dataplex Universal Catalog, and the data
product export then names the BigQuery table the steward picked as the member's asset.

The project holds the customer's own BigQuery table for sales.orders, created for the run
(provisa_e2e_*) with a description and column descriptions; Dataplex catalogs it on its own.
Through the production admin API, the pick list finds it in Dataplex's search, the offer reads its
entry, the apply writes what was accepted and links the table, and the next publish lists the
picked table as the product's asset in place of the member's export view. Everything the run
made is deleted after it.

Without the project's credentials the test fails, never skips (REQ-2119).
"""

# Requirements: REQ-2122

from __future__ import annotations

import asyncio
import json
import os
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest

from provisa.federation.replica_address import export_schema, replica_schema
from tests.integration.metadata_export_server import (
    DOMAIN,
    PRODUCT_FIELDS,
    published_server,
    save_targets,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_warehouse,
    pytest.mark.asyncio(loop_scope="session"),
]

ORG = "catlink_dataplex_e2e"
OWNER = "provisa-e2e-owner"
API = "https://dataplex.googleapis.com/v1"
CATALOG = "dataplex"
TABLE_DESCRIPTION = "Every order a customer placed, as the warehouse team keeps it"
AMOUNT_DESCRIPTION = "The order's total, tax included"
_CREDENTIALS = ("GOOGLE_CLOUD_PROJECT", "GOOGLE_APPLICATION_CREDENTIALS", "BIGQUERY_LOCATION")


def _credentials() -> tuple[str, str, str]:
    missing = [name for name in _CREDENTIALS if not os.environ.get(name)]
    assert not missing, f"the Dataplex link e2e needs {', '.join(missing)}"
    key = Path(os.environ["GOOGLE_APPLICATION_CREDENTIALS"]).read_text()
    return os.environ["GOOGLE_CLOUD_PROJECT"], os.environ["BIGQUERY_LOCATION"], key


def _token(key: str) -> str:
    from google.auth.transport.requests import Request
    from google.oauth2 import service_account

    credentials = service_account.Credentials.from_service_account_info(
        json.loads(key), scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    credentials.refresh(Request())
    return credentials.token


def _customer_table(project: str, location: str, dataset: str) -> str:
    """The customer's own BigQuery table, described by its team; its resource name."""
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    created = bigquery.Dataset(f"{project}.{dataset}")
    created.location = location
    client.create_dataset(created)
    table = bigquery.Table(
        f"{project}.{dataset}.orders",
        schema=[
            bigquery.SchemaField("id", "INT64", description="The order's number"),
            bigquery.SchemaField("amount", "NUMERIC", description=AMOUNT_DESCRIPTION),
        ],
    )
    table.description = TABLE_DESCRIPTION
    client.create_table(table)
    return f"//bigquery.googleapis.com/projects/{project}/datasets/{dataset}/tables/orders"


async def _hold_owner_role(client: httpx.AsyncClient, email: str) -> None:
    """A member holding the product's owner role, with ``email`` in the directory."""
    import asyncpg

    granted = await client.post(
        f"/admin/users/{OWNER}/assignments",
        json={"role_id": PRODUCT_FIELDS["owner_role"], "domain_id": "*"},
    )
    assert granted.status_code == 200, granted.text
    conn = await asyncpg.connect(
        host=os.environ["PG_HOST"],
        port=int(os.environ["PG_PORT"]),
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
        database=os.environ["PG_DATABASE"],
        timeout=15,
    )
    try:
        await conn.execute(
            f'INSERT INTO "org_{ORG}".user_directory (user_id, email) VALUES ($1, $2) '
            "ON CONFLICT (user_id) DO UPDATE SET email = EXCLUDED.email",
            OWNER,
            email,
        )
    finally:
        await conn.close()


def _cleanup(project: str, region: str, key: str, product_id: str, dataset: str) -> None:
    from google.cloud import bigquery

    headers = {"Authorization": f"Bearer {_token(key)}"}
    name = f"projects/{project}/locations/{region}/dataProducts/{product_id}"
    with httpx.Client(headers=headers, timeout=60) as http:
        listed = http.get(f"{API}/{name}/dataAssets")
        if listed.status_code == 200:
            for asset in listed.json().get("dataAssets", []):
                http.delete(f"{API}/{asset['name']}")
        http.delete(f"{API}/{name}")
    client = bigquery.Client(project=project)
    for made in (dataset, export_schema(ORG), replica_schema(ORG)):
        client.delete_dataset(f"{project}.{made}", delete_contents=True, not_found_ok=True)


async def _candidate(client: httpx.AsyncClient, table_id: int, dataset: str) -> dict[str, Any]:
    """The customer's table in Dataplex's pick list, once Dataplex has cataloged it (it does so
    on its own, minutes after the table is made)."""
    for _ in range(90):
        answer = await client.get(
            f"/admin/tables/{table_id}/catalog/candidates", params={"search": dataset}
        )
        assert answer.status_code == 200, answer.text
        (listing,) = answer.json()["catalogs"]
        assert "error" not in listing, listing
        for asset in listing["assets"]:
            if asset["name"].endswith(f"{dataset}.orders"):
                return asset
        await asyncio.sleep(10)
    raise AssertionError(f"Dataplex's search never listed {dataset}.orders")


async def test_a_member_takes_its_descriptions_from_dataplex_and_is_published_as_the_pick():
    project, location, key = _credentials()
    run = uuid.uuid4().hex[:8]
    dataset = f"provisa_e2e_{run}"
    product = f"provisa_e2e_{run}"
    product_id = product.replace("_", "-")
    region = location.lower()
    try:
        resource = _customer_table(project, location, dataset)
        async with published_server(
            ORG,
            engine="bigquery",
            env={"PROVISA_ENGINE_URL": f"bigquery://{project}?location={location}"},
            with_view=False,
            product=product,
        ) as client:
            for table in ("orders", "customers"):
                read = await client.post(
                    "/data/sql",
                    json={"sql": f'SELECT id FROM "{DOMAIN}"."{table}"', "role": "org_admin"},
                )
                assert read.status_code == 200, read.text
            await _hold_owner_role(client, json.loads(key)["client_email"])
            await save_targets(
                client,
                [
                    {
                        "id": CATALOG,
                        "provider": "dataplex",
                        "endpoint": "https://dataplex.googleapis.com",
                        "auth_mode": "google_service_account",
                        "google_service_account_json": key,
                        "gcp_project": project,
                        "gcp_location": region,
                        "timeout_seconds": 300,
                    }
                ],
            )
            tables = (
                await client.post("/admin/graphql", json={"query": "{ tables { id tableName } }"})
            ).json()["data"]["tables"]
            (table_id,) = [int(t["id"]) for t in tables if t["tableName"] == "orders"]

            asset = await _candidate(client, table_id, dataset)
            offer = (
                await client.get(
                    f"/admin/tables/{table_id}/catalog/{CATALOG}/offer",
                    params={"ref": asset["ref"]},
                )
            ).json()
            assert offer["description"]["catalog"] == TABLE_DESCRIPTION
            by_column = {c["name"]: c["description"] for c in offer["catalog_columns"]}
            assert by_column["amount"] == AMOUNT_DESCRIPTION

            applied = await client.post(
                f"/admin/tables/{table_id}/catalog/{CATALOG}/apply",
                json={"ref": asset["ref"], "description": True, "columns": {"amount": "amount"}},
            )
            assert applied.status_code == 200, applied.text

            published = await client.post("/admin/metadata-export/publish")
            assert published.status_code == 200, published.text
            (outcome,) = published.json()["targets"]
            assert outcome["ok"], outcome["errors"]
            headers = {"Authorization": f"Bearer {_token(key)}"}
            name = f"projects/{project}/locations/{region}/dataProducts/{product_id}"
            with httpx.Client(headers=headers, timeout=60) as http:
                assets = http.get(f"{API}/{name}/dataAssets")
            assert assets.status_code == 200, assets.text
            resources = {a["resource"] for a in assets.json()["dataAssets"]}
            assert resource in resources
    finally:
        _cleanup(project, region, key, product_id, dataset)
