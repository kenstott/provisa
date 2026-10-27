# Copyright (c) 2026 Kenneth Stott
# Canary: 3c095b84-b22c-4040-97d0-25fe6bde2e68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2118 e2e: a Provisa server on BigQuery publishes a data product to Google Dataplex.

The server's engine is the project's BigQuery. The data product's two members are tables of the
test stack's Postgres, which BigQuery cannot read in place: each is replicated on its first read,
and the reconcile publishes a view of each replica in the org's export dataset. The product's
owner role has one holder, a member whose address is the service account's own. The target is
saved and published through the admin API, then the data product and its assets are read back
from Dataplex itself. The product is named for the run (provisa-e2e-*) and deleted after it, with
the datasets the engine created for the org.

Without the project's credentials the test fails, never skips: a target with no end-to-end proof
is not complete (REQ-2119).
"""

# Requirements: REQ-2118, REQ-2119

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import httpx
import pytest

from provisa.api.metadata_export.dataplex import RESOURCE_MANAGER, DataplexExport, resource_id
from provisa.federation.replica_address import export_schema, replica_schema, replica_table_name
from tests.integration.metadata_export_server import (
    CLASS_TERM,
    DOMAIN,
    PRODUCT_FIELDS,
    SCHEMA,
    SOURCE,
    publish,
    published_server,
    refused,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_warehouse,
    pytest.mark.asyncio(loop_scope="session"),
]

ORG = "mdx_dataplex_e2e"
OWNER = "provisa-e2e-owner"
API = "https://dataplex.googleapis.com/v1"
_CREDENTIALS = ("GOOGLE_CLOUD_PROJECT", "GOOGLE_APPLICATION_CREDENTIALS", "BIGQUERY_LOCATION")


def _credentials() -> tuple[str, str, str]:
    missing = [name for name in _CREDENTIALS if not os.environ.get(name)]
    assert not missing, f"the Dataplex e2e needs {', '.join(missing)}"
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


async def _hold_owner_role(client: httpx.AsyncClient, email: str) -> None:
    """A member holding the product's owner role, with ``email`` in the directory: the grant
    through the admin API, the address as a member's sign-in records it."""
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


def _glossary_name(parent: str) -> str:
    return f"{parent}/glossaries/{resource_id(f'provisa-{ORG}')}"


def _remove_glossary(http: httpx.Client, parent: str) -> None:
    """The org's glossary, emptied and removed: its links, terms and categories first."""
    glossary = _glossary_name(parent)
    terms = http.get(f"{API}/{glossary}/terms").json().get("terms", [])
    numbered = _numbered(http, parent)
    for term in terms:
        entry = DataplexExport._term_entry(numbered, term["name"])
        links = http.get(f"{API}/{parent}:lookupEntryLinks", params={"entry": entry}).json()
        for link in links.get("entryLinks", []):
            http.delete(f"{API}/{link['name']}")
        http.delete(f"{API}/{glossary}/terms/{term['name'].rsplit('/', 1)[-1]}")
    for category in http.get(f"{API}/{glossary}/categories").json().get("categories", []):
        http.delete(f"{API}/{category['name']}")
    http.delete(f"{API}/{glossary}")


def _cleanup(project: str, location: str, key: str, product_id: str) -> None:
    """The run's data product, its assets and the org's datasets, removed."""
    from google.cloud import bigquery

    headers = {"Authorization": f"Bearer {_token(key)}"}
    name = f"projects/{project}/locations/{location}/dataProducts/{product_id}"
    with httpx.Client(headers=headers, timeout=60) as http:
        listed = http.get(f"{API}/{name}/dataAssets")
        if listed.status_code == 200:
            for asset in listed.json().get("dataAssets", []):
                http.delete(f"{API}/{asset['name']}")
        http.delete(f"{API}/{name}")
        _remove_glossary(http, f"projects/{project}/locations/{location}")
    client = bigquery.Client(project=project)
    for dataset in (export_schema(ORG), replica_schema(ORG)):
        client.delete_dataset(f"{project}.{dataset}", delete_contents=True, not_found_ok=True)


@pytest.fixture(scope="module")
async def published():
    project, location, key = _credentials()
    product = f"provisa_e2e_{uuid.uuid4().hex[:8]}"
    product_id = product.replace("_", "-")
    region = location.lower()
    try:
        async with published_server(
            ORG,
            engine="bigquery",
            env={"PROVISA_ENGINE_URL": f"bigquery://{project}?location={location}"},
            with_view=False,
            product=product,
        ) as client:
            # A member is replicated into BigQuery on its first read; its export view is the
            # Dataplex asset.
            for table in ("orders", "customers"):
                read = await client.post(
                    "/data/sql",
                    json={"sql": f'SELECT id FROM "{DOMAIN}"."{table}"', "role": "org_admin"},
                )
                assert read.status_code == 200, read.text
            owner_email = json.loads(key)["client_email"]
            await _hold_owner_role(client, owner_email)
            result = await publish(
                client,
                {
                    "provider": "dataplex",
                    "endpoint": "https://dataplex.googleapis.com",
                    "auth_mode": "google_service_account",
                    "google_service_account_json": key,
                    "gcp_project": project,
                    "gcp_location": region,
                    "timeout_seconds": 300,
                },
            )
            headers = {"Authorization": f"Bearer {_token(key)}"}
            name = f"projects/{project}/locations/{region}/dataProducts/{product_id}"
            with httpx.Client(headers=headers, timeout=60) as http:
                found = http.get(f"{API}/{name}")
                assets = http.get(f"{API}/{name}/dataAssets")
                glossary = _glossary_name(f"projects/{project}/locations/{region}")
                term = http.get(f"{API}/{glossary}/terms/{resource_id(CLASS_TERM)}")
                links = (
                    http.get(
                        f"{API}/projects/{project}/locations/{region}:lookupEntryLinks",
                        params={
                            "entry": DataplexExport._term_entry(
                                _numbered(http, f"projects/{project}/locations/{region}"),
                                term.json()["name"],
                            ),
                            "entryMode": "TARGET",
                        },
                    )
                    if term.status_code == 200
                    else None
                )
            yield result, found, assets, project, owner_email, term, links
    finally:
        _cleanup(project, region, key, product_id)


async def test_the_publish_reports_the_data_product_and_its_assets(published):
    result = published[0]
    assert result["ok"], refused(result)
    # REQ-1387/1389: the org's two defined terms -- the Class linked to the orders replica's
    # view, the Property to the two columns it grounds (orders.customer_id, customers.id).
    assert result["published"] == {
        "data_products": 1,
        "data_assets": 2,
        "glossary_term": 2,
        "term_assignment": 3,
    }


async def test_the_data_product_carries_its_fields_and_owners(published):
    found, owner_email = published[1], published[4]
    assert found.status_code == 200, found.text
    product = found.json()
    assert product["displayName"] == PRODUCT_FIELDS["name"]
    assert product["ownerEmails"] == [owner_email]
    assert product["accessApprovalConfig"]["approverEmails"] == [owner_email]
    assert PRODUCT_FIELDS["purpose"] in product["description"]
    assert PRODUCT_FIELDS["limitations"] in product["description"]
    assert product["labels"]["provisa-version"] == "1_2_0"
    assert product["labels"]["provisa-status"] == "live"


async def test_each_member_is_an_asset_through_its_export_view(published):
    assets, project = published[2], published[3]
    assert assets.status_code == 200, assets.text
    resources = {a["resource"] for a in assets.json()["dataAssets"]}
    assert resources == {
        f"//bigquery.googleapis.com/projects/{project}/datasets/{export_schema(ORG)}/tables/"
        f"{replica_table_name(SOURCE, SCHEMA, table)}"
        for table in ("orders", "customers")
    }


def _numbered(http: httpx.Client, parent: str) -> str:
    """``parent`` with its project named by number, as a term's catalog entry names it."""
    _, project, _, location = parent.split("/")
    found = http.get(f"{RESOURCE_MANAGER}/projects/{project}")
    found.raise_for_status()
    return f"{found.json()['name']}/locations/{location}"


async def test_the_class_term_is_in_classes_and_defines_its_tables_view(published):
    """REQ-1387/1389/2141: the Class term sits in the Classes category with its role label, and
    Provisa's definition link runs from the orders replica's view to it."""
    term, links, project = published[5], published[6], published[3]
    assert term.status_code == 200, term.text
    body = term.json()
    assert body["parent"].endswith("/categories/class")
    assert body["labels"]["provisa-role"] == "class"
    assert links is not None and links.status_code == 200, links and links.text
    view = (
        f"bigquery.googleapis.com/projects/{project}/datasets/{export_schema(ORG)}/tables/"
        f"{replica_table_name(SOURCE, SCHEMA, 'orders')}"
    )
    sources = [
        ref["name"]
        for link in links.json().get("entryLinks", [])
        if link["name"].rsplit("/", 1)[-1].startswith("provisa-")
        for ref in link["entryReferences"]
        if ref["type"] == "SOURCE"
    ]
    assert any(name.endswith(view) for name in sources), sources
