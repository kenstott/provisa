# Copyright (c) 2026 Kenneth Stott
# Canary: 1f5a90c7-63b8-4de2-a074-8c3b12e0f4a9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1069 e2e: the same governed config reaches a real Apache Atlas as entities.

Atlas is also the Microsoft Purview path — Purview serves the same ``/api/atlas/v2`` routes and
the same entity envelope, and only the authentication differs. What this run proves about the
mapping therefore holds for Purview; what it cannot prove is the Entra token exchange, which has
no self-provisionable stand-in.

Assertions read the entities back out of Atlas's own API, so what passes is what the catalog
stored — not what the adapter believed it sent.
"""

# Requirements: REQ-1069, REQ-1070, REQ-1071

from __future__ import annotations

import asyncio
import base64
import json
import os
import time

import httpx
import pytest

from provisa.api.metadata_export import metadata_export
from provisa.api.metadata_export.atlas import CLUSTER
from provisa.core.models import MetadataExportConfig
from tests.integration.metadata_export_fixture import (
    MASK_PATTERN,
    RLS_FILTER,
    governed_snapshot,
)

# REQ-1388/1389: the native Atlas path (to_native_entities) rides its own typed attribute,
# never GOVERNANCE_ATTRIBUTE ("userDescription") — that constant belongs to the legacy
# rdbms_* path (to_entities), used by the separate Atlan provider.
NATIVE_GOVERNANCE_ATTRIBUTE = "provisaGovernance"

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.requires_atlas,
    pytest.mark.asyncio(loop_scope="session"),
]

# The image's built-in administrator. Stock Atlas authenticates over HTTP basic and answers a
# bearer token with 401; the container is the isolated test stack's own.
ATLAS_USER = "admin"
ATLAS_PASSWORD = "admin"

ORDERS = f"wh.public.orders@{CLUSTER}"
CUSTOMERS = f"wh.public.customers@{CLUSTER}"
TOTALS = f"wh.public.order_totals@{CLUSTER}"
DERIVATION = f"wh.public.order_totals.derivation@{CLUSTER}"


def _base_url() -> str:
    return f"http://localhost:{os.environ['ATLAS_PORT']}"


def _export():
    return metadata_export(
        MetadataExportConfig(
            enabled=True,
            provider="atlas",
            endpoint=_base_url(),
            auth_mode="basic",
            username=ATLAS_USER,
            token=ATLAS_PASSWORD,
            timeout_seconds=60,
        )
    )


# Per test, not per module: the heavy-service fixture provisions Atlas for one test and removes
# it afterwards, so a module-scoped publish would leave every test after the first reading a
# catalog that no longer holds it. Publishing upserts on qualifiedName, so repeating it is cheap
# and lands the same state each time.
@pytest.fixture
async def published():
    export = _export()
    await export.health()
    result = await export.publish(governed_snapshot())
    assert result.ok, [e.message for e in result.errors]
    return result


async def _entity(type_name: str, qualified_name: str) -> dict:
    """Read an entity back by its Atlas search index — which lags a bulk create.

    ``atlas.py``'s own binding-capture path exists specifically because this index is
    "laggy" (see ``_publish_entities``'s guidAssignments comment); a publish reporting
    ``ok`` promises the entity was accepted, not that the search index has caught up yet.
    Poll on a monotonic deadline rather than assume immediate consistency, matching the
    established pattern used for the other service-readiness waits in this suite.
    """
    pair = base64.b64encode(f"{ATLAS_USER}:{ATLAS_PASSWORD}".encode()).decode()
    headers = {"Authorization": f"Basic {pair}"}
    url = f"{_base_url()}/api/atlas/v2/entity/uniqueAttribute/type/{type_name}"
    params = {"attr:qualifiedName": qualified_name}
    deadline = time.monotonic() + 60
    async with httpx.AsyncClient(timeout=60) as client:
        while True:
            response = await client.get(url, params=params, headers=headers)
            if response.status_code != 404 or time.monotonic() >= deadline:
                break
            await asyncio.sleep(0.5)
    response.raise_for_status()
    return response.json()["entity"]


async def test_every_governed_table_and_column_lands_in_the_catalog(published):
    assert published.published["table"] == 3
    assert published.published["column"] == 9
    orders = await _entity("provisa_table", ORDERS)
    assert orders["attributes"]["name"] == "orders"
    for qualified_name in (ORDERS, CUSTOMERS, TOTALS):
        assert (await _entity("provisa_table", qualified_name))["status"] == "ACTIVE"
    ssn = await _entity("provisa_column", f"wh.public.orders.ssn@{CLUSTER}")
    assert ssn["attributes"]["dataType"] == "text"
    amount = await _entity("provisa_column", f"wh.public.orders.amount@{CLUSTER}")
    assert amount["attributes"]["description"] == "Order total in cents"


async def test_governance_signals_arrive_as_atlas_classifications(published):
    ssn = await _entity("provisa_column", f"wh.public.orders.ssn@{CLUSTER}")
    masked = next(c for c in ssn["classifications"] if c["typeName"] == "provisa_masked")
    assert (
        masked["attributes"]["ruleId"] == "mask:provisa://acme/sales/tables/orders#field:ssn:regex"
    )
    assert masked["attributes"]["exemptRoles"] == ["admin"]
    assert masked["attributes"]["restrictedRoles"] == ["analyst"]

    margin = await _entity("provisa_column", f"wh.public.orders.margin@{CLUSTER}")
    assert [c["typeName"] for c in margin["classifications"]] == ["provisa_visibility_restricted"]

    orders = await _entity("provisa_table", ORDERS)
    assert [c["typeName"] for c in orders["classifications"]] == ["provisa_rls_restricted"]

    identifier = await _entity("provisa_column", f"wh.public.orders.id@{CLUSTER}")
    assert not identifier.get("classifications")


async def test_the_steward_and_the_approved_relationship_are_readable_from_the_catalog(published):
    orders = await _entity("provisa_table", ORDERS)
    # Atlas's own accountability field, so a catalog reader finds the steward where they already
    # look for an owner (REQ-609).
    assert orders["attributes"]["owner"] == "data-steward"
    document = json.loads(orders["attributes"][NATIVE_GOVERNANCE_ATTRIBUTE])
    assert document["domain"]["id"] == "sales"
    assert document["domain"]["steward"] == "data-steward"
    approved = document["approvedRelationships"]
    assert [edge["id"] for edge in approved] == ["orders-to-customers"]
    assert approved[0]["target"] == "wh.public.customers"
    assert approved[0]["sourceColumn"] == "customer_id"


async def test_column_lineage_of_the_derived_view_is_a_process_between_the_two_tables(published):
    assert published.published["lineage"] == 1
    process = await _entity("Process", DERIVATION)
    orders = await _entity("provisa_table", ORDERS)
    totals = await _entity("provisa_table", TOTALS)
    assert [ref["guid"] for ref in process["attributes"]["inputs"]] == [orders["guid"]]
    assert [ref["guid"] for ref in process["attributes"]["outputs"]] == [totals["guid"]]
    # Atlas has no column-level lineage type, so the column pairs and the compiled transform ride
    # on the process — the derivation stays explainable rather than collapsing to a table edge.
    transforms = {
        edge["to"]: edge["transforms"] for edge in json.loads(process["attributes"]["description"])
    }
    assert transforms["wh.public.order_totals.net"] == ["orders.amount * 2"]
    assert transforms["wh.public.order_totals.id"] == ["orders.id"]


async def test_republishing_lands_the_same_catalog_state(published):
    """REQ-1072's reconcile republishes the whole snapshot on a schedule, so a second publish
    has to be an upsert rather than a duplicate."""
    again = await _export().publish(governed_snapshot())
    assert again.ok, [e.message for e in again.errors]
    assert again.published == published.published
    orders = await _entity("provisa_table", ORDERS)
    assert [c["typeName"] for c in orders["classifications"]] == ["provisa_rls_restricted"]


async def test_the_catalog_is_never_handed_a_path_to_the_source(published):
    """Atlas must reach the data only through Provisa — a real connection config would give it
    an ungoverned route to the warehouse."""
    instance = await _entity("provisa_source", f"wh@{CLUSTER}")
    assert instance["attributes"]["sourceType"] == "postgresql"
    assert not instance["attributes"].get("hostname")
    assert not instance["attributes"].get("connectionString")


async def test_no_rule_body_is_stored_in_the_external_catalog(published):
    stored = json.dumps(
        [
            await _entity("provisa_table", ORDERS),
            await _entity("provisa_column", f"wh.public.orders.ssn@{CLUSTER}"),
            await _entity("provisa_column", f"wh.public.orders.margin@{CLUSTER}"),
            await _entity("Process", DERIVATION),
        ]
    )
    assert MASK_PATTERN not in stored
    assert RLS_FILTER not in stored
    assert "current_setting" not in stored
