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

from provisa.api.metadata_export.atlas import ASSIGNMENT_SOURCE, CLUSTER, _glossary_qn
from tests.integration.metadata_export_server import (
    CLASS_TERM,
    GLOSSARY_TERM,
    DOMAIN,
    MASK_PATTERN,
    ORDERS_AMOUNT_DESCRIPTION,
    RLS_FILTER,
    SCHEMA,
    SOURCE,
    VIEW,
    publish,
    published_server,
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

ORG = "mdx_atlas_e2e"
_ORDERS = f"{SOURCE}.{SCHEMA}.orders"
_TOTALS = f"__derived__.views.{VIEW}"
ORDERS = f"{_ORDERS}@{CLUSTER}"
CUSTOMERS = f"{SOURCE}.{SCHEMA}.customers@{CLUSTER}"
TOTALS = f"{_TOTALS}@{CLUSTER}"
DERIVATION = f"{_TOTALS}.derivation@{CLUSTER}"


def _base_url() -> str:
    return f"http://localhost:{os.environ['ATLAS_PORT']}"


def _target() -> dict:
    return {
        "provider": "atlas",
        "endpoint": _base_url(),
        "auth_mode": "basic",
        "username": ATLAS_USER,
        "token": ATLAS_PASSWORD,
        "timeout_seconds": 60,
    }


# Per test, not per module: the heavy-service fixture provisions Atlas for one test and removes
# it afterwards, so a module-scoped publish would leave every test after the first reading a
# catalog that no longer holds it. Publishing upserts on qualifiedName, so repeating it is cheap
# and lands the same state each time.
@pytest.fixture
async def published():
    """The production publish (REQ-2119): a Provisa server saves Atlas as its target and
    publishes over the admin API. The server stays up for the test, which republishes."""
    async with published_server(ORG) as client:
        result = await publish(client, _target())
        assert result["ok"], result["errors"]
        yield client, result


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
    assert published[1]["published"]["table"] == 3
    assert published[1]["published"]["column"] == 10
    orders = await _entity("provisa_table", ORDERS)
    assert orders["attributes"]["name"] == "orders"
    for qualified_name in (ORDERS, CUSTOMERS, TOTALS):
        assert (await _entity("provisa_table", qualified_name))["status"] == "ACTIVE"
    ssn = await _entity("provisa_column", f"{_ORDERS}.ssn@{CLUSTER}")
    assert ssn["attributes"]["dataType"] == "text"
    amount = await _entity("provisa_column", f"{_ORDERS}.amount@{CLUSTER}")
    assert amount["attributes"]["description"] == ORDERS_AMOUNT_DESCRIPTION


async def test_governance_signals_arrive_as_atlas_classifications(published):
    ssn = await _entity("provisa_column", f"{_ORDERS}.ssn@{CLUSTER}")
    masked = next(c for c in ssn["classifications"] if c["typeName"] == "provisa_masked")
    assert (
        masked["attributes"]["ruleId"]
        == f"mask:provisa://{ORG}/{DOMAIN}/tables/orders#field:ssn:regex"
    )
    assert masked["attributes"]["exemptRoles"] == ["admin"]
    assert masked["attributes"]["restrictedRoles"] == ["analyst", "data_owner"]

    margin = await _entity("provisa_column", f"{_ORDERS}.margin@{CLUSTER}")
    assert [c["typeName"] for c in margin["classifications"]] == ["provisa_visibility_restricted"]

    orders = await _entity("provisa_table", ORDERS)
    assert [c["typeName"] for c in orders["classifications"]] == ["provisa_rls_restricted"]

    identifier = await _entity("provisa_column", f"{_ORDERS}.id@{CLUSTER}")
    assert not identifier.get("classifications")


async def test_the_steward_and_the_approved_relationship_are_readable_from_the_catalog(published):
    orders = await _entity("provisa_table", ORDERS)
    # Atlas's own accountability field, so a catalog reader finds the steward where they already
    # look for an owner (REQ-609).
    assert orders["attributes"]["owner"] == "data_owner"
    document = json.loads(orders["attributes"][NATIVE_GOVERNANCE_ATTRIBUTE])
    assert document["domain"]["id"] == DOMAIN
    assert document["domain"]["steward"] == "data_owner"
    approved = document["approvedRelationships"]
    assert [edge["id"] for edge in approved] == ["orders-to-customers"]
    assert approved[0]["target"] == f"{SOURCE}.{SCHEMA}.customers"
    assert approved[0]["sourceColumn"] == "customer_id"


async def test_column_lineage_of_the_derived_view_is_a_process_between_the_two_tables(published):
    assert published[1]["published"]["lineage"] == 1
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
    assert transforms[f"{_TOTALS}.net"] == ["o.amount * 2"]
    assert transforms[f"{_TOTALS}.id"] == ["o.id"]


async def test_republishing_lands_the_same_catalog_state(published):
    """REQ-1072's reconcile republishes the whole snapshot on a schedule, so a second publish
    has to be an upsert rather than a duplicate."""
    client, first = published
    (again,) = (await client.post("/admin/metadata-export/publish")).json()["targets"]
    assert again["ok"], again["errors"]
    assert again["published"] == first["published"]
    orders = await _entity("provisa_table", ORDERS)
    assert [c["typeName"] for c in orders["classifications"]] == ["provisa_rls_restricted"]


async def test_the_catalog_is_never_handed_a_path_to_the_source(published):
    """Atlas must reach the data only through Provisa — a real connection config would give it
    an ungoverned route to the warehouse."""
    instance = await _entity("provisa_source", f"{SOURCE}@{CLUSTER}")
    assert instance["attributes"]["sourceType"] == "postgresql"
    assert not instance["attributes"].get("hostname")
    assert not instance["attributes"].get("connectionString")


async def test_no_rule_body_is_stored_in_the_external_catalog(published):
    stored = json.dumps(
        [
            await _entity("provisa_table", ORDERS),
            await _entity("provisa_column", f"{_ORDERS}.ssn@{CLUSTER}"),
            await _entity("provisa_column", f"{_ORDERS}.margin@{CLUSTER}"),
            await _entity("Process", DERIVATION),
        ]
    )
    assert MASK_PATTERN not in stored
    assert RLS_FILTER not in stored
    assert "current_setting" not in stored


async def test_provisa_assigns_its_terms_as_semantic_assignments_it_sourced(published):
    """REQ-1389 (amended): the Class term is assigned to the table it grounds and the Property
    term to its column, each an AtlasGlossarySemanticAssignment whose source is Provisa."""
    orders = await _entity("provisa_table", ORDERS)
    customer_id = await _entity("provisa_column", f"{_ORDERS}.customer_id@{CLUSTER}")
    for term, entity in ((CLASS_TERM, orders), (GLOSSARY_TERM, customer_id)):
        found = await _entity("AtlasGlossaryTerm", f"{term}@{_glossary_qn(ORG)}")
        assigned = found["relationshipAttributes"]["assignedEntities"]
        assert [
            (a["guid"], a["relationshipAttributes"]["attributes"]["source"]) for a in assigned
        ] == [(entity["guid"], ASSIGNMENT_SOURCE)]
