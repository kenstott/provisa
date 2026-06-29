# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""pytest-bdd step definitions for REQ-738 — NoSQL Adapters."""

from __future__ import annotations

import asyncio
import os

import pytest
from pytest_bdd import given, scenario, then, when

pytestmark = [pytest.mark.integration]


# ---------------------------------------------------------------------------
# Shared state fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data():
    return {}


# ---------------------------------------------------------------------------
# Scenario binding
# ---------------------------------------------------------------------------


@scenario(
    "../features/REQ-738.feature",
    "REQ-738 default behaviour",
)
def test_req_738_default():
    pass


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------


@given("a running MongoDB service with seeded test documents")
def given_running_mongodb_with_seeded_docs(shared_data):
    """Connect to a live MongoDB service and seed test documents."""
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    async def _body():
        import motor.motor_asyncio  # noqa: PLC0415

        host = os.environ.get("MONGO_HOST", "localhost")
        port = int(os.environ.get("MONGO_PORT", "27017"))

        client = motor.motor_asyncio.AsyncIOMotorClient(
            host=host,
            port=port,
            serverSelectionTimeoutMS=3000,
        )

        try:
            await client.admin.command("ping")
        except Exception as exc:  # noqa: BLE001
            client.close()
            pytest.skip(f"MongoDB not reachable: {exc}")

        db = client["provisa_test"]
        col = db["bdd_test_collection"]

        await col.delete_many({})
        await col.insert_many(
            [
                {"item": "alpha", "qty": 10, "category": "widget"},
                {"item": "beta", "qty": 25, "category": "widget"},
                {"item": "gamma", "qty": 3, "category": "gadget"},
            ]
        )

        shared_data["mongo_client"] = client
        shared_data["mongo_collection"] = col
        shared_data["filter_criteria"] = {"qty": {"$gt": 5}}

    asyncio.run(_body())


@when("the adapter queries the collection with filter criteria")
def when_adapter_queries_collection(shared_data):
    """Execute the filtered query via the motor async client."""
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    async def _body():
        col = shared_data["mongo_collection"]
        criteria = shared_data["filter_criteria"]
        docs = await col.find(criteria).to_list(length=100)
        shared_data["query_result"] = docs

    asyncio.run(_body())


@then("documents matching the filter are returned")
def then_documents_matching_filter_returned(shared_data):
    """Assert that only documents satisfying the filter criteria are present."""
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    async def _body():
        docs = shared_data.get("query_result", [])

        assert len(docs) == 2, f"Expected 2 documents matching qty > 5, got {len(docs)}: {docs}"

        items = {d["item"] for d in docs}
        assert "alpha" in items, f"'alpha' missing from results: {items}"
        assert "beta" in items, f"'beta' missing from results: {items}"
        assert "gamma" not in items, f"'gamma' should not match qty > 5 but was in: {items}"

        for doc in docs:
            assert doc["qty"] > 5, (
                f"Document {doc['item']} has qty={doc['qty']} which fails qty > 5"
            )

        client = shared_data.get("mongo_client")
        col = shared_data.get("mongo_collection")
        if col is not None:
            await col.delete_many({})
        if client is not None:
            client.close()

    asyncio.run(_body())
