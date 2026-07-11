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
import subprocess
from pathlib import Path

import pytest
from pytest_bdd import given, scenario, then, when

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
# The MongoDB the REQ-738 adapter scenario drives, provisioned by THIS test — not
# assumed to be already running. Project-scoped (provisa-test) + service-scoped so
# it composes with the other test-project services.
_MONGO_COMPOSE = ["docker", "compose", "-p", "provisa-test", "-f", "docker-compose.test.yml"]


@pytest.fixture(scope="module", autouse=True)
def _mongodb_service():
    """Provision MongoDB through docker for the REQ-738 scenario and reap it after.

    Tests own the services they exercise: bring the container up and block until
    healthy, then tear it down (unless PYTEST_DOCKER_KEEP=1) so nothing leaks.
    """
    # docker-compose.test.yml joins the shared `provisa_default` network (created by
    # the dev stack). The REQ-738 adapter connects to Mongo from the host over the
    # published 27017, so the network is incidental here — ensure it exists so the
    # service comes up standalone without the whole dev stack.
    subprocess.run(
        ["docker", "network", "create", "provisa_default"],
        capture_output=True,
        check=False,
    )
    subprocess.run([*_MONGO_COMPOSE, "up", "-d", "--wait", "mongodb"], cwd=_REPO, check=True)
    try:
        yield {"host": "localhost", "port": 27017}
    finally:
        if not os.environ.get("PYTEST_DOCKER_KEEP"):
            subprocess.run([*_MONGO_COMPOSE, "rm", "-sf", "mongodb"], cwd=_REPO, check=False)


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
    """Connect to the docker-provisioned MongoDB (_mongodb_service) and seed docs."""
    # A motor AsyncIOMotorClient binds to the event loop it is created in, so it
    # cannot be shared across the separate asyncio.run() loops of the
    # given/when/then steps. Each step creates and closes its own client; only
    # plain data (connection params, filter, result dicts) crosses step
    # boundaries.
    host = os.environ.get("MONGO_HOST", "localhost")
    port = int(os.environ.get("MONGO_PORT", "27017"))
    db_name = "provisa_test"
    col_name = "bdd_test_collection"

    async def _body():
        import motor.motor_asyncio  # noqa: PLC0415

        client = motor.motor_asyncio.AsyncIOMotorClient(
            host=host,
            port=port,
            serverSelectionTimeoutMS=3000,
        )
        try:
            await client.admin.command("ping")
            col = client[db_name][col_name]
            await col.delete_many({})
            await col.insert_many(
                [
                    {"item": "alpha", "qty": 10, "category": "widget"},
                    {"item": "beta", "qty": 25, "category": "widget"},
                    {"item": "gamma", "qty": 3, "category": "gadget"},
                ]
            )
        finally:
            client.close()

    asyncio.run(_body())
    shared_data["mongo_conn"] = {"host": host, "port": port, "db": db_name, "col": col_name}
    shared_data["filter_criteria"] = {"qty": {"$gt": 5}}


@when("the adapter queries the collection with filter criteria")
def when_adapter_queries_collection(shared_data):
    """Execute the filtered query via a fresh motor async client."""
    conn = shared_data["mongo_conn"]
    criteria = shared_data["filter_criteria"]

    async def _body():
        import motor.motor_asyncio  # noqa: PLC0415

        client = motor.motor_asyncio.AsyncIOMotorClient(
            host=conn["host"], port=conn["port"], serverSelectionTimeoutMS=3000
        )
        try:
            col = client[conn["db"]][conn["col"]]
            docs = await col.find(criteria).to_list(length=100)
            shared_data["query_result"] = docs
        finally:
            client.close()

    asyncio.run(_body())


@then("documents matching the filter are returned")
def then_documents_matching_filter_returned(shared_data):
    """Assert that only documents satisfying the filter criteria are present."""
    docs = shared_data.get("query_result", [])

    assert len(docs) == 2, f"Expected 2 documents matching qty > 5, got {len(docs)}: {docs}"

    items = {d["item"] for d in docs}
    assert "alpha" in items, f"'alpha' missing from results: {items}"
    assert "beta" in items, f"'beta' missing from results: {items}"
    assert "gamma" not in items, f"'gamma' should not match qty > 5 but was in: {items}"

    for doc in docs:
        assert doc["qty"] > 5, f"Document {doc['item']} has qty={doc['qty']} which fails qty > 5"

    # Cleanup in a fresh client bound to this step's loop.
    conn = shared_data["mongo_conn"]

    async def _cleanup():
        import motor.motor_asyncio  # noqa: PLC0415

        client = motor.motor_asyncio.AsyncIOMotorClient(
            host=conn["host"], port=conn["port"], serverSelectionTimeoutMS=3000
        )
        try:
            await client[conn["db"]][conn["col"]].delete_many({})
        finally:
            client.close()

    asyncio.run(_cleanup())
