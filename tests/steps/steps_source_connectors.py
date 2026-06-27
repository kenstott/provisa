# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

import os
import json

import pytest
import pytest_asyncio
import httpx
from pytest_bdd import given, when, then, scenarios, parsers

from provisa.sources.gql import GQLRemoteSource, GQLSourceConfig
from provisa.sources.counts import graph_counts


scenarios("../features/REQ-673.feature")


@pytest.fixture
def shared_data():
    return {}


class _StubGraphQLTransport(httpx.AsyncBaseTransport):
    """Records GraphQL requests and returns a canned aggregate count response."""

    def __init__(self, count_value):
        self.count_value = count_value
        self.requests = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8")) if request.content else {}
        self.requests.append(body)
        payload = {"data": {"users": {"totalCount": self.count_value}}}
        return httpx.Response(
            200,
            json=payload,
            request=request,
            headers={"content-type": "application/json"},
        )


@given("a GQL remote source with count_query configured and a cold Trino cache")
def gql_source_with_count_query(shared_data):
    transport = _StubGraphQLTransport(count_value=4242)
    client = httpx.AsyncClient(
        transport=transport,
        base_url="http://remote-gql.test",
    )

    config = GQLSourceConfig(
        name="users_source",
        endpoint="http://remote-gql.test/graphql",
        count_query="query { users { totalCount } }",
    )

    source = GQLRemoteSource(config=config, http_client=client)

    # Simulate a cold Trino cache: no cached counts available.
    shared_data["transport"] = transport
    shared_data["client"] = client
    shared_data["config"] = config
    shared_data["source"] = source
    shared_data["cache_warm"] = False

    assert config.count_query, "count_query must be configured for this scenario"
    assert shared_data["cache_warm"] is False


@when("the graph-counts endpoint is called")
@pytest.mark.asyncio
async def call_graph_counts(shared_data):
    source = shared_data["source"]
    result = await graph_counts(
        source=source,
        cache_warm=shared_data["cache_warm"],
    )
    shared_data["result"] = result
    await shared_data["client"].aclose()


@then(
    "the remote GraphQL API is queried to return node counts "
    "instead of returning no count"
)
def assert_remote_queried(shared_data):
    transport = shared_data["transport"]
    result = shared_data["result"]

    # The remote GraphQL API must have been contacted exactly because the
    # local Trino cache was cold and a count_query was configured.
    assert len(transport.requests) == 1, (
        "expected exactly one remote GraphQL query when cache is cold, "
        f"got {len(transport.requests)}"
    )

    sent_query = transport.requests[0].get("query", "")
    assert "totalCount" in sent_query, (
        "the configured count_query should have been sent to the remote API"
    )

    # A real, non-empty count must be returned rather than no count.
    assert result is not None, "graph-counts must not return None for a cold GQL source"
    count = result["users_source"] if isinstance(result, dict) else result
    assert count == 4242, f"expected remote-derived node count 4242, got {count}"
