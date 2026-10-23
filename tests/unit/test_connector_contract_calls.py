# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every non-SQL connector's remote call is its contract's call (REQ-1756): an API endpoint
(REST, GraphQL API, gRPC, SPARQL, Neo4j), a remote GraphQL field and a remote gRPC method. Each
plans a read with no capability (v1: it lands) and calls its remote with the plan's arguments."""

# Requirements: REQ-1756, REQ-863

from types import SimpleNamespace

import pytest

from provisa.api_source import caller
from provisa.api_source.contract import call_endpoint
from provisa.api_source.models import ApiSourceType
from provisa.federation.connector_contract import Read, contract_for
from provisa.graphql_remote import executor
from provisa.graphql_remote.contract import call_field
from provisa.source_adapters import grpc_remote_adapter
from provisa.source_adapters.grpc_remote_contract import call_method


@pytest.mark.parametrize(
    "source_type",
    ["openapi", "graphql_api", "grpc_api", "sparql", "neo4j", "graphql_remote", "grpc_remote"],
)
async def test_every_non_sql_connector_plans_no_complete_dataset(source_type):
    read = Read(table={"table_name": "t", "source_id": "s"}, name="t", statement="", source_id="s")
    contract = contract_for(source_type)
    assert contract is not None
    plan = await contract.plan(None, read)
    assert plan.complete_dataset is False
    assert "declares no capability" in (plan.incomplete_because or "")


async def test_an_endpoints_call_is_made_with_its_sources_address_credential_and_headers(
    monkeypatch,
):
    made: dict = {}

    async def _call_api(endpoint, params, *, base_url, auth, source_headers):
        made.update(endpoint=endpoint, params=params, base_url=base_url, auth=auth)
        made["headers"] = source_headers
        return "answer"

    monkeypatch.setattr(caller, "call_api", _call_api)
    endpoint = SimpleNamespace(source_id="crm", table_name="orders")
    source = SimpleNamespace(
        type=ApiSourceType.sparql, base_url="https://crm.example", auth="key", headers={"X": "1"}
    )
    assert await call_endpoint(endpoint, source, {"status": "open"}) == "answer"
    assert made == {
        "endpoint": endpoint,
        "params": {"status": "open"},
        "base_url": "https://crm.example",
        "auth": "key",
        "headers": {"X": "1"},
    }


async def test_a_remote_fields_call_is_its_request_with_the_plans_variables(monkeypatch):
    sent: dict = {}

    async def _execute_remote(**kwargs):
        sent.update(kwargs)
        return executor.RemoteAnswer([{"id": 1}])

    monkeypatch.setattr(executor, "execute_remote", _execute_remote)
    answer = await call_field(
        "gh", "issues", {"url": "https://gh.example/graphql", "field_name": "issues"}, {"n": 2}
    )
    assert answer.rows == [{"id": 1}]
    assert sent == {
        "url": "https://gh.example/graphql",
        "field_name": "issues",
        "variables": {"n": 2},
    }


async def test_a_remote_methods_call_is_its_request_with_the_plans_arguments(monkeypatch):
    sent: dict = {}

    async def _fetch(**kwargs):
        sent.update(kwargs)
        return [{"id": 1}]

    monkeypatch.setattr(grpc_remote_adapter, "fetch", _fetch)
    rows = await call_method("pay", "pay__Get", {"full_method_path": "/pay.Pay/Get"}, {"id": 1})
    assert rows == [{"id": 1}]
    assert sent == {"source_id": "pay", "full_method_path": "/pay.Pay/Get", "args": {"id": 1}}
