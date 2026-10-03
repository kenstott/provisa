# Copyright (c) 2026 Kenneth Stott
# Canary: be77d28f-7804-464d-8211-9a29a1dae68a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A source call's cached rows belong to one org, one environment and one model (REQ-318, REQ-327, REQ-595).

The gRPC-remote adapter keeps the rows a call returned in the response-cache store, under a key
made of the source id, the operation, the arguments and the role. It read and wrote it with no
scope, so the entry sat at the same key for every org and environment: a call in one was
answered with rows fetched for another whenever the source id, operation, arguments and role
name matched. It now reads and writes under the scope every other cache uses. (An OpenAPI table
is read through the API caller, whose cache is the store's API cache schema of the org.)
"""

# Requirements: REQ-318, REQ-327, REQ-595, REQ-1914, REQ-1529

from __future__ import annotations


import pytest

from provisa.cache import tenancy
from provisa.source_adapters import grpc_remote_adapter
from tests.unit.test_response_cache_shared import FakeCacheStore

BASE = "http://crm.test"


@pytest.fixture
def scope(monkeypatch):
    acting = {"scope": "acme:m7"}
    monkeypatch.setattr(tenancy, "acting_scope", lambda: acting["scope"])
    return acting


# --- gRPC remote ---------------------------------------------------------------------------------


@pytest.fixture
def grpc_calls(monkeypatch):
    """The remote answers with rows naming the org that asked; counts the calls made."""
    made: list[str] = []

    async def _execute_query(*_args, **_kwargs):
        made.append("call")
        return [{"id": len(made)}]

    monkeypatch.setattr(grpc_remote_adapter, "execute_query", _execute_query)
    monkeypatch.setattr(grpc_remote_adapter, "_get_channel", lambda *_a: object())
    return made


async def _grpc(store) -> list[dict]:
    return await grpc_remote_adapter.fetch(
        "crm", "/crm.Customers/List", "ListRequest", "ListReply", object(), {"page": 1}, {}, store,
        role="analyst",
    )  # fmt: skip


async def test_grpc_a_repeated_call_in_one_place_is_served_from_the_cache(scope, grpc_calls):
    store = FakeCacheStore()
    assert await _grpc(store) == [{"id": 1}]
    assert await _grpc(store) == [{"id": 1}]
    assert grpc_calls == ["call"]


@pytest.mark.parametrize("other", ["globex:m7", "acme_env_staging:m7", "acme:m8"])
async def test_grpc_the_same_call_elsewhere_is_not_served_these_rows(scope, grpc_calls, other):
    store = FakeCacheStore()
    assert await _grpc(store) == [{"id": 1}]
    scope["scope"] = other
    assert await _grpc(store) == [{"id": 2}]  # fetched again, for this org / environment / model
    scope["scope"] = "acme:m7"
    assert await _grpc(store) == [{"id": 1}]


def test_the_acting_scope_is_the_one_every_cache_uses():
    import provisa.api.app as appmod

    assert tenancy.acting_scope() == tenancy.cache_tenant(appmod.state)
