# Copyright (c) 2026 Kenneth Stott
# Canary: 67129f2a-7178-4194-a047-cdbf3d5e4b8c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A remote GraphQL source added with its endpoint as a ``${env:...}`` reference is read at the
endpoint the reference names.

A config-declared source keeps its path as written and boot resolves it before posting
(``app_loaders``). Adding a source through the form posted the reference itself, and introspection
failed with "Request URL is missing an 'http://' or 'https://' protocol"."""

# Requirements: REQ-307, REQ-320

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from provisa.api.admin import graphql_remote_router as router_mod
from provisa.api.admin.graphql_remote_router import (
    GraphQLRemoteSourceRequest,
    register_graphql_remote_source,
)
from provisa.api.errors import ApiError


@pytest.mark.asyncio
async def test_the_endpoint_reference_is_resolved_before_introspection(monkeypatch):
    monkeypatch.setenv("E2E_REMOTE_GQL_URL", "http://remote.test:4000/graphql")
    monkeypatch.setattr(router_mod, "require_capability_request", lambda request, cap: None)
    asked: list[str] = []

    async def _introspect(url, auth=None):
        asked.append(url)
        raise RuntimeError("stop after introspection")

    monkeypatch.setattr("provisa.graphql_remote.introspect.introspect_schema", _introspect)
    body = GraphQLRemoteSourceRequest(
        source_id="remote",
        url="${env:E2E_REMOTE_GQL_URL}",
        namespace="remote",
    )
    with pytest.raises(ApiError):
        await register_graphql_remote_source(cast(Any, SimpleNamespace()), body)
    assert asked == ["http://remote.test:4000/graphql"]
