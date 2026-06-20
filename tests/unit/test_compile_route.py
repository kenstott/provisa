# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-161 — POST /data/compile REST route (compile-only, governed SQL/route/params)."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from provisa.api.data.endpoint import CompileRequest, compile_endpoint


def _raw_request(role=None):
    return SimpleNamespace(state=SimpleNamespace(role=role))


@pytest.mark.asyncio
async def test_compile_returns_compiled_results():
    fake_state = MagicMock()
    fake_state.contexts = {"analyst": object()}
    results = [{"sql": "SELECT 1", "route": "DIRECT", "sources": ["pg"]}]
    with (
        patch("provisa.api.app.state", fake_state),
        patch(
            "provisa.api.admin.dev_queries.compile_query",
            new=AsyncMock(return_value=results),
        ) as cq,
    ):
        resp = await compile_endpoint(
            _raw_request(role="analyst"),
            CompileRequest(query="{ orders { id } }", variables=None),
            x_provisa_role=None,
        )
    body = json.loads(bytes(resp.body))
    assert body["compiled"] == results
    cq.assert_awaited_once()
    assert cq.await_args.args[0] == "analyst"  # role from auth


@pytest.mark.asyncio
async def test_compile_role_from_header_when_unauthenticated():
    fake_state = MagicMock()
    fake_state.contexts = {"analyst": object()}
    with (
        patch("provisa.api.app.state", fake_state),
        patch(
            "provisa.api.admin.dev_queries.compile_query",
            new=AsyncMock(return_value=[]),
        ) as cq,
    ):
        await compile_endpoint(
            _raw_request(role=None),
            CompileRequest(query="{ orders { id } }"),
            x_provisa_role="analyst",
        )
    assert cq.await_args.args[0] == "analyst"


@pytest.mark.asyncio
async def test_compile_403_for_unknown_role():
    fake_state = MagicMock()
    fake_state.contexts = {}
    with patch("provisa.api.app.state", fake_state):
        with pytest.raises(HTTPException) as ei:
            await compile_endpoint(
                _raw_request(role="ghost"),
                CompileRequest(query="{ orders { id } }"),
                x_provisa_role=None,
            )
    assert ei.value.status_code == 403


@pytest.mark.asyncio
async def test_compile_400_on_compile_error():
    fake_state = MagicMock()
    fake_state.contexts = {"analyst": object()}
    with (
        patch("provisa.api.app.state", fake_state),
        patch(
            "provisa.api.admin.dev_queries.compile_query",
            new=AsyncMock(side_effect=ValueError("Unknown root query field")),
        ),
    ):
        with pytest.raises(HTTPException) as ei:
            await compile_endpoint(
                _raw_request(role="analyst"),
                CompileRequest(query="{ nope { id } }"),
                x_provisa_role=None,
            )
    assert ei.value.status_code == 400
