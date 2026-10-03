# Copyright (c) 2026 Kenneth Stott
# Canary: 16f41389-3802-4e4a-b799-e020f99b5279
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1194: a request that asks for its result in the results store (``X-Provisa-Redirect``)
and whose engine-side write fails is refused by name.

It used to log the failure and answer with the rows inline: the caller received the result it
had asked not to receive, with nothing to say the redirect had failed."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from provisa.api.errors import ApiError
from provisa.transpiler.router import Route


async def test_a_forced_redirect_that_fails_is_refused_and_never_answered_inline(monkeypatch):
    from provisa.api.data import endpoint
    from provisa.pgwire import _pipeline

    inline = AsyncMock()
    monkeypatch.setattr(_pipeline, "extend_trace_scope_to_sources", AsyncMock())
    monkeypatch.setattr(_pipeline, "_resolve_pk_bounds", AsyncMock())
    monkeypatch.setattr(
        endpoint, "decide_route", lambda **kw: SimpleNamespace(route=Route.ENGINE, source_id=None)
    )
    monkeypatch.setattr(endpoint, "operator_floor", lambda state, ids: None)
    monkeypatch.setattr(endpoint, "note_request_route", lambda route: None)
    monkeypatch.setattr(
        "provisa.federation.live_concurrency.acquire_for_route",
        AsyncMock(return_value=SimpleNamespace(release=lambda: None)),
    )
    monkeypatch.setattr(
        endpoint, "_exec_ctas_route", AsyncMock(side_effect=RuntimeError("bucket unreachable"))
    )
    monkeypatch.setattr(endpoint, "_exec_inline_result", inline)
    compiled = SimpleNamespace(
        root_field="orders", sources={"pg"}, sql="SELECT 1", params=[], table_ids=()
    )
    state = SimpleNamespace(
        source_types={},
        source_dialects={},
        source_dsns={},
        response_cache_store=SimpleNamespace(stores_results=False),
        federation_engine=SimpleNamespace(writes_result=lambda fmt: fmt == "parquet"),
    )
    with pytest.raises(ApiError) as refused:
        await endpoint._execute_one_field(
            compiled,
            SimpleNamespace(),
            SimpleNamespace(has_rules=lambda: False),
            state,
            "analyst",
            "json",
            force_redirect=True,
            redirect_config=SimpleNamespace(),
            effective_redirect_format="parquet",
            probe_limit=None,
            response_cache_ttl=None,
            cache_opt_in=False,
        )
    assert (refused.value.status_code, refused.value.code) == (502, "data.redirect_failed")
    assert refused.value.params == {"error": "bucket unreachable"}
    inline.assert_not_awaited()
