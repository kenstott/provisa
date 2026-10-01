# Copyright (c) 2026 Kenneth Stott
# Canary: a45e8c20-6b17-4d93-9f0e-71c2d3b8a65f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The UI server's proxy timeout is an operator setting it reads from the API (REQ-1913).

The UI server is its own process with no control plane, so a value saved on the settings page
reaches it through the API: it asks for ``ui.proxy_timeout``, holds the answer for the same few
seconds a worker holds its settings snapshot, and applies the one resolution order — the stored
value, else its own environment, else what the API resolved."""

# Requirements: REQ-1913

from __future__ import annotations

import httpx
import pytest

from provisa import ui_server


@pytest.fixture
def api(monkeypatch):
    """Stand in for the API's answer; count the asks and control the clock."""
    calls: list[int] = []
    answer: dict = {"value": 480.0, "source": "default"}
    clock = [1000.0]

    async def _ask() -> dict:
        calls.append(1)
        if isinstance(answer.get("raise"), Exception):
            raise answer["raise"]
        return {"proxy_timeout": {"value": answer["value"], "source": answer["source"]}}

    monkeypatch.setattr(ui_server, "_ask_api_for_settings", _ask)
    monkeypatch.setattr(ui_server, "_settings_held", None)
    monkeypatch.setattr(ui_server.time, "monotonic", lambda: clock[0])
    monkeypatch.delenv("PROVISA_UI_PROXY_TIMEOUT", raising=False)
    return answer, calls, clock


async def test_with_nothing_set_the_timeout_is_what_the_api_resolved(api):
    assert await ui_server.proxy_timeout_s() == 480.0


async def test_the_ui_servers_own_environment_overrides_the_default(api, monkeypatch):
    monkeypatch.setenv("PROVISA_UI_PROXY_TIMEOUT", "120")
    assert await ui_server.proxy_timeout_s() == 120.0


async def test_the_stored_value_overrides_the_ui_servers_environment(api, monkeypatch):
    answer, _calls, _clock = api
    monkeypatch.setenv("PROVISA_UI_PROXY_TIMEOUT", "120")
    answer.update(value=30.0, source="stored")
    assert await ui_server.proxy_timeout_s() == 30.0


async def test_the_answer_is_held_for_the_snapshot_ttl(api):
    answer, calls, clock = api
    assert await ui_server.proxy_timeout_s() == 480.0
    answer.update(value=30.0, source="stored")
    clock[0] += ui_server.SETTINGS_TTL_SECONDS - 0.1
    assert await ui_server.proxy_timeout_s() == 480.0
    assert len(calls) == 1
    clock[0] += 0.2
    assert await ui_server.proxy_timeout_s() == 30.0
    assert len(calls) == 2


async def test_an_api_that_does_not_answer_a_refresh_leaves_the_held_value(api):
    answer, _calls, clock = api
    answer.update(value=30.0, source="stored")
    assert await ui_server.proxy_timeout_s() == 30.0
    answer["raise"] = httpx.ReadTimeout("busy")
    clock[0] += ui_server.SETTINGS_TTL_SECONDS + 1
    assert await ui_server.proxy_timeout_s() == 30.0


async def test_with_no_answer_yet_the_error_is_the_callers(api):
    """Nothing is held and the API is unreachable: there is no value to proxy with, and the
    request being proxied could not reach the API either."""
    answer, _calls, _clock = api
    answer["raise"] = httpx.ConnectError("refused")
    with pytest.raises(httpx.ConnectError):
        await ui_server.proxy_timeout_s()


async def test_a_bad_value_in_the_ui_servers_environment_names_the_variable(api, monkeypatch):
    monkeypatch.setenv("PROVISA_UI_PROXY_TIMEOUT", "soon")
    with pytest.raises(ValueError, match="PROVISA_UI_PROXY_TIMEOUT"):
        await ui_server.proxy_timeout_s()
