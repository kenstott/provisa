# Copyright (c) 2026 Kenneth Stott
# Canary: 53bcd39d-e0ec-4e1c-8f76-4ecf3ab55775
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1433: the engine restart goes through the Docker socket, not a CLI the image does not carry.

Shelling out to ``docker`` failed with "docker not found on PATH" on every deployment: the app
image has no docker CLI, only the bind-mounted ``/var/run/docker.sock``. The name is also resolved
against what docker reports rather than assumed from the engine name, since compose runs the
engine as ``compose-trino-1``.
"""

from __future__ import annotations

import httpx
import pytest

import provisa.federation.isolated_provisioner as prov
from provisa.api.admin.settings_router import _resolve_engine_container, restart_query_engine
from provisa.api.errors import ApiError


class _Request:
    def __init__(self) -> None:
        self.state = type("S", (), {"identity": None, "active_org_id": "acme"})()


@pytest.fixture
def socket(tmp_path, monkeypatch):
    sock = tmp_path / "docker.sock"
    sock.write_text("")  # only its existence is probed; the transport is mocked
    monkeypatch.setenv("PROVISA_ISOLATED_ENGINE_DOCKER_SOCKET", str(sock))
    monkeypatch.delenv("QUERY_ENGINE_CONTAINER", raising=False)
    return sock


@pytest.fixture
def bound_engine(monkeypatch):
    """A no-op platform-settings gate and an engine named "trino" bound to app state."""
    import provisa.api.admin.settings_router as router
    import provisa.api.app as app_mod

    monkeypatch.setattr(app_mod, "state", type("S", (), {"federation_engine": _Engine("trino")})())
    monkeypatch.setattr(router, "require_platform_settings", lambda _request: None)


class _Engine:
    def __init__(self, name: str) -> None:
        self.name = name


def _mock_docker(monkeypatch, handler):
    calls: list[httpx.Request] = []

    def record(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return handler(request)

    def fake_client(_socket: str, **_kwargs) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(record), base_url=prov._DOCKER_BASE)

    monkeypatch.setattr(prov, "_client", fake_client)
    return calls


def _listing(names: list[list[str]]):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/containers/json"):
            return httpx.Response(200, json=[{"Names": n} for n in names])
        return httpx.Response(204)

    return handler


@pytest.mark.asyncio
async def test_restart_uses_the_socket_not_the_docker_cli(socket, bound_engine, monkeypatch):
    calls = _mock_docker(monkeypatch, _listing([["/compose-trino-1"], ["/compose-postgres-1"]]))

    result = await restart_query_engine(_Request())  # pyright: ignore[reportArgumentType]

    assert result == {"success": True, "container": "compose-trino-1"}
    assert calls[-1].method == "POST"
    assert calls[-1].url.path.endswith("/containers/compose-trino-1/restart")


@pytest.mark.asyncio
async def test_an_explicit_container_skips_discovery(socket, bound_engine, monkeypatch):
    calls = _mock_docker(monkeypatch, _listing([["/compose-trino-1"]]))

    result = await restart_query_engine(_Request(), container="my-trino")  # pyright: ignore[reportArgumentType]

    assert result["container"] == "my-trino"
    assert [c.url.path for c in calls] == ["/v1.43/containers/my-trino/restart"]


@pytest.mark.asyncio
async def test_the_env_pin_wins_over_discovery(socket, bound_engine, monkeypatch):
    monkeypatch.setenv("QUERY_ENGINE_CONTAINER", "pinned-engine")
    calls = _mock_docker(monkeypatch, _listing([["/compose-trino-1"]]))

    await restart_query_engine(_Request())  # pyright: ignore[reportArgumentType]

    assert [c.url.path for c in calls] == ["/v1.43/containers/pinned-engine/restart"]


@pytest.mark.asyncio
async def test_an_unmounted_socket_says_so_rather_than_failing_at_the_call(
    tmp_path, bound_engine, monkeypatch
):
    monkeypatch.setenv("PROVISA_ISOLATED_ENGINE_DOCKER_SOCKET", str(tmp_path / "absent.sock"))
    monkeypatch.delenv("QUERY_ENGINE_CONTAINER", raising=False)

    with pytest.raises(ApiError) as exc:
        await restart_query_engine(_Request())  # pyright: ignore[reportArgumentType]
    assert exc.value.status_code == 503


@pytest.mark.asyncio
async def test_a_missing_container_is_reported_as_missing(socket, bound_engine, monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"message": "no such container"})

    _mock_docker(monkeypatch, handler)

    with pytest.raises(ApiError) as exc:
        await restart_query_engine(_Request(), container="ghost")  # pyright: ignore[reportArgumentType]
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_resolution_matches_the_engine_as_a_name_component(monkeypatch):
    # "compose-trino-1" is the engine; "trinodb-docs" merely contains the word.
    calls = _mock_docker(monkeypatch, _listing([["/compose-trino-1"], ["/trinodb-docs"]]))
    assert calls is not None
    async with prov._client("/unused") as client:
        assert await _resolve_engine_container(client, "trino") == "compose-trino-1"


@pytest.mark.asyncio
async def test_two_candidates_are_refused_rather_than_guessed(monkeypatch):
    _mock_docker(monkeypatch, _listing([["/a-trino-1"], ["/b-trino-2"]]))
    async with prov._client("/unused") as client:
        with pytest.raises(ApiError) as exc:
            await _resolve_engine_container(client, "trino")
    assert exc.value.status_code == 400
    assert "a-trino-1" in str(exc.value.detail) and "b-trino-2" in str(exc.value.detail)


@pytest.mark.asyncio
async def test_no_candidate_names_the_engine_it_looked_for(monkeypatch):
    _mock_docker(monkeypatch, _listing([["/compose-postgres-1"]]))
    async with prov._client("/unused") as client:
        with pytest.raises(ApiError) as exc:
            await _resolve_engine_container(client, "trino")
    assert exc.value.status_code == 400
