# Copyright (c) 2026 Kenneth Stott
# Canary: 537dfe97-fc23-48e0-b4c9-35177c7e7e31
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The engine watchdog restarts the trino container this process connects to, found by the port
it publishes -- never a fixed ``provisa-trino-1``, which names only one compose project."""

from __future__ import annotations

import json

import pytest

from provisa.federation import trino_lifecycle

pytestmark = pytest.mark.asyncio


def _fake_docker(containers: dict[str, dict], calls: list[tuple[str, ...]]):
    """``containers``: name -> PortBindings, as ``docker inspect`` reports them."""

    async def _docker(*args: str) -> str:
        calls.append(args)
        if args[0] == "ps":
            return "\n".join(f"id-{n}" for n in containers) + "\n"
        if args[0] == "inspect":
            return "".join(
                f"/{n}\t{json.dumps(containers[n])}\n"
                for n in (a.removeprefix("id-") for a in args[3:])
            )
        if args[0] == "start":
            return args[1] + "\n"
        raise AssertionError(args)

    return _docker


def _binding(port: int) -> dict:
    return {"8080/tcp": [{"HostIp": "", "HostPort": str(port)}]}


async def test_the_container_publishing_the_engine_port_is_found_whatever_its_project(
    monkeypatch,
):
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(
        trino_lifecycle,
        "_docker",
        _fake_docker(
            {"provisa-trino-1": _binding(8080), "ci-wt-itest-trino-1": _binding(18080)}, calls
        ),
    )
    assert await trino_lifecycle._engine_container(18080) == "ci-wt-itest-trino-1"
    assert calls[0] == ("ps", "-aq", "--filter", "label=com.docker.compose.service=trino")


@pytest.mark.parametrize(
    "containers",
    [{}, {"other-trino-1": _binding(8080)}, {"a-trino-1": _binding(9), "b-trino-1": _binding(9)}],
    ids=["none", "another-port", "ambiguous"],
)
async def test_no_single_match_is_a_lookup_error(monkeypatch, containers):
    monkeypatch.setattr(trino_lifecycle, "_docker", _fake_docker(containers, []))
    with pytest.raises(LookupError):
        await trino_lifecycle._engine_container(9)


async def test_watchdog_starts_the_found_container(monkeypatch):
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(
        trino_lifecycle, "_docker", _fake_docker({"proj-x-trino-1": _binding(18080)}, calls)
    )

    def _dead(_conn):
        raise ConnectionError("engine down")

    monkeypatch.setattr(trino_lifecycle, "_ping", _dead)

    class _State:
        engine_conn = object()
        engine_conn_kwargs = {"host": "localhost", "port": 18080}

    async def _no_wait(_seconds):
        raise _Stop

    class _Stop(Exception):
        pass

    monkeypatch.setattr(trino_lifecycle.asyncio, "sleep", _no_wait)
    with pytest.raises(_Stop):  # stops at the wait for health that follows the start
        await trino_lifecycle.watchdog(_State())
    assert ("start", "proj-x-trino-1") in calls


async def test_a_host_without_docker_is_a_lookup_error(monkeypatch):
    async def _missing(*_a, **_k):
        raise FileNotFoundError("docker")

    monkeypatch.setattr(trino_lifecycle.asyncio, "create_subprocess_exec", _missing)
    with pytest.raises(LookupError, match="docker CLI"):
        await trino_lifecycle._engine_container(8080)
