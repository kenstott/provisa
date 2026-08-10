# Copyright (c) 2026 Kenneth Stott
# Canary: 5e73a0c8-14bd-4c96-8f2a-6d31b90ae4c7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1416: the SaaS actually CREATES an isolated org's dedicated coordinator.

Before this, ``isolated_engine_endpoint`` resolved a hostname and nothing in the product ever
brought up something that answered there — onboarding offered an isolated engine the platform
could not deliver. What is asserted here is the join between the two: the container the
provisioner creates carries exactly the name the org's terminal will dial, and it is created on
the network the app can reach.

The Docker daemon is replaced by an httpx MockTransport that records the requests, so the calls
are asserted against the Engine API's real shapes without a daemon.
"""

from __future__ import annotations

import io
import json
import tarfile

import httpx
import pytest

from provisa.federation import isolated_provisioner as prov
from provisa.federation.engine import isolated_engine_endpoint

_TEMPLATE = "provisa-trino-{org_id}"


@pytest.fixture()
def configured(monkeypatch, tmp_path):
    sock = tmp_path / "docker.sock"
    sock.write_text("")  # only its existence is probed; the transport is mocked
    monkeypatch.setenv("PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE", _TEMPLATE)
    monkeypatch.setenv("PROVISA_ISOLATED_ENGINE_PORT", "8080")
    monkeypatch.setenv("PROVISA_ISOLATED_ENGINE_IMAGE", "trinodb/trino:481")
    monkeypatch.setenv("PROVISA_ISOLATED_ENGINE_NETWORK", "compose_default")
    monkeypatch.setenv("PROVISA_ISOLATED_ENGINE_MEMORY", "4g")
    monkeypatch.setenv("PROVISA_ISOLATED_ENGINE_DOCKER_SOCKET", str(sock))
    return sock


def _mock_docker(monkeypatch, handler):
    """Point the provisioner's Docker client at a recorded MockTransport."""
    calls: list[httpx.Request] = []

    def record(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return handler(request)

    def fake_client(_socket: str) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.MockTransport(record), base_url=prov._DOCKER_BASE
        )

    monkeypatch.setattr(prov, "_client", fake_client)
    return calls


# ---- availability ------------------------------------------------------------


def test_provisioning_needs_an_image_and_a_network(monkeypatch, configured):
    assert prov.provisioning_available() is True
    monkeypatch.delenv("PROVISA_ISOLATED_ENGINE_NETWORK")
    assert prov.provisioning_available() is False


def test_missing_settings_name_themselves(monkeypatch, configured):
    monkeypatch.delenv("PROVISA_ISOLATED_ENGINE_IMAGE")
    with pytest.raises(prov.IsolatedProvisioningError) as excinfo:
        prov.provisioner_settings()
    assert "PROVISA_ISOLATED_ENGINE_IMAGE" in str(excinfo.value)


def test_provisioning_is_unavailable_without_a_container_runtime(monkeypatch, configured):
    monkeypatch.setenv("PROVISA_ISOLATED_ENGINE_DOCKER_SOCKET", "/nonexistent/docker.sock")
    assert prov.provisioning_available() is False


# ---- the name is the join ----------------------------------------------------


def test_container_is_named_exactly_what_the_org_terminal_dials(configured):
    """The whole point: one derivation. If these two ever disagree the org binds a terminal at a
    hostname no container answers — the failure this module exists to remove."""
    assert prov.container_name("acme") == isolated_engine_endpoint("acme")[0] == "provisa-trino-acme"


# ---- container spec ----------------------------------------------------------


@pytest.mark.asyncio
async def test_creates_the_coordinator_on_the_reachable_network(monkeypatch, configured):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/containers/provisa-trino-acme/json"):
            return httpx.Response(404, json={"message": "no such container"})
        if request.url.path.endswith("/containers/create"):
            return httpx.Response(201, json={"Id": "deadbeef"})
        if "/archive" in request.url.path:
            return httpx.Response(200)
        if request.url.path.endswith("/start"):
            return httpx.Response(204)
        raise AssertionError(f"unexpected docker call {request.method} {request.url}")

    calls = _mock_docker(monkeypatch, handler)
    result = await prov.provision_isolated_engine("acme", wait=False)

    assert result == {"container": "provisa-trino-acme", "host": "provisa-trino-acme", "port": 8080}
    create = next(c for c in calls if c.url.path.endswith("/containers/create"))
    assert create.url.params["name"] == "provisa-trino-acme"
    body = json.loads(create.content)
    assert body["Image"] == "trinodb/trino:481"
    assert body["HostConfig"]["NetworkMode"] == "compose_default"
    # The alias is what makes the name resolve for the app container. Without it the coordinator
    # is only reachable by container name on the default bridge, which the app is not on.
    assert body["NetworkingConfig"]["EndpointsConfig"]["compose_default"]["Aliases"] == [
        "provisa-trino-acme"
    ]
    assert body["HostConfig"]["Memory"] == 4 * 1024**3
    assert body["Labels"]["dev.provisa.org"] == "acme"


@pytest.mark.asyncio
async def test_uploaded_config_makes_it_a_dynamic_catalog_coordinator(monkeypatch, configured):
    """A fresh coordinator gets no catalog FILES — the org's runtime issues CREATE CATALOG on it,
    which only works with dynamic catalog management."""
    uploaded: dict[str, bytes] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/json"):
            return httpx.Response(404, json={"message": "absent"})
        if request.url.path.endswith("/containers/create"):
            return httpx.Response(201, json={"Id": "deadbeef"})
        if "/archive" in request.url.path:
            uploaded["path"] = request.url.params["path"].encode()
            uploaded["tar"] = request.content
            return httpx.Response(200)
        return httpx.Response(204)

    _mock_docker(monkeypatch, handler)
    await prov.provision_isolated_engine("acme", wait=False)

    assert uploaded["path"] == b"/etc/trino"
    tar = tarfile.open(fileobj=io.BytesIO(uploaded["tar"]))
    assert sorted(tar.getnames()) == ["config.properties", "jvm.config"]
    config = tar.extractfile("config.properties").read().decode()  # type: ignore[union-attr]
    assert "catalog.management=dynamic" in config
    assert "coordinator=true" in config
    assert "node-scheduler.include-coordinator=true" in config
    assert "http-server.http.port=8080" in config
    # The shared cluster's jvm.config loads an OTel javaagent from a compose-mounted path that a
    # dedicated coordinator does not have; carrying it over aborts the JVM before Trino logs.
    assert "javaagent" not in tar.extractfile("jvm.config").read().decode()  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_reprovisioning_reuses_the_existing_coordinator(monkeypatch, configured):
    """An org moved off the isolated lane and back, or re-provisioned after a failure, must not
    collide on the container name."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/json"):
            return httpx.Response(
                200, json={"State": {"Status": "exited"}, "Config": {"Image": "trinodb/trino:481"}}
            )
        if request.url.path.endswith("/start"):
            return httpx.Response(204)
        raise AssertionError(f"unexpected docker call {request.method} {request.url}")

    calls = _mock_docker(monkeypatch, handler)
    await prov.provision_isolated_engine("acme", wait=False)
    assert [c.url.path for c in calls if c.url.path.endswith("/containers/create")] == []


@pytest.mark.asyncio
async def test_an_already_running_coordinator_is_not_an_error(monkeypatch, configured):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/json"):
            return httpx.Response(
                200, json={"State": {"Status": "running"}, "Config": {"Image": "x"}}
            )
        return httpx.Response(304)  # docker's "container already started"

    _mock_docker(monkeypatch, handler)
    await prov.provision_isolated_engine("acme", wait=False)


@pytest.mark.asyncio
async def test_status_reports_absent_before_anything_is_created(monkeypatch, configured):
    _mock_docker(monkeypatch, lambda _r: httpx.Response(404, json={"message": "absent"}))
    assert await prov.engine_status("acme") == {
        "container": "provisa-trino-acme",
        "state": "absent",
    }


@pytest.mark.asyncio
async def test_deprovision_removes_the_container(monkeypatch, configured):
    calls = _mock_docker(monkeypatch, lambda _r: httpx.Response(204))
    assert await prov.deprovision_isolated_engine("acme") == {
        "container": "provisa-trino-acme",
        "state": "removed",
    }
    assert calls[0].method == "DELETE"
    assert calls[0].url.params["force"] == "true"


@pytest.mark.asyncio
async def test_deprovisioning_something_absent_is_not_an_error(monkeypatch, configured):
    _mock_docker(monkeypatch, lambda _r: httpx.Response(404, json={"message": "absent"}))
    assert (await prov.deprovision_isolated_engine("acme"))["state"] == "absent"


# ---- readiness ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_readiness_wait_gives_up_with_the_last_thing_it_saw(monkeypatch, configured):
    async def never_ready(_self, _url, **_kw):
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(httpx.AsyncClient, "get", never_ready)
    with pytest.raises(prov.IsolatedProvisioningError) as excinfo:
        await prov._wait_until_ready("provisa-trino-acme", 8080, timeout=0.01)
    assert "connection refused" in str(excinfo.value)


def test_memory_specs_parse_the_way_compose_writes_them():
    assert prov._memory_bytes("4g") == 4 * 1024**3
    assert prov._memory_bytes("512m") == 512 * 1024**2
    assert prov._memory_bytes("2gb") == 2 * 1024**3
    assert prov._memory_bytes("1073741824") == 1024**3
