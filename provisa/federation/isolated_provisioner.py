# Copyright (c) 2026 Kenneth Stott
# Canary: 6a1d94c7-3b05-4f28-a7e1-0c92db4e5f31
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1416: bring an isolated org's dedicated federation-engine coordinator INTO EXISTENCE.

``isolated_engine_endpoint`` (engine.py) only NAMES the coordinator an isolated org routes to.
Naming without creating is what let onboarding offer "isolated engine" on a deployment where the
host resolved to nothing: the org bound a terminal at a hostname no container answered. This module
is the other half — it creates, starts, inspects and removes the container that answers there.

The name is not invented here. The container name and its network alias are both
``isolated_engine_endpoint(org_id)[0]``, so the thing that resolves the host and the thing that
creates it cannot drift apart.

The engine runs as a single-node Trino coordinator on the deployment's own container runtime,
reached over the Docker Engine API on a Unix socket. Catalogs are NOT files here: the coordinator
is started with ``catalog.management=dynamic`` and the org's runtime issues ``CREATE CATALOG`` on
its own terminal, so a fresh coordinator needs no per-source config on disk.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import tarfile
from typing import Any

import httpx

log = logging.getLogger(__name__)

# Docker Engine API over the mounted socket. The host part of the URL is ignored by the UDS
# transport; Docker requires a Host header, and "docker" is the conventional placeholder.
_DOCKER_BASE = "http://docker/v1.43"

_CONFIG_PROPERTIES = """coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port={port}
discovery.uri=http://localhost:{port}
catalog.management=dynamic
query.max-memory={query_max_memory}
query.max-memory-per-node={query_max_memory_per_node}
"""

# Deliberately NOT the shared cluster's jvm.config: that one loads the OpenTelemetry javaagent from
# /etc/trino/otel, a path only the compose mount provides. A dedicated coordinator has no such
# mount, and a missing -javaagent jar aborts the JVM before Trino logs anything.
_JVM_CONFIG = """-server
-XX:InitialRAMPercentage=70
-XX:MaxRAMPercentage=70
-XX:+ExitOnOutOfMemoryError
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-Djdk.attach.allowAttachSelf=true
--add-modules=jdk.incubator.vector
"""


class IsolatedProvisioningError(RuntimeError):
    """The dedicated coordinator could not be created, started, or reached."""


def _socket_path() -> str:
    return os.environ.get("PROVISA_ISOLATED_ENGINE_DOCKER_SOCKET", "/var/run/docker.sock")


def provisioner_settings() -> dict[str, str]:
    """The deployment settings that let this process create a dedicated coordinator.

    Raises when incomplete rather than guessing: an image tag or a container network cannot be
    inferred, and creating the coordinator on the wrong network produces one that starts cleanly
    and is unreachable from the app — the failure mode that is hardest to see.
    """
    image = os.environ.get("PROVISA_ISOLATED_ENGINE_IMAGE")
    network = os.environ.get("PROVISA_ISOLATED_ENGINE_NETWORK")
    missing = [
        name
        for name, value in (
            ("PROVISA_ISOLATED_ENGINE_IMAGE", image),
            ("PROVISA_ISOLATED_ENGINE_NETWORK", network),
        )
        if not value
    ]
    if missing:
        raise IsolatedProvisioningError(
            "this deployment cannot provision a dedicated federation engine: "
            f"{', '.join(missing)} unset"
        )
    assert image is not None and network is not None
    return {
        "image": image,
        "network": network,
        "memory": os.environ.get("PROVISA_ISOLATED_ENGINE_MEMORY", "4g"),
        "socket": _socket_path(),
    }


def provisioning_available() -> bool:
    """Whether this process can actually CREATE a dedicated coordinator.

    Distinct from ``PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE``, which only resolves where one would
    live. A deployment that runs its per-org coordinators out of band (Kubernetes, a separate
    control plane) resolves hosts without granting this process a container runtime.
    """
    try:
        provisioner_settings()
    except IsolatedProvisioningError:
        return False
    return os.path.exists(_socket_path())


def container_name(org_id: str) -> str:
    """The dedicated coordinator's container name — the same string the org's terminal dials."""
    from provisa.federation.engine import isolated_engine_endpoint

    return isolated_engine_endpoint(org_id)[0]


def _memory_bytes(spec: str) -> int:
    units = {"k": 1024, "m": 1024**2, "g": 1024**3}
    text = spec.strip().lower().rstrip("b")
    if text and text[-1] in units:
        return int(float(text[:-1]) * units[text[-1]])
    return int(text)


def _query_budget_gb(memory_bytes: int) -> int:
    """What one query may claim on a coordinator limited to ``memory_bytes`` (REQ-1449).

    A fraction of the JVM HEAP, not of the container limit: jvm.config sets MaxRAMPercentage=70, and
    Trino reserves a further 30% of the heap as memory.heap-headroom-per-node, so the most a query
    can be given is 0.7 x 0.7 = 0.49 of the limit. The earlier 60%/30% pair broke both ends of that
    — it asked for more than the heap could honour, and it set the per-node bound at HALF the
    cluster bound on a deployment that is one container, so a query was killed at half its stated
    budget with no second node to absorb the difference. Both bounds name the same pool here.
    """
    return max(1, int(memory_bytes / 1024**3 * 0.7 * 0.5))


def _config_archive(port: int, query_max_memory_gb: int) -> bytes:
    """A tar of the coordinator's etc files, for upload into the created container.

    Uploading beats a bind mount: a bind mount's source path is on the DOCKER HOST, which this
    process only sees through the socket — writing it from inside the app container would put the
    files somewhere the daemon does not look.
    """
    files = {
        "config.properties": _CONFIG_PROPERTIES.format(
            port=port,
            query_max_memory=f"{query_max_memory_gb}GB",
            query_max_memory_per_node=f"{query_max_memory_gb}GB",
        ),
        "jvm.config": _JVM_CONFIG,
    }
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for name, body in files.items():
            data = body.encode()
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            info.mode = 0o644
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _client(socket: str) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.AsyncHTTPTransport(uds=socket), base_url=_DOCKER_BASE, timeout=60.0
    )


async def _inspect(client: httpx.AsyncClient, name: str) -> dict | None:
    resp = await client.get(f"/containers/{name}/json")
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


async def engine_status(org_id: str) -> dict:
    """What exists for this org right now: ``state`` is docker's own container state, or
    ``"absent"`` when nothing has been created."""
    settings = provisioner_settings()
    name = container_name(org_id)
    async with _client(settings["socket"]) as client:
        info = await _inspect(client, name)
    if info is None:
        return {"container": name, "state": "absent"}
    return {
        "container": name,
        "state": info["State"]["Status"],
        "image": info["Config"]["Image"],
        "started_at": info["State"].get("StartedAt"),
    }


async def _wait_until_ready(host: str, port: int, timeout: float) -> None:
    """Poll the coordinator's own readiness endpoint until it reports it has finished starting."""
    deadline = asyncio.get_running_loop().time() + timeout
    last = "no response"
    async with httpx.AsyncClient(timeout=5.0) as probe:
        while True:  # always probe at least once — the timeout bounds retries, not the attempt
            try:
                resp = await probe.get(f"http://{host}:{port}/v1/info")
                if resp.status_code == 200 and not resp.json().get("starting", True):
                    return
                last = f"HTTP {resp.status_code}"
            except httpx.HTTPError as exc:
                last = str(exc)
            if asyncio.get_running_loop().time() >= deadline:
                break
            await asyncio.sleep(2.0)
    raise IsolatedProvisioningError(
        f"dedicated coordinator {host}:{port} did not become ready within {timeout:.0f}s ({last})"
    )


async def provision_isolated_engine(org_id: str, wait: bool = True, size: Any = None) -> dict:
    """Create and start the org's dedicated coordinator, and (by default) wait until it answers.

    Idempotent: an existing container is started if stopped and left alone if already running, so
    an org that is re-provisioned or moved back onto the isolated lane reuses its coordinator.

    ``size`` is the org's plan-fixed engine size (REQ-1449), resolved by the caller through
    ``provisa.core.commerce.engine_size_for_org`` — the plan vocabulary stays in the commercial
    plugin and only ``pod_cpu`` / ``pod_memory_gib`` cross into here. It bounds CPU as well as
    memory, which the deployment-wide settings cannot: a size IS a vCPU count, and a coordinator
    with no NanoCpus takes as much of the node as it can find no matter which size was sold.

    ``None`` is the deployment that does not size engines by plan — a self-hosted install, or an
    org an operator placed on the isolated lane without a plan that sells one. Those use
    PROVISA_ISOLATED_ENGINE_MEMORY, which is what every org used before sizes existed.
    """
    from provisa.federation.engine import isolated_engine_endpoint

    settings = provisioner_settings()
    host, port = isolated_engine_endpoint(org_id)
    name = host
    memory_bytes = (
        size.pod_memory_gib * 1024**3 if size is not None else _memory_bytes(settings["memory"])
    )
    budget_gb = size.query_max_memory_gb if size is not None else _query_budget_gb(memory_bytes)
    host_config: dict[str, Any] = {
        "Memory": memory_bytes,
        "RestartPolicy": {"Name": "unless-stopped"},
        "NetworkMode": settings["network"],
    }
    if size is not None:
        host_config["NanoCpus"] = int(float(size.pod_cpu) * 1_000_000_000)
    async with _client(settings["socket"]) as client:
        info = await _inspect(client, name)
        if info is None:
            body = {
                "Image": settings["image"],
                "Hostname": name,
                "Labels": {
                    "dev.provisa.role": "isolated-federation-engine",
                    "dev.provisa.org": org_id,
                },
                "ExposedPorts": {f"{port}/tcp": {}},
                "HostConfig": host_config,
                "NetworkingConfig": {"EndpointsConfig": {settings["network"]: {"Aliases": [name]}}},
            }
            created = await client.post("/containers/create", params={"name": name}, json=body)
            if created.status_code == 404:
                raise IsolatedProvisioningError(
                    f"image {settings['image']!r} is not present on the node and this process does "
                    "not pull images; pre-pull it and retry"
                )
            created.raise_for_status()
            cid = created.json()["Id"]
            archive = await client.put(
                f"/containers/{cid}/archive",
                params={"path": "/etc/trino"},
                content=_config_archive(port, budget_gb),
                headers={"Content-Type": "application/x-tar"},
            )
            archive.raise_for_status()
            log.info("created dedicated federation engine %s for org %s", name, org_id)
        started = await client.post(f"/containers/{name}/start")
        # 304 = already running, which is the idempotent case, not an error.
        if started.status_code != 304:
            started.raise_for_status()
    if wait:
        await _wait_until_ready(
            host, port, float(os.environ.get("PROVISA_ISOLATED_ENGINE_READY_TIMEOUT", "300"))
        )
    return {"container": name, "host": host, "port": port}


async def deprovision_isolated_engine(org_id: str) -> dict:
    """Stop and remove the org's dedicated coordinator.

    Removing loses nothing the org owns: a coordinator holds no data, and its catalogs are issued
    from the org's config every time its runtime is built.
    """
    settings = provisioner_settings()
    name = container_name(org_id)
    async with _client(settings["socket"]) as client:
        removed = await client.delete(f"/containers/{name}", params={"force": "true", "v": "true"})
        if removed.status_code == 404:
            return {"container": name, "state": "absent"}
        removed.raise_for_status()
    log.info("removed dedicated federation engine %s for org %s", name, org_id)
    return {"container": name, "state": "removed"}


def docker_error_detail(exc: Exception) -> str:
    """Docker's own message out of an HTTP error, which says far more than the status line."""
    if isinstance(exc, httpx.HTTPStatusError):
        try:
            return json.loads(exc.response.content).get("message", exc.response.text)
        except (ValueError, AttributeError):
            return exc.response.text
    return str(exc)
