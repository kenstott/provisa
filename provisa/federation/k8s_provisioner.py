# Copyright (c) 2026 Kenneth Stott
# Canary: 5b2e7f01-9c44-4d6a-b3e8-71a0c5d2ef93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1447/REQ-1448/REQ-1450: bring a federation engine into existence on Kubernetes.

The sibling of :mod:`provisa.federation.isolated_provisioner`, which creates a coordinator as a
container on the control plane's OWN Docker socket. That co-tenancy is the thing being retired: the
container bounds ``Memory`` but never ``NanoCpus``, and it runs with
``node-scheduler.include-coordinator=true``, so an org's scan executes on the node the control plane
is serving every other org from.

Here an engine is a Deployment on a node pool that hosts Trino and nothing else. The SHARED (Starter)
lane and the ISOLATED (Pro) lane are the same manifests from the same image; they differ only in
tenancy and in which pool they schedule onto. This module currently implements the shared lane —
:func:`ensure_shard_running` and its shard vocabulary — and the isolated lane lands on the same
primitives by passing a per-org shard name and pool.

Two things are deliberately not the way the Docker provisioner did them:

* **Readiness is ``readyReplicas``, not ``/v1/info``.** The old poll returned the moment the
  coordinator process answered ``starting=false``, which says nothing about whether the pod passed
  its readiness gate and entered the Service's endpoint set. A query released on the process's own
  word is dispatched at a Service that still routes to nothing.
* **Scale-in drains.** ``terminationGracePeriodSeconds`` covers the longest permitted query so that
  Trino's own SHUTTING_DOWN drain can finish it; a pod killed at the default 30s cuts it.

Authentication is the VM's attached service account, read from the GCE metadata server. The
control-plane VM is the only caller, and its IAM role carries exactly the container verbs it needs
(``terraform/gcp-saas/gke.tf``) — a narrower grant than the mounted Docker socket it replaces.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import ssl
import tempfile
import time
from pathlib import Path

import httpx

log = logging.getLogger(__name__)

_METADATA_TOKEN_URL = (
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
)

# The engine's config. Unlike the Docker provisioner's copy, this one is IDENTICAL for both lanes —
# the shared shard sets a resource-groups manager on top, and nothing else differs.
#
# include-coordinator is TRUE. It is false on the deployment's multi-node cluster, and it was false
# in the Docker provisioner for one reason: the container ran on the control plane's own node, so a
# tenant scan executing on the coordinator executed on the node serving every other org. On a
# dedicated node pool that reason is gone, and the setting inverts: a shard is a single pod, so with
# it false there is no node in the cluster willing to take a split and every query would sit
# queued forever. Growing the shared lane adds shards, not workers.
_CONFIG_PROPERTIES = """coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port={port}
discovery.uri=http://localhost:{port}
catalog.management=dynamic
query.max-memory={query_max_memory}
query.max-memory-per-node={query_max_memory_per_node}
"""

# Trino's OTel exporter has no "drop if unreachable" mode: pointed at a collector that does not
# exist, the coordinator retries every span export for the life of the process. Both this and the
# jvm.config block below are therefore rendered only when an endpoint is configured — the same rule
# provisa/api/trino_setup.py applies to the shared cluster.
_TRACING_PROPERTIES = """tracing.enabled=true
otel.exporter.endpoint={otlp_endpoint}
"""

# The agent jar is in the image (docker/trino-engine.Dockerfile), so this is the shared cluster's
# jvm.config verbatim rather than a stripped copy — the divergence the Docker provisioner had to
# carry is gone.
_JVM_CONFIG = """-server
-XX:InitialRAMPercentage=70
-XX:MaxRAMPercentage=70
-XX:+ExitOnOutOfMemoryError
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-Djdk.attach.allowAttachSelf=true
--add-modules=jdk.incubator.vector
-javaagent:/etc/trino/otel/opentelemetry-javaagent.jar
"""

# The agent is loaded for its OTLP exporter, which Trino's own tracing writes through; its
# auto-instrumentation is off, which is how the shared cluster runs it.
_JVM_OTEL = """-Dotel.javaagent.enabled=false
-Dotel.service.name=federation-engine
-Dotel.exporter.otlp.endpoint={otlp_endpoint}
-Dotel.exporter.otlp.protocol=grpc
"""


class K8sProvisioningError(RuntimeError):
    """An engine could not be created, scaled, or reached on the cluster."""


# ── Settings ────────────────────────────────────────────────────────────────────


def provisioner_settings() -> dict[str, str]:
    """The deployment settings that let this process create an engine on the cluster.

    Raises when incomplete rather than guessing. A cluster name or a project cannot be inferred, and
    pointing at the wrong one yields a provisioner that succeeds against a cluster no query will
    ever reach — the failure mode that is hardest to see.
    """
    names = {
        "project": "PROVISA_ENGINE_CLUSTER_PROJECT",
        "location": "PROVISA_ENGINE_CLUSTER_LOCATION",
        "cluster": "PROVISA_ENGINE_CLUSTER_NAME",
        "image": "PROVISA_ENGINE_IMAGE",
        # Required, not defaulted to cluster.local: the control plane is a VM, not a pod, and a VM
        # cannot resolve the in-cluster domain at all. GKE publishes Service records VPC-wide under
        # a domain unique to the cluster (Cloud DNS, VPC_SCOPE), and getting it wrong yields engines
        # that come up healthy and that nothing can dial (REQ-1451).
        "dns_domain": "PROVISA_ENGINE_CLUSTER_DNS_DOMAIN",
    }
    found = {key: os.environ.get(env) for key, env in names.items()}
    missing = sorted(names[key] for key, value in found.items() if not value)
    if missing:
        raise K8sProvisioningError(
            "this deployment cannot provision a federation engine on Kubernetes: "
            f"{', '.join(missing)} unset"
        )
    settings = {key: value for key, value in found.items() if value is not None}
    settings["namespace"] = os.environ.get("PROVISA_ENGINE_NAMESPACE", "provisa-engines")
    return settings


def provisioning_available() -> bool:
    """Whether this process can actually CREATE an engine.

    Distinct from ``PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE``, which only resolves where one would
    live. A deployment can route to engines somebody else operates.
    """
    try:
        provisioner_settings()
    except K8sProvisioningError:
        return False
    return True


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return int(raw)


# ── Credentials ─────────────────────────────────────────────────────────────────

_token_cache: tuple[str, float] = ("", 0.0)
_cluster_cache: dict[str, tuple[str, str]] = {}


async def _access_token() -> str:
    """An OAuth token for the VM's attached service account, from the metadata server.

    Cached until a minute before expiry — every provisioning call would otherwise pay a metadata
    round trip, and the wake path is already on the query's latency budget.
    """
    global _token_cache
    token, expires_at = _token_cache
    if token and time.time() < expires_at - 60:
        return token
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(_METADATA_TOKEN_URL, headers={"Metadata-Flavor": "Google"})
        resp.raise_for_status()
        payload = resp.json()
    _token_cache = (payload["access_token"], time.time() + payload["expires_in"])
    return _token_cache[0]


def _client(verify: ssl.SSLContext | bool = True) -> httpx.AsyncClient:
    """The single seam every outbound API call goes through, so a test can swap the transport."""
    return httpx.AsyncClient(verify=verify, timeout=60.0)


async def _container_api(method: str, path: str, body: dict | None = None) -> dict:
    """A call against the GKE control-plane API (clusters, node pools, operations)."""
    settings = provisioner_settings()
    base = (
        f"https://container.googleapis.com/v1/projects/{settings['project']}"
        f"/locations/{settings['location']}"
    )
    token = await _access_token()
    async with _client() as client:
        resp = await client.request(
            method, f"{base}/{path}", json=body, headers={"Authorization": f"Bearer {token}"}
        )
        if resp.status_code >= 400:
            raise K8sProvisioningError(f"GKE API {method} {path} failed: {gcp_error_detail(resp)}")
        return resp.json()


async def _cluster_access() -> tuple[str, str]:
    """``(endpoint, ca_cert_path)`` for the engine cluster's Kubernetes API.

    The CA is written to a file because ``ssl`` loads a trust root from a path, not from memory. It
    is cached for the process: the cluster's CA does not rotate on the timescale of a query.
    """
    settings = provisioner_settings()
    key = f"{settings['project']}/{settings['location']}/{settings['cluster']}"
    cached = _cluster_cache.get(key)
    if cached is not None:
        return cached
    info = await _container_api("GET", f"clusters/{settings['cluster']}")
    ca_pem = base64.b64decode(info["masterAuth"]["clusterCaCertificate"])
    handle = tempfile.NamedTemporaryFile(  # noqa: SIM115 — lives as long as the process does
        prefix="provisa-engine-ca-", suffix=".pem", delete=False
    )
    with handle as fh:
        fh.write(ca_pem)
    access = (info["endpoint"], handle.name)
    _cluster_cache[key] = access
    return access


async def _k8s(
    method: str, path: str, body: dict | None = None, *, content_type: str | None = None
) -> httpx.Response:
    """A call against the engine cluster's Kubernetes API, as the VM's service account."""
    endpoint, ca_path = await _cluster_access()
    token = await _access_token()
    headers = {"Authorization": f"Bearer {token}"}
    if content_type:
        headers["Content-Type"] = content_type
    ctx = ssl.create_default_context(cafile=ca_path)
    # The API server's certificate names the cluster's IP, not a hostname.
    ctx.check_hostname = False
    async with _client(verify=ctx) as client:
        return await client.request(method, f"https://{endpoint}{path}", json=body, headers=headers)


async def _k8s_apply(path: str, manifest: dict) -> dict:
    """Server-side apply, so re-provisioning converges an existing object instead of conflicting.

    ``force=true`` claims fields a previous manager owns: this provisioner is the only writer of
    these objects, and a stale field manager left by an earlier version would otherwise wedge every
    apply behind a conflict no operator is watching for.
    """
    resp = await _k8s(
        "PATCH",
        f"{path}?fieldManager=provisa-control-plane&force=true",
        manifest,
        content_type="application/apply-patch+yaml",
    )
    if resp.status_code >= 400:
        raise K8sProvisioningError(f"applying {path} failed: {gcp_error_detail(resp)}")
    return resp.json()


# ── Naming ──────────────────────────────────────────────────────────────────────


def shard_workload_name(shard: str) -> str:
    """The Deployment/Service name for a shard.

    ``orgs.shard`` holds ``shared_1``; Kubernetes object names are DNS labels and reject the
    underscore, so the two spellings differ by exactly this translation and nowhere else.
    """
    return f"trino-{shard.replace('_', '-')}"


def shard_node_pool(shard: str) -> str:
    """The node pool a shard schedules onto — one pool per shard, named the same way."""
    return shard.replace("_", "-")


def shard_endpoint(shard: str) -> tuple[str, int]:
    """``(host, port)`` the control plane dials for a shard, as Cloud DNS publishes it VPC-wide."""
    settings = provisioner_settings()
    return (
        f"{shard_workload_name(shard)}.{settings['namespace']}.svc.{settings['dns_domain']}",
        _int_env("PROVISA_ENGINE_PORT", 8080),
    )


# ── Manifests ───────────────────────────────────────────────────────────────────


def _memory_gib() -> int:
    return _int_env("PROVISA_ENGINE_MEMORY_GIB", 24)


def shared_resource_groups() -> str:
    """The shared lane's queue policy, verbatim (REQ-1450).

    The same file the shared cluster mounts, not a second copy authored here: the ``tenant-${USER}``
    subgroup and its selector are what keep one Starter org from taking the shard's whole concurrency
    budget, and a divergent copy would be a policy change nobody made.
    """
    path = Path(__file__).resolve().parents[2] / "trino" / "etc" / "resource-groups.json"
    return path.read_text(encoding="utf-8")


def _config_data(resource_groups: str | None) -> dict[str, str]:
    port = _int_env("PROVISA_ENGINE_PORT", 8080)
    heap = _memory_gib()
    otlp_endpoint = os.environ.get("PROVISA_ENGINE_OTLP_ENDPOINT", "").strip()
    config = _CONFIG_PROPERTIES.format(
        port=port,
        # A shard is one pod, so the cluster bound and the per-node bound are the same bound;
        # splitting them 60/30 would leave a query rejected for exceeding a limit no other node
        # is there to absorb.
        #
        # The budget is a fraction of the JVM HEAP, not of the pod limit. -XX:MaxRAMPercentage=70
        # above gives the heap 70% of the limit, and Trino reserves a further 30% of the heap as
        # memory.heap-headroom-per-node, so the ceiling a query may claim is 0.7 × 0.7 = 0.49 of
        # the pod's memory. Taking 0.6 of the pod limit blew straight through it: a 24 GiB pod
        # asked for 14 GB per node against a 16.8 GiB heap with 5.0 GiB of headroom, and Trino
        # refused to start — "The sum of max query memory per node and heap headroom cannot be
        # larger than the available heap memory". 0.7 heap × 0.5 leaves the default headroom
        # intact with room to spare.
        query_max_memory=f"{max(1, int(heap * 0.7 * 0.5))}GB",
        query_max_memory_per_node=f"{max(1, int(heap * 0.7 * 0.5))}GB",
    )
    jvm = _JVM_CONFIG
    if otlp_endpoint:
        config += _TRACING_PROPERTIES.format(otlp_endpoint=otlp_endpoint)
        jvm += _JVM_OTEL.format(otlp_endpoint=otlp_endpoint)
    data = {
        "config.properties": config,
        "jvm.config": jvm,
        "node.properties": "node.environment=provisa\nnode.data-dir=/data/trino\n",
    }
    if resource_groups is not None:
        data["resource-groups.json"] = resource_groups
        data["resource-groups.properties"] = (
            "resource-groups.configuration-manager=file\n"
            "resource-groups.config-file=/etc/trino/resource-groups.json\n"
        )
    return data


def _config_revision(data: dict[str, str]) -> str:
    """A digest of the rendered config, stamped on the pod template.

    Every file here is mounted by ``subPath``, and a subPath mount is NEVER refreshed by the
    kubelet — a changed ConfigMap alone would leave the running pod on the old config indefinitely.
    Carrying the digest in the pod's annotations is what turns a config change into a rollout.
    """
    payload = json.dumps(data, sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def _config_manifest(shard: str, resource_groups: str | None) -> dict:
    settings = provisioner_settings()
    data = _config_data(resource_groups)
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": f"{shard_workload_name(shard)}-config",
            "namespace": settings["namespace"],
            "labels": {"provisa.dev/shard": shard},
        },
        "data": data,
    }


def _config_mounts(resource_groups: str | None) -> list[dict]:
    """One subPath mount per config file. ConfigMap keys are not a directory the image can take
    whole: mounting the volume at /etc/trino would hide catalog/, log.properties and everything else
    the image ships there."""
    files = ["config.properties", "jvm.config", "node.properties"]
    if resource_groups is not None:
        files += ["resource-groups.json", "resource-groups.properties"]
    return [{"name": "config", "mountPath": f"/etc/trino/{f}", "subPath": f} for f in files] + [
        {"name": "data", "mountPath": "/data/trino"}
    ]


def _deployment_manifest(shard: str, lane: str, resource_groups: str | None = None) -> dict:
    settings = provisioner_settings()
    name = shard_workload_name(shard)
    port = _int_env("PROVISA_ENGINE_PORT", 8080)
    memory = _memory_gib()
    revision = _config_revision(_config_data(resource_groups))
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": name,
            "namespace": settings["namespace"],
            "labels": {"provisa.dev/shard": shard, "provisa.dev/lane": lane},
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"provisa.dev/shard": shard}},
            "template": {
                "metadata": {
                    "labels": {"provisa.dev/shard": shard, "provisa.dev/lane": lane},
                    "annotations": {"provisa.dev/config-revision": revision},
                },
                "spec": {
                    # The whole point of the move: this pod lands on a pool that hosts Trino and
                    # nothing else, so a tenant scan cannot contend with the control plane.
                    "nodeSelector": {"provisa.dev/shard": shard},
                    "tolerations": [
                        {
                            "key": "provisa.dev/shard",
                            "operator": "Equal",
                            "value": shard,
                            "effect": "NoSchedule",
                        }
                    ],
                    # Trino drains on SIGTERM (SHUTTING_DOWN) and finishes what is running. The
                    # grace period must therefore outlast the longest query the engine will accept;
                    # at the default 30s a scale-in cuts queries mid-flight.
                    "terminationGracePeriodSeconds": _int_env("PROVISA_ENGINE_DRAIN_SECONDS", 600),
                    "containers": [
                        {
                            "name": "trino",
                            "image": settings["image"],
                            "ports": [{"containerPort": port, "name": "http"}],
                            "resources": {
                                # Requests equal limits: Trino sizes its heap off the limit, and a
                                # burstable pod that gets throttled below its heap assumption fails
                                # queries rather than running them slowly.
                                "requests": {
                                    "memory": f"{memory}Gi",
                                    "cpu": os.environ.get("PROVISA_ENGINE_CPU", "6"),
                                },
                                "limits": {
                                    "memory": f"{memory}Gi",
                                    "cpu": os.environ.get("PROVISA_ENGINE_CPU", "6"),
                                },
                            },
                            "volumeMounts": _config_mounts(resource_groups),
                            # Liveness deliberately absent: a coordinator busy with a large query
                            # can miss an HTTP probe, and restarting it there turns a slow query
                            # into a failed one plus a cold engine.
                            "readinessProbe": {
                                "httpGet": {"path": "/v1/info", "port": port},
                                "initialDelaySeconds": 10,
                                "periodSeconds": 5,
                                "failureThreshold": 60,
                            },
                        }
                    ],
                    "volumes": [
                        {"name": "config", "configMap": {"name": f"{name}-config"}},
                        {"name": "data", "emptyDir": {}},
                    ],
                },
            },
        },
    }


def _service_manifest(shard: str) -> dict:
    settings = provisioner_settings()
    name = shard_workload_name(shard)
    port = _int_env("PROVISA_ENGINE_PORT", 8080)
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": name,
            "namespace": settings["namespace"],
            "labels": {"provisa.dev/shard": shard},
        },
        "spec": {
            "selector": {"provisa.dev/shard": shard},
            "ports": [{"name": "http", "port": port, "targetPort": port}],
        },
    }


# ── Node pool ───────────────────────────────────────────────────────────────────


async def node_pool_size(shard: str) -> int:
    """How many Ready nodes the shard currently has.

    Counted from the cluster's node list rather than read off the node pool's ``initialNodeCount``:
    that field records what the pool was last SET to, and the autoscaler moves the real count
    underneath it. The wake path needs the count that determines whether a pod can be placed.

    Zero is the resting state, not an error: an empty pool bills nothing, while a node with no pods
    on it bills in full — which is why idle-to-zero scales the POOL and not the Deployment.
    """
    resp = await _k8s("GET", f"/api/v1/nodes?labelSelector=provisa.dev%2Fshard%3D{shard}")
    if resp.status_code >= 400:
        raise K8sProvisioningError(f"listing nodes for {shard} failed: {gcp_error_detail(resp)}")
    ready = 0
    for node in resp.json().get("items", []):
        conditions = node.get("status", {}).get("conditions", [])
        if any(c.get("type") == "Ready" and c.get("status") == "True" for c in conditions):
            ready += 1
    return ready


async def set_node_pool_size(shard: str, size: int) -> None:
    """Resize the shard's pool and wait for GKE to finish the operation."""
    settings = provisioner_settings()
    pool = shard_node_pool(shard)
    op = await _container_api(
        "POST",
        f"clusters/{settings['cluster']}/nodePools/{pool}:setSize",
        {"nodeCount": size},
    )
    await _await_operation(op["name"])
    log.info("engine node pool %s resized to %d", pool, size)


async def _await_operation(name: str) -> None:
    """Poll a GKE operation to completion.

    Node provision is ~90-120s, which is why this is minutes-scale and not seconds-scale.
    """
    timeout = _int_env("PROVISA_ENGINE_NODE_TIMEOUT", 420)
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        op = await _container_api("GET", f"operations/{name.rsplit('/', 1)[-1]}")
        if op.get("status") == "DONE":
            if op.get("error"):
                raise K8sProvisioningError(f"GKE operation {name} failed: {op['error']}")
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise K8sProvisioningError(
                f"GKE operation {name} did not finish within {timeout}s "
                f"(last status {op.get('status')!r})"
            )
        await asyncio.sleep(5.0)


# ── Readiness ───────────────────────────────────────────────────────────────────


async def _await_ready(shard: str) -> None:
    """Wait until the Deployment reports a ready replica.

    NOT a poll of the coordinator's ``/v1/info``. That endpoint reports ``starting=false`` as soon
    as the process answers, which on a cluster is true before the engine is whole; ``readyReplicas``
    is the cluster's own statement that the pod passed its readiness gate and is in the Service's
    endpoint set — the condition that actually has to hold before a query is released.
    """
    settings = provisioner_settings()
    name = shard_workload_name(shard)
    path = f"/apis/apps/v1/namespaces/{settings['namespace']}/deployments/{name}"
    timeout = _int_env("PROVISA_ENGINE_READY_TIMEOUT", 420)
    deadline = asyncio.get_running_loop().time() + timeout
    last = "no status"
    while True:
        resp = await _k8s("GET", path)
        if resp.status_code == 200:
            status = resp.json().get("status", {})
            if int(status.get("readyReplicas", 0)) >= 1:
                return
            last = f"readyReplicas={status.get('readyReplicas', 0)}"
        else:
            last = gcp_error_detail(resp)
        if asyncio.get_running_loop().time() >= deadline:
            raise K8sProvisioningError(
                f"engine {name} had no ready replica within {timeout}s ({last})"
            )
        await asyncio.sleep(3.0)


# ── Lifecycle ───────────────────────────────────────────────────────────────────


async def ensure_shard_running(
    shard: str, *, lane: str = "shared", resource_groups: str | None = None
) -> dict:
    """Bring a shard's engine up and return its endpoint once it can actually serve.

    Idempotent, and cheap when the shard is already warm: a pool that already holds a node and a
    Deployment that already has a ready replica reduce this to two GETs.

    The caller must hold the org registry's lock. A wake is a coupled sequence — pool up, replica
    ready, org runtime rebuilt so its ``CREATE CATALOG`` statements are reissued — and a resumed
    engine boots with ``catalog.management=dynamic`` and NO catalogs, so a query released before the
    rebuild fails against an engine that is running perfectly (REQ-1448).
    """
    settings = provisioner_settings()
    if await node_pool_size(shard) < 1:
        log.info("waking engine shard %s: node pool is at zero", shard)
        await set_node_pool_size(shard, 1)

    ns = f"/api/v1/namespaces/{settings['namespace']}"
    await _k8s_apply(
        f"{ns}/configmaps/{shard_workload_name(shard)}-config",
        _config_manifest(shard, resource_groups),
    )
    await _k8s_apply(f"{ns}/services/{shard_workload_name(shard)}", _service_manifest(shard))
    await _k8s_apply(
        f"/apis/apps/v1/namespaces/{settings['namespace']}/deployments/"
        f"{shard_workload_name(shard)}",
        _deployment_manifest(shard, lane, resource_groups),
    )
    await _await_ready(shard)

    host, port = shard_endpoint(shard)
    return {"shard": shard, "host": host, "port": port}


async def ensure_shared_shard(shard: str) -> dict:
    """Bring up a shard of the SHARED (Starter) lane.

    The one entry point the Starter query path uses, so that the queue policy is not something each
    caller remembers to pass: a shared shard without ``resource-groups.json`` is a shard on which one
    org's query load starves every other org on it.
    """
    return await ensure_shard_running(
        shard, lane="shared", resource_groups=shared_resource_groups()
    )


async def scale_shard_to_zero(shard: str) -> dict:
    """Drain the shard's engine and release its node.

    Deployment first, then the pool: deleting the pod under a drain lets Trino finish what is
    running, whereas dropping the node out from under a live pod does not. The pool is what actually
    stops the billing.
    """
    settings = provisioner_settings()
    name = shard_workload_name(shard)
    resp = await _k8s(
        "PATCH",
        f"/apis/apps/v1/namespaces/{settings['namespace']}/deployments/{name}/scale"
        "?fieldManager=provisa-control-plane",
        {"spec": {"replicas": 0}},
        content_type="application/merge-patch+json",
    )
    if resp.status_code >= 400:
        raise K8sProvisioningError(f"scaling {name} to zero failed: {gcp_error_detail(resp)}")

    # The pod's grace period has to elapse before the node is taken, or the drain is pointless.
    await asyncio.sleep(_int_env("PROVISA_ENGINE_DRAIN_SECONDS", 600))
    await set_node_pool_size(shard, 0)
    log.info("engine shard %s scaled to zero", shard)
    return {"shard": shard, "state": "stopped"}


async def shard_status(shard: str) -> dict:
    """What exists for this shard right now, without changing anything."""
    settings = provisioner_settings()
    name = shard_workload_name(shard)
    resp = await _k8s("GET", f"/apis/apps/v1/namespaces/{settings['namespace']}/deployments/{name}")
    nodes = await node_pool_size(shard)
    if resp.status_code == 404:
        return {"shard": shard, "state": "absent", "nodes": nodes}
    if resp.status_code >= 400:
        raise K8sProvisioningError(f"reading {name} failed: {gcp_error_detail(resp)}")
    status = resp.json().get("status", {})
    ready = int(status.get("readyReplicas", 0))
    return {
        "shard": shard,
        "state": "ready" if ready >= 1 else ("stopped" if nodes == 0 else "starting"),
        "ready_replicas": ready,
        "nodes": nodes,
    }


def gcp_error_detail(resp: httpx.Response) -> str:
    """The API's own message out of a response, which says far more than the status line."""
    try:
        body = json.loads(resp.content)
    except ValueError:
        return f"HTTP {resp.status_code}: {resp.text}"
    if isinstance(body, dict):
        if isinstance(body.get("error"), dict):
            return f"HTTP {resp.status_code}: {body['error'].get('message', resp.text)}"
        if "message" in body:
            return f"HTTP {resp.status_code}: {body['message']}"
    return f"HTTP {resp.status_code}: {resp.text}"
