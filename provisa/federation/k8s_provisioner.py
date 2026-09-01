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
container on the control plane's OWN Docker socket. That co-tenancy is the thing being retired: a
sized container bounds both ``Memory`` and ``NanoCpus`` (REQ-1449) but it still runs with
``node-scheduler.include-coordinator=true``, so an org's scan executes on the node the control plane
is serving every other org from — a CPU bound shares that node politely, it does not leave it.

Here an engine is a Deployment on an Autopilot cluster, where a node is provisioned to fit the pod
and removed when the pod goes. The SHARED (Starter) lane and the ISOLATED (Pro) lane are the same
manifests from the same image; they differ only in tenancy and in size. This module currently
implements the shared lane — :func:`ensure_shard_running` and its shard vocabulary — and the isolated
lane lands on the same primitives by passing a per-org shard name.

Two things are deliberately not the way the Docker provisioner did them:

* **Readiness is ``readyReplicas``, not ``/v1/info``.** The old poll returned the moment the
  coordinator process answered ``starting=false``, which says nothing about whether the pod passed
  its readiness gate and entered the Service's endpoint set. A query released on the process's own
  word is dispatched at a Service that still routes to nothing.
* **Scale-in drains.** ``terminationGracePeriodSeconds`` covers the longest permitted query so that
  Trino's own SHUTTING_DOWN drain can finish it; a pod killed at the default 30s cuts it.

Authentication is the VM's attached service account, read from the GCE metadata server. The
control-plane VM is the only caller, and its IAM role is read-only at the GKE API — everything it
changes it changes through the Kubernetes API under a namespaced RBAC Role
(``terraform/gcp-saas/gke.tf``), a narrower grant than the mounted Docker socket it replaces.
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
from typing import Any

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
# dedicated node that reason is gone, and the setting inverts: a shard is a single pod, so with
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
        # The Arrow Flight SQL proxy, which runs beside the coordinator in the same pod rather than
        # on the control plane: it holds a JDBC connection to Trino, so a proxy that outlives the
        # shard holds a connection to an address that no longer exists, and one started before the
        # shard has no address to be given. In the pod it is created, woken and destroyed with the
        # engine it fronts, and reaches it at localhost (REQ-045, REQ-1448).
        "zaychik_image": "PROVISA_ZAYCHIK_IMAGE",
        # Required, not inferred from the location: an Autopilot cluster is REGIONAL (the API
        # refuses a zonal one), so without an explicit zone the scheduler may place a shard pod in
        # a zone the control-plane VM is not in and every byte of every result set is billed as
        # cross-zone egress. On a Standard cluster the location IS the zone and this restates it
        # (REQ-1465).
        "zone": "PROVISA_ENGINE_CLUSTER_ZONE",
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
    settings["mode"] = cluster_mode()
    return settings


def cluster_mode() -> str:
    """Which cluster topology this control plane is driving: ``autopilot`` or ``standard``.

    Both are supported, and the switch between them is planned rather than hypothetical: Autopilot
    is cheaper while the shared lane is idle most of the day, and a Standard cluster with a small
    always-on system pool is cheaper once it is busy — the crossover is about 460 shard-hours a
    month (REQ-1465). Everything the control plane does is IDENTICAL in both modes except the pod's
    placement stanza: a Standard shard pool autoscales 0↔1 because this pod is the only thing that
    tolerates its taint, so the same replica patch that starts a pod also brings the node, and the
    same patch to zero takes it away.

    Defaulted to autopilot because that is what terraform's own default builds
    (var.engine_cluster_mode); a value that is neither raises rather than being treated as one of
    them, because a mis-set mode yields pods that stay Pending on a Standard cluster.
    """
    mode = os.environ.get("PROVISA_ENGINE_CLUSTER_MODE", "autopilot")
    if mode not in ("autopilot", "standard"):
        raise K8sProvisioningError(
            f"PROVISA_ENGINE_CLUSTER_MODE={mode!r} is not a cluster topology; "
            "expected 'autopilot' or 'standard'"
        )
    return mode


def _placement(shard: str, zone: str) -> dict:
    """Where this shard's pod may land, which is the ONLY thing that differs between the modes.

    Autopilot has no pools to select between: the scheduler provisions a node sized to the pod's
    requests and puts nothing else on it beyond GKE's own agents, so a nodeSelector for a custom
    label would leave the pod unschedulable. The one selector it does carry is the well-known
    ``topology.kubernetes.io/zone``, because an Autopilot cluster is regional and a pod in another
    zone than the control-plane VM turns every result set into cross-zone egress.

    Standard puts each shard on its own pool, autoscaling from zero and tainted so that GKE's system
    Deployments cannot hold it up — those land on the small always-on system pool instead. The
    toleration is what lets this pod onto the tainted pool; the selector is what keeps it off any
    other (REQ-1465).
    """
    if cluster_mode() == "autopilot":
        return {"nodeSelector": {"topology.kubernetes.io/zone": zone}}
    return {
        "nodeSelector": {"topology.kubernetes.io/zone": zone, "provisa.dev/shard": shard},
        "tolerations": [
            {
                "key": "provisa.dev/shard",
                "operator": "Equal",
                "value": shard,
                "effect": "NoSchedule",
            }
        ],
    }


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
    """A call against the GKE control-plane API — reading the cluster's endpoint and CA."""
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


def isolated_shard(org_id: str) -> str:  # REQ-1510
    """The shard name of the coordinator that serves ``org_id`` alone.

    A dedicated engine is a shard like any other — it is woken, dialled and idled by the same
    machinery — so it is NAMED like one rather than reached through a hostname template the control
    plane has to be told about separately (REQ-1510). The org's shared-lane placement (``orgs.shard``)
    is a different string and is left alone, so a return to Starter lands back on the shard the org
    already had (REQ-1450).
    """
    return f"org_{org_id}"


def shard_workload_name(shard: str) -> str:
    """The Deployment/Service name for a shard.

    ``orgs.shard`` holds ``shared_1``; Kubernetes object names are DNS labels and reject the
    underscore, so the two spellings differ by exactly this translation and nowhere else.
    """
    return f"trino-{shard.replace('_', '-')}"


# The address last observed for each shard's ready pod. Written by the two calls that ask the
# cluster about a shard (_await_ready, shard_status) and cleared by the one that takes it away
# (scale_shard_to_zero), so what it holds is always the cluster's own last word.
_pod_ips: dict[str, str] = {}

# The pod UID behind each shard's recorded address, and a counter bumped every time that UID
# changes. This is the coordinator generation: an observation of the cluster, not a count of the
# restarts this process happened to drive. Two app containers share one shard, so a restart driven
# by either one — or by an eviction neither one asked for — has to be visible to both, or the
# container that did not drive it keeps dialing the pod that is gone (REQ-1448).
_pod_uids: dict[str, str] = {}
_coordinator_epoch: dict[str, int] = {}


def coordinator_epoch(shard: str) -> int:
    """How many distinct pods have served ``shard`` since this process started observing it."""
    return _coordinator_epoch.get(shard, 0)


def _forget_pod(shard: str) -> None:
    """Drop the recorded coordinator, so the next pod observed counts as a new one."""
    _pod_ips.pop(shard, None)
    _pod_uids.pop(shard, None)


async def _resolve_pod_ip(shard: str) -> str:
    """The IP of the shard's ready pod, asked of the Kubernetes API.

    The address is a pod IP rather than the Service's DNS name because the control plane is a VM
    inside the VPC but OUTSIDE the cluster, and it has no way to resolve ``<svc>.<ns>.svc.<domain>``.
    GKE can publish those records VPC-wide (Cloud DNS additive VPC scope), but on an Autopilot
    cluster the setting is creation-only AND ``google_container_cluster.dns_config`` carries
    ``DiffSuppressFunc: suppressDiffForAutopilot`` in the terraform provider (still present at
    v7.44.0), so the create request never contains it and the API refuses to add it afterwards.
    Pod IPs need none of that: they are VPC-native alias ranges, routable VPC-wide, and identical in
    both cluster topologies — which also takes a creation-time constraint off the Autopilot↔Standard
    cutover (REQ-1451, REQ-1465).
    """
    settings = provisioner_settings()
    resp = await _k8s(
        "GET",
        f"/api/v1/namespaces/{settings['namespace']}/pods"
        f"?labelSelector=provisa.dev%2Fshard%3D{shard}",
    )
    if resp.status_code >= 400:
        raise K8sProvisioningError(
            f"listing pods for shard {shard} failed: {gcp_error_detail(resp)}"
        )
    for pod in resp.json().get("items", []):
        meta = pod.get("metadata", {})
        # A pod being deleted still reports Ready until its grace period runs out. Dialing it hands
        # the terminal a coordinator that is already shutting down.
        if meta.get("deletionTimestamp"):
            continue
        status = pod.get("status", {})
        ready = any(
            c.get("type") == "Ready" and c.get("status") == "True"
            for c in status.get("conditions", [])
        )
        ip = status.get("podIP")
        if ready and ip:
            uid = meta["uid"]
            if _pod_uids.get(shard) != uid:
                _pod_uids[shard] = uid
                _coordinator_epoch[shard] = _coordinator_epoch.get(shard, 0) + 1
            _pod_ips[shard] = ip
            return ip
    raise K8sProvisioningError(f"shard {shard} has no ready pod with an address")


def recorded_shard_address(shard: str) -> str | None:
    """The address currently recorded for ``shard``, or None if none is.

    The read :func:`shard_endpoint` refuses to make: a caller comparing the address across a
    re-resolution is asking WHETHER one is held, and an absent record is the answer rather than an
    error.
    """
    return _pod_ips.get(shard)


def forget_shard_address(shard: str) -> None:
    """Discard the recorded address for ``shard``, so the next wake asks the cluster for it.

    The pod UID is deliberately KEPT. Whether the coordinator was replaced is the cluster's to say,
    and it says it by handing back a different UID at the next resolution — which bumps the
    generation and rebuilds the org runtimes bound to the old one. Dropping the UID here would
    declare a replacement this process never observed and rebuild every runtime over what may have
    been one unreachable moment of the same pod (REQ-1448).
    """
    _pod_ips.pop(shard, None)


def shard_endpoint(shard: str) -> tuple[str, int]:
    """``(host, port)`` the control plane dials for a shard.

    Synchronous, and answers from what the last cluster call observed: every dial is preceded by a
    wake (``engine_wake.ensure_shard_awake``), and both the cold path and the warm path record the
    ready pod's address on the way through. A shard with nothing recorded has not been woken, and
    saying so is the point — a guessed address is an engine that comes up healthy and that nothing
    can reach (REQ-1448).
    """
    ip = _pod_ips.get(shard)
    if not ip:
        raise K8sProvisioningError(
            f"shard {shard} has no address: it has not been woken in this process, so the pod "
            "serving it is unknown (REQ-1448)"
        )
    return ip, _int_env("PROVISA_ENGINE_PORT", 8080)


def shard_flight_endpoint(shard: str) -> tuple[str, int]:
    """``(host, port)`` for the shard's Arrow Flight SQL proxy.

    Same pod, same address, second port: the proxy runs as a sidecar beside the coordinator it
    fronts, so it is created, woken and destroyed with the engine and never outlives the address
    it holds a JDBC connection to (REQ-045, REQ-1448).
    """
    ip, _ = shard_endpoint(shard)
    return ip, _int_env("PROVISA_ENGINE_FLIGHT_PORT", 8480)


# ── Manifests ───────────────────────────────────────────────────────────────────


def _memory_gib() -> int:
    return _int_env("PROVISA_ENGINE_MEMORY_GIB", 24)


def _pod_shape(size: Any) -> tuple[str, int, int]:
    """``(cpu, memory_gib, query_budget_gb)`` for a pod of ``size``.

    ``size`` is the org's plan-fixed engine size (REQ-1449), resolved by the caller through
    ``provisa.core.commerce.engine_size_for_org``; only these three numbers cross out of the
    commercial plugin, so the plan vocabulary stays there.

    ``None`` is the shared lane and any deployment that does not size engines by plan: those are the
    deployment-wide ``PROVISA_ENGINE_CPU`` / ``PROVISA_ENGINE_MEMORY_GIB``, which is what every
    shard used before sizes existed. See the budget derivation in :func:`_config_data`.
    """
    if size is not None:
        return size.pod_cpu, size.pod_memory_gib, size.query_max_memory_gb
    memory = _memory_gib()
    return os.environ.get("PROVISA_ENGINE_CPU", "6"), memory, max(1, int(memory * 0.7 * 0.5))


def shared_resource_groups() -> str:
    """The shared lane's queue policy, verbatim (REQ-1450).

    The same file the shared cluster mounts, not a second copy authored here: the ``tenant-${USER}``
    subgroup and its selector are what keep one Starter org from taking the shard's whole concurrency
    budget, and a divergent copy would be a policy change nobody made.
    """
    path = Path(__file__).resolve().parents[2] / "trino" / "etc" / "resource-groups.json"
    return path.read_text(encoding="utf-8")


def _config_data(resource_groups: str | None, size: Any = None) -> dict[str, str]:
    port = _int_env("PROVISA_ENGINE_PORT", 8080)
    _, _, budget = _pod_shape(size)
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
        query_max_memory=f"{budget}GB",
        query_max_memory_per_node=f"{budget}GB",
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


def _config_manifest(shard: str, resource_groups: str | None, size: Any = None) -> dict:
    settings = provisioner_settings()
    data = _config_data(resource_groups, size)
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


def _deployment_manifest(
    shard: str, lane: str, resource_groups: str | None = None, size: Any = None
) -> dict:
    settings = provisioner_settings()
    name = shard_workload_name(shard)
    port = _int_env("PROVISA_ENGINE_PORT", 8080)
    flight_port = _int_env("PROVISA_ENGINE_FLIGHT_PORT", 8480)
    cpu, memory, _ = _pod_shape(size)
    revision = _config_revision(_config_data(resource_groups, size))
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
            # Take the old pod down before the new one comes up. A shard is ONE coordinator: a surge
            # pod is a second engine that holds none of the first one's catalogs, is billed at the
            # full Guaranteed request for the length of the roll, and — because the old pod stays
            # ready throughout — is not the pod a client would be handed. Expressed as a zero-surge
            # RollingUpdate rather than `type: Recreate` because the apiserver defaults a
            # rollingUpdate block onto this object, and a server-side apply that switches the type
            # is rejected for carrying it (`may not be specified when strategy type is Recreate`).
            "strategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {"maxSurge": 0, "maxUnavailable": 1},
            },
            "selector": {"matchLabels": {"provisa.dev/shard": shard}},
            "template": {
                "metadata": {
                    "labels": {"provisa.dev/shard": shard, "provisa.dev/lane": lane},
                    "annotations": {"provisa.dev/config-revision": revision},
                },
                "spec": {
                    # Placement is mode-dependent and nothing else is: on Autopilot the scheduler
                    # sizes a node for this pod and puts nothing else on it (REQ-1464); on Standard
                    # the selector and toleration below pin it to its own pool (REQ-1465). What
                    # keeps a tenant scan off the control plane in either mode is that the control
                    # plane is not in this cluster at all, and what keeps one shard off another's
                    # CPU is the Guaranteed QoS below.
                    **_placement(shard, settings["zone"]),
                    #
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
                                "requests": {"memory": f"{memory}Gi", "cpu": cpu},
                                "limits": {"memory": f"{memory}Gi", "cpu": cpu},
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
                        },
                        {
                            # Flight SQL for this shard. Its readiness gate is part of the pod's,
                            # so the address _await_ready hands back is an address on which BOTH
                            # protocols answer — the control plane connects Flight during boot's
                            # provision_infra and fails the whole startup if it cannot (REQ-045).
                            "name": "zaychik",
                            "image": settings["zaychik_image"],
                            "ports": [{"containerPort": flight_port, "name": "flight"}],
                            "env": [
                                {"name": "TF_TRINO_HOST", "value": "localhost"},
                                {"name": "TF_TRINO_PORT", "value": str(port)},
                                {"name": "TF_TRINO_SSL", "value": "false"},
                                # B104 reads 0.0.0.0 as exposure: this binds inside the pod's own
                                # network namespace, where it is the only way the Service can reach
                                # the listener. The Service is what decides who reaches the pod.
                                {"name": "TF_FLIGHT_HOST", "value": "0.0.0.0"},  # nosec B104
                                {"name": "TF_FLIGHT_PORT", "value": str(flight_port)},
                                {"name": "TF_FLIGHT_SSL", "value": "false"},
                                {"name": "TF_FLIGHT_AUTH_TYPE", "value": "trino"},
                                {"name": "TF_FLIGHT_BATCH_SIZE", "value": "10000"},
                            ],
                            "resources": {
                                # A proxy, not an engine: it streams Arrow batches through and
                                # holds no working set. The Helm chart that has run this proxy in
                                # Kubernetes since REQ-143 asks for 100m/256Mi; this is that shape
                                # with headroom, not the engine-sized slice a sidecar looks like it
                                # deserves — every core requested here is a core Autopilot bills for
                                # the whole time the shard is awake (REQ-1464). Requests equal
                                # limits because a burstable sidecar would drop the pod's QoS class
                                # below Guaranteed.
                                "requests": {
                                    "memory": os.environ.get("PROVISA_ZAYCHIK_MEMORY", "1Gi"),
                                    "cpu": os.environ.get("PROVISA_ZAYCHIK_CPU", "500m"),
                                },
                                "limits": {
                                    "memory": os.environ.get("PROVISA_ZAYCHIK_MEMORY", "1Gi"),
                                    "cpu": os.environ.get("PROVISA_ZAYCHIK_CPU", "500m"),
                                },
                            },
                            "readinessProbe": {
                                # TCP, not HTTP: the port speaks gRPC, which answers no GET.
                                "tcpSocket": {"port": flight_port},
                                "initialDelaySeconds": 10,
                                "periodSeconds": 5,
                                "failureThreshold": 60,
                            },
                            "livenessProbe": {
                                # The proxy holds a JDBC connection to the coordinator beside it. If
                                # it wedges, the pod stays Ready on Trino's probe alone and every
                                # Arrow client hangs against a shard that looks healthy — restarting
                                # the container is the only thing that reopens that connection.
                                "tcpSocket": {"port": flight_port},
                                "initialDelaySeconds": 30,
                                "periodSeconds": 10,
                                "failureThreshold": 6,
                            },
                        },
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
    flight_port = _int_env("PROVISA_ENGINE_FLIGHT_PORT", 8480)
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": name,
            "namespace": settings["namespace"],
            "labels": {"provisa.dev/shard": shard},
        },
        "spec": {
            # Headless. The client is the control-plane VM, which sits in the VPC but OUTSIDE the
            # cluster, and a ClusterIP is only routable from inside it: DNS answered
            # trino-shared-1.provisa-engines.svc.<domain> with 10.24.6.12 and every connect timed
            # out, while the pod IP answered /v1/info immediately. Pod IPs are VPC-native alias
            # ranges and route VPC-wide, so clusterIP: None publishes the address that actually
            # works — and a shard is one pod, so the record it publishes is unambiguous. The
            # alternative, an internal load balancer per shard, bills by the hour against a
            # zero-customer floor that must stay at $18.96/mo (REQ-1447).
            "clusterIP": "None",
            "selector": {"provisa.dev/shard": shard},
            "ports": [
                {"name": "http", "port": port, "targetPort": port},
                {"name": "flight", "port": flight_port, "targetPort": flight_port},
            ],
        },
    }


# ── Readiness ───────────────────────────────────────────────────────────────────


async def _await_ready(shard: str) -> str:
    """Wait until the Deployment reports a ready replica, and return that replica's address.

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
            body = resp.json()
            status = body.get("status", {})
            generation = int(body.get("metadata", {}).get("generation", 0))
            observed = int(status.get("observedGeneration", 0))
            replicas = int(status.get("replicas", 0))
            updated = int(status.get("updatedReplicas", 0))
            ready = int(status.get("readyReplicas", 0))
            # Not readyReplicas alone. During a roll the PREVIOUS release's pod is ready and its
            # address is the one _resolve_pod_ip would hand back, so boot would connect to the
            # coordinator the new manifest exists to replace — which is exactly how a control plane
            # carrying a new Flight sidecar dialed the old sidecar-less pod and got ECONNREFUSED.
            # observedGeneration proves the cluster has seen the manifest just applied, and
            # replicas == updatedReplicas proves no pod from the previous one is left.
            if observed >= generation and replicas == updated and ready >= 1:
                # Resolved here rather than by the caller: the pod that just passed its readiness
                # gate is the one this wake produced, and its IP is what the caller will dial.
                return await _resolve_pod_ip(shard)
            last = (
                f"generation={generation} observed={observed} "
                f"replicas={replicas} updated={updated} ready={ready}"
            )
        else:
            last = gcp_error_detail(resp)
        if asyncio.get_running_loop().time() >= deadline:
            raise K8sProvisioningError(
                f"engine {name} had no ready replica within {timeout}s ({last})"
            )
        await asyncio.sleep(3.0)


# ── Lifecycle ───────────────────────────────────────────────────────────────────


async def ensure_shard_running(
    shard: str, *, lane: str = "shared", resource_groups: str | None = None, size: Any = None
) -> dict:
    """Bring a shard's engine up and return its endpoint once it can actually serve.

    Idempotent, and cheap when the shard is already warm: applying manifests that already match is a
    no-op, and a Deployment that already has a ready replica returns on the first GET.

    There is no node step. Applying the Deployment at one replica is the whole wake: Autopilot
    provisions a node to fit the pod's requests, which is the ~2-4min ``PROVISA_ENGINE_READY_TIMEOUT``
    covers (REQ-1464).

    The caller must hold the org registry's lock. A wake is a coupled sequence — replica ready, org
    runtime rebuilt so its ``CREATE CATALOG`` statements are reissued — and a resumed engine boots
    with ``catalog.management=dynamic`` and NO catalogs, so a query released before the rebuild fails
    against an engine that is running perfectly (REQ-1448).

    ``size`` is the org's plan-fixed engine size on the isolated lane (REQ-1449). It sets the pod's
    requests and the query budget in its config, so a size change is a config revision and therefore
    a rollout — the shard comes back on the hardware the org now pays for rather than drifting from
    it. ``None`` is the shared lane and any deployment that does not size engines by plan.
    """
    settings = provisioner_settings()
    ns = f"/api/v1/namespaces/{settings['namespace']}"
    await _k8s_apply(
        f"{ns}/configmaps/{shard_workload_name(shard)}-config",
        _config_manifest(shard, resource_groups, size),
    )
    await _k8s_apply(f"{ns}/services/{shard_workload_name(shard)}", _service_manifest(shard))
    await _k8s_apply(
        f"/apis/apps/v1/namespaces/{settings['namespace']}/deployments/"
        f"{shard_workload_name(shard)}",
        _deployment_manifest(shard, lane, resource_groups, size),
    )
    host = await _await_ready(shard)
    return {"shard": shard, "host": host, "port": _int_env("PROVISA_ENGINE_PORT", 8080)}


async def ensure_shared_shard(shard: str) -> dict:
    """Bring up a shard of the SHARED (Starter) lane.

    The one entry point the Starter query path uses, so that the queue policy is not something each
    caller remembers to pass: a shared shard without ``resource-groups.json`` is a shard on which one
    org's query load starves every other org on it.
    """
    return await ensure_shard_running(
        shard, lane="shared", resource_groups=shared_resource_groups()
    )


async def ensure_isolated_shard(shard: str, size: Any) -> dict:
    """Bring up the shard of the ISOLATED (Pro) lane serving one org, at its plan's size.

    ``size`` is required rather than defaulted: the isolated lane is sold in fixed sizes (REQ-1449),
    and a shard brought up without one would run on the deployment-wide settings while the org is
    invoiced at the active-hour rate of the size it bought.

    No resource groups. The queue policy exists to keep one org off another's shard, and there is no
    other org here — the size is the whole limit, so a concurrency ceiling on top of it would bill
    for hardware and then refuse to let the org use it.
    """
    if size is None:
        raise K8sProvisioningError(
            f"shard {shard} is on the isolated lane but no engine size was resolved for it; "
            "a dedicated engine is provisioned at the size its plan sells (REQ-1449)"
        )
    return await ensure_shard_running(shard, lane="isolated", size=size)


async def scale_shard_to_zero(shard: str, *, await_drain: bool = True) -> dict:
    """Drain the shard's engine, which is what releases its node.

    On Autopilot the replica count IS the bill: pods are charged on their requests and a cluster
    with no running workloads scales to zero nodes, so taking the Deployment to zero is the whole
    stop — there is no pool left to size afterwards (REQ-1464).

    The pod is not gone when the PATCH returns: Trino drains on SIGTERM (SHUTTING_DOWN) and finishes
    what is running, for up to ``terminationGracePeriodSeconds``. This waits for the Deployment to
    observe zero replicas so the returned state is the cluster's word rather than ours, and so a
    wake arriving behind a stop is not racing a pod that is still terminating.

    ``await_drain=False`` returns as soon as the PATCH lands, for the one caller that cannot wait:
    the control plane's own shutdown (REQ-1629). The drain wait can run to
    PROVISA_ENGINE_DRAIN_SECONDS, far longer than the seconds a stopping process has, and the PATCH
    is already the whole instruction — the cluster terminates the pod whether or not this process
    survives to watch it. The returned state then says ``stopping``, because nothing observed zero.
    """
    settings = provisioner_settings()
    name = shard_workload_name(shard)
    path = f"/apis/apps/v1/namespaces/{settings['namespace']}/deployments/{name}"
    resp = await _k8s(
        "PATCH",
        f"{path}/scale?fieldManager=provisa-control-plane",
        {"spec": {"replicas": 0}},
        content_type="application/merge-patch+json",
    )
    if resp.status_code >= 400:
        raise K8sProvisioningError(f"scaling {name} to zero failed: {gcp_error_detail(resp)}")

    if not await_drain:
        _forget_pod(shard)
        log.info("engine shard %s scaled to zero; not waiting for the drain", shard)
        return {"shard": shard, "state": "stopping"}

    # The drain window bounds how long a pod may take to go; a wait shorter than it would report a
    # shard stopped while it is still billing.
    timeout = _int_env("PROVISA_ENGINE_DRAIN_SECONDS", 600)
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        resp = await _k8s("GET", path)
        if resp.status_code >= 400:
            raise K8sProvisioningError(f"reading {name} failed: {gcp_error_detail(resp)}")
        status = resp.json().get("status", {})
        if int(status.get("replicas", 0)) == 0:
            break
        if asyncio.get_running_loop().time() >= deadline:
            raise K8sProvisioningError(
                f"engine {name} still had {status.get('replicas')} replica(s) {timeout}s after "
                "being scaled to zero"
            )
        await asyncio.sleep(5.0)

    _forget_pod(shard)
    log.info("engine shard %s scaled to zero", shard)
    return {"shard": shard, "state": "stopped"}


async def shard_status(shard: str) -> dict:
    """What exists for this shard right now, without changing anything."""
    settings = provisioner_settings()
    name = shard_workload_name(shard)
    resp = await _k8s("GET", f"/apis/apps/v1/namespaces/{settings['namespace']}/deployments/{name}")
    if resp.status_code == 404:
        return {"shard": shard, "state": "absent", "ready_replicas": 0}
    if resp.status_code >= 400:
        raise K8sProvisioningError(f"reading {name} failed: {gcp_error_detail(resp)}")
    body = resp.json()
    ready = int(body.get("status", {}).get("readyReplicas", 0))
    if ready >= 1:
        # The warm path through ensure_shard_awake ends here and dials without going near
        # ensure_shard_running, so this is where a warm shard's address is refreshed. A pod that was
        # replaced under us (eviction, node repair) comes back with a different IP and the same
        # ready Deployment, and reusing the old one would dial an address nothing answers on.
        await _resolve_pod_ip(shard)
    else:
        _forget_pod(shard)
    # Desired replicas, not node count: on Autopilot a shard at zero replicas has no node by
    # construction, and one with a replica that is not ready yet is a node being provisioned.
    desired = int(body.get("spec", {}).get("replicas", 0))
    return {
        "shard": shard,
        "state": "ready" if ready >= 1 else ("stopped" if desired == 0 else "starting"),
        "ready_replicas": ready,
        "replicas": desired,
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
