# Copyright (c) 2026 Kenneth Stott
# Canary: 8f41c2b7-5d09-4e3a-9c16-2b7e04af6d51
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1447/REQ-1448/REQ-1450: an engine comes up on the cluster, not on the control plane's host.

The Docker provisioner put a tenant's coordinator on the same machine the control plane serves every
other org from, with no CPU bound and with the coordinator itself executing splits. What is asserted
here is that the replacement does not: the pod is pinned to a shard's own node pool, it is only
declared usable once the CLUSTER says a replica is ready, and a scale-in drains before the node goes
away.

Both the GKE and the Kubernetes APIs are replaced by an httpx MockTransport that records requests,
so the calls are asserted against the real API shapes without a cluster.
"""

from __future__ import annotations

import base64
import datetime
import json

import httpx
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from provisa.federation import k8s_provisioner as prov


def _self_signed_ca() -> str:
    """A real CA PEM, base64'd as the GKE API returns it.

    ``_k8s`` builds an ``ssl`` context from whatever the cluster hands back, and ``ssl`` rejects
    anything that is not a parseable certificate — so a placeholder string would fail in the TLS
    setup rather than in the call under test.
    """
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "provisa-test-ca")])
    now = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    return base64.b64encode(cert.public_bytes(serialization.Encoding.PEM)).decode()


_CA_B64 = _self_signed_ca()


@pytest.fixture()
def configured(monkeypatch):
    monkeypatch.setenv("PROVISA_ENGINE_CLUSTER_PROJECT", "provisa-saas")
    monkeypatch.setenv("PROVISA_ENGINE_CLUSTER_LOCATION", "us-central1-a")
    monkeypatch.setenv("PROVISA_ENGINE_CLUSTER_NAME", "provisa-saas-engine")
    monkeypatch.setenv("PROVISA_ENGINE_CLUSTER_DNS_DOMAIN", "provisa-saas-engine.internal")
    monkeypatch.setenv("PROVISA_ENGINE_IMAGE", "gcr.io/provisa/trino-engine:v1")
    monkeypatch.setenv("PROVISA_ENGINE_NAMESPACE", "provisa-engines")
    monkeypatch.setenv("PROVISA_ENGINE_PORT", "8080")
    monkeypatch.setenv("PROVISA_ENGINE_MEMORY_GIB", "24")
    monkeypatch.setenv("PROVISA_ENGINE_DRAIN_SECONDS", "0")
    monkeypatch.setattr(prov, "_cluster_cache", {})
    monkeypatch.setattr(prov, "_token_cache", ("stub-token", 1 << 40))


def _mock_api(monkeypatch, handler):
    """Point every outbound call at a recorded MockTransport."""
    calls: list[httpx.Request] = []

    def record(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return handler(request)

    def fake_client(verify=True) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(record))

    monkeypatch.setattr(prov, "_client", fake_client)
    return calls


def _cluster_get() -> httpx.Response:
    return httpx.Response(
        200,
        json={"endpoint": "10.0.0.2", "masterAuth": {"clusterCaCertificate": _CA_B64}},
    )


# ---- availability ------------------------------------------------------------


def test_provisioning_needs_a_cluster_and_an_image(monkeypatch, configured):
    assert prov.provisioning_available() is True
    monkeypatch.delenv("PROVISA_ENGINE_IMAGE")
    assert prov.provisioning_available() is False


def test_missing_settings_name_themselves(monkeypatch, configured):
    monkeypatch.delenv("PROVISA_ENGINE_CLUSTER_NAME")
    with pytest.raises(prov.K8sProvisioningError) as excinfo:
        prov.provisioner_settings()
    assert "PROVISA_ENGINE_CLUSTER_NAME" in str(excinfo.value)


def test_dns_domain_is_required_not_defaulted(monkeypatch, configured):
    """The control plane is a VM, so falling back to svc.cluster.local would resolve nowhere. An
    unset domain must fail loudly instead of producing an engine nothing can dial (REQ-1451)."""
    monkeypatch.delenv("PROVISA_ENGINE_CLUSTER_DNS_DOMAIN")
    with pytest.raises(prov.K8sProvisioningError) as excinfo:
        prov.shard_endpoint("shared_1")
    assert "PROVISA_ENGINE_CLUSTER_DNS_DOMAIN" in str(excinfo.value)


# ---- naming is one derivation ------------------------------------------------


def test_shard_names_translate_once(configured):
    """``orgs.shard`` spells the shard with an underscore; a Kubernetes object name cannot hold one.
    The two spellings must differ by exactly this translation, or the control plane dials a Service
    that does not exist."""
    assert prov.shard_workload_name("shared_1") == "trino-shared-1"
    assert prov.shard_node_pool("shared_1") == "shared-1"
    assert prov.shard_endpoint("shared_1") == (
        "trino-shared-1.provisa-engines.svc.provisa-saas-engine.internal",
        8080,
    )


# ---- the pod cannot land on the control plane --------------------------------


def test_pod_is_pinned_to_the_shard_pool(configured):
    """The co-tenancy being retired: without both the selector and the toleration the coordinator
    schedules onto whatever node has room, which is the shared-host problem again."""
    spec = prov._deployment_manifest("shared_1", "shared")["spec"]["template"]["spec"]
    assert spec["nodeSelector"] == {"provisa.dev/shard": "shared_1"}
    assert spec["tolerations"] == [
        {
            "key": "provisa.dev/shard",
            "operator": "Equal",
            "value": "shared_1",
            "effect": "NoSchedule",
        }
    ]


def test_cpu_is_bounded(configured):
    """The Docker provisioner set ``Memory`` and never ``NanoCpus``, so one org's scan could take
    the whole host. A limit equal to the request is what removes that."""
    container = prov._deployment_manifest("shared_1", "shared")["spec"]["template"]["spec"][
        "containers"
    ][0]
    assert container["resources"]["limits"]["cpu"] == container["resources"]["requests"]["cpu"]
    assert container["resources"]["limits"]["memory"] == "24Gi"


def test_a_shard_can_schedule_its_own_splits(configured):
    """A shard is a single pod. With ``include-coordinator=false`` there is no node in the cluster
    willing to take a split and every query queues forever — the setting that was a co-tenancy
    hazard on the control plane's own host is a requirement on a pool that hosts Trino alone."""
    config = prov._config_manifest("shared_1", None)["data"]["config.properties"]
    assert "node-scheduler.include-coordinator=true" in config
    assert "catalog.management=dynamic" in config


def test_config_files_reach_the_pod(configured):
    """Every ConfigMap key needs its own subPath mount. A key with no mount is a file Trino never
    sees — the way resource-groups.properties would have been written and then ignored."""
    for groups in (None, '{"rootGroups": []}'):
        data = prov._config_manifest("shared_1", groups)["data"]
        mounts = prov._deployment_manifest("shared_1", "shared", groups)["spec"]["template"][
            "spec"
        ]["containers"][0]["volumeMounts"]
        assert {m["subPath"] for m in mounts if "subPath" in m} == set(data)


def test_config_change_rolls_the_pod(configured):
    """A subPath mount is never refreshed by the kubelet, so a changed ConfigMap alone would leave
    the running engine on the old config indefinitely."""
    plain = prov._deployment_manifest("shared_1", "shared", None)
    grouped = prov._deployment_manifest("shared_1", "shared", '{"rootGroups": []}')
    revision = "provisa.dev/config-revision"
    assert (
        plain["spec"]["template"]["metadata"]["annotations"][revision]
        != grouped["spec"]["template"]["metadata"]["annotations"][revision]
    )


def test_tracing_is_off_without_a_collector(configured, monkeypatch):
    """Trino's OTel exporter has no drop-if-unreachable mode: pointed at a collector that does not
    exist it retries every span for the life of the process."""
    monkeypatch.delenv("PROVISA_ENGINE_OTLP_ENDPOINT", raising=False)
    data = prov._config_manifest("shared_1", None)["data"]
    assert "tracing.enabled" not in data["config.properties"]
    assert "otel.exporter.otlp.endpoint" not in data["jvm.config"]

    monkeypatch.setenv("PROVISA_ENGINE_OTLP_ENDPOINT", "http://otel-collector:4317")
    data = prov._config_manifest("shared_1", None)["data"]
    assert "tracing.enabled=true" in data["config.properties"]
    assert "-Dotel.exporter.otlp.endpoint=http://otel-collector:4317" in data["jvm.config"]


def test_shared_lane_gets_the_deployments_own_queue_policy(configured):
    """The shared entry point reads trino/etc/resource-groups.json rather than restating it, so the
    ``tenant-${USER}`` subgroup on the cluster and on a shard are the same policy."""
    groups = json.loads(prov.shared_resource_groups())
    names = {g["name"] for g in groups["rootGroups"][0]["subGroups"]}
    assert "tenant-${USER}" in names


def test_otel_agent_is_configured_because_the_image_carries_it(configured):
    """The Docker provisioner had to ship a jvm.config with no -javaagent line: the jar existed only
    where compose mounted it, and a missing agent aborts the JVM before Trino logs anything. The
    engine image bakes it in, so the divergence is gone."""
    jvm = prov._config_manifest("shared_1", None)["data"]["jvm.config"]
    assert "-javaagent:/etc/trino/otel/opentelemetry-javaagent.jar" in jvm


def test_drain_window_outlasts_a_query(configured, monkeypatch):
    """A pod killed at the default 30s cuts running queries; Trino drains on SIGTERM only if it is
    given the time."""
    monkeypatch.delenv("PROVISA_ENGINE_DRAIN_SECONDS")
    spec = prov._deployment_manifest("shared_1", "shared")["spec"]["template"]["spec"]
    assert spec["terminationGracePeriodSeconds"] == 600


def test_shared_lane_carries_its_resource_groups(configured):
    """REQ-1450: the per-tenant queues are a ConfigMap on the shared lane and absent on an isolated
    engine, which has no other tenant to be queued against."""
    shared = prov._config_manifest("shared_1", '{"rootGroups": []}')["data"]
    assert "resource-groups.json" in shared
    assert (
        "resource-groups.config-file=/etc/trino/resource-groups.json"
        in (shared["resource-groups.properties"])
    )
    assert "resource-groups.json" not in prov._config_manifest("org_acme", None)["data"]


# ---- readiness is the cluster's word, not the coordinator's ------------------


@pytest.mark.asyncio
async def test_ready_waits_for_readyreplicas(monkeypatch, configured):
    """/v1/info answers ``starting=false`` as soon as the process is up, which on a cluster is true
    before the pod is in the Service's endpoint set. Releasing a query there runs it against an
    engine that is not yet whole."""
    seen: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if "/deployments/" in request.url.path:
            seen.append(1)
            ready = 1 if len(seen) >= 3 else 0
            return httpx.Response(200, json={"status": {"readyReplicas": ready}})
        return _cluster_get()

    _mock_api(monkeypatch, handler)
    monkeypatch.setattr(prov.asyncio, "sleep", _no_sleep)
    await prov._await_ready("shared_1")
    assert len(seen) == 3


@pytest.mark.asyncio
async def test_ready_times_out_rather_than_releasing_a_query(monkeypatch, configured):
    monkeypatch.setenv("PROVISA_ENGINE_READY_TIMEOUT", "0")

    def handler(request: httpx.Request) -> httpx.Response:
        if "/deployments/" in request.url.path:
            return httpx.Response(200, json={"status": {"readyReplicas": 0}})
        return _cluster_get()

    _mock_api(monkeypatch, handler)
    with pytest.raises(prov.K8sProvisioningError) as excinfo:
        await prov._await_ready("shared_1")
    assert "readyReplicas=0" in str(excinfo.value)


async def _no_sleep(_seconds: float) -> None:
    return None


# ---- node count is the real one ----------------------------------------------


@pytest.mark.asyncio
async def test_node_count_counts_ready_nodes(monkeypatch, configured):
    """Read from the node list, not from the pool's ``initialNodeCount``: that field records what
    the pool was last SET to, and the autoscaler moves the real count underneath it."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/nodes":
            return httpx.Response(
                200,
                json={
                    "items": [
                        {"status": {"conditions": [{"type": "Ready", "status": "True"}]}},
                        {"status": {"conditions": [{"type": "Ready", "status": "False"}]}},
                    ]
                },
            )
        return _cluster_get()

    calls = _mock_api(monkeypatch, handler)
    assert await prov.node_pool_size("shared_1") == 1
    selector = [c for c in calls if c.url.path == "/api/v1/nodes"][0].url.params["labelSelector"]
    assert selector == "provisa.dev/shard=shared_1"


# ---- the wake sequence -------------------------------------------------------


@pytest.mark.asyncio
async def test_wake_scales_the_pool_before_applying_the_engine(monkeypatch, configured):
    """An empty pool bills nothing; a node with no pods on it bills in full. Idle-to-zero therefore
    scales the POOL, and a wake has to put a node back before a pod has anywhere to land."""
    state = {"nodes": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/api/v1/nodes":
            items = [{"status": {"conditions": [{"type": "Ready", "status": "True"}]}}] * state[
                "nodes"
            ]
            return httpx.Response(200, json={"items": items})
        if path.endswith(":setSize"):
            state["nodes"] = json.loads(request.content)["nodeCount"]
            return httpx.Response(200, json={"name": "op/1"})
        if "/operations/" in path:
            return httpx.Response(200, json={"status": "DONE"})
        if "/deployments/" in path and request.method == "GET":
            return httpx.Response(200, json={"status": {"readyReplicas": 1}})
        if request.method == "PATCH":
            return httpx.Response(200, json={"metadata": {"name": "applied"}})
        return _cluster_get()

    calls = _mock_api(monkeypatch, handler)
    monkeypatch.setattr(prov.asyncio, "sleep", _no_sleep)

    result = await prov.ensure_shard_running("shared_1", resource_groups='{"rootGroups": []}')

    assert result == {
        "shard": "shared_1",
        "host": "trino-shared-1.provisa-engines.svc.provisa-saas-engine.internal",
        "port": 8080,
    }
    order = [c.url.path for c in calls]
    sized = next(i for i, p in enumerate(order) if p.endswith(":setSize"))
    applied = next(i for i, (p, c) in enumerate(zip(order, calls)) if c.method == "PATCH")
    assert sized < applied, "the node pool must be up before the engine is applied"

    kinds = [
        json.loads(c.content)["kind"] for c in calls if c.method == "PATCH" and b"kind" in c.content
    ]
    assert kinds == ["ConfigMap", "Service", "Deployment"]


@pytest.mark.asyncio
async def test_a_warm_shard_is_not_rescaled(monkeypatch, configured):
    """The wake runs on the query path under the registry lock, so the already-warm case has to be
    cheap — and resizing a pool that already has its node would be a several-minute no-op."""

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/api/v1/nodes":
            return httpx.Response(
                200,
                json={"items": [{"status": {"conditions": [{"type": "Ready", "status": "True"}]}}]},
            )
        if "/deployments/" in path and request.method == "GET":
            return httpx.Response(200, json={"status": {"readyReplicas": 1}})
        if request.method == "PATCH":
            return httpx.Response(200, json={})
        return _cluster_get()

    calls = _mock_api(monkeypatch, handler)
    await prov.ensure_shard_running("shared_1")
    assert not [c for c in calls if c.url.path.endswith(":setSize")]


@pytest.mark.asyncio
async def test_scale_to_zero_drains_before_taking_the_node(monkeypatch, configured):
    """Deployment first, then the pool. Dropping the node out from under a live pod does not let
    Trino finish what is running; scaling the pod down does."""
    order: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/scale"):
            order.append("scale-pod")
            return httpx.Response(200, json={})
        if path.endswith(":setSize"):
            order.append("scale-pool")
            assert json.loads(request.content)["nodeCount"] == 0
            return httpx.Response(200, json={"name": "op/1"})
        if "/operations/" in path:
            return httpx.Response(200, json={"status": "DONE"})
        return _cluster_get()

    _mock_api(monkeypatch, handler)
    result = await prov.scale_shard_to_zero("shared_1")
    assert order == ["scale-pod", "scale-pool"]
    assert result["state"] == "stopped"


# ---- failures say what the API said ------------------------------------------


@pytest.mark.asyncio
async def test_errors_carry_the_api_message(monkeypatch, configured):
    def handler(request: httpx.Request) -> httpx.Response:
        if "/deployments/" in request.url.path:
            return httpx.Response(
                403, json={"message": "deployments.apps is forbidden", "kind": "Status"}
            )
        return _cluster_get()

    _mock_api(monkeypatch, handler)
    with pytest.raises(prov.K8sProvisioningError) as excinfo:
        await prov.shard_status("shared_1")
    assert "deployments.apps is forbidden" in str(excinfo.value)
