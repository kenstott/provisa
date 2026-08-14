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
here is that the replacement does not: the pod is Guaranteed-QoS on a node Autopilot provisions for
it alone, it is only declared usable once the CLUSTER says a replica is ready, and a stop waits for
the drain to finish rather than reporting a shard free while it is still billing.

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
    monkeypatch.setenv("PROVISA_ENGINE_CLUSTER_ZONE", "us-central1-a")
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
    assert prov.shard_endpoint("shared_1") == (
        "trino-shared-1.provisa-engines.svc.provisa-saas-engine.internal",
        8080,
    )


# ---- the pod cannot contend with anything else --------------------------------


def test_pod_is_guaranteed_qos(configured):
    """The co-tenancy being retired: on Autopilot there is no pool to select between, so what keeps
    one shard off another's CPU is requests equal to limits — a Guaranteed pod Autopilot sizes a
    node for (REQ-1464). Best-effort or burstable here is the shared-host problem again."""
    spec = prov._deployment_manifest("shared_1", "shared")["spec"]["template"]["spec"]
    resources = spec["containers"][0]["resources"]
    assert resources["requests"] == resources["limits"]


def test_autopilot_pins_the_zone_and_selects_no_pool(configured):
    """An Autopilot cluster is REGIONAL — the API refuses a zonal one — so an unpinned pod can be
    placed in a zone the control-plane VM is not in and every result set is billed as cross-zone
    egress. The zone selector is the whole placement stanza: a custom pool label would leave the
    pod unschedulable, because Autopilot has no pools to select between (REQ-1465)."""
    spec = prov._deployment_manifest("shared_1", "shared")["spec"]["template"]["spec"]
    assert spec["nodeSelector"] == {"topology.kubernetes.io/zone": "us-central1-a"}
    assert "tolerations" not in spec


def test_standard_pins_the_pod_to_its_own_shard_pool(monkeypatch, configured):
    """The other topology (REQ-1465). Each shard owns a pool autoscaling 0..1, tainted so GKE's
    system Deployments cannot hold the node up; the toleration is what lets this pod onto it and
    the selector is what keeps it off any other. Nothing else about provisioning changes — the same
    replica patch that starts the pod is what brings the node."""
    monkeypatch.setenv("PROVISA_ENGINE_CLUSTER_MODE", "standard")
    spec = prov._deployment_manifest("shared_1", "shared")["spec"]["template"]["spec"]
    assert spec["nodeSelector"] == {
        "topology.kubernetes.io/zone": "us-central1-a",
        "provisa.dev/shard": "shared_1",
    }
    assert spec["tolerations"] == [
        {
            "key": "provisa.dev/shard",
            "operator": "Equal",
            "value": "shared_1",
            "effect": "NoSchedule",
        }
    ]


def test_an_unknown_cluster_mode_is_not_guessed_at(monkeypatch, configured):
    """A mis-set mode yields pods that stay Pending forever on a Standard cluster — the failure
    that looks like a slow wake rather than a misconfiguration. It fails at settings time instead."""
    monkeypatch.setenv("PROVISA_ENGINE_CLUSTER_MODE", "autopilo")
    with pytest.raises(prov.K8sProvisioningError) as excinfo:
        prov.provisioner_settings()
    assert "autopilo" in str(excinfo.value)


def test_cpu_is_bounded(configured):
    """The Docker provisioner set ``Memory`` and never ``NanoCpus``, so one org's scan could take
    the whole host. A limit equal to the request is what removes that."""
    container = prov._deployment_manifest("shared_1", "shared")["spec"]["template"]["spec"][
        "containers"
    ][0]
    assert container["resources"]["limits"]["cpu"] == container["resources"]["requests"]["cpu"]
    assert container["resources"]["limits"]["memory"] == "24Gi"


def test_query_memory_fits_under_the_heap_headroom(configured):
    """Trino refuses to start when max query memory per node plus its 30% heap headroom exceeds
    the heap, and the heap is only 70% of the pod limit (-XX:MaxRAMPercentage=70). Budgeting 0.6 of
    the POD limit shipped a 24 GiB shard asking 14 GB against a 16.8 GiB heap, and every wake
    crash-looped with "Invalid memory configuration"."""
    config = prov._config_manifest("shared_1", None)["data"]["config.properties"]
    per_node = int(
        next(
            line.split("=", 1)[1] for line in config.splitlines() if line.startswith("query.max-memory-per-node=")
        ).removesuffix("GB")
    )
    pod_gib = int(
        prov._deployment_manifest("shared_1", "shared")["spec"]["template"]["spec"]["containers"][0][
            "resources"
        ]["limits"]["memory"].removesuffix("Gi")
    )
    heap = pod_gib * 0.7
    assert per_node + heap * 0.3 <= heap


def test_the_shard_service_is_headless(configured):
    """The control plane dials a shard from a VM in the VPC but outside the cluster, and a ClusterIP
    routes only inside it: the name resolved to 10.24.6.12 and every connect timed out while the pod
    IP served /v1/info. Headless publishes the VPC-routable pod IP instead."""
    assert prov._service_manifest("shared_1")["spec"]["clusterIP"] == "None"


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


# ---- status is the Deployment's word -----------------------------------------


@pytest.mark.asyncio
async def test_status_reads_state_off_the_deployment(monkeypatch, configured):
    """No node count anywhere: on Autopilot a shard at zero replicas has no node by construction, so
    what separates stopped from starting is spec.replicas, and ready is readyReplicas (REQ-1464)."""

    body = {"spec": {"replicas": 0}, "status": {}}

    def handler(request: httpx.Request) -> httpx.Response:
        if "/deployments/" in request.url.path:
            return httpx.Response(200, json=body)
        return _cluster_get()

    calls = _mock_api(monkeypatch, handler)

    assert await prov.shard_status("shared_1") == {
        "shard": "shared_1",
        "state": "stopped",
        "ready_replicas": 0,
        "replicas": 0,
    }

    body["spec"]["replicas"] = 1
    assert (await prov.shard_status("shared_1"))["state"] == "starting"

    body["status"]["readyReplicas"] = 1
    assert (await prov.shard_status("shared_1"))["state"] == "ready"

    assert not [c for c in calls if c.url.path == "/api/v1/nodes"]


@pytest.mark.asyncio
async def test_status_of_a_shard_that_was_never_applied(monkeypatch, configured):
    """A 404 is a shard that has never existed, which is not an error — it is what the first wake
    for a newly-provisioned lane sees."""

    def handler(request: httpx.Request) -> httpx.Response:
        if "/deployments/" in request.url.path:
            return httpx.Response(404, json={"kind": "Status", "message": "not found"})
        return _cluster_get()

    _mock_api(monkeypatch, handler)
    assert await prov.shard_status("shared_1") == {
        "shard": "shared_1",
        "state": "absent",
        "ready_replicas": 0,
    }


# ---- the wake sequence -------------------------------------------------------


@pytest.mark.asyncio
async def test_wake_applies_config_then_service_then_deployment(monkeypatch, configured):
    """There is no pool to size first — Autopilot provisions a node to fit the pod. What still has
    to hold is the apply ORDER: the coordinator reads its config from the ConfigMap at start, so a
    Deployment applied ahead of it would boot against the previous generation's config."""

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
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
    assert not [c for c in calls if c.url.path.endswith(":setSize")]

    kinds = [
        json.loads(c.content)["kind"] for c in calls if c.method == "PATCH" and b"kind" in c.content
    ]
    assert kinds == ["ConfigMap", "Service", "Deployment"]


@pytest.mark.asyncio
async def test_scale_to_zero_waits_for_the_pod_to_go(monkeypatch, configured):
    """The PATCH returns immediately, but Trino drains on SIGTERM and finishes what is running. A
    stop that returned there would report a shard free while its pod — and so its node — is still
    billing, and would let a wake race a terminating pod."""
    monkeypatch.setenv("PROVISA_ENGINE_DRAIN_SECONDS", "600")
    observed = [2, 1, 0]

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/scale"):
            assert json.loads(request.content) == {"spec": {"replicas": 0}}
            return httpx.Response(200, json={})
        if "/deployments/" in path and request.method == "GET":
            return httpx.Response(200, json={"status": {"replicas": observed.pop(0)}})
        return _cluster_get()

    calls = _mock_api(monkeypatch, handler)
    monkeypatch.setattr(prov.asyncio, "sleep", _no_sleep)

    result = await prov.scale_shard_to_zero("shared_1")
    assert result["state"] == "stopped"
    assert observed == []
    assert not [c for c in calls if c.url.path.endswith(":setSize")]


@pytest.mark.asyncio
async def test_scale_to_zero_fails_rather_than_calling_a_live_shard_stopped(monkeypatch, configured):
    """PROVISA_ENGINE_DRAIN_SECONDS bounds how long a pod may take to go. Past it the shard is still
    billing, and saying otherwise would hand the reaper a stop it never got."""
    monkeypatch.setenv("PROVISA_ENGINE_DRAIN_SECONDS", "0")

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/scale"):
            return httpx.Response(200, json={})
        if "/deployments/" in path and request.method == "GET":
            return httpx.Response(200, json={"status": {"replicas": 1}})
        return _cluster_get()

    _mock_api(monkeypatch, handler)
    with pytest.raises(prov.K8sProvisioningError) as excinfo:
        await prov.scale_shard_to_zero("shared_1")
    assert "still had 1 replica" in str(excinfo.value)


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
