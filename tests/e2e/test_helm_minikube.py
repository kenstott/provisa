# Copyright (c) 2026 Kenneth Stott
# Canary: b7e2f4a1-3c8d-4e9b-a5f0-1d6c2b8e3f7a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E tests for Helm chart deployment on minikube (REQ-056 / Phase M).

Requires minikube running via Docker driver:
    minikube start --driver=docker
    python -m pytest tests/e2e/test_helm_minikube.py -v
"""

from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
import time
import urllib.request
from pathlib import Path

import pytest

# `cluster`: this test deploys the Helm chart to a minikube profile of its own, created for this
# run and deleted after it. Run it through the one-heavy-job slot; it stops no other container.
# Every minikube, kubectl and helm call is pinned to that profile's context (see _pin), so it can
# never reach whatever cluster the machine's current kube context names. Runs in its own lane
# (pytest -m cluster); deselected from the default suite (see pyproject addopts).
CHART_DIR = Path(__file__).parents[2] / "helm" / "provisa"
RELEASE = "provisa-test"
NAMESPACE = "provisa-e2e"
# Every external call here has its own deadline, named below, so a stall fails with the call's own
# message ("minikube image load failed", "helm install failed" + the pod report). The module-scoped
# install runs inside the first test's pytest-timeout and its teardown inside the last's, so the
# per-test bound (CLUSTER_TEST_BOUND_S, the module's timeout mark; it replaces the lane's 900 s) is
# the sum of every deadline one test can be charged, and each named deadline fires before it.
# tests/unit/test_helm_minikube_bounds.py holds this sum and the cluster job's timeout-minutes to
# it.
DOWNLOAD_S = 300  # the minikube binary, when it is not on PATH
MINIKUBE_STATUS_S = 10
MINIKUBE_START_S = 600
KUBE_CONTEXT_S = 10
DOCKER_BUILD_S = 600
DOCKER_TAG_S = 10
# A fresh profile holds no image yet, so the whole image is copied in: a deadline sized for a
# multi-gigabyte copy on a loaded machine.
IMAGE_LOAD_S = 1500
MINIKUBE_SSH_S = 30
MINIKUBE_DELETE_S = 300
NAMESPACE_DELETE_S = 300  # --wait=true: its PVCs must be gone before the hostpath dir is wiped
KUBECTL_S = 60
# helm's own --wait. The install (a cold pull of every image into minikube) gets the longer one.
HELM_INSTALL_WAIT_S = 600
HELM_WAIT_S = 360
_HELM_SLACK_S = 60  # past helm's --wait, for helm to report and exit
# _why_not_ready stops issuing kubectl calls once this is spent; the call in flight may add one
# KUBECTL_S.
REPORT_BUDGET_S = 180
_REPORT_S = REPORT_BUDGET_S + KUBECTL_S
_SECRETS = 3  # master key, session key, break-glass password

_SETUP_S = (
    DOWNLOAD_S
    + MINIKUBE_STATUS_S
    + MINIKUBE_START_S
    + KUBE_CONTEXT_S
    + DOCKER_BUILD_S
    + IMAGE_LOAD_S
    + DOCKER_TAG_S
    + IMAGE_LOAD_S
    + NAMESPACE_DELETE_S
    + MINIKUBE_SSH_S
    + KUBECTL_S  # create namespace
    + _SECRETS * KUBECTL_S
    + HELM_INSTALL_WAIT_S
    + _HELM_SLACK_S
    + _REPORT_S
)
# The longest test body: test_helm_upgrade_scales_worker_replicas, two upgrades, each of which may
# end in a pod report, plus the pod listing between them.
_LONGEST_TEST_S = 2 * (HELM_WAIT_S + _HELM_SLACK_S + _REPORT_S) + KUBECTL_S
_TEARDOWN_S = (
    KUBECTL_S  # helm uninstall
    + KUBECTL_S  # delete namespace
    + MINIKUBE_SSH_S
    + MINIKUBE_DELETE_S
)
CLUSTER_TEST_BOUND_S = _SETUP_S + _LONGEST_TEST_S + _TEARDOWN_S

pytestmark = [pytest.mark.e2e, pytest.mark.cluster, pytest.mark.timeout(CLUSTER_TEST_BOUND_S)]
# This run's own minikube profile, and so its own kube context. Never the default profile, and
# never the machine's current context, which may name a production cluster.
PROFILE = f"provisa-itest-{os.getpid()}"

# REQ-1265: the chart renders only once a provider is chosen. This cluster runs the break-glass
# account alone, so the install proves the auth block reaches the API.
_AUTH_SETS = [
    "--set",
    "auth.provider=local",
    "--set",
    "auth.sessionSecret.existingSecret=provisa-session",
    "--set",
    "auth.breakGlass.username=platform-admin",
    "--set",
    "auth.breakGlass.existingSecret=provisa-break-glass",
]

# What this test deploys, stated once and passed whole to the install AND every upgrade. A
# `helm upgrade` re-renders from the chart defaults plus exactly the values it is given, so an
# upgrade that repeated only some of the install's values redeployed the rest at production sizes
# (2Gi Trino requests, 2GB per node, mongodb on, no exchange store) on this 6 GiB minikube, and
# the release never turned Ready.
# Minimal values: single replicas, no autoscaling, no ingress. flightService.type=ClusterIP avoids
# the LoadBalancer pending-IP stall in minikube. Trino probe timeouts are generous because minikube
# JVM startup is slow.
_CHART_SETS = [
    "--set",
    "encryption.existingSecret=provisa-master-key",
    *_AUTH_SETS,
    "--set",
    "provisa.replicaCount=1",
    "--set",
    "provisa.hpa.enabled=false",
    "--set",
    "trino.worker.autoscaling.enabled=false",
    "--set",
    "ingress.enabled=false",
    "--set",
    "provisa.flightService.type=ClusterIP",
    # CPU requests sized to the 4-CPU minikube: at the chart's production default (1 CPU per Trino
    # pod) the scaling test's second worker never schedules ("0/1 nodes are available: Insufficient
    # cpu", run 37318305658) beside the coordinator, the first worker and the rest of the release.
    "--set",
    "trino.coordinator.resources.requests.cpu=250m",
    "--set",
    "trino.worker.resources.requests.cpu=250m",
    "--set",
    "trino.coordinator.resources.requests.memory=512Mi",
    "--set",
    "trino.coordinator.resources.limits.memory=1Gi",
    "--set",
    "trino.worker.resources.requests.memory=512Mi",
    "--set",
    "trino.worker.resources.limits.memory=1Gi",
    # Trino query/FTE memory must fit inside the 1Gi container limit
    # above (query.max-memory-per-node must not exceed the JVM heap);
    # the chart defaults (2GB per node, 5GB FTE task) are sized for
    # production nodes and fail config validation on these minikube pods.
    "--set",
    "trino.memory.maxMemory=1GB",
    "--set",
    "trino.memory.maxMemoryPerNode=512MB",
    "--set",
    "trino.memory.maxTotalMemory=1GB",
    "--set",
    "trino.fte.taskMemory=512MB",
    "--set",
    "trino.coordinator.livenessProbe.initialDelaySeconds=180",
    "--set",
    "trino.coordinator.livenessProbe.periodSeconds=30",
    "--set",
    "trino.coordinator.livenessProbe.failureThreshold=10",
    "--set",
    "trino.coordinator.readinessProbe.initialDelaySeconds=60",
    "--set",
    "trino.coordinator.readinessProbe.failureThreshold=20",
    "--set",
    "mongodb.enabled=false",
    # Trino fault-tolerant execution (REQ-817) requires a shared exchange
    # store; the chart fails render if neither minio.enabled nor an
    # external trino.exchange.s3.endpoint is provided (no silent
    # fallback). Deploy in-cluster MinIO as that exchange store.
    "--set",
    "minio.enabled=true",
]

_LOCAL_BIN = Path.home() / ".local" / "bin"


def _acquire_minikube() -> None:
    """Download the minikube binary into ~/.local/bin (it runs in Docker, tests own this)."""
    system = platform.system().lower()
    machine = platform.machine().lower()
    arch = "arm64" if ("arm" in machine or "aarch64" in machine) else "amd64"
    url = f"https://storage.googleapis.com/minikube/releases/latest/minikube-{system}-{arch}"
    _LOCAL_BIN.mkdir(parents=True, exist_ok=True)
    dest = _LOCAL_BIN / "minikube"
    with urllib.request.urlopen(url, timeout=DOWNLOAD_S) as resp:  # noqa: S310 - fixed https URL
        dest.write_bytes(resp.read())
    dest.chmod(0o755)
    os.environ["PATH"] = f"{_LOCAL_BIN}:{os.environ['PATH']}"


def _ensure_tools() -> None:
    if shutil.which("helm") is None:
        pytest.fail("helm not found on PATH — install helm before running these tests")

    if shutil.which("minikube") is None:
        _acquire_minikube()

    try:
        status = subprocess.run(
            _pin(["minikube", "status", "--format={{.Host}}"]),
            capture_output=True,
            text=True,
            timeout=MINIKUBE_STATUS_S,
        )
        running = status.returncode == 0 and status.stdout.strip() == "Running"
    except subprocess.TimeoutExpired:
        running = False

    if not running:
        start = subprocess.run(
            _pin(["minikube", "start", "--driver=docker", "--memory=6144", "--cpus=4"]),
            capture_output=True,
            text=True,
            timeout=MINIKUBE_START_S,
        )
        if start.returncode != 0:
            pytest.fail(f"minikube start failed:\n{start.stderr}")


def _pin(cmd: list[str]) -> list[str]:
    """``cmd`` pinned to this run's profile: no cluster command may use the current context."""
    tool = cmd[0]
    if tool == "minikube":
        return [tool, "-p", PROFILE, *cmd[1:]]
    if tool == "kubectl":
        return [tool, f"--context={PROFILE}", *cmd[1:]]
    if tool == "helm":
        return [tool, f"--kube-context={PROFILE}", *cmd[1:]]
    return cmd


def _assert_own_context() -> None:
    """Refuse to go on unless this run's profile has a context of its own to pin to."""
    names = subprocess.run(
        ["kubectl", "config", "get-contexts", "-o", "name"],
        capture_output=True,
        text=True,
        timeout=KUBE_CONTEXT_S,
    ).stdout.split()
    if PROFILE not in names:
        pytest.fail(f"minikube profile {PROFILE!r} has no kube context; refusing to run")


def _run(cmd: list[str], timeout_s: int = KUBECTL_S) -> subprocess.CompletedProcess:
    return subprocess.run(_pin(cmd), capture_output=True, text=True, timeout=timeout_s)


def _helm_deploy(*, worker_replicas: int, install: bool = False) -> subprocess.CompletedProcess:
    """Install or upgrade the release with _CHART_SETS and this many Trino workers, waiting for it
    to turn Ready."""
    wait_s = HELM_INSTALL_WAIT_S if install else HELM_WAIT_S
    return _run(
        [
            "helm",
            "upgrade",
            *(["--install"] if install else []),
            RELEASE,
            str(CHART_DIR),
            f"--namespace={NAMESPACE}",
            *_CHART_SETS,
            "--set",
            f"trino.worker.replicaCount={worker_replicas}",
            "--wait",
            f"--timeout={wait_s}s",
        ],
        wait_s + _HELM_SLACK_S,
    )


def _why_not_ready() -> str:
    """What a timed-out ``helm install --wait`` was waiting on: every pod's state, and for each pod
    that is not Ready its events and the end of each container's log (the previous run's too, for
    one that restarted). "context deadline exceeded" alone names nothing. Stops issuing calls once
    REPORT_BUDGET_S is spent, saying so, so the report itself stays inside the test's bound."""
    deadline = time.monotonic() + REPORT_BUDGET_S
    lines: list[str] = []

    class _Spent(Exception):
        pass

    def kubectl(*args: str) -> subprocess.CompletedProcess:
        if time.monotonic() >= deadline:
            raise _Spent
        return _kubectl(*args)

    try:
        pods = kubectl("get", "pods", "-o", "wide")
        lines.append(f"=== pods\n{pods.stdout}{pods.stderr}")
        listed = kubectl("get", "pods", "-o", "json")
        if listed.returncode != 0:
            lines.append(f"=== kubectl get pods -o json failed\n{listed.stderr}")
            return "\n".join(lines)
        for pod in json.loads(listed.stdout)["items"]:
            ready = any(
                c["type"] == "Ready" and c["status"] == "True"
                for c in pod["status"].get("conditions", [])
            )
            if ready:
                continue
            name = pod["metadata"]["name"]
            described = kubectl("describe", "pod", name)
            lines.append(f"=== describe {name}\n{described.stdout[-4000:]}")
            # Per container: `--all-containers --previous` returns nothing at all when any
            # container (an init container) has no previous run, which hid a crash-looping
            # container's last log.
            for container in pod["spec"]["containers"]:
                for previous in (False, True):
                    flags = ["--previous"] if previous else []
                    logs = kubectl("logs", name, "-c", container["name"], "--tail=120", *flags)
                    label = "previous logs" if previous else "logs"
                    lines.append(
                        f"=== {label} {name}/{container['name']}\n{logs.stdout}{logs.stderr}"
                    )
        # Reachability between pods: which services have ready endpoints, and what the chart's
        # network policies admit (a policy that leaves out a client shows up as a connect timeout).
        endpoints = kubectl("get", "endpoints", "-o", "wide")
        lines.append(f"=== endpoints\n{endpoints.stdout}{endpoints.stderr}")
        policies = kubectl("get", "networkpolicy", "-o", "yaml")
        lines.append(f"=== network policies\n{policies.stdout[-6000:]}{policies.stderr}")
        # The API's boot provisions catalogs on the Trino coordinator; its side of a stalled boot.
        trino = kubectl("logs", "-l", "app=trino,role=coordinator", "--tail=120")
        lines.append(f"=== trino coordinator logs\n{trino.stdout}{trino.stderr}")
    except _Spent:
        lines.append(f"=== report stopped: its {REPORT_BUDGET_S} s budget is spent")
    return "\n".join(lines)


def _kubectl(*args: str) -> subprocess.CompletedProcess:
    return _run(["kubectl", f"--namespace={NAMESPACE}", *args])


def _get_pods() -> list[dict]:
    result = _kubectl("get", "pods", "-o", "json")
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)["items"]


@pytest.fixture(scope="module", autouse=True)
def helm_install(request):
    """Install the Provisa Helm chart into a dedicated minikube namespace."""
    # A kubeconfig of this run's own: `minikube start` writes and selects its context in whatever
    # kubeconfig is in force, and the machine's own may hold a production context the operator
    # has selected. With this one, nothing outside the run's own file is read or changed.
    import tempfile

    previous = os.environ.get("KUBECONFIG")
    os.environ["KUBECONFIG"] = str(Path(tempfile.mkdtemp(prefix="kubeconfig-")) / "config")

    def _cleanup() -> None:
        subprocess.run(
            ["minikube", "delete", "-p", PROFILE], capture_output=True, timeout=MINIKUBE_DELETE_S
        )
        if previous is None:
            os.environ.pop("KUBECONFIG", None)
        else:
            os.environ["KUBECONFIG"] = previous

    # Registered before the profile exists, so a failure anywhere below still deletes it.
    request.addfinalizer(_cleanup)
    _ensure_tools()
    _assert_own_context()

    # Build provisa:latest and load into minikube so IfNotPresent can find it
    repo_root = Path(__file__).parents[2]
    build = subprocess.run(
        [
            "docker",
            "build",
            "-f",
            str(repo_root / "Dockerfile.dev"),
            "-t",
            "provisa:latest",
            str(repo_root),
        ],
        capture_output=True,
        text=True,
        timeout=DOCKER_BUILD_S,
    )
    if build.returncode != 0:
        pytest.fail(f"docker build failed:\n{build.stdout}\n{build.stderr}")

    load = subprocess.run(
        _pin(["minikube", "image", "load", "provisa:latest"]),
        capture_output=True,
        text=True,
        timeout=IMAGE_LOAD_S,
    )
    if load.returncode != 0:
        pytest.fail(f"minikube image load failed:\n{load.stdout}\n{load.stderr}")

    # Tag and load zaychik (Arrow Flight SQL proxy). Built by docker-compose as
    # provisa-zaychik:latest; re-tagged to zaychik:latest for helm chart reference.
    subprocess.run(
        ["docker", "tag", "provisa-zaychik:latest", "zaychik:latest"],
        capture_output=True,
        timeout=DOCKER_TAG_S,
    )
    zload = subprocess.run(
        _pin(["minikube", "image", "load", "zaychik:latest"]),
        capture_output=True,
        text=True,
        timeout=IMAGE_LOAD_S,
    )
    if zload.returncode != 0:
        pytest.fail(f"minikube image load (zaychik) failed:\n{zload.stdout}\n{zload.stderr}")

    # Delete any existing namespace+PVCs from a prior run, then wipe the
    # hostpath dir. Order matters: PVCs must be gone before the dir is removed
    # so the provisioner doesn't re-claim stale data with wrong permissions.
    _run(
        ["kubectl", "delete", "namespace", NAMESPACE, "--ignore-not-found=true", "--wait=true"],
        NAMESPACE_DELETE_S,
    )
    subprocess.run(
        _pin(["minikube", "ssh", f"sudo rm -rf /tmp/hostpath-provisioner/{NAMESPACE}/"]),
        capture_output=True,
        text=True,
        timeout=MINIKUBE_SSH_S,
    )

    _run(["kubectl", "create", "namespace", NAMESPACE])
    # The chart requires the deployment's master key (a Secret, or a persistent data volume):
    # without one it does not render. A key of this test's own, held as a Secret.
    import base64

    _run(
        [
            "kubectl",
            "create",
            "secret",
            "generic",
            "provisa-master-key",
            f"--namespace={NAMESPACE}",
            f"--from-literal=master-key={base64.b64encode(os.urandom(32)).decode()}",
        ]
    )
    # REQ-1265: the chart is installed with auth.provider=local, the break-glass account alone;
    # its session key and password come from Secrets, as an operator supplies them.
    for name, key in (("provisa-session", "session-secret"), ("provisa-break-glass", "password")):
        _run(
            [
                "kubectl",
                "create",
                "secret",
                "generic",
                name,
                f"--namespace={NAMESPACE}",
                f"--from-literal={key}={base64.b64encode(os.urandom(36)).decode()}",
            ]
        )

    result = _helm_deploy(worker_replicas=1, install=True)
    if result.returncode != 0:
        pytest.fail(f"helm install failed:\n{result.stdout}\n{result.stderr}\n{_why_not_ready()}")

    yield

    # Teardown
    _run(["helm", "uninstall", RELEASE, f"--namespace={NAMESPACE}"])
    _run(["kubectl", "delete", "namespace", NAMESPACE, "--ignore-not-found=true"])
    subprocess.run(
        _pin(["minikube", "ssh", f"sudo rm -rf /tmp/hostpath-provisioner/{NAMESPACE}/"]),
        capture_output=True,
        text=True,
        timeout=MINIKUBE_SSH_S,
    )


class TestPodsRunning:
    def test_all_pods_are_running(self):
        """Every long-running pod is Running after helm install; a Job's pod (the exchange bucket
        job) ends Succeeded, which is its healthy end."""
        pods = _get_pods()
        assert len(pods) > 0, "No pods found in namespace"
        for pod in pods:
            phase = pod["status"].get("phase", "Unknown")
            name = pod["metadata"]["name"]
            owners = {o["kind"] for o in pod["metadata"].get("ownerReferences", [])}
            healthy = ("Running", "Succeeded") if "Job" in owners else ("Running",)
            assert phase in healthy, (
                f"Pod {name!r} is in phase {phase!r}, not {' or '.join(healthy)}"
            )

    def test_provisa_deployment_pod_exists(self):
        """At least one pod of the API Deployment (app: provisa) is running. Selected by label:
        every pod of the release has "provisa" in its name, a completed Job's among them."""
        pods = _get_pods()
        provisa_pods = [p for p in pods if p["metadata"].get("labels", {}).get("app") == "provisa"]
        assert len(provisa_pods) >= 1, "No provisa pods found"
        for pod in provisa_pods:
            assert pod["status"]["phase"] == "Running"

    def test_trino_coordinator_pod_exists(self):
        """Trino coordinator pod is running."""
        pods = _get_pods()
        trino_pods = [p for p in pods if "trino-coordinator" in p["metadata"]["name"]]
        assert len(trino_pods) >= 1, "No trino-coordinator pod found"
        for pod in trino_pods:
            assert pod["status"]["phase"] == "Running"

    def test_trino_worker_pod_exists(self):
        """Trino worker pod is running."""
        pods = _get_pods()
        worker_pods = [p for p in pods if "trino-worker" in p["metadata"]["name"]]
        assert len(worker_pods) >= 1, "No trino-worker pod found"
        for pod in worker_pods:
            assert pod["status"]["phase"] == "Running"

    def test_postgresql_pod_exists(self):
        """PostgreSQL pod is running."""
        pods = _get_pods()
        pg_pods = [
            p
            for p in pods
            if "postgresql" in p["metadata"]["name"] or "postgres" in p["metadata"]["name"]
        ]
        assert len(pg_pods) >= 1, "No postgresql pod found"
        for pod in pg_pods:
            assert pod["status"]["phase"] == "Running"

    def test_no_pods_in_crash_loop(self):
        """No pods are in CrashLoopBackOff."""
        pods = _get_pods()
        for pod in pods:
            for container in pod["status"].get("containerStatuses", []):
                state = container.get("state", {})
                waiting = state.get("waiting", {})
                reason = waiting.get("reason", "")
                assert reason != "CrashLoopBackOff", (
                    f"Pod {pod['metadata']['name']!r} container "
                    f"{container['name']!r} is in CrashLoopBackOff"
                )


class TestServices:
    def test_provisa_service_exists(self):
        """Provisa ClusterIP service is created."""
        result = _kubectl("get", "service", "-o", "json")
        assert result.returncode == 0
        services = json.loads(result.stdout)["items"]
        names = [s["metadata"]["name"] for s in services]
        assert any("provisa" in n for n in names), f"No provisa service found; services: {names}"

    def test_trino_service_exists(self):
        """Trino service is created."""
        result = _kubectl("get", "service", "-o", "json")
        assert result.returncode == 0
        services = json.loads(result.stdout)["items"]
        names = [s["metadata"]["name"] for s in services]
        assert any("trino" in n for n in names), f"No trino service found; services: {names}"


class TestConfigMaps:
    def test_provisa_configmap_exists(self):
        """Provisa ConfigMap is created with expected keys."""
        result = _kubectl("get", "configmap", "-o", "json")
        assert result.returncode == 0
        cms = json.loads(result.stdout)["items"]
        names = [c["metadata"]["name"] for c in cms]
        assert any("provisa" in n for n in names), (
            f"No provisa configmap found; configmaps: {names}"
        )

    def test_trino_configmap_exists(self):
        """Trino ConfigMap is created."""
        result = _kubectl("get", "configmap", "-o", "json")
        assert result.returncode == 0
        cms = json.loads(result.stdout)["items"]
        names = [c["metadata"]["name"] for c in cms]
        assert any("trino" in n for n in names), f"No trino configmap found; configmaps: {names}"


class TestWorkerScaling:
    def test_trino_worker_hpa_not_present_when_disabled(self):
        """HPA is absent when autoscaling is disabled (our test install disables it)."""
        result = _kubectl("get", "hpa", "-o", "json")
        if result.returncode != 0:
            # HPA resource may not exist at all — that's fine
            return
        hpa_items = json.loads(result.stdout).get("items", [])
        trino_hpas = [h for h in hpa_items if "trino-worker" in h["metadata"]["name"]]
        assert len(trino_hpas) == 0, (
            "Trino worker HPA should not exist when autoscaling is disabled"
        )

    def test_helm_upgrade_scales_worker_replicas(self):
        """helm upgrade --set trino.worker.replicaCount=2 adds a second worker pod."""
        result = _helm_deploy(worker_replicas=2)
        assert result.returncode == 0, f"helm upgrade failed:\n{result.stderr}\n{_why_not_ready()}"

        pods = _get_pods()
        worker_pods = [p for p in pods if "trino-worker" in p["metadata"]["name"]]
        assert len(worker_pods) >= 2, (
            f"Expected ≥2 worker pods after scaling to replicaCount=2, got {len(worker_pods)}"
        )

        # Scale back to 1, so the module's later tests see the release as installed.
        result = _helm_deploy(worker_replicas=1)
        assert result.returncode == 0, (
            f"helm upgrade (scale back) failed:\n{result.stderr}\n{_why_not_ready()}"
        )


class TestAuthReachesTheApi:
    """REQ-1265: the provider chosen at install is the one the running API is configured with."""

    def test_the_api_config_carries_the_break_glass_block(self):
        import yaml

        result = _kubectl("get", "configmap", f"{RELEASE}-config", "-o", "json")
        assert result.returncode == 0, result.stderr
        config = yaml.safe_load(json.loads(result.stdout)["data"]["provisa.yaml"])
        assert config["auth"]["provider"] == "basic"
        assert config["auth"]["allow_registration"] is False
        assert config["auth"]["superuser"]["username"] == "platform-admin"

    def test_the_api_reads_the_password_from_the_secret(self):
        result = _kubectl("get", "deployment", f"{RELEASE}-provisa", "-o", "json")
        assert result.returncode == 0, result.stderr
        containers = json.loads(result.stdout)["spec"]["template"]["spec"]["containers"]
        env = {e["name"]: e for c in containers if c["name"] == "provisa" for e in c["env"]}
        ref = env["PROVISA_AUTH_BREAK_GLASS_PASSWORD"]["valueFrom"]["secretKeyRef"]
        assert ref == {"name": "provisa-break-glass", "key": "password"}
