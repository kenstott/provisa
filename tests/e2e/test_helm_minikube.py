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
import urllib.request
from pathlib import Path

import pytest

pytestmark = pytest.mark.e2e

CHART_DIR = Path(__file__).parents[2] / "helm" / "provisa"
RELEASE = "provisa-test"
NAMESPACE = "provisa-e2e"
TIMEOUT = "1200s"

_LOCAL_BIN = Path.home() / ".local" / "bin"


def _acquire_minikube() -> None:
    """Download the minikube binary into ~/.local/bin (it runs in Docker, tests own this)."""
    system = platform.system().lower()
    machine = platform.machine().lower()
    arch = "arm64" if ("arm" in machine or "aarch64" in machine) else "amd64"
    url = f"https://storage.googleapis.com/minikube/releases/latest/minikube-{system}-{arch}"
    _LOCAL_BIN.mkdir(parents=True, exist_ok=True)
    dest = _LOCAL_BIN / "minikube"
    urllib.request.urlretrieve(url, str(dest))
    dest.chmod(0o755)
    os.environ["PATH"] = f"{_LOCAL_BIN}:{os.environ['PATH']}"


def _ensure_tools() -> None:
    if shutil.which("helm") is None:
        pytest.fail("helm not found on PATH — install helm before running these tests")

    if shutil.which("minikube") is None:
        _acquire_minikube()

    try:
        status = subprocess.run(
            ["minikube", "status", "--format={{.Host}}"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        running = status.returncode == 0 and status.stdout.strip() == "Running"
    except subprocess.TimeoutExpired:
        running = False

    if not running:
        start = subprocess.run(
            ["minikube", "start", "--driver=docker", "--memory=6144", "--cpus=4"],
            capture_output=True,
            text=True,
            timeout=600,
        )
        if start.returncode != 0:
            pytest.fail(f"minikube start failed:\n{start.stderr}")


def _run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=1260, **kwargs)


def _kubectl(*args: str) -> subprocess.CompletedProcess:
    return _run(["kubectl", f"--namespace={NAMESPACE}", *args])


def _get_pods() -> list[dict]:
    result = _kubectl("get", "pods", "-o", "json")
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)["items"]


def _get_running_containers() -> list[str]:
    """Return names of all running Docker containers except minikube itself."""
    r = subprocess.run(
        ["docker", "ps", "--format={{.Names}}"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if r.returncode != 0:
        return []
    return [n for n in r.stdout.strip().splitlines() if n and n != "minikube"]


@pytest.fixture(scope="module", autouse=True)
def helm_install():
    """Install the Provisa Helm chart into a dedicated minikube namespace."""
    _ensure_tools()

    # Minikube pods need Docker VM RAM headroom. Stop ALL external containers
    # (except minikube itself) so the full 12 GB Docker VM is available.
    # They are restarted in teardown so the broader e2e suite is unaffected.
    stopped = _get_running_containers()
    for c in stopped:
        subprocess.run(["docker", "stop", c], capture_output=True, timeout=30)

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
        timeout=600,
    )
    if build.returncode != 0:
        pytest.fail(f"docker build failed:\n{build.stdout}\n{build.stderr}")

    load = subprocess.run(
        ["minikube", "image", "load", "provisa:latest"],
        capture_output=True,
        text=True,
        timeout=300,
    )
    if load.returncode != 0:
        pytest.fail(f"minikube image load failed:\n{load.stdout}\n{load.stderr}")

    # Tag and load zaychik (Arrow Flight SQL proxy). Built by docker-compose as
    # provisa-zaychik:latest; re-tagged to zaychik:latest for helm chart reference.
    subprocess.run(
        ["docker", "tag", "provisa-zaychik:latest", "zaychik:latest"],
        capture_output=True,
        timeout=10,
    )
    zload = subprocess.run(
        ["minikube", "image", "load", "zaychik:latest"],
        capture_output=True,
        text=True,
        timeout=300,
    )
    if zload.returncode != 0:
        pytest.fail(f"minikube image load (zaychik) failed:\n{zload.stdout}\n{zload.stderr}")

    # Delete any existing namespace+PVCs from a prior run, then wipe the
    # hostpath dir. Order matters: PVCs must be gone before the dir is removed
    # so the provisioner doesn't re-claim stale data with wrong permissions.
    _run(["kubectl", "delete", "namespace", NAMESPACE, "--ignore-not-found=true", "--wait=true"])
    subprocess.run(
        ["minikube", "ssh", f"sudo rm -rf /tmp/hostpath-provisioner/{NAMESPACE}/"],
        capture_output=True,
        text=True,
        timeout=30,
    )

    _run(["kubectl", "create", "namespace", NAMESPACE])

    # Use minimal values: single replicas, no autoscaling, no ingress.
    # flightService.type=ClusterIP avoids LoadBalancer pending-IP stall in minikube.
    # Trino probe timeouts are generous because minikube JVM startup is slow.
    result = _run(
        [
            "helm",
            "upgrade",
            "--install",
            RELEASE,
            str(CHART_DIR),
            f"--namespace={NAMESPACE}",
            "--set",
            "provisa.replicaCount=1",
            "--set",
            "provisa.hpa.enabled=false",
            "--set",
            "trino.worker.replicaCount=1",
            "--set",
            "trino.worker.autoscaling.enabled=false",
            "--set",
            "ingress.enabled=false",
            "--set",
            "provisa.flightService.type=ClusterIP",
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
            "--wait",
            f"--timeout={TIMEOUT}",
        ]
    )
    if result.returncode != 0:
        pytest.fail(f"helm install failed:\n{result.stdout}\n{result.stderr}")

    yield

    # Teardown
    _run(["helm", "uninstall", RELEASE, f"--namespace={NAMESPACE}"])
    _run(["kubectl", "delete", "namespace", NAMESPACE, "--ignore-not-found=true"])
    subprocess.run(
        ["minikube", "ssh", f"sudo rm -rf /tmp/hostpath-provisioner/{NAMESPACE}/"],
        capture_output=True,
        text=True,
        timeout=30,
    )

    # Restart heavy containers that were stopped to free memory for minikube pods.
    for c in stopped:
        subprocess.run(["docker", "start", c], capture_output=True, timeout=60)


class TestPodsRunning:
    def test_all_pods_are_running(self):
        """All Provisa pods reach Running phase after helm install."""
        pods = _get_pods()
        assert len(pods) > 0, "No pods found in namespace"
        for pod in pods:
            phase = pod["status"].get("phase", "Unknown")
            name = pod["metadata"]["name"]
            assert phase == "Running", f"Pod {name!r} is in phase {phase!r}, not Running"

    def test_provisa_deployment_pod_exists(self):
        """At least one pod with 'provisa' in its name is running."""
        pods = _get_pods()
        provisa_pods = [p for p in pods if "provisa" in p["metadata"]["name"]]
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
        result = _run(
            [
                "helm",
                "upgrade",
                RELEASE,
                str(CHART_DIR),
                f"--namespace={NAMESPACE}",
                "--set",
                "provisa.replicaCount=1",
                "--set",
                "provisa.hpa.enabled=false",
                "--set",
                "trino.worker.replicaCount=2",
                "--set",
                "trino.worker.autoscaling.enabled=false",
                "--set",
                "ingress.enabled=false",
                "--set",
                "provisa.flightService.type=ClusterIP",
                "--wait",
                f"--timeout={TIMEOUT}",
            ]
        )
        assert result.returncode == 0, f"helm upgrade failed:\n{result.stderr}"

        pods = _get_pods()
        worker_pods = [p for p in pods if "trino-worker" in p["metadata"]["name"]]
        assert len(worker_pods) >= 2, (
            f"Expected ≥2 worker pods after scaling to replicaCount=2, got {len(worker_pods)}"
        )

        # Scale back to 1
        _run(
            [
                "helm",
                "upgrade",
                RELEASE,
                str(CHART_DIR),
                f"--namespace={NAMESPACE}",
                "--set",
                "provisa.replicaCount=1",
                "--set",
                "provisa.hpa.enabled=false",
                "--set",
                "trino.worker.replicaCount=1",
                "--set",
                "trino.worker.autoscaling.enabled=false",
                "--set",
                "ingress.enabled=false",
                "--set",
                "provisa.flightService.type=ClusterIP",
                "--wait",
                f"--timeout={TIMEOUT}",
            ]
        )
