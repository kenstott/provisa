# Copyright (c) 2026 Kenneth Stott
# Canary: a510e540-45e7-4e14-abe8-29d49cd2fd36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2072: Trino's internal TLS, an operator option off by default, through every path that
renders or starts the engine: the app's rendering of the engine's config and of the launch-settings
file, the refusals of a save that cannot work, the compose overlay the launcher adds, the launcher
itself, and the Helm chart. The engine's tree is shared by the app and the engine (compose), which
is also what carries the engine's sizing to it."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
import yaml

from provisa.api import trino_setup
from provisa.api.admin import settings_guards

_REPO = Path(__file__).resolve().parents[2]


def _render(tmp_path: Path, monkeypatch, **env: str) -> Path:
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    root = tmp_path / "trino"
    (root / "etc" / "worker").mkdir(parents=True)
    monkeypatch.setenv("PROVISA_TRINO_ETC_DIR", str(root))
    (tmp_path / "config").mkdir()
    cfg = tmp_path / "config" / "provisa.yaml"
    cfg.write_text(yaml.safe_dump({"fault_tolerant_execution": False, "jvm_heap_gb": 6}))
    trino_setup.write_trino_config(str(cfg))
    return root


def _lines(path: Path) -> list[str]:
    return path.read_text().splitlines()


def test_off_by_default_the_nodes_find_each_other_by_name(tmp_path, monkeypatch):
    monkeypatch.delenv("PROVISA_TRINO_INTERNAL_TLS", raising=False)
    root = _render(tmp_path, monkeypatch)
    coordinator, worker = (
        _lines(root / "etc" / "config.properties"),
        _lines(root / "etc" / "worker" / "config.properties"),
    )
    assert "discovery.uri=http://localhost:8080" in coordinator
    assert "discovery.uri=http://trino:8080" in worker
    assert not any(line.startswith("internal-communication.https") for line in coordinator + worker)
    launch = dict(line.split("=", 1) for line in _lines(root / "launch-settings.env"))
    assert launch["PROVISA_TRINO_INTERNAL_TLS"] == "false"


def test_on_every_node_requires_https_from_the_pinned_coordinator(tmp_path, monkeypatch):
    root = _render(
        tmp_path,
        monkeypatch,
        PROVISA_TRINO_INTERNAL_TLS="true",
        PROVISA_TRINO_INTERNAL_SUBNET="10.77.0.0/24",
        PROVISA_TRINO_COORDINATOR_IP="10.77.0.20",
    )
    for path in (root / "etc" / "config.properties", root / "etc" / "worker" / "config.properties"):
        lines = _lines(path)
        assert "internal-communication.https.required=true" in lines
        assert "http-server.https.enabled=true" in lines
        assert "discovery.uri=https://10.77.0.20:8443" in lines
    launch = dict(line.split("=", 1) for line in _lines(root / "launch-settings.env"))
    assert launch == {
        "PROVISA_TRINO_INTERNAL_TLS": "true",
        "PROVISA_TRINO_INTERNAL_SUBNET": "10.77.0.0/24",
        "PROVISA_TRINO_COORDINATOR_IP": "10.77.0.20",
        "PROVISA_TRINO_DYNAMIC_RANGE": "10.77.0.128/25",
    }


def test_the_engines_sizing_reaches_the_tree_the_engine_mounts(tmp_path, monkeypatch):
    root = _render(tmp_path, monkeypatch)
    assert "-Xmx6G" in (root / "etc" / "jvm.config").read_text()


# --- a save that cannot work is refused by name -------------------------------------------------


@pytest.fixture
def trino(monkeypatch):
    monkeypatch.setenv("PROVISA_ENGINE", "trino")
    monkeypatch.delenv("PROVISA_ROLE", raising=False)
    monkeypatch.delenv("PROVISA_ENGINE_CONFIG_SOURCE", raising=False)


def _refusal(values: dict) -> str | None:
    try:
        settings_guards.check(values)
    except settings_guards.Refused as refused:
        return refused.reason
    return None


def test_a_valid_save_is_accepted(trino):
    assert _refusal({"engine.trino_internal_tls": True}) is None


@pytest.mark.parametrize(
    ("env", "reason"),
    [
        ({"PROVISA_ENGINE_CONFIG_SOURCE": "chart"}, "internal_tls_set_by_chart"),
        ({"PROVISA_ENGINE": "duckdb"}, "internal_tls_not_trino"),
        ({"PROVISA_ROLE": "control-plane"}, "internal_tls_single_node"),
    ],
)
def test_a_deployment_that_cannot_carry_it_refuses_by_name(trino, monkeypatch, env, reason):
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    assert _refusal({"engine.trino_internal_tls": True}) == reason


@pytest.mark.parametrize(
    ("values", "reason"),
    [
        ({"engine.trino_internal_subnet": "nonsense"}, "not_a_subnet"),
        ({"engine.trino_coordinator_ip": "x"}, "not_an_address"),
        ({"engine.trino_coordinator_ip": "10.0.0.5"}, "outside_subnet"),
        ({"engine.trino_coordinator_ip": "172.30.250.200"}, "not_reserved_for_coordinator"),
        ({"engine.trino_coordinator_ip": "172.30.250.1"}, "not_reserved_for_coordinator"),
    ],
)
def test_a_coordinator_address_off_its_reserved_range_is_refused(trino, values, reason):
    assert _refusal(values) == reason


# --- compose and the launcher ------------------------------------------------------------------


def test_the_overlay_pins_the_coordinator_and_keeps_others_off_its_address():
    overlay = yaml.safe_load((_REPO / "docker-compose.trino-internal-tls.yml").read_text())
    ipam = overlay["networks"]["default"]["ipam"]["config"][0]
    assert ipam["subnet"].startswith("${PROVISA_TRINO_INTERNAL_SUBNET:?")
    assert ipam["ip_range"].startswith("${PROVISA_TRINO_DYNAMIC_RANGE:?")
    address = overlay["services"]["trino"]["networks"]["default"]["ipv4_address"]
    assert address.startswith("${PROVISA_TRINO_COORDINATOR_IP:?")


def test_the_app_and_the_engine_share_the_engines_tree():
    core = yaml.safe_load((_REPO / "docker-compose.core.yml").read_text())["services"]
    for service, target in (
        ("trino", "/etc/trino/config.properties"),
        ("trino-worker", "/etc/trino/config.properties"),
    ):
        mounts = [str(v) for v in core[service]["volumes"]]
        assert any(
            m.startswith("${PROVISA_TRINO_ETC_HOST:-./trino}/etc/")
            and m.split(":")[-2 if m.endswith(":ro") else -1] == target
            for m in mounts
        ), service
    app = yaml.safe_load((_REPO / "docker-compose.app.yml").read_text())["services"]["provisa"]
    assert any(
        str(v).startswith("${PROVISA_TRINO_ETC_HOST:?") and str(v).endswith(":/app/trino-host")
        for v in app["volumes"]
    )
    assert app["environment"]["PROVISA_TRINO_ETC_DIR"] == "/app/trino-host"


def _launcher(tmp_path: Path, launch: str | None) -> subprocess.CompletedProcess:
    home = tmp_path / "home"
    home.mkdir()
    if launch is not None:
        (home / "trino").mkdir()
        (home / "trino" / "etc").mkdir()
        (home / "trino" / "etc" / "config.properties").write_text("coordinator=true\n")
        (home / "trino" / "launch-settings.env").write_text(launch)
    text = (_REPO / "scripts" / "provisa").read_text()
    functions = []
    for name in ("engine_tree", "_build_compose_files"):
        start = text.index(f"{name}() {{")
        functions.append(text[start : text.index("\n}\n", start) + 3])
    script = "\n".join(
        [
            *functions,
            "engine_tree",
            'echo "TREE=$PROVISA_TRINO_ETC_HOST"',
            'echo "TLS=${PROVISA_TRINO_INTERNAL_TLS:-}"',
            'echo "FILES=$(_build_compose_files)"',
        ]
    )
    return subprocess.run(
        ["bash", "-c", script],
        env={
            "PATH": "/usr/bin:/bin",
            "PROVISA_HOME": str(home),
            "PROJECT_DIR": str(_REPO),
            "ROLE_CFG": "primary",
            "IMAGE_SOURCE": "build",
            "RUNTIME": "docker",
            "OBSERVABILITY": "false",
        },
        capture_output=True,
        text=True,
    )


def test_the_launcher_seeds_the_tree_from_the_release_on_its_first_start(tmp_path):
    result = _launcher(tmp_path, None)
    assert result.returncode == 0, result.stderr
    tree = tmp_path / "home" / "trino"
    assert (tree / "etc" / "config.properties").read_text() == (
        _REPO / "trino" / "etc" / "config.properties"
    ).read_text()
    for sub in ("kafka", "kafka-client", "redis"):
        assert (tree / sub).is_dir()
    # The engine keeps its catalogs in memory: there is no catalog directory to seed.
    assert not (tree / "catalog").exists()
    assert "trino-internal-tls" not in result.stdout


def test_the_launcher_adds_the_overlay_the_launch_settings_turn_on(tmp_path):
    launch = (
        "PROVISA_TRINO_INTERNAL_TLS=true\nPROVISA_TRINO_INTERNAL_SUBNET=10.77.0.0/24\n"
        "PROVISA_TRINO_COORDINATOR_IP=10.77.0.20\nPROVISA_TRINO_DYNAMIC_RANGE=10.77.0.128/25\n"
        "PATH=/nowhere\n"
    )
    result = _launcher(tmp_path, launch)
    assert result.returncode == 0, result.stderr
    assert "TLS=true" in result.stdout
    assert "docker-compose.trino-internal-tls.yml" in result.stdout
    # Only the engine's own names are taken from the file.
    assert "PATH=/nowhere" not in result.stdout


# --- the Helm chart ------------------------------------------------------------------------------


def _helm(*sets: str) -> subprocess.CompletedProcess:
    from tests.unit.test_helm_auth import _LOCAL, _render

    return _render(*_LOCAL, *sets)


def test_the_chart_renders_it_off_by_default():
    rendered = _helm()
    assert rendered.returncode == 0, rendered.stderr
    assert "internal-communication.https.required" not in rendered.stdout
    assert "name: PROVISA_ENGINE_CONFIG_SOURCE" in rendered.stdout


def _rendered_on() -> list[dict]:
    import yaml

    rendered = _helm("trino.internalTls=true")
    assert rendered.returncode == 0, rendered.stderr
    return [doc for doc in yaml.safe_load_all(rendered.stdout) if doc]


def _trino_config(docs: list[dict], key: str) -> str:
    config = next(
        d for d in docs if d["kind"] == "ConfigMap" and d["metadata"]["name"] == "rel-trino-config"
    )
    return config["data"][key]


def test_the_nodes_find_the_coordinator_by_its_service_name_when_it_is_on():
    docs = _rendered_on()
    for key in ("coordinator-config.properties", "worker-config.properties"):
        lines = _trino_config(docs, key).splitlines()
        assert "internal-communication.https.required=true" in lines
        assert "discovery.uri=https://rel-trino-coordinator:8443" in lines
        # Each node is addressed by its pod DNS name, set at its start.
        assert "node.internal-address=${ENV:PROVISA_TRINO_NODE_ADDRESS}" in lines
        assert (
            "internal-communication.https.keystore.path=/etc/trino/internal-tls/keystore.pem"
            in lines
        )
        assert (
            "internal-communication.https.truststore.path=/etc/trino/internal-tls/truststore.pem"
            in lines
        )
        assert "http-server.https.keystore.path=/etc/trino/internal-tls/keystore.pem" in lines
    coordinator = next(
        d
        for d in docs
        if d["kind"] == "Service" and d["metadata"]["name"] == "rel-trino-coordinator"
    )
    assert "clusterIP" not in coordinator["spec"]  # no address pinned


def test_the_chart_generates_a_certificate_naming_the_service_and_the_pods():
    import base64

    from cryptography import x509

    docs = _rendered_on()
    secret = next(
        d
        for d in docs
        if d["kind"] == "Secret" and d["metadata"]["name"] == "rel-trino-internal-tls"
    )
    keystore = base64.b64decode(secret["data"]["keystore.pem"])
    assert b"PRIVATE KEY-----" in keystore
    cert = x509.load_pem_x509_certificate(keystore)
    names = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    assert sorted(names.get_values_for_type(x509.DNSName)) == [
        "*.default.pod.cluster.local",
        "rel-trino-coordinator",
        "rel-trino-coordinator.default",
        "rel-trino-coordinator.default.svc.cluster.local",
    ]
    ca = x509.load_pem_x509_certificate(base64.b64decode(secret["data"]["truststore.pem"]))
    assert cert.issuer == ca.subject


def test_every_trino_pod_mounts_the_certificate_and_sets_its_address():
    docs = _rendered_on()
    pods = [
        d["spec"]["template"]["spec"]
        for d in docs
        if d["kind"] in ("StatefulSet", "Deployment")
        and d["metadata"]["name"].startswith("rel-trino-")
    ]
    assert len(pods) == 2
    for pod in pods:
        trino = pod["containers"][0]
        assert (
            'PROVISA_TRINO_NODE_ADDRESS="${POD_IP//./-}.${POD_NAMESPACE}.pod.cluster.local"'
            in (trino["command"][-1])
        )
        env = {e["name"]: e.get("valueFrom") for e in trino["env"]}
        assert env["POD_IP"] == {"fieldRef": {"fieldPath": "status.podIP"}}
        assert env["POD_NAMESPACE"] == {"fieldRef": {"fieldPath": "metadata.namespace"}}
        mount = next(m for m in trino["volumeMounts"] if m["name"] == "internal-tls")
        assert mount["mountPath"] == "/etc/trino/internal-tls"
        volume = next(v for v in pod["volumes"] if v["name"] == "internal-tls")
        assert volume["secret"]["secretName"] == "rel-trino-internal-tls"


def test_off_the_chart_makes_no_certificate_and_keeps_the_image_start():
    rendered = _helm()
    assert rendered.returncode == 0, rendered.stderr
    assert "rel-trino-internal-tls" not in rendered.stdout
    assert "PROVISA_TRINO_NODE_ADDRESS" not in rendered.stdout


def test_every_service_port_is_named_when_it_is_on():
    # Kubernetes rejects a multi-port Service with an unnamed port; TLS adds the coordinator's 8443.
    services = [d for d in _rendered_on() if d["kind"] == "Service"]
    coordinator = [s for s in services if s["spec"]["selector"].get("role") == "coordinator"]
    assert [p["port"] for p in coordinator[0]["spec"]["ports"]][1:] == [8443]
    unnamed = [
        (s["metadata"]["name"], p["port"])
        for s in services
        if len(s["spec"]["ports"]) > 1
        for p in s["spec"]["ports"]
        if not p.get("name")
    ]
    assert unnamed == []


def test_under_the_chart_the_settings_show_it_read_only(monkeypatch):
    from provisa.core import settings_registry

    monkeypatch.setenv("PROVISA_ENGINE_CONFIG_SOURCE", "chart")
    described = settings_registry.describe("engine.trino_internal_tls")
    assert described["editable"] is False
    assert described["readonly_reason"] == "set_by_chart"
    assert described["chart_value"] == "trino.internalTls"
