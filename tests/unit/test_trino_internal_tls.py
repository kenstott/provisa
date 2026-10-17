# Copyright (c) 2026 Kenneth Stott
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
    for sub in ("catalog", "kafka", "kafka-client", "redis"):
        assert (tree / sub).is_dir()
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


def test_the_chart_pins_the_coordinator_when_it_is_on():
    rendered = _helm("trino.internalTls=true", "trino.coordinatorClusterIP=10.96.0.50")
    assert rendered.returncode == 0, rendered.stderr
    assert rendered.stdout.count("internal-communication.https.required=true") == 2
    assert rendered.stdout.count("discovery.uri=https://10.96.0.50:8443") == 2
    assert "node.internal-address=10.96.0.50" in rendered.stdout
    assert "clusterIP: 10.96.0.50" in rendered.stdout


def test_the_chart_refuses_it_without_the_pinned_address():
    rendered = _helm("trino.internalTls=true")
    assert rendered.returncode != 0
    assert "trino.coordinatorClusterIP" in rendered.stderr


def test_under_the_chart_the_settings_show_it_read_only(monkeypatch):
    from provisa.core import settings_registry

    monkeypatch.setenv("PROVISA_ENGINE_CONFIG_SOURCE", "chart")
    described = settings_registry.describe("engine.trino_internal_tls")
    assert described["editable"] is False
    assert described["readonly_reason"] == "set_by_chart"
    assert described["chart_value"] == "trino.internalTls"
