# Copyright (c) 2026 Kenneth Stott
# Canary: 31f91a83-870b-4737-801f-5e8b63ba9673
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The compose stack carries no store credential of its own and publishes no store on every
interface: PostgreSQL, Redis, MinIO and Grafana take passwords the launcher generates, and every
store, the engine and the telemetry services publish on PROVISA_STORE_BIND_IP."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest
import yaml

_REPO = Path(__file__).resolve().parents[2]
_BIND = "${PROVISA_STORE_BIND_IP:?"
_STORE_FILES = (
    "docker-compose.core.yml",
    "docker-compose.observability.yml",
    "docker-compose.dev.yml",
    "docker-compose.dev-install.yml",
)


def _services(name: str) -> dict:
    return yaml.safe_load((_REPO / name).read_text())["services"]


@pytest.mark.parametrize("name", (*_STORE_FILES, "docker-compose.app.yml"))
def test_no_default_credential_is_written_in_the_stack(name):
    text = (_REPO / name).read_text()
    assert "minioadmin" not in text
    assert "PASSWORD: provisa" not in text
    # A credential variable may pass through empty (unset is the product's own choice); it never
    # defaults to a value.
    assert not re.search(r"(PASSWORD|_KEY):-[^}]", text)


@pytest.mark.parametrize("name", _STORE_FILES)
def test_every_port_publishes_on_the_store_address(name):
    for service, spec in _services(name).items():
        for port in spec.get("ports") or []:
            assert str(port).startswith(_BIND), f"{name}: {service} publishes {port}"


def test_each_store_requires_its_generated_password():
    core = _services("docker-compose.core.yml")
    assert core["postgres"]["environment"]["POSTGRES_PASSWORD"].startswith(
        "${PROVISA_PG_PASSWORD:?"
    )
    assert core["pgbouncer"]["environment"]["DB_PASSWORD"].startswith("${PROVISA_PG_PASSWORD:?")
    assert core["redis"]["command"][:2] == ["redis-server", "--requirepass"]
    assert core["redis"]["command"][2].startswith("${PROVISA_REDIS_PASSWORD:?")
    assert core["minio"]["environment"]["MINIO_ROOT_PASSWORD"].startswith(
        "${PROVISA_MINIO_ROOT_PASSWORD:?"
    )
    app = _services("docker-compose.app.yml")["provisa"]["environment"]
    assert app["PG_PASSWORD"].startswith("${CONFIG_DB_PASSWORD:?")
    assert app["REDIS_URL"].startswith("redis://:${PROVISA_REDIS_PASSWORD:?")


@pytest.mark.parametrize("name", ("docker-compose.observability.yml", "docker-compose.dev.yml"))
def test_grafana_is_administered_only_with_its_generated_password(name):
    env = _services(name)["grafana"]["environment"]
    assert env["GF_AUTH_ANONYMOUS_ORG_ROLE"] == "Viewer"
    assert "GF_AUTH_DISABLE_LOGIN_FORM" not in env
    assert env["GF_SECURITY_ADMIN_PASSWORD"].startswith("${PROVISA_GRAFANA_ADMIN_PASSWORD:?")


def test_no_tracked_trino_catalog_carries_a_store_credential():
    tracked = subprocess.run(
        ["git", "ls-files", "--", "trino"],
        cwd=_REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    for path in tracked:
        if path.endswith(".properties"):
            text = (_REPO / path).read_text()
            assert "minioadmin" not in text and "password=provisa" not in text, path


# --- the launchers generate them; terraform gives a deployment's to every node -------------------

_VARS = (
    "PROVISA_PG_PASSWORD",
    "PROVISA_REDIS_PASSWORD",
    "PROVISA_MINIO_ROOT_PASSWORD",
    "PROVISA_GRAFANA_ADMIN_PASSWORD",
)


def _store_credentials(home: Path, role: str, env: dict[str, str] | None = None):
    """scripts/provisa's store_credentials(), run alone against ``home``."""
    script = (
        "eval \"$(awk '/^store_credentials\\(\\) \\{/,/^}/' scripts/provisa)\"\n"
        'err() { echo "$*" >&2; }\n'
        "store_credentials\n"
        "env | grep -E '^(PROVISA_|CONFIG_DB_PASSWORD)' | sort\n"
    )
    return subprocess.run(
        ["bash", "-c", script],
        cwd=_REPO,
        env={
            "PATH": "/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin:/usr/local/bin",
            "PROVISA_HOME": str(home),
            "STORE_CREDENTIALS": str(home / "store-credentials.env"),
            "ROLE_CFG": role,
            **(env or {}),
        },
        capture_output=True,
        text=True,
    )


def test_a_node_generates_its_credentials_once_and_keeps_them_private(tmp_path):
    first = _store_credentials(tmp_path, "primary")
    assert first.returncode == 0, first.stderr
    kept = tmp_path / "store-credentials.env"
    assert kept.stat().st_mode & 0o777 == 0o600
    values = dict(line.split("=", 1) for line in kept.read_text().split())
    assert set(values) == set(_VARS)
    assert all(re.fullmatch(r"[0-9a-f]{48}", v) for v in values.values())
    out = dict(line.split("=", 1) for line in first.stdout.split())
    assert out["PROVISA_STORE_BIND_IP"] == "127.0.0.1"
    assert out["CONFIG_DB_PASSWORD"] == values["PROVISA_PG_PASSWORD"]
    assert out["PROVISA_OTEL_S3_SECRET_KEY"] == values["PROVISA_MINIO_ROOT_PASSWORD"]
    again = _store_credentials(tmp_path, "primary")
    assert dict(line.split("=", 1) for line in kept.read_text().split()) == values
    assert "PROVISA_PG_PASSWORD=" + values["PROVISA_PG_PASSWORD"] in again.stdout


def test_a_deployments_own_credentials_are_used_as_given(tmp_path):
    given = {var: f"given-{i}" for i, var in enumerate(_VARS)}
    result = _store_credentials(tmp_path, "secondary", given)
    assert result.returncode == 0, result.stderr
    for var, value in given.items():
        assert f"{var}={value}" in result.stdout
    assert (tmp_path / "store-credentials.env").read_text() == ""


def test_a_secondary_without_the_primarys_credentials_does_not_start(tmp_path):
    result = _store_credentials(tmp_path, "secondary")
    assert result.returncode != 0
    assert "PROVISA_PG_PASSWORD is not set" in result.stderr


def test_the_development_stack_generates_its_own_outside_the_checkout(tmp_path):
    result = subprocess.run(
        ["bash", "-c", ". scripts/dev-store-credentials.sh; echo $PG_PASSWORD; echo $REDIS_URL"],
        cwd=_REPO,
        env={
            "PATH": "/usr/bin:/bin:/opt/homebrew/bin:/usr/local/bin",
            "PROVISA_HOME": str(tmp_path),
        },
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    kept = tmp_path / "dev-store-credentials.env"
    assert kept.stat().st_mode & 0o777 == 0o600
    values = dict(line.split("=", 1) for line in kept.read_text().split())
    pg, redis_url = result.stdout.split()
    assert pg == values["PROVISA_PG_PASSWORD"]
    assert redis_url == f"redis://:{values['PROVISA_REDIS_PASSWORD']}@localhost:6379"


@pytest.mark.parametrize("module", ["gcp", "aws", "azure", "gcp-saas"])
def test_terraform_gives_every_node_the_deployments_generated_credentials(module):
    tf = "".join(p.read_text() for p in sorted((_REPO / "terraform" / module).glob("*.tf")))
    assert 'resource "random_password" "store"' in tf
    for var in ("PROVISA_REDIS_PASSWORD", "PROVISA_MINIO_ROOT_PASSWORD"):
        assert re.search(var + r"""=["']\$\{random_password\.store\["\w+"\]\.result\}""", tf), var
    assert "minioadmin" not in tf


def test_first_launch_keeps_the_given_credentials_for_provisa_start():
    text = (_REPO / "packaging/linux/first-launch.sh").read_text()
    allow = text[text.index("for var in PROVISA_IDP") :]
    allow = allow[: allow.index("; do")]
    for var in (*_VARS, "PROVISA_STORE_BIND_IP"):
        assert var in allow
    assert "minioadmin" not in text and "provisa:provisa@${PRIMARY_IP}" not in text
