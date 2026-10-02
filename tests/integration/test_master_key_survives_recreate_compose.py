# Copyright (c) 2026 Kenneth Stott
# Canary: 9a2d6f48-3c17-4e95-b6a0-5d8e1f7c2b39
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The master key survives the API container being replaced (compose deployment).

The shipped compose files (``docker-compose.core.yml`` + ``docker-compose.app.yml``) are brought
up as an isolated project: its own name, its own volumes, no fixed host port. A secret is stored
through the admin API — which mints the master key into the data directory — the API container
is then REPLACED (``--force-recreate``: a new container, not a restart of the old one), and the
new container must start and still hold the secret. Starting is the proof that the key came
back: the server binds the org's vault at config load, and a vault with secrets and no key is
refused there.

The control is the same stack with the data-directory volume taken away, as the compose file
used to be: the replaced container cannot open the vault and does not come up."""

from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

import pytest
import yaml

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_IMAGE = "provisa-keytest:local"
_ORG = "default"
_SECRET = "KEYTEST_TOKEN"


def _docker(*args: str, timeout: int = 900) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)


@pytest.fixture(scope="module")
def image() -> str:
    """The API image, built from the repository (Dockerfile.dev: the same application, with no
    pre-built wheel set needed)."""
    built = _docker("build", "-q", "-f", "Dockerfile.dev", "-t", _IMAGE, str(_REPO), timeout=3600)
    assert built.returncode == 0, built.stderr[-3000:]
    return _IMAGE


class _Stack:
    """The shipped compose stack as an isolated project."""

    def __init__(self, tmp: Path, image: str, *, with_data_volume: bool) -> None:
        self.project = f"itest-keysurvive-{uuid.uuid4().hex[:8]}"
        config = tmp / "keytest.yaml"
        config.write_text(
            yaml.safe_dump(
                {
                    "sources": [],
                    "domains": [],
                    "tables": [],
                    "auth": {"provider": "none"},
                    "roles": [
                        {
                            "id": "org_admin",
                            "capabilities": ["query_development", "org_settings"],
                            "domain_access": ["*"],
                        }
                    ],
                }
            )
        )
        provisa: dict = {
            "build": _Tag("!reset", None),
            "image": image,
            "restart": "no",
            # The shipped image runs as root; the dev image's unprivileged user could not write
            # a freshly created named volume.
            "user": "0",
            "command": ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"],
            "ports": _Tag("!override", ["127.0.0.1::8000"]),
            "depends_on": _Tag("!override", {"postgres": {"condition": "service_healthy"}}),
            "environment": {
                "PROVISA_ENGINE": "duckdb",
                "PROVISA_CONFIG": "/app/config/keytest.yaml",
                "PROVISA_CONFIG_REPLACE": "true",
                "PROVISA_DEMO": "false",
                "PROVISA_IDP": "",
                "PROVISA_REDIS_EMBEDDED": "1",
                "ORG_ID": _ORG,
                "OTEL_SDK_DISABLED": "true",
                "PYTHON_KEYRING_BACKEND": "keyring.backends.fail.Keyring",
                "PROVISA_MATERIALIZE_URL": "duckdb:////tmp/store.duckdb",
                "PROVISA_REDIRECT_ENABLED": "false",
            },
        }
        volumes = [f"{config}:/app/config/keytest.yaml:ro"]
        if with_data_volume:
            provisa["volumes"] = volumes  # appended to the shipped ones, data volume included
        else:
            # The stack as it was before: no volume for the data directory.
            provisa["volumes"] = _Tag("!override", volumes)
            provisa["environment"]["PROVISA_DATA_DIR"] = _Tag("!reset", None)
        override = tmp / "override.yml"
        override.write_text(
            yaml.dump(
                {
                    "services": {
                        "postgres": {"ports": _Tag("!override", [])},
                        "provisa": provisa,
                    }
                },
                Dumper=_Dumper,
            )
        )
        self._base = [
            "compose",
            "-p",
            self.project,
            "-f",
            str(_REPO / "docker-compose.core.yml"),
            "-f",
            str(_REPO / "docker-compose.app.yml"),
            "-f",
            str(override),
        ]

    def compose(self, *args: str, timeout: int = 900) -> subprocess.CompletedProcess:
        return _docker(*self._base, *args, timeout=timeout)

    def up(self, *flags: str) -> None:
        started = self.compose("up", "-d", *flags, "provisa")
        assert started.returncode == 0, started.stderr[-3000:]

    def container_id(self) -> str:
        return self.compose("ps", "-a", "-q", "provisa").stdout.strip()

    def base_url(self) -> str:
        port = self.compose("port", "provisa", "8000").stdout.strip().rsplit(":", 1)[-1]
        assert port.isdigit(), "the API container has no published port (is it running?)"
        return f"http://127.0.0.1:{port}"

    def logs(self) -> str:
        return self.compose("logs", "--no-color", "provisa").stdout

    def running(self) -> bool:
        state = _docker("inspect", "-f", "{{.State.Running}}", self.container_id()).stdout.strip()
        return state == "true"

    def wait_healthy(self, timeout: float = 240.0) -> bool:
        """True once /health answers 200; False when the container stopped first."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not self.running():
                return False
            try:
                with urllib.request.urlopen(f"{self.base_url()}/health", timeout=5) as resp:
                    if resp.status == 200:
                        return True
            except (urllib.error.URLError, OSError, AssertionError):
                pass
            time.sleep(2)
        raise AssertionError(f"the API neither became healthy nor stopped:\n{self.logs()[-3000:]}")

    def request(self, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
        req = urllib.request.Request(
            f"{self.base_url()}{path}",
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json", "x-provisa-role": "org_admin"},
            method=method,
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode() or "{}")

    def down(self) -> None:
        self.compose("down", "-v", "--remove-orphans")


class _Tag:
    """A value written with a compose merge tag (``!override`` / ``!reset``)."""

    def __init__(self, tag: str, value) -> None:
        self.tag, self.value = tag, value


class _Dumper(yaml.SafeDumper):
    pass


def _represent_tag(dumper: yaml.SafeDumper, data: _Tag):
    if isinstance(data.value, dict):
        return dumper.represent_mapping(data.tag, data.value)
    if isinstance(data.value, list):
        return dumper.represent_sequence(data.tag, data.value)
    return dumper.represent_scalar(data.tag, "null")


_Dumper.add_representer(_Tag, _represent_tag)


def _store_a_secret(stack: _Stack) -> None:
    status, body = stack.request(
        "PUT", f"/admin/orgs/{_ORG}/secrets/{_SECRET}", {"value": "s3cr3t-value"}
    )
    assert status == 200, body
    status, body = stack.request("GET", f"/admin/orgs/{_ORG}/secrets")
    assert status == 200 and _SECRET in json.dumps(body), body


def test_the_secret_is_still_readable_after_the_api_container_is_replaced(image, tmp_path):
    stack = _Stack(tmp_path, image, with_data_volume=True)
    try:
        stack.up()
        assert stack.wait_healthy(), stack.logs()[-3000:]
        _store_a_secret(stack)
        first = stack.container_id()

        stack.up("--force-recreate")  # a NEW container: the old one's filesystem is gone
        assert stack.container_id() != first
        assert stack.wait_healthy(), (
            "the replaced container did not start:\n" + stack.logs()[-3000:]
        )
        status, body = stack.request("GET", f"/admin/orgs/{_ORG}/secrets")
        assert status == 200 and _SECRET in json.dumps(body), body
        # It is the SAME key that opens the vault: a second secret is stored under it and the
        # deployment's recorded key fingerprint is not contradicted.
        status, body = stack.request(
            "PUT", f"/admin/orgs/{_ORG}/secrets/{_SECRET}_2", {"value": "another"}
        )
        assert status == 200, body
        assert "holds no encryption master key" not in stack.logs()
    finally:
        stack.down()


def test_without_the_data_volume_the_replaced_container_cannot_open_the_vault(image, tmp_path):
    """The control: what the volume is for. The key minted at the first secret lived in the
    first container's own filesystem; its replacement has none and refuses the vault."""
    stack = _Stack(tmp_path, image, with_data_volume=False)
    try:
        stack.up()
        assert stack.wait_healthy(), stack.logs()[-3000:]
        _store_a_secret(stack)

        stack.up("--force-recreate")
        assert stack.wait_healthy() is False, "the replaced container started without the key"
        assert "holds no encryption master key" in stack.logs()
    finally:
        stack.down()


def test_the_shipped_stack_puts_the_data_directory_on_a_named_volume():
    """The two stacks an operator runs, as compose merges them."""
    for files in (
        ["docker-compose.core.yml", "docker-compose.app.yml"],
        ["docker-compose.core.yml", "docker-compose.app.yml", "docker-compose.airgap.yml"],
    ):
        args = [a for f in files for a in ("-f", str(_REPO / f))]
        env = {k: v for k, v in os.environ.items() if k != "PROVISA_ENCRYPTION_KEY"}
        merged = subprocess.run(
            ["docker", "compose", *args, "config"], capture_output=True, text=True, env=env
        )
        assert merged.returncode == 0, merged.stderr
        config = yaml.safe_load(merged.stdout)
        service = config["services"]["provisa"]
        mounts = {(v["source"], v["target"]) for v in service["volumes"]}
        assert ("provisa_data", "/var/lib/provisa") in mounts, files
        assert service["environment"]["PROVISA_DATA_DIR"] == "/var/lib/provisa"
        assert service["environment"]["PROVISA_ENCRYPTION_KEY"] == ""  # passed through when set
        assert "provisa_data" in config["volumes"]

        keyed = subprocess.run(
            ["docker", "compose", *args, "config"],
            capture_output=True,
            text=True,
            env={**env, "PROVISA_ENCRYPTION_KEY": "operator-supplied"},
        )
        passed = yaml.safe_load(keyed.stdout)["services"]["provisa"]["environment"]
        assert passed["PROVISA_ENCRYPTION_KEY"] == "operator-supplied"
