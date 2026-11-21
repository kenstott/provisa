# Copyright (c) 2026 Kenneth Stott
# Canary: e9f18d82-05cd-4f13-94e0-77cdeadfd9c0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2205: the dev container's toolchains and images are the repository's own pins, so a bump in
one place without the other fails here."""

# Requirements: REQ-2205

from __future__ import annotations

import json
import re
from pathlib import Path

from provisa.runtime_deps.code_runtimes import RUNTIMES

REPO_ROOT = Path(__file__).resolve().parents[2]
DEVCONTAINER = REPO_ROOT / ".devcontainer"
SEMVER = r"\d+\.\d+\.\d+"


def _devcontainer() -> dict:
    return json.loads((DEVCONTAINER / "devcontainer.json").read_text())


def _feature(name: str) -> tuple[str, dict]:
    """The pinned tag and options of ghcr.io/devcontainers/features/<name>."""
    prefix = f"ghcr.io/devcontainers/features/{name}:"
    found = [
        (ref, opts) for ref, opts in _devcontainer()["features"].items() if ref.startswith(prefix)
    ]
    assert len(found) == 1, f"one {name} feature expected, found {[ref for ref, _ in found]}"
    ref, opts = found[0]
    return ref.removeprefix(prefix), opts


def _from_image(dockerfile: Path, image: str) -> str:
    """The tag of the first ``FROM [--platform=...] <image>:<tag>`` in ``dockerfile``."""
    match = re.search(
        rf"^FROM\s+(?:--platform=\S+\s+)?{re.escape(image)}:(\S+)", dockerfile.read_text(), re.M
    )
    assert match, f"{dockerfile.name} has no FROM {image}"
    return match.group(1)


def _uv_version(dockerfile: Path) -> str:
    match = re.search(r"COPY --from=ghcr\.io/astral-sh/uv:(\S+) ", dockerfile.read_text())
    assert match, f"{dockerfile.name} copies no uv"
    return match.group(1)


def test_python_is_the_requires_python_minor():
    requires = re.search(
        r'^requires-python\s*=\s*">=(\d+\.\d+),<', (REPO_ROOT / "pyproject.toml").read_text(), re.M
    )
    assert requires
    tag = _from_image(DEVCONTAINER / "Dockerfile", "mcr.microsoft.com/devcontainers/python")
    assert re.fullmatch(rf"{SEMVER}-(\d+\.\d+)-\w+", tag), tag
    assert tag.split("-")[1] == requires.group(1)
    assert (
        _devcontainer()["containerEnv"]["UV_PYTHON"] == f"/usr/local/bin/python{requires.group(1)}"
    )


def test_uv_is_the_product_images_uv():
    assert _uv_version(DEVCONTAINER / "Dockerfile") == _uv_version(REPO_ROOT / "Dockerfile")


def test_rust_is_the_product_images_toolchain():
    rust = _from_image(REPO_ROOT / "Dockerfile", "rust")
    assert _feature("rust")[1]["version"] == rust.split("-")[0]


def test_go_is_the_code_source_pin():
    assert _feature("go")[1]["version"] == RUNTIMES["go"].version


def test_node_is_provisa_uis_pin():
    nvmrc = (REPO_ROOT / ".nvmrc").read_text().strip()
    assert _feature("node")[1]["version"] == nvmrc


def test_it_is_one_container_built_from_the_dockerfile():
    # J628: Codespaces created no container for the compose definition (error 1302). The unit
    # tests run PostgreSQL embedded (pgserver), so the dev container needs no sidecar.
    config = _devcontainer()
    assert config["build"] == {"dockerfile": "Dockerfile"}
    assert not {"dockerComposeFile", "service", "runServices"} & config.keys()
    assert not (DEVCONTAINER / "compose.yml").exists()


def test_every_feature_is_pinned_to_a_release():
    for ref in _devcontainer()["features"]:
        assert re.fullmatch(rf"ghcr\.io/devcontainers/features/[\w-]+:{SEMVER}", ref), ref


def test_docker_is_available_inside():
    _feature("docker-in-docker")


def test_the_environment_is_the_checkouts_venv_on_a_volume():
    # start-ui-install.sh and rust/build-local.sh use <checkout>/.venv; a volume keeps a host's own
    # .venv in the bind-mounted checkout from being synced over.
    config = _devcontainer()
    venv = "${containerWorkspaceFolder}/.venv"
    assert config["containerEnv"]["UV_PROJECT_ENVIRONMENT"] == venv
    assert config["remoteEnv"]["PATH"].startswith(venv + "/bin:")
    assert (
        config["customizations"]["vscode"]["settings"]["python.defaultInterpreterPath"]
        == "${workspaceFolder}/.venv/bin/python"
    )
    assert any(m.endswith(f"target={venv},type=volume") for m in config["mounts"])


def test_the_slow_setup_is_one_a_codespaces_prebuild_runs():
    # A prebuild runs onCreateCommand and updateContentCommand; postCreateCommand runs on every
    # codespace's first start, so the sync and builds there would never be cached.
    config = _devcontainer()
    assert config["updateContentCommand"] == "bash .devcontainer/update-content.sh"
    assert (DEVCONTAINER / "update-content.sh").is_file()
    assert "postCreateCommand" not in config


def test_codespaces_picks_a_machine_that_builds_it():
    assert _devcontainer()["hostRequirements"] == {"cpus": 4, "memory": "16gb", "storage": "32gb"}


def test_the_ui_and_api_ports_are_forwarded_with_labels():
    config = _devcontainer()
    assert config["forwardPorts"] == [3000, 8001]
    ports = config["portsAttributes"]
    assert ports["3000"] == {"label": "Provisa UI", "onAutoForward": "openBrowser"}
    assert ports["8001"] == {"label": "Provisa API", "onAutoForward": "silent"}


def test_the_manifest_is_fetched_with_credentials():
    # A private Codespaces port redirects an uncredentialed manifest fetch to github.dev/pf-signin,
    # which the browser blocks under CORS.
    html = (REPO_ROOT / "provisa-ui" / "index.html").read_text()
    assert re.search(
        r'<link rel="manifest" href="/site\.webmanifest" crossorigin="use-credentials"\s*/>', html
    )


def test_the_ui_port_is_made_public_on_every_start():
    # devcontainer.json has no field for port visibility and a public port reverts to private when
    # the codespace restarts, so a postStartCommand sets it with the GitHub CLI.
    config = _devcontainer()
    _feature("github-cli")
    assert "public-ui-port.sh" in config["postStartCommand"]
    assert config["postStartCommand"].rstrip().endswith("&")
    script = (DEVCONTAINER / "public-ui-port.sh").read_text()
    assert 'gh codespace ports visibility "$UI_PORT:public" -c "$CODESPACE_NAME"' in script
    assert "UI_PORT=3000" in script


def test_the_public_port_script_does_nothing_outside_a_codespace(tmp_path):
    import os
    import subprocess

    env = {k: v for k, v in os.environ.items() if k != "CODESPACE_NAME"}
    run = subprocess.run(
        ["bash", str(DEVCONTAINER / "public-ui-port.sh")], env=env, capture_output=True, timeout=30
    )
    assert run.returncode == 0
    assert run.stdout == b"" and run.stderr == b""


def test_the_public_port_script_warns_and_continues_when_gh_refuses(tmp_path):
    import os
    import subprocess

    gh = tmp_path / "gh"
    gh.write_text(
        "#!/bin/bash\n"
        'case "$2" in\n'
        '  ports) if [ "${3:-}" = visibility ]; then echo "HTTP 403: denied" >&2; exit 1; fi;'
        " echo 3000 ;;\n"
        "esac\n"
    )
    gh.chmod(0o755)
    env = {**os.environ, "CODESPACE_NAME": "x", "PATH": f"{tmp_path}:{os.environ['PATH']}"}
    run = subprocess.run(
        ["bash", str(DEVCONTAINER / "public-ui-port.sh")],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert run.returncode == 0
    assert "WARNING" in run.stderr and "403" in run.stderr
