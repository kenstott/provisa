# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""deploy-cloud.sh: a deploy brings the node's generated control-plane compose up to the one the
shipped first-launch.sh writes, keeping the node's own values and refusing, by name, a variable
the node does not set (push_cp_compose); and a failed verify poll names the container that is
down, with its last log lines (diagnose_containers)."""

# Requirements: REQ-1451, REQ-2068

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest
import yaml

_REPO = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO / "scripts" / "deploy-cloud.sh"
_FIRST_LAUNCH = _REPO / "packaging" / "linux" / "first-launch.sh"


def _function(name: str) -> str:
    text = _SCRIPT.read_text()
    start = text.index(f"{name}() {{")
    return text[start : text.index("\n}\n", start) + 3]


def _first_launch_compose(tmp_path: Path, home: str) -> str:
    """write_control_plane_compose run directly, as first-launch runs it on a node at `home`."""
    script = _FIRST_LAUNCH.read_text()
    start = script.index("write_control_plane_compose() {")
    body = script[start : script.index("\n}\n", start) + 3]
    out = tmp_path / "direct"
    out.mkdir()
    subprocess.run(
        [
            "bash",
            "-c",
            f'set -e; ok() {{ :; }}; ROLE=control-plane; PROVISA_HOME="$1"; CERT_DIR="$2/certs"\n'
            f"{body}\nwrite_control_plane_compose",
            "_",
            str(out),
            home,
        ],
        check=True,
    )
    return (out / "compose-control-plane.yml").read_text().replace(str(out), home)


class _Node:
    """A node faked locally: its PROVISA_HOME, env file and compose under `root`, sudo that just
    runs, a systemctl that records the restart, and ssh/scp that run against that tree."""

    def __init__(self, tmp_path: Path):
        self.tmp = tmp_path
        self.home = tmp_path / "node" / "root" / ".provisa"
        self.home.mkdir(parents=True)
        self.node_tmp = tmp_path / "node" / "tmp"
        self.node_tmp.mkdir()
        self.stage = tmp_path / "stage"
        self.stage.mkdir()
        self.restarts = tmp_path / "restarts"
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        (bin_dir / "sudo").write_text('#!/bin/bash\n"$@"\n')
        (bin_dir / "systemctl").write_text(f'#!/bin/bash\necho "$*" >> {self.restarts}\n')
        for tool in ("sudo", "systemctl"):
            (bin_dir / tool).chmod(0o755)
        self.path = f"{bin_dir}:/usr/bin:/bin:/opt/homebrew/bin:/usr/local/bin"

    @property
    def compose(self) -> Path:
        return self.home / "compose-control-plane.yml"

    def expected(self) -> str:
        return _first_launch_compose(self.tmp, str(self.home))

    def push(self) -> subprocess.CompletedProcess:
        script = "\n".join(
            [
                "set -euo pipefail",
                f"REPO={_REPO}",
                f"STAGE={self.stage}",
                "NODE=test-node",
                f"NODE_TMP={self.node_tmp}",
                # The node's /tmp is its own: the fake ssh and scp both address it under NODE_TMP.
                'ssh_node() { bash -c "${1//\\/tmp\\//$NODE_TMP/}"; }',
                'scp_node() { cp "$1" "$NODE_TMP/${2#/tmp/}"; }',
                f'ENV_FILE="{self.home}/provisa.env"',
                f'CP_HOME="{self.home}"',
                'CP_YML="$CP_HOME/compose-control-plane.yml"',
                'STORE_CREDENTIALS="$CP_HOME/store-credentials.env"',
                _function("render_control_plane_compose"),
                _function("missing_compose_vars"),
                _function("push_cp_compose"),
                "push_cp_compose",
            ]
        )
        return subprocess.run(
            ["bash", "-c", script], env={"PATH": self.path}, capture_output=True, text=True
        )


def _required(text: str) -> set[str]:
    return set(re.findall(r"\$\{([A-Za-z_][A-Za-z0-9_]*):\?", text))


def _stale(expected: str) -> str:
    """The cloud node's compose before the fix: the regions demos as a bare list, no node map."""
    stale = expected.replace(
        'PROVISA_SANDBOX_REGIONS_DEMOS: "retail=us,banking=warehouse"',
        'PROVISA_SANDBOX_REGIONS_DEMOS: "retail"',
    )
    stale = "\n".join(
        line for line in stale.splitlines() if "PROVISA_SANDBOX_REGIONS_NODES:" not in line
    )
    assert stale != expected
    return stale + "\n"


def _env(node: _Node, names: set[str]) -> None:
    node_env = "".join(f"{name}=value-of-{name}\n" for name in sorted(names))
    (node.home / "provisa.env").write_text(node_env)


# --- the refresh -------------------------------------------------------------------------------


def test_the_rendered_compose_is_first_launchs_for_the_node(tmp_path):
    node = _Node(tmp_path)
    expected = node.expected()
    node.compose.write_text(expected)
    _env(node, _required(expected))
    result = node.push()
    assert result.returncode == 0, result.stderr
    rendered = (node.stage / "compose-control-plane.yml").read_text()
    assert rendered == expected
    # No staging path survives into what the node runs.
    assert str(node.stage) not in rendered
    services = yaml.safe_load(rendered)["services"]
    assert services["provisa-ui"]["volumes"] == [f"{node.home}/certs:/app/certs:ro"]
    assert f"{node.home}/sandbox-data:/var/lib/provisa" in services["provisa-sandbox"]["volumes"]


def test_a_current_node_is_left_alone(tmp_path):
    node = _Node(tmp_path)
    expected = node.expected()
    node.compose.write_text(expected)
    _env(node, _required(expected))
    result = node.push()
    assert result.returncode == 0, result.stderr
    assert "control-plane compose: up to date" in result.stdout
    assert not node.restarts.exists()
    assert list(node.home.glob("compose-control-plane.yml.bak-*")) == []


def test_a_stale_node_gets_the_shipped_compose_and_the_stack_is_recreated(tmp_path):
    node = _Node(tmp_path)
    expected = node.expected()
    stale = _stale(expected)
    node.compose.write_text(stale)
    _env(node, _required(expected))
    result = node.push()
    assert result.returncode == 0, result.stderr
    assert node.compose.read_text() == expected
    ui_env = yaml.safe_load(node.compose.read_text())["services"]["provisa-ui"]["environment"]
    assert ui_env["PROVISA_SANDBOX_REGIONS_DEMOS"] == "retail=us,banking=warehouse"
    assert "PROVISA_SANDBOX_REGIONS_NODES" in ui_env
    # The node's previous file is kept beside it.
    (backup,) = node.home.glob("compose-control-plane.yml.bak-*")
    assert backup.read_text() == stale
    assert node.restarts.read_text() == "restart provisa\n"
    # What changed is printed, so the deploy log says what the refresh did.
    assert "PROVISA_SANDBOX_REGIONS_DEMOS" in result.stdout


def test_the_nodes_values_are_not_read_into_the_deploy(tmp_path):
    node = _Node(tmp_path)
    expected = node.expected()
    node.compose.write_text(_stale(expected))
    _env(node, _required(expected))
    result = node.push()
    assert result.returncode == 0, result.stderr
    assert "value-of-" not in result.stdout + result.stderr
    assert "value-of-" not in node.compose.read_text()
    assert "value-of-" not in "".join(p.read_text() for p in node.stage.iterdir() if p.is_file())


def test_a_variable_the_shipped_file_newly_requires_and_the_node_lacks_stops_the_deploy(tmp_path):
    node = _Node(tmp_path)
    expected = node.expected()
    required = _required(expected)
    assert "PROVISA_OBJECT_STORE_DIR" in required
    # The installed file predates the requirement, and the node's env file does not set it.
    installed = _stale(expected).replace(
        "${PROVISA_OBJECT_STORE_DIR:?the object store's data directory}", "/data-dir"
    )
    node.compose.write_text(installed)
    _env(node, required - {"PROVISA_OBJECT_STORE_DIR"})
    result = node.push()
    assert result.returncode != 0
    assert "PROVISA_OBJECT_STORE_DIR" in result.stderr
    # Nothing changed on the node.
    assert node.compose.read_text() == installed
    assert not node.restarts.exists()


def test_an_empty_value_does_not_satisfy_a_requirement(tmp_path):
    node = _Node(tmp_path)
    expected = node.expected()
    required = _required(expected)
    installed = _stale(expected).replace(
        "${PROVISA_OBJECT_STORE_DIR:?the object store's data directory}", "/data-dir"
    )
    node.compose.write_text(installed)
    _env(node, required - {"PROVISA_OBJECT_STORE_DIR"})
    with (node.home / "provisa.env").open("a") as env:
        env.write("PROVISA_OBJECT_STORE_DIR=\n")
    result = node.push()
    assert result.returncode != 0
    assert "PROVISA_OBJECT_STORE_DIR" in result.stderr


def test_a_requirement_the_launcher_meets_is_met_from_its_store_credentials(tmp_path):
    node = _Node(tmp_path)
    expected = node.expected()
    required = _required(expected)
    assert "PROVISA_REDIS_PASSWORD" in required
    installed = _stale(expected).replace(
        "${PROVISA_REDIS_PASSWORD:?the deployment Redis password}", "fixed"
    )
    node.compose.write_text(installed)
    _env(node, required - {"PROVISA_REDIS_PASSWORD"})
    (node.home / "store-credentials.env").write_text("PROVISA_REDIS_PASSWORD=generated\n")
    result = node.push()
    assert result.returncode == 0, result.stderr
    assert node.compose.read_text() == expected


def test_what_the_installed_file_already_requires_is_not_asked_again(tmp_path):
    # TRINO_HOST and the like are met by the launcher however it meets them; the running stack
    # proves it. Only what the shipped file adds is asked of the env file.
    node = _Node(tmp_path)
    expected = node.expected()
    node.compose.write_text(_stale(expected))
    _env(node, set())
    result = node.push()
    assert result.returncode == 0, result.stderr
    assert node.compose.read_text() == expected


def test_a_node_without_a_control_plane_compose_is_refused(tmp_path):
    node = _Node(tmp_path)
    _env(node, set())
    result = node.push()
    assert result.returncode != 0
    assert "compose-control-plane.yml" in result.stderr
    assert not node.compose.exists()


@pytest.mark.parametrize("arm", ["api", "cfg", "all", "patch"])
def test_every_arm_that_pushes_refreshes_the_compose_before_the_pushes(arm):
    text = _SCRIPT.read_text()
    line = next(line for line in text.splitlines() if re.match(rf"\s+{arm}\)", line))
    body = text.split(line, 1)[1].split(";;", 1)[0]
    assert body.index("push_app") < body.index("push_cp_compose") < body.index("push_overlay")
    assert body.index("push_cp_compose") < body.index("restart; verify")


def test_the_sandbox_vm_refreshes_its_compose_too():
    body = _function("deploy_sandbox_vm")
    assert body.index("push_cp_compose") < body.index("push_overlay")


# --- the verify polls name what is down ----------------------------------------------------


def _failing(lines: list[str]) -> list[str]:
    result = subprocess.run(
        ["bash", "-c", f"{_function('failing_containers')}\nfailing_containers"],
        input="".join(f"{line}\n" for line in lines),
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.split()


def test_failing_containers_names_what_is_down_and_only_that():
    assert _failing(
        [
            "compose-provisa-1 running 0 0 healthy",
            # A crash loop caught between two restarts.
            "compose-provisa-ui-1 running 0 7 -",
            "compose-provisa-sandbox-1 restarting 1 3 -",
            "compose-redis-1 running 0 0 unhealthy",
            "compose-minio-1 exited 137 0 -",
            # An init container that finished.
            "compose-minio-init-1 exited 0 0 -",
            "compose-otel-collector-1 running 0 0 -",
            "compose-petstore-mock-1 created 0 0 -",
        ]
    ) == [
        "compose-provisa-ui-1",
        "compose-provisa-sandbox-1",
        "compose-redis-1",
        "compose-minio-1",
        "compose-petstore-mock-1",
    ]


def _diagnose(tmp_path: Path, states: str) -> subprocess.CompletedProcess:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "sudo").write_text('#!/bin/bash\n"$@"\n')
    (bin_dir / "docker").write_text(
        "#!/bin/bash\n"
        'case "$1" in\n'
        "  ps) echo a1; echo b2 ;;\n"
        f"  inspect) printf '%s' '{states}' ;;\n"
        '  logs) echo "log of ${@: -1}: ValueError: PROVISA_SANDBOX_REGIONS_DEMOS"; '
        'echo "second line" ;;\n'
        "esac\n"
    )
    for tool in ("sudo", "docker"):
        (bin_dir / tool).chmod(0o755)
    script = "\n".join(
        [
            "set -euo pipefail",
            "NODE=test-node",
            'ssh_node() { bash -c "$1"; }',
            _function("failing_containers"),
            _function("diagnose_containers"),
            "diagnose_containers",
        ]
    )
    return subprocess.run(
        ["bash", "-c", script],
        env={"PATH": f"{bin_dir}:/usr/bin:/bin"},
        capture_output=True,
        text=True,
    )


def test_a_failed_poll_names_the_crash_looping_container_and_shows_its_log(tmp_path):
    result = _diagnose(
        tmp_path, "/compose-provisa-1 running 0 0 healthy\n/compose-provisa-ui-1 running 0 9 -\n"
    )
    assert result.returncode == 0, result.stderr
    assert "compose-provisa-ui-1 running 0 9 -" in result.stderr
    assert "compose-provisa-ui-1 is down" in result.stderr
    assert "log of compose-provisa-ui-1: ValueError: PROVISA_SANDBOX_REGIONS_DEMOS" in result.stderr
    assert "compose-provisa-1 is down" not in result.stderr
    assert "log of compose-provisa-1:" not in result.stderr


def test_a_node_that_does_not_answer_is_said_so(tmp_path):
    result = _diagnose(tmp_path, "")
    assert result.returncode == 0, result.stderr
    assert "no container state" in result.stderr


@pytest.mark.parametrize("name", ["verify", "verify_api"])
def test_each_poll_diagnoses_before_it_fails(name):
    body = _function(name)
    assert body.index("diagnose_containers") < body.index("return 1")
