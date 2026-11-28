# Copyright (c) 2026 Kenneth Stott
# Canary: 623440ee-fa31-4f58-a818-0d2aecd2b039
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""deploy-cloud-release.sh installs a release on cloud.provisa.dev from its own artifacts (REQ-1989).

The source hot-patch (deploy-cloud.sh) pushed this checkout's tree into a node whose image was
older, and failed whenever the two disagreed (a module the old image's Python could not find). A
release deploy runs the node's own installer for a tag and carries no source at all.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO / "scripts" / "deploy-cloud-release.sh"
_LIB = _REPO / "scripts" / "cloud-node-lib.sh"
_HOTPATCH = _REPO / "scripts" / "deploy-cloud.sh"


def _function(name: str) -> str:
    text = _SCRIPT.read_text()
    found = re.search(rf"^{name}\(\) \{{\n.*?^\}}\n", text, re.S | re.M)
    assert found is not None, f"deploy-cloud-release.sh has no {name}"
    return found.group(0)


def _steps() -> list[str]:
    """The top-level calls the script makes, in order, after its last function."""
    text = _SCRIPT.read_text()
    tail = text[text.rindex("\n}\n") + 3 :]
    return re.findall(r"^([a-z_]+)(?: [^\n]*)?$", tail, re.M)


def _node_installer(tmp_path: Path) -> str:
    out = tmp_path / "node-install.sh"
    done = subprocess.run(
        ["bash", "-c", f'{_function("write_node_installer")}\nwrite_node_installer "$1"', "_", out],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert done.returncode == 0, done.stderr
    return out.read_text()


@pytest.mark.parametrize("args", [[], ["latest"], ["0.1.0-alpha.513"]])
def test_a_release_tag_is_required(args):
    done = subprocess.run(["bash", str(_SCRIPT), *args], capture_output=True, text=True, timeout=30)
    assert done.returncode == 2
    assert "usage:" in done.stderr


def test_the_steps_run_in_order():
    steps = _steps()
    order = [
        "check_release",
        "wake_node",
        "wait_for_stack",
        "install_release",
        "recreate_stack",
        "reset_state",
        "verify",
    ]
    assert [s for s in steps if s in order] == order, steps
    reset = steps.index("reset_state")
    for check in (
        "verify_api",
        "verify_demo",
        "verify_engine",
        "verify_org_base",
        "verify_commerce",
    ):
        assert steps.index(check) > reset, (check, steps)


def test_the_engine_and_commerce_settings_are_written_before_the_recreate():
    steps = _steps()
    install, recreate = steps.index("install_release"), steps.index("recreate_stack")
    assert install < steps.index("push_engine_env") < recreate
    assert install < steps.index("push_commerce_env") < recreate
    # The running container is read only once the recreate has brought it up.
    assert recreate < steps.index("preflight_commerce")
    assert steps.count("preflight_engine") == 2
    assert recreate < len(steps) - 1 - steps[::-1].index("preflight_engine")


def test_the_release_engine_images_are_the_tag():
    assert 'push_engine_env "$TAG"' in _SCRIPT.read_text()


def test_no_source_is_pushed():
    # Comments name the hot-patch's steps to say why they are absent; calls are what count.
    text = re.sub(r"^\s*#.*$", "", _SCRIPT.read_text(), flags=re.M)
    for push in (
        "build_ui",
        "build_api",
        "build_cfg",
        "build_deps",
        "push_ui",
        "push_api",
        "push_cfg",
        "push_deps",
        "push_overlay",
        "stage_runtime_deps",
        "push_app",
        "push_cp_compose",
    ):
        assert not re.search(rf"\b{push}\b", text), push
    assert "deploy-cloud.sh" not in text
    # Nothing from the checkout is packed for the node: the one file sent is the installer
    # this script writes.
    assert not re.search(r"\btar\s", text)
    assert re.findall(r"scp_node ([^ ]+)", text) == ['"$STAGE/node-install.sh"']


def test_the_installer_is_the_nodes_own_startup_script(tmp_path):
    installer = _node_installer(tmp_path)
    assert "computeMetadata/v1/instance/attributes/startup-script" in installer
    assert installer.rstrip().endswith('bash "$dir/startup.sh"')
    done = subprocess.run(["bash", "-n"], input=installer, capture_output=True, text=True)
    assert done.returncode == 0, done.stderr


def test_the_installer_clears_what_would_stop_or_shadow_the_install(tmp_path):
    installer = _node_installer(tmp_path)
    run = installer.index('bash "$dir/startup.sh"')
    for cleared in (
        'rm -f "$overlay_ext"',
        "rm -f /root/.provisa/images/.version",
        "rm -f /var/lib/provisa-startup.sha256",
    ):
        assert installer.index(cleared) < run, cleared


def test_the_version_rewrite_names_the_tag_everywhere_and_nothing_else(tmp_path):
    installer = _node_installer(tmp_path)
    rewrite = next(line for line in installer.splitlines() if "perl -pe" in line)
    command = rewrite.split("perl -pe ", 1)[1].rsplit(" \\", 1)[0]
    startup = (
        'export PROVISA_VERSION="v0.1.0-alpha.51"\n'
        'gh_base="https://github.com/o/r/releases/download/v0.1.0-alpha.51"\n'
        'manifest="provisa-core-images-amd64-v0.1.0-alpha.51.sha256"\n'
        'engine="ghcr.io/o/provisa-trino-engine:v0.1.0-alpha.51"\n'
        'unrelated="v0.1.0-alpha.512"\n'
    )
    done = subprocess.run(
        ["bash", "-c", f"perl -pe {command}"],
        input=startup,
        capture_output=True,
        text=True,
        env={"OLD": "v0.1.0-alpha.51", "NEW": "v0.1.0-alpha.513", "PATH": "/usr/bin:/bin"},
    )
    assert done.returncode == 0, done.stderr
    assert done.stdout == (
        'export PROVISA_VERSION="v0.1.0-alpha.513"\n'
        'gh_base="https://github.com/o/r/releases/download/v0.1.0-alpha.513"\n'
        'manifest="provisa-core-images-amd64-v0.1.0-alpha.513.sha256"\n'
        'engine="ghcr.io/o/provisa-trino-engine:v0.1.0-alpha.513"\n'
        'unrelated="v0.1.0-alpha.512"\n'
    )


def test_the_install_runs_detached_and_is_checked_after():
    body = _function("install_release")
    assert "systemd-run --unit $INSTALL_UNIT" in body
    assert body.index("systemd-run") < body.index("$SENTINEL") < body.index("$IMAGES_MARKER")


def test_wake_hits_the_site_until_running_then_waits_for_ssh():
    body = _function("wake_node")
    assert '"$SITE/health"' in body
    assert body.index("RUNNING") < body.index("ssh_node true")
    assert "instances start" not in body  # the front door wakes it, as a visitor's hit does


def test_the_stack_is_awaited_before_its_environment_is_read():
    steps = _steps()
    assert steps.index("wait_for_stack") < steps.index("preflight_engine")
    body = _function("wait_for_stack")
    assert (
        body.index("google-startup-scripts")
        < body.index("$API_CONTAINER")
        < body.index("verify_api")
    )


def test_the_hot_patch_still_sources_the_shared_library():
    text = _HOTPATCH.read_text()
    assert '. "$REPO/scripts/cloud-node-lib.sh"' in text
    lib = _LIB.read_text()
    for name in ("ssh_node", "preflight_engine", "reset_state", "verify_demo", "verify_engine"):
        assert re.search(rf"^{name}\(\) \{{", lib, re.M), name
        assert not re.search(rf"^{name}\(\) \{{", text, re.M), name


@pytest.mark.parametrize("path", [_SCRIPT, _LIB], ids=lambda p: p.name)
def test_no_check_stops_reading_a_pipe_early(path):
    # Under pipefail `producer | grep -q` fails whenever the producer was still writing.
    early = [
        line
        for line in path.read_text().splitlines()
        if re.search(r"\|\s*grep\s+-[A-Za-z]*q", line)
    ]
    assert not early, early


@pytest.mark.parametrize("path", [_SCRIPT, _LIB], ids=lambda p: p.name)
def test_the_scripts_parse(path):
    done = subprocess.run(["bash", "-n", str(path)], capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
