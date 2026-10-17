# Copyright (c) 2026 Kenneth Stott
# Canary: 3a103a8d-6333-49df-a99d-001aeaea09e8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""deploy-cloud.sh: a patch gives the node every variable the compose files it pushes require,
from what the node runs (push_store_env); and the demos each sandbox tier offers come from one
list, which the patch overlay, terraform's sandbox VM and first-launch all read."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO / "scripts" / "deploy-cloud.sh"


def _function(name: str) -> str:
    text = _SCRIPT.read_text()
    start = text.index(f"{name}() {{")
    return text[start : text.index("\n}\n", start) + 3]


def _run(
    tmp_path: Path, env_lines: list[str]
) -> tuple[subprocess.CompletedProcess, dict[str, str]]:
    """push_store_env against a node faked locally: its env file, a docker that reports a MinIO's
    environment, and sudo that just runs."""
    env_file = tmp_path / "provisa.env"
    env_file.write_text("".join(f"{line}\n" for line in env_lines))
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "sudo").write_text('#!/bin/bash\n"$@"\n')
    (bin_dir / "docker").write_text(
        "#!/bin/bash\nprintf 'PATH=/usr/bin\\nMINIO_ROOT_USER=root-user\\nMINIO_ROOT_PASSWORD=root-pw\\n'\n"
    )
    # The node's sed is GNU's, whose -i takes no suffix; BSD's (macOS) needs an empty one.
    (bin_dir / "sed").write_text(
        '#!/bin/bash\nif [ "$(uname)" = Darwin ] && [ "$1" = -i ]; then shift; exec /usr/bin/sed -i "" "$@"; fi\n'
        'exec /usr/bin/sed "$@"\n'
    )
    for tool in ("sudo", "docker", "sed"):
        (bin_dir / tool).chmod(0o755)
    script = "\n".join(
        [
            f"ENV_FILE={env_file}",
            'ssh_node() { bash -c "$1"; }',
            _function("set_node_env"),
            'MINIO_CONTAINER="compose-minio-1"',
            _function("push_store_env"),
            "push_store_env",
        ]
    )
    result = subprocess.run(
        ["bash", "-c", script],
        env={"PATH": f"{bin_dir}:/usr/bin:/bin:/opt/homebrew/bin:/usr/local/bin"},
        capture_output=True,
        text=True,
    )
    values = dict(line.split("=", 1) for line in env_file.read_text().split())
    return result, values


def test_a_node_from_an_earlier_release_is_given_what_it_runs(tmp_path):
    result, values = _run(tmp_path, ["PROVISA_MINIO_BIND_IP=10.10.0.4"])
    assert result.returncode == 0, result.stderr
    assert values["PROVISA_STORE_BIND_IP"] == "10.10.0.4"
    assert values["PROVISA_OTEL_S3_ACCESS_KEY"] == "root-user"
    assert values["PROVISA_OTEL_S3_SECRET_KEY"] == "root-pw"
    assert re.fullmatch(r"[0-9a-f]{48}", values["PROVISA_GRAFANA_ADMIN_PASSWORD"])
    assert "root-pw" not in result.stdout + result.stderr
    assert "store env: reconciled" in result.stdout


def test_values_a_node_already_has_are_kept(tmp_path):
    kept = [
        "PROVISA_MINIO_BIND_IP=10.10.0.4",
        "PROVISA_STORE_BIND_IP=127.0.0.1",
        "PROVISA_OTEL_S3_ACCESS_KEY=a",
        "PROVISA_OTEL_S3_SECRET_KEY=b",
        "PROVISA_GRAFANA_ADMIN_PASSWORD=c",
    ]
    result, values = _run(tmp_path, kept)
    assert result.returncode == 0, result.stderr
    assert values == dict(line.split("=", 1) for line in kept)
    assert "store env: up to date" in result.stdout


def test_a_node_naming_no_store_address_is_refused(tmp_path):
    result, _ = _run(tmp_path, [])
    assert result.returncode != 0
    assert "PROVISA_MINIO_BIND_IP" in result.stderr


@pytest.mark.parametrize("arm", ["api", "cfg", "all", "patch"])
def test_every_arm_that_pushes_compose_files_reconciles_first(arm):
    line = next(line for line in _SCRIPT.read_text().splitlines() if re.match(rf"\s+{arm}\)", line))
    body = _SCRIPT.read_text().split(line, 1)[1].split(";;", 1)[0]
    assert body.index("push_store_env") < body.index("push_app") < body.index("push_obs")


# --- one list of the demos each tier offers --------------------------------------------------

_LISTS = _REPO / "packaging" / "linux" / "demo-stores"


def _list(name: str) -> str:
    return (_LISTS / name).read_text().strip()


def test_the_patch_overlay_and_the_sandbox_vm_read_the_lists():
    assert "packaging/linux/demo-stores/sandbox-tier-bundles" in _SCRIPT.read_text()
    tf = (_REPO / "terraform" / "gcp-saas" / "sandbox.tf").read_text()
    assert "demo-stores/sandbox-tier-bundles" in tf and "demo-stores/regions-tier-bundles" in tf
    assert "PROVISA_DEMO_BUNDLES=${local.sandbox_tier_bundles},${local.regions_tier_bundles}" in tf


def test_first_launch_offers_what_the_lists_say():
    text = (_REPO / "packaging" / "linux" / "first-launch.sh").read_text()
    offered = re.findall(r'PROVISA_DEMO_BUNDLES: "([^"]+)"', text)
    assert sorted(offered) == sorted([_list("sandbox-tier-bundles"), _list("regions-tier-bundles")])
