# Copyright (c) 2026 Kenneth Stott
# Canary: ebeab983-3d2a-400c-ac8c-f5f927956df7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What an install starts names nothing outside the install (#201).

`provisa start` runs docker-compose.core.yml + docker-compose.app.yml (scripts/provisa). The core
file once defaulted four bind paths to one developer's checkout: on any other host the Hive
warehouse volume failed to mount and the other three were created there as root. Mounts a test or
development stack needs live in docker-compose.e2e-mounts.yml, whose variables have no default.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
OVERLAY = "docker-compose.e2e-mounts.yml"
# What `provisa start` can add to the core file (scripts/provisa _build_compose_files).
INSTALL_FILES = (
    "docker-compose.core.yml",
    "docker-compose.app.yml",
    "docker-compose.airgap.yml",
    "docker-compose.observability.yml",
    "docker-compose.dev.yml",
    "docker-compose.trino-internal-tls.yml",  # REQ-2072, when the launch settings turn it on
)
_VARS = (
    "PROVISA_E2E_FILE_LAKE_HOST",
    "PROVISA_E2E_SHAREPOINT_CERT_HOST",
    "PROVISA_E2E_HIVE_WAREHOUSE_HOST",
)


def _code(name: str) -> str:
    lines = (REPO / name).read_text().splitlines()
    return "\n".join(line for line in lines if not line.lstrip().startswith("#"))


def test_what_an_install_starts_names_no_developer_path_and_no_test_mount():
    for name in INSTALL_FILES:
        code = _code(name)
        assert "PROVISA_E2E_" not in code, name
        # No default of a variable is an absolute host path, and no home or volume path is named.
        assert not re.search(r":-/(Volumes|Users|home)/", code), name
        assert "sharepoint.pfx" not in code, name


def test_the_install_reads_exactly_those_files():
    script = (REPO / "scripts" / "provisa").read_text()
    named = set(re.findall(r"\$\{PROJECT_DIR\}/(docker-compose\.[a-z-]+\.yml)", script))
    assert named == set(INSTALL_FILES)
    assert OVERLAY not in script


def test_the_hive_warehouse_of_an_install_is_a_plain_named_volume():
    core = yaml.safe_load((REPO / "docker-compose.core.yml").read_text())
    assert core["volumes"]["hive_warehouse"] is None


def test_the_test_mounts_have_no_default():
    code = _code(OVERLAY)
    for var in _VARS:
        assert f"${{{var}:?" in code, var
        assert f"${{{var}:-" not in code, var
    overlay = yaml.safe_load((REPO / OVERLAY).read_text())
    assert set(overlay["services"]) == {"trino", "trino-worker", "hive-metastore"}
    assert overlay["volumes"]["hive_warehouse"]["driver_opts"]["o"] == "bind"


def test_every_test_stack_adds_the_overlay_right_after_the_core_file():
    from tests.itest_stack import COMPOSE_ARGS, CORE_COMPOSE, E2E_MOUNTS_COMPOSE

    files = [COMPOSE_ARGS[i + 1] for i, arg in enumerate(COMPOSE_ARGS) if arg == "-f"]
    assert files[:2] == [CORE_COMPOSE, E2E_MOUNTS_COMPOSE]
    for path in (
        "tests/e2e/conftest.py",
        "tests/integration/test_trino_worker_fanout.py",
        "scripts/run-linux-mem-lane.sh",
        "scripts/ci/lanes/trino-stack-prepare.sh",
        "start-ui.sh",
        "start-ui-install.sh",
        "reset-volumes.sh",
    ):
        text = (REPO / path).read_text()
        assert OVERLAY in text, path
        # No command of a test starter brings the core file up without it.
        for line in text.splitlines():
            if re.search(r"compose -f docker-compose\.core\.yml .*\bup\b", line):
                assert OVERLAY in line, f"{path}: {line.strip()}"


def test_a_development_starter_names_the_mounts_from_its_own_checkout():
    """The overlay's variables have no default; a starter that adds it sets each one to a path
    under the directory the script itself is in, unless the caller already set it."""
    for script in ("start-ui.sh", "start-ui-install.sh", "reset-volumes.sh"):
        text = (REPO / script).read_text()
        assert f"-f {OVERLAY}" in text, script
        for var in _VARS:
            assert re.search(rf'export {var}="\$\{{{var}:-\$SCRIPT_DIR/[\w.-]+\}}"', text), (
                script,
                var,
            )
        # Named before the first compose command that reads them.
        assert text.index("PROVISA_E2E_FILE_LAKE_HOST") < text.index(
            "docker compose $COMPOSE_FILES"
        )


def test_trino_in_a_test_stack_can_name_the_host_and_an_install_is_not_given_it():
    """The e2e harness addresses a host-published source from inside Trino as
    host.docker.internal, which a Linux container resolves only when given it."""
    overlay = yaml.safe_load((REPO / OVERLAY).read_text())["services"]
    for service in ("trino", "trino-worker"):
        assert overlay[service]["extra_hosts"] == ["host.docker.internal:host-gateway"], service
    for name in INSTALL_FILES:
        assert "extra_hosts" not in _code(name), name
