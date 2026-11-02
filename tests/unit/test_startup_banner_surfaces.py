# Copyright (c) 2026 Kenneth Stott
# Canary: 3b9e6d14-58a2-4c7f-b0e1-9d2a6f4c8e73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The local start scripts' banner lists every query surface the backend serves.

Each script's ``print_query_surfaces`` is run in bash: the HTTP lines must be exactly
``system_health.HTTP_SURFACES``, and the gRPC and Arrow Flight lines must print the ports the
script hands the backend, under the environment variables the backend reads them from.
"""

# Requirements: REQ-2193

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

from provisa.api.admin.system_health import HTTP_SURFACES
from provisa.core.settings_catalog import DECLARED

REPO = Path(__file__).resolve().parents[2]
SCRIPTS = ["start-ui-install.sh", "start-ui.sh"]
_API_PORT = "18001"
_GRPC_PORT = "15051"
_FLIGHT_PORT = "18815"


def _function(script: str, name: str) -> str:
    """The text of the shell function ``name`` in ``script``, from its header to its closing brace."""
    text = (REPO / script).read_text()
    match = re.search(rf"^{name}\(\) \{{\n.*?^\}}\n", text, re.MULTILINE | re.DOTALL)
    assert match, f"{script} defines no {name}()"
    return match.group(0)


def _banner(script: str, **env: str) -> list[str]:
    """The lines ``print_query_surfaces`` of ``script`` prints under ``env``."""
    source = _function(script, "print_query_surfaces")
    if script == "start-ui-install.sh":
        source += _function(script, "print_mcp_info")
    out = subprocess.run(
        ["bash", "-c", f"set -euo pipefail\n{source}\nprint_query_surfaces"],
        env={
            "PATH": "/usr/bin:/bin",
            "PROVISA_API_PORT": _API_PORT,
            "GRPC_PORT": _GRPC_PORT,
            "FLIGHT_PORT": _FLIGHT_PORT,
            "PROVISA_MCP_PORT": "0",
            **env,
        },
        capture_output=True,
        text=True,
        check=True,
    )
    return out.stdout.splitlines()


@pytest.mark.parametrize("script", SCRIPTS)
def test_every_http_surface_has_its_line(script):
    lines = _banner(script)
    for name, path in HTTP_SURFACES.items():
        url = f"http://localhost:{_API_PORT}{path}"
        assert any(line.split()[1:2] == [url] for line in lines), (
            f"{script}: no banner line for {name} at {url}"
        )


@pytest.mark.parametrize("script", SCRIPTS)
def test_no_http_line_names_a_path_the_backend_does_not_serve(script):
    printed = {
        m.group(1)
        for line in _banner(script)
        if (m := re.search(rf"http://localhost:{_API_PORT}(/\S+)", line))
    }
    assert printed <= set(HTTP_SURFACES.values()), printed - set(HTTP_SURFACES.values())


@pytest.mark.parametrize("script", SCRIPTS)
def test_grpc_and_flight_lines_print_the_ports_the_backend_reads(script):
    envs = {s.key: s.env for s in DECLARED}
    assert (envs["server.grpc_port"], envs["server.flight_port"]) == ("GRPC_PORT", "FLIGHT_PORT")
    lines = _banner(script)
    assert f"  grpc:    localhost:{_GRPC_PORT}" in "\n".join(lines)
    assert f"  flight:  grpc://localhost:{_FLIGHT_PORT}" in "\n".join(lines)
    text = (REPO / script).read_text()
    assert 'export GRPC_PORT="${GRPC_PORT:-50051}"' in text
    assert 'export FLIGHT_PORT="${FLIGHT_PORT:-8815}"' in text


@pytest.mark.parametrize("script", SCRIPTS)
def test_a_listener_on_port_zero_has_no_line(script):
    joined = "\n".join(_banner(script, PROVISA_BOLT_PORT="0", PROVISA_MCP_PORT="0"))
    assert "bolt:" not in joined
    assert "mcp:" not in joined
    joined = "\n".join(_banner(script, PROVISA_BOLT_PORT="17687"))
    assert "  bolt:    bolt://localhost:17687" in joined


@pytest.mark.parametrize("script", SCRIPTS)
def test_both_banner_blocks_print_the_surfaces(script):
    text = (REPO / script).read_text()
    # One call per banner block (demo mode and the other), after the function's definition.
    assert text.count("\n  print_query_surfaces\n") == 2
