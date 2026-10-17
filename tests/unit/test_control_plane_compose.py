# Copyright (c) 2026 Kenneth Stott
# Canary: 20b6d9c4-0ef3-4300-9b7c-42f2e1e6b42c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""The control-plane node's compose (first-launch.sh write_control_plane_compose) splits into the
control plane and the sandbox tier by compose profile (REQ-2006), so the sandbox tier and its
demo stores can run on a VM of their own that reaches the control plane's database, redis and
minio by address."""

# Requirements: REQ-2006, REQ-1451

from __future__ import annotations

import subprocess
from pathlib import Path

import yaml

_FIRST_LAUNCH = Path(__file__).resolve().parents[2] / "packaging" / "linux" / "first-launch.sh"


def _compose(tmp_path: Path) -> dict:
    script = _FIRST_LAUNCH.read_text()
    start = script.index("write_control_plane_compose() {")
    body = script[start : script.index("\n}\n", start) + 3]
    subprocess.run(
        [
            "bash",
            "-c",
            f'set -e; ok() {{ :; }}; ROLE=control-plane; PROVISA_HOME="$1"; CERT_DIR=/certs\n{body}\nwrite_control_plane_compose',
            "_",
            str(tmp_path),
        ],
        check=True,
    )
    return yaml.safe_load((tmp_path / "compose-control-plane.yml").read_text())


def test_every_service_is_the_control_planes_or_the_sandbox_tiers(tmp_path):
    services = _compose(tmp_path)["services"]
    profiles = {name: service.get("profiles") for name, service in services.items()}
    assert profiles == {
        "provisa": ["control"],
        "provisa-sandbox": ["sandbox"],
        # REQ-2068: the regions sandbox tier and each region's own PostgreSQL.
        "provisa-sandbox-regions": ["sandbox"],
        "region-pg-eu": ["sandbox"],
        "region-pg-us": ["sandbox"],
        "provisa-ui": ["control"],
        "redis": ["control"],
        "minio": ["control"],
        "minio-init": ["control"],
    }


def test_the_sandbox_tier_reaches_the_control_plane_by_address_when_it_runs_alone(tmp_path):
    services = _compose(tmp_path)["services"]
    env = services["provisa-sandbox"]["environment"]
    assert env["REDIS_URL"] == (
        "redis://:${PROVISA_REDIS_PASSWORD:?the deployment Redis password}"
        "@${PROVISA_REDIS_HOST:-redis}:6379"
    )
    assert env["PROVISA_OTEL_S3_ENDPOINT"] == "http://${PROVISA_MINIO_HOST:-minio}:9000"
    assert env["PG_HOST"].startswith("${CONFIG_DB_HOST:?")
    assert env["PROVISA_SANDBOX_INSTANCE"] == "${PROVISA_SANDBOX_INSTANCE:-}"
    assert env["PROVISA_SANDBOX_IDLE_STOP_SECONDS"] == "${PROVISA_SANDBOX_IDLE_STOP_SECONDS:-}"
    # Without the control profile there is no redis or minio in the file to wait for.
    assert all(
        dep["required"] is False for dep in services["provisa-sandbox"]["depends_on"].values()
    )
    ui = services["provisa-ui"]
    assert (
        ui["environment"]["PROVISA_SANDBOX_API_URL"]
        == "${PROVISA_SANDBOX_API_URL:-https://provisa-sandbox:8000}"
    )
    assert ui["depends_on"]["provisa-sandbox"]["required"] is False
    assert ui["environment"]["PROVISA_SANDBOX_INSTANCE"] == "${PROVISA_SANDBOX_INSTANCE:-}"
    # A sandbox VM publishes the tier on its VPC address; a node running both, on loopback only.
    assert services["provisa-sandbox"]["ports"] == [
        "${PROVISA_SANDBOX_PUBLISH:-127.0.0.1:18000}:8000"
    ]
    # Published on the VPC address only, as minio is: a sandbox node's tier reaches it there.
    assert services["redis"]["ports"] == [
        "${PROVISA_MINIO_BIND_IP:?the node's VPC-internal address}:6379:6379"
    ]


def test_a_control_plane_node_runs_both_unless_told_otherwise():
    script = _FIRST_LAUNCH.read_text()
    assert 'export COMPOSE_PROFILES="${COMPOSE_PROFILES:-control,sandbox}"' in script
    env_vars = script[
        script.index("for var in PROVISA_IDP") : script.index(
            "; do", script.index("for var in PROVISA_IDP")
        )
    ]
    for name in (
        "COMPOSE_PROFILES",
        "PROVISA_REDIS_HOST",
        "PROVISA_MINIO_HOST",
        "PROVISA_SANDBOX_API_URL",
        "PROVISA_SANDBOX_PUBLISH",
    ):
        assert name in env_vars.split(), name


def test_the_regions_tier_runs_as_its_visitor_region_with_a_store_per_region(tmp_path):
    """REQ-2068: its own boot org and config, the us region, a PostgreSQL per region (never one
    shared), the demo bundles the sync pulls, and the demos that select regions."""
    services = _compose(tmp_path)["services"]
    tier = services["provisa-sandbox-regions"]
    env = tier["environment"]
    assert env["ORG_ID"] == "sbxrtier"
    assert env["PROVISA_CONFIG"] == "/app/config/provisa-sandbox-regions.yaml"
    assert env["PROVISA_REGION"] == "us"
    assert env["PROVISA_DEMO_BUNDLES"] == "retail"
    assert "@region-pg-eu:5432/" in env["PROVISA_DEMO_EU_PG_URL"]
    assert "@region-pg-us:5432/" in env["PROVISA_DEMO_US_PG_URL"]
    assert "--port" in tier["command"] and "8100" in tier["command"]
    assert tier["ports"] == ["${PROVISA_SANDBOX_REGIONS_PUBLISH:-127.0.0.1:18100}:8100"]
    assert f"{tmp_path}/sandbox-data/demo-bundles:/var/lib/provisa/demo-bundles" in tier["volumes"]
    eu, us = services["region-pg-eu"], services["region-pg-us"]
    assert eu["volumes"] != us["volumes"]
    assert "ports" not in eu and "ports" not in us
    ui = services["provisa-ui"]["environment"]
    assert ui["PROVISA_SANDBOX_REGIONS_DEMOS"] == "retail"
    assert "provisa-sandbox-regions:8100" in ui["PROVISA_SANDBOX_REGIONS_API_URL"]
