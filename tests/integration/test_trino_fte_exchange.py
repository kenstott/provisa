# Copyright (c) 2026 Kenneth Stott
# Canary: ce341f4e-0d36-4279-aec3-075183c061ca
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-817 Trino Memory Management — Fault-Tolerant Execution (FTE) config.

Validates that both Docker and Helm deployments wire up FTE with a shared
exchange manager (replacing legacy spill-to-disk) and that the Helm exchange
Secret fails loud when no exchange store is provided.
"""

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

pytestmark = [pytest.mark.integration]

REPO = Path(__file__).resolve().parents[2]
CHART = REPO / "helm" / "provisa"

FTE_SETTINGS = {
    "retry-policy": "TASK",
    "task.low-memory-killer.policy": "total-reservation-on-blocked-nodes",
}


def _parse_props(path: Path) -> dict[str, str]:
    props: dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        props[k.strip()] = v.strip()
    return props


class TestDockerFTEConfig:
    @pytest.mark.parametrize(
        "cfg",
        ["trino/etc/config.properties", "trino/etc/worker/config.properties"],
    )
    def test_fte_settings_present(self, cfg):
        props = _parse_props(REPO / cfg)
        for key, expected in FTE_SETTINGS.items():
            assert props.get(key) == expected, f"{cfg}: {key}={props.get(key)!r}"

    def test_exchange_manager_filesystem(self):
        props = _parse_props(REPO / "trino/etc/exchange-manager.properties")
        assert props["exchange-manager.name"] == "filesystem"
        assert props["exchange.base-directories"] == "/data/provisa/exchange"

    def test_compose_shares_exchange_across_nodes(self):
        compose = yaml.safe_load((REPO / "docker-compose.core.yml").read_text())
        services = compose["services"]
        for svc in ("trino", "trino-worker"):
            vols = services[svc]["volumes"]
            assert any("exchange-manager.properties" in v for v in vols), svc
            assert any(v.startswith("provisa_exchange:") for v in vols), (
                f"{svc} missing shared exchange volume"
            )
        assert "provisa_exchange" in compose["volumes"]


needs_helm = pytest.mark.skipif(shutil.which("helm") is None, reason="helm CLI not installed")


def _render_secret(*sets: str) -> subprocess.CompletedProcess:
    cmd = [
        "helm",
        "template",
        "t",
        str(CHART),
        "--show-only",
        "templates/trino-exchange-secret.yaml",
    ]
    for s in sets:
        cmd += ["--set", s]
    return subprocess.run(cmd, capture_output=True, text=True)


@needs_helm
class TestHelmExchangeSecret:
    def test_minio_backed_exchange_uses_s3(self):
        r = _render_secret("trino.exchange.s3.endpoint=")
        assert r.returncode == 0, r.stderr
        assert "exchange.base-directories=s3://provisa-exchange" in r.stdout
        assert "exchange.s3.endpoint=http://t-minio:9000" in r.stdout

    def test_fail_loud_when_no_exchange_store(self):
        r = _render_secret("minio.enabled=false", "trino.exchange.s3.endpoint=")
        assert r.returncode != 0
        assert "needs an exchange store" in r.stderr

    def test_fail_loud_when_s3_endpoint_missing_credentials(self):
        r = _render_secret("trino.exchange.s3.endpoint=https://s3.amazonaws.com")
        assert r.returncode != 0
        assert "accessKey/secretKey are empty" in r.stderr

    def test_bucket_init_job_runs_only_for_incluster_minio(self):
        cmd = [
            "helm",
            "template",
            "t",
            str(CHART),
            "--show-only",
            "templates/trino-exchange-bucket-job.yaml",
        ]
        with_minio = subprocess.run(cmd, capture_output=True, text=True)
        assert with_minio.returncode == 0, with_minio.stderr
        assert "role: exchange-bucket-init" in with_minio.stdout

        external = subprocess.run(
            cmd
            + [
                "--set",
                "trino.exchange.s3.endpoint=https://s3.amazonaws.com",
                "--set",
                "trino.exchange.s3.accessKey=k",
                "--set",
                "trino.exchange.s3.secretKey=s",
            ],
            capture_output=True,
            text=True,
        )
        assert "role: exchange-bucket-init" not in external.stdout
