# Copyright (c) 2026 Kenneth Stott
# Canary: 6cf1b1e6-2d2b-4a5f-8ad4-8e0d0f5c1a71
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""No deployment points telemetry at a collector it does not run.

The otel-collector service is defined only in docker-compose.observability.yml. An app-overlay
default of http://otel-collector:4317 therefore names a nonexistent host in every deployment that
omits that overlay — the SaaS node logged unbounded "Failed to resolve 'otel-collector'" retries for
/v1/traces, /v1/logs and /v1/metrics. Both the app container env and Trino's coordinator config are
derived from OTEL_EXPORTER_OTLP_ENDPOINT instead, and stay off when it is unset.
"""

from __future__ import annotations

import os
from pathlib import Path

import yaml

from provisa.api import trino_setup

_REPO = Path(__file__).resolve().parents[2]


def test_app_overlay_does_not_default_the_otlp_endpoint():
    compose = yaml.safe_load((_REPO / "docker-compose.app.yml").read_text())
    env = compose["services"]["provisa"]["environment"]
    assert env["OTEL_EXPORTER_OTLP_ENDPOINT"] == "${OTEL_EXPORTER_OTLP_ENDPOINT:-}"


def test_only_the_observability_overlay_defines_the_collector():
    obs = yaml.safe_load((_REPO / "docker-compose.observability.yml").read_text())
    assert "image" in obs["services"]["otel-collector"]
    for other in ("docker-compose.core.yml", "docker-compose.app.yml"):
        services = yaml.safe_load((_REPO / other).read_text())["services"]
        assert "otel-collector" not in services, other


def test_provisa_cli_exports_the_endpoint_only_with_an_observability_overlay():
    script = (_REPO / "scripts" / "provisa").read_text()
    assert "*observability*)" in script
    # dev.yml's otel-collector block is an override with no image; the overlay that defines the
    # service must be merged whenever --observability is passed.
    assert 'if { [ "$OBSERVABILITY" = "true" ] || [ "$RUNTIME" = "bundled" ]; } &&' in script


def _render(tmp_path: Path) -> str:
    cfg = {
        "jvm_heap_gb": 4,
        "query_max_memory": "2GB",
        "query_max_memory_per_node": "1GB",
        "query_max_total_memory": "2GB",
        "fault_tolerant_execution": False,
        "node_role": "coordinator",
    }
    (tmp_path / "config").mkdir()
    # _write logs and moves on when a target directory is absent, so the tree must exist first.
    (tmp_path / "trino" / "etc" / "worker").mkdir(parents=True)
    cfg_path = tmp_path / "config" / "provisa.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg))
    trino_setup.write_trino_config(str(cfg_path))
    return (tmp_path / "trino" / "etc" / "config.properties").read_text()


def test_trino_tracing_is_off_without_an_endpoint(tmp_path, monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    props = _render(tmp_path)
    assert "tracing.enabled" not in props
    assert "otel.exporter.endpoint" not in props


def test_trino_tracing_uses_the_configured_endpoint(tmp_path, monkeypatch):
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector.internal:4317")
    props = _render(tmp_path)
    assert "tracing.enabled=true" in props
    assert "otel.exporter.endpoint=http://collector.internal:4317" in props
    assert os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] == "http://collector.internal:4317"
