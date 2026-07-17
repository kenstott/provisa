# Copyright (c) 2026 Kenneth Stott
# Canary: d463f19b-6858-4e7c-ab0d-be13bfd265db
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-836: Trino FTE exchange spooling volume is chowned to uid 1000 by a
one-shot init service before the coordinator and worker start.

Parses docker-compose.core.yml — no Docker daemon required.
"""

from __future__ import annotations

from pathlib import Path

import yaml

_REPO = Path(__file__).resolve().parents[2]
_EXCHANGE_DIR = "/data/provisa/exchange"


def _compose() -> dict:
    return yaml.safe_load((_REPO / "docker-compose.core.yml").read_text())


def test_exchange_init_service_chowns_to_uid_1000():
    svc = _compose()["services"]["trino-exchange-init"]
    cmd = svc["command"]
    assert _EXCHANGE_DIR in cmd
    assert "chown" in cmd and "1000:1000" in cmd
    assert any(v.startswith("provisa_exchange:") for v in svc["volumes"])


def test_trino_nodes_wait_for_exchange_init():
    services = _compose()["services"]
    for node in ("trino", "trino-worker"):
        deps = services[node]["depends_on"]
        assert "trino-exchange-init" in deps, f"{node} must depend on exchange init"
        assert deps["trino-exchange-init"]["condition"] == "service_completed_successfully", node


def test_exchange_volume_is_declared():
    assert "provisa_exchange" in _compose()["volumes"]
