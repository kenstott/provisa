# Copyright (c) 2026 Kenneth Stott
# Canary: 6a57880b-068a-4aa4-8c2b-d10cb6803af5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Trino JVM heap fits inside the container that runs it.

The jvm_heap_gb setting defaulted to 8 and trino/etc/jvm.config carried -Xmx8G, while
docker-compose.core.yml caps the engine container at mem_limit 7g. G1 grows the heap toward -Xmx
over a long run, so the cgroup killed the coordinator mid-way through the functions conformance
run (run 44511176594: 6.44 of 7 GiB, then connection refused on tsvector_cmp and reset on
tsvector_concat while Trino restarted). The default heap, the shipped jvm.config and every
container that mounts it agree, with room left for the JVM's native memory.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

from provisa.api import trino_setup
from provisa.core.models import ProvisaConfig

ROOT = Path(__file__).resolve().parents[2]
JVM_CONFIG = ROOT / "trino" / "etc" / "jvm.config"
CORE_COMPOSE = ROOT / "docker-compose.core.yml"

# The heap's largest share of the container: the rest is metaspace, thread stacks, direct
# buffers, the code cache and the OpenTelemetry agent.
MAX_HEAP_SHARE = 0.75


def _heap_gb(text: str) -> int:
    found = re.findall(r"^-Xmx(\d+)G$", text, flags=re.MULTILINE)
    assert len(found) == 1, found
    return int(found[0])


def _limit_gb(limit: str) -> float:
    m = re.fullmatch(r"(\d+)([mg])", limit)
    assert m is not None, limit
    return int(m.group(1)) / (1024 if m.group(2) == "m" else 1)


def _services_mounting_jvm_config() -> dict[str, dict]:
    services = yaml.safe_load(CORE_COMPOSE.read_text())["services"]
    return {
        name: svc
        for name, svc in services.items()
        if any("etc/jvm.config:" in str(v) for v in svc.get("volumes", []))
    }


def test_shipped_jvm_config_is_the_default_heap() -> None:
    default = ProvisaConfig.model_fields["jvm_heap_gb"].default
    assert _heap_gb(JVM_CONFIG.read_text()) == default
    assert _heap_gb(trino_setup._JVM_TEMPLATE.format(heap_gb=default)) == default


def test_default_heap_fits_every_container_mounting_jvm_config() -> None:
    heap = _heap_gb(JVM_CONFIG.read_text())
    services = _services_mounting_jvm_config()
    assert {"trino", "trino-worker"} <= set(services), sorted(services)
    for name, svc in services.items():
        limit = svc.get("mem_limit")
        assert limit is not None, f"{name}: no mem_limit for a {heap}G heap"
        assert heap <= _limit_gb(str(limit)) * MAX_HEAP_SHARE, (
            f"{name}: a {heap}G heap leaves no native room in mem_limit {limit}"
        )
