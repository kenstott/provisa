# Copyright (c) 2026 Kenneth Stott
# Canary: 7b3e9d1a-2f4c-4a6e-8d0b-5c7f1e3a9b2d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Write Trino jvm.config and config.properties from provisa config."""

# Requirements: REQ-054, REQ-055, REQ-250, REQ-461

from __future__ import annotations

import logging
import os
from typing import Any

import trino.dbapi
import yaml

_log = logging.getLogger(__name__)

_JVM_TEMPLATE = """-server
-Xmx{heap_gb}G
-XX:InitialRAMPercentage=80
-XX:MaxRAMPercentage=80
-XX:+ExitOnOutOfMemoryError
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-Djdk.attach.allowAttachSelf=true
--add-modules=jdk.incubator.vector
-javaagent:/etc/trino/otel/opentelemetry-javaagent.jar
-Dotel.javaagent.enabled=false
-Dotel.service.name=federation-engine
-Dotel.exporter.otlp.endpoint=http://otel-collector:4317
-Dotel.exporter.otlp.protocol=grpc
"""

_COORDINATOR_TEMPLATE = """coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
catalog.management=dynamic
query.max-memory={query_max_memory}
query.max-memory-per-node={query_max_memory_per_node}
query.max-total-memory={query_max_total_memory}
spill-enabled={spill_enabled}
spiller-spill-path={spill_path}
spill-encryption-enabled=true
tracing.enabled=true
otel.exporter.endpoint=http://otel-collector:4317
"""

_WORKER_TEMPLATE = """coordinator=false
http-server.http.port=8080
discovery.uri=http://trino:8080
catalog.management=dynamic
query.max-memory-per-node={query_max_memory_per_node}
spill-enabled={spill_enabled}
spiller-spill-path={spill_path}
spill-encryption-enabled=true
"""

_RESOURCE_GROUPS_PROPERTIES_TEMPLATE = """resource-groups.configuration-manager=file
resource-groups.config-file=/etc/trino/resource-groups.json
"""

# docker-compose.core.yml trino service volumes must include:
#   - ./trino/etc/resource-groups.json:/etc/trino/resource-groups.json:ro
#   - ./trino/etc/resource-groups.properties:/etc/trino/resource-groups.properties:ro


def write_trino_config(config_path: str) -> None:  # REQ-055, REQ-250
    """Regenerate trino/etc/jvm.config and trino/etc/config.properties from provisa config."""
    cfg: dict = {}
    try:
        with open(config_path) as _f:
            cfg = yaml.safe_load(_f) or {}
    except Exception:
        pass

    heap_gb = int(cfg.get("jvm_heap_gb", 8))
    spill_enabled = str(cfg.get("spill_enabled", True)).lower()
    spill_path = cfg.get("spill_path", "/tmp/provisa-spill")  # nosec B108 - Trino spill dir default, config-overridable
    query_max_memory = cfg.get("query_max_memory", "4GB")
    query_max_memory_per_node = cfg.get("query_max_memory_per_node", "2GB")
    query_max_total_memory = cfg.get("query_max_total_memory", "8GB")

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(config_path)))
    trino_etc = os.path.join(project_root, "trino", "etc")

    _write(
        os.path.join(trino_etc, "jvm.config"),
        _JVM_TEMPLATE.format(heap_gb=heap_gb),
    )
    _write(
        os.path.join(trino_etc, "config.properties"),
        _COORDINATOR_TEMPLATE.format(
            query_max_memory=query_max_memory,
            query_max_memory_per_node=query_max_memory_per_node,
            query_max_total_memory=query_max_total_memory,
            spill_enabled=spill_enabled,
            spill_path=spill_path,
        ),
    )
    _write(
        os.path.join(trino_etc, "worker", "config.properties"),
        _WORKER_TEMPLATE.format(
            query_max_memory_per_node=query_max_memory_per_node,
            spill_enabled=spill_enabled,
            spill_path=spill_path,
        ),
    )
    _write(
        os.path.join(trino_etc, "resource-groups.properties"),
        _RESOURCE_GROUPS_PROPERTIES_TEMPLATE,
    )


def get_trino_connection(
    trino_conn_kwargs: dict[str, Any],
    tenant_id: str | None = None,
) -> trino.dbapi.Connection:  # REQ-054, REQ-461
    """Return a Trino connection scoped to tenant_id as the Trino user.

    In multi-tenant mode callers pass tenant_id; Trino resource groups use
    ${USER} to assign the query to the correct per-tenant group.
    When tenant_id is None the connection is made with the kwargs as-is
    (single-tenant / system pass-through).
    """
    kwargs = dict(trino_conn_kwargs)
    if tenant_id is not None:
        kwargs["user"] = tenant_id
    return trino.dbapi.connect(**kwargs)


def _write(path: str, content: str) -> None:
    try:
        try:
            if open(path).read() == content:
                return
        except OSError:
            pass
        with open(path, "w") as _f:
            _f.write(content)
        _log.debug("wrote %s", path)
    except Exception as exc:
        _log.debug("could not write %s: %s", path, exc)
