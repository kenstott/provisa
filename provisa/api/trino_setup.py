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
{fte_block}tracing.enabled=true
otel.exporter.endpoint=http://otel-collector:4317
"""

_WORKER_TEMPLATE = """coordinator=false
http-server.http.port=8080
discovery.uri=http://trino:8080
catalog.management=dynamic
query.max-memory-per-node={query_max_memory_per_node}
{fte_block}"""

# Fault-tolerant execution replaces legacy spill-to-disk. Rendered into both node
# configs when enabled; requires a spool (exchange manager) — see below.
_FTE_TEMPLATE = """retry-policy=TASK
task.low-memory-killer.policy=total-reservation-on-blocked-nodes
fault-tolerant-execution-task-memory={task_memory}
"""

# Local filesystem spool (single host / shared volume).
_EXCHANGE_FILESYSTEM_TEMPLATE = """exchange-manager.name=filesystem
exchange.base-directories={spool_dir}
"""

# S3-backed spool (multi-host: a local dir is not shared across hosts).
_EXCHANGE_S3_TEMPLATE = """exchange-manager.name=filesystem
exchange.base-directories=s3://{bucket}
exchange.s3.endpoint={endpoint}
exchange.s3.region={region}
exchange.s3.aws-access-key={access_key}
exchange.s3.aws-secret-key={secret_key}
exchange.s3.path-style-access=true
"""

_RESOURCE_GROUPS_PROPERTIES_TEMPLATE = """resource-groups.configuration-manager=file
resource-groups.config-file=/etc/trino/resource-groups.json
"""

# docker-compose.core.yml trino service volumes must include:
#   - ./trino/etc/resource-groups.json:/etc/trino/resource-groups.json:ro
#   - ./trino/etc/resource-groups.properties:/etc/trino/resource-groups.properties:ro


def _cfg(cfg: dict, key: str) -> Any:
    """Read a config value, falling back to the single source of truth for its
    default — the ProvisaConfig field default in models.py — never a literal here."""
    from provisa.core.models import ProvisaConfig

    return cfg.get(key, ProvisaConfig.model_fields[key].default)


def _exchange_manager_config(cfg: dict) -> str:
    """Render exchange-manager.properties from config: S3 spool when an endpoint is
    set (multi-host), otherwise a local filesystem spool."""
    if _cfg(cfg, "exchange_spool_s3_endpoint"):
        return _EXCHANGE_S3_TEMPLATE.format(
            bucket=_cfg(cfg, "exchange_spool_bucket"),
            endpoint=_cfg(cfg, "exchange_spool_s3_endpoint"),
            region=_cfg(cfg, "exchange_spool_s3_region"),
            access_key=_cfg(cfg, "exchange_spool_s3_access_key"),
            secret_key=_cfg(cfg, "exchange_spool_s3_secret_key"),
        )
    return _EXCHANGE_FILESYSTEM_TEMPLATE.format(spool_dir=_cfg(cfg, "exchange_spool_dir"))


def write_trino_config(config_path: str) -> None:  # REQ-055, REQ-250
    """Regenerate trino/etc/jvm.config and trino/etc/config.properties from provisa config."""
    # Falling back to defaults on unreadable config silently drops FTE/memory settings — propagate.
    with open(config_path) as _f:
        cfg = yaml.safe_load(_f) or {}

    heap_gb = int(_cfg(cfg, "jvm_heap_gb"))
    query_max_memory = _cfg(cfg, "query_max_memory")
    query_max_memory_per_node = _cfg(cfg, "query_max_memory_per_node")
    query_max_total_memory = _cfg(cfg, "query_max_total_memory")

    fte_block = (
        _FTE_TEMPLATE.format(task_memory=_cfg(cfg, "fault_tolerant_task_memory"))
        if _cfg(cfg, "fault_tolerant_execution")
        else ""
    )

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(config_path)))
    trino_etc = os.path.join(project_root, "trino", "etc")

    _write(
        os.path.join(trino_etc, "jvm.config"),
        _JVM_TEMPLATE.format(heap_gb=heap_gb),
    )
    coordinator_props = _COORDINATOR_TEMPLATE.format(
        query_max_memory=query_max_memory,
        query_max_memory_per_node=query_max_memory_per_node,
        query_max_total_memory=query_max_total_memory,
        fte_block=fte_block,
    )
    worker_props = _WORKER_TEMPLATE.format(
        query_max_memory_per_node=query_max_memory_per_node,
        fte_block=fte_block,
    )
    # This instance's config.properties reflects its selected role (REQ-916); the worker/ variant is
    # always emitted so a multi-node deployment can mount it on worker instances.
    this_role = _cfg(cfg, "node_role")
    _write(
        os.path.join(trino_etc, "config.properties"),
        worker_props if this_role == "worker" else coordinator_props,
    )
    _write(os.path.join(trino_etc, "worker", "config.properties"), worker_props)
    if fte_block:
        _write(
            os.path.join(trino_etc, "exchange-manager.properties"),
            _exchange_manager_config(cfg),
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
            with open(path) as _existing:
                if _existing.read() == content:
                    return
        except OSError:
            pass
        with open(path, "w") as _f:
            _f.write(content)
        _log.debug("wrote %s", path)
    except Exception as exc:
        _log.debug("could not write %s: %s", path, exc)
