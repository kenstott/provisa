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

# Every node of the cluster authenticates the others with one secret, read from the environment the
# launcher gives the containers (Trino substitutes ${ENV:...} in its properties files).
_COORDINATOR_TEMPLATE = """coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri={discovery_uri}
catalog.management=dynamic
catalog.store=memory
internal-communication.shared-secret=${{ENV:PROVISA_TRINO_SHARED_SECRET}}
query.max-memory={query_max_memory}
query.max-memory-per-node={query_max_memory_per_node}
query.max-total-memory={query_max_total_memory}
{internal_tls_block}{fte_block}{tracing_block}"""

# Trino's OTel exporter has no "drop if unreachable" mode: with tracing.enabled=true and a
# collector that does not exist, the coordinator retries every span export for the life of the
# process. The block is therefore rendered only when OTEL_EXPORTER_OTLP_ENDPOINT names a real
# collector — the same signal docker-compose.app.yml passes through to the app container.
_TRACING_TEMPLATE = """tracing.enabled=true
otel.exporter.endpoint={endpoint}
"""

_WORKER_TEMPLATE = """coordinator=false
http-server.http.port=8080
discovery.uri={discovery_uri}
catalog.management=dynamic
catalog.store=memory
internal-communication.shared-secret=${{ENV:PROVISA_TRINO_SHARED_SECRET}}
query.max-memory-per-node={query_max_memory_per_node}
{internal_tls_block}{fte_block}"""

# Fault-tolerant execution replaces legacy spill-to-disk. Rendered into both node
# configs when enabled; requires a spool (exchange manager) — see below.
# Spooled exchange data is encrypted with a key made for each query and discarded when it ends
# (Trino's default, written out so no change of default turns it off).
_FTE_TEMPLATE = """retry-policy=TASK
fault-tolerant-execution.exchange-encryption-enabled=true
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


# Trino's internal TLS (engine.trino_internal_tls, off by default): every node requires HTTPS from
# the others, on its own HTTPS port, and finds the coordinator at its pinned address -- Trino 481
# addresses it by IP alone for this and makes the nodes' certificates itself.
_INTERNAL_HTTPS_PORT = 8443
_INTERNAL_TLS_TEMPLATE = f"""internal-communication.https.required=true
http-server.https.enabled=true
http-server.https.port={_INTERNAL_HTTPS_PORT}
"""

#: Beside the engine's config: what the launcher reads at the next start of the stack (the
#: coordinator's pinned address and the engine network it is on).
LAUNCH_SETTINGS = "launch-settings.env"


def dynamic_range(subnet: str) -> str:
    """The part of the engine network Docker hands out addresses from: its upper half, so the
    coordinator's pinned address, in the lower half, is never given to another container."""
    import ipaddress

    return str(list(ipaddress.ip_network(subnet).subnets(prefixlen_diff=1))[1])


def internal_tls() -> tuple[bool, str, str]:
    """Whether the nodes require HTTPS between them, the engine network, and the coordinator's
    address on it -- the values the engine starts on next."""
    from provisa.core import settings_registry

    return (
        bool(settings_registry.value("engine.trino_internal_tls")),
        str(settings_registry.value("engine.trino_internal_subnet")),
        str(settings_registry.value("engine.trino_coordinator_ip")),
    )


def _cfg(cfg: dict, key: str) -> Any:
    """Read a config value, falling back to the single source of truth for its
    default — the ProvisaConfig field default in models.py — never a literal here."""
    from provisa.core import settings_registry
    from provisa.core.models import ProvisaConfig
    from provisa.core.settings_catalog_engine import SIZING

    # REQ-1913: the engine's sizing is operator settings. A value stored through the settings page
    # is what the engine's files are rendered from — the value the engine will start on next.
    if key in SIZING:
        stored = settings_registry.stored_value(f"engine.{key}")
        if stored is not None:
            return stored
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
    # REQ-1669: includes-aware, so a wrapper config's fragments are seen.
    from provisa.core.config_loader import read_config_with_includes

    cfg = read_config_with_includes(config_path)

    heap_gb = int(_cfg(cfg, "jvm_heap_gb"))
    query_max_memory = _cfg(cfg, "query_max_memory")
    query_max_memory_per_node = _cfg(cfg, "query_max_memory_per_node")
    query_max_total_memory = _cfg(cfg, "query_max_total_memory")

    fte_block = (
        _FTE_TEMPLATE.format(task_memory=_cfg(cfg, "fault_tolerant_task_memory"))
        if _cfg(cfg, "fault_tolerant_execution")
        else ""
    )

    # The engine's own config, in the Trino tree the engine mounts (PROVISA_TRINO_ETC_DIR, shared
    # with the host by the launcher), so what is rendered here is what the engine starts on next.
    from provisa.core.trino_catalog_files import trino_etc_dir

    trino_root = trino_etc_dir()
    trino_etc = str(trino_root / "etc")
    tls_on, tls_subnet, coordinator_ip = internal_tls()
    internal_tls_block = _INTERNAL_TLS_TEMPLATE if tls_on else ""

    _write(
        os.path.join(trino_etc, "jvm.config"),
        _JVM_TEMPLATE.format(heap_gb=heap_gb),
    )
    from provisa.core import settings_registry  # REQ-1913: the operator setting `otel.endpoint`

    _otlp = settings_registry.value("otel.endpoint")
    otlp_endpoint = _otlp if _otlp is not None else ""  # unset: no tracing block is written
    tracing_block = (
        _TRACING_TEMPLATE.format(endpoint=otlp_endpoint) if otlp_endpoint.strip() else ""
    )
    coordinator_props = _COORDINATOR_TEMPLATE.format(
        query_max_memory=query_max_memory,
        query_max_memory_per_node=query_max_memory_per_node,
        query_max_total_memory=query_max_total_memory,
        fte_block=fte_block,
        tracing_block=tracing_block,
        internal_tls_block=internal_tls_block,
        discovery_uri=(
            f"https://{coordinator_ip}:{_INTERNAL_HTTPS_PORT}"
            if tls_on
            else "http://localhost:8080"
        ),
    )
    worker_props = _WORKER_TEMPLATE.format(
        query_max_memory_per_node=query_max_memory_per_node,
        fte_block=fte_block,
        internal_tls_block=internal_tls_block,
        discovery_uri=(
            f"https://{coordinator_ip}:{_INTERNAL_HTTPS_PORT}" if tls_on else "http://trino:8080"
        ),
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
    _write(
        str(trino_root / LAUNCH_SETTINGS),
        f"PROVISA_TRINO_INTERNAL_TLS={'true' if tls_on else 'false'}\n"
        f"PROVISA_TRINO_INTERNAL_SUBNET={tls_subnet}\n"
        f"PROVISA_TRINO_COORDINATOR_IP={coordinator_ip}\n"
        f"PROVISA_TRINO_DYNAMIC_RANGE={dynamic_range(tls_subnet)}\n",
    )


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
