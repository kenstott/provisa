# Copyright (c) 2026 Kenneth Stott
# Canary: 4f1c8a92-7d3e-4b6a-9c05-2e8f1a6b3d47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Trino engine's lifecycle/admin/error implementation (REQ-825, REQ-840).

This is the ONE module (alongside catalog.py / ops_trino.py / trino_setup.py / executor.trino*)
that may import ``trino``. Generic code never provisions, watchdogs, reloads, or classifies
Trino errors directly — it reaches these through ``EngineRuntime`` seam terminals, which delegate
here only when the bound engine is Trino. A native engine (duckdb/pg/…) skips all of this.
"""

# complexity-gate: allow-ble=7 reason="Trino lifecycle ops (watchdog ping/restart/reconnect, infra bucket+schema setup, catalog reload) are best-effort: failures are logged and the phase degrades, never crashing boot/serve"

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

import trino

log = logging.getLogger(__name__)


def connect(conn_kwargs: dict) -> trino.dbapi.Connection:
    """Open a fresh Trino dbapi connection from the stored kwargs."""
    return trino.dbapi.connect(**conn_kwargs)


def write_config(config_path: str) -> None:
    """Regenerate the Trino cluster config (jvm.config / config.properties) from platform config."""
    from provisa.api.trino_setup import write_trino_config

    write_trino_config(config_path)


def configure_session(state: Any, server_cfg: dict) -> None:
    """Compute Trino Fault-Tolerant-Execution session hints (env > config > disabled) and store them
    on ``state.engine_session_hints`` — the executor injects them into every ENGINE query."""
    fte_cfg = server_cfg.get("federation_fte", {})
    enabled = os.environ.get(
        "TRINO_FTE_ENABLED", str(fte_cfg.get("enabled", False))
    ).lower() not in ("0", "false", "no")
    if not enabled:
        return
    retry_policy = (
        os.environ.get("TRINO_FTE_RETRY_POLICY") or str(fte_cfg.get("retry_policy", "TASK"))
    ).upper()
    hints = {"retry_policy": retry_policy}
    for k, v in fte_cfg.items():
        if k in ("enabled", "retry_policy"):
            continue
        hints.setdefault(k, str(v))
    state.engine_session_hints = hints


def register_kafka_catalog(state: Any, kafka_source: dict) -> None:
    """Write the Trino Kafka catalog files and register the catalog dynamically."""
    from provisa.core.trino_catalog_files import write_kafka_catalog_files

    write_kafka_catalog_files(kafka_source, trino_conn=state.engine_conn)


def polling_provider(state: Any, catalog: str, schema: str, table: str, watermark_column: str):
    """A Trino change-data polling provider bound to this engine's coordinator."""
    from provisa.subscriptions.trino_polling_provider import TrinoPollingProvider

    eck = state.engine_conn_kwargs or {}
    return TrinoPollingProvider(
        host=eck.get("host", "localhost"),
        port=int(eck.get("port", 8080)),
        catalog=catalog,
        schema=schema,
        table=table,
        watermark_column=watermark_column,
    )


# -- boot provisioning (was app.py `_apply_server_and_trino_config` Trino branch) --------------


def provision(state: Any, ops_views: list, retention_hours: int | None) -> None:
    """Connect the Trino terminal and seed the OTel ops catalog. Boot-time; blocking."""
    from provisa.federation.engine import configured_engine_endpoint

    trino_host, trino_port = configured_engine_endpoint()
    state.engine_conn_kwargs = dict(
        host=trino_host,
        port=trino_port,
        user="provisa",
        catalog="system",
        schema=f"org_{state.org_id}",
        http_scheme="http",
        request_timeout=10,
    )
    state.engine_conn = trino.dbapi.connect(**state.engine_conn_kwargs)

    from provisa.compiler import schema_service
    from provisa.observability.ops_trino import seed_ops_trino

    schema_service.init(state.federation_engine)
    seed_ops_trino(state.engine_conn, ops_views, retention_hours)


async def connect_infra(state: Any) -> None:  # REQ-143, REQ-171
    """Concurrently connect Arrow Flight (Zaychik) and set up MinIO/results-schema."""

    async def _connect_flight() -> None:
        from provisa.executor.trino_flight import create_flight_connection

        zaychik_host = os.environ.get("ZAYCHIK_HOST", "localhost")
        zaychik_port = int(os.environ.get("ZAYCHIK_PORT", "8480"))
        state.flight_client = await asyncio.to_thread(
            create_flight_connection, host=zaychik_host, port=zaychik_port
        )

    async def _setup_object_store() -> None:
        # MinIO results bucket (REQ-171) — already async.
        from provisa.executor.redirect import RedirectConfig, ensure_results_bucket

        await ensure_results_bucket(RedirectConfig.from_env())

        # MinIO OTEL bucket for otlp2parquet (blocking boto3 → thread).
        def _ensure_otel_bucket() -> None:
            import boto3
            from botocore.config import Config as BotoConfig

            _otel_endpoint = os.environ.get("PROVISA_OTEL_S3_ENDPOINT", "http://minio:9000")
            _otel_bucket = os.environ.get("PROVISA_OTEL_BUCKET", "provisa-otel")
            _s3 = boto3.client(
                "s3",
                endpoint_url=_otel_endpoint,
                aws_access_key_id=os.environ.get("PROVISA_OTEL_S3_ACCESS_KEY", "minioadmin"),
                aws_secret_access_key=os.environ.get("PROVISA_OTEL_S3_SECRET_KEY", "minioadmin"),
                region_name="us-east-1",
                config=BotoConfig(signature_version="s3v4"),
            )
            existing = [b["Name"] for b in _s3.list_buckets().get("Buckets", [])]
            if _otel_bucket not in existing:
                _s3.create_bucket(Bucket=_otel_bucket)
                log.info("Created MinIO bucket: %s", _otel_bucket)

        try:
            await asyncio.to_thread(_ensure_otel_bucket)
        except Exception:
            log.warning(
                "Could not ensure OTEL bucket — otlp2parquet storage may fail", exc_info=True
            )

        # Results schema for CTAS redirects (blocking Trino → thread).
        try:
            from provisa.executor.trino_write import ensure_results_schema

            assert state.engine_conn is not None
            await asyncio.to_thread(ensure_results_schema, state.engine_conn)
        except Exception:
            log.warning(
                "Could not create results schema — CTAS redirect unavailable", exc_info=True
            )

    await asyncio.gather(_connect_flight(), _setup_object_store())


# -- watchdog (was scheduler/jobs.py `watch_trino`) -------------------------------------------


def _ping(conn: trino.dbapi.Connection) -> None:
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.fetchone()


async def watchdog(state: Any) -> None:
    """Restart the Trino container if it stops responding, then replace the dead connection."""
    if state.engine_conn is None:
        return

    try:
        await asyncio.to_thread(_ping, state.engine_conn)
        return
    except Exception:
        pass

    log.warning("watchdog: Trino unresponsive — attempting restart")
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker",
            "start",
            "provisa-trino-1",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            log.error("watchdog: docker start failed: %s", stderr.decode().strip())
            return
        log.info("watchdog: provisa-trino-1 started, waiting for healthy state")
    except Exception:
        log.exception("watchdog: docker start provisa-trino-1 failed")
        return

    # Wait up to 120 s for Trino to accept connections, then replace the dead conn.
    deadline = asyncio.get_event_loop().time() + 120
    while asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(5)
        try:
            new_conn = await asyncio.to_thread(
                lambda: trino.dbapi.connect(**state.engine_conn_kwargs)
            )
            await asyncio.to_thread(_ping, new_conn)
            old_conn = state.engine_conn
            state.engine_conn = new_conn
            try:
                old_conn.close()
            except Exception:
                pass
            log.info("watchdog: Trino reconnected successfully")
            return
        except Exception:
            pass

    log.error("watchdog: Trino did not become healthy within 120 s")


# -- catalog reload (was settings_router `reload_query_engine_catalog`) ------------------------


async def reload_catalog(
    state: Any, catalog: str, ops_views: list, retention_hours: int | None
) -> dict:
    """Reload a Trino catalog via the coordinator REST API, then reconnect and re-run OTel DDL."""
    import httpx

    if not state.engine_conn_kwargs:
        return {"success": False, "errors": ["Query engine connection not configured"]}

    host = state.engine_conn_kwargs.get("host", "localhost")
    port = state.engine_conn_kwargs.get("port", 8080)
    base_url = f"http://{host}:{port}"

    catalog_dir = os.path.join(
        os.environ.get("PROVISA_CATALOG_DIR", "trino/catalog"), f"{catalog}.properties"
    )
    if not os.path.isabs(catalog_dir):
        script_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        catalog_path = os.path.join(script_dir, catalog_dir)
    else:
        catalog_path = catalog_dir

    if not os.path.exists(catalog_path):
        return {"success": False, "errors": [f"Catalog properties not found: {catalog_path}"]}

    props: dict[str, str] = {}
    with open(catalog_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                props[k.strip()] = v.strip()

    connector_name = props.pop("connector.name", None)
    if not connector_name:
        return {"success": False, "errors": [f"connector.name missing in {catalog_path}"]}

    errors: list[str] = []
    async with httpx.AsyncClient(timeout=30) as client:
        del_resp = await client.delete(f"{base_url}/v1/catalog/{catalog}")
        if del_resp.status_code not in (200, 204, 404):
            errors.append(f"DELETE /v1/catalog/{catalog} → {del_resp.status_code}: {del_resp.text}")

        post_resp = await client.post(
            f"{base_url}/v1/catalog",
            json={"catalogName": catalog, "connectorName": connector_name, "properties": props},
        )
        if post_resp.status_code not in (200, 201):
            errors.append(f"POST /v1/catalog → {post_resp.status_code}: {post_resp.text}")

    if errors:
        return {"success": False, "errors": errors}

    try:
        new_conn = trino.dbapi.connect(**state.engine_conn_kwargs)
        cur = new_conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        state.engine_conn = new_conn
    except Exception as exc:
        return {"success": False, "errors": [f"Reconnect failed: {exc}"]}

    try:
        from provisa.observability.ops_trino import seed_ops_trino

        seed_ops_trino(state.engine_conn, ops_views, retention_hours)
    except Exception as exc:
        errors.append(str(exc))

    return {"success": not errors, "errors": errors}


# -- error classification (was cypher/neo4j `trino.exceptions` isinstance checks) --------------


def classify_error(exc: Exception) -> str | None:
    """Map a Trino driver exception to an engine-agnostic category for HTTP status selection.

    Returns ``"connection"`` (→ 503), ``"query"`` (→ 400), or ``None`` (not an engine error the
    Trino backend recognizes — the caller picks a default)."""
    if isinstance(exc, trino.exceptions.TrinoConnectionError):
        return "connection"
    if isinstance(exc, trino.exceptions.TrinoQueryError):
        return "query"
    return None
