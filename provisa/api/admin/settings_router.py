# Copyright (c) 2026 Kenneth Stott
# Canary: 0f8a2c4d-1b3e-4f6a-8c9d-2e5f7a0b1c3d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Admin config + platform settings REST endpoints."""

# Requirements: REQ-164, REQ-165, REQ-194, REQ-253, REQ-302, REQ-303, REQ-416

import os

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response

from provisa.api.admin._config_io import config_path, read_config, write_config

router = APIRouter()


@router.get("/admin/config")
async def download_config():  # REQ-164
    """Download the current config YAML."""
    path = config_path()
    if not path.exists():
        raise HTTPException(status_code=404, detail="Config file not found")
    return Response(
        content=path.read_text(),
        media_type="application/x-yaml",
        headers={"Content-Disposition": f"attachment; filename={path.name}"},
    )


@router.put("/admin/config")
async def upload_config(request: Request):  # REQ-164
    """Upload a revised config YAML and reload."""
    from provisa.api.app import _load_and_build  # lazy to avoid circular import

    body = await request.body()
    path = config_path()
    if not path.exists() or path.read_bytes() != body:
        if path.exists():
            backup = path.with_suffix(".yaml.bak")
            backup.write_text(path.read_text())
        path.write_bytes(body)
    try:
        await _load_and_build(str(path))
        return {"success": True, "message": "Config uploaded and reloaded"}
    except Exception as e:
        return {"success": False, "message": str(e)}


@router.get("/admin/settings")
async def get_settings():  # REQ-165, REQ-302, REQ-303, REQ-416
    """Return current platform settings."""
    from provisa.executor.redirect import RedirectConfig
    from provisa.compiler.sql_gen import _get_default_row_limit
    from provisa.api.app import state

    from provisa.core.models import ProvisaConfig

    rc = RedirectConfig.from_env()
    cfg = read_config()
    naming_cfg = cfg.get("naming", {})
    otel_cfg = cfg.get("observability", {})

    def _eng(key: str):
        # Default lives in one place — the ProvisaConfig field default.
        return cfg.get(key, ProvisaConfig.model_fields[key].default)

    return {
        "engine": {
            "jvm_heap_gb": int(_eng("jvm_heap_gb")),
            "query_max_memory": _eng("query_max_memory"),
            "query_max_memory_per_node": _eng("query_max_memory_per_node"),
            "query_max_total_memory": _eng("query_max_total_memory"),
            "fault_tolerant_execution": bool(_eng("fault_tolerant_execution")),
            "fault_tolerant_task_memory": _eng("fault_tolerant_task_memory"),
            "exchange_spool_dir": _eng("exchange_spool_dir"),
        },
        "redirect": {
            "enabled": rc.enabled,
            "threshold": rc.threshold,
            "default_format": rc.default_format,
            "ttl": rc.ttl,
        },
        "limits": {
            "default_row_limit": _get_default_row_limit(),
        },
        "cache": {
            "default_ttl": state.response_cache_default_ttl,
        },
        "naming": {
            "domain_prefix": naming_cfg.get("domain_prefix", False),
            "convention": naming_cfg.get("convention", "apollo_graphql"),
            "use_domains": naming_cfg.get("use_domains", None),
            "default_domain": naming_cfg.get("default_domain", "default"),
        },
        "relationships": {
            "auto_track_fk": os.environ.get("PROVISA_AUTO_TRACK_FK", "true").lower()
            not in ("0", "false", "no"),
        },
        "sampling": {
            "default_sample_size": int(os.environ.get("PROVISA_SAMPLE_SIZE", "10000")),
        },
        "otel": {
            "endpoint": os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
            or otel_cfg.get("endpoint", ""),
            "service_name": os.environ.get("OTEL_SERVICE_NAME")
            or otel_cfg.get("service_name", "provisa"),
            "sample_rate": float(otel_cfg.get("sample_rate", 1.0)),
            "support_endpoint": os.environ.get("PROVISA_SUPPORT_OTLP_ENDPOINT")
            or otel_cfg.get("support_endpoint", ""),
            "support_redact_sql_literals": bool(
                otel_cfg.get("support_telemetry_filter", {}).get("redact_sql_literals", True)
            ),
            "support_redact_attributes": list(
                otel_cfg.get("support_telemetry_filter", {}).get("redact_attributes", [])
            ),
        },
    }


def _apply_redirect(r: dict, updated: list) -> None:
    """Apply the `redirect` settings block (env-var backed)."""
    if "enabled" in r:
        os.environ["PROVISA_REDIRECT_ENABLED"] = str(r["enabled"]).lower()
        updated.append("redirect.enabled")
    if "threshold" in r:
        os.environ["PROVISA_REDIRECT_THRESHOLD"] = str(r["threshold"])
        updated.append("redirect.threshold")
    if "default_format" in r:
        os.environ["PROVISA_REDIRECT_FORMAT"] = r["default_format"]
        updated.append("redirect.default_format")
    if "ttl" in r:
        os.environ["PROVISA_REDIRECT_TTL"] = str(r["ttl"])
        updated.append("redirect.ttl")


def _apply_otel(o: dict, updated: list) -> None:
    """Apply the `otel` observability block (config file + env + live exporters)."""
    path = config_path()
    try:
        cfg = read_config()
        cfg.setdefault("observability", {})
        if "endpoint" in o:
            cfg["observability"]["endpoint"] = o["endpoint"]
            os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = o["endpoint"]
            updated.append("otel.endpoint")
        if "service_name" in o:
            cfg["observability"]["service_name"] = o["service_name"]
            os.environ["OTEL_SERVICE_NAME"] = o["service_name"]
            updated.append("otel.service_name")
        if "sample_rate" in o:
            cfg["observability"]["sample_rate"] = float(o["sample_rate"])
            updated.append("otel.sample_rate")
        if "support_endpoint" in o:
            cfg["observability"]["support_endpoint"] = o["support_endpoint"]
            os.environ["PROVISA_SUPPORT_OTLP_ENDPOINT"] = o["support_endpoint"]
            updated.append("otel.support_endpoint")
        if "support_redact_sql_literals" in o:
            cfg["observability"].setdefault("support_telemetry_filter", {})[
                "redact_sql_literals"
            ] = bool(o["support_redact_sql_literals"])
            updated.append("otel.support_redact_sql_literals")
        if "support_redact_attributes" in o:
            cfg["observability"].setdefault("support_telemetry_filter", {})["redact_attributes"] = (
                list(o["support_redact_attributes"])
            )
            updated.append("otel.support_redact_attributes")
        write_config(path, cfg)
        if "endpoint" in o and o["endpoint"]:
            from provisa.api.otel_setup import attach_otlp_exporters

            service = cfg["observability"].get("service_name", "provisa")
            attach_otlp_exporters(o["endpoint"], service)
    except Exception:
        pass


@router.put("/admin/settings")
async def update_settings(request: Request):  # REQ-165, REQ-194, REQ-253, REQ-302, REQ-303, REQ-416
    """Update platform settings at runtime."""
    from provisa.api.app import state, _load_and_build

    body = await request.json()
    updated = []
    restart_required = False

    if "engine" in body:
        # Execution-engine (federation) sizing + fault-tolerant execution. These are
        # written to the config file and regenerate the engine's config.properties, but
        # only take effect on an engine restart — the caller is told so.
        e = body["engine"]
        engine_keys = (
            "jvm_heap_gb",
            "query_max_memory",
            "query_max_memory_per_node",
            "query_max_total_memory",
            "fault_tolerant_execution",
            "fault_tolerant_task_memory",
            "exchange_spool_dir",
        )
        path = config_path()
        cfg = read_config()
        changed = False
        for k in engine_keys:
            if k in e:
                cfg[k] = int(e[k]) if k == "jvm_heap_gb" else e[k]
                updated.append(f"engine.{k}")
                changed = True
        if changed:
            write_config(path, cfg)
            from provisa.api.trino_setup import write_trino_config

            write_trino_config(str(path))
            restart_required = True

    if "redirect" in body:
        _apply_redirect(body["redirect"], updated)

    if "limits" in body:
        s = body["limits"]
        if "default_row_limit" in s:
            os.environ["PROVISA_DEFAULT_ROW_LIMIT"] = str(s["default_row_limit"])
            updated.append("limits.default_row_limit")

    if "cache" in body:
        c = body["cache"]
        if "default_ttl" in c:
            state.response_cache_default_ttl = int(c["default_ttl"])
            updated.append("cache.default_ttl")

    if "naming" in body:
        n = body["naming"]
        needs_reload = False
        path = config_path()
        cfg = read_config()
        if "domain_prefix" in n:
            cfg.setdefault("naming", {})["domain_prefix"] = bool(n["domain_prefix"])
            updated.append("naming.domain_prefix")
            needs_reload = True
        if "convention" in n:
            from provisa.compiler.naming import VALID_CONVENTIONS

            if n["convention"] not in VALID_CONVENTIONS:
                return {"success": False, "message": f"Invalid convention: {n['convention']!r}"}
            cfg.setdefault("naming", {})["convention"] = n["convention"]
            updated.append("naming.convention")
            needs_reload = True
        # NOTE: use_domains / default_domain are NOT editable here. Changing the domain
        # policy is destructive (it invalidates every registered table's domain) — it is
        # handled by the dedicated POST /admin/domain-policy endpoint which backs up and
        # resets the config.
        if needs_reload:
            write_config(path, cfg)
            try:
                await _load_and_build(str(path))
            except Exception:
                pass

    if "relationships" in body:
        r = body["relationships"]
        if "auto_track_fk" in r:
            os.environ["PROVISA_AUTO_TRACK_FK"] = "true" if r["auto_track_fk"] else "false"
            updated.append("relationships.auto_track_fk")

    if "otel" in body:
        _apply_otel(body["otel"], updated)

    return {"success": True, "updated": updated, "restart_required": restart_required}


@router.post("/admin/domain-policy")
async def set_domain_policy(request: Request):  # REQ-165
    """Change the domain policy (use_domains / default_domain).

    DESTRUCTIVE: the domain policy is a foundational decision — every registered table's
    domain_id is bound to it. Changing it backs up the current config, then resets the
    config to a clean default state (no sources, tables, domains, or relationships;
    auth and roles preserved) with the new policy applied, and reloads.
    """
    import datetime

    from provisa.api.app import _load_and_build

    body = await request.json()
    use_domains = body.get("use_domains", None)
    default_domain = body.get("default_domain", "default")
    if use_domains not in (None, True, False):
        raise HTTPException(status_code=400, detail="use_domains must be true, false, or null")
    if use_domains is False and not default_domain:
        raise HTTPException(
            status_code=400, detail="default_domain required when use_domains=false"
        )

    path = config_path()
    cfg = read_config()

    # 1. Timestamped backup of the existing config.
    backup_name = ""
    if path.exists():
        ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        backup = path.with_name(f"{path.stem}.{ts}.bak{path.suffix}")
        backup.write_text(path.read_text())
        backup_name = backup.name

    # 2. Reset to a clean default state — preserve only auth + roles.
    naming: dict = {}
    if use_domains is not None:
        naming["use_domains"] = use_domains
        if use_domains is False:
            naming["default_domain"] = default_domain
    new_cfg: dict = {
        "sources": [],
        "domains": [],
        "tables": [],
        "relationships": [],
        "roles": cfg.get("roles", []),
        "naming": naming,
    }
    if cfg.get("auth") is not None:
        new_cfg["auth"] = cfg["auth"]
    write_config(path, new_cfg)

    # 3. Reload in replace mode to purge the prior sources/tables from the metadata DB.
    _prev_replace = os.environ.get("PROVISA_CONFIG_REPLACE")
    os.environ["PROVISA_CONFIG_REPLACE"] = "true"
    try:
        await _load_and_build(str(path))
    finally:
        if _prev_replace is None:
            os.environ.pop("PROVISA_CONFIG_REPLACE", None)
        else:
            os.environ["PROVISA_CONFIG_REPLACE"] = _prev_replace

    return {"success": True, "backup": backup_name, "use_domains": use_domains}


@router.post("/admin/query-engine/reload-catalog")
async def reload_query_engine_catalog(catalog: str = "otel"):
    """Reload a catalog via the query engine coordinator REST API, then reconnect and re-run DDL.

    Uses DELETE + POST on the coordinator's /v1/catalog endpoint so all workers pick up the
    change automatically via the discovery service — no container restart required.
    """
    import httpx
    import trino as _trino
    from provisa.api.app import state, _seed_ops_trino

    if not state.trino_conn_kwargs:
        raise HTTPException(status_code=503, detail="Query engine connection not configured")

    host = state.trino_conn_kwargs.get("host", "localhost")
    port = state.trino_conn_kwargs.get("port", 8080)
    base_url = f"http://{host}:{port}"

    # Read catalog properties file from disk
    catalog_dir = os.path.join(
        os.environ.get("PROVISA_CATALOG_DIR", "trino/catalog"), f"{catalog}.properties"
    )
    if not os.path.isabs(catalog_dir):
        script_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
        catalog_path = os.path.join(script_dir, catalog_dir)
    else:
        catalog_path = catalog_dir

    if not os.path.exists(catalog_path):
        raise HTTPException(status_code=404, detail=f"Catalog properties not found: {catalog_path}")

    props: dict[str, str] = {}
    with open(catalog_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                props[k.strip()] = v.strip()

    connector_name = props.pop("connector.name", None)
    if not connector_name:
        raise HTTPException(status_code=500, detail=f"connector.name missing in {catalog_path}")

    errors: list[str] = []
    async with httpx.AsyncClient(timeout=30) as client:
        # Drop the catalog (ignore 404 — may not be registered yet)
        del_resp = await client.delete(f"{base_url}/v1/catalog/{catalog}")
        if del_resp.status_code not in (200, 204, 404):
            errors.append(f"DELETE /v1/catalog/{catalog} → {del_resp.status_code}: {del_resp.text}")

        # Re-register the catalog
        post_resp = await client.post(
            f"{base_url}/v1/catalog",
            json={"catalogName": catalog, "connectorName": connector_name, "properties": props},
        )
        if post_resp.status_code not in (200, 201):
            errors.append(f"POST /v1/catalog → {post_resp.status_code}: {post_resp.text}")

    if errors:
        return {"success": False, "errors": errors}

    # Reconnect Provisa's internal connection and re-run OTel DDL
    try:
        new_conn = _trino.dbapi.connect(**state.trino_conn_kwargs)
        cur = new_conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        state.trino_conn = new_conn
    except Exception as exc:
        return {"success": False, "errors": [f"Reconnect failed: {exc}"]}

    try:
        _seed_ops_trino(state.trino_conn, getattr(state, "otel_snapshot_retention_hours", None))
    except Exception as exc:
        errors.append(str(exc))

    return {"success": not errors, "errors": errors}


@router.post("/admin/query-engine/restart")
async def restart_query_engine(container: str | None = None):
    """Restart the query engine container (single-node dev only). Falls back to QUERY_ENGINE_CONTAINER env var."""
    import asyncio

    container = container or os.environ.get("QUERY_ENGINE_CONTAINER", "trino")
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker",
            "restart",
            container,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="docker restart timed out")
    except FileNotFoundError:
        raise HTTPException(status_code=503, detail="docker not found on PATH")

    if proc.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail=stderr.decode().strip() or f"docker restart exited {proc.returncode}",
        )

    return {"success": True, "container": container, "output": stdout.decode().strip()}


@router.post("/admin/schema-clusters/recompute")
async def recompute_schema_clusters():  # REQ-510
    """Rerun Louvain clustering on the schema graph and refresh schema_clusters."""
    from provisa.api.app import state, _compute_and_store_clusters

    if not state.pg_pool:
        raise HTTPException(status_code=503, detail="Database not available")
    async with state.pg_pool.acquire() as conn:
        count = await _compute_and_store_clusters(conn)  # type: ignore[arg-type]
    return {"success": True, "tables_clustered": count}


@router.get("/admin/traces/recent")
async def get_recent_traces(limit: int = 50):  # REQ-302, REQ-303
    """Return the last N completed spans from the in-memory buffer."""
    try:
        from provisa.api.otel_setup import span_buffer

        return {"traces": span_buffer.recent(min(limit, 200))}
    except Exception:
        return {"traces": []}
