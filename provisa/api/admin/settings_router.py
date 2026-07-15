# Copyright (c) 2026 Kenneth Stott
# Canary: 0f8a2c4d-1b3e-4f6a-8c9d-2e5f7a0b1c3d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Admin config + platform settings REST endpoints."""

# complexity-gate: allow-ble=4 reason="best-effort admin/settings handlers: config upload+reload
# reports the failure to the caller (never 500s the admin API); OTLP-exporter attach, config reload
# on domain-policy apply, and the recent-traces read each degrade (log/pass/empty) rather than crash a
# settings request"

# Requirements: REQ-164, REQ-165, REQ-194, REQ-253, REQ-302, REQ-303, REQ-416

import os

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response

from provisa.api.admin._config_io import config_path, read_config, write_config

router = APIRouter()


@router.get("/admin/config")
async def download_config():  # REQ-164
    """Download the ORIGINAL config YAML (the on-disk boot seed). The live-state view is
    ``/admin/config/live``; the UI diffs the two."""
    path = config_path()
    if not path.exists():
        raise HTTPException(status_code=404, detail="Config file not found")
    return Response(
        content=path.read_text(),
        media_type="application/x-yaml",
        headers={"Content-Disposition": f"attachment; filename={path.name}"},
    )


def _require_live_export() -> None:
    """Live config export/diff/patch is opt-in (config_live_export) — coherent only where the
    generated/normalized config is canonical (the demo), not a hand-authored file a normalized patch
    could not stay faithful to. 404 when off."""
    from provisa.api.app import state

    if not getattr(state, "config_live_export", False):
        raise HTTPException(
            status_code=404,
            detail="Live config export is disabled (set live_config_export: true).",
        )


@router.get("/admin/config/live")
async def download_live_config():  # REQ-164
    """The CURRENT config generated from live state (admin-created views/MVs, relationships, roles,
    rls, domains overlaid on the file base)."""
    _require_live_export()
    from provisa.api.admin.config_export import build_live_config_yaml

    return Response(
        content=await build_live_config_yaml(),
        media_type="application/x-yaml",
        headers={"Content-Disposition": "attachment; filename=provisa.live.yaml"},
    )


@router.get("/admin/config/diff")
async def config_diff():  # REQ-164
    """Both sides of the config diff — ``original`` (startup baseline) and ``current`` (live state) —
    NORMALIZED identically so the side-by-side view shows only genuine changes, not reordering."""
    _require_live_export()
    from provisa.api.admin.config_export import config_diff as _diff

    return await _diff()


@router.post("/admin/config/patch")
async def config_patch(request: Request):  # REQ-164
    """A unified-diff patch from the baseline to the posted (curated) config — git-apply / ``patch``
    compatible, for committing config changes made in the UI through CI/CD."""
    _require_live_export()
    from provisa.api.admin.config_export import make_config_patch

    revised = (await request.body()).decode("utf-8")
    patch = make_config_patch(revised)
    return Response(
        content=patch,
        media_type="text/x-patch",
        headers={"Content-Disposition": "attachment; filename=provisa.config.patch"},
    )


@router.put("/admin/config")
async def upload_config(request: Request):  # REQ-164
    """Upload a revised config YAML and reload.

    When live config export is on (the generated-config-is-canonical contract), the config is
    NORMALIZED on consume and the normalized form is persisted — so the on-disk file stays byte-faithful
    to the diff/patch baseline and a downloaded patch applies cleanly via ``git apply``. With the flag
    off the file is written verbatim (a hand-authored config keeps its comments/ordering)."""
    from provisa.api.app import _load_and_build, state  # lazy to avoid circular import

    body = await request.body()
    if getattr(state, "config_live_export", False):
        import yaml

        from provisa.api.admin.config_export import normalize_config

        parsed = yaml.safe_load(body.decode("utf-8")) or {}
        body = yaml.dump(
            normalize_config(parsed), default_flow_style=False, sort_keys=False
        ).encode("utf-8")

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

    from provisa.core.models import GraphQLRemoteConfig, OtelConfig, ProvisaConfig

    rc = RedirectConfig.from_env()
    cfg = read_config()
    naming_cfg = cfg.get("naming", {})
    otel_cfg = cfg.get("observability", {})
    gqr_cfg = cfg.get("graphql_remote", {}) or {}

    def _eng(key: str):
        # Default lives in one place — the ProvisaConfig field default.
        return cfg.get(key, ProvisaConfig.model_fields[key].default)

    return {
        # Opt-in features the UI gates on (REQ-164): live config export/diff/patch.
        "features": {"live_config_export": bool(getattr(state, "config_live_export", False))},
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
        "cdc": {  # REQ-931: Provisa-level inbound-CDC consumer group (receiver identity)
            "consumer_group_id": _eng("cdc_consumer_group_id"),
        },
        # Materialization-store DSN — the admin UI reads its scheme to decide native-CDC
        # availability for materialized views. Canonical write path is /admin/cache-storage.
        "materialize": {"store_url": cfg.get("materialize_store_url") or ""},
        "sampling": {
            "default_sample_size": int(os.environ.get("PROVISA_SAMPLE_SIZE", "10000")),
        },
        "otel": {
            "endpoint": os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
            or otel_cfg.get("endpoint", ""),
            "service_name": os.environ.get("OTEL_SERVICE_NAME")
            or otel_cfg.get("service_name", "provisa"),
            "sample_rate": float(otel_cfg.get("sample_rate", 1.0)),
            # REQ-545: tracing pipeline tuning. Defaults mirror OtelConfig field defaults.
            "log_level": os.environ.get("OTEL_LOG_LEVEL")
            or otel_cfg.get("log_level", OtelConfig.model_fields["log_level"].default),
            "compact_cron": otel_cfg.get(
                "compact_cron", OtelConfig.model_fields["compact_cron"].default
            ),
            "compact_batch_size": int(
                otel_cfg.get(
                    "compact_batch_size", OtelConfig.model_fields["compact_batch_size"].default
                )
            ),
            "compact_file_chunk": int(
                otel_cfg.get(
                    "compact_file_chunk", OtelConfig.model_fields["compact_file_chunk"].default
                )
            ),
            "ops_snapshot_retention_hours": otel_cfg.get("ops_snapshot_retention_hours"),
            "span_export_delay_millis": int(
                otel_cfg.get(
                    "span_export_delay_millis",
                    OtelConfig.model_fields["span_export_delay_millis"].default,
                )
            ),
            "otlp2parquet_max_age_secs": int(
                otel_cfg.get(
                    "otlp2parquet_max_age_secs",
                    OtelConfig.model_fields["otlp2parquet_max_age_secs"].default,
                )
            ),
            "collector_batch_timeout_ms": int(
                otel_cfg.get(
                    "collector_batch_timeout_ms",
                    OtelConfig.model_fields["collector_batch_timeout_ms"].default,
                )
            ),
            "s3_endpoint": otel_cfg.get(
                "s3_endpoint", OtelConfig.model_fields["s3_endpoint"].default
            ),
            "support_endpoint": os.environ.get("PROVISA_SUPPORT_OTLP_ENDPOINT")
            or otel_cfg.get("support_endpoint", ""),
            "support_redact_sql_literals": bool(
                otel_cfg.get("support_telemetry_filter", {}).get("redact_sql_literals", True)
            ),
            "support_redact_attributes": list(
                otel_cfg.get("support_telemetry_filter", {}).get("redact_attributes", [])
            ),
        },
        "graphql_remote": {  # remote-GraphQL source traversal limits
            "max_object_depth": gqr_cfg.get(
                "max_object_depth", GraphQLRemoteConfig.model_fields["max_object_depth"].default
            ),
            "max_list_depth": gqr_cfg.get(
                "max_list_depth", GraphQLRemoteConfig.model_fields["max_list_depth"].default
            ),
            "max_list_items": gqr_cfg.get(
                "max_list_items", GraphQLRemoteConfig.model_fields["max_list_items"].default
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
        # REQ-545: tracing pipeline tuning (applied on restart).
        if "log_level" in o:
            cfg["observability"]["log_level"] = o["log_level"]
            os.environ["OTEL_LOG_LEVEL"] = str(o["log_level"])
            updated.append("otel.log_level")
        for _k in ("compact_cron", "s3_endpoint"):
            if _k in o:
                cfg["observability"][_k] = o[_k]
                updated.append(f"otel.{_k}")
        for _k in (
            "compact_batch_size",
            "compact_file_chunk",
            "span_export_delay_millis",
            "otlp2parquet_max_age_secs",
            "collector_batch_timeout_ms",
        ):
            if _k in o:
                cfg["observability"][_k] = int(o[_k])
                updated.append(f"otel.{_k}")
        if "ops_snapshot_retention_hours" in o:
            v = o["ops_snapshot_retention_hours"]
            cfg["observability"]["ops_snapshot_retention_hours"] = (
                int(v) if v not in (None, "") else None
            )
            updated.append("otel.ops_snapshot_retention_hours")
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


_ENGINE_KEYS = (
    "jvm_heap_gb",
    "query_max_memory",
    "query_max_memory_per_node",
    "query_max_total_memory",
    "fault_tolerant_execution",
    "fault_tolerant_task_memory",
    "exchange_spool_dir",
)


def _apply_engine(e: dict, state, updated: list) -> bool:
    """Apply execution-engine (federation) sizing keys. Returns True if a restart is needed.

    Written to config + regenerated into the engine's config.properties, but only take effect
    on an engine restart.
    """
    path = config_path()
    cfg = read_config()
    changed = False
    for k in _ENGINE_KEYS:
        if k in e:
            cfg[k] = int(e[k]) if k == "jvm_heap_gb" else e[k]
            updated.append(f"engine.{k}")
            changed = True
    if changed:
        write_config(path, cfg)
        state.federation_engine.write_config(str(path))
    return changed


def _apply_graphql_remote(g: dict, updated: list) -> None:
    """Apply the remote-GraphQL traversal limits (applied on reload)."""
    path = config_path()
    cfg = read_config()
    gqr = dict(cfg.get("graphql_remote", {}) or {})
    for k in ("max_object_depth", "max_list_depth", "max_list_items"):
        if k in g:
            gqr[k] = int(g[k])
            updated.append(f"graphql_remote.{k}")
    cfg["graphql_remote"] = gqr
    write_config(path, cfg)


async def _apply_naming(n: dict, updated: list) -> str | None:
    """Apply naming (domain_prefix / convention). Returns an error message on invalid input.

    use_domains / default_domain are NOT editable here — changing the domain policy is
    destructive and handled by POST /admin/domain-policy.
    """
    from provisa.api.app import _load_and_build

    path = config_path()
    cfg = read_config()
    needs_reload = False
    if "domain_prefix" in n:
        cfg.setdefault("naming", {})["domain_prefix"] = bool(n["domain_prefix"])
        updated.append("naming.domain_prefix")
        needs_reload = True
    if "convention" in n:
        from provisa.compiler.naming import VALID_CONVENTIONS

        if n["convention"] not in VALID_CONVENTIONS:
            return f"Invalid convention: {n['convention']!r}"
        cfg.setdefault("naming", {})["convention"] = n["convention"]
        updated.append("naming.convention")
        needs_reload = True
    if needs_reload:
        write_config(path, cfg)
        try:
            await _load_and_build(str(path))
        except Exception:
            pass
    return None


def _apply_scalars(body: dict, state, updated: list) -> None:
    """Apply the simple env/state-backed scalar setting blocks (limits/cache/sampling/relationships)."""
    if "limits" in body and "default_row_limit" in body["limits"]:
        os.environ["PROVISA_DEFAULT_ROW_LIMIT"] = str(body["limits"]["default_row_limit"])
        updated.append("limits.default_row_limit")
    if "cache" in body and "default_ttl" in body["cache"]:
        state.response_cache_default_ttl = int(body["cache"]["default_ttl"])
        updated.append("cache.default_ttl")
    if "sampling" in body and "default_sample_size" in body["sampling"]:
        os.environ["PROVISA_SAMPLE_SIZE"] = str(int(body["sampling"]["default_sample_size"]))
        updated.append("sampling.default_sample_size")
    if "relationships" in body and "auto_track_fk" in body["relationships"]:
        os.environ["PROVISA_AUTO_TRACK_FK"] = (
            "true" if body["relationships"]["auto_track_fk"] else "false"
        )
        updated.append("relationships.auto_track_fk")


@router.put("/admin/settings")
async def update_settings(request: Request):  # REQ-165, REQ-194, REQ-253, REQ-302, REQ-303, REQ-416
    """Update platform settings at runtime."""
    from provisa.api.app import state

    body = await request.json()
    updated: list = []
    restart_required = False

    if "engine" in body:
        restart_required = _apply_engine(body["engine"], state, updated) or restart_required
    if "redirect" in body:
        _apply_redirect(body["redirect"], updated)
    _apply_scalars(body, state, updated)
    if "graphql_remote" in body:
        _apply_graphql_remote(body["graphql_remote"], updated)
    if "naming" in body:
        err = await _apply_naming(body["naming"], updated)
        if err is not None:
            return {"success": False, "message": err}
    if "otel" in body:
        _apply_otel(body["otel"], updated)

    if "cdc" in body:  # REQ-931: Provisa-level inbound-CDC consumer group; applied on restart
        c = body["cdc"]
        if "consumer_group_id" in c:
            path = config_path()
            cfg = read_config()
            val = (c["consumer_group_id"] or "").strip()
            if val:
                cfg["cdc_consumer_group_id"] = val
            else:
                cfg.pop("cdc_consumer_group_id", None)  # blank → inherit the model default
            write_config(path, cfg)
            updated.append("cdc.consumer_group_id")
            restart_required = True

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


@router.get("/admin/federation-engine")
async def get_federation_engine():  # REQ-916
    """Current federation-engine selection + connection config, and the selectable-engine registry."""
    from provisa.federation.engine import engine_registry
    from provisa.core.models import ProvisaConfig

    cfg = read_config()

    def _eng(key: str):
        return cfg.get(key, ProvisaConfig.model_fields[key].default)

    # Return current values for every config key any engine declares (each is a ProvisaConfig
    # field), so the tab can render per-engine fields — connection AND execution tuning — generically.
    all_keys = {f["config_key"] for e in engine_registry() for f in e["config_fields"]}
    return {
        "current": _eng("federation_engine"),
        "config": {k: _eng(k) for k in sorted(all_keys)},
        "engines": engine_registry(),
        "restart_required_note": "Changing the federation engine takes effect after the service is restarted.",
    }


@router.put("/admin/federation-engine")
async def set_federation_engine(request: Request):  # REQ-916
    """Persist the federation-engine selection + connection config. Applied on service restart."""
    from provisa.federation.engine import engine_registry

    from provisa.api.app import state

    body = await request.json()
    engine = body.get("engine")
    registry = engine_registry()
    valid = {e["key"] for e in registry}
    if engine not in valid:
        raise HTTPException(
            status_code=400, detail=f"unknown engine {engine!r}; valid: {sorted(valid)}"
        )

    path = config_path()
    cfg = read_config()
    cfg["federation_engine"] = engine
    updated = ["federation_engine"]

    def _coerce(field: dict, val):
        if val in (None, ""):
            return None
        t = field["type"]
        return int(val) if t == "number" else bool(val) if t == "boolean" else val

    # Persist only the keys this engine declares (connection + execution tuning), coerced by the
    # field's declared type. A blank value resets the key to its ProvisaConfig default.
    selected_fields = {
        f["config_key"]: f for e in registry if e["key"] == engine for f in e["config_fields"]
    }
    for ck, field in selected_fields.items():
        if ck in body:
            coerced = _coerce(field, body[ck])
            if coerced is None:
                cfg.pop(ck, None)
            else:
                cfg[ck] = coerced
            updated.append(ck)

    # Reset keys OTHER engines declare but this one doesn't, so switching engines never leaves a
    # stale coordinator host / URL that the newly selected engine (which reads config by key) picks up.
    other_keys = {f["config_key"] for e in registry for f in e["config_fields"]} - set(
        selected_fields
    )
    for ck in other_keys:
        if cfg.pop(ck, None) is not None:
            updated.append(f"-{ck}")

    write_config(path, cfg)
    # Regenerate the engine's derived config (e.g. Trino jvm.config/config.properties) so sizing
    # changes are written out; a native engine's write_config is a no-op. Applies on restart.
    state.federation_engine.write_config(str(path))
    return {"success": True, "updated": updated, "restart_required": True}


@router.get("/admin/cache-storage")
async def get_cache_storage():  # REQ-917
    """Hot-cache (Redis) + materialize-store settings for the admin UI."""
    from provisa.api.app import state

    from provisa.core.models import HotTablesConfig, MaterializedViewsConfig, WarmTablesConfig

    cfg = read_config()
    cache = cfg.get("cache", {}) or {}
    hot = cfg.get("hot_tables", {}) or {}
    warm = cfg.get("warm_tables", {}) or {}
    mv = cfg.get("materialized_views", {}) or {}
    # The DSN the active engine offers itself as its materialize target absent explicit config
    # (engine.py:_*_materialize_default). Reported so the UI shows the real "empty →" fallback
    # for THIS engine rather than a hardcoded string — None when the engine declares no default.
    default_store = state.federation_engine.engine.default_materialize_store()
    hf = HotTablesConfig.model_fields
    wf = WarmTablesConfig.model_fields
    # Defaults mirror the single source of truth — the model field defaults / reads in hot_tables.py.
    return {
        "cache": {
            "enabled": bool(cache.get("enabled", False)),
            "redis_url": cache.get("redis_url", ""),  # empty → embedded fakeredis
            "default_ttl": cache.get("default_ttl"),
        },
        "hot_tables": {
            "auto_threshold": hot.get("auto_threshold", hf["auto_threshold"].default),
            "max_rows": hot.get(
                "max_rows", hot.get("auto_threshold", hf["auto_threshold"].default)
            ),
            "max_bytes": hot.get("max_bytes", hf["max_bytes"].default),
            "refresh_interval": hot.get("refresh_interval"),
        },
        "warm_tables": {  # REQ-240: tier-promotion thresholds + engine filesystem read-cache
            "query_threshold": warm.get("query_threshold", wf["query_threshold"].default),
            "max_rows": warm.get("max_rows", wf["max_rows"].default),
            "refresh_interval": warm.get("refresh_interval", wf["refresh_interval"].default),
            "fs_cache_enabled": bool(warm.get("fs_cache_enabled", wf["fs_cache_enabled"].default)),
            "fs_cache_directories": warm.get(
                "fs_cache_directories", wf["fs_cache_directories"].default
            ),
            "fs_cache_max_sizes": warm.get("fs_cache_max_sizes", wf["fs_cache_max_sizes"].default),
        },
        "materialized_views": {  # REQ-543: default MV refresh TTL for MVs without their own
            "default_ttl": mv.get(
                "default_ttl", MaterializedViewsConfig.model_fields["default_ttl"].default
            ),
        },
        "materialize": {
            "store_url": cfg.get("materialize_store_url") or "",
            "default_store_url": default_store or "",
        },
        "restart_required_note": "Redis and materialize-store connections bind at startup — changes take effect after a service restart.",
    }


@router.put("/admin/cache-storage")
async def set_cache_storage(request: Request):  # REQ-917
    """Persist hot-cache (Redis) + materialize-store settings. Applied on service restart."""
    body = await request.json()
    path = config_path()
    cfg = read_config()
    updated: list[str] = []

    if "cache" in body:
        cache = dict(cfg.get("cache", {}) or {})
        for k in ("enabled", "redis_url", "default_ttl"):
            if k in body["cache"]:
                cache[k] = body["cache"][k]
                updated.append(f"cache.{k}")
        cfg["cache"] = cache
    if "hot_tables" in body:
        hot = dict(cfg.get("hot_tables", {}) or {})
        for k in ("auto_threshold", "max_rows", "max_bytes", "refresh_interval"):
            if k in body["hot_tables"]:
                v = body["hot_tables"][k]
                hot[k] = int(v) if v not in (None, "") else None
                updated.append(f"hot_tables.{k}")
        cfg["hot_tables"] = hot
    if "warm_tables" in body:  # REQ-240
        warm = dict(cfg.get("warm_tables", {}) or {})
        for k in ("query_threshold", "max_rows", "refresh_interval"):
            if k in body["warm_tables"]:
                v = body["warm_tables"][k]
                warm[k] = int(v) if v not in (None, "") else None
                updated.append(f"warm_tables.{k}")
        if "fs_cache_enabled" in body["warm_tables"]:
            warm["fs_cache_enabled"] = bool(body["warm_tables"]["fs_cache_enabled"])
            updated.append("warm_tables.fs_cache_enabled")
        for k in ("fs_cache_directories", "fs_cache_max_sizes"):
            if k in body["warm_tables"]:
                warm[k] = body["warm_tables"][k]
                updated.append(f"warm_tables.{k}")
        cfg["warm_tables"] = warm
    if "materialized_views" in body and "default_ttl" in body["materialized_views"]:  # REQ-543
        v = body["materialized_views"]["default_ttl"]
        cfg["materialized_views"] = dict(cfg.get("materialized_views", {}) or {})
        cfg["materialized_views"]["default_ttl"] = int(v) if v not in (None, "") else None
        updated.append("materialized_views.default_ttl")
    if "materialize" in body and "store_url" in body["materialize"]:
        cfg["materialize_store_url"] = body["materialize"]["store_url"] or None
        updated.append("materialize_store_url")

    write_config(path, cfg)
    return {"success": True, "updated": updated, "restart_required": True}


def _encryption_providers() -> list[dict]:
    """UI view of the encryption-provider registry (REQ-918, REQ-690-694).

    Derived live from the extensible registry, so built-in AND enterprise-registered
    custom providers (custom KMS/HSM endpoints) surface automatically with their
    declared config_fields. ``available`` reflects whether the provider's runtime is
    installed — the UI shows-but-blocks unavailable ones, matching the factory's
    fail-closed selection.
    """
    from provisa.encryption.registry import encryption_provider_registry

    return [
        {
            "key": s.key,
            "label": s.label,
            "description": s.description,
            "available": s.available(),
            "config_fields": s.config_fields,
        }
        for s in encryption_provider_registry()
    ]


@router.get("/admin/encryption")
async def get_encryption():  # REQ-918
    """Encryption provider + master-key status for the admin UI."""
    from provisa.encryption.providers import master_key_present

    cfg = read_config()
    enc = cfg.get("encryption", {}) or {}
    provider = enc.get("provider", "null")
    key_id = enc.get("key_id")
    providers = _encryption_providers()
    return {
        "provider": provider,
        "key_id": key_id,
        "key_present": master_key_present(key_id) if provider == "local" else None,
        "providers": providers,
        # Per-provider persisted config (mirrors /admin/auth). key_id stays top-level for `local`.
        "config": {p["key"]: dict(enc.get(p["key"], {}) or {}) for p in providers},
        "restart_required_note": "The encryption provider binds at startup — changes take effect after a service restart.",
    }


@router.put("/admin/encryption")
async def set_encryption(request: Request):  # REQ-918
    """Persist the encryption provider + key id. Applied on service restart."""
    from provisa.encryption.registry import get_provider_spec

    body = await request.json()
    provider = body.get("provider")
    spec = get_provider_spec(provider)
    if spec is None:
        raise HTTPException(status_code=400, detail=f"unknown encryption provider {provider!r}")
    if not spec.available():
        # Fail closed — the runtime (factory.build_encryption_service) can't build this.
        raise HTTPException(
            status_code=400,
            detail=f"encryption provider {provider!r} is not available (its SDK/runtime is not installed)",
        )
    path = config_path()
    cfg = read_config()
    enc = dict(cfg.get("encryption", {}) or {})
    # Persist the canonical key (spec.key), so aliases resolve consistently at boot.
    enc["provider"] = spec.key
    if "key_id" in body:
        enc["key_id"] = body["key_id"] or None
    # Persist only the keys this provider declares (mirrors /admin/auth).
    allowed = {f["config_key"] for f in spec.config_fields}
    pcfg = dict(enc.get(spec.key, {}) or {})
    for k, v in (body.get("config") or {}).items():
        if k in allowed:
            pcfg[k] = v
    if pcfg:
        enc[spec.key] = pcfg
    cfg["encryption"] = enc
    write_config(path, cfg)
    return {"success": True, "restart_required": True}


@router.post("/admin/encryption/generate-key")
async def generate_encryption_key(request: Request):  # REQ-918
    """Generate a fresh AES-256 master key and store it in the OS keychain under ``key_id``. When no
    OS keychain is available, the base64 key is returned once so the operator can set it as
    ``PROVISA_ENCRYPTION_KEY``. Never re-displayed after this call."""
    from provisa.encryption.providers import generate_master_key_b64, store_master_key

    body = await request.json()
    key_id = body.get("key_id") or None
    key_b64 = generate_master_key_b64()
    stored = store_master_key(key_b64, key_id)
    return {
        "stored": stored,
        "key_id": key_id or "master",
        # Only returned when it could NOT be stored in a keychain — the operator must persist it.
        "key_b64": None if stored else key_b64,
        "env_var": None if stored else "PROVISA_ENCRYPTION_KEY",
    }


_AUTH_PROVIDERS = [
    {
        "key": "none",
        "label": "None",
        "description": "No authentication — open access. Development only.",
        "config_fields": [],
    },
    {
        "key": "firebase",
        "label": "Firebase",
        "description": "Google Firebase ID-token verification.",
        "config_fields": [
            {"config_key": "project_id", "label": "Project ID", "type": "string", "required": True},
            {
                "config_key": "service_account_key",
                "label": "Service account key",
                "type": "string",
                "required": False,
                "secret": True,
            },
        ],
    },
    {
        "key": "keycloak",
        "label": "Keycloak",
        "description": "Keycloak OIDC (realm + client).",
        "config_fields": [
            {
                "config_key": "server_url",
                "label": "Server URL",
                "type": "string",
                "required": True,
                "placeholder": "https://keycloak.example.com",
            },
            {"config_key": "realm", "label": "Realm", "type": "string", "required": True},
            {"config_key": "client_id", "label": "Client ID", "type": "string", "required": True},
            {
                "config_key": "client_secret",
                "label": "Client secret",
                "type": "string",
                "required": False,
                "secret": True,
            },
        ],
    },
    {
        "key": "oauth",
        "label": "OAuth / OIDC",
        "description": "Generic OIDC provider via a discovery URL.",
        "config_fields": [
            {
                "config_key": "discovery_url",
                "label": "Discovery URL",
                "type": "string",
                "required": True,
                "placeholder": "https://issuer/.well-known/openid-configuration",
            },
            {"config_key": "client_id", "label": "Client ID", "type": "string", "required": True},
            {"config_key": "audience", "label": "Audience", "type": "string", "required": False},
            {
                "config_key": "role_claim",
                "label": "Role claim",
                "type": "string",
                "required": False,
                "placeholder": "roles",
            },
        ],
    },
    {
        "key": "simple",
        "label": "Simple (username/password)",
        "description": "Built-in username/password. NOT for production — requires the production guard.",
        "config_fields": [
            {
                "config_key": "jwt_secret",
                "label": "JWT signing secret",
                "type": "string",
                "required": True,
                "secret": True,
            },
        ],
    },
]


@router.get("/admin/auth")
async def get_auth():  # REQ-919
    """Auth provider selection + per-provider config + role settings for the admin UI."""
    from provisa.core.models import AuthConfig

    cfg = read_config()
    auth = cfg.get("auth", {}) or {}
    provider = auth.get("provider", "none")

    def _pcfg(pkey: str) -> dict:
        # jwt_secret lives at the auth top level (not under `simple`); surface it there for the UI.
        block = dict(auth.get(pkey, {}) or {})
        if pkey == "simple":
            block["jwt_secret"] = auth.get("jwt_secret", "")
        return block

    af = AuthConfig.model_fields
    return {
        "provider": provider,
        "providers": _AUTH_PROVIDERS,
        "config": {p["key"]: _pcfg(p["key"]) for p in _AUTH_PROVIDERS},
        "common": {
            "default_role": auth.get("default_role", af["default_role"].default),
            "assignments_source": auth.get("assignments_source", af["assignments_source"].default),
            "trust_upstream": bool(auth.get("trust_upstream", af["trust_upstream"].default)),
            "allow_simple_auth": bool(
                auth.get("allow_simple_auth", af["allow_simple_auth"].default)
            ),
        },
        "restart_required_note": "The auth provider binds at startup — changes take effect after a service restart.",
    }


@router.put("/admin/auth")
async def set_auth(request: Request):  # REQ-919
    """Persist the auth provider + its config + role settings. Applied on service restart."""
    body = await request.json()
    provider = body.get("provider")
    valid = {p["key"] for p in _AUTH_PROVIDERS}
    if provider not in valid:
        raise HTTPException(
            status_code=400, detail=f"unknown auth provider {provider!r}; valid: {sorted(valid)}"
        )

    path = config_path()
    cfg = read_config()
    auth = dict(cfg.get("auth", {}) or {})
    auth["provider"] = provider

    allowed = {
        f["config_key"] for p in _AUTH_PROVIDERS if p["key"] == provider for f in p["config_fields"]
    }
    pcfg = dict(auth.get(provider, {}) or {})
    for k, v in (body.get("config") or {}).items():
        if k not in allowed:
            continue
        if k == "jwt_secret":  # top-level, not under the provider block
            auth["jwt_secret"] = v
        else:
            pcfg[k] = v
    if provider != "simple" or pcfg:
        auth[provider] = pcfg

    for k in ("default_role", "assignments_source", "trust_upstream", "allow_simple_auth"):
        if k in (body.get("common") or {}):
            auth[k] = body["common"][k]

    cfg["auth"] = auth
    write_config(path, cfg)
    return {"success": True, "restart_required": True}


@router.post("/admin/query-engine/reload-catalog")
async def reload_query_engine_catalog(catalog: str = "otel"):
    """Reload an engine catalog without a restart, then reconnect and re-run OTel DDL.

    Delegates to the bound engine through the seam — the engine re-registers via its coordinator
    REST API so all workers pick up the change via discovery; a native engine has no reloadable
    catalog.
    """
    from provisa.api.app import state
    from provisa.api.startup_seed import _OPS_VIEWS

    return await state.federation_engine.reload_catalog(
        catalog, _OPS_VIEWS, getattr(state, "otel_snapshot_retention_hours", None)
    )


@router.post("/admin/query-engine/restart")
async def restart_query_engine(container: str | None = None):
    """Restart the query engine container (single-node dev only). Falls back to
    QUERY_ENGINE_CONTAINER env var, then the bound engine's name."""
    import asyncio

    from provisa.api.app import state

    container = (
        container or os.environ.get("QUERY_ENGINE_CONTAINER") or state.federation_engine.name
    )
    if not container:
        raise HTTPException(
            status_code=400,
            detail="no container specified and none resolvable from QUERY_ENGINE_CONTAINER or the bound engine",
        )
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
    from provisa.api.app import state
    from provisa.api.startup_seed import _compute_and_store_clusters

    if not state.tenant_db:
        raise HTTPException(status_code=503, detail="Database not available")
    async with state.tenant_db.acquire() as conn:
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
