# Copyright (c) 2026 Kenneth Stott
# Canary: 8e350ae5-8ad0-48aa-aaa4-f1df43ef1966
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

from provisa.api.admin._platform_guard import (
    has_deployment_settings,
    has_platform_settings,
    require_deployment_settings,
    require_org_settings,
)
from provisa.api.admin._config_io import config_path, read_config, write_config
from provisa.api.admin.secret_redaction import (  # REQ-1575
    redact,
    redact_per_provider,
    redact_url_password,
    restore_url_password,
)
from provisa.api.errors import ApiError
from provisa.compiler.sampling import get_sample_size as _get_sample_size
from provisa.core import deployment_settings as _deployment_settings
from provisa.core import settings_registry as _settings_registry

router = APIRouter()


@router.get("/admin/config")
async def download_config(request: Request):  # REQ-164
    """Download the ORIGINAL config YAML (the on-disk boot seed). The live-state view is
    ``/admin/config/live``; the UI diffs the two."""
    require_deployment_settings(request)  # REQ-1913: platform administrators, everywhere
    path = config_path()
    if not path.exists():
        raise ApiError(404, "settings.config_file_not_found", "Config file not found")
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
        raise ApiError(
            404,
            "settings.live_export_disabled",
            "Live config export is disabled (set live_config_export: true).",
        )


@router.get("/admin/config/live")
async def download_live_config(request: Request):  # REQ-164
    """The CURRENT config generated from live state (admin-created views/MVs, relationships, roles,
    rls, domains overlaid on the file base)."""
    require_deployment_settings(request)  # REQ-1913: platform administrators, everywhere
    _require_live_export()
    from provisa.api.admin.config_export import build_live_config_yaml

    return Response(
        content=await build_live_config_yaml(),
        media_type="application/x-yaml",
        headers={"Content-Disposition": "attachment; filename=provisa.live.yaml"},
    )


@router.get("/admin/config/diff")
async def config_diff(request: Request):  # REQ-164
    """Both sides of the config diff — ``original`` (startup baseline) and ``current`` (live state) —
    NORMALIZED identically so the side-by-side view shows only genuine changes, not reordering."""
    require_deployment_settings(request)  # REQ-1913: platform administrators, everywhere
    _require_live_export()
    from provisa.api.admin.config_export import config_diff as _diff

    return await _diff()


@router.post("/admin/config/patch")
async def config_patch(request: Request):  # REQ-164
    """A unified-diff patch from the baseline to the posted (curated) config — git-apply / ``patch``
    compatible, for committing config changes made in the UI through CI/CD."""
    require_deployment_settings(request)  # REQ-1913: platform administrators, everywhere
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
    require_deployment_settings(request)  # REQ-1913: platform administrators, everywhere
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


# The blocks whose subject is the DEPLOYMENT, not the org: engine sizing, the row-limit and
# sampling ceilings, FK auto-tracking, the inbound-CDC consumer identity, the materialization
# store, the tracing pipeline, and the remote-GraphQL traversal limits. Omitted entirely for a
# caller without `platform_settings` (REQ-1349) — an org administrator neither reads nor writes
# them, and the endpoint answers ordinary pages so it cannot refuse the whole request instead.
_PLATFORM_BLOCKS = (
    "engine",
    "limits",
    "relationships",
    "cdc",
    "materialize",
    "sampling",
    "otel",
    "graphql_remote",
)

# `naming` is split rather than dropped: the domain mode is the org's (REQ-1266), the conventions
# configure the process-global naming module and are the deployment's.
_NAMING_ORG_SUBKEYS = ("use_domains", "default_domain")

# The PUT blocks an org administrator may write with ``org_settings`` alone. A body touching
# anything else is checked against ``platform_settings`` as well.
_ORG_BLOCKS = ("redirect", "cache")


@router.get("/admin/settings")
async def get_settings(request: Request):  # REQ-165, REQ-302, REQ-303, REQ-416, REQ-1349
    """Return the settings visible to this caller: the acting org's, plus — for a caller holding
    ``platform_settings`` — the deployment-wide blocks."""
    from provisa.executor.redirect import RedirectConfig
    from provisa.core.limits import default_row_limit as _get_default_row_limit
    from provisa.api.app import state

    from provisa.core.models import (
        GraphQLRemoteConfig,
        ProvisaConfig,
    )

    from provisa.core import domain_policy

    rc = RedirectConfig.from_env()
    cfg = read_config()
    naming_cfg = cfg.get("naming", {})
    gqr_cfg = cfg.get("graphql_remote", {}) or {}

    def _eng(key: str):
        # Default lives in one place — the ProvisaConfig field default.
        return cfg.get(key, ProvisaConfig.model_fields[key].default)

    # REQ-1913: the deployment-wide blocks are the platform administrator's in every deployment —
    # the same rule as the settings catalog, which reads and writes the same stored values. A
    # single-tenant org administrator holds `platform_settings` and still does not get them.
    is_platform = has_deployment_settings(request)
    # The org's LIVE domain mode — its own override where it set one, the deployment's otherwise.
    # Read off the policy rather than the config file, which only ever states the deployment's.
    policy_use_domains, policy_default_domain = domain_policy.snapshot()

    payload = {
        # Opt-in features the UI gates on (REQ-164): live config export/diff/patch.
        "features": {
            "live_config_export": bool(getattr(state, "config_live_export", False)),
            # Lets the UI place a control under the right tab without a second round trip, and
            # tells it which blocks below were omitted rather than being empty.
            # `platform_settings` is the RIGHT (it opens the other platform surfaces: engine,
            # cache storage, encryption, auth, the config file). `deployment_settings` says
            # whether the deployment-wide blocks below are present.
            "platform_settings": has_platform_settings(request),
            "deployment_settings": is_platform,
        },
        "engine": _engine_block(cfg),
        "redirect": {
            "enabled": rc.enabled,
            "threshold": rc.threshold,
            "default_format": rc.default_format,
            "ttl": rc.ttl,
        },
        "limits": {
            "default_row_limit": _get_default_row_limit(),
            # REQ-1905: the default request timeout, and each transport's own value (null: the
            # transport uses the default). Always all ten transports.
            "request_timeout": _settings_registry.value("limits.request_timeout"),
            "request_timeouts": _settings_registry.value("limits.request_timeouts"),
        },
        "cache": {
            "default_ttl": state.response_cache_default_ttl,
        },
        "naming": {
            "domain_prefix": naming_cfg.get("domain_prefix", False),
            "convention": naming_cfg.get("convention", "apollo_graphql"),
            "sql_convention": naming_cfg.get("sql_convention", "snake"),
            "use_domains": policy_use_domains,
            "default_domain": policy_default_domain,
        },
        "relationships": {
            "auto_track_fk": _deployment_settings.auto_track_fk(),
        },
        "cdc": {  # REQ-931: Provisa-level inbound-CDC consumer group (receiver identity)
            "consumer_group_id": _eng("cdc_consumer_group_id"),
        },
        # Materialization-store DSN — the admin UI reads its scheme to decide native-CDC
        # availability for materialized views. Canonical write path is /admin/cache-storage.
        "materialize": {"store_url": cfg.get("materialize_store_url") or ""},
        "sampling": {
            "default_sample_size": _get_sample_size(),
        },
        "otel": _otel_block(),
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

    if not is_platform:
        for block in _PLATFORM_BLOCKS:
            payload.pop(block)
        payload["naming"] = {k: payload["naming"][k] for k in _NAMING_ORG_SUBKEYS}
    return payload


# The `redirect` fields an org owns: WHEN its results redirect and how long the link lives. The
# object store they land in — bucket, endpoint, credentials, region — is the deployment's and has
# no representation here (REQ-1349); RedirectConfig.from_env reads those from the platform env.
_REDIRECT_ORG_KEYS = ("enabled", "threshold", "default_format", "ttl")


async def _apply_org_blocks(request, body: dict, updated: list) -> None:
    """Persist the blocks whose subject is the ACTING ORG: `redirect` and `cache.default_ttl`.

    Written to the org's ``org_settings`` rows rather than to process env or the deployment YAML.
    On a shared shard those are one storage location for every org, so the previous env-backed
    write handed whichever org saved last a redirect threshold and a cache TTL that then governed
    every other org's queries.
    """
    from provisa.api.app import state
    from provisa.core.org_settings import read_org_overrides, write_org_overrides

    if not set(body) & set(_ORG_BLOCKS):
        return  # nothing org-scoped in this body — do not demand the org right for a platform save
    assert state.tenant_db is not None
    require_org_settings(request)  # REQ-1349
    overrides = await read_org_overrides(state.tenant_db)
    updates: dict = {}

    if "redirect" in body:
        r = body["redirect"] or {}
        unknown = set(r) - set(_REDIRECT_ORG_KEYS)
        if unknown:
            raise ApiError(
                400,
                "settings.redirect_not_org_overridable",
                f"not org-overridable: {', '.join(sorted(unknown))}",
            )
        merged = dict(overrides.get("redirect") or {})
        for k in _REDIRECT_ORG_KEYS:
            if k not in r:
                continue
            # Null clears the org's override for that field, restoring the deployment's value.
            if r[k] is None:
                merged.pop(k, None)
            else:
                merged[k] = r[k]
            updated.append(f"redirect.{k}")
        updates["redirect"] = merged or None

    if "cache" in body and "default_ttl" in body["cache"]:
        merged = dict(overrides.get("cache") or {})
        ttl = body["cache"]["default_ttl"]
        if ttl is None:
            merged.pop("default_ttl", None)
        else:
            merged["default_ttl"] = int(ttl)
        updates["cache"] = merged or None
        updated.append("cache.default_ttl")

    if not updates:
        return
    identity = getattr(request.state, "identity", None)
    await write_org_overrides(
        state.tenant_db, updates, updated_by=getattr(identity, "user_id", "anonymous")
    )
    # The query path reads these off the bound runtime, so the cached copy is refreshed here —
    # otherwise the org's next query still redirects and caches on the pre-save values.
    state.settings_overrides = await read_org_overrides(state.tenant_db)


# The `otel` block's fields, by the operator setting each one is (REQ-1913). A field left out of
# a save is not touched.
_OTEL_FIELDS = (
    "endpoint",
    "protocol",
    "service_name",
    "sample_rate",
    "log_level",
    "compact_cron",
    "compact_batch_size",
    "compact_file_chunk",
    "compact_max_files_per_run",
    "ops_snapshot_retention_hours",
    "span_export_delay_millis",
    "otlp2parquet_max_age_secs",
    "collector_batch_timeout_ms",
    "s3_endpoint",
    "support_endpoint",
    "support_redact_sql_literals",
    "support_redact_attributes",
)
# Fields that may be unset: the block states "unset" as an empty value.
_OTEL_UNSETTABLE = ("endpoint", "support_endpoint", "ops_snapshot_retention_hours")


def _otel_block() -> dict:
    """The `otel` block of GET /admin/settings: the telemetry settings as resolved now."""
    from provisa.core import settings_registry
    from provisa.core.models import SubsystemTracesConfig

    value = settings_registry.value
    block = {field: value(f"otel.{field}") for field in _OTEL_FIELDS}
    # The page edits these two as text; "no endpoint" is the empty string there.
    for field in ("endpoint", "support_endpoint"):
        if block[field] is None:
            block[field] = ""
    block["subsystem_traces"] = {  # REQ-1432: per-subsystem trace switches
        name: value(f"otel.subsystem_traces.{name}") for name in SubsystemTracesConfig.model_fields
    }
    return block


def _apply_otel(o: dict, updated: list, *, updated_by: str = "anonymous") -> None:
    """Store the `otel` observability block and apply it in this worker (REQ-1913).

    The fields are operator settings: validated together, stored in the control plane (where every
    worker and instance reads them), and refused with the field named when one cannot be used.
    They used to be written to this node's config file and this one worker's environment.
    """
    from provisa.api import otel_setup
    from provisa.api.admin.settings_catalog_router import _invalid, _refused
    from provisa.api.app import state
    from provisa.core import settings_registry
    from provisa.core.settings_registry import SettingInvalid, UnknownSetting

    values: dict = {}
    for field in _OTEL_FIELDS:
        if field not in o:
            continue
        raw = o[field]
        if field in _OTEL_UNSETTABLE and raw in (None, ""):
            raw = None  # clears the stored value
        values[f"otel.{field}"] = raw
        updated.append(f"otel.{field}")
    if "subsystem_traces" in o:
        for name, on in (o["subsystem_traces"] or {}).items():
            values[f"otel.subsystem_traces.{name}"] = on
        updated.append("otel.subsystem_traces")
    if not values:
        return
    assert state.admin_db is not None, "telemetry settings need the platform control plane"
    try:
        settings_registry.store(state.admin_db, values, updated_by=updated_by)
    except SettingInvalid as err:
        raise _invalid(err) from None
    except UnknownSetting as err:
        raise _refused(err.key, "unknown_setting") from None
    otel_setup.apply_exporter_settings()


_ENGINE_KEYS = (
    "jvm_heap_gb",
    "query_max_memory",
    "query_max_memory_per_node",
    "query_max_total_memory",
    "fault_tolerant_execution",
    "fault_tolerant_task_memory",
    "exchange_spool_dir",
)


def _engine_setting(cfg: dict, key: str):
    """One of the engine's sizing keys as saved: the stored value, else the config file's, else
    the ProvisaConfig default (the one place the default lives)."""
    from provisa.api.trino_setup import _cfg

    return _cfg(cfg, key)


def _engine_block(cfg: dict) -> dict:
    """The `engine` block of GET /admin/settings: the sizing the engine will start on next."""
    return {key: _engine_setting(cfg, key) for key in _ENGINE_KEYS}


def _store_engine_sizing(values: dict, *, updated_by: str) -> None:
    """Store engine sizing keys (``{field: value | None}``; None clears) and regenerate the
    engine's own config files from them. They take effect when the engine restarts."""
    from provisa.api.admin.settings_catalog_router import _invalid
    from provisa.api.app import state
    from provisa.core import settings_registry
    from provisa.core.settings_registry import SettingInvalid

    assert state.admin_db is not None, "engine settings need the platform control plane"
    try:
        settings_registry.store(
            state.admin_db,
            {f"engine.{field}": value for field, value in values.items()},
            updated_by=updated_by,
        )
    except SettingInvalid as err:
        raise _invalid(err) from None


def _apply_engine(e: dict, state, updated: list, *, updated_by: str = "anonymous") -> bool:
    """Apply execution-engine (federation) sizing keys. Returns True if a restart is needed.

    REQ-1913: stored in the control plane (they used to be written to this node's config file),
    then regenerated into the engine's config.properties; they take effect on an engine restart.
    """
    values = {k: e[k] for k in _ENGINE_KEYS if k in e}
    if not values:
        return False
    _store_engine_sizing(values, updated_by=updated_by)
    updated.extend(f"engine.{k}" for k in values)
    state.federation_engine.write_config(str(config_path()))
    return True


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
    # REQ-471 keeps the GraphQL and SQL planes on separate conventions, so each is its own key:
    # `convention` names GraphQL fields (and Cypher property keys), `sql_convention` names the SQL
    # plane the pgwire/Bolt/semantic surfaces read.
    from provisa.compiler.naming import VALID_CONVENTIONS

    for key in ("convention", "sql_convention"):
        if key not in n:
            continue
        if n[key] not in VALID_CONVENTIONS:
            return f"Invalid convention: {n[key]!r}"
        cfg.setdefault("naming", {})[key] = n[key]
        updated.append(f"naming.{key}")
        needs_reload = True
    if needs_reload:
        write_config(path, cfg)
        try:
            await _load_and_build(str(path))
        except Exception:
            pass
    return None


def _apply_scalars(body: dict, state, updated: list, updated_by: str = "anonymous") -> None:
    """Apply the deployment-wide env-backed scalar blocks (limits/sampling/relationships).

    `cache.default_ttl` is NOT here — it governs the org's own results and is written as an org
    override by _apply_org_blocks.
    """
    # REQ-1900: stored in the platform control plane, where every worker process and every
    # instance reads them (provisa/core/deployment_settings.py) — these used to be written to
    # the environment of the one process serving this request.
    values: dict = {}
    if "limits" in body and "default_row_limit" in body["limits"]:
        values["limits.default_row_limit"] = int(body["limits"]["default_row_limit"])
    if "sampling" in body and "default_sample_size" in body["sampling"]:
        values["sampling.default_sample_size"] = int(body["sampling"]["default_sample_size"])
    if "relationships" in body and "auto_track_fk" in body["relationships"]:
        values["relationships.auto_track_fk"] = bool(body["relationships"]["auto_track_fk"])
    # REQ-1905: the request timeout, default and per transport. Stored through the settings
    # registry, which validates them (a positive number; a known transport) and stores the map
    # key by key: the transports given are set, a null clears one back to the default, the
    # others keep what they have.
    timeouts: dict = {}
    if "limits" in body and "request_timeout" in body["limits"]:
        timeouts["limits.request_timeout"] = body["limits"]["request_timeout"]
    if "limits" in body and "request_timeouts" in body["limits"]:
        timeouts["limits.request_timeouts"] = body["limits"]["request_timeouts"]
    if not values and not timeouts:
        return
    assert state.admin_db is not None, "deployment settings need the platform control plane"
    if timeouts:
        try:
            _settings_registry.validate(timeouts)  # before anything in this request is stored
        except (_settings_registry.SettingInvalid, _settings_registry.UnknownSetting) as err:
            raise ApiError(
                400,
                "settings.invalid_value",
                f"setting {err.key} refused: {getattr(err, 'reason', 'unknown_setting')}",
                field=err.key,
                reason=getattr(err, "reason", "unknown_setting"),
            ) from err
    if values:
        _deployment_settings.write(state.admin_db, values, updated_by=updated_by)
    if timeouts:
        _settings_registry.store(state.admin_db, timeouts, updated_by=updated_by)
    updated.extend([*values, *timeouts])


@router.put("/admin/settings")
async def update_settings(request: Request):  # REQ-165, REQ-253, REQ-303, REQ-416, REQ-1349
    """Update settings at runtime, gated per block by whose settings the block is.

    `redirect` and `cache` are the acting org's and need only ``org_settings``; every other block
    is deployment-wide and still needs ``platform_settings``. A body carrying both is checked
    against both rights.
    """
    from provisa.api.app import state

    body = await request.json()
    updated: list = []
    restart_required = False

    # Every right the body needs is checked before any of it is written: the org blocks used to
    # be stored first, so a body refused for its deployment blocks was left half applied.
    if set(body) & set(_ORG_BLOCKS):
        require_org_settings(request)  # REQ-1349
    if set(body) - set(_ORG_BLOCKS):
        require_deployment_settings(request)  # REQ-1913: platform administrators, everywhere
    await _apply_org_blocks(request, body, updated)

    if "engine" in body:
        restart_required = (
            _apply_engine(
                body["engine"],
                state,
                updated,
                updated_by=getattr(
                    getattr(request.state, "identity", None), "user_id", "anonymous"
                ),
            )
            or restart_required
        )
    _identity = getattr(request.state, "identity", None)
    _apply_scalars(body, state, updated, getattr(_identity, "user_id", "anonymous"))
    if "graphql_remote" in body:
        _apply_graphql_remote(body["graphql_remote"], updated)
    if "naming" in body:
        err = await _apply_naming(body["naming"], updated)
        if err is not None:
            return {"success": False, "message": err}
    if "otel" in body:
        _apply_otel(body["otel"], updated, updated_by=getattr(_identity, "user_id", "anonymous"))

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
async def set_domain_policy(request: Request):  # REQ-165, REQ-1266, REQ-1349
    """Change the ACTING ORG's domain policy (use_domains / default_domain).

    DESTRUCTIVE, and destructive for this org only: every registered table's domain_id is bound to
    the policy, so the org's sources, tables, domains and relationships are purged and its schemas
    rebuilt. The policy is stored as the org's ``naming`` override — one org modelling a single
    domain says nothing about the next, and on a shared shard rewriting the deployment YAML here
    would reset every other org's catalog along with this one's.
    """
    require_org_settings(request)  # REQ-1349
    from provisa.api.app import _rebuild_schemas, state
    from provisa.api.app_loaders import _build_source_pools_and_enums
    from provisa.core import domain_policy
    from provisa.core.config_loader import load_config, parse_config_dict
    from provisa.core.org_settings import read_org_overrides, write_org_overrides
    from provisa.core.repositories import role as role_repo

    body = await request.json()
    use_domains = body.get("use_domains", None)
    default_domain = body.get("default_domain", "default")
    if use_domains not in (None, True, False):
        raise ApiError(
            400, "settings.use_domains_invalid", "use_domains must be true, false, or null"
        )
    if use_domains is False and not default_domain:
        raise ApiError(
            400,
            "settings.default_domain_required",
            "default_domain required when use_domains=false",
        )

    tenant_db = state.tenant_db
    if tenant_db is None:
        raise ApiError(
            409,
            "settings.no_active_org",
            "no org is bound to this request; sign in to an org first",
        )

    # 1. Persist the org's policy. `None` means "inherit the deployment's", which is the same value
    #    that clears the override row outright.
    naming: dict | None = None
    if use_domains is not None:
        naming = {"use_domains": use_domains}
        if use_domains is False:
            naming["default_domain"] = default_domain
    identity = getattr(request.state, "identity", None)
    await write_org_overrides(
        tenant_db, {"naming": naming}, updated_by=getattr(identity, "user_id", "anonymous")
    )
    state.settings_overrides = await read_org_overrides(tenant_db)

    # 2. Apply it to this org's policy scope before anything re-registers against it. A cleared
    #    override (use_domains=None) resolves to the deployment's own naming block — the value the
    #    org inherits — so the scope never reads as inert when the deployment namespaces domains.
    deployment = read_config().get("naming", {}) or {}
    effective = {
        "use_domains": use_domains if use_domains is not None else deployment.get("use_domains"),
        "default_domain": default_domain
        if use_domains is False
        else deployment.get("default_domain", "default"),
    }
    domain_policy.configure(effective["use_domains"], effective["default_domain"])

    # 3. Purge THIS org's catalog: an empty config in replace mode deletes the sources, tables,
    #    domains and relationships bound to the old policy. Same config→org sequence the import
    #    surface runs — load into the org's tenant_db, then rebuild its pools and schemas.
    async with tenant_db.acquire() as conn:
        # Roles are org auth, not catalog: replace mode deletes every role absent from the config it
        # is handed, so the org's own roles are read back and handed to it unchanged.
        existing_roles = await role_repo.list_all(conn)
    empty = parse_config_dict(
        {
            "sources": [],
            "domains": [],
            "tables": [],
            "relationships": [],
            "roles": existing_roles,
            # The effective policy, not the raw override: load_config re-configures the scope from
            # whatever naming block it is handed, so handing it the cleared form would undo step 2.
            "naming": effective,
        }
    )
    async with tenant_db.acquire() as conn:
        await load_config(
            empty,
            conn,
            state.federation_engine,
            replace=True,
            catalog_names=state.source_catalogs,
        )
    await _build_source_pools_and_enums(empty)
    await _rebuild_schemas()

    return {"success": True, "use_domains": use_domains}


@router.get("/admin/federation-engine")
async def get_federation_engine():  # REQ-916
    """Current federation-engine selection + connection config, and the selectable-engine registry."""
    from provisa.federation.engine import engine_registry
    from provisa.core.models import ProvisaConfig

    from provisa.api.app import state

    cfg = read_config()

    def _eng(key: str):
        if key in _ENGINE_KEYS:
            return _engine_setting(cfg, key)  # REQ-1913: stored in the control plane
        return cfg.get(key, ProvisaConfig.model_fields[key].default)

    # `current` names the engine the process actually booted — build_engine records which key won its
    # precedence (arg > $PROVISA_ENGINE > persisted field > duckdb). Reporting the persisted field
    # would lie about a pinned deployment, so both are returned and the tab edits `persisted`.
    env_pinned = os.environ.get("PROVISA_ENGINE")
    # The pin is reported as data (`env_pinned_engine`) and the tab renders its own alert for it
    # (FederationEngineTab's `federationEngineTab.envPinned`). Restating it here too put two pin
    # banners on the page, one of them un-translatable because it is server prose.
    note = "Changing the federation engine takes effect after the service is restarted."

    # Return current values for every config key any engine declares (each is a ProvisaConfig
    # field), so the tab can render per-engine fields — connection AND execution tuning — generically.
    all_keys = {f["config_key"] for e in engine_registry() for f in e["config_fields"]}
    # REQ-1575: the S3 secret key never comes back, and a DSN comes back without its password —
    # `federation_engine_url` reads as an address but carries a warehouse credential in its userinfo.
    _fields = [f for e in engine_registry() for f in e["config_fields"]]
    _safe, _is_set = redact({k: _eng(k) for k in sorted(all_keys)}, _fields)
    _safe = {
        k: (redact_url_password(v) if k.endswith("_url") and isinstance(v, str) else v)
        for k, v in _safe.items()
    }
    return {
        "current": state.federation_engine.selected_key,
        "persisted": _eng("federation_engine"),
        "env_pinned_engine": env_pinned,
        "config": _safe,
        "secret_set": _is_set,
        "engines": engine_registry(),
        "restart_required_note": note,
    }


@router.put("/admin/federation-engine")
async def set_federation_engine(request: Request):  # REQ-916
    """Persist the federation-engine selection + connection config. Applied on service restart."""
    require_deployment_settings(request)  # REQ-1913: platform administrators, everywhere
    from provisa.federation.engine import engine_registry

    from provisa.api.app import state

    body = await request.json()
    engine = body.get("engine")
    registry = engine_registry()
    valid = {e["key"] for e in registry}
    if engine not in valid:
        raise ApiError(
            400,
            "settings.unknown_engine",
            f"unknown engine {engine!r}; valid: {sorted(valid)}",
            engine=str(engine),
            valid=sorted(valid),
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
    # REQ-1913: the sizing keys are operator settings — stored in the control plane, where the
    # settings page stores them too, not in this node's config file.
    sizing: dict = {}
    for ck, field in selected_fields.items():
        if ck in body:
            coerced = _coerce(field, body[ck])
            if ck in _ENGINE_KEYS:
                sizing[ck] = coerced  # None clears the stored value
                updated.append(ck)
                continue
            if ck.endswith("_url") and isinstance(coerced, str):
                # REQ-1575: the form was handed this URL without its password; posting the page back
                # unchanged must not be what deletes the credential.
                coerced = restore_url_password(coerced, cfg.get(ck))
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
        if ck in _ENGINE_KEYS:
            sizing[ck] = None

    if sizing:
        _identity = getattr(request.state, "identity", None)
        _store_engine_sizing(sizing, updated_by=getattr(_identity, "user_id", "anonymous"))
    write_config(path, cfg)
    # Regenerate the engine's derived config (e.g. Trino jvm.config/config.properties) so sizing
    # changes are written out; a native engine's write_config is a no-op. Applies on restart.
    state.federation_engine.write_config(str(path))
    return {"success": True, "updated": updated, "restart_required": True}


# The blocks of /admin/cache-storage that are operator settings (REQ-1913), by block and field.
_CACHE_STORAGE_SETTINGS = {
    "cache": ("enabled", "redis_url", "default_ttl"),
    "hot_tables": ("auto_threshold", "max_rows", "max_bytes", "refresh_interval"),
    "warm_tables": (
        "query_threshold",
        "max_rows",
        "refresh_interval",
        "fs_cache_enabled",
        "fs_cache_directories",
        "fs_cache_max_sizes",
    ),
    "materialized_views": ("default_ttl",),
}


@router.get("/admin/cache-storage")
async def get_cache_storage(request: Request):  # REQ-917
    """Hot-cache (Redis) + materialize-store settings for the admin UI."""
    require_deployment_settings(request)  # REQ-1913: platform administrators, everywhere
    from provisa.api.app import state

    from provisa.api.admin.secret_redaction import redact_url_password
    from provisa.core import settings_registry

    cfg = read_config()
    # The DSN the active engine offers itself as its materialize target absent explicit config
    # (engine.py:_*_materialize_default). Reported so the UI shows the real "empty →" fallback
    # for THIS engine rather than a hardcoded string — None when the engine declares no default.
    default_store = state.federation_engine.engine.default_materialize_store()
    # REQ-1913: the cache and tier settings are operator settings — what is reported is what is
    # saved (stored, then environment, then config, then the declared default).
    saved = settings_registry.resolve

    def _tier(block: str, *fields: str) -> dict:
        return {field: saved(f"{block}.{field}").value for field in fields}

    hot = _tier("hot_tables", "auto_threshold", "max_bytes", "refresh_interval")
    # REQ-230: with no ceiling of its own the hot tier's row ceiling is its auto threshold.
    own_max_rows = saved("hot_tables.max_rows").value
    hot["max_rows"] = own_max_rows if own_max_rows is not None else hot["auto_threshold"]
    redis_url = saved("cache.redis_url").value
    return {
        "cache": {
            "enabled": saved("cache.enabled").value,
            # empty → embedded fakeredis. The address is returned WITHOUT its password; a save
            # that posts it back unchanged keeps the stored one (restore_url_password).
            "redis_url": redact_url_password(redis_url) if redis_url is not None else "",
            "default_ttl": saved("cache.default_ttl").value,
        },
        "hot_tables": hot,
        "warm_tables": _tier(  # REQ-240: tier-promotion thresholds + engine filesystem read-cache
            "warm_tables",
            "query_threshold",
            "max_rows",
            "refresh_interval",
            "fs_cache_enabled",
            "fs_cache_directories",
            "fs_cache_max_sizes",
        ),
        # REQ-543: default MV refresh TTL for MVs without their own
        "materialized_views": _tier("materialized_views", "default_ttl"),
        "materialize": {
            "store_url": cfg.get("materialize_store_url") or "",
            "default_store_url": default_store or "",
        },
        # Telemetry-store DSN (REQ — mirrors materialize_store_url). Read by
        # observability.ops_schema.configured_ops_store_url(); empty -> embedded
        # DuckDB under telemetry_dir().
        "ops": {"store_url": cfg.get("ops_store_url") or ""},
        "restart_required_note": "Redis and materialize-store connections bind at startup — changes take effect after a service restart.",
    }


@router.put("/admin/cache-storage")
async def set_cache_storage(request: Request):  # REQ-917
    """Persist hot-cache (Redis) + materialize-store settings. Applied on service restart."""
    require_deployment_settings(request)  # REQ-1913: platform administrators, everywhere
    body = await request.json()
    path = config_path()
    cfg = read_config()
    updated: list[str] = []

    # REQ-1913: the cache and tier settings are operator settings, stored in the control plane
    # (they used to be written to this node's config file). A blank value clears the stored one.
    from provisa.api.admin.secret_redaction import restore_url_password
    from provisa.api.admin.settings_catalog_router import _invalid
    from provisa.api.app import state
    from provisa.core import settings_registry
    from provisa.core.settings_registry import SettingInvalid

    values: dict = {}
    for block, fields in _CACHE_STORAGE_SETTINGS.items():
        for field in fields:
            if field in (body.get(block) or {}):
                raw = body[block][field]
                values[f"{block}.{field}"] = None if raw in (None, "") else raw
                updated.append(f"{block}.{field}")
    if values.get("cache.redis_url") is not None:
        # REQ-1575: the page was handed this address without its password; posting it back
        # unchanged must not be what deletes the credential.
        values["cache.redis_url"] = restore_url_password(
            values["cache.redis_url"], settings_registry.resolve("cache.redis_url").value
        )
    if values:
        assert state.admin_db is not None, "cache settings need the platform control plane"
        identity = getattr(request.state, "identity", None)
        try:
            settings_registry.store(
                state.admin_db, values, updated_by=getattr(identity, "user_id", "anonymous")
            )
        except SettingInvalid as err:
            raise _invalid(err) from None
    if "materialize" in body and "store_url" in body["materialize"]:
        cfg["materialize_store_url"] = body["materialize"]["store_url"] or None
        updated.append("materialize_store_url")
    if "ops" in body and "store_url" in body["ops"]:
        cfg["ops_store_url"] = body["ops"]["store_url"] or None
        updated.append("ops_store_url")

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
async def get_encryption(request: Request):  # REQ-918
    """Encryption provider + master-key status for the admin UI."""
    require_deployment_settings(request)  # REQ-1913: platform administrators, everywhere
    from provisa.encryption.providers import master_key_present

    cfg = read_config()
    enc = cfg.get("encryption", {}) or {}
    provider = enc.get("provider", "null")
    key_id = enc.get("key_id")
    providers = _encryption_providers()
    _enc_safe, _enc_set = redact_per_provider(
        {p["key"]: enc.get(p["key"]) or {} for p in providers}, providers
    )
    return {
        "provider": provider,
        "key_id": key_id,
        "key_present": master_key_present(key_id) if provider == "local" else None,
        "providers": providers,
        # Per-provider persisted config (mirrors /admin/auth). key_id stays top-level for `local`.
        # REQ-1575: minus every field the registry marks secret — a KMS credential is not something
        # this surface hands to a browser. `secret_set` carries the one bit the form needs.
        "config": _enc_safe,
        "secret_set": _enc_set,
        "restart_required_note": "The encryption provider binds at startup — changes take effect after a service restart.",
    }


@router.put("/admin/encryption")
async def set_encryption(request: Request):  # REQ-918
    """Persist the encryption provider + key id. Applied on service restart."""
    require_deployment_settings(request)  # REQ-1913: platform administrators, everywhere
    from provisa.encryption.registry import get_provider_spec

    body = await request.json()
    provider = body.get("provider")
    spec = get_provider_spec(provider)
    if spec is None:
        raise ApiError(
            400,
            "settings.unknown_encryption_provider",
            f"unknown encryption provider {provider!r}",
            provider=str(provider),
        )
    if not spec.available():
        # Fail closed — the runtime (factory.build_encryption_service) can't build this.
        raise ApiError(
            400,
            "settings.encryption_provider_unavailable",
            f"encryption provider {provider!r} is not available (its SDK/runtime is not installed)",
            provider=str(provider),
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


def _secrets_providers() -> list[dict]:
    """UI view of the secrets-provider registry (REQ-1557, REQ-1558).

    Every registered backend is listed, available or not. An operator asking "does Provisa talk to
    our Vault?" is answered by the row being there; ``available`` and ``requires`` then say that
    the only thing missing is the client library, which the page renders as
    "(requires hvac import)" rather than hiding the row and leaving the question open.
    """
    from provisa.core.secrets_registry import secrets_provider_registry

    return [
        {
            "key": s.key,
            "label": s.label,
            "description": s.description,
            "available": s.available(),
            "requires": s.requires,
            "writable": s.writable,
            "config_fields": s.config_fields,
        }
        for s in secrets_provider_registry()
    ]


@router.get("/admin/secrets-service")
async def get_secrets_service(request: Request):  # REQ-1557, REQ-1558
    """Which secrets service this deployment is wired to, and what else it could be.

    The SERVICE is the deployment's, so this is platform_settings — distinct from an org's secret
    NAMES, which are org_settings and live under /admin/orgs/{org_id}/secrets.
    """
    require_deployment_settings(request)  # REQ-1913: platform administrators, everywhere
    cfg = read_config()
    sec = cfg.get("secrets", {}) or {}
    providers = _secrets_providers()
    # Unset is not unconfigured: it selects Provisa's own encrypted per-org store (REQ-1557).
    # REQ-1575: the Vault token and its like go out of this response entirely; `secret_set` says
    # only whether one is on file, which is what the form renders.
    safe, is_set = redact_per_provider(
        {p["key"]: sec.get(p["key"]) or {} for p in providers}, providers
    )
    return {
        "provider": sec.get("provider", "provisa"),
        "providers": providers,
        "config": safe,
        "secret_set": is_set,
    }


@router.put("/admin/secrets-service")
async def set_secrets_service(request: Request):  # REQ-1557, REQ-1558
    """Select the secrets backend. Persisted to provisa.yaml AND applied to this process."""
    require_deployment_settings(request)  # REQ-1913: platform administrators, everywhere
    from provisa.core.secrets_registry import get_secrets_provider_spec
    from provisa.core.secrets_runtime import configure_secrets

    body = await request.json()
    provider = body.get("provider")
    spec = get_secrets_provider_spec(provider)
    if spec is None:
        raise ApiError(
            400,
            "settings.unknown_secrets_provider",
            f"unknown secrets provider {provider!r}",
            provider=str(provider),
        )
    if not spec.available():
        # Fail closed — selecting a backend that cannot be built must not quietly leave the
        # deployment on the previous one (REQ-1557).
        raise ApiError(
            400,
            "settings.secrets_provider_unavailable",
            f"secrets provider {provider!r} is not available (install {spec.requires!r} to use it)",
            provider=str(provider),
        )
    path = config_path()
    cfg = read_config()
    sec = dict(cfg.get("secrets", {}) or {})
    sec["provider"] = spec.key
    allowed = {f["config_key"] for f in spec.config_fields}
    pcfg = dict(sec.get(spec.key, {}) or {})
    for k, v in (body.get("config") or {}).items():
        if k in allowed:
            pcfg[k] = v
    if pcfg:
        sec[spec.key] = pcfg
    cfg["secrets"] = sec
    write_config(path, cfg)
    # The backend is built on first use, so rebinding here is the whole of applying it — no
    # restart, unlike encryption whose service is held by objects built at startup.
    configure_secrets(spec.key, config=pcfg)
    return {"success": True, "provider": spec.key}


@router.post("/admin/encryption/generate-key")
async def generate_encryption_key(request: Request):  # REQ-918, REQ-1574, REQ-1801
    """Generate a fresh AES-256 master key into the OS keychain under ``key_id``. Never returns it.

    REQ-1574 amends REQ-918, whose one-time display was the last place a key was ever shown by any
    surface. A key that reaches the browser has been through a response body, a devtools network
    tab and whatever cache sits between — so when there is no keychain to store it in, this refuses
    rather than printing it, and the operator generates one out of band and sets
    ``PROVISA_ENCRYPTION_KEY`` themselves. The key generated here exists in the keychain and
    nowhere else.

    REQ-1801: takes effect immediately, no restart. Storing the key is only half the job — the
    running process's EncryptionService was built once at startup from whatever key existed then
    (or none), and nothing rebuilt it after this endpoint wrote a new one, so a freshly-generated
    key sat in the keychain unused until the next restart. Since the provider here is always
    "local" (the only one this endpoint's keychain path applies to) and its config hasn't changed
    — only the key material backing it has — re-running configure_encryption with the SAME
    provider/config is exactly the idempotent re-provision it's documented to support, not a
    provider swap (which is what still legitimately needs the PUT /admin/encryption restart note).
    """
    require_deployment_settings(request)  # REQ-1913: platform administrators, everywhere
    from provisa.encryption import configure_encryption
    from provisa.encryption.providers import generate_master_key_b64, store_master_key

    body = await request.json()
    key_id = body.get("key_id") or None
    if not store_master_key(generate_master_key_b64(), key_id):
        raise ApiError(
            503,
            "encryption.no_keystore",
            "no OS keychain is available to hold the key, and a key is never returned over the "
            "wire. Generate one out of band (openssl rand -base64 32) and set "
            "PROVISA_ENCRYPTION_KEY.",
            env_var="PROVISA_ENCRYPTION_KEY",
        )

    # REQ-1801: rebuild the live service NOW, from the SAME provider/config already active — this
    # is a key rotation on the running "local" provider, not a provider change, so none of
    # PUT /admin/encryption's restart caveats apply.
    enc_cfg = read_config().get("encryption", {}) or {}
    configure_encryption("local", key_id=key_id, config=enc_cfg.get("local", {}))
    return {"stored": True, "key_id": key_id or "master"}


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
async def get_auth(request: Request):  # REQ-919
    """Auth provider selection + per-provider config + role settings for the admin UI."""
    require_deployment_settings(request)  # REQ-1913: platform administrators, everywhere
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

    # REQ-1575: client secrets and the JWT signing secret never leave the server.
    _auth_safe, _auth_set = redact_per_provider(
        {p["key"]: _pcfg(p["key"]) for p in _AUTH_PROVIDERS}, _AUTH_PROVIDERS
    )
    af = AuthConfig.model_fields
    return {
        "provider": provider,
        "providers": _AUTH_PROVIDERS,
        "config": _auth_safe,
        "secret_set": _auth_set,
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
    require_deployment_settings(request)  # REQ-1913: platform administrators, everywhere
    body = await request.json()
    provider = body.get("provider")
    valid = {p["key"] for p in _AUTH_PROVIDERS}
    if provider not in valid:
        raise ApiError(
            400,
            "settings.unknown_auth_provider",
            f"unknown auth provider {provider!r}; valid: {sorted(valid)}",
            provider=str(provider),
            valid=sorted(valid),
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
async def reload_query_engine_catalog(request: Request, catalog: str = "otel"):
    """Reload an engine catalog without a restart, then reconnect and re-run OTel DDL.

    Delegates to the bound engine through the seam — the engine re-registers via its coordinator
    REST API so all workers pick up the change via discovery; a native engine has no reloadable
    catalog.
    """
    require_deployment_settings(request)  # REQ-1913: platform administrators, everywhere
    from provisa.api.app import state
    from provisa.api.startup_seed import _OPS_VIEWS

    return await state.federation_engine.reload_catalog(catalog, _OPS_VIEWS)


async def _resolve_engine_container(client, engine_name: str) -> str:  # REQ-1433
    """The name docker knows the query engine by.

    An explicit name is the caller's or the deployment's business. Without one, ask docker what is
    running rather than assuming the container is named after the engine: under compose it is
    ``compose-trino-1``, and restarting the name "trino" is a 404 that reads as a broken button.
    A match has to be unambiguous — two candidates mean this process cannot tell which engine is
    the one it is bound to, and picking either would restart a container at random.
    """
    resp = await client.get("/containers/json", params={"all": "true"})
    resp.raise_for_status()
    candidates = sorted(
        {
            name.lstrip("/")
            for c in resp.json()
            for name in c.get("Names", [])
            if engine_name in name.lstrip("/").split("-")
        }
    )
    if len(candidates) == 1:
        return candidates[0]
    raise ApiError(
        400,
        "settings.no_container_resolvable",
        f"cannot tell which container runs {engine_name}: "
        f"{'no container matches' if not candidates else 'matched ' + ', '.join(candidates)}. "
        "Set QUERY_ENGINE_CONTAINER or pass container=.",
    )


@router.post("/admin/query-engine/restart")
async def restart_query_engine(request: Request, container: str | None = None):  # REQ-1433
    """Restart the query engine container.

    Speaks the Docker Engine API over the bind-mounted socket. The app image carries no docker CLI
    — shelling out to one failed with "docker not found on PATH" on every deployment that mounts
    the socket, which is all of them.
    """
    require_deployment_settings(request)  # REQ-1913: platform administrators, everywhere
    import httpx

    from provisa.api.app import state
    from provisa.federation.isolated_provisioner import _client, _socket_path

    socket = _socket_path()
    if not os.path.exists(socket):
        raise ApiError(
            503,
            "settings.docker_socket_unavailable",
            f"the docker socket is not mounted at {socket}; this deployment cannot restart containers",
        )

    try:
        async with _client(socket) as client:
            name = (
                container
                or os.environ.get("QUERY_ENGINE_CONTAINER")
                or await _resolve_engine_container(client, state.federation_engine.name)
            )
            resp = await client.post(f"/containers/{name}/restart", timeout=120.0)
    except httpx.TimeoutException:
        raise ApiError(504, "settings.docker_restart_timeout", "the restart timed out")
    except httpx.HTTPError as exc:
        raise ApiError(502, "settings.docker_unreachable", f"docker socket error: {exc}")

    if resp.status_code == 404:
        raise ApiError(404, "settings.container_not_found", f"no container named {name}")
    if resp.status_code >= 400:
        raise HTTPException(
            status_code=500, detail=resp.text.strip() or f"docker {resp.status_code}"
        )

    return {"success": True, "container": name}


@router.post("/admin/schema-clusters/recompute")
async def recompute_schema_clusters():  # REQ-510
    """Rerun Louvain clustering on the schema graph and refresh schema_clusters."""
    from provisa.api.app import state
    from provisa.api.startup_seed import _compute_and_store_clusters

    if not state.tenant_db:
        raise ApiError(503, "settings.database_not_available", "Database not available")
    async with state.tenant_db.acquire() as conn:
        count = await _compute_and_store_clusters(conn)  # type: ignore[arg-type]
    return {"success": True, "tables_clustered": count}


@router.get("/admin/traces/recent")
async def get_recent_traces(request: Request, limit: int = 50):  # REQ-302, REQ-303, REQ-1349
    """Return the last N completed spans from the in-memory buffer.

    Read-only performance data, gated on the ``observability`` right. A caller without
    ``cross_org`` sees only the spans their own org produced — a tenant operator can measure their
    org without seeing another tenant's traffic.
    """
    from provisa.api.admin._platform_guard import require_observability
    from provisa.api.admin.capabilities import _resolved_capabilities
    from provisa.api.app import state
    from provisa.api.otel_setup import span_buffer
    from provisa.security.rights import Capability, has_platform_bypass

    require_observability(request)
    identity = getattr(request.state, "identity", None)
    caps = _resolved_capabilities(identity, state) if identity is not None else set()
    org_scope = (
        None
        if (has_platform_bypass(caps) or Capability.CROSS_ORG.value in caps)
        else getattr(request.state, "active_org_id", None)
    )
    return {"traces": span_buffer.recent(min(limit, 200), org_id=org_scope)}
