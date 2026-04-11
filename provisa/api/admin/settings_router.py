# Copyright (c) 2026 Kenneth Stott
# Canary: 0f8a2c4d-1b3e-4f6a-8c9d-2e5f7a0b1c3d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Admin config + platform settings REST endpoints."""

import os
from pathlib import Path

import yaml
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response

router = APIRouter()


@router.get("/admin/config")
async def download_config():
    """Download the current config YAML."""
    config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")
    path = Path(config_path)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Config file not found")
    return Response(
        content=path.read_text(),
        media_type="application/x-yaml",
        headers={"Content-Disposition": f"attachment; filename={path.name}"},
    )


@router.put("/admin/config")
async def upload_config(request: Request):
    """Upload a revised config YAML and reload."""
    from provisa.api.app import _load_and_build  # lazy to avoid circular import
    body = await request.body()
    config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")
    path = Path(config_path)
    if path.exists():
        backup = path.with_suffix(".yaml.bak")
        backup.write_text(path.read_text())
    path.write_bytes(body)
    try:
        await _load_and_build(config_path)
        return {"success": True, "message": "Config uploaded and reloaded"}
    except Exception as e:
        return {"success": False, "message": str(e)}


def _read_config() -> dict:
    config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")
    try:
        with open(config_path) as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


@router.get("/admin/settings")
async def get_settings():
    """Return current platform settings."""
    from provisa.executor.redirect import RedirectConfig
    from provisa.compiler.sampling import get_sample_size
    from provisa.api.app import state
    rc = RedirectConfig.from_env()
    cfg = _read_config()
    naming_cfg = cfg.get("naming", {})
    otel_cfg = cfg.get("observability", {})
    return {
        "redirect": {
            "enabled": rc.enabled,
            "threshold": rc.threshold,
            "default_format": rc.default_format,
            "ttl": rc.ttl,
        },
        "sampling": {
            "default_sample_size": get_sample_size(),
        },
        "cache": {
            "default_ttl": state.cache_default_ttl,
        },
        "naming": {
            "domain_prefix": naming_cfg.get("domain_prefix", False),
            "convention": naming_cfg.get("convention", "snake_case"),
        },
        "otel": {
            "endpoint": os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT") or otel_cfg.get("endpoint", ""),
            "service_name": os.environ.get("OTEL_SERVICE_NAME") or otel_cfg.get("service_name", "provisa"),
            "sample_rate": float(otel_cfg.get("sample_rate", 1.0)),
        },
    }


@router.put("/admin/settings")
async def update_settings(request: Request):
    """Update platform settings at runtime."""
    from provisa.api.app import state, _load_and_build
    body = await request.json()
    updated = []

    if "redirect" in body:
        r = body["redirect"]
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

    if "sampling" in body:
        s = body["sampling"]
        if "default_sample_size" in s:
            os.environ["PROVISA_SAMPLE_SIZE"] = str(s["default_sample_size"])
            updated.append("sampling.default_sample_size")

    if "cache" in body:
        c = body["cache"]
        if "default_ttl" in c:
            state.cache_default_ttl = int(c["default_ttl"])
            updated.append("cache.default_ttl")

    if "naming" in body:
        n = body["naming"]
        needs_reload = False
        config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")
        path = Path(config_path)
        with open(path) as f:
            cfg = yaml.safe_load(f)
        if "domain_prefix" in n:
            cfg.setdefault("naming", {})["domain_prefix"] = bool(n["domain_prefix"])
            updated.append("naming.domain_prefix")
            needs_reload = True
        if "convention" in n:
            valid = ("none", "snake_case", "camelCase", "PascalCase")
            if n["convention"] not in valid:
                return {"success": False, "message": f"Invalid convention: {n['convention']!r}"}
            cfg.setdefault("naming", {})["convention"] = n["convention"]
            updated.append("naming.convention")
            needs_reload = True
        if needs_reload:
            _write_config(path, cfg)
            try:
                await _load_and_build(config_path)
            except Exception:
                pass

    if "otel" in body:
        o = body["otel"]
        config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")
        path = Path(config_path)
        try:
            cfg = _read_config()
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
            _write_config(path, cfg)
        except Exception:
            pass

    return {"success": True, "updated": updated}


@router.get("/admin/traces/recent")
async def get_recent_traces(limit: int = 50):
    """Return the last N completed spans from the in-memory buffer."""
    try:
        from provisa.api.otel_setup import span_buffer
        return {"traces": span_buffer.recent(min(limit, 200))}
    except Exception:
        return {"traces": []}


def _write_config(path: Path, cfg: dict) -> None:
    backup = path.with_suffix(".yaml.bak")
    backup.write_text(path.read_text())
    with open(path, "w") as f:
        yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)
