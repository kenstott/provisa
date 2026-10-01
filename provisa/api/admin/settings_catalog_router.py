# Copyright (c) 2026 Kenneth Stott
# Canary: 4a8c2e71-9b35-4d06-a1f8-6e3d7c05b942
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The operator settings catalog (REQ-1913).

``GET /admin/settings/catalog`` lists every declared operator setting, grouped into the cards the
admin UI renders, each with its value, the source that value comes from, its range, and whether a
change is live or waits for a restart. ``PUT /admin/settings/catalog`` stores values in the
control plane, where they take precedence over the environment and the config file and reach
every worker and instance (REQ-1900).

The declarations, the resolution order and the validation are ``provisa.core.settings_registry``'s;
this module is the HTTP door: who may use it, the guards on the settings that can lock an operator
out, and the error shape. It sits beside ``/admin/settings`` rather than inside it because that
endpoint is read by ordinary pages and omits what a caller may not see; this one refuses.
"""

# Requirements: REQ-1913, REQ-1337, REQ-1900

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from sqlalchemy import select

from provisa.api.admin import settings_guards
from provisa.api.admin._platform_guard import is_anonymous, require_deployment_settings
from provisa.api.errors import ApiError
from provisa.core import deployment_settings, settings_registry
from provisa.core.schema_admin import deployment_settings as _table
from provisa.core.settings_registry import Setting, SettingInvalid, UnknownSetting

router = APIRouter()

_ANONYMOUS = "anonymous"

# What the UI server asks for. It is its own process with no control plane and no identity, so
# the path is outside the bearer gate (provisa/auth/middleware.py); the answer is one number.
UI_SERVER_SETTINGS_PATH = "/internal/ui-server-settings"


def _refused(field: str, reason: str, **params: Any) -> ApiError:
    return ApiError(
        400,
        "settings.invalid_value",
        f"setting {field} refused: {reason}",
        field=field,
        reason=reason,
        **params,
    )


def _invalid(err: SettingInvalid) -> ApiError:
    """The registry's refusal as the API's: the field, the reason, and the bound it broke."""
    params: dict[str, Any] = {}
    if err.reason in ("below_min", "above_max"):
        params = {"min": err.min, "max": err.max}
    elif err.reason == "not_in_choices":
        params = {"choices": list(err.choices or ())}
    return _refused(err.key, err.reason, **params)


def _audit() -> dict[str, tuple[str | None, Any]]:
    """Who last changed each stored setting, and when."""
    from provisa.api.app import state

    assert state.admin_db is not None, "the settings catalog needs the platform control plane"
    with state.admin_db.engine.connect() as conn:
        rows = conn.execute(select(_table.c.key, _table.c.updated_by, _table.c.updated_at))
        return {key: (by, at) for key, by, at in rows}


def _entry(s: Setting, audit: dict[str, tuple[str | None, Any]]) -> dict[str, Any]:
    by, at = audit.get(s.key, (None, None))
    return {
        **settings_registry.describe(s.key),
        "updated_by": by,
        "updated_at": at.isoformat() if hasattr(at, "isoformat") else at,
    }


@router.get("/admin/settings/catalog")
async def get_catalog(request: Request) -> dict[str, Any]:  # REQ-1913
    """Every operator setting, grouped into cards, with value, source and pending state."""
    require_deployment_settings(request)
    audit = _audit()
    by_card: dict[str, list[dict[str, Any]]] = {}
    for s in settings_registry.all_settings():
        by_card.setdefault(s.card, []).append(_entry(s, audit))
    return {
        "snapshot_ttl_seconds": deployment_settings.SNAPSHOT_TTL_SECONDS,
        "pending_restart": settings_registry.pending(),
        "cards": [
            {"id": card, "settings": by_card[card]}
            for card in settings_registry.CARDS
            if card in by_card
        ],
    }


def _body(body: Any) -> tuple[dict[str, Any], set[str]]:
    values = body.get("values") if isinstance(body, dict) else None
    confirm = body.get("confirm", []) if isinstance(body, dict) else None
    if not isinstance(values, dict) or not values or not isinstance(confirm, list):
        raise ApiError(
            400,
            "settings.invalid_body",
            'the body is {"values": {setting: value}, "confirm": [setting, ...]}',
        )
    return values, {str(key) for key in confirm}


def _check_guards(request: Request, values: dict[str, Any], confirmed: set[str]) -> None:
    """The guards on the settings that can lock an operator out or widen exposure (REQ-1913).

    Such a setting is refused for the anonymous caller of a deployment with no auth provider, and
    for anyone needs to be named in ``confirm`` — the UI's explicit confirmation.
    """
    for key in values:
        try:
            s = settings_registry.setting(key)
        except UnknownSetting:
            raise _refused(key, "unknown_setting") from None
        if s.guard is None:
            continue
        if is_anonymous(request):
            raise ApiError(
                403,
                "settings.auth_required",
                f"setting {key} can only be changed by an authenticated platform administrator",
                field=key,
                reason="auth_required",
            )
        if key not in confirmed:
            raise _refused(key, "confirmation_required")


@router.put("/admin/settings/catalog")
async def put_catalog(request: Request) -> dict[str, Any]:  # REQ-1913
    """Store settings. Every value is checked before any is stored; ``null`` clears a setting's
    stored value, returning it to the environment and the config file."""
    from provisa.api.app import state

    require_deployment_settings(request)
    values, confirmed = _body(await request.json())
    _check_guards(request, values, confirmed)
    assert state.admin_db is not None, "the settings catalog needs the platform control plane"
    identity = getattr(request.state, "identity", None)
    try:
        settings_registry.validate(values)
        settings_guards.check(values)
        settings_registry.store(
            state.admin_db, values, updated_by=getattr(identity, "user_id", _ANONYMOUS)
        )
    except SettingInvalid as err:
        raise _invalid(err) from None
    except settings_guards.Refused as err:
        raise _refused(err.field, err.reason, **err.params) from None
    return {"updated": list(values), "pending_restart": settings_registry.pending()}


@router.get(UI_SERVER_SETTINGS_PATH)
async def ui_server_settings() -> dict[str, Any]:  # REQ-1913
    """The operator settings the UI server runs on, each with the source it resolved from — the
    UI server applies its own environment between a stored value and the default."""
    resolved = settings_registry.resolve("ui.proxy_timeout")
    return {"proxy_timeout": {"value": resolved.value, "source": resolved.source}}
