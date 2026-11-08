# Copyright (c) 2026 Kenneth Stott
# Canary: 9a3e6c51-7b28-4d0f-8e14-c2f5a7d9b036
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The org's request delivery setting (REQ-2216): how the members a request is addressed to are
told of it -- ``email`` (the default), ``events`` (Provisa sends no notice; a workflow tool
subscribes to ops.requests) or ``both``. Held under ``org_settings``, the org administrators'
right, as every org-owned setting is."""

# Requirements: REQ-2216

from __future__ import annotations

from fastapi import APIRouter, Request
from pydantic import BaseModel

from provisa.api.admin._guards import require_active_org_id
from provisa.api.admin._platform_guard import require_org_settings
from provisa.api.admin.request_queue import DELIVERY_MODES, SETTINGS_KEY, delivery_of
from provisa.api.errors import ApiError

router = APIRouter()


def _model_db():
    """The org's model store, where its settings are kept, or a 503."""
    from provisa.api.app import state

    if state.model_db is None:
        raise ApiError(503, "requests.database_not_connected", "Database not connected")
    return state.model_db


class DeliveryBody(BaseModel):
    delivery: str


@router.get("/admin/settings/request-delivery")
async def get_request_delivery(request: Request) -> dict:  # REQ-2216
    """The org's delivery mode and the modes it may pick."""
    require_org_settings(request)
    require_active_org_id(request)
    from provisa.core.org_settings import resolve_org_config

    return {
        "delivery": delivery_of(await resolve_org_config(_model_db())),
        "modes": list(DELIVERY_MODES),
    }


@router.put("/admin/settings/request-delivery")
async def set_request_delivery(request: Request, body: DeliveryBody) -> dict:  # REQ-2216
    """Keep the org's delivery mode; a mode outside the three is refused by name."""
    require_org_settings(request)
    require_active_org_id(request)
    if body.delivery not in DELIVERY_MODES:
        raise ApiError(
            400,
            "requests.delivery_unknown",
            f"Delivery {body.delivery!r} is not one of {', '.join(DELIVERY_MODES)}",
            delivery=body.delivery,
        )
    from provisa.core.org_settings import write_org_overrides

    identity = getattr(request.state, "identity", None)
    await write_org_overrides(
        _model_db(),
        {SETTINGS_KEY: {"delivery": body.delivery}},
        updated_by=getattr(identity, "user_id", None) or "anonymous",
    )
    return {"delivery": body.delivery}
