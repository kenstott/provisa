# Copyright (c) 2026 Kenneth Stott
# Canary: 9d007b30-d4e0-4118-bcec-3ef6a7e2e161
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""FastAPI billing router — /billing prefix.

Billing is provided by Lemon Squeezy as Merchant of Record (REQ-1075). Checkout goes
through the hosted Lemon Squeezy flow; plan lifecycle is driven by signed webhooks.

REQ-1355: every endpoint identifies the subject by ``org_id``. There is no separate tenant id.
"""

# Requirements: REQ-073, REQ-074, REQ-1075, REQ-1355

from __future__ import annotations

import json

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from provisa.api.billing.kms import create_org_key
from provisa.api.errors import ApiError
from provisa.api.billing.lemonsqueezy_client import (
    create_checkout,
    get_customer_portal_url,
    verify_webhook_signature,
)
from provisa.api.billing.models import PLAN_LIMITS, plan_from_variant
from provisa.api.billing.org_db import (
    get_org_billing,
    get_org_by_ls_customer,
    set_org_kms_key,
    update_org_ls_customer,
    update_org_plan,
)

router = APIRouter(tags=["billing"])


class SignupBody(BaseModel):
    org_id: str


class CheckoutBody(BaseModel):
    org_id: str
    variant_id: str
    redirect_url: str


def _pool(request: Request):  # pyright: ignore[reportUnusedParameter]
    # Platform control plane: the billing columns live on ``orgs``, which is a registry table.
    # This used to read ``request.app.state.tenant_db`` — the per-org data plane, which has never
    # held the billing tables.
    from provisa.api.app import state

    assert state.admin_db is not None
    return state.admin_db


@router.post("/signup")  # REQ-073, REQ-1355
async def signup(body: SignupBody, request: Request):
    """Initialize billing for an existing org: mint its KMS customer key.

    The org is created by ``/admin/orgs`` (which provisions its schema); this only attaches the
    billing facts. Rebinding an existing key would strand every DEK already wrapped under the old
    one, so a second call is rejected rather than silently rotating."""
    pool = _pool(request)
    org = await get_org_billing(pool, body.org_id)
    if org is None:
        raise ApiError(404, "billing.org_not_found", "Org not found")
    if org.kms_key_arn:
        raise ApiError(409, "billing.already_initialized", "Org billing is already initialized")
    key_arn = await create_org_key(body.org_id)
    await set_org_kms_key(pool, body.org_id, key_arn)
    return {
        "org_id": org.org_id,
        "plan": org.plan.value,
        "source_limit": org.source_limit,
    }


@router.post("/checkout")  # REQ-073, REQ-1075, REQ-1355
async def checkout(body: CheckoutBody, request: Request):
    pool = _pool(request)
    org = await get_org_billing(pool, body.org_id)
    if org is None:
        raise ApiError(404, "billing.org_not_found", "Org not found")
    url = await create_checkout(body.variant_id, body.org_id, body.redirect_url)
    return {"checkout_url": url}


# Lemon Squeezy subscription events → plan lifecycle (REQ-1075).
_ACTIVATE_EVENTS = {"subscription_created", "subscription_updated"}
_DEACTIVATE_EVENTS = {"subscription_cancelled", "subscription_expired"}


@router.post("/webhook")  # REQ-073, REQ-074, REQ-1075, REQ-1355
async def webhook(request: Request):
    payload = await request.body()
    sig = request.headers.get("X-Signature", "")
    if not verify_webhook_signature(payload, sig):
        raise ApiError(400, "billing.invalid_signature", "Invalid Lemon Squeezy signature")

    event = json.loads(payload)
    meta = event.get("meta") or {}
    event_name = meta.get("event_name", "")
    data = event.get("data") or {}
    attrs = data.get("attributes") or {}
    ls_customer_id = attrs.get("customer_id")
    if ls_customer_id is not None:
        ls_customer_id = str(ls_customer_id)
    ls_subscription_id = data.get("id")
    if ls_subscription_id is not None:
        ls_subscription_id = str(ls_subscription_id)

    pool = _pool(request)

    if event_name in _ACTIVATE_EVENTS:
        # org_id is carried in the checkout custom_data and echoed back in meta.custom_data.
        org_id = (meta.get("custom_data") or {}).get("org_id")
        if org_id is None and ls_customer_id is not None:
            resolved = await get_org_by_ls_customer(pool, ls_customer_id)
            org_id = resolved.org_id if resolved else None
        if org_id is None:
            raise ApiError(
                400, "billing.webhook_missing_org_linkage", "Webhook missing org linkage"
            )
        if ls_customer_id is not None:
            await update_org_ls_customer(pool, org_id, ls_customer_id, ls_subscription_id)
        plan_name = plan_from_variant(attrs.get("variant_name", ""))
        await update_org_plan(pool, org_id, plan_name, PLAN_LIMITS[plan_name])

    elif event_name in _DEACTIVATE_EVENTS:
        if ls_customer_id is None:
            raise ApiError(400, "billing.webhook_missing_customer_id", "Webhook missing customer id")
        org = await get_org_by_ls_customer(pool, ls_customer_id)
        if org:
            await update_org_plan(pool, org.org_id, "trial", PLAN_LIMITS["trial"])

    return JSONResponse(content={"received": True})


@router.get("/portal")  # REQ-073, REQ-074, REQ-1075, REQ-1355
async def portal(org_id: str, request: Request):
    pool = _pool(request)
    org = await get_org_billing(pool, org_id)
    if org is None:
        raise ApiError(404, "billing.org_not_found", "Org not found")
    if not org.ls_customer_id:
        raise ApiError(400, "billing.org_no_ls_customer", "Org has no Lemon Squeezy customer")
    url = await get_customer_portal_url(org.ls_customer_id)
    return {"portal_url": url}


@router.get("/status")  # REQ-073, REQ-074, REQ-1075, REQ-1355
async def status(org_id: str, request: Request):
    pool = _pool(request)
    org = await get_org_billing(pool, org_id)
    if org is None:
        raise ApiError(404, "billing.org_not_found", "Org not found")
    return {
        "org_id": org.org_id,
        "kms_key_arn": org.kms_key_arn,
        "ls_customer_id": org.ls_customer_id,
        "ls_subscription_id": org.ls_subscription_id,
        "plan": org.plan.value,
        "source_limit": org.source_limit,
        "created_at": org.created_at.isoformat(),
    }
