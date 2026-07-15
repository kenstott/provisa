# Copyright (c) 2026 Kenneth Stott
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""FastAPI billing router — /billing prefix.

Billing is provided by Lemon Squeezy as Merchant of Record (REQ-1015). Checkout goes
through the hosted Lemon Squeezy flow; plan lifecycle is driven by signed webhooks.
"""

# Requirements: REQ-073, REQ-074, REQ-1015

from __future__ import annotations

import json
import uuid

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from provisa.api.billing.kms import create_tenant_key
from provisa.api.billing.lemonsqueezy_client import (
    create_checkout,
    get_customer_portal_url,
    verify_webhook_signature,
)
from provisa.api.billing.models import PLAN_LIMITS, plan_from_variant
from provisa.api.billing.tenant_db import (
    create_tenant,
    get_tenant,
    get_tenant_by_ls_customer,
    update_tenant_ls_customer,
    update_tenant_plan,
)

router = APIRouter(tags=["billing"])


class SignupBody(BaseModel):
    email: str


class CheckoutBody(BaseModel):
    tenant_id: str
    variant_id: str
    redirect_url: str


def _pool(request: Request):
    return request.app.state.tenant_db


@router.post("/signup")  # REQ-073
async def signup(_body: SignupBody, request: Request):
    pool = _pool(request)
    temp_id = str(uuid.uuid4())
    key_arn = await create_tenant_key(temp_id)
    tenant = await create_tenant(pool, key_arn)
    return {
        "tenant_id": str(tenant.id),
        "plan": tenant.plan.value,
        "source_limit": tenant.source_limit,
    }


@router.post("/checkout")  # REQ-073, REQ-1015
async def checkout(body: CheckoutBody, request: Request):
    pool = _pool(request)
    tenant = await get_tenant(pool, body.tenant_id)
    if tenant is None:
        raise HTTPException(status_code=404, detail="Tenant not found")
    url = await create_checkout(body.variant_id, body.tenant_id, body.redirect_url)
    return {"checkout_url": url}


# Lemon Squeezy subscription events → plan lifecycle (REQ-1015).
_ACTIVATE_EVENTS = {"subscription_created", "subscription_updated"}
_DEACTIVATE_EVENTS = {"subscription_cancelled", "subscription_expired"}


@router.post("/webhook")  # REQ-073, REQ-074, REQ-1015
async def webhook(request: Request):
    payload = await request.body()
    sig = request.headers.get("X-Signature", "")
    if not verify_webhook_signature(payload, sig):
        raise HTTPException(status_code=400, detail="Invalid Lemon Squeezy signature")

    event = json.loads(payload)
    meta = event.get("meta") or {}
    event_name = meta.get("event_name", "")
    attrs = (event.get("data") or {}).get("attributes") or {}
    ls_customer_id = attrs.get("customer_id")
    if ls_customer_id is not None:
        ls_customer_id = str(ls_customer_id)

    pool = _pool(request)

    if event_name in _ACTIVATE_EVENTS:
        # tenant_id is carried in the checkout custom_data and echoed back in meta.custom_data.
        tenant_id = (meta.get("custom_data") or {}).get("tenant_id")
        if tenant_id is None and ls_customer_id is not None:
            resolved = await get_tenant_by_ls_customer(pool, ls_customer_id)
            tenant_id = str(resolved.id) if resolved else None
        if tenant_id is None:
            raise HTTPException(status_code=400, detail="Webhook missing tenant linkage")
        if ls_customer_id is not None:
            await update_tenant_ls_customer(pool, tenant_id, ls_customer_id)
        plan_name = plan_from_variant(attrs.get("variant_name", ""))
        await update_tenant_plan(pool, tenant_id, plan_name, PLAN_LIMITS[plan_name])

    elif event_name in _DEACTIVATE_EVENTS:
        if ls_customer_id is None:
            raise HTTPException(status_code=400, detail="Webhook missing customer id")
        tenant = await get_tenant_by_ls_customer(pool, ls_customer_id)
        if tenant:
            await update_tenant_plan(pool, str(tenant.id), "trial", PLAN_LIMITS["trial"])

    return JSONResponse(content={"received": True})


@router.get("/portal")  # REQ-073, REQ-074, REQ-1015
async def portal(tenant_id: str, request: Request):
    pool = _pool(request)
    tenant = await get_tenant(pool, tenant_id)
    if tenant is None:
        raise HTTPException(status_code=404, detail="Tenant not found")
    if not tenant.ls_customer_id:
        raise HTTPException(status_code=400, detail="Tenant has no Lemon Squeezy customer")
    url = await get_customer_portal_url(tenant.ls_customer_id)
    return {"portal_url": url}


@router.get("/status")  # REQ-073, REQ-074, REQ-1015
async def status(tenant_id: str, request: Request):
    pool = _pool(request)
    tenant = await get_tenant(pool, tenant_id)
    if tenant is None:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return {
        "tenant_id": str(tenant.id),
        "kms_key_arn": tenant.kms_key_arn,
        "ls_customer_id": tenant.ls_customer_id,
        "plan": tenant.plan.value,
        "source_limit": tenant.source_limit,
        "created_at": tenant.created_at.isoformat(),
    }
