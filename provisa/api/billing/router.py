# Copyright (c) 2026 Kenneth Stott
# Canary: 5b3532f4-1f8a-4652-926e-f0be2b2adc4f
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""FastAPI billing router — /billing prefix."""

# Requirements: REQ-073, REQ-074

from __future__ import annotations

import json
import os
import uuid

import stripe
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from provisa.api.billing.kms import create_tenant_key
from provisa.api.billing.models import PLAN_LIMITS
from provisa.api.billing.stripe_client import get_stripe_client
from provisa.api.billing.tenant_db import (
    create_tenant,
    get_tenant,
    get_tenant_by_stripe_customer,
    update_tenant_plan,
    update_tenant_stripe_customer,
)

router = APIRouter(tags=["billing"])


class SignupBody(BaseModel):
    email: str


class CheckoutBody(BaseModel):
    tenant_id: str
    price_id: str
    success_url: str
    cancel_url: str


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


@router.post("/checkout")  # REQ-073
async def checkout(body: CheckoutBody, request: Request):
    pool = _pool(request)
    tenant = await get_tenant(pool, body.tenant_id)
    if tenant is None:
        raise HTTPException(status_code=404, detail="Tenant not found")
    client = get_stripe_client()
    session = client.v1.checkout.sessions.create(
        params={
            "mode": "subscription",
            "line_items": [{"price": body.price_id, "quantity": 1}],
            "success_url": body.success_url,
            "cancel_url": body.cancel_url,
            "metadata": {"tenant_id": body.tenant_id},
        }
    )
    return {"checkout_url": session.url}


@router.post("/webhook")  # REQ-073, REQ-074
async def webhook(request: Request):
    payload = await request.body()
    sig = request.headers.get("Stripe-Signature", "")
    secret = os.environ["STRIPE_WEBHOOK_SECRET"]
    try:
        stripe.WebhookSignature.verify_header(payload, sig, secret)
    except stripe.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Invalid Stripe signature")

    event = json.loads(payload)
    event_type = event.get("type", "")
    obj = event.get("data", {}).get("object", {})

    pool = _pool(request)

    if event_type == "checkout.session.completed":
        tenant_id = (obj.get("metadata") or {}).get("tenant_id")
        customer_id = obj.get("customer")
        if tenant_id and customer_id:
            await update_tenant_stripe_customer(pool, tenant_id, customer_id)

    elif event_type == "customer.subscription.updated":
        customer_id = obj.get("customer")
        tenant = await get_tenant_by_stripe_customer(pool, customer_id)
        if tenant:
            items = (obj.get("items") or {}).get("data", [])
            price = items[0].get("price", {}) if items else {}
            nickname = (price.get("nickname") or "").lower()
            plan_name = next((p for p in ("trial", "starter", "pro") if p in nickname), None)
            if plan_name is None:
                raise HTTPException(
                    status_code=400, detail=f"Unrecognized Stripe price nickname: {nickname!r}"
                )
            source_limit = PLAN_LIMITS[plan_name]
            await update_tenant_plan(pool, str(tenant.id), plan_name, source_limit)

    elif event_type == "customer.subscription.deleted":
        customer_id = obj.get("customer")
        tenant = await get_tenant_by_stripe_customer(pool, customer_id)
        if tenant:
            await update_tenant_plan(pool, str(tenant.id), "trial", PLAN_LIMITS["trial"])

    return JSONResponse(content={"received": True})


@router.get("/portal")  # REQ-073, REQ-074
async def portal(tenant_id: str, request: Request):
    pool = _pool(request)
    tenant = await get_tenant(pool, tenant_id)
    if tenant is None:
        raise HTTPException(status_code=404, detail="Tenant not found")
    if not tenant.stripe_customer_id:
        raise HTTPException(status_code=400, detail="Tenant has no Stripe customer")
    client = get_stripe_client()
    session = client.v1.billing_portal.sessions.create(
        params={"customer": tenant.stripe_customer_id}
    )
    return {"portal_url": session.url}


@router.get("/status")  # REQ-073, REQ-074
async def status(tenant_id: str, request: Request):
    pool = _pool(request)
    tenant = await get_tenant(pool, tenant_id)
    if tenant is None:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return {
        "tenant_id": str(tenant.id),
        "kms_key_arn": tenant.kms_key_arn,
        "stripe_customer_id": tenant.stripe_customer_id,
        "plan": tenant.plan.value,
        "source_limit": tenant.source_limit,
        "created_at": tenant.created_at.isoformat(),
    }
