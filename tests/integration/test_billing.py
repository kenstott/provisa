# Copyright (c) 2026 Kenneth Stott
# Canary: deba7605-e74e-43e6-b363-07fe87387131
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Integration tests for the /billing endpoints."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

os.environ.setdefault("STRIPE_BASE_URL", "http://localhost:12111")
os.environ.setdefault("STRIPE_API_KEY", "sk_test_any")
os.environ.setdefault("STRIPE_WEBHOOK_SECRET", "whsec_test")

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


def _make_app(pool: asyncpg.Pool) -> FastAPI:
    from provisa.api.billing.router import router as billing_router

    app = FastAPI()
    app.state.pg_pool = pool
    app.include_router(billing_router, prefix="/billing")
    return app


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def pool():
    pg_host = os.environ.get("PG_HOST", "localhost")
    pg_port = int(os.environ.get("PG_PORT", "5432"))
    pg_db = os.environ.get("PG_DATABASE", "provisa")
    pg_user = os.environ.get("PG_USER", "provisa")
    pg_password = os.environ.get("PG_PASSWORD", "provisa")

    p = await asyncpg.create_pool(
        host=pg_host,
        port=pg_port,
        database=pg_db,
        user=pg_user,
        password=pg_password,
        min_size=1,
        max_size=3,
    )
    from provisa.api.billing.tenant_db import init_billing_schema

    await init_billing_schema(p)
    yield p
    await p.close()


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client(pool):
    with patch("provisa.api.billing.router.create_tenant_key", new_callable=AsyncMock) as mock_key:
        mock_key.return_value = "arn:aws:kms:us-east-1:123456789012:key/test-key"
        app = _make_app(pool)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


def _stripe_sig(payload: bytes, secret: str) -> str:
    timestamp = int(time.time())
    signed = f"{timestamp}.{payload.decode()}"
    sig = hmac.new(secret.encode(), signed.encode(), hashlib.sha256).hexdigest()
    return f"t={timestamp},v1={sig}"


class TestSignup:
    async def test_signup_creates_tenant(self, client):
        with patch(
            "provisa.api.billing.router.create_tenant_key", new_callable=AsyncMock
        ) as mock_key:
            mock_key.return_value = "arn:aws:kms:us-east-1:123456789012:key/test-key"
            resp = await client.post("/billing/signup", json={"email": "test@example.com"})
        assert resp.status_code == 200
        data = resp.json()
        assert "tenant_id" in data
        assert data["plan"] == "trial"
        assert data["source_limit"] == 2
        uuid.UUID(data["tenant_id"])  # raises if invalid


class TestCheckout:
    async def test_checkout_creates_session(self, pool, client):
        with patch(
            "provisa.api.billing.router.create_tenant_key", new_callable=AsyncMock
        ) as mock_key:
            mock_key.return_value = "arn:aws:kms:us-east-1:123456789012:key/test-key"
            signup = await client.post("/billing/signup", json={"email": "checkout@example.com"})
        tenant_id = signup.json()["tenant_id"]

        mock_session = MagicMock()
        mock_session.url = "https://checkout.stripe.com/pay/cs_test_abc"
        mock_client = MagicMock()
        mock_client.checkout.sessions.create.return_value = mock_session

        with patch("provisa.api.billing.router.get_stripe_client", return_value=mock_client):
            resp = await client.post(
                "/billing/checkout",
                json={
                    "tenant_id": tenant_id,
                    "price_id": "price_test_starter",
                    "success_url": "http://example.com/success",
                    "cancel_url": "http://example.com/cancel",
                },
            )
        assert resp.status_code == 200
        assert "checkout_url" in resp.json()


class TestWebhook:
    async def test_webhook_upgrades_plan(self, pool, client):
        from provisa.api.billing.tenant_db import update_tenant_stripe_customer

        with patch(
            "provisa.api.billing.router.create_tenant_key", new_callable=AsyncMock
        ) as mock_key:
            mock_key.return_value = "arn:aws:kms:us-east-1:123456789012:key/test-key"
            signup = await client.post("/billing/signup", json={"email": "webhook@example.com"})
        tenant_id = signup.json()["tenant_id"]
        customer_id = f"cus_{uuid.uuid4().hex[:14]}"
        await update_tenant_stripe_customer(pool, tenant_id, customer_id)

        event_payload = json.dumps(
            {
                "type": "customer.subscription.updated",
                "data": {
                    "object": {
                        "customer": customer_id,
                        "items": {"data": [{"price": {"nickname": "Starter Plan"}}]},
                    }
                },
            }
        ).encode()

        sig = _stripe_sig(event_payload, "whsec_test")

        with patch("stripe.WebhookSignature.verify_header"):
            resp = await client.post(
                "/billing/webhook",
                content=event_payload,
                headers={"Stripe-Signature": sig, "Content-Type": "application/json"},
            )
        assert resp.status_code == 200

        from provisa.api.billing.tenant_db import get_tenant

        tenant = await get_tenant(pool, tenant_id)
        assert tenant is not None
        assert tenant.plan.value == "starter"
        assert tenant.source_limit == 10

    async def test_webhook_invalid_signature_rejected(self, client):
        event_payload = json.dumps(
            {"type": "customer.subscription.updated", "data": {"object": {}}}
        ).encode()
        resp = await client.post(
            "/billing/webhook",
            content=event_payload,
            headers={"Stripe-Signature": "t=0,v1=badsig", "Content-Type": "application/json"},
        )
        assert resp.status_code == 400


class TestStatus:
    async def test_status_returns_plan(self, pool, client):
        with patch(
            "provisa.api.billing.router.create_tenant_key", new_callable=AsyncMock
        ) as mock_key:
            mock_key.return_value = "arn:aws:kms:us-east-1:123456789012:key/test-key"
            signup = await client.post("/billing/signup", json={"email": "status@example.com"})
        tenant_id = signup.json()["tenant_id"]

        resp = await client.get(f"/billing/status?tenant_id={tenant_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["plan"] == "trial"
        assert data["source_limit"] == 2
        assert data["tenant_id"] == tenant_id
