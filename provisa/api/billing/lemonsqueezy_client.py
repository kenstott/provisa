# Copyright (c) 2026 Kenneth Stott
# Canary: c96309de-dc39-4538-b687-09a16f2f47d5
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Lemon Squeezy REST client (REQ-1075).

Lemon Squeezy acts as Merchant of Record; Provisa integrates over the REST API
(JSON:API) and signed webhooks. No vendor SDK — a thin httpx wrapper. Env is the
only source of credentials: ``LEMONSQUEEZY_API_KEY``, ``LEMONSQUEEZY_STORE_ID``,
``LEMONSQUEEZY_SIGNING_SECRET``. ``LEMONSQUEEZY_BASE_URL`` overrides the endpoint
for tests; production uses the public API.
"""

from __future__ import annotations

import hashlib
import hmac
import os

import httpx

_DEFAULT_API = "https://api.lemonsqueezy.com/v1"
_JSONAPI = "application/vnd.api+json"


def _base_url() -> str:
    return os.environ.get("LEMONSQUEEZY_BASE_URL", _DEFAULT_API).rstrip("/")


def _headers() -> dict[str, str]:
    api_key = os.environ["LEMONSQUEEZY_API_KEY"]
    return {
        "Authorization": f"Bearer {api_key}",
        "Accept": _JSONAPI,
        "Content-Type": _JSONAPI,
    }


async def create_checkout(variant_id: str, tenant_id: str, redirect_url: str) -> str:  # REQ-1075
    """Create a Lemon Squeezy checkout for ``variant_id`` and return its hosted URL.

    ``tenant_id`` is carried in checkout ``custom_data`` so the subscription webhook can
    resolve the tenant. Returns ``checkout.data.attributes.url``.
    """
    store_id = os.environ["LEMONSQUEEZY_STORE_ID"]
    body = {
        "data": {
            "type": "checkouts",
            "attributes": {
                "checkout_data": {"custom": {"tenant_id": tenant_id}},
                "product_options": {"redirect_url": redirect_url},
            },
            "relationships": {
                "store": {"data": {"type": "stores", "id": str(store_id)}},
                "variant": {"data": {"type": "variants", "id": str(variant_id)}},
            },
        }
    }
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{_base_url()}/checkouts", json=body, headers=_headers())
        resp.raise_for_status()
        return resp.json()["data"]["attributes"]["url"]


async def get_customer_portal_url(customer_id: str) -> str:  # REQ-1075
    """Return the Lemon Squeezy-hosted customer portal URL from the customer object
    (``data.attributes.urls.customer_portal``)."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{_base_url()}/customers/{customer_id}", headers=_headers())
        resp.raise_for_status()
        return resp.json()["data"]["attributes"]["urls"]["customer_portal"]


def verify_webhook_signature(raw_body: bytes, signature: str) -> bool:  # REQ-1075, REQ-074
    """Verify a Lemon Squeezy webhook: HMAC-SHA256 over the RAW request body keyed by
    ``LEMONSQUEEZY_SIGNING_SECRET``, compared to the hex ``X-Signature`` header in constant time."""
    secret = os.environ["LEMONSQUEEZY_SIGNING_SECRET"].encode()
    expected = hmac.new(secret, raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature or "")
