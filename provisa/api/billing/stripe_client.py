# Copyright (c) 2026 Kenneth Stott
# Canary: 8060e459-2871-4ed5-8aed-18da31ad599f
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Stripe client factory."""

from __future__ import annotations

import os

import stripe


def get_stripe_client() -> stripe.StripeClient:
    api_key = os.environ["STRIPE_API_KEY"]
    base_url = os.environ.get("STRIPE_BASE_URL")
    if base_url:
        return stripe.StripeClient(api_key, base_url=base_url)
    return stripe.StripeClient(api_key)
