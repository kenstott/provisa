# Copyright (c) 2026 Kenneth Stott
# Canary: ddc84598-6a7e-43fd-9d02-623ab5aac56a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for the Lemon Squeezy billing integration (REQ-1075).

Covers the governance-critical pure logic: variant→plan mapping and webhook
signature verification. No live Lemon Squeezy API is contacted.
"""

from __future__ import annotations

import hashlib
import hmac

import pytest

from provisa.api.billing import lemonsqueezy_client as ls
from provisa.api.billing.models import PLAN_LIMITS, plan_from_variant


class TestVariantMapping:
    @pytest.mark.parametrize(
        ("variant", "plan"),
        [
            ("Trial", "trial"),
            ("Starter Monthly", "starter"),
            ("PRO annual", "pro"),
        ],
    )
    def test_known_variants_map_to_plan(self, variant, plan):
        assert plan_from_variant(variant) == plan
        assert plan in PLAN_LIMITS

    def test_unknown_variant_raises(self):
        # An unrecognized variant is an error, never a silent default (REQ-1075).
        with pytest.raises(ValueError, match="Unrecognized Lemon Squeezy variant"):
            plan_from_variant("Gold")


class TestWebhookSignature:
    def test_valid_signature_accepted(self, monkeypatch):
        monkeypatch.setenv("LEMONSQUEEZY_SIGNING_SECRET", "s3cret")
        body = b'{"meta":{"event_name":"subscription_created"}}'
        sig = hmac.new(b"s3cret", body, hashlib.sha256).hexdigest()
        assert ls.verify_webhook_signature(body, sig) is True

    def test_wrong_signature_rejected(self, monkeypatch):
        monkeypatch.setenv("LEMONSQUEEZY_SIGNING_SECRET", "s3cret")
        assert ls.verify_webhook_signature(b'{"a":1}', "deadbeef") is False

    def test_empty_signature_rejected(self, monkeypatch):
        monkeypatch.setenv("LEMONSQUEEZY_SIGNING_SECRET", "s3cret")
        assert ls.verify_webhook_signature(b'{"a":1}', "") is False

    def test_body_tamper_rejected(self, monkeypatch):
        monkeypatch.setenv("LEMONSQUEEZY_SIGNING_SECRET", "s3cret")
        sig = hmac.new(b"s3cret", b"original", hashlib.sha256).hexdigest()
        assert ls.verify_webhook_signature(b"tampered", sig) is False
