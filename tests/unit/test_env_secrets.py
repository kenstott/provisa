# Copyright (c) 2026 Kenneth Stott
# Canary: 7e2b9c04-1af5-4d63-8b27-c05f3a91d6e8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A credential typed into a carried field is refused at the write (REQ-1525)."""

# Requirements: REQ-1489, REQ-1491, REQ-1524, REQ-1525

from __future__ import annotations

import pytest
from sqlalchemy import select

from provisa.core.env_secrets import (
    REFERENCE_FORM,
    CredentialLiteralError,
    check_row,
    find_credential,
    guard_statement,
)
from provisa.core.schema_org import metrics, sources, tracked_webhooks

GITHUB = "ghp_" + "a" * 36
AWS = "AKIAIOSFODNN7EXAMPLE"


class TestWhatCountsAsACredential:
    @pytest.mark.parametrize(
        "value,reason",
        [
            (f"https://hooks.example/x?token={AWS}", "an AWS access key id"),
            (f"https://hooks.example/x?token={GITHUB}", "a GitHub token"),
            ("xoxb-1234567890-abcdefghij", "a Slack token"),
            ("sk_live_" + "b" * 24, "a Stripe secret key"),
            ("AIza" + "c" * 35, "a Google API key"),
            ("-----BEGIN RSA PRIVATE KEY-----\nMIIE", "a private key"),
            ("postgres://svc:hunter2@db.internal/sales", "a password in a URL"),
        ],
    )
    def test_a_literal_is_named_for_what_it_is(self, value, reason):
        assert find_credential(value) == reason

    @pytest.mark.parametrize(
        "value",
        [
            "SUM(o.total) / NULLIF(COUNT(DISTINCT o.customer_id), 0)",
            "https://hooks.example/services/T00000/B00000/abcdefghijkl",
            "postgres://readonly@db.internal/sales",
            "the quarterly revenue recognised at contract signature",
            "",
            None,
            42,
            {"nested": AWS},
        ],
    )
    def test_a_legitimate_value_is_not_refused(self, value):
        """Entropy is not the test: an identifier and a key look the same to one, and a false
        refusal teaches authors to route around the check."""
        assert find_credential(value) is None


class TestTheReferenceFormIsTheWayThrough:
    def test_a_reference_in_a_url_is_accepted(self):
        assert find_credential("postgres://svc:${env:PGPASSWORD}@db.internal/sales") is None

    def test_the_refusal_shows_the_form_to_use(self):
        with pytest.raises(CredentialLiteralError) as excinfo:
            check_row("tracked_webhooks", {"url": f"https://x/?t={GITHUB}"})
        assert REFERENCE_FORM in str(excinfo.value)

    def test_the_refusal_names_the_field(self):
        with pytest.raises(CredentialLiteralError) as excinfo:
            check_row("metrics", {"expression": f"http_get('https://x/?t={AWS}')"})
        assert excinfo.value.table == "metrics"
        assert excinfo.value.column == "expression"


class TestOnlyCarriedFieldsAreScanned:
    def test_a_binding_may_hold_a_credential(self):
        """A source's connection values are a BINDING (REQ-1491), which is where a credential is
        supposed to be and which no copy and no commit ever carries."""
        check_row("sources", {"password": "hunter2", "host": "db.internal"})

    def test_a_write_to_a_binding_table_passes_the_seam(self):
        assert guard_statement(sources.insert().values(id="s", password="hunter2")) is not None


class TestTheSeam:
    def test_an_insert_carrying_a_credential_is_refused(self):
        with pytest.raises(CredentialLiteralError):
            guard_statement(tracked_webhooks.insert().values(url=f"https://x/?t={GITHUB}"))

    def test_an_update_carrying_a_credential_is_refused(self):
        with pytest.raises(CredentialLiteralError):
            guard_statement(metrics.update().values(expression=f"f('{AWS}')"))

    def test_a_clean_write_is_returned_unchanged(self):
        stmt = tracked_webhooks.insert().values(url="https://hooks.example/x")
        assert guard_statement(stmt) is stmt

    def test_a_select_is_never_scanned(self):
        stmt = select(tracked_webhooks)
        assert guard_statement(stmt) is stmt
