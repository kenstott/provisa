# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1575: the deployment's own secrets go in and never come back out."""

from __future__ import annotations

from provisa.api.admin.secret_redaction import (
    redact,
    redact_per_provider,
    redact_url_password,
    restore_url_password,
    secret_keys,
)

FIELDS = [
    {"config_key": "host", "secret": False},
    {"config_key": "token", "secret": True},
    {"config_key": "password", "secret": True},
]


def test_a_secret_field_is_absent_not_masked():
    safe, is_set = redact({"host": "vault.internal", "token": "s3cr3t", "password": ""}, FIELDS)
    assert safe == {"host": "vault.internal"}
    # No mask either: a row of asterisks is still a claim about the value, and a form that posts it
    # back writes the mask over the real credential.
    assert "s3cr3t" not in str(safe)
    assert is_set == {"password": False, "token": True}


def test_secret_keys_reads_the_registry():
    assert secret_keys(FIELDS) == {"token", "password"}


def test_per_provider_redaction_keeps_providers_apart():
    specs = [
        {"key": "vault", "config_fields": FIELDS},
        {"key": "aws", "config_fields": [{"config_key": "region", "secret": False}]},
    ]
    safe, is_set = redact_per_provider(
        {"vault": {"host": "h", "token": "t"}, "aws": {"region": "us-east-1"}}, specs
    )
    assert safe == {"vault": {"host": "h"}, "aws": {"region": "us-east-1"}}
    assert is_set == {"vault": {"password": False, "token": True}, "aws": {}}


def test_url_password_is_stripped_and_the_rest_survives():
    out = redact_url_password("postgresql://svc:hunter2@db.internal:5432/provisa?ssl=require")
    assert out == "postgresql://svc@db.internal:5432/provisa?ssl=require"
    assert "hunter2" not in out


def test_a_url_without_a_password_is_untouched():
    for url in ("postgresql://db.internal/provisa", "https://vault.internal:8200", None, ""):
        assert redact_url_password(url) == url


def test_the_same_address_without_a_password_keeps_the_stored_one():
    stored = "postgresql://svc:hunter2@db.internal:5432/provisa"
    submitted = redact_url_password(stored)
    assert restore_url_password(submitted, stored) == stored


def test_a_submitted_password_wins():
    stored = "postgresql://svc:hunter2@db.internal:5432/provisa"
    submitted = "postgresql://svc:newpass@db.internal:5432/provisa"
    assert restore_url_password(submitted, stored) == submitted


def test_a_changed_address_does_not_inherit_the_old_password():
    stored = "postgresql://svc:hunter2@db.internal:5432/provisa"
    # Pointing the DSN somewhere else must not send the old host's credential to the new one.
    for moved in (
        "postgresql://svc@other.internal:5432/provisa",
        "postgresql://svc@db.internal:5433/provisa",
        "postgresql://other@db.internal:5432/provisa",
        "postgresql://svc@db.internal:5432/other",
    ):
        assert restore_url_password(moved, stored) == moved


def test_nothing_to_restore_when_nothing_is_stored():
    assert restore_url_password("postgresql://db/x", None) == "postgresql://db/x"
    assert restore_url_password("", "postgresql://svc:p@db/x") == ""
