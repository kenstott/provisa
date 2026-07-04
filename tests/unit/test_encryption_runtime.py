# Copyright (c) 2026 Kenneth Stott
# Canary: 2e4a6c8b-0d1f-4952-8a3c-5b7d9f0e1a2c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-684/686: the process-configured EncryptionService accessor."""

from __future__ import annotations

import base64
import os

import pytest

from provisa.encryption import (
    EnvelopeEncryption,
    NullEncryption,
    configure_encryption,
    encryption_service,
    reset_encryption,
)


@pytest.fixture(autouse=True)
def _reset():
    reset_encryption()
    yield
    reset_encryption()


def test_default_is_null_passthrough():
    assert isinstance(encryption_service(), NullEncryption)


def test_configure_null_provider():
    configure_encryption("null")
    assert isinstance(encryption_service(), NullEncryption)


def test_configure_local_provider(monkeypatch):
    monkeypatch.setenv("PROVISA_ENCRYPTION_KEY", base64.b64encode(os.urandom(32)).decode())
    svc = configure_encryption("local")
    assert isinstance(svc, EnvelopeEncryption)
    assert encryption_service() is svc
    assert svc.decrypt(svc.encrypt(b"x")) == b"x"


def test_reset_returns_to_null():
    configure_encryption("null")
    reset_encryption()
    assert isinstance(encryption_service(), NullEncryption)
