# Copyright (c) 2026 Kenneth Stott
# Canary: 1b42cecf-76ea-4ad8-9d7c-a6c22ffcd4a2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1574: the org's key is a ring, it is never readable, and selection fails closed."""

from __future__ import annotations

import base64
import os

import pytest

from provisa.core.org_encryption import KEY_BYTES, OrgKeyError, decode_key, fingerprint
from provisa.encryption import (
    EnvelopeEncryption,
    NullEncryption,
    RingEnvelopeEncryption,
    bind_org_selector,
    configure_encryption,
    encryption_service,
    envelope_key_id,
    note_org_rings,
    org_encryption_loaded,
    reset_encryption,
    set_org_encryption,
)
from provisa.encryption.providers import LocalKeychain


@pytest.fixture(autouse=True)
def _reset():
    reset_encryption()
    yield
    reset_encryption()


def _ring(active: str, ids: list[str], *, v1: str | None = None) -> RingEnvelopeEncryption:
    return RingEnvelopeEncryption(
        active, {k: LocalKeychain(os.urandom(KEY_BYTES)) for k in ids}, v1_key_id=v1
    )


def test_blob_names_the_key_that_wrapped_it():
    ring = _ring("k1", ["k1"])
    blob = ring.encrypt(b"secret")
    assert envelope_key_id(blob) == "k1"
    assert ring.decrypt(blob) == b"secret"


def test_rotation_keeps_reading_what_the_retired_key_wrote():
    providers = {k: LocalKeychain(os.urandom(KEY_BYTES)) for k in ("k1", "k2")}
    old = RingEnvelopeEncryption("k1", providers)
    written_under_k1 = old.encrypt(b"before the rotation")
    rotated = RingEnvelopeEncryption("k2", providers)
    # Rotation changes what NEW writes use; it is not a re-encryption of what is already stored.
    assert rotated.decrypt(written_under_k1) == b"before the rotation"
    assert envelope_key_id(rotated.encrypt(b"after")) == "k2"


def test_a_blob_naming_a_key_the_ring_lacks_raises():
    stranger = _ring("k9", ["k9"])
    blob = stranger.encrypt(b"not yours")
    with pytest.raises(ValueError, match="does not hold"):
        _ring("k1", ["k1"]).decrypt(blob)


def test_v1_blob_is_opened_only_by_the_adopting_entry():
    deployment_key = os.urandom(KEY_BYTES)
    v1_blob = EnvelopeEncryption(LocalKeychain(deployment_key)).encrypt(b"written before the ring")
    assert envelope_key_id(v1_blob) is None
    adopting = RingEnvelopeEncryption("k1", {"k1": LocalKeychain(deployment_key)}, v1_key_id="k1")
    assert adopting.decrypt(v1_blob) == b"written before the ring"
    with pytest.raises(ValueError, match="adopts no unnamed blobs"):
        _ring("k1", ["k1"]).decrypt(v1_blob)


def test_active_key_must_be_in_the_ring():
    with pytest.raises(ValueError, match="not in the ring"):
        RingEnvelopeEncryption("k2", {"k1": LocalKeychain(os.urandom(KEY_BYTES))})


def test_fingerprint_is_short_and_not_the_key():
    raw = os.urandom(KEY_BYTES)
    fp = fingerprint(raw)
    assert len(fp) == 16
    assert base64.b64encode(raw).decode() not in fp
    assert fingerprint(raw) == fp  # stable, so an operator can recognise the key they set


def test_decode_key_refuses_anything_that_is_not_a_key():
    assert len(decode_key(base64.b64encode(os.urandom(KEY_BYTES)).decode())) == KEY_BYTES
    with pytest.raises(OrgKeyError, match="base64"):
        decode_key("not base64!!")
    with pytest.raises(OrgKeyError, match="32 bytes"):
        decode_key(base64.b64encode(os.urandom(16)).decode())


def test_bound_org_with_a_ring_is_served_that_ring():
    configure_encryption("null")
    ring = _ring("k1", ["k1"])
    set_org_encryption("acme", ring)
    bind_org_selector(lambda: "acme")
    assert encryption_service() is ring


def test_org_off_the_roster_gets_the_deployment_service():
    deployment = configure_encryption("null")
    bind_org_selector(lambda: "unkeyed-org")
    assert encryption_service() is deployment


def test_org_on_the_roster_without_a_loaded_ring_raises():
    configure_encryption("null")
    note_org_rings(["acme"])
    bind_org_selector(lambda: "acme")
    with pytest.raises(RuntimeError, match="has not loaded"):
        encryption_service()


def test_clearing_an_org_takes_it_off_the_roster():
    configure_encryption("null")
    note_org_rings(["acme"])
    set_org_encryption("acme", None)  # the org holds no key of its own after all
    bind_org_selector(lambda: "acme")
    assert isinstance(encryption_service(), NullEncryption)
    assert org_encryption_loaded("acme")


def test_no_bound_org_is_the_deployment_service():
    deployment = configure_encryption("null")
    set_org_encryption("acme", _ring("k1", ["k1"]))
    bind_org_selector(lambda: None)
    assert encryption_service() is deployment
