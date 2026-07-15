# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Client-side encryption / KMS unit tests (REQ-691, REQ-692, REQ-694)."""

from __future__ import annotations

import base64
import os
from unittest.mock import MagicMock, patch

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from provisa_client.encryption import (
    AwsKmsProvider,
    AzureKeyVaultProvider,
    ClientEncryptionService,
    DecryptionError,
    GcpKmsProvider,
    _DekCache,
    build_client_encryption,
    build_kms_provider,
    decrypt_rows,
)
from provisa_client.graphql_decrypt import (
    GraphQLDecryptClient,
    decrypt_response,
    encrypted_fields_from_sdl,
)


class FakeKmsClient:
    """boto3-KMS-shaped fake: wraps DEKs with a local master key (never a real network call)."""

    def __init__(self) -> None:
        self._master = os.urandom(32)
        self.decrypt_calls = 0

    def generate_data_key(self, *, KeyId: str, KeySpec: str) -> dict:  # noqa: N803
        dek = os.urandom(32)
        nonce = os.urandom(12)
        wrapped = nonce + AESGCM(self._master).encrypt(nonce, dek, None)
        return {"Plaintext": dek, "CiphertextBlob": wrapped}

    def decrypt(self, *, KeyId: str, CiphertextBlob: bytes) -> dict:  # noqa: N803
        self.decrypt_calls += 1
        nonce, ct = CiphertextBlob[:12], CiphertextBlob[12:]
        return {"Plaintext": AESGCM(self._master).decrypt(nonce, ct, None)}


def _svc(client: FakeKmsClient | None = None, ttl: float = 300.0) -> ClientEncryptionService:
    kms = client or FakeKmsClient()
    return build_client_encryption(
        kms_provider="aws", kms_key_arn="arn:aws:kms:...:key/abc", dek_cache_ttl=ttl, _client=kms
    )


# -- round-trip -------------------------------------------------------------------------------


def test_round_trip_encrypt_decrypt():
    svc = _svc()
    blob = svc.encrypt(b"secret-value")
    assert blob != b"secret-value"
    assert svc.decrypt(blob) == b"secret-value"


def test_decrypt_field_base64_string():
    svc = _svc()
    blob = svc.encrypt(b"pii@example.com")
    b64 = base64.b64encode(blob).decode()
    assert svc.decrypt_field(b64) == "pii@example.com"


def test_decrypt_field_none_passes_through():
    assert _svc().decrypt_field(None) is None


# -- DEK cache TTL (REQ-691) ------------------------------------------------------------------


def test_dek_cache_avoids_repeat_kms_calls():
    kms = FakeKmsClient()
    svc = _svc(kms, ttl=300.0)
    blob = svc.encrypt(b"x")  # generate_data_key, no decrypt yet
    svc.decrypt(blob)  # 1 unwrap
    svc.decrypt(blob)  # cache hit → no new unwrap
    svc.decrypt(blob)
    assert kms.decrypt_calls == 1


def test_dek_cache_expiry_forces_reunwrap():
    kms = FakeKmsClient()
    svc = _svc(kms, ttl=0.0)  # everything expires immediately
    blob = svc.encrypt(b"x")
    svc.decrypt(blob)
    svc.decrypt(blob)
    assert kms.decrypt_calls == 2


def test_dek_cache_unit_ttl():
    cache = _DekCache(10.0)
    cache.put(b"w", b"d" * 32, now=100.0)
    assert cache.get(b"w", now=105.0) == b"d" * 32
    assert cache.get(b"w", now=111.0) is None  # expired


# -- KMS provider abstraction never stores raw key (REQ-694) ----------------------------------


def test_providers_never_store_raw_key_material():
    for prov in (
        AwsKmsProvider("arn", client=FakeKmsClient()),
        AzureKeyVaultProvider("https://vault/keys/k", client=MagicMock()),
        GcpKmsProvider("projects/p/keys/k", client=MagicMock()),
    ):
        blob = vars(prov)
        # Only an identifier + client handle are held — no 32-byte key material.
        assert not any(isinstance(v, (bytes, bytearray)) and len(v) == 32 for v in blob.values())


def test_build_kms_provider_selects_impl():
    assert isinstance(build_kms_provider("aws", "arn", client=MagicMock()), AwsKmsProvider)
    assert isinstance(build_kms_provider("azure", "k", client=MagicMock()), AzureKeyVaultProvider)
    assert isinstance(build_kms_provider("gcp", "k", client=MagicMock()), GcpKmsProvider)


def test_build_kms_provider_unknown_fails_closed():
    with pytest.raises(ValueError, match="Unknown kms_provider"):
        build_kms_provider("hashicorp", "k")


def test_build_client_encryption_requires_both_params():
    assert build_client_encryption(kms_provider=None, kms_key_arn=None) is None
    with pytest.raises(ValueError, match="both kms_provider and kms_key_arn"):
        build_client_encryption(kms_provider="aws", kms_key_arn=None)


# -- decrypt failure fails loud ---------------------------------------------------------------


def test_decrypt_tampered_blob_fails_loud():
    svc = _svc()
    blob = bytearray(svc.encrypt(b"secret"))
    blob[-1] ^= 0xFF  # corrupt the GCM tag
    with pytest.raises(DecryptionError, match="authentication failed"):
        svc.decrypt(bytes(blob))


def test_decrypt_non_envelope_fails_loud():
    with pytest.raises(DecryptionError, match="not a provisa envelope"):
        _svc().decrypt(b"plaintext-not-an-envelope")


def test_revoked_grant_fails_loud():
    kms = FakeKmsClient()
    svc = _svc(kms)
    blob = svc.encrypt(b"secret")

    def _revoked(*, KeyId, CiphertextBlob):  # noqa: N803
        raise RuntimeError("AccessDeniedException: grant revoked")

    kms.decrypt = _revoked  # type: ignore[assignment]
    with pytest.raises(DecryptionError, match="grant revoked or key unavailable"):
        svc.decrypt(blob)


# -- column-flagged decrypt in the python client path (REQ-691) -------------------------------


def test_decrypt_rows_helper_decrypts_flagged_columns():
    svc = _svc()
    email = base64.b64encode(svc.encrypt(b"a@b.com")).decode()
    rows = [{"id": 1, "email": email}, {"id": 2, "email": None}]
    out = decrypt_rows(rows, ["email"], svc)
    assert out[0]["email"] == "a@b.com"
    assert out[0]["id"] == 1
    assert out[1]["email"] is None


def test_dbapi_client_decrypts_flagged_columns():
    from provisa_client import dbapi

    kms = FakeKmsClient()
    tmp_svc = _svc(kms)
    secret_b64 = base64.b64encode(tmp_svc.encrypt(b"555-1234")).decode()

    class _Resp:
        status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return {"data": [{"id": 1, "phone": secret_b64}], "encrypted_columns": ["phone"]}

    with patch.object(dbapi.httpx, "post", return_value=_Resp()):
        conn = dbapi.Connection(
            base_url="http://x",
            token=None,
            role="admin",
            encryption=build_client_encryption(kms_provider="aws", kms_key_arn="arn", _client=kms),
        )
        cur = conn.cursor()
        cur.execute("SELECT id, phone FROM t")
        assert cur.fetchall() == [(1, "555-1234")]


def test_dbapi_client_flagged_but_unconfigured_fails_loud():
    from provisa_client import dbapi

    class _Resp:
        status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return {"data": [{"id": 1, "phone": "AAAA"}], "encrypted_columns": ["phone"]}

    with patch.object(dbapi.httpx, "post", return_value=_Resp()):
        conn = dbapi.Connection(base_url="http://x", token=None, role="admin", encryption=None)
        cur = conn.cursor()
        with pytest.raises(dbapi.DataError, match="no kms_provider"):
            cur.execute("SELECT id, phone FROM t")


# -- @encrypted directive marks + wrapper decrypts (REQ-692) ----------------------------------


def test_encrypted_fields_from_sdl():
    sdl = """
type Employee {
  id: Int
  email: String @encrypted
  ssn: String @encrypted
  name: String
}
"""
    assert encrypted_fields_from_sdl(sdl) == {"email", "ssn"}


def test_decrypt_response_walks_nested():
    svc = _svc()
    enc = base64.b64encode(svc.encrypt(b"secret")).decode()
    data = {"employees": [{"name": "x", "ssn": enc}, {"name": "y", "ssn": None}]}
    out = decrypt_response(data, {"ssn"}, svc)
    assert out["employees"][0]["ssn"] == "secret"
    assert out["employees"][1]["ssn"] is None


def test_graphql_wrapper_decrypts_flagged_fields():
    kms = FakeKmsClient()
    tmp = _svc(kms)
    enc = base64.b64encode(tmp.encrypt(b"top-secret")).decode()

    sdl = "type Q {\n  secret: String @encrypted\n  plain: String\n}"

    class _Http:
        def get(self, url, headers=None):
            return _R(text=sdl)

        def post(self, url, json=None, headers=None):
            return _R(json_body={"data": {"secret": enc, "plain": "ok"}})

        def close(self):
            pass

    class _R:
        def __init__(self, text="", json_body=None):
            self.text = text
            self._j = json_body

        def raise_for_status(self):
            pass

        def json(self):
            return self._j

    client = GraphQLDecryptClient(
        url="http://x", kms_provider="aws", kms_key_arn="arn", _kms_client=kms, _http=_Http()
    )
    body = client.query("{ secret plain }")
    assert body["data"]["secret"] == "top-secret"
    assert body["data"]["plain"] == "ok"


def test_graphql_wrapper_flagged_but_unconfigured_fails_loud():
    sdl = "type Q {\n  secret: String @encrypted\n}"

    class _Http:
        def get(self, url, headers=None):
            return _R(text=sdl)

        def post(self, url, json=None, headers=None):
            return _R(json_body={"data": {"secret": "AAAA"}})

        def close(self):
            pass

    class _R:
        def __init__(self, text="", json_body=None):
            self.text = text
            self._j = json_body

        def raise_for_status(self):
            pass

        def json(self):
            return self._j

    client = GraphQLDecryptClient(url="http://x", _http=_Http())
    with pytest.raises(DecryptionError, match="no kms_provider"):
        client.query("{ secret }")
