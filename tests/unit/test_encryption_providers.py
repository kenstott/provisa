# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Extensible encryption-provider registry + cloud/custom providers.

Requirements: REQ-684, REQ-685, REQ-690, REQ-691, REQ-692, REQ-918.
"""

import os
from unittest.mock import MagicMock, patch

import pytest

from provisa.encryption import (
    EncryptionProviderSpec,
    build_encryption_service,
    register_encryption_provider,
)
from provisa.encryption.registry import encryption_provider_registry, get_provider_spec
from provisa.encryption.service import EncryptionService, NullEncryption


class TestRegistry:
    def test_builtins_present(self):
        keys = {s.key for s in encryption_provider_registry()}
        assert {"null", "local", "aws_kms", "hashicorp_vault", "azure_key_vault"} <= keys

    def test_aliases_resolve(self):
        assert get_provider_spec("none").key == "null"
        assert get_provider_spec("passthrough").key == "null"
        assert get_provider_spec("keychain").key == "local"

    def test_unknown_provider_fails_closed(self):
        with pytest.raises(ValueError, match="Unknown encryption provider"):
            build_encryption_service("no-such-kms")

    def test_unavailable_provider_fails_closed(self):
        # Register a provider whose runtime probe is False — independent of installed SDKs.
        register_encryption_provider(
            EncryptionProviderSpec(
                key="unit_unavailable",
                label="Unavailable",
                description="test",
                available=lambda: False,
                build=lambda cfg, key_id, ttl: NullEncryption(),
            )
        )
        with pytest.raises(ValueError, match="not available"):
            build_encryption_service("unit_unavailable")

    def test_register_custom_provider(self):
        register_encryption_provider(
            EncryptionProviderSpec(
                key="unit_custom",
                label="Unit Custom",
                description="test",
                build=lambda cfg, key_id, ttl: NullEncryption(),
            )
        )
        assert isinstance(build_encryption_service("unit_custom"), NullEncryption)


class TestExtensionLoading:
    def test_env_module_is_imported(self, tmp_path, monkeypatch):
        # A module that registers a provider on import, loaded via the env hook.
        mod = tmp_path / "acme_provider_ext.py"
        mod.write_text(
            "from provisa.encryption import register_encryption_provider, EncryptionProviderSpec\n"
            "from provisa.encryption.service import NullEncryption\n"
            "register_encryption_provider(EncryptionProviderSpec(\n"
            "    key='acme_ext', label='Acme', description='ext',\n"
            "    build=lambda cfg, key_id, ttl: NullEncryption()))\n"
        )
        monkeypatch.syspath_prepend(str(tmp_path))
        monkeypatch.setenv("PROVISA_ENCRYPTION_PROVIDER_MODULES", "acme_provider_ext")
        # Force a fresh extension load.
        import provisa.encryption.registry as reg

        monkeypatch.setattr(reg, "_EXTENSIONS_LOADED", False)
        assert get_provider_spec("acme_ext") is not None


class TestAwsKms:
    def test_wrap_unwrap_roundtrips_through_kms_client(self):
        from provisa.encryption.providers import AwsKmsMasterKey

        fake = MagicMock()
        fake.encrypt.return_value = {"CiphertextBlob": b"WRAPPED"}
        fake.decrypt.return_value = {"Plaintext": b"DEK-BYTES"}
        with patch("boto3.client", return_value=fake) as mk:
            p = AwsKmsMasterKey("arn:aws:kms:us-east-1:1:key/x", region="us-east-1")
            assert p.wrap_dek(b"DEK-BYTES") == b"WRAPPED"
            assert p.unwrap_dek(b"WRAPPED") == b"DEK-BYTES"
        mk.assert_called_once()
        fake.encrypt.assert_called_once_with(
            KeyId="arn:aws:kms:us-east-1:1:key/x", Plaintext=b"DEK-BYTES"
        )
        fake.decrypt.assert_called_once_with(
            CiphertextBlob=b"WRAPPED", KeyId="arn:aws:kms:us-east-1:1:key/x"
        )

    def test_custom_endpoint_url_is_passed(self):
        from provisa.encryption.providers import AwsKmsMasterKey

        with patch("boto3.client", return_value=MagicMock()) as mk:
            AwsKmsMasterKey("arn:x", endpoint_url="https://kms.internal:4599")
        assert mk.call_args.kwargs["endpoint_url"] == "https://kms.internal:4599"

    def test_missing_key_arn_raises(self):
        from provisa.encryption.providers import AwsKmsMasterKey

        with pytest.raises(ValueError, match="key_arn"):
            AwsKmsMasterKey("")

    def test_factory_builds_envelope_service_for_aws_kms(self):
        with patch("boto3.client", return_value=MagicMock()):
            svc = build_encryption_service(
                "aws_kms", config={"key_arn": "arn:x", "region": "us-east-1"}
            )
        assert isinstance(svc, EncryptionService)
        assert not isinstance(svc, NullEncryption)  # real envelope path


def teardown_module(_mod):
    # Drop test-registered providers so registry state doesn't leak across modules.
    import provisa.encryption.registry as reg

    for k in ("unit_custom", "acme_ext", "unit_unavailable"):
        reg._REGISTRY.pop(k, None)
    os.environ.pop("PROVISA_ENCRYPTION_PROVIDER_MODULES", None)
