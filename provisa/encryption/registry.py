# Copyright (c) 2026 Kenneth Stott
# Canary: 679a69e8-7365-409f-9f53-112e97b498ae
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Extensible encryption-provider registry (REQ-684, REQ-690-694).

A provider is a named spec: UI metadata (label, description, config_fields), an
``available()`` probe (its SDK/runtime is importable), and a ``build`` callable
that turns the persisted per-provider config into an EncryptionService. The
built-in providers register at import; enterprises add their own — including a
fully custom KMS/HSM endpoint — by calling ``register_encryption_provider`` from
a module named in ``PROVISA_ENCRYPTION_PROVIDER_MODULES`` or exposed via the
``provisa.encryption_providers`` entry-point group. No core edit required.

Selection is fail-closed: an unknown or unavailable provider raises rather than
silently degrading to plaintext (see ``factory.build_encryption_service``).
"""

# complexity-gate: allow-ble=2 reason="entry-point extension loading must not let a broken
# third-party encryption plugin brick startup — a failed ep.load() / metadata read is swallowed
# so a bad plugin is skipped, never fatal (env-listed modules still raise, being explicit operator config)"

from __future__ import annotations

import importlib
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from importlib.util import find_spec

from provisa.encryption.service import EncryptionService

# (config: per-provider dict, key_id, dek_cache_ttl) -> EncryptionService
ProviderBuilder = Callable[[dict, str | None, float], EncryptionService]


@dataclass(frozen=True)
class EncryptionProviderSpec:
    key: str
    label: str
    description: str
    build: ProviderBuilder
    # UI field descriptors (config_key/label/type/required/secret/placeholder), like /admin/auth.
    config_fields: list[dict] = field(default_factory=list)
    # Runtime availability probe (default: always). Cloud providers probe their SDK import.
    available: Callable[[], bool] = lambda: True
    # Alternate names that resolve to this spec (e.g. "none"/"passthrough" → "null").
    aliases: tuple[str, ...] = ()


_REGISTRY: dict[str, EncryptionProviderSpec] = {}
_ALIASES: dict[str, str] = {}
_EXTENSIONS_LOADED = False


def register_encryption_provider(spec: EncryptionProviderSpec) -> None:
    """Register (or replace) a provider spec. Public extension API."""
    _REGISTRY[spec.key] = spec
    for a in spec.aliases:
        _ALIASES[a.lower()] = spec.key


def get_provider_spec(name: str | None) -> EncryptionProviderSpec | None:
    """Resolve a provider name (or alias) to its spec, or None if unregistered."""
    load_provider_extensions()
    key = (name or "null").lower()
    key = _ALIASES.get(key, key)
    return _REGISTRY.get(key)


def encryption_provider_registry() -> list[EncryptionProviderSpec]:
    """All registered provider specs (built-ins + extensions), stable order."""
    load_provider_extensions()
    return list(_REGISTRY.values())


def _importable(module: str) -> Callable[[], bool]:
    """An availability probe that is True when ``module`` can be imported.

    ``find_spec`` raises (not returns None) when a *parent* package is absent
    (e.g. probing ``azure.keyvault.keys`` with no ``azure.keyvault``), so treat any
    failure as unavailable.
    """

    def _probe() -> bool:
        try:
            return find_spec(module) is not None
        except (ImportError, ValueError):
            return False

    return _probe


def load_provider_extensions() -> None:
    """Import extension modules so their register_* side effects run. Idempotent.

    Sources: the ``PROVISA_ENCRYPTION_PROVIDER_MODULES`` env var (comma-separated
    importable module paths) and the ``provisa.encryption_providers`` entry-point
    group. A broken extension must not take down encryption selection — it is
    skipped with the error surfaced via the module's own import raising here only
    for the env-listed modules (explicit operator config), while entry-point
    failures are swallowed so a bad third-party package can't brick startup.
    """
    global _EXTENSIONS_LOADED
    if _EXTENSIONS_LOADED:
        return
    _EXTENSIONS_LOADED = True  # set first: registrations below re-enter get_provider_spec
    _register_builtins()
    for mod in filter(None, (os.environ.get("PROVISA_ENCRYPTION_PROVIDER_MODULES", "")).split(",")):
        importlib.import_module(mod.strip())
    try:
        from importlib.metadata import entry_points  # noqa: PLC0415

        for ep in entry_points(group="provisa.encryption_providers"):
            try:
                ep.load()  # loader is expected to call register_encryption_provider
            except Exception:  # noqa: BLE0001 - a bad plugin must not brick encryption
                continue
    except Exception:  # noqa: BLE0001
        pass


_BUILTINS_REGISTERED = False


def _register_builtins() -> None:
    global _BUILTINS_REGISTERED
    if _BUILTINS_REGISTERED:
        return
    _BUILTINS_REGISTERED = True

    from provisa.encryption.envelope import EnvelopeEncryption
    from provisa.encryption.providers import (
        AwsKmsMasterKey,
        AzureKeyVaultMasterKey,
        GcpKmsMasterKey,
        HashiCorpVaultMasterKey,
        LocalKeychain,
        NullMasterKey,
    )
    from provisa.encryption.service import NullEncryption

    register_encryption_provider(
        EncryptionProviderSpec(
            key="null",
            label="None (passthrough)",
            description="No encryption — data stored in plaintext. Development/test only.",
            build=lambda cfg, key_id, ttl: NullEncryption(),
            aliases=("none", "passthrough"),
        )
    )
    register_encryption_provider(
        EncryptionProviderSpec(
            key="local",
            label="Local keychain (AES-256-GCM)",
            description="Envelope encryption with a 32-byte master key held on this host (OS keychain or PROVISA_ENCRYPTION_KEY).",
            build=lambda cfg, key_id, ttl: EnvelopeEncryption(
                LocalKeychain.from_config(key_id), dek_cache_ttl=ttl
            ),
            aliases=("local_keychain", "keychain"),
        )
    )
    register_encryption_provider(
        EncryptionProviderSpec(
            key="null_envelope",
            label="Null envelope (test)",
            description="Envelope format with an unwrapped DEK — exercises the envelope path in tests.",
            build=lambda cfg, key_id, ttl: EnvelopeEncryption(NullMasterKey(), dek_cache_ttl=ttl),
        )
    )
    register_encryption_provider(
        EncryptionProviderSpec(
            key="aws_kms",
            label="AWS KMS",
            description="Envelope encryption with the master key held in AWS KMS (or a KMS-compatible endpoint).",
            available=_importable("boto3"),
            config_fields=[
                {
                    "config_key": "key_arn",
                    "label": "KMS key ARN / ID",
                    "type": "string",
                    "required": True,
                    "placeholder": "arn:aws:kms:us-east-1:123456789012:key/abcd-…",
                },
                {
                    "config_key": "region",
                    "label": "AWS region",
                    "type": "string",
                    "required": False,
                    "placeholder": "us-east-1",
                },
                {
                    "config_key": "endpoint_url",
                    "label": "Custom endpoint URL",
                    "type": "string",
                    "required": False,
                    "placeholder": "blank = AWS; set for a private/compatible KMS",
                },
            ],
            build=lambda cfg, key_id, ttl: EnvelopeEncryption(
                AwsKmsMasterKey(
                    cfg.get("key_arn", ""),
                    region=cfg.get("region") or None,
                    endpoint_url=cfg.get("endpoint_url") or None,
                ),
                dek_cache_ttl=ttl,
            ),
        )
    )
    register_encryption_provider(
        EncryptionProviderSpec(
            key="hashicorp_vault",
            label="HashiCorp Vault (Transit)",
            description="Envelope encryption via Vault's Transit engine. Requires the 'hvac' package.",
            available=_importable("hvac"),
            config_fields=[
                {
                    "config_key": "key_name",
                    "label": "Transit key name",
                    "type": "string",
                    "required": True,
                },
                {
                    "config_key": "url",
                    "label": "Vault address",
                    "type": "string",
                    "required": False,
                    "placeholder": "https://vault.internal:8200 (or VAULT_ADDR)",
                },
                {
                    "config_key": "token",
                    "label": "Vault token",
                    "type": "string",
                    "required": False,
                    "secret": True,
                    "placeholder": "blank = VAULT_TOKEN",
                },
                {
                    "config_key": "mount",
                    "label": "Transit mount",
                    "type": "string",
                    "required": False,
                    "placeholder": "transit",
                },
                {
                    "config_key": "namespace",
                    "label": "Vault namespace (Enterprise)",
                    "type": "string",
                    "required": False,
                },
            ],
            build=lambda cfg, key_id, ttl: EnvelopeEncryption(
                HashiCorpVaultMasterKey(
                    cfg.get("key_name", ""),
                    url=cfg.get("url") or None,
                    token=cfg.get("token") or None,
                    mount=cfg.get("mount") or "transit",
                    namespace=cfg.get("namespace") or None,
                ),
                dek_cache_ttl=ttl,
            ),
        )
    )
    register_encryption_provider(
        EncryptionProviderSpec(
            key="gcp_kms",
            label="Google Cloud KMS",
            description="Envelope encryption with the master key held in Google Cloud KMS. Requires the 'google-cloud-kms' package.",
            available=_importable("google.cloud.kms"),
            config_fields=[
                {
                    "config_key": "key_name",
                    "label": "CryptoKey resource path",
                    "type": "string",
                    "required": True,
                    "placeholder": "projects/P/locations/L/keyRings/R/cryptoKeys/K",
                },
            ],
            build=lambda cfg, key_id, ttl: EnvelopeEncryption(
                GcpKmsMasterKey(cfg.get("key_name", "")), dek_cache_ttl=ttl
            ),
        )
    )
    register_encryption_provider(
        EncryptionProviderSpec(
            key="azure_key_vault",
            label="Azure Key Vault",
            description="Envelope encryption with the master key held in Azure Key Vault. Requires the 'azure-keyvault-keys' package.",
            available=_importable("azure.keyvault.keys"),
            config_fields=[
                {
                    "config_key": "vault_url",
                    "label": "Key Vault URL",
                    "type": "string",
                    "required": True,
                    "placeholder": "https://myvault.vault.azure.net",
                },
                {"config_key": "key_name", "label": "Key name", "type": "string", "required": True},
            ],
            build=lambda cfg, key_id, ttl: EnvelopeEncryption(
                AzureKeyVaultMasterKey(cfg.get("vault_url", ""), cfg.get("key_name", "")),
                dek_cache_ttl=ttl,
            ),
        )
    )
