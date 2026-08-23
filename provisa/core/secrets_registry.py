# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""WHICH SECRETS SERVICE ``${secret:NAME}`` ASKS (REQ-1557).

``${provider:reference}`` (REQ-125) has named a provider since V1, and ``register_provider`` has
existed to add more since REQ-557 -- but only ``env`` was ever registered and nothing ever called
the hook, so a secret could only be an environment variable of the server process. That makes a
secret something only the operator of the HOST can set, which a hosted org is not.

``secret`` IS THE LOGICAL NAME, not a backend. It resolves through whichever service the
deployment configured, so an org that starts on Provisa's own store and later moves to Vault does
not rewrite a single reference. ``env`` stays explicit and separate: it is how the deployment's own
process configuration is written, including the credential a central backend is opened with.

Registration mirrors the encryption registry (``provisa/encryption/registry.py``): a named spec
with UI metadata, an availability probe over the backend's SDK, and a builder over the persisted
config block. Selection is FAIL-CLOSED -- an unknown or unavailable backend raises, and a
misconfigured one never quietly becomes a different one.
"""

# Requirements: REQ-125, REQ-557, REQ-1557

# complexity-gate: allow-ble=2 reason="mirrors the encryption registry: a broken third-party
# secrets plugin loaded from an entry point must be skipped rather than brick startup, while
# env-listed modules still raise, being explicit operator config"

from __future__ import annotations

import importlib
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from importlib.util import find_spec

from provisa.core.secrets import SecretsProvider

#: The provider name the logical secrets service answers to in a reference.
SECRET = "secret"

#: (config block) -> SecretsProvider
SecretsBuilder = Callable[[dict], SecretsProvider]


@dataclass(frozen=True)
class SecretsProviderSpec:
    key: str
    label: str
    description: str
    build: SecretsBuilder
    #: Whether this backend can also CREATE and DELETE secrets. Only Provisa's own store can: a
    #: central service owns its own lifecycle, and the Secrets page says so rather than offering
    #: a create button that would write into somebody else's system of record.
    writable: bool = False
    #: UI field descriptors (config_key/label/type/required/secret/placeholder).
    config_fields: list[dict] = field(default_factory=list)
    available: Callable[[], bool] = lambda: True
    aliases: tuple[str, ...] = ()


_REGISTRY: dict[str, SecretsProviderSpec] = {}
_ALIASES: dict[str, str] = {}
_EXTENSIONS_LOADED = False


def register_secrets_provider(spec: SecretsProviderSpec) -> None:
    """Register (or replace) a backend spec. Public extension API."""
    _REGISTRY[spec.key] = spec
    for alias in spec.aliases:
        _ALIASES[alias.lower()] = spec.key


def get_secrets_provider_spec(name: str | None) -> SecretsProviderSpec | None:
    """Resolve a backend name (or alias) to its spec, or None if unregistered."""
    load_secrets_extensions()
    key = (name or "provisa").lower()
    return _REGISTRY.get(_ALIASES.get(key, key))


def secrets_provider_registry() -> list[SecretsProviderSpec]:
    """All registered backend specs (built-ins + extensions), stable order."""
    load_secrets_extensions()
    return list(_REGISTRY.values())


def _importable(module: str) -> Callable[[], bool]:
    def _probe() -> bool:
        try:
            return find_spec(module) is not None
        except (ImportError, ValueError):
            return False

    return _probe


def load_secrets_extensions() -> None:
    """Import extension modules so their register_* side effects run. Idempotent."""
    global _EXTENSIONS_LOADED
    if _EXTENSIONS_LOADED:
        return
    _EXTENSIONS_LOADED = True  # set first: registrations below re-enter get_secrets_provider_spec
    _register_builtins()
    for mod in filter(None, os.environ.get("PROVISA_SECRETS_PROVIDER_MODULES", "").split(",")):
        importlib.import_module(mod.strip())
    try:
        from importlib.metadata import entry_points  # noqa: PLC0415

        for ep in entry_points(group="provisa.secrets_providers"):
            try:
                ep.load()  # loader is expected to call register_secrets_provider
            except Exception:  # noqa: BLE001 - a bad plugin must not brick secrets selection
                continue
    except Exception:  # noqa: BLE001
        pass


def _cfg(config: dict, key: str, *, required: bool = True) -> str | None:
    """One config value, with ``${env:...}`` resolved.

    ENV ONLY. A backend's own credential cannot come out of the store that credential opens, so
    the resolution here is restricted to the process environment (REQ-1557).
    """
    from provisa.core.secrets import resolve_secrets

    raw = config.get(key)
    if raw is None or raw == "":
        if required:
            raise ValueError(f"secrets provider config is missing {key!r}")
        return None
    return resolve_secrets(str(raw), providers=("env",))


_BUILTINS_REGISTERED = False


def _register_builtins() -> None:
    global _BUILTINS_REGISTERED
    if _BUILTINS_REGISTERED:
        return
    _BUILTINS_REGISTERED = True

    from provisa.core.secrets_store import StoredSecretsProvider
    from provisa.core.secrets_providers import (
        AwsSecretsManagerProvider,
        AzureKeyVaultSecretsProvider,
        GcpSecretManagerProvider,
        VaultSecretsProvider,
    )

    register_secrets_provider(
        SecretsProviderSpec(
            key="provisa",
            label="Provisa (built-in, encrypted)",
            description=(
                "Secrets are held per-org in the control plane, each value encrypted under the "
                "deployment's master key. The default when no central secrets service is connected."
            ),
            build=lambda cfg: StoredSecretsProvider(),
            writable=True,
            aliases=("builtin", "internal", "local"),
        )
    )
    register_secrets_provider(
        SecretsProviderSpec(
            key="hashicorp_vault",
            label="HashiCorp Vault (KV v2)",
            description=(
                "Reads names out of a Vault KV v2 mount. A reference is 'path#key', or 'path' when "
                "the key is 'value'. Requires the 'hvac' package."
            ),
            build=lambda cfg: VaultSecretsProvider(
                url=_cfg(cfg, "url"),
                token=_cfg(cfg, "token"),
                mount=_cfg(cfg, "mount", required=False) or "secret",
                namespace=_cfg(cfg, "namespace", required=False),
            ),
            available=_importable("hvac"),
            config_fields=[
                {
                    "config_key": "url",
                    "label": "Vault address",
                    "type": "string",
                    "required": True,
                    "placeholder": "https://vault.internal:8200",
                },
                {
                    "config_key": "token",
                    "label": "Vault token",
                    "type": "string",
                    "required": True,
                    "secret": True,
                    "placeholder": "${env:VAULT_TOKEN}",
                },
                {
                    "config_key": "mount",
                    "label": "KV mount point",
                    "type": "string",
                    "required": False,
                    "placeholder": "secret",
                },
                {
                    "config_key": "namespace",
                    "label": "Vault namespace (Enterprise)",
                    "type": "string",
                    "required": False,
                },
            ],
            aliases=("vault",),
        )
    )
    register_secrets_provider(
        SecretsProviderSpec(
            key="aws_secrets_manager",
            label="AWS Secrets Manager",
            description=(
                "Reads names out of AWS Secrets Manager. A reference is the secret id, or "
                "'id#json_key' to pull one field out of a JSON secret. Requires 'boto3'."
            ),
            build=lambda cfg: AwsSecretsManagerProvider(
                region=_cfg(cfg, "region", required=False),
                endpoint_url=_cfg(cfg, "endpoint_url", required=False),
            ),
            available=_importable("boto3"),
            config_fields=[
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
                },
            ],
            aliases=("aws",),
        )
    )
    register_secrets_provider(
        SecretsProviderSpec(
            key="gcp_secret_manager",
            label="Google Secret Manager",
            description=(
                "Reads names out of GCP Secret Manager. A reference is the secret id, or "
                "'id#version'. Requires 'google-cloud-secret-manager'."
            ),
            build=lambda cfg: GcpSecretManagerProvider(project=_cfg(cfg, "project")),
            available=_importable("google.cloud.secretmanager"),
            config_fields=[
                {
                    "config_key": "project",
                    "label": "GCP project id",
                    "type": "string",
                    "required": True,
                },
            ],
            aliases=("gcp",),
        )
    )
    register_secrets_provider(
        SecretsProviderSpec(
            key="azure_key_vault",
            label="Azure Key Vault (secrets)",
            description=(
                "Reads names out of an Azure Key Vault. A reference is the secret name, or "
                "'name#version'. Requires 'azure-keyvault-secrets' and 'azure-identity'."
            ),
            build=lambda cfg: AzureKeyVaultSecretsProvider(vault_url=_cfg(cfg, "vault_url")),
            available=_importable("azure.keyvault.secrets"),
            config_fields=[
                {
                    "config_key": "vault_url",
                    "label": "Key Vault URL",
                    "type": "string",
                    "required": True,
                    "placeholder": "https://my-vault.vault.azure.net/",
                },
            ],
            aliases=("azure",),
        )
    )
