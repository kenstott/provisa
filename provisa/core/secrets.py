# Copyright (c) 2025 Kenneth Stott
# Canary: 6773fcaa-c61e-4fc2-b7e3-cc9ab9ab3d01
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Pluggable secrets provider. V1: env vars. Extensible to Vault, K8s, AWS."""

import os
import re
from abc import ABC, abstractmethod

_SECRET_PATTERN = re.compile(r"\$\{(\w+):([^}]+)\}")


class SecretsProvider(ABC):
    @abstractmethod
    def resolve(self, reference: str) -> str: ...


class EnvSecretsProvider(SecretsProvider):
    def resolve(self, reference: str) -> str:
        value = os.environ.get(reference)
        if value is None:
            raise KeyError(f"Environment variable not set: {reference}")
        return value


_PROVIDERS: dict[str, SecretsProvider] = {
    "env": EnvSecretsProvider(),
}


def register_provider(name: str, provider: SecretsProvider) -> None:
    _PROVIDERS[name] = provider


def resolve_secrets(value: str) -> str:
    """Replace ${provider:reference} patterns with resolved secret values."""

    def _replace(match: re.Match) -> str:
        provider_name = match.group(1)
        reference = match.group(2)
        provider = _PROVIDERS.get(provider_name)
        if provider is None:
            raise ValueError(f"Unknown secrets provider: {provider_name}")
        return provider.resolve(reference)

    return _SECRET_PATTERN.sub(_replace, value)


def resolve_secrets_in_dict(data: dict) -> dict:
    """Recursively resolve secret references in a dict."""
    result = {}
    for key, value in data.items():
        if isinstance(value, str):
            result[key] = resolve_secrets(value)
        elif isinstance(value, dict):
            result[key] = resolve_secrets_in_dict(value)
        elif isinstance(value, list):
            result[key] = [
                resolve_secrets_in_dict(item) if isinstance(item, dict)
                else resolve_secrets(item) if isinstance(item, str)
                else item
                for item in value
            ]
        else:
            result[key] = value
    return result
