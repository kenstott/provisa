# Copyright (c) 2026 Kenneth Stott
# Canary: f6cde115-6b0b-4c2c-8232-577d35967837
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provider resolution for metadata egress (REQ-1068, REQ-1069).

Same shape as ``provisa/core/mail.py`` ``email_sender``: the configured name resolves to an
adapter here, at construction, where the error can name the setting that is wrong.
"""

# Requirements: REQ-1068, REQ-1069

from __future__ import annotations

from typing import TYPE_CHECKING

from provisa.api.metadata_egress.provider import (
    MetadataEgress,
    MetadataEgressNotConfiguredError,
)

if TYPE_CHECKING:
    from provisa.core.models import MetadataEgressConfig

_PROVIDERS: dict[str, type[MetadataEgress]] = {}


def register_provider(cls: type[MetadataEgress]) -> type[MetadataEgress]:
    """Class decorator registering an adapter under its ``provider_name``.

    A provider that does not set ``provider_name``, or that collides with one already
    registered, fails at import — the two ways a silently-wrong provider could be selected
    at runtime.
    """
    name = getattr(cls, "provider_name", None)
    if not name:
        raise ValueError(f"{cls.__name__} does not set provider_name")
    if name in _PROVIDERS and _PROVIDERS[name] is not cls:
        raise ValueError(
            f"metadata egress provider {name!r} is already registered to "
            f"{_PROVIDERS[name].__name__}"
        )
    _PROVIDERS[name] = cls
    return cls


def registered_providers() -> list[str]:
    """Names the factory will accept, for config validation and the admin UI."""
    return sorted(_PROVIDERS)


def metadata_egress(config: MetadataEgressConfig) -> MetadataEgress:
    """The configured adapter behind the port.

    Raises when egress is disabled rather than returning nothing: a caller that reaches this
    function has already decided to publish, and handing it a no-op would turn a
    misconfiguration into silently unpublished metadata.
    """
    if not config.enabled:
        raise MetadataEgressNotConfiguredError(
            "Metadata egress is disabled (metadata_egress.enabled). Enable it and set a "
            "provider and endpoint to publish to an external catalog."
        )
    if not config.provider:
        raise MetadataEgressNotConfiguredError(
            "No metadata egress provider is configured (metadata_egress.provider); "
            f"expected one of: {', '.join(registered_providers())}"
        )
    try:
        provider = _PROVIDERS[config.provider]
    except KeyError:
        raise MetadataEgressNotConfiguredError(
            f"Unknown metadata egress provider {config.provider!r} "
            f"(metadata_egress.provider); expected one of: "
            f"{', '.join(registered_providers())}"
        ) from None
    return provider(config)
