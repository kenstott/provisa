# Copyright (c) 2026 Kenneth Stott
# Canary: f6cde115-6b0b-4c2c-8232-577d35967837
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provider resolution for metadata export (REQ-1068, REQ-1069).

Same shape as ``provisa/core/mail.py`` ``email_sender``: the configured name resolves to an
adapter here, at construction, where the error can name the setting that is wrong.
"""

# Requirements: REQ-1068, REQ-1069

from __future__ import annotations

from typing import TYPE_CHECKING

from provisa.api.metadata_export.provider import (
    MetadataExport,
    MetadataExportNotConfiguredError,
)

if TYPE_CHECKING:
    from provisa.core.models import MetadataExportTarget

_PROVIDERS: dict[str, type[MetadataExport]] = {}


def register_provider[T: type[MetadataExport]](cls: T) -> T:
    """Class decorator registering an adapter under its ``provider_name``.

    Generic in the decorated class, not ``type[MetadataExport]`` in and out: a widened return type
    would erase the decorated adapter's own members, and a subclass of one (Atlan extends Atlas)
    would inherit only the base protocol.

    A provider that does not set ``provider_name``, or that collides with one already
    registered, fails at import — the two ways a silently-wrong provider could be selected
    at runtime.
    """
    name = getattr(cls, "provider_name", None)
    if not name:
        raise ValueError(f"{cls.__name__} does not set provider_name")
    if name in _PROVIDERS and _PROVIDERS[name] is not cls:
        raise ValueError(
            f"metadata export provider {name!r} is already registered to "
            f"{_PROVIDERS[name].__name__}"
        )
    _PROVIDERS[name] = cls
    return cls


def registered_providers() -> list[str]:
    """Names the factory will accept, for config validation and the admin UI."""
    return sorted(_PROVIDERS)


def metadata_export(target: MetadataExportTarget) -> MetadataExport:
    """The adapter behind the port for one configured target (REQ-2118: an org may have several).

    An unknown provider is refused here, naming the setting, rather than handed back as a no-op:
    a misconfiguration must not turn into silently unpublished metadata.
    """
    try:
        provider = _PROVIDERS[target.provider]
    except KeyError:
        raise MetadataExportNotConfiguredError(
            f"Unknown metadata export provider {target.provider!r} for target {target.id!r} "
            f"(metadata_export.targets); expected one of: {', '.join(registered_providers())}"
        ) from None
    return provider(target)
