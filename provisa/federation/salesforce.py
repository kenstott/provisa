# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1d8f27-3b64-4e90-a2f7-9d0e6b4a1c38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Salesforce source's login URL (REQ-1946), read by both of its readers: the pgwire server's
model (``pgwire_replica``) and the Trino catalog (``trino_connectors``)."""

from __future__ import annotations

from typing import Any

from provisa.core.secrets import resolve_secrets
from provisa.federation.connector_config import MissingConnectorConfig

LOGIN_URL_SCHEME = "https://"


def salesforce_login_url(source: Any) -> str:
    """The org's My Domain login URL, from ``base_url`` or ``host`` with its secret reference
    resolved. It must be the full URL: the adapter appends the token path to it as written, so a
    bare host fails there as an unknown URL. A missing one, or one that is not ``https://…``, is a
    config error naming the field; the scheme is never added for the steward."""
    who = f"salesforce source {source.id!r}"
    raw = source.base_url or source.host
    login_url = resolve_secrets(raw) if raw else ""
    if not login_url:
        raise MissingConnectorConfig(f"{who}: requires loginUrl (the org's My Domain URL)")
    if not login_url.startswith(LOGIN_URL_SCHEME) or login_url == LOGIN_URL_SCHEME:
        raise MissingConnectorConfig(
            f"{who}: loginUrl must be the full {LOGIN_URL_SCHEME}… My Domain URL "
            f"(for example https://acme.my.salesforce.com), got {login_url!r}"
        )
    return login_url
