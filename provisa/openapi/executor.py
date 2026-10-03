# Copyright (c) 2026 Kenneth Stott
# Canary: af919853-693c-4b27-893e-b146bcc8d07f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The auth headers an OpenAPI source's write operations are sent with (REQ-320).

A registered OpenAPI table is read through ``api_source.caller`` from its ``api_endpoints`` row
(``api_source.openapi_endpoint``); nothing here reads a table."""

from __future__ import annotations

import logging


# Requirements: REQ-314, REQ-316, REQ-317, REQ-318, REQ-319, REQ-320

log = logging.getLogger(__name__)


def _build_auth_headers(auth_config: dict[str, str] | None) -> dict[str, str]:  # REQ-320
    if not auth_config:
        return {}
    auth_type = auth_config.get("type", "none")
    if auth_type == "bearer":
        token = auth_config.get("token")
        if not token:
            raise ValueError("bearer auth requires a 'token'")
        return {"Authorization": f"Bearer {token}"}
    if auth_type == "basic":
        import base64

        username = auth_config.get("username")
        password = auth_config.get("password")
        if not username or not password:
            raise ValueError("basic auth requires 'username' and 'password'")
        creds = base64.b64encode(f"{username}:{password}".encode()).decode()
        return {"Authorization": f"Basic {creds}"}
    if auth_type == "api_key":
        header_name = auth_config.get("header_name", "X-API-Key")
        api_key = auth_config.get("api_key")
        if not api_key:
            raise ValueError("api_key auth requires an 'api_key'")
        return {header_name: api_key}
    return {}
