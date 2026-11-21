# Copyright (c) 2026 Kenneth Stott
# Canary: 3f9c2a71-8e4b-4d6a-b1c5-0e7d9a2f4c86
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The HTTP face of "a query node serves no UI" (REQ-1916, amended 2026-11-02).

Only a coordinator or an every-mode node serves the UI and the UI's backend, so the UI has one
stable address to point people to. On a query node a request for either is answered 503 with the
coded refusal, naming that address (``mail.base_url``, the UI's public origin), before it reaches a
route. The mirror of :mod:`provisa.api.coordinator_gate`.

The API names its UI-backend routes in :func:`is_ui_path`; everything else a data client needs is
served: every ``/data`` route, the other query surfaces outside it (neo4j's HTTP API at ``/`` and
``/db``, NL queries at ``/query/nl``), a product's open-standard documents a catalog pulls, the
health probes, the API's own docs, and ``POST /auth/login`` -- the password-for-token exchange the
JDBC driver and scripts sign in with. Every other ``/auth`` route is the UI's: the account, the
personal-token page, and the interactive sign-in round trips. The UI server is all UI, so it
refuses every path."""

# Requirements: REQ-1916

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from provisa.core import process_mode

# The UI backend's route prefixes: the admin API, the setup wizard, an org's sign-in branding,
# invite and demo pages, the demo sandboxes, the control plane's org pages, billing, and the UI
# server's own settings.
_UI_PREFIXES = (
    "/admin",
    "/setup",
    "/orgs/branding",
    "/public",
    "/sandboxes",
    "/control-plane",
    "/billing",
    "/internal/ui-server-settings",
)
_AUTH_PREFIX = "/auth"
# The one /auth route a data client calls (provisa/auth/login_router.py).
_DATA_CLIENT_AUTH = "/auth/login"
# A data product's panels in the UI read its lineage and its contracts as JSON; a catalog pulls
# its ODPS and ODCS documents as YAML (provisa/api/admin/data_product_documents_router.py).
_DATA_PRODUCTS_PREFIX = "/data-products/"


def _under(path: str, prefix: str) -> bool:
    return path == prefix or path.startswith(prefix.rstrip("/") + "/")


def is_ui_path(path: str) -> bool:
    """Whether ``path`` on the API is a route of the UI's backend, which a query node refuses."""
    if any(_under(path, prefix) for prefix in _UI_PREFIXES):
        return True
    if _under(path, _AUTH_PREFIX):
        return path != _DATA_CLIENT_AUTH
    if path.startswith(_DATA_PRODUCTS_PREFIX):
        return path.endswith("/lineage") or path.endswith(".json")
    return False


class QueryUiGate:
    """ASGI middleware refusing the UI and the UI's backend on a query node, by name.

    ``refuses`` says which paths are the UI's (:func:`is_ui_path` on the API, every path on the UI
    server); ``ui_address`` reads the address the UI is served at, at the time of the refusal."""

    def __init__(
        self,
        inner: Any,
        *,
        refuses: Callable[[str], bool],
        ui_address: Callable[[], str | None],
    ) -> None:
        self._inner = inner
        self._refuses = refuses
        self._ui_address = ui_address

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] == "http" and self._refuses(scope.get("path", "")):
            try:
                process_mode.refuse_ui_request(self._ui_address())
            except process_mode.QueryServesNoUi as refused:
                # The shape every ApiError is answered with (app._api_error_handler).
                body = json.dumps(
                    {
                        "detail": str(refused),
                        "code": refused.code,
                        "params": {"ui_address": refused.ui_address},
                    }
                ).encode()
                await send(
                    {
                        "type": "http.response.start",
                        "status": 503,
                        "headers": [(b"content-type", b"application/json")],
                    }
                )
                await send({"type": "http.response.body", "body": body})
                return
        await self._inner(scope, receive, send)
