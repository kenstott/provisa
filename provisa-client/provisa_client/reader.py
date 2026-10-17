# Copyright (c) 2026 Kenneth Stott
# Canary: 0cecaca0-bb93-41b0-afd0-6088bd36c651
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``query(text, role=...)``: a governed SQL, GraphQL or Cypher read as a pandas DataFrame, the
same call on a desktop Python and in the browser.

The server detects the statement's language, as ``/data/query`` and Arrow Flight do for every
client. On CPython the read goes through :class:`provisa_client.ProvisaClient`, over Arrow Flight
or HTTP as the connection says (:mod:`provisa_client.credentials`). In Pyodide it goes to the
server that served the page, as the page's signed-in user (:mod:`provisa_client.browser`).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from provisa_client import browser

_session: Any = None  # the Credentials sign_in resolved, once it has been called


@dataclass(frozen=True)
class SignedIn:
    """Who :func:`sign_in` connected as: the server, the user the server knows, and the role,
    org and environment the reads act in. It displays as one line in a notebook."""

    url: str
    user: str
    role: str | None
    org: str | None
    env: str | None

    def __repr__(self) -> str:
        return (
            f"Signed in to {self.url or 'this page'} as {self.user}, "
            f"role {self.role or '(your own)'}, org {self.org or '(none)'}, "
            f"environment {self.env or 'prod'}"
        )


def sign_in(**settings: Any) -> SignedIn:
    """Connect, and say as whom. With no arguments it does the right thing where it runs: in the
    browser the connection is the page's signed-in user (the auth bridge); on a desktop Python it
    is resolved once from the environment and ``~/.provisa/credentials``
    (:func:`provisa_client.credentials.resolve`, whose settings it also takes) and kept for every
    later :func:`query`. Either way the server is asked who that is (``GET /auth/me``)."""
    global _session
    if browser.in_browser():
        if settings:
            raise TypeError(
                f"in the browser the connection is the page's signed-in user; {sorted(settings)} "
                "cannot be set here"
            )
        auth = browser.read_bridge()
        me = browser.get_json_sync("/auth/me")
        return SignedIn(auth["baseUrl"], _who(me), auth["role"], auth["org"], auth["env"])
    import httpx

    from provisa_client.credentials import resolve

    creds = resolve(**settings)
    headers = {}
    if creds.token:
        headers["Authorization"] = f"Bearer {creds.token}"
    if creds.role:
        headers["X-Provisa-Role"] = creds.role
    if creds.org:
        headers["X-Org-Provisa"] = creds.org
    r = httpx.get(f"{creds.url}/auth/me", headers=headers, timeout=30.0)
    if r.status_code != 200:
        raise PermissionError(
            f"{creds.url} did not accept the sign-in: HTTP {r.status_code}: {r.text}"
        )
    me = r.json()
    _session = creds
    return SignedIn(creds.url, _who(me), creds.role, creds.org or me.get("active_org_id"), None)


def _who(me: dict[str, Any]) -> str:
    """The user ``/auth/me`` names: their display name, with their email when it differs."""
    name = me.get("display_name") or me["user_id"]
    email = me.get("email")
    return f"{name} <{email}>" if email and email != name else name


def query(text: str, *, role: str | None = None, variables: dict[str, Any] | None = None):
    """``text`` (SQL, GraphQL or Cypher) answered as a pandas DataFrame. ``role`` acts as that
    role (``X-Provisa-Role``) in place of the connection's; the server honours it only when the
    user is assigned it."""
    if browser.in_browser():
        return browser.fetch_sync(text, role=role, variables=variables)
    return _desktop(text, role=role, variables=variables)


async def aquery(text: str, *, role: str | None = None, variables: dict[str, Any] | None = None):
    """:func:`query`, awaitable: in the browser it reads over ``fetch``, on any thread."""
    if browser.in_browser():
        return await browser.fetch(text, role=role, variables=variables)
    return await asyncio.to_thread(_desktop, text, role=role, variables=variables)


def _desktop(text: str, *, role: str | None, variables: dict[str, Any] | None):
    from provisa_client.client import ProvisaClient

    global _session
    if _session is None:
        from provisa_client.credentials import resolve

        _session = resolve()  # once: a password is exchanged for a token only the first time
    creds = _session
    client = ProvisaClient(
        creds.url,
        token=creds.token,
        role=role or creds.role,
        org=creds.org,
        flight_port=creds.flight_port,
    )
    if creds.transport == "flight":
        return client.flight_df(text, variables)
    return client.query_df(text, variables)
