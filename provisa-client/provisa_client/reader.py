# Copyright (c) 2026 Kenneth Stott
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
from typing import Any

from provisa_client import browser

_session: Any = None  # the Credentials sign_in resolved, once it has been called


def sign_in(**settings: Any):
    """Resolve the connection once (:func:`provisa_client.credentials.resolve`) and keep it for
    every later :func:`query`. Without it, the first query resolves from the environment and the
    profile file. In the browser the page's user is the connection, so there is nothing to sign
    in to."""
    global _session
    if browser.in_browser():
        raise RuntimeError("in the browser, reads act as the page's signed-in user")
    from provisa_client.credentials import resolve

    _session = resolve(**settings)
    return _session


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

    creds = _session if _session is not None else sign_in()
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
