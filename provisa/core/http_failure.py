# Copyright (c) 2026 Kenneth Stott
# Canary: ea0ca276-0c48-4a0e-bc2a-6b0306ccf00f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What an httpx failure was, in words a client can act on.

``str()`` of an httpx error is the text of the exception it was mapped from, and that is often
empty: a read timeout, and some connect failures, carry no message at all. A message built as
``f"... unreachable: {exc}"`` then ends at the colon. :func:`describe_http_failure` names the
failure kind from the exception's type (and, for a connect failure, the OS error under it) and
the address the request went to, and adds the exception's own text only when there is some.
"""

from __future__ import annotations

import socket
import ssl
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import httpx


def _chain(exc: BaseException) -> list[BaseException]:
    """``exc`` and every exception it was raised from or during, outermost first."""
    seen: list[BaseException] = []
    current: BaseException | None = exc
    while current is not None and current not in seen:
        seen.append(current)
        current = current.__cause__ or current.__context__
    return seen


def _connect_kind(chain: list[BaseException], url: str) -> str:
    """The kind of a connect failure, read from the OS error httpx mapped it from."""
    for cause in chain:
        if isinstance(cause, socket.gaierror):
            return f"the host name of {url} did not resolve"
        if isinstance(cause, ConnectionRefusedError):
            return f"the connection to {url} was refused"
        if isinstance(cause, ssl.SSLError):
            return f"the TLS handshake with {url} failed"
    return f"could not connect to {url}"


def _kind(exc: httpx.HTTPError, chain: list[BaseException], url: str) -> str:
    import httpx

    # Subclasses before their bases: ConnectTimeout is a TimeoutException, ProxyError a
    # TransportError, and so on.
    kinds: list[tuple[type[httpx.HTTPError], str]] = [
        (httpx.ConnectTimeout, f"timed out connecting to {url}"),
        (httpx.ReadTimeout, f"timed out waiting for a response from {url}"),
        (httpx.WriteTimeout, f"timed out sending the request to {url}"),
        (httpx.PoolTimeout, f"timed out waiting for a free connection to {url}"),
        (httpx.TimeoutException, f"timed out reaching {url}"),
        (httpx.ProxyError, f"the egress proxy refused the connection to {url}"),
        (httpx.RemoteProtocolError, f"{url} closed the connection or answered malformed HTTP"),
        (httpx.ReadError, f"the connection to {url} broke while reading the response"),
        (httpx.WriteError, f"the connection to {url} broke while sending the request"),
        (httpx.UnsupportedProtocol, f"{url} names a protocol the client does not speak"),
        (httpx.TooManyRedirects, f"{url} redirected too many times"),
        (httpx.DecodingError, f"the response from {url} could not be decoded"),
    ]
    if isinstance(exc, httpx.ConnectError):
        return _connect_kind(chain, url)
    for kind, text in kinds:
        if isinstance(exc, kind):
            return text
    return f"{type(exc).__name__} reaching {url}"


def describe_http_failure(exc: httpx.HTTPError) -> str:
    """What ``exc`` was and the address it happened at; never empty.

    An HTTP status error keeps httpx's own text (the status and the URL). Any other failure is
    named by kind and address, with the first non-empty text along its cause chain in
    parentheses, when there is one. ``exc.request`` is set on every error an httpx client raises
    (``httpx._exceptions.request_context``), so the address is always there to name.
    """
    import httpx

    if isinstance(exc, httpx.HTTPStatusError):
        return str(exc)
    request = exc.request
    url = f"{request.method} {request.url}"
    chain = _chain(exc)
    kind = _kind(exc, chain, url)
    detail = next((str(cause) for cause in chain if str(cause)), "")
    return f"{kind} ({detail})" if detail else kind
