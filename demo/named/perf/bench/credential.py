# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""What the load clients present to a deployment that has auth (REQ-1911).

How each transport authenticates, from the product (``provisa/auth/providers/simple.py``,
``provisa/auth/middleware.py``, ``provisa/grpc/auth.py``, ``provisa/api/flight/server.py``,
``provisa/bolt/session.py``): HTTP, gRPC and Flight carry a bearer token (a provider token or a
personal access token); the simple provider mints a 30-minute JWT from ``POST /auth/login``.
pgwire and Bolt carry the user name and password directly.

A ``password`` credential logs in for the bearer token and renews it before it expires; a ``token``
credential presents the token as it is. The secret is read from the environment variable the
contract names and never written anywhere."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass

import httpx

LOGIN_PATH = "/auth/login"
# The simple provider's JWT lives 30 minutes; renew at 20 so no request carries an expired one.
REFRESH_AFTER_S = 20 * 60
_LOGIN_TIMEOUT_S = 30.0

_now = time.monotonic
_TOKENS: dict[tuple[str, str], tuple[float, str]] = {}  # (base url, user) -> (obtained at, token)
_LOCK = threading.Lock()


class CredentialError(RuntimeError):
    """The deployment refused the credential."""


@dataclass(frozen=True)
class Credential:
    kind: str  # "password" | "token"
    user: str | None
    secret: str
    base_url: str  # where to log in

    def bearer(self) -> str:
        """The bearer token to present now: the token itself, or one from a login that is renewed
        after ``REFRESH_AFTER_S``. One login at a time per process however many threads ask."""
        if self.kind == "token":
            return self.secret
        key = (self.base_url, self.user or "")
        with _LOCK:
            known = _TOKENS.get(key)
            if known is not None and _now() - known[0] < REFRESH_AFTER_S:
                return known[1]
            token = self._login()
            _TOKENS[key] = (_now(), token)
            return token

    def _login(self) -> str:
        url = self.base_url.rstrip("/") + LOGIN_PATH
        resp = httpx.post(
            url, json={"username": self.user, "password": self.secret}, timeout=_LOGIN_TIMEOUT_S
        )
        if resp.status_code != 200:
            raise CredentialError(f"login as {self.user} refused by {url}: {resp.status_code}")
        return resp.json()["access_token"]

    def basic(self, role: str) -> tuple[str, str]:
        """(user, password) for pgwire and Bolt. A token names no principal, so the role is the
        user, as it is with auth off."""
        return (self.user or role, self.secret)

    def auth_header(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.bearer()}"}
