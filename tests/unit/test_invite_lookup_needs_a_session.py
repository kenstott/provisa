# Copyright (c) 2026 Kenneth Stott
# Canary: e237e9f9-7adc-49ce-ab21-72dd9a9c6b44
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GET /auth/invite/{token} needs a session (REQ-516, the maintainer's ruling).

Its docstring once said invites were public until redeemed, but the bearer gate never exempted the
route and no screen calls it signed out. The behaviour is the rule: with auth enforced, a request
that presents no credential is refused before the handler runs."""

from __future__ import annotations

import pytest

from provisa.auth.middleware import AuthMiddleware
from provisa.auth.models import AuthIdentity


class _URL:
    def __init__(self, path: str) -> None:
        self.path = path


class _State:
    pass


class _Req:
    def __init__(self, path: str, headers: dict[str, str]) -> None:
        self.headers = headers
        self.url = _URL(path)
        self.state = _State()
        self.method = "GET"


class _Provider:
    auth_scheme = "bearer"

    async def validate_token(self, _token):
        return AuthIdentity(
            user_id="u", email=None, display_name="U", roles=["analyst"], raw_claims={}
        )


@pytest.mark.asyncio
async def test_a_credential_less_invite_lookup_is_refused_with_401():
    mw = AuthMiddleware(app=None, provider=_Provider())
    resp = await mw._process(_Req("/auth/invite/some-token", {}))
    assert resp is not None and resp.status_code == 401
