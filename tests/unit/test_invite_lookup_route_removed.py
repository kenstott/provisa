# Copyright (c) 2026 Kenneth Stott
# Canary: e237e9f9-7adc-49ce-ab21-72dd9a9c6b44
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GET /auth/invite/{token} no longer exists (REQ-516, the maintainer's ruling 2026-11-20).

Nothing called it; signed-out invite lookup is /public/invite-info/."""

from __future__ import annotations

from provisa.api.auth_router import router


def test_no_auth_invite_lookup_route_is_registered():
    paths = {getattr(r, "path", "") for r in router.routes}
    assert not any("/invite/{" in p for p in paths), paths
    assert not hasattr(__import__("provisa.api.auth_router", fromlist=["x"]), "get_invite")
