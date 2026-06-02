# Copyright (c) 2026 Kenneth Stott
# Canary: 3c8f2b1a-6e7d-4a90-bf12-9d4e5c6a7b80
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

import pytest

from tests._noauth_config import pin_no_auth_config


@pytest.fixture(scope="session", autouse=True)
def _disable_auth_for_e2e(tmp_path_factory):
    """E2E tests build the in-process app (create_app) and call it with a `role`
    but no bearer token; force auth off so AuthMiddleware is not installed and
    requests are not rejected with HTTP 401."""
    yield from pin_no_auth_config(tmp_path_factory.mktemp("noauth-cfg"))
