# Copyright (c) 2026 Kenneth Stott
# Canary: 87c88210-a4bc-4f64-8e50-e46ff9be6a27
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
def _disable_auth_for_integration(tmp_path_factory):
    """Integration tests build the in-process app and call it with a `role` but no
    bearer token; force auth off so create_app() does not install AuthMiddleware."""
    yield from pin_no_auth_config(tmp_path_factory.mktemp("noauth-cfg"))


@pytest.fixture(autouse=True, scope="module")
def _reset_app_state():
    """Reset global app state between test modules.

    The module-level `state` singleton in provisa.api.app accumulates
    auth_config from whichever module-scoped lifespan fixture ran last.
    Resetting before each module ensures create_app() sees a clean slate.
    """
    from provisa.api import app as _app_module

    _app_module.state.auth_config = None
    yield
    _app_module.state.auth_config = None
