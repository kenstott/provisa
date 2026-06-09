# Copyright (c) 2026 Kenneth Stott
# Canary: 3c8f2b1a-6e7d-4a90-bf12-9d4e5c6a7b80
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

import os
import socket

import pytest

from tests._noauth_config import pin_no_auth_config


def _pg_available() -> bool:
    try:
        host = os.environ.get("PG_HOST", "localhost")
        port = int(os.environ.get("PG_PORT", "5432"))
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def _trino_available() -> bool:
    try:
        host = os.environ.get("TRINO_HOST", "localhost")
        port = int(os.environ.get("TRINO_PORT", "8080"))
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session", autouse=True)
def _require_stack():
    """Skip all e2e tests when PG + Trino stack is not running."""
    if not (_pg_available() and _trino_available()):
        pytest.skip("Docker Compose stack (PG + Trino) not running")
    yield


@pytest.fixture(scope="session", autouse=True)
def _disable_auth_for_e2e(tmp_path_factory):
    """E2E tests build the in-process app (create_app) and call it with a `role`
    but no bearer token; force auth off so AuthMiddleware is not installed and
    requests are not rejected with HTTP 401."""
    yield from pin_no_auth_config(tmp_path_factory.mktemp("noauth-cfg"))
