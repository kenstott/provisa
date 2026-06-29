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
import subprocess
import time

import pytest

from tests._noauth_config import pin_no_auth_config

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_COMPOSE_FILE = "docker-compose.core.yml"

__all__ = ["docker_stack", "_disable_auth_for_e2e"]


def _wait_for_port(host: str, port: int, timeout: float = 120.0) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except OSError:
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"Service at {host}:{port} did not become reachable within {timeout}s"
                )
            time.sleep(1)


@pytest.fixture(scope="session", autouse=True)
def docker_stack():
    """Spin up the core Docker Compose stack for e2e tests.

    If PROVISA_E2E_EXTERNAL_STACK is set, assume an external stack is already
    running and skip spin-up/teardown.
    """
    if os.environ.get("PROVISA_E2E_EXTERNAL_STACK"):
        yield
        return

    subprocess.run(
        ["docker", "compose", "-f", _COMPOSE_FILE, "up", "-d"],
        cwd=_REPO_ROOT,
        check=True,
    )

    pg_host = os.environ.get("PG_HOST", "localhost")
    pg_port = int(os.environ.get("PG_PORT", "5432"))
    trino_host = os.environ.get("TRINO_HOST", "localhost")
    trino_port = int(os.environ.get("TRINO_PORT", "8080"))

    _wait_for_port(pg_host, pg_port)
    _wait_for_port(trino_host, trino_port)

    yield

    subprocess.run(
        ["docker", "compose", "-f", _COMPOSE_FILE, "down"],
        cwd=_REPO_ROOT,
        check=True,
    )


@pytest.fixture(scope="session", autouse=True)
def _disable_auth_for_e2e(tmp_path_factory):  # pyright: ignore
    """E2E tests build the in-process app (create_app) and call it with a `role`
    but no bearer token; force auth off so AuthMiddleware is not installed and
    requests are not rejected with HTTP 401.

    Also points PROVISA_CONFIG at the sales-analytics test fixture config so
    all e2e tests see the expected sa__ schema, and sets PROVISA_CONFIG_REPLACE=1
    so the first create_app() call loads that config into the DB."""
    sample_cfg = os.path.join(_REPO_ROOT, "tests", "fixtures", "sample_config.yaml")
    prev_config = os.environ.get("PROVISA_CONFIG")
    prev_replace = os.environ.get("PROVISA_CONFIG_REPLACE")
    os.environ["PROVISA_CONFIG"] = sample_cfg
    os.environ["PROVISA_CONFIG_REPLACE"] = "1"
    try:
        yield from pin_no_auth_config(tmp_path_factory.mktemp("noauth-cfg"))
    finally:
        if prev_config is None:
            os.environ.pop("PROVISA_CONFIG", None)
        else:
            os.environ["PROVISA_CONFIG"] = prev_config
        if prev_replace is None:
            os.environ.pop("PROVISA_CONFIG_REPLACE", None)
        else:
            os.environ["PROVISA_CONFIG_REPLACE"] = prev_replace
