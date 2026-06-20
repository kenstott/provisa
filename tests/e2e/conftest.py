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

_REPO_ROOT = "/Volumes/main/Users/kennethstott/PycharmProjects/provisa-group-11"
_COMPOSE_FILE = "docker-compose.core.yml"


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


@pytest.fixture(scope="session")
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
def _require_stack(docker_stack):
    """Ensure PG + Trino stack is running before e2e tests execute."""
    yield


@pytest.fixture(scope="session", autouse=True)
def _disable_auth_for_e2e(tmp_path_factory):
    """E2E tests build the in-process app (create_app) and call it with a `role`
    but no bearer token; force auth off so AuthMiddleware is not installed and
    requests are not rejected with HTTP 401."""
    yield from pin_no_auth_config(tmp_path_factory.mktemp("noauth-cfg"))
