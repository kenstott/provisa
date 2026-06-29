# Copyright (c) 2026 Kenneth Stott
# Canary: 87c88210-a4bc-4f64-8e50-e46ff9be6a27
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

import os
import subprocess
from pathlib import Path

import pytest

from tests._noauth_config import pin_no_auth_config

_PG_BIN = "/Library/PostgreSQL/16/bin"
_SNAPSHOT_PATH = Path.home() / "provisa-test-db-snapshot.dump"
_LIVE_SERVER_URL = os.environ.get("PROVISA_URL", "http://localhost:8000")


require_stack = pytest.mark.usefixtures()


def _pg_env() -> dict:
    env = os.environ.copy()
    env["PGPASSWORD"] = os.environ.get("PG_PASSWORD", "provisa")
    return env


def _pg_args() -> list[str]:
    return [
        "-h",
        os.environ.get("PG_HOST", "localhost"),
        "-p",
        os.environ.get("PG_PORT", "5432"),
        "-U",
        os.environ.get("PG_USER", "provisa"),
        os.environ.get("PG_DATABASE", "provisa"),
    ]


@pytest.fixture(scope="session", autouse=True)
def _db_snapshot_restore():
    """Snapshot the PG DB before the test session and restore it after.

    Tests are free to corrupt DB state — it will be restored at session end.
    After restore the live server is told to rebuild its schema so its in-memory
    state matches the restored DB.
    """
    import time

    # Retry pg_dump up to 5 times — live server DDL may hold AccessExclusiveLock
    # briefly at startup, causing pg_dump's AccessShareLock to deadlock/fail.
    result = None
    for attempt in range(5):
        result = subprocess.run(
            [
                f"{_PG_BIN}/pg_dump",
                "--format=custom",
                "--no-acl",
                "--no-owner",
                "-f",
                str(_SNAPSHOT_PATH),
            ]
            + _pg_args(),
            env=_pg_env(),
            check=False,
        )
        if result.returncode == 0:
            break
        if attempt < 4:
            time.sleep(3)
    else:
        code = result.returncode if result is not None else "unknown"
        raise RuntimeError(f"pg_dump failed after 5 attempts (exit code {code})")

    yield

    subprocess.run(
        [
            f"{_PG_BIN}/pg_restore",
            "--clean",
            "--if-exists",
            "--no-acl",
            "--no-owner",
            "--single-transaction",
            "-h",
            os.environ.get("PG_HOST", "localhost"),
            "-p",
            os.environ.get("PG_PORT", "5432"),
            "-U",
            os.environ.get("PG_USER", "provisa"),
            "-d",
            os.environ.get("PG_DATABASE", "provisa"),
            str(_SNAPSHOT_PATH),
        ],
        env=_pg_env(),
        check=False,
    )

    # Tell the live server to rebuild its in-memory schema from the restored DB
    try:
        import httpx

        httpx.post(
            f"{_LIVE_SERVER_URL}/admin/graphql",
            json={"query": "mutation { rebuildSchemas { success } }"},
            timeout=30,
        )
    except Exception:
        pass


@pytest.fixture(scope="session", autouse=True)
def _require_stack():
    """Docker services are started by _DockerServiceManager before tests run."""
    yield


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
