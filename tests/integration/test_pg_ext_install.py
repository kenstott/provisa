# Copyright (c) 2026 Kenneth Stott
# Canary: 6b28ef31-59bc-4db0-9ee4-41d35f329bc9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1873: `provisa pg-ext install` against real Postgres containers.

pg-ext-target-16 is a Postgres a bundle is built for (16, linux-x64); pg-ext-target-15 is one
none is built for. The command is run as an operator runs it, against the container by name.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.requires_pg_ext_targets]

_REPO = Path(__file__).resolve().parents[2]


def _container(service: str) -> str:
    from tests.itest_stack import COMPOSE_ARGS

    found = subprocess.run(
        ["docker", "compose", *COMPOSE_ARGS, "ps", "-q", service],
        capture_output=True,
        text=True,
        cwd=_REPO,
        check=True,
    ).stdout.strip()
    assert found, f"{service} is not running"
    return found


def _exec(container: str, *cmd: str) -> str:
    return subprocess.run(
        ["docker", "exec", container, *cmd], capture_output=True, text=True, check=True
    ).stdout.strip()


def _sql(container: str, sql: str) -> str:
    return _exec(container, "psql", "-U", "postgres", "-Atc", sql)


def _listing(container: str) -> str:
    """Every file in the module and extension directories: what an install would change."""
    lib = _exec(container, "pg_config", "--pkglibdir")
    share = _exec(container, "pg_config", "--sharedir")
    return _exec(container, "sh", "-c", f"ls -la {lib} {share}/extension | sort")


def _cli(*args: str, env: dict | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "provisa.cli", "pg-ext", "install", *args],
        capture_output=True,
        text=True,
        cwd=_REPO,
        env={**os.environ, **(env or {})},
    )


def _restart(container: str) -> None:
    subprocess.run(["docker", "restart", container], capture_output=True, check=True)
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        ready = subprocess.run(
            ["docker", "exec", container, "pg_isready", "-U", "postgres"], capture_output=True
        )
        if ready.returncode == 0:
            return
        time.sleep(2)
    raise RuntimeError(f"{container} did not come back after restart")


def test_a_matching_postgres_gets_the_extensions():
    target = _container("pg-ext-target-16")
    staged = _cli("--docker", target)
    assert staged.returncode == 0, staged.stderr
    assert "pg_duckdb" in staged.stdout and "Restart the server" in staged.stdout
    # ALTER SYSTEM takes effect at the restart; until then the change is the file's pending value.
    pending = _sql(
        target, "SELECT setting FROM pg_file_settings WHERE name = 'shared_preload_libraries'"
    )
    assert "pg_duckdb" in pending

    _restart(target)
    assert "pg_duckdb" in _sql(target, "SHOW shared_preload_libraries")
    created = _cli("--docker", target, "--create")
    print(created.stdout)
    for name in ("postgres_fdw", "file_fdw", "pg_duckdb"):
        assert f"{name}: created" in created.stdout, created.stdout
    extensions = set(_sql(target, "SELECT extname FROM pg_extension").split())
    assert {"postgres_fdw", "file_fdw", "pg_duckdb"} <= extensions
    # An extension that cannot be created is reported by name, with the reason, never skipped.
    for name in ("mysql_fdw", "sqlite_fdw"):
        assert f"{name}: " in created.stdout


def test_a_major_with_no_bundle_is_refused_and_nothing_is_written():
    target = _container("pg-ext-target-15")
    before = _listing(target)
    refused = _cli("--docker", target)
    assert refused.returncode == 2
    assert "PostgreSQL 15 on linux-x64" in refused.stderr
    assert "PostgreSQL 16 on linux-x64" in refused.stderr
    assert _listing(target) == before


def test_a_corrupted_bundle_is_refused_and_nothing_is_written(tmp_path):
    import provisa_pg_ext

    tampered = tmp_path / "provisa_pg_ext"
    shutil.copytree(Path(provisa_pg_ext.__file__).parent, tampered)
    with open(tampered / "_ext" / "linux-x64" / "lib" / "postgres_fdw.so", "ab") as handle:
        handle.write(b"\0")
    target16 = _container("pg-ext-target-16")
    before = _listing(target16)
    refused = _cli("--docker", target16, env={"PYTHONPATH": str(tmp_path)})
    assert refused.returncode == 2
    assert "postgres_fdw" in refused.stderr and "checksum" in refused.stderr
    assert _listing(target16) == before
