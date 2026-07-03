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
# MinIO (S3) lives in observability.yml, not core.yml, but Trino's `otel` Iceberg
# catalog is S3-backed and is exercised on every app startup by _seed_ops_trino
# (CREATE SCHEMA otel.signals). Without MinIO that DDL blocks forever on the
# missing S3 endpoint (a hang, not an error), so the server never finishes
# starting. Provision both files and bring up only the services e2e needs
# (avoids grafana/tempo/prometheus/otel).
_COMPOSE_ARGS = [
    "-f",
    "docker-compose.core.yml",
    "-f",
    "docker-compose.observability.yml",
    "-f",
    "docker-compose.e2e.yml",
]
# kafka backs the kafka-source query (REQ-147) and kafka/debezium live-delivery
# strategies that Provisa consumes; producer-side messages are written by tests.
_SERVICES = ["postgres", "trino", "redis", "pgbouncer", "zaychik", "minio", "kafka"]

# The root `_wait_for_trino` session fixture pre-flights a shared, already-
# configured Trino (SHOW SCHEMAS FROM sales_pg) before any app runs. The e2e
# suite provisions its OWN fresh, isolated stack (see docker_stack below) whose
# Trino catalogs are configured by each in-process app on startup, so that pre-
# flight can never pass at session start. Readiness for e2e is established by
# docker_stack's `--wait` (container health) plus per-test app startup. Skip the
# shared-stack pre-flight for e2e sessions only (this conftest loads only when
# tests/e2e is collected; integration runs are unaffected).
os.environ.setdefault("PROVISA_SKIP_TRINO_WAIT", "1")

# The Bolt server (Cypher over Bolt, REQ-802) only starts when PROVISA_BOLT_PORT
# is set (app.py defaults it to "0" = off). test_bolt_cypher connects to :5251,
# so enable it here — the provisa_server subprocess inherits os.environ and the
# test client reads the same PROVISA_BOLT_PORT default (5251).
os.environ.setdefault("PROVISA_BOLT_PORT", "5251")
# Provision the e2e core stack under a DEDICATED compose project, isolated from
# the default `provisa` project used by the dev stack / installer. Without this
# the fixture's teardown (`down`) would tear down the developer's running stack,
# and conversely a dev-side restart would tear down the e2e stack mid-run — they
# would share containers, network, and lifecycle. A distinct project gives the
# e2e stack its own containers and network so its lifecycle is self-contained.
_PROJECT = os.environ.get("PROVISA_E2E_PROJECT", "provisa-e2e")

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

    # --wait blocks until every service with a healthcheck (postgres, redis,
    # trino, zaychik) reports healthy, so tests never start against a Trino that
    # is up but not yet query-ready. -p isolates the stack in its own project.
    subprocess.run(
        ["docker", "compose", "-p", _PROJECT, *_COMPOSE_ARGS, "up", "-d", "--wait", *_SERVICES],
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
        ["docker", "compose", "-p", _PROJECT, *_COMPOSE_ARGS, "down"],
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
