# Copyright (c) 2026 Kenneth Stott
# Canary: 21419425-2d02-4fbd-bb3f-3f47cfd42ea2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: Trino TLS + PASSWORD-authenticated Arrow Flight (issue #74, auth half).

Proves that a Flight SQL client CAN authenticate to Trino with a real username +
password when the Trino hop is TLS — the root cause zaychik forwards the Flight
client's user:password straight to Trino over the Trino JDBC driver, and Trino
refuses password auth over plaintext (see tests/integration/
test_trino_flight_engine_e2e.py, which documents why Provisa's own Flight
connection intentionally uses an empty password on the plaintext core stack).

This test does NOT touch docker-compose.core.yml or trino/etc/* — enabling
PASSWORD auth on the shared/dev Trino would break every existing test that
connects user-only with an empty password. It stands up a fully separate,
dedicated stack (tests/fixtures/trino_tls/) with:
  - a self-signed TLS keystore for a `trino-tls` Trino node
    (tests/fixtures/trino_tls/keystore.p12, CN/SAN = trino-tls + localhost)
  - a TLS + `PASSWORD` (file authenticator) Trino config
  - a zaychik-tls image (zaychik:latest + the test CA imported into the JRE
    truststore, since zaychik's Trino JDBC client only exposes TF_TRINO_SSL and
    has no truststore knob — see Dockerfile.zaychik-tls) pointed at trino-tls
    over TLS; the Flight hop (zaychik -> Flight client) stays plaintext, since
    only the Trino hop is under test here.

Resolution recipe for #74 (the config that makes Trino accept password auth):
  trino config.properties:
    http-server.https.enabled=true
    http-server.https.port=8443
    http-server.https.keystore.path=/etc/trino/keystore.p12
    http-server.https.keystore.key=<keystore password>
    http-server.authentication.type=PASSWORD
    http-server.process-forwarded=true
    internal-communication.shared-secret=<any-shared-secret>
  password-authenticator.properties:
    password-authenticator.name=file
    file.password-file=/etc/trino/password.db   # user:bcrypt-hash lines
  zaychik env:
    TF_TRINO_SSL=true, TF_TRINO_PORT=8443, TF_TRINO_HOST=trino-tls
    (zaychik's JDBC client trusts only the JRE default truststore — the test
    CA must be `keytool -importcert`-ed into it, as there's no
    TF_TRINO_*TRUSTSTORE* env knob in zaychik-trino-proxy)
"""

from __future__ import annotations

import os
import socket
import subprocess
import time
import uuid

import pytest

pytestmark = [pytest.mark.integration]

_FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "..", "fixtures", "trino_tls")
_COMPOSE_FILE = os.path.join(_FIXTURE_DIR, "docker-compose.trino-tls.yml")

_FLIGHT_USER = "flightuser"
_FLIGHT_PASSWORD = "Provisa_2026!"


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _wait_healthy(project: str, service: str, timeout_s: int) -> None:
    """Poll `docker compose ps` until the named service reports (healthy)."""
    deadline = time.monotonic() + timeout_s
    last_status = ""
    while time.monotonic() < deadline:
        out = subprocess.run(
            ["docker", "compose", "-p", project, "-f", _COMPOSE_FILE, "ps", "--format", "{{.Service}} {{.Status}}"],
            cwd=_FIXTURE_DIR,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        for line in out.splitlines():
            if line.startswith(f"{service} "):
                last_status = line
                if "healthy" in line:
                    return
        time.sleep(2)
    raise RuntimeError(
        f"{service} did not become healthy within {timeout_s}s (last status: {last_status!r})"
    )


@pytest.fixture(scope="module")
def trino_tls_stack():
    """Bring up the isolated TLS Trino + zaychik-tls stack under a unique compose
    project name, and tear it down unconditionally — this test NEVER shares the
    conftest-managed core stack or its project name."""
    project = f"trino-tls-e2e-{uuid.uuid4().hex[:10]}"
    zaychik_port = _free_port()
    env = dict(os.environ, ZAYCHIK_TLS_PORT=str(zaychik_port))

    subprocess.run(
        ["docker", "compose", "-p", project, "-f", _COMPOSE_FILE, "up", "-d", "--build"],
        cwd=_FIXTURE_DIR,
        env=env,
        check=True,
    )
    try:
        # Trino TLS cold boot + zaychik JDBC-over-TLS handshake can take a couple
        # of minutes on a loaded CI host — generous, matching the task's guidance.
        _wait_healthy(project, "trino-tls", timeout_s=150)
        _wait_healthy(project, "zaychik-tls", timeout_s=60)
        yield {"zaychik_port": zaychik_port}
    finally:
        subprocess.run(
            ["docker", "compose", "-p", project, "-f", _COMPOSE_FILE, "down", "--volumes"],
            cwd=_FIXTURE_DIR,
            env=env,
            check=False,
        )


def _connect(port: int, user: str, password: str):
    import adbc_driver_flightsql.dbapi as flight_sql

    return flight_sql.connect(
        uri=f"grpc://localhost:{port}",
        db_kwargs={
            "username": user,
            "password": password,
            "adbc.flight.sql.client_option.authority": f"localhost:{port}",
        },
        autocommit=True,
    )


def test_password_authenticated_flight_over_tls_trino_succeeds(trino_tls_stack):
    """POSITIVE: real user + real password, over the TLS Trino hop -> rows come back."""
    from provisa.executor.trino_flight import execute_trino_flight_arrow

    port = trino_tls_stack["zaychik_port"]
    conn = _connect(port, _FLIGHT_USER, _FLIGHT_PASSWORD)
    try:
        table = execute_trino_flight_arrow(conn, "SELECT 1 AS x, 'tls-ok' AS y", None)
        assert table.num_rows == 1
        assert table.to_pydict() == {"x": [1], "y": ["tls-ok"]}
    finally:
        conn.close()


def test_wrong_password_over_tls_trino_is_rejected(trino_tls_stack):
    """NEGATIVE: wrong password must be rejected, proving auth is enforced, not bypassed."""
    from provisa.executor.trino_flight import execute_trino_flight_arrow

    port = trino_tls_stack["zaychik_port"]
    conn = _connect(port, _FLIGHT_USER, "definitely-not-the-password")
    try:
        # Trino rejects the wrong password at JDBC connect time inside zaychik,
        # which surfaces to the Flight client as an error on the first query
        # (never as a successful result set) — proving PASSWORD auth is actually
        # enforced on this TLS Trino, not silently bypassed.
        with pytest.raises(Exception):
            execute_trino_flight_arrow(conn, "SELECT 1", None)
    finally:
        conn.close()
