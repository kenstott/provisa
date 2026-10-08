# Copyright (c) 2026 Kenneth Stott
# Canary: 1a9c5e83-7f24-4b6d-8e30-d2b7f4a6c951
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The JDBC driver against a real server with auth enforced (REQ-293, REQ-131, REQ-1263).

The driver's own integration tests are run by Maven against one real server.
``FlightTransportIT`` signs in with a user name and password, runs a query over Arrow Flight with
the session token in its ticket, and is refused by name for a role the user does not hold.
``ProvisaDriverIT`` reads the catalog metadata (tables, columns, keys) and a table's rows, and
checks that a user whose role is not served a table sees it in neither getTables nor getColumns. This file starts the server and hands Maven its
address; the assertions are the ITs'.

Needs ``mvn`` and a JDK 21 on the PATH (the core lane's job sets them up). Lands on the TEST
instance only: one real server over a database the harness creates."""

# Requirements: REQ-293, REQ-131, REQ-1263

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import bcrypt
import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_PASSWORD = "correct horse"
_DRIVER = Path(__file__).resolve().parents[2] / "jdbc-driver"


@pytest.fixture(scope="module")
def server():
    base = _config(_PG_HOST, _PG_PORT, "unused")
    orders = base["tables"][0]
    for column in orders["columns"]:
        column["visible_to"] = ["seller", "org_admin"]
    # Served to org_admin only: the seller's catalog must not list it.
    payroll = {
        "source_id": "sales-pg",
        "domain_id": "sales",
        "schema": "public",
        "table": "payroll",
        "columns": [{"name": "id", "data_type": "integer", "visible_to": ["org_admin"]}],
    }
    hashed = bcrypt.hashpw(_PASSWORD.encode(), bcrypt.gensalt()).decode()
    reads = ["query_development", "full_results"]
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "auth": {
                "provider": "simple",
                "allow_simple_auth": True,
                "jwt_secret": "jdbc-driver-e2e-test-signing-key",
                "default_role": "seller",
                "simple": {
                    "users": [
                        {"username": "sam", "password_hash": hashed, "roles": ["seller"]},
                        # Reads the registered catalog for the metadata calls.
                        {"username": "ada", "password_hash": hashed, "roles": ["org_admin"]},
                    ]
                },
            },
            "tables": [orders, payroll],
            # org_admin is the reserved administrative role (REQ-1349): not declared.
            "roles": [
                {"id": "seller", "capabilities": reads, "domain_access": ["*"]},
                {"id": "hr_reader", "capabilities": reads, "domain_access": ["*"]},
            ],
        },
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    own = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
    with own.connect() as conn:
        conn.execute(sa.text("CREATE TABLE public.payroll (id integer PRIMARY KEY)"))
    own.dispose()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _cached_repository() -> list[str]:
    """Maven's read-only tail (3.9): what the machine's ~/.m2 already holds resolves from there
    and is never written, so a cached lane does not download everything again. Absent on a
    machine that has no ~/.m2, where Maven fetches what it needs."""
    cached = Path.home() / ".m2" / "repository"
    return [f"-Dmaven.repo.local.tail={cached}"] if cached.is_dir() else []


def test_the_drivers_integration_tests_pass_against_an_authenticated_server(
    server, tmp_path_factory
):
    url = f"jdbc:provisa://127.0.0.1:{server.ports['http']}?flight_port={server.ports['flight']}"
    run = subprocess.run(
        [
            "mvn",
            "-B",
            # Its own local repository: a test must not write to a directory it does not own.
            f"-Dmaven.repo.local={tmp_path_factory.mktemp('m2')}",
            *_cached_repository(),
            "-f",
            str(_DRIVER / "pom.xml"),
            "verify",
            # `verify` runs the unit tests first (stubs only, a few seconds), then this IT.
            "-Dit.test=FlightTransportIT,ProvisaDriverIT",
            f"-Dprovisa.url={url}",
            "-Dprovisa.user=sam",
            f"-Dprovisa.password={_PASSWORD}",
            "-Dprovisa.sql=SELECT id, region FROM sales.orders",
            "-Dprovisa.unheldRole=hr_reader",
            "-Dprovisa.adminUser=ada",
            f"-Dprovisa.adminPassword={_PASSWORD}",
            # Named, because in claims mode a call that names no role acts as default_role.
            "-Dprovisa.adminRole=org_admin",
            "-Dprovisa.table=orders",
            "-Dprovisa.columns=id,region",
            "-Dprovisa.hiddenTable=payroll",
        ],
        capture_output=True,
        text=True,
        timeout=900,
        check=False,
    )
    output = run.stdout[-6000:] + run.stderr[-2000:]
    assert run.returncode == 0, output
    # Every test of both ITs ran (3 + 6) — none was skipped for want of a property.
    assert "Tests run: 9, Failures: 0, Errors: 0, Skipped: 0" in run.stdout, output
