# Copyright (c) 2026 Kenneth Stott
# Canary: 9c3f7a41-5b28-4d6e-8f10-6a2d94b3ce77
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""True E2E for Cassandra as a connector-source: registration through to a served query.

``test_cassandra_source_e2e.py`` proves Trino can reach Cassandra. This proves Provisa can, which
is a strictly larger claim — see ``connector_source_harness`` for why the gap between the two is
where a connector source actually fails (introspection-driven registration, and agreement between
the recorded catalog name and the physically created one).

Cassandra is the pathfinder for the connector-only family because it boots fastest and its Trino
connector is a first-party one with a conventional ``information_schema``; if the harness cannot
drive this source, the harness is wrong rather than the source.
"""

from __future__ import annotations

import subprocess
import time

import pytest

from tests.integration.connector_source_harness import (
    assert_registration_and_query,
    connector_client,  # noqa: F401 — imported for pytest fixture discovery
)
from tests.itest_stack import ITEST_PROJECT as _ITEST_PROJECT

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]

_KEYSPACE = "provisa_pipeline"
_TABLE = "widgets"
_WIDGETS = [(1, "Widget A"), (2, "Widget B"), (3, "Widget C")]

# The source id doubles as the catalog name (``source_to_catalog`` only maps ``-`` → ``_``), and
# ``database`` is left empty so ``createSource``'s recorded catalog and ``create_catalog``'s
# physical one cannot diverge. Keeping them in agreement is the point of the assertion, not an
# incidental choice — see the harness docstring on schema_mutation.py:378-381.
_SOURCE_ID = "cassandra_pipeline"
_DOMAIN = "cass_e2e"


def _cassandra_container_id() -> str:
    out = subprocess.run(
        [
            "docker",
            "ps",
            "-q",
            "--filter",
            f"label=com.docker.compose.project={_ITEST_PROJECT}",
            "--filter",
            "label=com.docker.compose.service=cassandra",
        ],  # fmt: skip
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    ids = out.splitlines()
    if not ids:
        raise RuntimeError(f"No running cassandra container for project {_ITEST_PROJECT!r}")
    return ids[0]


def _cqlsh(container_id: str, statement: str) -> None:
    subprocess.run(
        ["docker", "exec", container_id, "cqlsh", "-e", statement],
        capture_output=True,
        text=True,
        check=True,
    )


def _seed_cassandra() -> None:
    """Create the keyspace/table and insert 3 rows, retrying while schema agreement settles."""
    container_id = _cassandra_container_id()
    ddl = (
        f"CREATE KEYSPACE IF NOT EXISTS {_KEYSPACE} "
        "WITH replication = {'class':'SimpleStrategy','replication_factor':1}"
    )
    deadline = time.monotonic() + 60
    last_err: subprocess.CalledProcessError | None = None
    while time.monotonic() < deadline:
        try:
            _cqlsh(container_id, ddl)
            break
        except subprocess.CalledProcessError as e:
            last_err = e
            time.sleep(3)
    else:
        raise RuntimeError(f"cassandra keyspace creation never succeeded: {last_err!r}")

    _cqlsh(
        container_id,
        f"CREATE TABLE IF NOT EXISTS {_KEYSPACE}.{_TABLE} (id int PRIMARY KEY, name text)",
    )
    for wid, name in _WIDGETS:
        _cqlsh(
            container_id, f"INSERT INTO {_KEYSPACE}.{_TABLE} (id, name) VALUES ({wid}, '{name}')"
        )


@pytest.mark.requires_cassandra
async def test_cassandra_registers_and_serves_semantic_query(connector_client):  # noqa: F811
    """createSource → registerTable (types introspected) → rebuildSchemas → semantic SELECT.

    ``host="cassandra"`` is the compose service name: Trino dials it on the isolated stack's
    private network. The test process never opens a Cassandra connection itself — seeding goes
    through ``cqlsh`` inside the container — so nothing here depends on a host-published port.
    """
    _seed_cassandra()

    await assert_registration_and_query(
        connector_client,
        source_id=_SOURCE_ID,
        source_type="cassandra",
        host="cassandra",
        port=9042,
        domain_id=_DOMAIN,
        schema_name=_KEYSPACE,
        table_name=_TABLE,
        columns=["id", "name"],
        order_by="id",
        expected_rows=[{"id": i, "name": n} for i, n in _WIDGETS],
    )
