# Copyright (c) 2026 Kenneth Stott
# Canary: 9a5c7e13-4d28-4f06-b3a1-7e0c2d9f6b85
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Live cloud inventory source (REQ-1947): read through its pgwire server.

A ``Source`` row (type=cloudops) carries one or more clouds in ``mapping`` (see
``provisa/federation/cloudops.py``). ``provisa.federation.pgwire_replica`` writes the server's
model from them and starts the bundled ``pgwire-cloudops`` server from the source's own state
directory; every engine but Trino attaches it.

Credentials
-----------
There is no cloud emulator behind the adapter: a real AWS account is required, so this runs in the
warehouse lane. It reads CLOUDOPS_AWS_ACCESS_KEY_ID, CLOUDOPS_AWS_SECRET_ACCESS_KEY,
CLOUDOPS_AWS_REGION and CLOUDOPS_AWS_ACCOUNT_IDS. The prefix is the source's own: the plain AWS_*
variables of the lane belong to its object store, and the test asserts that a source naming one
cloud reads no other.
"""

from __future__ import annotations

import os

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_cloudops,
    pytest.mark.requires_warehouse,
]

_aio = pytest.mark.asyncio(loop_scope="session")

_SOURCE_ID = "cloudops-itest"
_SCHEMA = _SOURCE_ID.replace("-", "_")
_TABLES = {
    "compute_resources",
    "storage_resources",
    "kubernetes_clusters",
    "container_registries",
    "network_resources",
    "iam_resources",
    "database_resources",
}


def _source():
    from provisa.core.models import Source, SourceType

    return Source(
        id=_SOURCE_ID,
        type=SourceType.cloudops,
        mapping={
            "aws_access_key_id": os.environ["CLOUDOPS_AWS_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["CLOUDOPS_AWS_SECRET_ACCESS_KEY"],
            "aws_region": os.environ["CLOUDOPS_AWS_REGION"],
            "aws_account_ids": os.environ["CLOUDOPS_AWS_ACCOUNT_IDS"],
        },
    )


@pytest.fixture(scope="module")
def endpoint(tmp_path_factory):
    """The source's pgwire server, started from a state directory of this run's own."""
    from provisa.federation import pgwire_replica as pr

    previous = os.environ.get("PROVISA_DATA_DIR")
    os.environ["PROVISA_DATA_DIR"] = str(tmp_path_factory.mktemp("cloudops-state"))
    try:
        yield pr.ensure_endpoint(_source())
    finally:
        pr.stop_endpoint(_SOURCE_ID)
        if previous is None:
            del os.environ["PROVISA_DATA_DIR"]
        else:
            os.environ["PROVISA_DATA_DIR"] = previous


@_aio
async def test_the_inventory_tables_are_offered(endpoint):
    from provisa.federation import pgwire_replica as pr

    conn = await pr._pg_connect(endpoint.calcite_child_host, endpoint.pgwire_port)
    try:
        rows = await conn.fetch(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = $1", _SCHEMA
        )
    finally:
        await conn.close()
    assert _TABLES <= {r["table_name"] for r in rows}


@_aio
async def test_a_source_naming_aws_reads_aws_and_no_other_cloud(endpoint):
    from provisa.federation import pgwire_replica as pr

    conn = await pr._pg_connect(endpoint.calcite_child_host, endpoint.pgwire_port)
    try:
        rows = await conn.fetch(
            f'SELECT DISTINCT "cloud_provider" FROM {_SCHEMA}."storage_resources"'
        )
    finally:
        await conn.close()
    assert {str(r["cloud_provider"]).lower() for r in rows} <= {"aws"}
