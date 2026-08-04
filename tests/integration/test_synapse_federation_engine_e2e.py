# Copyright (c) 2026 Kenneth Stott
# Canary: 98bacd57-5d26-4639-8b80-f3bc72c46654
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: Azure Synapse serverless SQL as a federation engine.

Synapse CAN land (a dedicated pool has managed tables; even serverless writes replicas to ADLS via
CETAS), but its most-used capability is the zero-copy external link: this test exercises that -- the
ATTACH connector exposes a view over ``OPENROWSET`` of an ADLS Parquet, read via the Arrow path.
Synapse OPENROWSET reads Azure storage only (ADLS/Blob), not S3/R2. (Serverless objects must live in a
user database, not ``master`` -- the provisioned database is one.)

The lane self-provisions: ``synapse_provision.synapse_lane`` creates a stamped resource group holding
an ADLS Gen2 account (seeded with the Parquet this reads) and a serverless workspace, then deletes the
group when the module's tests finish, so no Azure resource bills between runs. Set SYNAPSE_SQL_SERVER
/ SYNAPSE_DATABASE / SYNAPSE_ADLS_URL to point at a standing workspace instead. pyodbc + the Microsoft
ODBC driver + an ``az login`` session are required."""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.requires_warehouse]

pytest.importorskip("pyodbc", reason="pyodbc required")
pytest.importorskip("azure.identity", reason="azure-identity required")

from tests.integration.synapse_provision import cli_ready, synapse_lane  # noqa: E402

_CLI_PROBLEM = cli_ready()
pytestmark.append(
    pytest.mark.skipif(bool(_CLI_PROBLEM), reason=f"Synapse lane cannot provision: {_CLI_PROBLEM}")
)

from provisa.federation.mssql_warehouse_runtime import MssqlWarehouseRuntime  # noqa: E402


@pytest.fixture(scope="module")
def lane():
    with synapse_lane() as coordinates:
        yield coordinates


@pytest.fixture(scope="module")
def runtime(lane):
    sql_server, database, _adls_url = lane
    rt = MssqlWarehouseRuntime(server=sql_server, database=database, engine_name="synapse")
    try:
        yield rt
    finally:
        # Dropped explicitly even though teardown deletes the whole resource group: with a pinned
        # standing workspace (SYNAPSE_SQL_SERVER set) there is no group delete, and a leftover view
        # would make the next run's CREATE OR ALTER pass over stale state.
        cur = rt.connection.cursor()
        try:
            cur.execute("DROP VIEW IF EXISTS [provisa_ext_it].[ext]")
        finally:
            cur.close()
        rt.close()


def test_synapse_external_link_reads_adls_via_openrowset(runtime, lane):
    from types import SimpleNamespace

    from provisa.core.models import SourceType

    _sql_server, _database, adls_url = lane
    # An ADLS Parquet reachable by the workspace identity (direct URL -- no shortcut needed).
    src = SimpleNamespace(
        id="syn-ext",
        type=SourceType.parquet,
        schema_name="provisa_ext_it",
        table_name="ext",
        path=adls_url,
        federation_hints={},
    )
    runtime.attach_source(src)  # CREATE VIEW over OPENROWSET(BULK '<adls>') + validate
    table = runtime.run_arrow("SELECT TOP 3 * FROM [provisa_ext_it].[ext]")
    assert table.num_rows == 3  # read the seeded external ADLS data in place, zero-copy


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
