# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: Azure Synapse serverless SQL as a federation engine.

Synapse CAN land (a dedicated pool has managed tables; even serverless writes replicas to ADLS via
CETAS), but its most-used capability is the zero-copy external link: this test exercises that — the
ATTACH connector exposes a view over ``OPENROWSET`` of an ADLS Parquet, read via the Arrow path.
Synapse OPENROWSET reads Azure storage only (ADLS/Blob), not S3/R2. (Serverless objects must live in a
user database, not ``master`` — SYNAPSE_DATABASE points at one.)

Skipped without SYNAPSE_SQL_SERVER / SYNAPSE_DATABASE / SYNAPSE_ADLS_URL (an ADLS parquet the workspace
identity can read). pyodbc + the Microsoft ODBC driver + Azure AD (``az login``) required."""

from __future__ import annotations

import os

import pytest

pytestmark = [pytest.mark.integration]

pytest.importorskip("pyodbc", reason="pyodbc required")
pytest.importorskip("azure.identity", reason="azure-identity required")

_HAVE = bool(
    os.environ.get("SYNAPSE_SQL_SERVER")
    and os.environ.get("SYNAPSE_DATABASE")
    and os.environ.get("SYNAPSE_ADLS_URL")  # a Parquet on ADLS the Synapse identity can read
)
pytestmark.append(
    pytest.mark.skipif(not _HAVE, reason="Synapse creds / ADLS test parquet not set (SYNAPSE_*)")
)

from provisa.federation.mssql_warehouse_runtime import MssqlWarehouseRuntime  # noqa: E402


@pytest.fixture(scope="module")
def runtime():
    rt = MssqlWarehouseRuntime(
        server=os.environ["SYNAPSE_SQL_SERVER"],
        database=os.environ["SYNAPSE_DATABASE"],
        engine_name="synapse",
    )
    try:
        yield rt
    finally:
        cur = rt.connection.cursor()
        try:
            cur.execute("DROP VIEW IF EXISTS [provisa_ext_it].[ext]")
        finally:
            cur.close()
        rt.close()


def test_synapse_external_link_reads_adls_via_openrowset(runtime):
    from types import SimpleNamespace

    from provisa.core.models import SourceType

    # An ADLS Parquet reachable by the Synapse workspace identity (direct URL — no shortcut needed).
    src = SimpleNamespace(
        id="syn-ext",
        type=SourceType.parquet,
        schema_name="provisa_ext_it",
        table_name="ext",
        path=os.environ["SYNAPSE_ADLS_URL"],
        federation_hints={},
    )
    runtime.attach_source(src)  # CREATE VIEW over OPENROWSET(BULK '<adls>') + validate
    table = runtime.run_arrow("SELECT TOP 3 * FROM [provisa_ext_it].[ext]")
    assert table.num_rows >= 1  # read external ADLS data in place, zero-copy


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
