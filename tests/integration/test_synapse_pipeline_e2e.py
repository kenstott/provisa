# Copyright (c) 2026 Kenneth Stott
# Canary: 0f2c7d19-64a8-4d3e-9b51-8ac0f5e21d47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""True E2E for Azure Synapse as the federation ENGINE: registration through to a served query.

``test_synapse_federation_engine_e2e.py`` proves the engine terminal itself — ``attach_source``
creates the ``OPENROWSET`` view and ``run_arrow`` reads it — by driving ``MssqlWarehouseRuntime``
directly. That says nothing about whether Provisa can *serve* Synapse: the settled pipeline
(``_govern_and_route``/``_execute_plan``) never calls a runtime by hand, and rejects the physical
ref that test uses. This closes the gap over the same ephemeral lane, through the same harness the
connector bucket uses — see ``connector_source_harness`` for what the two steps in between prove.

Two things differ from every other module in that harness, both forced by Synapse being an engine
rather than a Trino connector:

**The engine pin.** ``connector_client`` pins ``PROVISA_ENGINE=trino`` because a connector source
exists only as a Trino catalog. Here the source *is* read by the warehouse, so the fixture below
pins ``synapse`` and the lane's server/database instead.

**Explicit column types.** Registration normally introspects, and the harness documents why that
matters. It cannot work for this source shape: the object whose ``information_schema`` would be
read is the ``OPENROWSET`` view, and that view is created by ``NativeEngineBackend._attach_registered``
(``provisa/federation/native_backend.py:76``) from ``state.tables`` — i.e. only *after* this
registration lands. So the types are steward-assigned, which is the designed path for a column the
engine cannot introspect at design time (``provisa/api/admin/types.py:363-366``).

Why ``database`` is the Synapse database name
---------------------------------------------
``createSource`` records ``state.source_catalogs[id]`` from ``input.database or
source_to_catalog(id)`` (``schema_mutation.py:377-385``), and that value is what the pipeline's
unknown-catalog gate checks and what the compiler emits. A fixed-catalog warehouse engine reads at
exactly ``<SYNAPSE_DATABASE>.<schema>.<table>`` (``app_loaders.py:200-208``), so ``database`` must
carry the lane's database name — with it empty the source registers cleanly and every query then
fails the gate.

The lane self-provisions: ``synapse_provision.synapse_lane`` creates a stamped resource group
holding an ADLS Gen2 account (seeded with the Parquet this reads) and a serverless workspace, then
deletes the group when the module finishes, so no Azure resource bills between runs.
"""

from __future__ import annotations

import os

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.requires_warehouse,
    pytest.mark.asyncio(loop_scope="session"),
]

pytest.importorskip("pyodbc", reason="pyodbc required")
pytest.importorskip("azure.identity", reason="azure-identity required")

from tests.integration.connector_source_harness import (  # noqa: E402
    assert_registration_and_query,
)
from tests.integration.synapse_provision import cli_ready, synapse_lane  # noqa: E402

_CLI_PROBLEM = cli_ready()
pytestmark.append(
    pytest.mark.skipif(bool(_CLI_PROBLEM), reason=f"Synapse lane cannot provision: {_CLI_PROBLEM}")
)

_SOURCE_ID = "synapse-ext"
_DOMAIN = "synapse_e2e"
_SCHEMA = "provisa_ext_pipe"
_TABLE = "orders"

# The rows synapse_provision seeds into the ADLS Parquet, and their steward-assigned IR types.
_COLUMN_TYPES = {"order_id": "bigint", "customer": "text", "amount": "double"}
_EXPECTED = [
    {"order_id": 1, "customer": "ada", "amount": 10.5},
    {"order_id": 2, "customer": "grace", "amount": 20.25},
    {"order_id": 3, "customer": "alan", "amount": 30.0},
]


@pytest.fixture(scope="module")
def lane():
    with synapse_lane() as coordinates:
        yield coordinates


@pytest_asyncio.fixture(loop_scope="session")
async def synapse_client(lane):
    """An in-process app whose federation engine is the lane's Synapse workspace.

    Same rebuild ``connector_client`` performs and for the same reason: ``AppState.__init__`` calls
    ``build_engine`` once at import of ``provisa.api.app``, long before any fixture runs, so setting
    ``PROVISA_ENGINE`` alone would leave the app on whatever engine the session started with. The
    env pin, the ``SYNAPSE_*`` coordinates and the previous engine are all restored on teardown so
    the rest of the session is unaffected.
    """
    import provisa.api.app as app_mod
    from provisa.federation.engine import build_engine
    from provisa.federation.runtime import EngineRuntime

    sql_server, database, _adls_url = lane
    pins = {
        "PROVISA_ENGINE": "synapse",
        "SYNAPSE_SQL_SERVER": sql_server,
        "SYNAPSE_DATABASE": database,
    }
    prior = {k: os.environ.get(k) for k in pins}
    os.environ.update(pins)
    os.environ.setdefault("PG_PASSWORD", "provisa")
    prior_engine = app_mod.state.federation_engine
    app_mod.state.federation_engine = EngineRuntime(build_engine(), app_mod.state)
    app_mod.state.federation_engine.bind_terminal()
    try:
        app = app_mod.create_app()
        async with app.router.lifespan_context(app):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test", timeout=180) as c:
                yield c
    finally:
        app_mod.state.federation_engine = prior_engine
        for key, value in prior.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


async def test_synapse_registers_and_serves_semantic_query(synapse_client, lane):
    """createSource(path=<adls>) → registerTable → rebuildSchemas → semantic SELECT.

    The source carries no host/port: Synapse reaches its data by object-store location, and the
    ``OPENROWSET`` view over that location is what the served query reads — zero-copy, in place.
    """
    _sql_server, database, adls_url = lane

    await assert_registration_and_query(
        synapse_client,
        source_id=_SOURCE_ID,
        source_type="parquet",
        host="",
        port=0,
        database=database,
        path=adls_url,
        domain_id=_DOMAIN,
        schema_name=_SCHEMA,
        table_name=_TABLE,
        alias="orders",
        columns=["order_id", "customer", "amount"],
        column_types=_COLUMN_TYPES,
        order_by="order_id",
        expected_rows=_EXPECTED,
    )


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
