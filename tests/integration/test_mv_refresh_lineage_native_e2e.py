# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-862: a materialized view's refresh lineage, read through the governed name on a native
engine. The in-process app runs on DuckDB, which has no otel catalog: the app lands its own trace
records as Parquet and DuckDB reads them at otel.signals.traces, as Trino reads its Iceberg
table. Building a view records its refresh; "ops"."mv_refresh_lineage" answers it over /data/sql
with the versions it was built from."""

# Requirements: REQ-862, REQ-1910

import os
import tempfile

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.helpers import registered_id, release_mutation

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]

_ENV = ("PROVISA_MATERIALIZE_URL", "PROVISA_TELEMETRY_DIR", "OTEL_SDK_DISABLED")


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")
    saved = {name: os.environ.get(name) for name in _ENV}
    scratch = tempfile.mkdtemp(prefix="lineage_")
    os.environ["PROVISA_MATERIALIZE_URL"] = f"duckdb:///{os.path.join(scratch, 'mat.duckdb')}"
    os.environ["PROVISA_TELEMETRY_DIR"] = os.path.join(scratch, "telemetry")
    # Spans are recorded only with the SDK on.
    os.environ["OTEL_SDK_DISABLED"] = "false"
    try:
        from provisa.api.app import create_app

        app = create_app()
        async with app.router.lifespan_context(app):
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test",
                headers={"X-Provisa-Role": "org_admin"},
            ) as c:
                yield c
    finally:
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


async def _admin(client, query: str):
    r = await client.post("/admin/graphql", json={"query": query})
    assert r.status_code == 200, r.text
    return r.json()


async def test_a_views_refresh_is_read_back_through_the_lineage_view(client):
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider

    from provisa.api.app import state
    from provisa.mv.delta import definition_version_of

    # The precondition under test: a native engine reading the traces its own process lands.
    assert state.federation_engine.landed_signals == frozenset({"traces"})
    assert not state.federation_engine.has_otel_catalog

    await _admin(
        client, 'mutation { createDomain(input: { id: "lin", description: "x" }) { success } }'
    )
    visible = 'visibleTo: ["org_admin"]'
    reg = await _admin(
        client,
        f"""mutation {{ registerTable(input: {{
            sourceId: "__derived__", domainId: "lin", schemaName: "views",
            tableName: "lin_view", alias: "lin_view",
            viewSql: "SELECT 1 AS id, 10 AS amount", materialize: true,
            columns: [{{ name: "id", {visible} }}, {{ name: "amount", {visible} }}]
        }}) {{ success message }} }}""",
    )
    assert reg["data"]["registerTable"]["success"], reg
    # Released, the view is built: its refresh records its lineage.
    released = await _admin(
        client, release_mutation(registered_id(reg["data"]["registerTable"]["message"]))
    )
    assert released["data"]["setTableDraft"]["success"], released
    mv = state.mv_registry.get("view-lin_view")
    assert mv is not None
    # The native lane exports in batches: what was recorded is landed now.
    provider = trace.get_tracer_provider()
    assert isinstance(provider, TracerProvider)
    assert provider.force_flush()

    r = await client.post(
        "/data/sql",
        json={
            "sql": "SELECT mv_id, definition_version, input_version_kind, trace_id, tenant_id "
            'FROM "ops"."mv_refresh_lineage" WHERE mv_id = \'view-lin_view\'',
            "role": "org_admin",
        },
    )
    assert r.status_code == 200, r.text
    rows = r.json()["data"]["sql"]
    assert rows, r.text
    assert {row["definition_version"] for row in rows} == {definition_version_of(mv)}
    assert all(row["input_version_kind"] and len(row["trace_id"]) == 32 for row in rows)
    assert {row["tenant_id"] for row in rows} == {state.org_id}
