# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Endpoint-level ABAC approval-hook integration test (REQ-203).

Drives a governed query through the live /data/graphql endpoint with a stub
ApprovalHook injected into AppState, executing against a running Postgres source.
Pins the three contractual behaviours of the hook seam in ``_prepare_compiled``:

  1. The hook is evaluated for the query, receiving a payload that reflects the
     governed query (tables + requested columns) — it runs AFTER RLS/visibility.
  2. A denial (``approved=False``) aborts execution with HTTP 403.
  3. An ``additional_filter`` is ANDed into the governed WHERE clause and
     therefore narrows the real result set.

The query targets a single Postgres source, so it routes direct-to-PG (no Trino).
This is distinct from Apollo APQ (REQ-288-291) and from the removed query
registry — access here is purely rights- + hook-based.
"""

from __future__ import annotations

import os

import asyncpg
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]

_PG_HOST: str = os.environ.get("PG_HOST", "localhost")
_PG_PORT: int = int(os.environ.get("PG_PORT", "5432"))
_PG_DB: str = os.environ.get("PG_DATABASE", "provisa")
_PG_USER: str = os.environ.get("PG_USER", "provisa")
_PG_PASSWORD: str = os.environ.get("PG_PASSWORD", "provisa")


async def _pg_ready() -> bool:
    try:
        conn = await asyncpg.connect(
            host=_PG_HOST,
            port=_PG_PORT,
            database=_PG_DB,
            user=_PG_USER,
            password=_PG_PASSWORD,
            timeout=3,
        )
    except Exception:
        return False
    try:
        return await conn.fetchval("select to_regclass('public.orders')") is not None
    finally:
        await conn.close()


def _build_schema():
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.compiler.schema_gen import SchemaInput, generate_schema
    from provisa.compiler.sql_gen import build_context

    tables = [
        {
            "id": 1,
            "source_id": "test-pg",
            "domain_id": "default",
            "schema_name": "public",
            "table_name": "orders",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
            ],
        }
    ]
    column_types = {
        1: [
            ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
            ColumnMetadata(column_name="region", data_type="varchar(50)", is_nullable=False),
            ColumnMetadata(column_name="amount", data_type="numeric", is_nullable=False),
        ]
    }
    role = {"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]}
    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types=column_types,
        naming_rules=[],
        role=role,
        domains=[{"id": "default", "description": "Default"}],
        source_types={"test-pg": "postgresql"},
    )
    return generate_schema(si), build_context(si), role


def _orders_field(schema) -> str:
    fields = schema.query_type.fields  # type: ignore[union-attr]
    for name in fields:
        if name.endswith("orders") or "orders" in name:
            return name
    raise AssertionError(f"no orders query field in {list(fields)}")


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client_field():
    os.environ.setdefault("PG_PASSWORD", "provisa")
    assert await _pg_ready(), (
        "Postgres source with public.orders not reachable on localhost:5432"
    )

    import provisa.api.app as appmod
    from fastapi import FastAPI
    from provisa.api.app import AppState
    from provisa.api.data.endpoint import router as data_router
    from provisa.compiler.rls import RLSContext
    from provisa.executor.pool import SourcePool

    schema, ctx, role = _build_schema()
    field_name = _orders_field(schema)

    source_pool = SourcePool()
    await source_pool.add(
        "test-pg",
        source_type="postgresql",
        host=_PG_HOST,
        port=_PG_PORT,
        database=_PG_DB,
        user=_PG_USER,
        password=_PG_PASSWORD,
    )

    st = AppState()
    st.schemas = {"admin": schema}
    st.contexts = {"admin": ctx}
    st.rls_contexts = {"admin": RLSContext.empty()}
    st.roles = {"admin": role}
    st.source_pools = source_pool
    st.source_types = {"test-pg": "postgresql"}
    st.source_dialects = {"test-pg": "postgres"}
    st.masking_rules = {}

    # The /data/graphql handler reads the module-global AppState; swap it in.
    prev_state = appmod.state
    appmod.state = st

    app = FastAPI()
    app.include_router(data_router)
    transport = ASGITransport(app=app)
    try:
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c, field_name, st
    finally:
        appmod.state = prev_state
        await source_pool.close_all()


# --- Stub hook -------------------------------------------------------------


def _make_hook(response):
    from provisa.auth.approval_hook import ApprovalHook

    class _RecordingHook(ApprovalHook):
        def __init__(self):
            self.requests = []

        async def evaluate(self, request):
            self.requests.append(request)
            return response

    return _RecordingHook()


def _install_hook(st, hook):
    """Inject a hook (scope=all) into the AppState; return a reset callable."""
    from provisa.auth.approval_hook import ApprovalHookConfig

    prev_hook = st.approval_hook
    prev_cfg = st.approval_hook_config
    st.approval_hook = hook
    st.approval_hook_config = ApprovalHookConfig(scope="all")

    def _reset():
        st.approval_hook = prev_hook
        st.approval_hook_config = prev_cfg

    return _reset


async def _query(client, field_name, fields="id region"):
    return await client.post(
        "/data/graphql",
        json={"query": f"{{ {field_name} {{ {fields} }} }}"},
        headers={"X-Provisa-Role": "admin"},
    )


class TestApprovalHookEndpoint:
    async def test_baseline_without_hook_returns_rows(self, client_field):
        """Sanity: with no hook installed the endpoint returns the full set."""
        client, field_name, _ = client_field
        resp = await _query(client, field_name)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert "errors" not in body, body.get("errors")
        rows = body["data"][field_name]
        assert len(rows) >= 1

    async def test_hook_is_evaluated_with_governed_payload(self, client_field):
        """REQ-203: the hook is called, and its payload reflects the governed query."""
        from provisa.auth.approval_hook import ApprovalResponse

        client, field_name, st = client_field
        hook = _make_hook(ApprovalResponse(approved=True))
        reset = _install_hook(st, hook)
        try:
            resp = await _query(client, field_name)
            assert resp.status_code == 200, resp.text
        finally:
            reset()

        assert len(hook.requests) == 1
        req = hook.requests[0]
        assert req.operation == "query"
        assert req.user == "admin"
        assert "admin" in req.roles
        # Payload reflects the compiled/governed query, not the raw request.
        assert req.tables, "hook payload missing table ids"
        assert "id" in req.columns and "region" in req.columns

    async def test_denial_yields_403(self, client_field):
        """REQ-203: a hook denial aborts execution with HTTP 403."""
        from provisa.auth.approval_hook import ApprovalResponse

        client, field_name, st = client_field
        hook = _make_hook(ApprovalResponse(approved=False, reason="not allowed here"))
        reset = _install_hook(st, hook)
        try:
            resp = await _query(client, field_name)
        finally:
            reset()

        assert resp.status_code == 403, resp.text
        assert "not allowed here" in resp.text

    async def test_additional_filter_narrows_results(self, client_field):
        """REQ-203: additional_filter is ANDed in and narrows the real result set."""
        from provisa.auth.approval_hook import ApprovalResponse

        client, field_name, st = client_field
        hook = _make_hook(ApprovalResponse(approved=True, additional_filter="region = 'us-east'"))
        reset = _install_hook(st, hook)
        try:
            resp = await _query(client, field_name)
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert "errors" not in body, body.get("errors")
            rows = body["data"][field_name]
        finally:
            reset()

        assert len(rows) == 9, f"filter did not narrow to us-east: {len(rows)} rows"
        assert all(r["region"] == "us-east" for r in rows)
