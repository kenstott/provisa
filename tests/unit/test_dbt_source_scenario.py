# Copyright (c) 2026 Kenneth Stott
# Canary: 3fd19a74-b6c8-4e22-90e1-d8c7a05e3b16
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2228's scenario, on a real dbt manifest and a real control-plane database.

The manifest is ``tests/fixtures/dbt/manifest.json`` (see ``test_dbt_source``). The control plane
is the real schema on SQLite; the only stand-in is the schema rebuild, which needs a running engine.
"""

# Requirements: REQ-2228

from __future__ import annotations

import copy
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import insert, select

from provisa.api.admin._dbt_table_registration import (
    model_columns,
    offered_schemas,
    offered_tables,
    read_through_warehouse,
)
from provisa.api.admin.dbt_router import (
    DbtRefreshRequest,
    DbtRegisterRequest,
    DbtSpec,
    preview_dbt_manifest,
    refresh_dbt_source,
    register_dbt_source,
)
from provisa.api.errors import ApiError
from provisa.core import schema_org, secrets_providers
from provisa.core.database import Database, create_engine_from_url
from provisa.core.models import Source, SourceType
from provisa.core.repositories import source as source_repo
from provisa.dbt.store import command_target, lineage_views
from provisa.executor.source_operation import offered_operations
from provisa.lineage.merge import build_federation_graph

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _single_org(monkeypatch: pytest.MonkeyPatch) -> None:
    """source_repo.upsert calls sandbox_egress.pinned_source → many_organisations(); guard
    against xdist worker ordering leaving _many_organisations=None (REQ-2228 scenario tests)."""
    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)


FIXTURE = Path(__file__).parent.parent / "fixtures" / "dbt" / "manifest.json"
SOURCE = "dbt-jaffle"
WAREHOUSE = "wh-jaffle"


@pytest.fixture(name="document")
def _document() -> dict:
    return json.loads(FIXTURE.read_text())


@pytest.fixture(name="db")
def _db() -> Database:
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="dbt-source")
    with db.engine.begin() as conn:
        schema_org.metadata.create_all(
            conn,
            tables=[
                schema_org.domains,
                schema_org.sources,
                schema_org.registered_tables,
                schema_org.table_columns,
                schema_org.dbt_manifests,
                schema_org.dbt_node_runs,
            ],
        )
    return db


@pytest.fixture(name="state")
def _state(db: Database, monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    import provisa.api.admin.dbt_router as router
    import provisa.api.admin.schema_helpers as helpers
    import provisa.api.app as app

    state = app.state
    monkeypatch.setattr(state, "model_db", db, raising=False)
    monkeypatch.setattr(state, "source_types", {}, raising=False)
    monkeypatch.setattr(state, "multitenancy", False, raising=False)
    monkeypatch.setattr(app, "_rebuild_schemas", AsyncMock())
    monkeypatch.setattr(router, "require_capability_request", lambda *a, **k: None)

    async def get_pool() -> Database:
        return db

    monkeypatch.setattr(helpers, "_get_pool", get_pool)
    return state


async def _add_warehouse(db: Database, database_path: str = "/data/jaffle.duckdb") -> None:
    async with db.acquire() as conn:
        await source_repo.upsert(
            conn, Source(id=WAREHOUSE, type=SourceType.duckdb, path=database_path)
        )


async def _add_domain(db: Database) -> None:
    async with db.acquire() as conn:
        await conn.execute_core(insert(schema_org.domains).values(id="sales", description=""))


async def _add_source(state: SimpleNamespace, document: dict, **spec: Any) -> None:
    body = DbtRegisterRequest(
        source_id=SOURCE,
        spec="upload",
        content=json.dumps(document),
        command_line="dbt --project-dir /srv/jaffle",
        **spec,
    )
    await register_dbt_source(SimpleNamespace(), body)  # pyright: ignore[reportArgumentType]
    state.source_types[SOURCE] = "dbt"
    state.source_types[WAREHOUSE] = "duckdb"


def _table_input(**overrides: Any) -> SimpleNamespace:
    columns = [
        SimpleNamespace(name="customer_id", description=None),
        SimpleNamespace(name="lifetime_value", description="As the steward put it."),
    ]
    return SimpleNamespace(
        **{
            "source_id": SOURCE,
            "schema_name": "analytics",
            "table_name": "customer_orders",
            "description": None,
            "columns": columns,
            **overrides,
        }
    )


async def _registered(db: Database) -> list[Any]:
    async with db.acquire() as conn:
        return (await conn.execute_core(select(schema_org.registered_tables))).fetchall()


async def _steward_registers(db: Database, input: SimpleNamespace) -> None:
    """What the mutation stores once ``read_through_warehouse`` has rewritten ``input``."""
    await _add_domain(db)
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(schema_org.registered_tables).values(
                source_id=input.source_id,
                domain_id="sales",
                schema_name=input.schema_name,
                table_name=input.table_name,
                description=input.description,
            )
        )
        table_id = (await conn.execute_core(select(schema_org.registered_tables.c.id))).scalar()
        for column in input.columns:
            await conn.execute_core(
                insert(schema_org.table_columns).values(
                    table_id=table_id, column_name=column.name, description=column.description
                )
            )


async def test_a_new_source_offers_its_models_and_registers_nothing(state, db, document):
    await _add_warehouse(db)
    await _add_source(state, document)

    async with db.acquire() as conn:
        assert await offered_schemas(SOURCE, conn) == ["analytics"]
        tables = await offered_tables(SOURCE, "analytics", conn)
    assert {t.name for t in tables} == {"customer_orders", "order_events", "stg_orders"}
    assert next(t for t in tables if t.name == "customer_orders").comment == (
        "One row per customer with lifetime value."
    )
    assert await _registered(db) == []


async def test_a_registered_model_is_a_table_of_the_warehouse_the_manifest_names(
    state, db, document
):
    await _add_warehouse(db)
    await _add_source(state, document)

    table = _table_input()
    assert await read_through_warehouse(table) is None  # pyright: ignore[reportArgumentType]

    assert table.source_id == WAREHOUSE
    assert (table.schema_name, table.table_name) == ("analytics", "customer_orders")
    assert table.description == "One row per customer with lifetime value."
    described = {c.name: c.description for c in table.columns}
    assert described["customer_id"] == "The customer's identifier."
    # What the steward wrote stands.
    assert described["lifetime_value"] == "As the steward put it."


async def test_another_sources_registration_is_left_alone(state, db, document):
    await _add_source(state, document)
    state.source_types["pg"] = "postgresql"
    table = _table_input(source_id="pg", description=None)
    assert await read_through_warehouse(table) is None  # pyright: ignore[reportArgumentType]
    assert table.source_id == "pg"
    assert table.description is None


async def test_a_model_the_warehouse_cannot_read_is_refused_by_name(state, db, document):
    await _add_source(state, document)
    refused = await read_through_warehouse(_table_input())  # pyright: ignore[reportArgumentType]
    assert refused is not None and refused.code == "dbt.no_warehouse"
    assert refused.params == {"adapter": "duckdb", "database": "jaffle"}


async def test_a_model_the_manifest_does_not_hold_is_refused(state, db, document):
    await _add_warehouse(db)
    await _add_source(state, document)
    refused = await read_through_warehouse(  # pyright: ignore[reportArgumentType]
        _table_input(table_name="nope")
    )
    assert refused is not None and refused.code == "dbt.model_not_offered"


async def test_lineage_runs_through_the_compiled_sql_of_a_registered_model(state, db, document):
    await _add_warehouse(db)
    await _add_source(state, document)
    table = _table_input()
    await read_through_warehouse(table)  # pyright: ignore[reportArgumentType]
    await _steward_registers(db, table)

    async with db.acquire() as conn:
        rows = await lineage_views(conn)
    views = [(r["relation"], r["view_sql"]) for r in rows]
    assert "sales.customer_orders" in {relation for relation, _ in views}
    graph = build_federation_graph(views).graph
    assert any(
        e.target == "sales.customer_orders.lifetime_value"
        and e.source == "analytics.stg_orders.amount"
        for e in graph.edges
    )


async def test_each_model_test_and_snapshot_is_offered_as_a_mutation_command(state, db, document):
    await _add_warehouse(db)
    await _add_source(state, document)

    offered = {op.name: op for op in await offered_operations(state, SOURCE) or []}
    assert {"run_customer_orders", "run_order_events", "snapshot_orders_snapshot"} <= set(offered)
    assert sum(1 for name in offered if name.startswith("test_")) == 3
    assert not any(op.reads for op in offered.values())


async def test_a_core_source_runs_its_commands_where_dbt_is_installed(state, db, document):
    await _add_source(state, document)
    kind, binding = await command_target(db, SOURCE, "run_order_events")
    assert kind == "script"
    assert binding["argv"] == [
        "dbt",
        "--project-dir",
        "/srv/jaffle",
        "run",
        "--select",
        "order_events",
    ]


async def test_a_command_the_manifest_no_longer_offers_is_refused(state, db, document):
    await _add_source(state, document)
    with pytest.raises(ApiError) as refused:
        await command_target(db, SOURCE, "run_gone")
    assert refused.value.code == "functions.operation_not_offered"


async def test_a_new_manifest_updates_descriptions_lineage_and_commands(state, db, document):
    await _add_warehouse(db)
    await _add_source(state, document)
    table = _table_input()
    await read_through_warehouse(table)  # pyright: ignore[reportArgumentType]
    await _steward_registers(db, table)
    assert "run_gone" not in {op.name for op in await offered_operations(state, SOURCE) or []}

    newer = copy.deepcopy(document)
    model = newer["nodes"]["model.jaffle.customer_orders"]
    model["description"] = "Rewritten upstream."
    model["columns"]["customer_id"]["description"] = "Identifier, rewritten."
    model["compiled_code"] = model["compiled_code"].replace("sum(o.amount)", "max(o.status)")
    newer["nodes"]["model.jaffle.gone"] = {
        **newer["nodes"]["model.jaffle.order_events"],
        "unique_id": "model.jaffle.gone",
        "name": "gone",
        "alias": "gone",
    }
    answer = await refresh_dbt_source(
        SimpleNamespace(),  # pyright: ignore[reportArgumentType]
        SOURCE,
        DbtRefreshRequest(content=json.dumps(newer)),
    )
    assert answer["models"] == 4

    row = (await _registered(db))[0]
    assert row.description == "Rewritten upstream."
    async with db.acquire() as conn:
        columns = {
            c.column_name: c.description
            for c in (await conn.execute_core(select(schema_org.table_columns))).fetchall()
        }
    assert columns["customer_id"] == "Identifier, rewritten."
    assert {op.name for op in await offered_operations(state, SOURCE) or []} >= {"run_gone"}

    async with db.acquire() as conn:
        rows = await lineage_views(conn)
    graph = build_federation_graph([(r["relation"], r["view_sql"]) for r in rows]).graph
    assert any(
        e.target == "sales.customer_orders.lifetime_value"
        and e.source == "analytics.stg_orders.status"
        for e in graph.edges
    )


async def test_the_preview_finds_a_warehouse_or_offers_to_create_one(state, db, document):
    body = DbtSpec(spec="upload", content=json.dumps(document))
    request: Any = SimpleNamespace()

    missing = await preview_dbt_manifest(request, body)
    assert missing["warehouses"] == [
        {"database": "jaffle", "source_type": "duckdb", "source_id": None}
    ]
    assert missing["models"] == 3

    await _add_warehouse(db)
    found = await preview_dbt_manifest(request, body)
    assert found["warehouses"][0]["source_id"] == WAREHOUSE


async def test_a_document_that_is_no_manifest_is_refused_at_registration(state, db):
    with pytest.raises(ApiError) as refused:
        await register_dbt_source(
            SimpleNamespace(),  # pyright: ignore[reportArgumentType]
            DbtRegisterRequest(source_id=SOURCE, spec="upload", content='{"openapi": "3"}'),
        )
    assert refused.value.code == "dbt.not_a_manifest"


async def test_the_token_is_held_as_entered_and_never_listed(state, db, document, monkeypatch):
    import provisa.api.admin.dbt_router as router

    monkeypatch.setattr(router, "latest_manifest", AsyncMock(return_value=json.dumps(document)))
    monkeypatch.setattr(
        router,
        "latest_run_results",
        AsyncMock(
            return_value=json.dumps(
                {
                    "metadata": {
                        "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v6.json",
                        "generated_at": "2026-11-09T10:00:00Z",
                    },
                    "results": [],
                }
            )
        ),
    )
    monkeypatch.setattr(router, "resolved_credential", AsyncMock(return_value="resolved"))
    entered = "${secret:dbt_cloud}"
    await register_dbt_source(
        SimpleNamespace(),  # pyright: ignore[reportArgumentType]
        DbtRegisterRequest(
            source_id=SOURCE,
            spec="cloud",
            host="https://cloud.getdbt.com",
            account_id="12",
            job_id="34",
            token=entered,
        ),
    )
    async with db.acquire() as conn:
        held = (
            await conn.execute_core(
                select(schema_org.sources.c.password_ref, schema_org.sources.c.type).where(
                    schema_org.sources.c.id == SOURCE
                )
            )
        ).fetchone()
    assert held.password_ref == entered
    assert held.type == "dbt"
    listed = await router.list_dbt_sources(SimpleNamespace())  # pyright: ignore[reportArgumentType]
    assert listed[0]["has_token"] is True
    assert entered not in json.dumps(listed)

    kind, binding = await command_target(db, SOURCE, "build")
    assert kind == "http"
    assert binding["credential_source"] == SOURCE
    assert binding["url"].endswith("/api/v2/accounts/12/jobs/34/run/")
    assert entered not in json.dumps(binding)


async def test_a_model_column_keeps_the_warehouses_type_with_the_manifests_words(
    state, db, document, monkeypatch
):
    import provisa.api.admin.schema_query as query

    await _add_warehouse(db)
    await _add_source(state, document)

    async def warehouse_columns(source_id: str, schema: str, table: str) -> list[SimpleNamespace]:
        assert (source_id, schema, table) == (WAREHOUSE, "analytics", "customer_orders")
        return [SimpleNamespace(name="customer_id", data_type="integer", comment=None)]

    monkeypatch.setattr(query, "_introspect_available_columns", warehouse_columns)
    columns = await model_columns(SOURCE, "analytics", "customer_orders")
    assert columns[0].data_type == "integer"
    assert columns[0].comment == "The customer's identifier."


class _Session:
    correlation_id = "c1"


async def test_a_script_command_answers_dbts_text_and_passes_macro_arguments(monkeypatch):
    import provisa.executor.function_dispatch as dispatch

    seen: list[list[str]] = []

    async def run(argv: list[str], _payload: bytes) -> bytes:
        seen.append(argv)
        return b"Done. PASS=1\n"

    monkeypatch.setattr(dispatch, "_run_subprocess", run)
    fn = {
        "binding": {
            "argv": ["dbt", "run-operation", "grant_read"],
            "stdout": "text",
            "json_argument": {"flag": "--args", "argument": "args"},
        }
    }
    rows = await dispatch._exec_script(fn, {}, None, {"args": {"role": "r"}}, _Session())
    assert rows == [{"output": "Done. PASS=1\n"}]
    assert seen[0] == ["dbt", "run-operation", "grant_read", "--args", '{"role": "r"}']

    await dispatch._exec_script(fn, {}, None, {}, _Session())
    assert seen[1] == ["dbt", "run-operation", "grant_read"]


async def test_dbts_failure_is_reported_from_its_output(monkeypatch):
    import asyncio

    import provisa.executor.function_dispatch as dispatch

    class _Process:
        returncode = 1

        async def communicate(self, _payload: bytes) -> tuple[bytes, bytes]:
            return b"Compilation Error in model orders", b""

    async def spawn(*_argv: Any, **_kwargs: Any) -> _Process:
        return _Process()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    with pytest.raises(ApiError) as failed:
        await dispatch._run_subprocess(["dbt", "run"], b"{}")
    assert "Compilation Error in model orders" in str(failed.value)


async def test_a_cloud_command_posts_its_body_with_the_sources_token(state, db, monkeypatch):
    import provisa.executor.function_dispatch as dispatch

    async with db.acquire() as conn:
        await source_repo.upsert(
            conn,
            Source(id=SOURCE, type=SourceType.dbt, path=":inline:", password="tok-as-entered"),
        )
    monkeypatch.setattr(dispatch, "_check_egress", lambda *a, **k: None)

    sent: dict[str, Any] = {}

    async def call(
        method: str, url: str, payload: dict, timeout: float, headers: dict | None = None
    ) -> dict:
        sent.update(method=method, url=url, payload=payload, headers=headers)
        return {"data": {"id": 9}}

    monkeypatch.setattr(dispatch, "_http_call", call)
    fn = {
        "binding": {
            "url": "https://cloud.getdbt.com/api/v2/accounts/1/jobs/2/run/",
            "method": "POST",
            "body": {"cause": "c", "steps_override": ["dbt build"]},
            "credential_source": SOURCE,
            "auth_scheme": "Token",
        }
    }
    rows = await dispatch._exec_http(fn, {}, state, {}, _Session())
    assert rows == [{"data": {"id": 9}}]
    assert sent["payload"] == {"cause": "c", "steps_override": ["dbt build"]}
    assert sent["headers"] == {"Authorization": "Token tok-as-entered"}
