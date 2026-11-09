# Copyright (c) 2026 Kenneth Stott
# Canary: 8d41c7e2-5b06-4f3a-9c18-2e7f60a1d394
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2228, REQ-858: a dbt model's freshness comes from dbt's run results, and a new manifest
updates only the descriptions no steward has changed."""

# Requirements: REQ-2228, REQ-858

from __future__ import annotations

import copy
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import insert, select, update

from provisa.api.admin.dbt_router import (
    DbtRefreshRequest,
    DbtRegisterRequest,
    refresh_dbt_source,
    register_dbt_source,
)
from provisa.api.errors import ApiError
from provisa.core import schema_org
from provisa.core.database import Database, create_engine_from_url
from provisa.core.models import Source, SourceType
from provisa.core.repositories import source as source_repo
from provisa.dbt.run_results import NotRunResults, parse_run_results
from provisa.dbt.store import dbt_model_tables, model_freshness_token, store_run_results
from provisa.events.probes import source_transport

pytestmark = pytest.mark.asyncio

FIXTURE = Path(__file__).parent.parent / "fixtures" / "dbt" / "manifest.json"
SOURCE = "dbt-jaffle"
WAREHOUSE = "wh-jaffle"
MODEL = "model.jaffle.customer_orders"
OTHER_MODEL = "model.jaffle.order_events"


def _result(unique_id: str, status: str, completed_at: str | None) -> dict:
    timing = (
        []
        if completed_at is None
        else [
            {"name": "compile", "started_at": completed_at, "completed_at": completed_at},
            {"name": "execute", "started_at": completed_at, "completed_at": completed_at},
        ]
    )
    return {
        "unique_id": unique_id,
        "status": status,
        "timing": timing,
        "thread_id": "Thread-1",
        "execution_time": 0.1,
        "adapter_response": {},
        "message": None,
        "failures": None,
    }


def _run_results(generated_at: str, *results: dict) -> dict:
    return {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v6.json",
            "dbt_version": "1.12.6",
            "generated_at": generated_at,
            "invocation_id": "e212985d-e19d-4482-b41d-4a8a49d86500",
            "env": {},
        },
        "results": list(results),
        "elapsed_time": 1.0,
        "args": {},
    }


T1 = "2026-11-09T10:00:00.000000Z"
T2 = "2026-11-09T11:00:00.000000Z"
T3 = "2026-11-09T12:00:00.000000Z"


@pytest.fixture(name="document")
def _document() -> dict:
    return json.loads(FIXTURE.read_text())


@pytest.fixture(name="db")
def _db() -> Database:
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="dbt-run-results")
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
def _state(db: Database, monkeypatch: pytest.MonkeyPatch) -> Any:
    import provisa.api.admin.dbt_router as router
    import provisa.api.app as app

    state = app.state
    monkeypatch.setattr(state, "model_db", db, raising=False)
    monkeypatch.setattr(state, "source_types", {}, raising=False)
    monkeypatch.setattr(app, "_rebuild_schemas", AsyncMock())
    monkeypatch.setattr(router, "require_capability_request", lambda *a, **k: None)
    return state


async def _add_source(db: Database, *, wh: bool = True) -> None:
    async with db.acquire() as conn:
        if wh:
            await source_repo.upsert(
                conn, Source(id=WAREHOUSE, type=SourceType.duckdb, path="/data/jaffle.duckdb")
            )
        await source_repo.upsert(conn, Source(id=SOURCE, type=SourceType.dbt, path=":inline:"))


# --- reading the document ------------------------------------------------------------------------


async def test_a_document_that_is_not_run_results_is_refused():
    with pytest.raises(NotRunResults):
        parse_run_results('{"metadata": {"dbt_schema_version": "x/manifest/v12.json"}}')
    with pytest.raises(NotRunResults):
        parse_run_results("not json")


async def test_run_results_are_read_as_each_nodes_outcome():
    parsed = parse_run_results(
        json.dumps(
            _run_results(T1, _result(MODEL, "success", T1), _result(OTHER_MODEL, "skipped", None))
        )
    )
    assert parsed.generated_at.isoformat().startswith("2026-11-09T10:00:00")
    by_id = {r.unique_id: r for r in parsed.results}
    assert by_id[MODEL].status == "success"
    assert by_id[MODEL].completed_at is not None
    assert by_id[OTHER_MODEL].completed_at is None


# --- what is held, and the token -----------------------------------------------------------------


async def _store(db: Database, document: dict) -> None:
    async with db.acquire() as conn:
        await store_run_results(conn, SOURCE, parse_run_results(json.dumps(document)))


async def _token(db: Database, unique_id: str = MODEL) -> str | None:
    async with db.acquire() as conn:
        return await model_freshness_token(conn, SOURCE, unique_id)


async def test_a_model_never_run_has_no_token(db):
    await _add_source(db)
    assert await _token(db) is None


async def test_a_successful_run_gives_a_token_that_changes_with_the_next_build(db):
    await _add_source(db)
    await _store(db, _run_results(T1, _result(MODEL, "success", T1)))
    first = await _token(db)
    assert first is not None
    await _store(db, _run_results(T2, _result(MODEL, "success", T2)))
    second = await _token(db)
    assert second is not None and second != first


async def test_the_same_run_read_twice_keeps_the_token(db):
    await _add_source(db)
    document = _run_results(T1, _result(MODEL, "success", T1))
    await _store(db, document)
    first = await _token(db)
    await _store(db, document)
    assert await _token(db) == first


async def test_a_run_that_selected_other_models_leaves_a_model_alone(db):
    await _add_source(db)
    await _store(db, _run_results(T1, _result(MODEL, "success", T1)))
    first = await _token(db)
    await _store(db, _run_results(T2, _result(OTHER_MODEL, "success", T2)))
    assert await _token(db) == first
    assert await _token(db, OTHER_MODEL) is not None


async def test_an_older_document_does_not_move_a_model_back(db):
    await _add_source(db)
    await _store(db, _run_results(T2, _result(MODEL, "success", T2)))
    newest = await _token(db)
    await _store(db, _run_results(T1, _result(MODEL, "success", T1)))
    assert await _token(db) == newest


@pytest.mark.parametrize("status", ["error", "skipped"])
async def test_a_latest_run_that_did_not_build_leaves_freshness_unknown(db, status):
    await _add_source(db)
    await _store(db, _run_results(T1, _result(MODEL, "success", T1)))
    await _store(db, _run_results(T2, _result(MODEL, status, None)))
    assert await _token(db) is None


async def test_a_build_after_a_failure_gives_a_token_again(db):
    await _add_source(db)
    await _store(db, _run_results(T1, _result(MODEL, "success", T1)))
    first = await _token(db)
    await _store(db, _run_results(T2, _result(MODEL, "error", T2)))
    await _store(db, _run_results(T3, _result(MODEL, "success", T3)))
    token = await _token(db)
    assert token is not None and token != first


async def test_only_what_dbt_builds_is_held(db):
    await _add_source(db)
    test_id = "test.jaffle.not_null_customer_orders_customer_id.582e6bcaa0"
    await _store(db, _run_results(T1, _result(test_id, "pass", T1), _result(MODEL, "success", T1)))
    async with db.acquire() as conn:
        rows = (await conn.execute_core(select(schema_org.dbt_node_runs.c.unique_id))).fetchall()
    assert [r.unique_id for r in rows] == [MODEL]


# --- the probe of a model's table ----------------------------------------------------------------


async def test_a_models_table_probes_its_run_results(state, db, document):
    await _add_source(db)
    await register_dbt_source(
        SimpleNamespace(),  # pyright: ignore[reportArgumentType]
        DbtRegisterRequest(
            source_id=SOURCE,
            spec="upload",
            content=json.dumps(document),
            run_results=json.dumps(_run_results(T1, _result(MODEL, "success", T1))),
        ),
    )
    async with db.acquire() as conn:
        tables = await dbt_model_tables(conn)
    assert tables[(WAREHOUSE, "analytics", "customer_orders")] == (SOURCE, MODEL)
    state.dbt_model_tables = tables

    source = SimpleNamespace(id=WAREHOUSE, type="duckdb")
    table = SimpleNamespace(schema_name="analytics", table_name="customer_orders")
    transport = source_transport(state, {}, source, table)
    assert transport is not None
    assert await transport() == await _token(db)

    other = SimpleNamespace(schema_name="analytics", table_name="not_a_model")
    assert source_transport(state, {}, source, other) is None


# --- how run results arrive ----------------------------------------------------------------------


async def test_a_refresh_can_bring_run_results_alone(state, db, document):
    await _add_source(db)
    await register_dbt_source(
        SimpleNamespace(),  # pyright: ignore[reportArgumentType]
        DbtRegisterRequest(source_id=SOURCE, spec="upload", content=json.dumps(document)),
    )
    answer = await refresh_dbt_source(
        SimpleNamespace(),  # pyright: ignore[reportArgumentType]
        SOURCE,
        DbtRefreshRequest(run_results=json.dumps(_run_results(T1, _result(MODEL, "success", T1)))),
    )
    assert answer["models"] == 3
    assert await _token(db) is not None


async def test_a_location_source_reads_its_run_results_from_their_location(
    state, db, document, monkeypatch
):
    import provisa.api.admin.dbt_router as router

    await _add_source(db)
    held = {
        "s3://b/manifest.json": json.dumps(document).encode(),
        "s3://b/run_results.json": json.dumps(
            _run_results(T1, _result(MODEL, "success", T1))
        ).encode(),
    }

    async def read_bytes(location: str) -> bytes:
        return held[location]

    monkeypatch.setattr(router, "read_bytes", read_bytes)
    monkeypatch.setattr(router, "resolved_credential", AsyncMock(side_effect=lambda x: x))
    await register_dbt_source(
        SimpleNamespace(),  # pyright: ignore[reportArgumentType]
        DbtRegisterRequest(
            source_id=SOURCE,
            spec="location",
            location="s3://b/manifest.json",
            run_results_location="s3://b/run_results.json",
        ),
    )
    first = await _token(db)
    assert first is not None

    held["s3://b/run_results.json"] = json.dumps(
        _run_results(T2, _result(MODEL, "success", T2))
    ).encode()
    await refresh_dbt_source(
        SimpleNamespace(),  # pyright: ignore[reportArgumentType]
        SOURCE,
        DbtRefreshRequest(),
    )
    assert await _token(db) != first


async def test_a_document_that_is_no_run_results_is_refused_by_name(state, db, document):
    with pytest.raises(ApiError) as refused:
        await register_dbt_source(
            SimpleNamespace(),  # pyright: ignore[reportArgumentType]
            DbtRegisterRequest(
                source_id=SOURCE,
                spec="upload",
                content=json.dumps(document),
                run_results=json.dumps(document),
            ),
        )
    assert refused.value.code == "dbt.not_run_results"


# --- a description a steward changed ------------------------------------------------------------


async def _registered_model(db: Database, description: str | None, column: str | None) -> None:
    async with db.acquire() as conn:
        await conn.execute_core(insert(schema_org.domains).values(id="sales", description=""))
        await conn.execute_core(
            insert(schema_org.registered_tables).values(
                source_id=WAREHOUSE,
                domain_id="sales",
                schema_name="analytics",
                table_name="customer_orders",
                description=description,
            )
        )
        table_id = (await conn.execute_core(select(schema_org.registered_tables.c.id))).scalar()
        await conn.execute_core(
            insert(schema_org.table_columns).values(
                table_id=table_id, column_name="customer_id", description=column
            )
        )


async def _descriptions(db: Database) -> tuple[str | None, str | None]:
    async with db.acquire() as conn:
        table = (
            await conn.execute_core(select(schema_org.registered_tables.c.description))
        ).scalar()
        column = (await conn.execute_core(select(schema_org.table_columns.c.description))).scalar()
    return table, column


async def _newer_manifest(state, document: dict, table: str, column: str) -> None:
    newer = copy.deepcopy(document)
    model = newer["nodes"][MODEL]
    model["description"] = table
    model["columns"]["customer_id"]["description"] = column
    await refresh_dbt_source(
        SimpleNamespace(),  # pyright: ignore[reportArgumentType]
        SOURCE,
        DbtRefreshRequest(content=json.dumps(newer)),
    )


async def _with_registered_source(state, db, document) -> None:
    await _add_source(db)
    await register_dbt_source(
        SimpleNamespace(),  # pyright: ignore[reportArgumentType]
        DbtRegisterRequest(source_id=SOURCE, spec="upload", content=json.dumps(document)),
    )
    model = document["nodes"][MODEL]
    await _registered_model(
        db, model["description"], model["columns"]["customer_id"]["description"]
    )


async def test_a_description_no_steward_changed_follows_the_manifest(state, db, document):
    await _with_registered_source(state, db, document)
    await _newer_manifest(state, document, "New table words.", "New column words.")
    assert await _descriptions(db) == ("New table words.", "New column words.")


async def test_it_goes_on_following_each_new_manifest(state, db, document):
    await _with_registered_source(state, db, document)
    await _newer_manifest(state, document, "Second.", "Second column.")
    await _newer_manifest(state, document, "Third.", "Third column.")
    assert await _descriptions(db) == ("Third.", "Third column.")


async def test_a_table_description_a_steward_changed_stands(state, db, document):
    await _with_registered_source(state, db, document)
    async with db.acquire() as conn:
        await conn.execute_core(
            update(schema_org.registered_tables).values(description="The steward's words.")
        )
    await _newer_manifest(state, document, "New table words.", "New column words.")
    assert await _descriptions(db) == ("The steward's words.", "New column words.")


async def test_a_column_description_a_steward_changed_stands(state, db, document):
    await _with_registered_source(state, db, document)
    async with db.acquire() as conn:
        await conn.execute_core(
            update(schema_org.table_columns).values(description="The steward's column.")
        )
    await _newer_manifest(state, document, "New table words.", "New column words.")
    assert await _descriptions(db) == ("New table words.", "The steward's column.")


async def test_a_description_a_steward_cleared_stays_clear(state, db, document):
    await _with_registered_source(state, db, document)
    async with db.acquire() as conn:
        await conn.execute_core(update(schema_org.registered_tables).values(description=None))
    await _newer_manifest(state, document, "New table words.", "New column words.")
    table, _ = await _descriptions(db)
    assert table is None


async def test_a_field_the_manifest_first_describes_is_filled_in(state, db, document):
    bare = copy.deepcopy(document)
    bare["nodes"][MODEL]["description"] = ""
    bare["nodes"][MODEL]["columns"]["customer_id"]["description"] = ""
    await _add_source(db)
    await register_dbt_source(
        SimpleNamespace(),  # pyright: ignore[reportArgumentType]
        DbtRegisterRequest(source_id=SOURCE, spec="upload", content=json.dumps(bare)),
    )
    await _registered_model(db, None, None)
    await _newer_manifest(state, document, "First words.", "First column words.")
    assert await _descriptions(db) == ("First words.", "First column words.")
