# Copyright (c) 2026 Kenneth Stott
# Canary: 1f6a92c4-7de3-4b05-a8c1-94d27e0b3f65
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2228: on an operator-run connector a dbt command is a code source @command taking the dbt
invocation as a fixed argument (REQ-885, REQ-2221)."""

# Requirements: REQ-2228, REQ-885, REQ-2221

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from provisa.api.admin.dbt_router import DbtRegisterRequest, register_dbt_source
from provisa.api.errors import ApiError
from provisa.codesource.manifest import COMMAND, Export, Manifest, Parameter, Scalar
from provisa.codesource.worker import ABSENT
from provisa.core import schema_org
from provisa.core.database import Database, create_engine_from_url
from provisa.core.models import Source, SourceType
from provisa.core.repositories import source as source_repo
from provisa.dbt.store import command_target
from provisa.executor.function_dispatch import dispatch_function

pytestmark = pytest.mark.asyncio

FIXTURE = Path(__file__).parent.parent / "fixtures" / "dbt" / "manifest.json"
SOURCE = "dbt-jaffle"
CONNECTOR = "dbt-runner"


@pytest.fixture(name="document")
def _document() -> dict:
    return json.loads(FIXTURE.read_text())


@pytest.fixture(name="db")
def _db() -> Database:
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="dbt-connector")
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
    monkeypatch.setattr(state, "source_types", {CONNECTOR: "python"}, raising=False)
    monkeypatch.setattr(app, "_rebuild_schemas", AsyncMock())
    monkeypatch.setattr(router, "require_capability_request", lambda *a, **k: None)
    return state


async def _register(db: Database, document: dict, **more: Any) -> None:
    async with db.acquire() as conn:
        await source_repo.upsert(
            conn, Source(id=CONNECTOR, type=SourceType.python, path="/srv/runner")
        )
    await register_dbt_source(
        SimpleNamespace(),  # pyright: ignore[reportArgumentType]
        DbtRegisterRequest(
            source_id=SOURCE,
            spec="upload",
            content=json.dumps(document),
            connector_source=CONNECTOR,
            connector_command="run_dbt",
            **more,
        ),
    )


async def test_a_model_command_is_the_connectors_command_with_its_invocation_fixed(
    state, db, document
):
    await _register(db, document)
    kind, binding = await command_target(db, SOURCE, "run_order_events")
    assert kind == "code_command"
    assert binding == {
        "source_id": CONNECTOR,
        "command": "run_dbt",
        "fixed_arguments": {"invocation": ["run", "--select", "order_events"]},
    }


async def test_the_invocations_argument_can_be_named(state, db, document):
    await _register(db, document, connector_invocation_argument="dbt_args")
    _, binding = await command_target(db, SOURCE, "build")
    assert binding["fixed_arguments"] == {"dbt_args": ["build"]}


async def test_a_connector_that_is_no_code_source_is_refused(state, db, document):
    await _register(db, document)
    async with db.acquire() as conn:
        await source_repo.upsert(
            conn, Source(id=CONNECTOR, type=SourceType.duckdb, path="/data/x.duckdb")
        )
    with pytest.raises(ApiError) as refused:
        await command_target(db, SOURCE, "build")
    assert refused.value.code == "dbt.target_not_stated"
    assert CONNECTOR in str(refused.value)


async def test_a_connector_named_without_its_command_is_refused(state, db, document):
    async with db.acquire() as conn:
        await source_repo.upsert(
            conn, Source(id=CONNECTOR, type=SourceType.python, path="/srv/runner")
        )
    await register_dbt_source(
        SimpleNamespace(),  # pyright: ignore[reportArgumentType]
        DbtRegisterRequest(
            source_id=SOURCE,
            spec="upload",
            content=json.dumps(document),
            connector_source=CONNECTOR,
        ),
    )
    with pytest.raises(ApiError) as refused:
        await command_target(db, SOURCE, "build")
    assert refused.value.code == "dbt.target_not_stated"


# --- the fixed argument at the call ------------------------------------------------------------


def _param(name: str) -> Parameter:
    return Parameter(name=name, shape=Scalar("text"), nullable=True, required=False)


class _Worker:
    def __init__(self, answer: Any) -> None:
        self.answer = answer
        self.calls: list[tuple[str, list, list]] = []

    def command(self, export: str, args: Any, names: Any = (), *, schema: Any = None) -> Any:
        self.calls.append((export, list(args), list(names)))
        return self.answer


def _manifest(*parameters: str) -> Manifest:
    export = Export(
        name="run_dbt",
        kind=COMMAND,
        description="",
        parameters=tuple(_param(p) for p in parameters),
        returns=None,
    )
    return Manifest(exports=(export,), types={})


def _state_with(monkeypatch: pytest.MonkeyPatch, worker: _Worker, manifest: Manifest) -> Any:
    import provisa.codesource.commands as commands

    prepared = SimpleNamespace(manifest=manifest, worker=lambda: worker)
    monkeypatch.setattr(commands, "_prepared", AsyncMock(return_value=prepared))
    return SimpleNamespace(udf_trace_sink=None, minted_sessions=None)


def _fn(**binding: Any) -> dict:
    return {
        "name": "run_customer_orders",
        "impl_kind": "code_command",
        "arguments": [{"name": "args", "type": "json"}],
        "binding": {
            "source_id": CONNECTOR,
            "command": "run_dbt",
            "fixed_arguments": {"invocation": ["run", "--select", "customer_orders"]},
            **binding,
        },
    }


async def test_the_call_carries_the_fixed_invocation(monkeypatch):
    worker = _Worker("Done. PASS=1")
    state = _state_with(monkeypatch, worker, _manifest("invocation", "args"))
    rows = await dispatch_function(_fn(), {}, state, "admin")
    assert rows == [{"output": "Done. PASS=1"}]
    export, args, names = worker.calls[0]
    assert export == "run_dbt"
    assert names == ["invocation", "args"]
    assert args[0] == ["run", "--select", "customer_orders"]
    assert args[1] is ABSENT


async def test_a_macros_arguments_travel_beside_the_fixed_one(monkeypatch):
    worker = _Worker({"ok": True})
    state = _state_with(monkeypatch, worker, _manifest("invocation", "args"))
    rows = await dispatch_function(_fn(), {"args": {"role": "r"}}, state, "admin")
    assert rows == [{"ok": True}]
    assert worker.calls[0][1][1] == {"role": "r"}


async def test_a_positional_call_binds_to_the_declared_argument(monkeypatch):
    worker = _Worker(None)
    state = _state_with(monkeypatch, worker, _manifest("invocation", "args"))
    rows = await dispatch_function(_fn(), {"a0": "x"}, state, "admin")
    assert rows == []
    assert worker.calls[0][1][1] == "x"


async def test_a_caller_cannot_set_the_fixed_argument(monkeypatch):
    worker = _Worker("x")
    state = _state_with(monkeypatch, worker, _manifest("invocation", "args"))
    with pytest.raises(ApiError) as refused:
        await dispatch_function(_fn(), {"invocation": ["run", "--select", "other"]}, state, "admin")
    assert refused.value.code == "functions.fixed_argument_given"
    assert worker.calls == []


async def test_a_command_the_source_does_not_declare_is_refused(monkeypatch):
    worker = _Worker("x")
    state = _state_with(monkeypatch, worker, _manifest("invocation"))
    with pytest.raises(ApiError) as refused:
        await dispatch_function(_fn(command="nope"), {}, state, "admin")
    assert refused.value.code == "functions.code_command_unknown"


async def test_a_fixed_argument_the_command_does_not_take_is_refused(monkeypatch):
    worker = _Worker("x")
    state = _state_with(monkeypatch, worker, _manifest("something_else"))
    with pytest.raises(ApiError) as refused:
        await dispatch_function(_fn(), {}, state, "admin")
    assert refused.value.code == "functions.code_command_argument_unknown"
    assert worker.calls == []
