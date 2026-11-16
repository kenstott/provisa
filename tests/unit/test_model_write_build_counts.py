# Copyright (c) 2026 Kenneth Stott
# Canary: dcebcb24-73e4-4c16-9e1a-8c81b61219ac
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How many schema builds a model write costs today, counted (REQ-1914).

REQ-1914 (amended 2026-11-16): a model write costs in proportion to what it changes; a batch of
writes builds once, and a write already covered by a build costs nothing. These tests are the
baseline that work starts from. Each one runs a write against a control plane of its own, with the
real entry to the build and the real stamps, and counts the builds it caused and who caused them.
Each is named for the count it has today and the count the amendment gives it: when the build
count of one of these writes changes, its test fails and is rewritten to the new count.
"""

# Requirements: REQ-1914, REQ-2234, REQ-313, REQ-322

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import insert, select

import provisa.api.app as appmod
from provisa.api import model_reload, rebuild_trace
from provisa.api.admin import _table_ops, schema_mutation, schema_mutation_ops
from provisa.api.admin.types import ColumnInput, TableInput
from provisa.core import config_stamp
from provisa.core.database import Database
from provisa.core.models import Column
from provisa.core.schema_org import domains, registered_tables, roles, sources
from tests.unit.gate_identity import serve_every_column

REGISTER_TABLE = "provisa.api.admin.schema_mutation_ops:register_table"
REGISTERED_RESULT = "provisa.api.admin.schema_mutation_ops:_registered_result"
GRPC_REGISTER = "provisa.api.admin.grpc_remote_router:_load_and_register"


async def _nothing(*_a, **_k):
    return None


@pytest.fixture
async def plane(monkeypatch, control_plane) -> Database:
    """The deployment org's runtime over a control plane of the test's own, not built yet; the
    steps of a save that reach outside the control plane (the engine, a source's catalog) stood
    in for. The build's entry and the stamps are real."""
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql", cache_ttl=300))
        await conn.execute_core(insert(domains).values(id="sales"))
        await conn.execute_core(
            insert(roles).values(id="support", capabilities=["write"], domain_access=["sales"])
        )

    async def _columns(_pool, inp):
        return [
            Column(
                name=c.name,
                data_type=c.data_type,
                visible_to=c.visible_to,
                is_primary_key=c.is_primary_key,
            )
            for c in inp.columns
        ], None

    engine = types.SimpleNamespace(
        name="duckdb",
        reconcile_landed_tables=_nothing,
        materialize_store_target=lambda _org: ("mat_store", "views"),
    )
    state = appmod.state
    monkeypatch.setattr(state, "tenant_db", db, raising=False)
    monkeypatch.setattr(state, "model_db", db, raising=False)
    runtime = state._active_runtime()
    monkeypatch.setattr(runtime, "model_stamp", None)
    monkeypatch.setattr(runtime, "build_stamp", None)
    monkeypatch.setattr(state, "source_types", {"pg": "postgresql"}, raising=False)
    monkeypatch.setattr(state, "federation_engine", engine, raising=False)
    monkeypatch.setattr(state, "grpc_remote_sources", {}, raising=False)
    config = types.SimpleNamespace(graphql_remote=types.SimpleNamespace(max_rows=1000))
    monkeypatch.setattr(state, "config", config, raising=False)
    for module in (schema_mutation, schema_mutation_ops):
        monkeypatch.setattr(module, "_build_columns_for_input", _columns)
        monkeypatch.setattr(module, "column_field_refusal", _nothing)
    monkeypatch.setattr(schema_mutation, "require_capability", lambda *_a, **_k: None)
    monkeypatch.setattr(schema_mutation_ops, "reprovision_source_after_registration", _nothing)
    monkeypatch.setattr("provisa.dq.scan.reschedule_org_checkers", _nothing)
    monkeypatch.setattr("provisa.core.deployment_settings.auto_track_fk", lambda: False)
    monkeypatch.setattr(schema_mutation_ops, "_apply_mv_relationship_gate", _nothing)
    # What a watcher's reload does after a build, none of it a build.
    monkeypatch.setattr(model_reload, "prune_region_state", _nothing)
    monkeypatch.setattr(model_reload, "reschedule_triggers", _nothing)
    monkeypatch.setattr("provisa.api.redaction_load.refresh_org", _nothing)
    assert _table_ops is not None
    return db


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    """Every column is served here: which ones a caller may read is REQ-2152's own tests."""
    serve_every_column(monkeypatch)


def _info() -> Any:
    request = types.SimpleNamespace(
        state=types.SimpleNamespace(identity=None, active_org_id="a", role="support")
    )
    return types.SimpleNamespace(context={"request": request})


def _table(name: str) -> TableInput:
    return TableInput(
        source_id="pg",
        domain_id="sales",
        schema_name="public",
        table_name=name,
        columns=[
            ColumnInput(name="id", data_type="integer", visible_to=["support"]),
            ColumnInput(name="status", data_type="varchar", visible_to=["support"]),
        ],
    )


async def _registered(db: Database) -> list[str]:
    async with db.acquire() as conn:
        rows = await conn.execute_core(select(registered_tables.c.table_name))
        return sorted(r.table_name for r in rows.fetchall())


async def _watcher_pass() -> None:
    """The config watcher's turn for this runtime's model, as its interval brings it round."""
    await model_reload.reload_model(appmod.state._active_runtime())


async def test_registering_n_tables_in_one_action_runs_n_builds_today__one_when_a_batch_builds_once(
    plane, model_builds
) -> None:
    """registerTables registers each table by registerTable, and each of those builds the whole
    org: three tables, three builds. Under the amendment the batch builds once."""
    names = ["orders", "customers", "invoices"]

    outcomes = await schema_mutation.Mutation().register_tables(  # pyright: ignore[reportCallIssue]
        _info(), [_table(name) for name in names]
    )

    assert [(o.table_name, o.result.success) for o in outcomes] == [(n, True) for n in names]
    assert await _registered(plane) == sorted(names)
    assert model_builds.causes == [REGISTER_TABLE] * len(names)
    for build in model_builds.builds:
        assert build.phase_names == list(rebuild_trace.PHASES)


async def test_registering_a_table_that_completes_a_relationship_runs_two_builds_today__one_when_a_covered_write_builds_nothing(
    plane, model_builds, monkeypatch
) -> None:
    """A registration that completes a relationship between two of its source's tables stores
    the relationship after its own build, and builds again for it: two builds for one write.
    Under the amendment the relationship is stored before the one build."""

    async def _one_detected(_state, _source_id) -> int:
        return 1  # REQ-313: the detection itself has its own tests

    monkeypatch.setattr(
        "provisa.api.admin._graphql_table_registration.sync_detected_relationships", _one_detected
    )

    result = await schema_mutation_ops.register_table(_info(), _table("orders"))

    assert result.success, result.message
    assert await _registered(plane) == ["orders"]
    assert model_builds.causes == [REGISTER_TABLE, REGISTERED_RESULT]
    for build in model_builds.builds:
        assert build.phase_names == list(rebuild_trace.PHASES)


@pytest.fixture
def grpc_proto(monkeypatch, tmp_path):
    """A proto offering no query method: adding the source registers no table (REQ-322)."""
    proto = tmp_path / "orders.proto"
    proto.write_text('syntax = "proto3";\npackage orders;\n', encoding="utf-8")

    async def _loaded(_path, import_paths=None):
        del import_paths
        return {}

    monkeypatch.setattr("provisa.grpc_remote.loader.load_proto", _loaded)
    monkeypatch.setattr(
        "provisa.grpc_remote.loader.compile_proto_stubs", lambda *_a, **_k: ("pb2", "pb2_grpc")
    )
    monkeypatch.setattr("provisa.grpc_remote.executor.load_stubs", lambda _path: object())
    monkeypatch.setattr("provisa.grpc_remote.mapper.map_proto", lambda *_a, **_k: ([], []))
    return str(proto)


async def _ui_saves_the_grpc_source(proto_path: str, *, cache_ttl: int) -> None:
    """One save of the Sources form for a gRPC source (SourcesPage.tsx handleGrpcRegister): the
    registration is posted, then the form's cache TTL is saved by its own mutation. An edit of
    the TTL posts the same registration again, then the new TTL."""
    from provisa.api.admin.grpc_remote_router import _load_and_register

    await _load_and_register(
        "billing",
        proto_path,
        "billing.example:443",
        "billing",
        "",
        [],
        False,
        None,
        300,
        appmod.state,
    )
    saved = await schema_mutation.Mutation().update_source_cache(  # pyright: ignore[reportCallIssue]
        _info(), "billing", True, cache_ttl
    )
    assert saved.success, saved.message


async def test_a_grpc_source_added_then_edited_from_the_ui_runs_three_builds_today__one_when_an_unchanged_edit_and_a_cache_setting_build_nothing(
    plane, model_builds, grpc_proto
) -> None:
    """Add a gRPC source from the UI, then edit its TTL, as the Sources page does both: each save
    posts the registration (a build, awaited) and then saves the cache setting, a write that
    advances the build stamp and builds nothing itself. The first save's cache write is covered
    by the second save's build. The second's is covered by nothing, so the writer's own config
    watcher finds the stored stamp ahead of its runtime and builds a third time, for a model in
    which no table changed. Under the amendment: the add builds once; the edit posts nothing
    that changed and a cache setting is not a build."""
    del plane
    await _ui_saves_the_grpc_source(grpc_proto, cache_ttl=60)  # add
    # Edit the TTL: the same registration again, which changes no row, then the new TTL.
    await _ui_saves_the_grpc_source(grpc_proto, cache_ttl=300)
    await _watcher_pass()

    assert model_builds.causes == [GRPC_REGISTER, GRPC_REGISTER, rebuild_trace.WATCHER]
    add, edit, watcher = model_builds.builds
    # The edit's build read the first cache write's stamp: that write cost no build of its own.
    assert edit.read_stamps[config_stamp.BUILD] > add.read_stamps[config_stamp.BUILD]
    # The watcher's build is for the second cache write alone: the stamp the edit's build was
    # loaded at was already behind the stored one.
    assert watcher.loaded_stamps[config_stamp.BUILD] == edit.read_stamps[config_stamp.BUILD]
    assert watcher.read_stamps[config_stamp.BUILD] > edit.read_stamps[config_stamp.BUILD]
    for build in model_builds.builds:
        assert build.phase_names == list(rebuild_trace.PHASES)

    # Brought in line, the watcher's next pass builds nothing.
    await _watcher_pass()
    assert len(model_builds.builds) == 3
    assert model_builds.calls[-1].outcome == rebuild_trace.CURRENT
