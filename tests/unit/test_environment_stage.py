# Copyright (c) 2026 Kenneth Stott
# Canary: 5b2e8c41-7d93-4a06-b1f4-c8e2a97d3f15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2261: every environment declares its stage -- production, preprod, test or development.

The org's original environment starts as production and a new one as development; changing the
stage is an environment-management act recorded in the audit log with the old and new stage; the
query audit rows, the report views and the catalog and lineage exports carry it; and no
authorization path reads it.
"""

# Requirements: REQ-2261

from __future__ import annotations

import ast
import asyncio
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import insert

from provisa.core import schema_admin
from provisa.core.database import Database, create_engine_from_url
from provisa.core.env_classes import INHERIT, REFUSED
from provisa.core.env_store import (
    STAGES,
    EnvironmentStageError,
    ensure_prod,
    get_env,
    reserve_env,
    set_stage,
)
from provisa.core.schema_admin import environments, orgs

ORG = "acme"
ROOT = Path(__file__).resolve().parents[2]
#: The guards are replaced in these tests, so the request is never read.
_REQUEST: Any = SimpleNamespace()


def _registry(name: str) -> Database:
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name=name)
    with db.engine.begin() as conn:
        schema_admin.metadata.create_all(conn)
        conn.execute(insert(orgs).values(id=ORG, name="Acme"))
    return db


async def _org_with_feature(name: str) -> Database:
    db = _registry(name)
    await ensure_prod(db, ORG, "u1")
    await reserve_env(
        SimpleNamespace(),
        db,
        ORG,
        "feature",
        created_by="u1",
        parent="prod",
        data_mode=INHERIT,
        mutation_handling=REFUSED,
    )
    return db


# -- defaults -------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_original_environment_is_production_and_a_new_one_is_development():
    db = await _org_with_feature("stage-defaults")
    prod = await get_env(db, ORG, "prod")
    feature = await get_env(db, ORG, "feature")
    assert prod is not None and prod["stage"] == "production"
    assert feature is not None and feature["stage"] == "development"


@pytest.mark.asyncio
async def test_any_number_of_environments_may_share_a_stage_and_unknown_stages_are_refused():
    db = await _org_with_feature("stage-shared")
    await reserve_env(
        SimpleNamespace(),
        db,
        ORG,
        "eu",
        created_by="u1",
        parent="prod",
        data_mode=INHERIT,
        mutation_handling=REFUSED,
    )
    assert await set_stage(db, ORG, "feature", "production") == "development"
    assert await set_stage(db, ORG, "eu", "production") == "development"
    for name in ("prod", "feature", "eu"):
        row = await get_env(db, ORG, name)
        assert row is not None and row["stage"] == "production"
    with pytest.raises(EnvironmentStageError, match="unknown stage 'staging'"):
        await set_stage(db, ORG, "eu", "staging")
    assert STAGES == ("production", "preprod", "test", "development")


def test_the_registry_column_refuses_a_stage_outside_the_four():
    from sqlalchemy.exc import IntegrityError

    db = _registry("stage-check")
    with db.engine.begin() as conn, pytest.raises(IntegrityError):
        conn.execute(insert(environments).values(org_id=ORG, name="prod", stage="staging"))
    with db.engine.begin() as conn, pytest.raises(IntegrityError):
        conn.execute(insert(environments).values(org_id=ORG, name="prod", stage=None))  # NOT NULL


# -- change + audit -------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_stage_change_is_audited_with_the_old_and_new_stage(monkeypatch):
    from provisa.api.admin import environments_router as er

    db = await _org_with_feature("stage-patch")
    trail: list[dict] = []
    guarded: list[str] = []

    async def record(_model_db: object, **entry: Any) -> None:
        del _model_db
        trail.append(entry)

    async def model_db(_org: str) -> object:
        del _org
        return object()

    async def guard_owned(_request, _org, name):
        guarded.append(f"owned:{name}")
        return "u1"

    async def guard(_request, _org):
        guarded.append("manage")
        return "u1"

    async def confined(_request, _org, _name):
        return None

    monkeypatch.setattr(er, "_admin_pool", lambda: db)
    monkeypatch.setattr(er, "_guard_owned", guard_owned)
    monkeypatch.setattr(er, "_guard", guard)
    monkeypatch.setattr(er, "_confined", confined)
    monkeypatch.setattr("provisa.api.admin.orgs_router._org_model_db", model_db)
    monkeypatch.setattr("provisa.core.org_membership.record_admin_action", record)

    out = await er.patch_environment(_REQUEST, ORG, "feature", er.PatchEnvBody(stage="preprod"))
    assert out["environment"]["stage"] == "preprod"
    # A stage change alone is managing that one environment: its owner's right suffices.
    assert guarded == ["owned:feature"]
    assert trail == [
        {
            "org_id": ORG,
            "action": "environment.stage",
            "actor_id": "u1",
            "subject_id": "feature",
            "detail": {"from": "development", "to": "preprod", "stage": "preprod"},
        }
    ]

    # Restating the stage it already declares changes nothing and records nothing.
    trail.clear()
    await er.patch_environment(_REQUEST, ORG, "feature", er.PatchEnvBody(stage="preprod"))
    assert trail == []

    # Any other entry about the environment carries the stage it declares when written.
    await er.patch_environment(_REQUEST, ORG, "feature", er.PatchEnvBody(protected=True))
    assert guarded[-1] == "manage"
    assert trail[-1]["action"] == "environment.update"
    assert trail[-1]["detail"] == {"protected": True, "stage": "preprod"}

    # An act on the org's environments as a whole names no environment and carries no stage.
    await er._audit(ORG, "u1", "environment.repo_fetch", "-", {"branches": []})
    assert trail[-1]["detail"] == {"branches": []}


def test_the_patch_body_refuses_a_stage_outside_the_four():
    from pydantic import ValidationError

    from provisa.api.admin.environments_router import PatchEnvBody

    with pytest.raises(ValidationError):
        PatchEnvBody(stage="staging")  # type: ignore[arg-type]


# -- query audit rows and reports -----------------------------------------------------------------


def _audit_record(**overrides):
    from provisa.audit.writer import AuditRecord
    from provisa.encryption import NullEncryption

    fields = dict(
        record_db=object(),
        tenant_id=ORG,
        user_id="u1",
        role_id="analyst",
        query_text="SELECT 1",
        table_ids=(),
        source="http",
        status_code=200,
        duration_ms=3,
        logged_at=datetime(2026, 11, 20, tzinfo=UTC),
        trace_id=None,
        encryption=NullEncryption(),
        model_stamp=None,
        model_env="feature",
        enforced=None,
        route_reason=None,
        sources=(),
        data_age=None,
        region="us",
        time_travel=None,
        tables=(),
        meter_pool=object(),
        meter_org=ORG,
    )
    fields.update(overrides)
    return AuditRecord(**fields)  # type: ignore[arg-type]


def test_a_query_audit_row_carries_the_stage_its_environment_declares_when_written():
    from provisa.audit.writer import AuditWriter

    landed: list[dict] = []
    asked: list[tuple[str, str]] = []
    stage = {"feature": "test"}

    async def insert_rows(_db, rows):
        landed.extend(rows)

    async def env_stage(_pool, org, env):
        asked.append((org, env))
        return stage[env]

    async def no_commit(*_a):
        return None

    async def meter(*_a):
        return None

    writer = AuditWriter(
        interval_s=0.01,
        retry_s=0.01,
        insert=insert_rows,
        meter=meter,
        deployed_commit=no_commit,
        env_stage=env_stage,
    ).start()
    try:
        writer.enqueue(_audit_record())
        assert writer.flush(2.0)
        stage["feature"] = "production"  # a change applies to the next row, never cached
        writer.enqueue(_audit_record())
        assert writer.flush(2.0)
    finally:
        writer.close(5.0)
    assert [row["stage"] for row in landed] == ["test", "production"]
    assert asked == [(ORG, "feature"), (ORG, "feature")]


def test_a_deployment_without_a_control_plane_records_no_stage():
    row = _audit_record(meter_pool=None).row(None, None)
    assert row["stage"] is None


def test_the_audit_report_views_can_be_filtered_and_grouped_by_stage():
    """Each audit-based report exposes the recorded stage as its LAST column (so an existing view
    is replaced in place), and the daily rollups group by it."""
    import sqlite3

    from provisa.api._meta_views import _OPS_LOG_TABLE_VIEWS, _OPS_REPORT_VIEWS

    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE query_audit_log (
            id INTEGER PRIMARY KEY, user_id TEXT, role_id TEXT, query_hash TEXT,
            table_ids TEXT, table_names TEXT, source TEXT, status_code INT, duration_ms INT,
            logged_at TEXT, route TEXT, row_count INT, route_reason TEXT, sources TEXT,
            data_age TEXT, region TEXT, model_stamp INT, model_commit TEXT, enforced TEXT,
            as_of TEXT, as_recorded TEXT, trace_id TEXT, stage TEXT);
        CREATE TABLE user_directory (user_id TEXT, display_name TEXT);
        INSERT INTO query_audit_log (id, user_id, role_id, source, status_code, duration_ms,
                                     logged_at, stage)
        VALUES (1, 'u', 'r', 'http', 200, 5, '2026-11-20T01:00:00', 'development'),
               (2, 'u', 'r', 'http', 403, 7, '2026-11-20T02:00:00', 'development'),
               (3, 'u', 'r', 'http', 403, 9, '2026-11-20T03:00:00', 'production');
        """
    )
    for ddl in (
        _OPS_LOG_TABLE_VIEWS["query_audit_log"],
        _OPS_REPORT_VIEWS["policy_denials"],
        _OPS_REPORT_VIEWS["surface_mix"],
        _OPS_REPORT_VIEWS["query_health"],
    ):
        conn.execute(ddl.replace("CREATE OR REPLACE VIEW", "CREATE VIEW"))

    def last_column(view: str) -> str:
        return conn.execute(f"SELECT * FROM {view} LIMIT 0").description[-1][0]

    for view in ("query_audit_log_ops", "policy_denials", "surface_mix", "query_health"):
        assert last_column(view) == "stage", view

    denied = conn.execute("SELECT stage FROM policy_denials ORDER BY id").fetchall()
    assert denied == [("development",), ("production",)]
    health = conn.execute(
        "SELECT id, stage, query_count, error_count FROM query_health ORDER BY stage"
    ).fetchall()
    assert health == [
        ("http:2026-11-20:development", "development", 2, 1),
        ("http:2026-11-20:production", "production", 1, 1),
    ]
    mix = conn.execute("SELECT stage, query_count FROM surface_mix ORDER BY stage").fetchall()
    assert mix == [("development", 2), ("production", 1)]
    # The queries report states it too (its joins need more of the schema than this sqlite holds).
    assert "q.stage" in _OPS_REPORT_VIEWS["queries"].split("FROM query_audit_log q")[0]


# -- exports --------------------------------------------------------------------------------------


def _snapshot(stage: str | None):
    from provisa.api.metadata_export.model import (
        AssetKind,
        AssetRef,
        ColumnAsset,
        EnvironmentStage,
        MetadataSnapshot,
        TableAsset,
    )

    ref = AssetRef(kind=AssetKind.TABLE, parts=("wh", "public", "orders"))
    table = TableAsset(
        ref=ref,
        name="orders",
        source_id="wh",
        domain_id=None,
        description="",
        columns=[
            ColumnAsset(
                ref=AssetRef(kind=AssetKind.COLUMN, parts=(*ref.parts, "id")),
                name="id",
                data_type="integer",
                description="",
                semantic_uri="provisa://acme/wh/public/orders/id",
            )
        ],
        semantic_uri="provisa://acme/wh/public/orders",
    )
    environment = EnvironmentStage(name="eu", stage=stage) if stage is not None else None
    return MetadataSnapshot(org_id=ORG, tables=[table], environment=environment)


def test_an_openlineage_dataset_carries_the_environment_and_its_stage():
    from provisa.api.metadata_export.openlineage import to_events

    events = to_events(_snapshot("production"), event_time=datetime(2026, 11, 20, tzinfo=UTC))
    facet = events[0].payload["dataset"]["facets"]["provisa_environment"]
    assert facet["environment"] == "eu"
    assert facet["stage"] == "production"
    # without an environment registry there is no stage to state, and no facet claims one
    bare = to_events(_snapshot(None), event_time=datetime(2026, 11, 20, tzinfo=UTC))
    assert "provisa_environment" not in bare[0].payload["dataset"]["facets"]


def test_a_catalog_dataset_carries_the_environment_and_its_stage():
    from provisa.api.metadata_export.datahub import to_proposals

    proposals = to_proposals(_snapshot("preprod"))
    props = [
        p.aspect["customProperties"] for p in proposals if p.aspect_name == "datasetProperties"
    ]
    assert props, "the dataset's properties are proposed"
    assert props[0]["provisaEnvironment"] == "eu"
    assert props[0]["provisaStage"] == "preprod"


def test_the_publish_path_reads_the_stage_from_the_environment_registry(monkeypatch):
    from provisa.api import app as app_module
    from provisa.api.metadata_export import publishing
    from provisa.core.request_context import current_env

    async def run():
        db = await _org_with_feature("stage-export")
        await set_stage(db, ORG, "feature", "test")
        monkeypatch.setattr(app_module.state, "admin_db", db, raising=False)
        token = current_env.set("feature")
        try:
            return await publishing.environment_stage(ORG)
        finally:
            current_env.reset(token)

    stated = asyncio.run(run())
    assert stated is not None
    assert (stated.name, stated.stage) == ("feature", "test")


# -- no authorization effect ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_stage_change_alters_no_capability_check():
    """The rights an owner holds in an environment, its protection, and its ownership read the
    same at every stage."""
    from provisa.core.env_approvals import is_protected
    from provisa.core.env_authority import capabilities_within, owns_environment

    db = await _org_with_feature("stage-auth")
    seen = set()
    for stage in STAGES:
        for name in ("prod", "feature"):
            await set_stage(db, ORG, name, stage)
        seen.add(
            (
                await capabilities_within(db, ORG, "feature", "u1"),
                await capabilities_within(db, ORG, "feature", "u2"),
                await owns_environment(db, ORG, "feature", "u1"),
                await is_protected(db, ORG, "prod", 2),
                await is_protected(db, ORG, "feature", 2),
            )
        )
    assert len(seen) == 1


_AUTH_PATHS = (
    "provisa/auth",
    "provisa/security",
    "provisa/api/env_routing.py",
    "provisa/core/env_authority.py",
    "provisa/core/env_approvals.py",
    "provisa/api/admin/capabilities.py",
    "provisa/api/admin/_guards.py",
    "provisa/api/admin/_platform_guard.py",
)


def _reads_stage(tree: ast.AST) -> list[int]:
    hits = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.attr == "stage":
            hits.append(node.lineno)
        elif (
            isinstance(node, ast.Subscript)
            and isinstance(node.slice, ast.Constant)
            and node.slice.value == "stage"
        ):
            hits.append(node.lineno)
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "get"
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and node.args[0].value == "stage"
        ):
            hits.append(node.lineno)
    return hits


def test_no_authorization_path_reads_the_stage():
    files: list[Path] = []
    for entry in _AUTH_PATHS:
        path = ROOT / entry
        files.extend(sorted(path.rglob("*.py")) if path.is_dir() else [path])
    assert files
    offenders = {
        str(f.relative_to(ROOT)): lines
        for f in files
        if (lines := _reads_stage(ast.parse(f.read_text())))
    }
    assert offenders == {}
