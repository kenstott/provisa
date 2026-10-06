# Copyright (c) 2026 Kenneth Stott
# Canary: 6f2b8e14-3c9a-4d71-b0e5-a8d4c7f19e23
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A configuration file seeds the model store once, across real boots (REQ-1919).

The deployment's real boot (``create_app`` + its lifespan, ``_load_and_build``) runs three times
against an org schema of its own on the TEST instance's PostgreSQL:

1. the first start seeds the empty store from the file;
2. an admin changes the model between boots;
3. a reboot with a CHANGED file — a role and a table dropped, another added, the source's host
   changed — leaves the store exactly as the admin left it, and the process's configuration is
   the store's model, not the file's;
4. an explicit apply of the changed file adds and updates and removes nothing.

A demo organisation is the exception (DEMO ORGANISATIONS ARE THEIR CONFIG): every build of its
runtime applies the demo configuration again, so an edit to it lasts until its next build.

The boots are bound to their own org (``ORG_ID``), so no other module's model is touched; the
module creates and drops that org's schema, and the demo org's.
"""

# Requirements: REQ-1919

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml
from sqlalchemy import select

from provisa.core.database import Database, create_engine_from_url

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

_ORG_ID = "seedreboot"
_SCHEMA = f"org_{_ORG_ID}"
_DEMO_ID = "seeddemo"
_DEMO_SCHEMA = f"org_{_DEMO_ID}"
_SAMPLE = Path(__file__).resolve().parents[1] / "fixtures" / "sample_config.yaml"


def _first_file() -> dict:
    raw = yaml.safe_load(_SAMPLE.read_text())
    raw["roles"] = [
        *raw.get("roles", []),
        {"id": "auditor", "capabilities": ["usage"], "domain_access": ["sales-analytics"]},
    ]
    return raw


def _changed_file() -> dict:
    raw = _first_file()
    raw["roles"] = [r for r in raw["roles"] if r["id"] != "auditor"] + [
        {"id": "buyer", "capabilities": ["usage"], "domain_access": ["sales-analytics"]}
    ]
    raw["tables"] = [t for t in raw["tables"] if t["table"] != "customers"]
    raw["sources"][0]["host"] = "elsewhere.invalid"
    return raw


def _write(path: Path, raw: dict) -> None:
    path.write_text(yaml.safe_dump(raw, sort_keys=False))


def _plane() -> Database:
    return Database(
        create_engine_from_url(os.environ["TENANT_DATABASE_URL"]),
        name="seed-reboot",
        search_path=_SCHEMA,
    )


async def _drop_schema() -> None:
    plane = Database(create_engine_from_url(os.environ["TENANT_DATABASE_URL"]), name="drop")
    try:
        async with plane.acquire() as conn:
            for schema in (_SCHEMA, _DEMO_SCHEMA):
                await conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
                await conn.execute(f"DROP SCHEMA IF EXISTS {schema}_mv_cache CASCADE")
    finally:
        await plane.close()


async def _model() -> dict:
    from provisa.core.store_config import store_model

    plane = _plane()
    try:
        async with plane.acquire() as conn:
            return await store_model(conn)
    finally:
        await plane.close()


async def _seeded() -> bool:
    from provisa.core.schema_org import model_seed

    plane = _plane()
    try:
        async with plane.acquire() as conn:
            return (await conn.execute_core(select(model_seed.c.id))).fetchone() is not None
    finally:
        await plane.close()


async def _boot(config: Path, monkeypatch, then=None):
    """One launch of the deployment with ``config`` as its file; ``then(state)`` runs while it
    is up. Returns what ``then`` returned."""
    from provisa.api.app import create_app, state

    monkeypatch.setenv("PROVISA_CONFIG", str(config))
    monkeypatch.setenv("ORG_ID", _ORG_ID)
    monkeypatch.setenv("PG_PASSWORD", os.environ.get("PG_PASSWORD", "provisa"))
    app = create_app()
    async with app.router.lifespan_context(app):
        assert state.org_id == _ORG_ID
        return None if then is None else await then(state)


@pytest.fixture
async def clean():
    await _drop_schema()
    yield
    await _drop_schema()


async def test_a_reboot_never_reapplies_the_file_and_an_admin_edit_survives(
    clean, tmp_path, monkeypatch
):
    from provisa.core import model_change
    from provisa.core.models import Role
    from provisa.core.repositories import role as role_repo

    config = tmp_path / "provisa.yaml"

    # 1. The first start seeds the empty store from the file.
    _write(config, _first_file())
    await _boot(config, monkeypatch)
    assert await _seeded()
    first = await _model()
    assert {r["id"] for r in first["roles"]} >= {"analyst", "auditor"}
    assert {t["table"] for t in first["tables"]} >= {"orders", "customers"}
    assert first["sources"][0]["host"] == "localhost"

    # 2. An admin changes the model: a seeded role is redefined.
    async def _edit(state):
        async with model_change.scope("admin edit"), state.model_db.acquire() as conn:
            await role_repo.upsert(
                conn,
                Role(id="auditor", capabilities=["usage", "write"], domain_access=["*"]),
                org_id=_ORG_ID,
            )

    await _boot(config, monkeypatch, then=_edit)
    edited = await _model()
    auditor = next(r for r in edited["roles"] if r["id"] == "auditor")
    assert (sorted(auditor["capabilities"]), auditor["domain_access"]) == (
        ["usage", "write"],
        ["*"],
    )

    # 3. A reboot with a changed file changes nothing in the store.
    _write(config, _changed_file())

    async def _process_config(state):
        return state.config

    running = await _boot(config, monkeypatch, then=_process_config)
    assert await _model() == edited
    # The process runs the store's model: the file's new host, its dropped table and role, and
    # its new role are none of the running configuration's.
    assert running.sources[0].host == "localhost"
    assert "customers" in {t.table_name for t in running.tables}
    assert "auditor" in {r.id for r in running.roles}
    assert "buyer" not in {r.id for r in running.roles}

    # 4. An explicit apply of the changed file adds and updates, and removes nothing.
    async def _apply(state):
        from provisa.core.config_loader import apply_config, parse_config_dict
        from provisa.core.secrets_store import bound_to_request_org

        async with bound_to_request_org(), state.model_db.acquire() as conn:
            await apply_config(parse_config_dict(_changed_file()), conn)

    await _boot(config, monkeypatch, then=_apply)
    applied = await _model()
    assert {r["id"] for r in applied["roles"]} >= {"auditor", "buyer"}
    assert {t["table"] for t in applied["tables"]} >= {"orders", "customers"}
    assert applied["sources"][0]["host"] == "elsewhere.invalid"


async def test_a_demo_org_starts_as_its_config_at_every_build(clean, tmp_path, monkeypatch):
    """REQ-1919 (DEMO ORGANISATIONS ARE THEIR CONFIG): an edit to a demo organisation lasts until
    its next build, which applies the demo configuration again."""
    from provisa.api.app import build_org_runtime
    from provisa.core import model_change
    from provisa.core.models import Role
    from provisa.core.repositories import role as role_repo
    from provisa.core.request_context import reset_current_org, set_current_org

    config = tmp_path / "provisa.yaml"
    _write(config, _first_file())

    async def _auditor(rt) -> list[str]:
        async with rt.model_db.acquire() as conn:
            held = await role_repo.get(conn, "auditor")
        assert held is not None
        return sorted(held["capabilities"])

    async def _demo(state):
        token = set_current_org(_DEMO_ID)
        try:
            rt = await build_org_runtime(_DEMO_ID, include_demo=True)
            assert await _auditor(rt) == ["usage"]
            async with model_change.scope("demo edit"), rt.model_db.acquire() as conn:
                await role_repo.upsert(
                    conn,
                    Role(id="auditor", capabilities=["usage", "write"], domain_access=["*"]),
                    org_id=_DEMO_ID,
                )
            assert await _auditor(rt) == ["usage", "write"]
            rebuilt = await build_org_runtime(_DEMO_ID, include_demo=True)
            return await _auditor(rebuilt)
        finally:
            reset_current_org(token)

    assert await _boot(config, monkeypatch, then=_demo) == ["usage"]
