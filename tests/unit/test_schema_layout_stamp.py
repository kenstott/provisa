# Copyright (c) 2026 Kenneth Stott
# Canary: 3c7e91d2-5a48-4f06-b1e3-8d20c6f4a917
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An org schema is laid out (schema.sql, then its audit schema) only when its layout stamp is not
this release's: a rebuild beside a live runtime's reads runs no DDL, whose AccessExclusive locks
deadlocked those readers (J710). The stamp is written once both have run, so a layout that failed
part way runs again; the tenancy grants are asserted on every build."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from provisa.api import app
from provisa.audit import query_log
from provisa.audit.query_log import AUDIT_SCHEMA_SQL
from provisa.core import db, org_provisioning
from provisa.core.db import layout_digest, read_layout_digest

SCHEMA_SQL = (Path(app.__file__).parent.parent / "core" / "schema.sql").read_text()
ORG = "default"
SCHEMA = "org_default"


@pytest.fixture
def ran(monkeypatch):
    """The layout steps that ran, each still running for real."""
    steps: list[str] = []
    init_schema, init_audit_schema = db.init_schema, query_log.init_audit_schema

    async def _init_schema(*args, **kwargs):
        steps.append("init_schema")
        await init_schema(*args, **kwargs)

    async def _init_audit_schema(*args, **kwargs):
        steps.append("init_audit_schema")
        await init_audit_schema(*args, **kwargs)

    monkeypatch.setattr(db, "init_schema", _init_schema)
    monkeypatch.setattr(query_log, "init_audit_schema", _init_audit_schema)
    return steps


def test_the_digest_follows_what_the_build_ignores(monkeypatch):
    """The trigger function is replaced only when the layout runs again, and the layout runs again
    only when its digest changes (REQ-1914)."""
    from provisa.core import config_stamp

    laid_out = layout_digest(SCHEMA_SQL, AUDIT_SCHEMA_SQL)
    monkeypatch.setattr(config_stamp, "NOT_READ_BY_THE_BUILD", {"data_products": ("status",)})
    assert layout_digest(SCHEMA_SQL, AUDIT_SCHEMA_SQL) != laid_out


def _lay_out(pool, script: str = SCHEMA_SQL) -> bool:
    return asyncio.run(org_provisioning.lay_out_schema(pool, script, ORG))


def test_the_digest_is_the_layouts_and_changes_with_any_part_of_it():
    assert layout_digest(SCHEMA_SQL, AUDIT_SCHEMA_SQL) == layout_digest(
        SCHEMA_SQL, AUDIT_SCHEMA_SQL
    )
    assert layout_digest(SCHEMA_SQL + "\n-- changed", AUDIT_SCHEMA_SQL) != layout_digest(
        SCHEMA_SQL, AUDIT_SCHEMA_SQL
    )
    assert layout_digest(SCHEMA_SQL, AUDIT_SCHEMA_SQL + "\n-- changed") != layout_digest(
        SCHEMA_SQL, AUDIT_SCHEMA_SQL
    )
    # a part's boundary is part of it: moving text from one part to the next is another layout
    assert layout_digest(SCHEMA_SQL + "x", AUDIT_SCHEMA_SQL) != layout_digest(
        SCHEMA_SQL, "x" + AUDIT_SCHEMA_SQL
    )


def test_a_current_stamp_runs_no_ddl(control_plane, ran):
    pool = control_plane.org()
    assert _lay_out(pool) is True
    ran.clear()
    assert _lay_out(pool) is False
    assert ran == []


def test_a_changed_script_runs_it_once_and_writes_the_stamp(control_plane, ran):
    pool = control_plane.org()
    _lay_out(pool)
    changed = SCHEMA_SQL + "\n-- a later release's script\n"
    ran.clear()
    assert _lay_out(pool, changed) is True
    assert ran == ["init_schema", "init_audit_schema"]
    stamp = asyncio.run(read_layout_digest(pool, SCHEMA))
    assert stamp == layout_digest(changed, AUDIT_SCHEMA_SQL)
    ran.clear()
    assert _lay_out(pool, changed) is False
    assert ran == []


def test_a_failed_layout_writes_no_stamp_and_runs_again(control_plane, ran, monkeypatch):
    pool = control_plane.org()
    _lay_out(pool)
    before = asyncio.run(read_layout_digest(pool, SCHEMA))
    changed = SCHEMA_SQL + "\n-- a later release's script\n"

    async def _audit_fails(*args, **kwargs):
        ran.append("init_audit_schema")
        raise RuntimeError("the audit schema could not be laid out")

    with monkeypatch.context() as patched:
        patched.setattr(query_log, "init_audit_schema", _audit_fails)
        with pytest.raises(RuntimeError, match="audit schema"):
            _lay_out(pool, changed)
    assert asyncio.run(read_layout_digest(pool, SCHEMA)) == before
    ran.clear()
    assert _lay_out(pool, changed) is True
    assert ran == ["init_schema", "init_audit_schema"]


def test_a_schema_never_stamped_reads_no_stamp(control_plane):
    pool = control_plane.org()
    asyncio.run(_drop_stamp_table(pool))
    assert asyncio.run(read_layout_digest(pool, SCHEMA)) is None


async def _drop_stamp_table(pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(f'DROP TABLE IF EXISTS "{SCHEMA}".schema_layout')


def test_the_grants_are_asserted_whether_or_not_the_layout_runs(control_plane, ran, monkeypatch):
    pool = control_plane.org()
    granted: list[str] = []
    apply_grants = db.apply_tenancy_role_grants

    async def _grants(*args, **kwargs):
        granted.append(args[1])
        await apply_grants(*args, **kwargs)

    monkeypatch.setattr(db, "apply_tenancy_role_grants", _grants)
    monkeypatch.setattr(app, "state", SimpleNamespace(model_db=pool, multitenancy=True))
    asyncio.run(app._set_up_org_schema(ORG, "prod"))
    assert ran == ["init_schema", "init_audit_schema"] and granted == [ORG]
    ran.clear()
    asyncio.run(app._set_up_org_schema(ORG, "prod"))
    assert ran == [] and granted == [ORG, ORG]


def test_each_part_of_a_layout_is_timed_inside_a_timed_act(control_plane, caplog):
    """Inside a timed act (a sandbox's open, REQ-2006) a layout names what it spent where: its
    stamp's read, its script, its columns, roles and stamps, its audit schema and its stamp's
    write; a current stamp names only its read."""
    from provisa.core import step_clock

    pool = control_plane.org()
    changed = SCHEMA_SQL + "\n-- a later release's script\n"
    label = "demo sandbox sbx0 of alpha"

    def _timed(script: str) -> list[str]:
        caplog.clear()
        with caplog.at_level("WARNING"), step_clock.every_step_of(label):
            _lay_out(pool, script)
        return [
            message.removeprefix(f"{label}: layout of {SCHEMA}: ").rsplit(" took ", 1)[0]
            for record in caplog.records
            if (message := record.getMessage()).startswith(f"{label}: layout of {SCHEMA}: ")
        ]

    assert _timed(changed) == [
        "its stamp read",
        "its script",
        "its columns, roles and stamps",
        "its audit schema",
        "its stamp written",
    ]
    assert _timed(changed) == ["its stamp read"]
