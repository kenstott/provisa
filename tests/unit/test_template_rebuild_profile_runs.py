# Copyright (c) 2026 Kenneth Stott
# Canary: c75d708c-d281-4548-b719-bb238c05a643
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A demo template keeps its bundle's profile runs through every rebuild of its model (REQ-2052).

A template org is a demo organisation: every build of its runtime removes its model and applies
its demo's configuration again (REQ-1919), and each table is registered afresh, under a new id. A
profile result table is named by its member's id (``<member>_<id>_profile_<kind>``), so after the
rebuild the profiling demo's ``customer_profile_runs`` named a relation nothing had loaded. On
cloud the start served the profiling template as built, holding the runs (``ready (as built)``);
its runtime was built at the next open, and every sandbox cloned from it after that answered
``SELECT ... FROM "harbor"."customer_profile_runs"`` with no rows."""

# Requirements: REQ-2052, REQ-1919, REQ-2006

from __future__ import annotations

import contextlib
import json
from pathlib import Path

import pytest
import yaml
from sqlalchemy import func, insert, select

# The API layer says at import whether the deployment holds many organizations, which the apply
# asks; run alone, nothing else here imports it.
import provisa.api.app  # noqa: F401
from provisa.api import demo_templates
from provisa.core import config_loader, secrets_store
from provisa.core.environments import org_schema
from provisa.core.request_context import reset_current_org, set_current_org
from provisa.core.schema_org import metadata, registered_tables
from provisa.profiler import carry
from tests.pg_control_plane import ORG

pytestmark = pytest.mark.unit

_PROFILING = Path(__file__).resolve().parents[2] / "demo" / "named" / "profiling"
_CARRIED_RUNS = 2


@pytest.fixture(autouse=True)
def no_vault(monkeypatch):
    """The demo's source carries no secret reference: the apply needs no org vault bound."""

    @contextlib.asynccontextmanager
    async def _unbound():
        yield

    monkeypatch.setattr(secrets_store, "bound_to_request_org", _unbound)


async def _bound(control_plane, monkeypatch):
    """The template org's model store, bound as its runtime build binds it."""
    # The namespace of the demo's data in the node's stores, as template_seed gives it.
    monkeypatch.setenv("PROFILING_PG_NS", "")
    plane = control_plane.org()
    async with plane.acquire() as conn:
        # The built-in source views register on, which the startup seed creates first.
        await conn.execute_core(
            insert(metadata.tables["sources"]).values(id="__derived__", type="postgresql")
        )
    token = set_current_org(ORG)
    yield plane
    reset_current_org(token)


@pytest.fixture
async def template(control_plane, monkeypatch):
    async for plane in _bound(control_plane, monkeypatch):
        yield plane


@pytest.fixture(scope="module")
def small_lock_table():
    """A Postgres of its own whose lock table is small (max_locks_per_transaction 16, about 2,000
    slots with the default max_connections), so a backlog larger than it is quick to build."""
    from tests.embedded_pg import start_local_postgres
    from tests.pg_control_plane import build_template

    pg = start_local_postgres(settings={"max_locks_per_transaction": "16"})
    yield pg, build_template(pg)
    pg.stop()


@pytest.fixture
def small_lock_plane(small_lock_table):
    from tests.pg_control_plane import clone

    pg, built = small_lock_table
    plane = clone(pg, built)
    yield plane
    plane.close()


@pytest.fixture
async def small_lock_template(small_lock_plane, monkeypatch):
    async for plane in _bound(small_lock_plane, monkeypatch):
        yield plane


def _bundle(tmp_path: Path) -> Path:
    """The profiling demo's bundle as the build packs it: its config and setup, and the runs its
    setup's two profile steps made."""
    for name in ("config.yaml", "setup.yaml"):
        (tmp_path / name).write_text((_PROFILING / name).read_text())
    runs = [{"run_id": f"r{i}", "status": "ok"} for i in range(_CARRIED_RUNS)]
    path = carry.results_path(tmp_path)
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {"results": [{"table": ["crm", "public", "customers"], "kind": "runs", "rows": runs}]}
        )
    )
    return tmp_path


def _seed(directory: Path):
    return config_loader.parse_config_dict(yaml.safe_load((directory / "config.yaml").read_text()))


async def _served(plane) -> int:
    """The rows ``harbor.customer_profile_runs`` reads: the relation its registration names."""
    from sqlalchemy import table

    async with plane.acquire() as conn:
        name = (
            await conn.execute_core(
                select(registered_tables.c.table_name).where(
                    registered_tables.c.alias == "customer_profile_runs"
                )
            )
        ).scalar_one()
        return (await conn.execute_core(select(func.count()).select_from(table(name)))).scalar_one()


async def _unheld(plane, directory: Path) -> list[str]:
    async with plane.acquire() as conn:
        return await carry.unheld_runs(conn, carry.results_path(directory), org_schema(ORG))


async def test_a_template_rebuilt_from_its_config_still_serves_its_bundles_runs(template, tmp_path):
    directory = _bundle(tmp_path)
    # The template as its build leaves it: its runtime built, then its bundle's runs loaded
    # (_prepare, profile_template).
    async with template.acquire() as conn:
        await demo_templates.rebuild_template(directory, _seed(directory), conn, None)
        await carry.load_runs(conn, carry.results_path(directory))
    assert await _unheld(template, directory) == []
    assert await _served(template) == _CARRIED_RUNS

    # The next start serves it as built; the next open builds its runtime, which rebuilds its model.
    async with template.acquire() as conn:
        await demo_templates.rebuild_template(directory, _seed(directory), conn, None)
    assert await _unheld(template, directory) == []
    assert await _served(template) == _CARRIED_RUNS


async def _result_relations(plane, schema: str) -> list[str]:
    from sqlalchemy import text

    from provisa.profiler.schema import is_result_table_name

    async with plane.acquire() as conn:
        rows = (
            await conn.execute_core(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = :schema ORDER BY table_name"
                ).bindparams(schema=schema)
            )
        ).fetchall()
    return [r[0] for r in rows if is_result_table_name(r[0])]


async def _current_relations(plane) -> list[str]:
    """The result relations the model names as current: one per carried run's table, under the
    id the model gives that table now."""
    async with plane.acquire() as conn:
        (table_id,) = (
            await conn.execute_core(
                select(registered_tables.c.id).where(
                    registered_tables.c.source_id == "crm",
                    registered_tables.c.schema_name == "public",
                    registered_tables.c.table_name == "customers",
                )
            )
        ).one()
    return [f"customers_{table_id}_profile_runs"]


async def test_a_rebuild_leaves_no_superseded_result_relation_and_a_sandbox_copies_none(
    template, tmp_path
):
    """Each rebuild renumbers the template's tables, and the relations named by the old ids were
    left behind: about 40 per rebuild for profiling, each copied into every sandbox of it
    (env_copy.copy_profile_results). A rebuild removes them in its own transaction."""
    from sqlalchemy import text

    from provisa.core.env_copy import copy_profile_results

    directory = _bundle(tmp_path)
    for _ in range(3):
        async with template.acquire() as conn:
            await demo_templates.rebuild_template(directory, _seed(directory), conn, None)
    current = await _current_relations(template)
    assert await _result_relations(template, org_schema(ORG)) == current
    assert await _served(template) == _CARRIED_RUNS

    sandbox = "sbx_profile_copy"
    async with template.acquire() as conn, conn.transaction():
        await conn.execute_core(text(f'DROP SCHEMA IF EXISTS "{sandbox}" CASCADE'))
        await conn.execute_core(text(f'CREATE SCHEMA "{sandbox}"'))
        copied = await copy_profile_results(conn, org_schema(ORG), sandbox)
    try:
        assert copied == current
        assert await _result_relations(template, sandbox) == current
    finally:
        async with template.acquire() as conn:
            await conn.execute_core(text(f'DROP SCHEMA "{sandbox}" CASCADE'))


def _lock_slots(control_plane) -> int:
    """The lock table's documented size: max_locks_per_transaction x (max_connections +
    max_prepared_transactions)."""
    import psycopg

    with psycopg.connect(control_plane.pg.dsn(control_plane.name)) as conn:
        locks, connections, prepared = (
            int(conn.execute(f"SHOW {name}").fetchone()[0])  # type: ignore[index]
            for name in (
                "max_locks_per_transaction",
                "max_connections",
                "max_prepared_transactions",
            )
        )
    return locks * (connections + prepared)


def _plant_superseded(control_plane, like: str, count: int) -> list[str]:
    """``count`` result relations of ids no table holds, as the rebuilds of a template left them:
    each created in its own transaction, as each rebuild created its own."""
    import psycopg

    schema = org_schema(ORG)
    names = [f"customers_{900000 + i}_profile_runs" for i in range(count)]
    with psycopg.connect(control_plane.pg.dsn(control_plane.name), autocommit=True) as conn:
        for name in names:
            conn.execute(f'CREATE TABLE "{schema}"."{name}" (LIKE "{schema}"."{like}")')
    return names


async def test_a_rebuild_drops_a_backlog_larger_than_the_lock_table(
    small_lock_template, small_lock_plane, tmp_path
):
    """On cloud the control database held 36,869 relations, most of them superseded result
    relations, and the rebuild dropped them all in its one transaction: every DROP holds its
    locks to the commit, and the API's start failed with 'out of shared memory'. A backlog of
    more relations than the lock table has slots is dropped, and the rebuild starts."""
    directory = _bundle(tmp_path)
    async with small_lock_template.acquire() as conn:
        await demo_templates.rebuild_template(directory, _seed(directory), conn, None)
    (current,) = await _current_relations(small_lock_template)
    _plant_superseded(small_lock_plane, current, _lock_slots(small_lock_plane))

    async with small_lock_template.acquire() as conn:
        await demo_templates.rebuild_template(directory, _seed(directory), conn, None)

    assert await _result_relations(small_lock_template, org_schema(ORG)) == (
        await _current_relations(small_lock_template)
    )
    assert await _served(small_lock_template) == _CARRIED_RUNS


async def test_a_sandbox_copies_no_superseded_result_relation(template, control_plane, tmp_path):
    """A template's superseded relations were copied into every sandbox cloned from it
    (env_copy.copy_profile_results), each sandbox a second copy of the backlog. Only the
    relations its model names are copied."""
    from sqlalchemy import text

    from provisa.core.env_copy import copy_profile_results

    directory = _bundle(tmp_path)
    async with template.acquire() as conn:
        await demo_templates.rebuild_template(directory, _seed(directory), conn, None)
    current = await _current_relations(template)
    _plant_superseded(control_plane, current[0], 3)

    sandbox = "sbx_profile_superseded"
    async with template.acquire() as conn, conn.transaction():
        await conn.execute_core(text(f'DROP SCHEMA IF EXISTS "{sandbox}" CASCADE'))
        await conn.execute_core(text(f'CREATE SCHEMA "{sandbox}"'))
        copied = await copy_profile_results(conn, org_schema(ORG), sandbox)
    try:
        assert copied == current
        assert await _result_relations(template, sandbox) == current
    finally:
        async with template.acquire() as conn:
            await conn.execute_core(text(f'DROP SCHEMA "{sandbox}" CASCADE'))
