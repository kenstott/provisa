# Copyright (c) 2026 Kenneth Stott
# Canary: 4ee1b532-0133-4de7-9907-f5d571d4c9fb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A demo's Test (synthetic) environments, generated once and carried like its replicas
(REQ-1942, REQ-2053).

A demo's setup.yaml may declare an environment in Test (synthetic): its whole model generated from
the profile of its parent, prod. Declaring it is the demo's confirmation of the generation's
warning. It is generated once, where the demo's template is first built: prod's profilers run, and
the environment is generated from their runs, as confirming the warning on the Environments page
generates it. The bundle build's seeding run writes what was generated -- the store schema of its
rows, and its dataset, generation and report records -- beside the replicas' seed; a template
built from the bundle adopts them instead of generating. Each sandbox's environment, created anew
from the sandbox's prod, is then given the template's: its rows copied into the sandbox's store,
its records into the sandbox's model, its sources bound to them, and marked generated. No
generation runs when a sandbox is opened. An ordinary environment is generated as it always is.
"""

# Requirements: REQ-1942, REQ-2053

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import delete, insert, select

log = logging.getLogger(__name__)

#: The seed's files for the generated environments: their rows' store, and their records.
SEED_STORE = "synthetic.duckdb"
SEED_RECORD = "synthetic.json"
#: Columns of the records carried that hold a time, written as ISO 8601.
_TIMES = ("generated_at", "created_at", "finished_at")
#: What a carried dataset table names its table by: [source id, schema, table].
_TABLE_KEY = "table"


def _record_tables() -> tuple[Any, ...]:
    """The environment's records of its generation, parents first."""
    from provisa.core.schema_org import (
        synthetic_dataset_tables,
        synthetic_datasets,
        synthetic_generations,
        synthetic_report,
    )

    return (synthetic_datasets, synthetic_dataset_tables, synthetic_report, synthetic_generations)


def synthetic_envs(environments: list[dict[str, Any]]) -> list[str]:
    """The names of ``environments`` (``{name, data_mode, ...}``) that are Test (synthetic)."""
    from provisa.core.env_classes import TEST_SYNTHETIC

    return [env["name"] for env in environments if env["data_mode"] == TEST_SYNTHETIC]


def seed_paths(demo_directory: Path) -> tuple[Path, Path]:
    from provisa.api.demo_replicas import SEED_DIR

    seed = demo_directory / SEED_DIR
    return seed / SEED_STORE, seed / SEED_RECORD


def _schema(org_id: str, env: str) -> str:
    from provisa.federation.replica_address import synthetic_schema
    from provisa.synthetic.env_model import DATASET_ID

    return synthetic_schema(org_id, env, DATASET_ID)


@contextmanager
def _bound(org_id: str, env: str) -> Iterator[None]:
    """``org_id``'s environment ``env`` bound to this task: its model store answers from its
    schema."""
    from provisa.core.request_context import (
        reset_current_env,
        reset_current_org,
        set_current_env,
        set_current_org,
    )

    org_token = set_current_org(org_id)
    env_token = set_current_env(env)
    try:
        yield
    finally:
        reset_current_env(env_token)
        reset_current_org(org_token)


async def _model_db(org_id: str, env: str) -> Any:
    from provisa.api.app import ensure_org_runtime

    rt = await ensure_org_runtime(org_id, env)
    assert rt.model_db is not None, "an environment's runtime holds its model store"
    return rt.model_db


def _admin() -> Any:
    """The platform control plane: always bound where a demo's template or sandbox is built."""
    from provisa.api.app import state

    assert state.admin_db is not None, "a demo's environments need the platform control plane"
    return state.admin_db


async def _env_row(org_id: str, env: str) -> dict[str, Any]:
    """The environment ``env`` of ``org_id``, which its demo's setup made before it is asked for."""
    from provisa.core.env_store import get_env

    row = await get_env(_admin(), org_id, env)
    assert row is not None, f"{org_id} has no environment {env!r}"
    return row


async def prepare(org_id: str, demo_directory: Path, envs: list[str], *, exporting: bool) -> None:
    """Give the template ``org_id`` each of its Test (synthetic) environments ``envs`` generated:
    adopted from the demo's seed when its bundle carries one, else generated here -- and, in the
    bundle build's seeding run, written out as the seed. The template org is bound."""
    if not envs:
        return
    store, record = seed_paths(demo_directory)
    if record.exists() and not exporting:
        held = json.loads(record.read_text())
        for env in envs:
            await _adopt(org_id, env, str(store), held["environments"][env])
        return
    await _profile_prod()
    for env in envs:
        await _generate(org_id, env)
    if exporting:
        await _export(org_id, envs, store, record)


async def _profile_prod() -> None:
    """Run every profiler of the bound org's prod, so each table has a run to generate from."""
    from provisa.api.app import state
    from provisa.core.models import SourceType
    from provisa.core.schema_org import sources
    from provisa.profiler.source import run_source

    assert state.model_db is not None, "the bound org's runtime holds its model store"
    async with state.model_db.acquire() as conn:
        profilers = [
            row[0]
            for row in (
                await conn.execute_core(
                    select(sources.c.id).where(sources.c.type == SourceType.data_profiler.value)
                )
            ).fetchall()
        ]
    for source_id in profilers:
        outcomes = await run_source(state, source_id)
        failed = [o for o in outcomes if o["error"] is not None]
        if failed:
            raise RuntimeError(f"profiler {source_id!r} failed on {failed}")


async def _generate(org_id: str, env: str) -> None:
    """Generate ``env`` from prod's latest successful profile runs, as confirming its warning does
    (environment_data_router.confirm_generation); refused by name when it does not end generated."""
    from provisa.api.app import state
    from provisa.api.admin.environment_data_router import kept_counts
    from provisa.core.env_store import set_data
    from provisa.core.environments import PROD, org_schema
    from provisa.synthetic import env_model

    db = await _model_db(org_id, env)
    with _bound(org_id, env):
        async with db.acquire() as conn:
            plan = await env_model.model_plan(state, conn, PROD, env)
    runs: dict[int, tuple[str, str]] = {}
    for t in plan["tables"]:
        chosen = t["selected"]
        if chosen is None:
            raise RuntimeError(f"{env!r}: {t['tableName']!r} has no profile run in prod")
        runs[t["tableId"]] = ({r["runId"]: r["env"] for r in t["runs"]}[chosen], chosen)
    from provisa.api.admin.orgs_router import _pool

    async with _pool().acquire() as conn:
        kept = await kept_counts(conn, org_schema(org_id, env))
    shown = env_model.warning(plan, runs, seed=0, scale=1.0, kept_mutations=kept)
    await set_data(
        _admin(),
        org_id,
        env,
        data_status="generating",
        data_error=None,
        synthetic_dataset=env_model.DATASET_ID,
    )
    await env_model._generate(  # noqa: SLF001 -- the generation itself, awaited, not spawned
        state,
        org_id=org_id,
        env=env,
        runs=runs,
        seed=0,
        scale=1.0,
        generated_rows=shown["estimatedRows"],
    )
    row = await _env_row(org_id, env)
    if row["data_status"] != "ready":
        raise RuntimeError(f"{org_id}/{env} was not generated: {row['data_error']}")
    log.info("demo template %s: %s generated", org_id, env)


def _plain(value: Any) -> Any:
    return value.isoformat() if isinstance(value, datetime) else value


async def _records(org_id: str, env: str) -> dict[str, list[dict[str, Any]]]:
    from provisa.core.schema_org import registered_tables

    db = await _model_db(org_id, env)
    out: dict[str, list[dict[str, Any]]] = {}
    with _bound(org_id, env):
        async with db.acquire() as conn:
            keys = {
                r.id: [r.source_id, r.schema_name, r.table_name]
                for r in (await conn.execute_core(select(registered_tables))).fetchall()
            }
            for table in _record_tables():
                rows = (await conn.execute_core(select(table))).fetchall()
                out[table.name] = [{k: _plain(v) for k, v in r._mapping.items()} for r in rows]
    # A table is carried by its source, schema and name: another deployment numbers it apart.
    for row in out["synthetic_dataset_tables"]:
        row[_TABLE_KEY] = keys[row.pop("table_id")]
    return out


async def _export(org_id: str, envs: list[str], store: Path, record: Path) -> None:
    """Write each generated environment's rows and records out as the demo's seed."""
    from provisa.api.demo_replicas import _store_path
    from provisa.federation.materialize_broker import copy_schema

    store.parent.mkdir(parents=True, exist_ok=True)
    store.unlink(missing_ok=True)
    held: dict[str, Any] = {}
    for env in envs:
        schema = _schema(org_id, env)
        await asyncio.to_thread(copy_schema, _store_path(org_id), schema, str(store), schema)
        held[env] = {"schema": schema, "records": await _records(org_id, env)}
    record.write_text(json.dumps({"environments": held}, indent=2, default=str) + "\n")


def _restored(table: Any, row: dict[str, Any], schema: str) -> dict[str, Any]:
    out = dict(row)
    for name in _TIMES:
        if out.get(name) is not None and name in table.c:
            out[name] = datetime.fromisoformat(out[name])
    if "store_schema" in table.c:
        out["store_schema"] = schema
    return out


async def _land(
    org_id: str, env: str, store: str, from_schema: str, records: dict[str, list[dict[str, Any]]]
) -> None:
    """Land generated rows (``from_schema`` of ``store``) and their records as ``org_id``'s
    ``env``, bind its sources to them, and mark it generated."""
    from provisa.api.app import state
    from provisa.api.demo_replicas import _store_path
    from provisa.api.org_runtime import runtime_key
    from provisa.core.env_store import set_data
    from provisa.federation.materialize_broker import copy_schema
    from provisa.synthetic import env_model

    schema = _schema(org_id, env)
    await asyncio.to_thread(copy_schema, store, from_schema, _store_path(org_id), schema)
    from provisa.core.schema_org import registered_tables

    db = await _model_db(org_id, env)
    with _bound(org_id, env):
        async with db.acquire() as conn, conn.transaction():
            ids = {
                (r.source_id, r.schema_name, r.table_name): r.id
                for r in (await conn.execute_core(select(registered_tables))).fetchall()
            }
            records = {
                **records,
                "synthetic_dataset_tables": [
                    {
                        **{k: v for k, v in r.items() if k != _TABLE_KEY},
                        "table_id": ids[tuple(r[_TABLE_KEY])],
                    }
                    for r in records["synthetic_dataset_tables"]
                ],
            }
            for table in reversed(_record_tables()):
                await conn.execute_core(delete(table))
            for table in _record_tables():
                rows = [_restored(table, r, schema) for r in records[table.name]]
                if rows:
                    await conn.execute_core(insert(table).values(rows))
        await env_model.bind_generated(state, org_id, env)
    state.org_registry.invalidate(runtime_key(org_id, env))
    await set_data(
        _admin(),
        org_id,
        env,
        data_status="ready",
        data_error=None,
        synthetic_dataset=env_model.DATASET_ID,
    )


async def _adopt(org_id: str, env: str, store: str, held: dict[str, Any]) -> None:
    await _land(org_id, env, store, held["schema"], held["records"])
    log.info("demo template %s: %s adopted from the seed", org_id, env)


async def copy_to_sandbox(template_org: str, org_id: str, envs: list[str]) -> None:
    """The template's generated environments ``envs``, into the sandbox ``org_id``'s environments
    of the same names; one the template has not generated is refused by name."""
    from provisa.api.demo_replicas import _store_path

    for env in envs:
        row = await _env_row(template_org, env)
        if row["data_status"] != "ready":
            raise RuntimeError(
                f"the {template_org} environment {env!r} is not generated ({row['data_status']}: "
                f"{row['data_error']}); its sandbox cannot be given its rows"
            )
        await _land(
            org_id,
            env,
            _store_path(template_org),
            _schema(template_org, env),
            await _records(template_org, env),
        )
