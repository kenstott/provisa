# Copyright (c) 2026 Kenneth Stott
# Canary: 9994a10b-f3c8-4493-a833-2c61c34962b7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Test (synthetic) environment's whole-model generation (REQ-1942).

Test (synthetic) generates the whole model -- implicit relationships make generating part of it
unsafe: every table, each from a profile run in the environment's parent, the latest successful
one unless the operator picks another, or from a declared profile. Nothing in a Test (synthetic)
environment calls a source API: an API table that can be read in full is generated like any
table; one that needs a required parameter has no full set of rows to measure, so it is generated
only from a declared profile and is otherwise not available there, a read of it refused; and the
commands backed by a source bound to the synthetic store -- an API's operations, a database's
procedures -- are not defined there, a call to one refused.

Generation is two steps. Generate answers the Limitations of Synthetic Data warning -- what the
environment loses and, for this generation, what will be generated, what will not be available
and what will not be defined -- and generates nothing; confirming that warning, by its digest,
starts the generation. It runs in the background: the environment shows Generating, then Ready,
or Failed with the reason; until it finishes, its tables read what they read before.
"""

# Requirements: REQ-1942, REQ-1939

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

log = logging.getLogger(__name__)

#: The dataset a Test (synthetic) environment's whole model is generated as.
DATASET_ID = "model"

#: What a Test (synthetic) environment loses, whatever is generated (REQ-1942, Limitations of
#: Synthetic Data): by a stable key, so the admin UI words each in the reader's language.
LIMITATIONS: tuple[dict[str, str], ...] = (
    {
        "key": "no_source_api",
        "text": "A synthetic environment calls no source API: every source is read from the "
        "generated data in the environment's synthetic store.",
    },
    {
        "key": "required_parameter_tables",
        "text": "A table that is read from an API only by a required parameter has no full set "
        "of rows to generate from. It is generated only from a declared profile, and is "
        "otherwise not available in the environment.",
    },
    {
        "key": "source_commands",
        "text": "The commands backed by a generated source -- an API's operations, a database's "
        "procedures -- are not defined in the environment: a call to one is refused.",
    },
    {
        "key": "manual_fix_up",
        "text": "A developer can restore them by hand: for example by standing up their own "
        "instance of the API that reads the synthetic tables, and editing the source on the "
        "Sources page to point at it.",
    },
)

#: What a required parameter is, by the type of the API source it is a parameter of: the
#: native-filter kind of the column carrying it. An OpenAPI path parameter; a remote GraphQL
#: field's required argument; a remote gRPC method's input field.
_REQUIRED_PARAMETER: dict[str, str] = {
    "openapi": "path_param",
    "graphql_remote": "query_param",
    "grpc_remote": "grpc_input",
}


def api_source_types() -> frozenset[str]:
    """The source types whose tables are backed by an API."""
    from provisa.api_source.models import ApiSourceType
    from provisa.compiler.complexity import REMOTE_API_SOURCE_TYPES

    return frozenset(e.value for e in ApiSourceType) | REMOTE_API_SOURCE_TYPES


def required_parameters(table: dict, source_type: str) -> list[str]:
    """The columns of ``table`` (a fetch_tables row) carrying a parameter its API cannot be read
    without: with any, the table has no full set of rows to measure or to generate from."""
    kind = _REQUIRED_PARAMETER.get(source_type)
    if kind is None:
        return []
    return [c["column_name"] for c in table["columns"] if c["native_filter_type"] == kind]


def unavailable_reason(table_name: str, parameters: list[str]) -> str:
    """Why a required-parameter API table is not available in a Test (synthetic) environment,
    and what generates it (REQ-1942)."""
    return (
        f"{table_name!r} is not available in a Test (synthetic) environment: it is read from "
        f"its API only by the required parameter(s) {', '.join(parameters)}, so it has no full "
        f"set of rows to generate from. Declare a profile of it and generate again to generate it."
    )


def undefined_reason(command: str, source_id: str) -> str:
    """Why a command of a source bound to a synthetic store is not defined there (REQ-1942)."""
    return (
        f"Command {command!r} is not defined in this environment: it is backed by the source "
        f"{source_id!r}, which is bound to a synthetic store here, and nothing in a synthetic "
        f"environment calls a source API or a procedure of a source."
    )


async def _source_types(conn: Any) -> dict[str, str]:
    """Each source's type as the model gives it: a source already bound to a synthetic store is
    still the API, or the database, it was generated from."""
    from provisa.core.env_classes import BINDING_COLUMN, model_type
    from provisa.core.schema_org import sources
    from sqlalchemy import select

    rows = await conn.execute_core(
        select(sources.c.id, sources.c.type, sources.c[BINDING_COLUMN], sources.c.synthetic)
    )
    return {r._mapping["id"]: model_type(dict(r._mapping)) for r in rows.fetchall()}


async def source_commands(conn: Any, source_ids: set[str]) -> dict[str, list[str]]:
    """The commands backed by each of ``source_ids``, by source id (REQ-1942): an API's
    operations and a database's procedures alike -- what binding the source to a synthetic store
    leaves not defined."""
    from provisa.core.schema_org import tracked_functions as tf
    from sqlalchemy import select

    out: dict[str, list[str]] = {}
    rows = await conn.execute_core(select(tf.c.name, tf.c.source_id).order_by(tf.c.name))
    for name, source_id in rows.fetchall():
        if source_id in source_ids:
            out.setdefault(source_id, []).append(name)
    return out


async def undefined_commands(conn: Any) -> dict[str, str]:
    """``{command: why it is not defined}`` in the environment ``conn`` is scoped to: the
    commands of each source bound to a synthetic store (REQ-1942) -- by generating the
    environment's model, or copied from the environment it was created from."""
    from provisa.synthetic.datasets import synthetic_sources

    bound = set(await synthetic_sources(conn))
    if not bound:
        return {}
    return {
        name: undefined_reason(name, source_id)
        for source_id, names in (await source_commands(conn, bound)).items()
        for name in names
    }


async def record_generation(conn: Any, rows: int, seconds: float) -> None:
    """Record a finished whole-model generation of the environment ``conn`` is scoped to."""
    from datetime import UTC, datetime

    from provisa.core.schema_org import synthetic_generations as sg
    from sqlalchemy import insert

    await conn.execute_core(
        insert(sg).values(finished_at=datetime.now(UTC), generated_rows=rows, seconds=seconds)
    )


async def last_generation(conn: Any) -> tuple[int, float] | None:
    """The rows and seconds of the environment's last finished whole-model generation, or None
    before its first."""
    from provisa.core.schema_org import synthetic_generations as sg
    from sqlalchemy import select

    row = (
        await conn.execute_core(
            select(sg.c.generated_rows, sg.c.seconds).order_by(sg.c.finished_at.desc()).limit(1)
        )
    ).fetchone()
    return None if row is None else (int(row[0]), float(row[1]))


async def model_plan(state: Any, conn: Any, parent: str, env: str) -> dict[str, Any]:
    """What a whole-model generation in ``env`` (the environment ``conn`` is scoped to) would do
    (REQ-1942). ``tables``: every table it may generate, each with the profiles it may be
    generated from, newest first -- the parent's successful profile runs, measured or declared,
    and ``env``'s own declared profiles -- and the one preselected: the parent's latest measured
    run, else the latest declared profile. A table that needs a required parameter
    (``requiredParameters``) is generated only from a declared profile; with none it is in
    ``unavailable``, with why, and does not hold Generate back. ``commandsNotDefined``: the
    commands of each API source. ``ready`` when every other table has a profile and no column of
    it is left with nothing to generate from."""
    from provisa.api.admin.db_queries import fetch_relationships, fetch_tables
    from provisa.core.models import BUILT_IN_SOURCE_IDS
    from provisa.core.request_context import require_current_org
    from provisa.synthetic.run import profile_runs_in, uncovered_columns

    # The model's own tables: the built-in sources' (the catalog, the observability store) are the
    # platform's, not the operator's data.
    tables = [t for t in await fetch_tables(conn) if t["source_id"] not in BUILT_IN_SOURCE_IDS]
    types = await _source_types(conn)
    api_types = api_source_types()
    runs: dict[int, list[dict]] = {}
    for r in await profile_runs_in(conn, parent, tables):
        runs.setdefault(r["tableId"], []).extend(r["runs"])
    for r in await profile_runs_in(conn, env, tables):
        runs.setdefault(r["tableId"], []).extend(x for x in r["runs"] if x["origin"] == "declared")
    relationships = await fetch_relationships(conn)
    entries, unavailable = [], []
    for t in tables:
        source_type = types[t["source_id"]]
        required = required_parameters(t, source_type)
        held = sorted(runs.get(t["id"], []), key=lambda x: x["runTime"], reverse=True)
        if required:
            held = [x for x in held if x["origin"] == "declared"]  # nothing could measure it
        preferred = [x for x in held if x["origin"] == "measured"] or held
        selected = preferred[0] if preferred else None
        if required and selected is None:
            unavailable.append(
                {
                    "tableId": t["id"],
                    "tableName": t["table_name"],
                    "source": t["source_id"],
                    "requiredParameters": required,
                    "reason": unavailable_reason(t["table_name"], required),
                }
            )
            continue
        entries.append(
            {
                "tableId": t["id"],
                "tableName": t["table_name"],
                "source": t["source_id"],
                "api": source_type in api_types,
                "requiredParameters": required,
                "runs": held,
                "selected": None if selected is None else selected["runId"],
                # REQ-1942: what the preselected profile leaves with nothing to generate from.
                "uncovered": []
                if selected is None
                else await uncovered_columns(
                    conn,
                    org_id=require_current_org(),
                    env=selected["env"],
                    table=t,
                    run_id=selected["runId"],
                    relationships=relationships,
                ),
            }
        )
    store_type = state.federation_engine.engine.replica_store_backend()
    return {
        "parent": parent,
        "tables": entries,
        # REQ-1942: each source whose type and connection generating changes to the store's.
        "sources": [
            {"id": sid, "type": types[sid], "becomes": store_type}
            for sid in sorted({t["source_id"] for t in tables})
        ],
        "unavailable": unavailable,
        "commandsNotDefined": [
            {"source": sid, "commands": names}
            for sid, names in sorted(
                (await source_commands(conn, {t["source_id"] for t in tables})).items()
            )
        ],
        # REQ-1942: (rows, seconds) of the environment's last finished generation, or None.
        "lastGeneration": await last_generation(conn),
        "ready": all(e["selected"] is not None and not e["uncovered"] for e in entries),
    }


def warning(
    plan: dict[str, Any],
    runs: dict[int, tuple[str, str]],
    *,
    seed: int,
    scale: float,
    kept_mutations: dict[int, int],
) -> dict[str, Any]:
    """The Limitations of Synthetic Data warning of one generation (REQ-1942): what a synthetic
    environment loses in general, and for this generation -- ``plan``'s tables from ``runs``
    (each as the environment holding its profile and the run id) at ``scale`` -- each table to
    be generated with its profile and its estimated rows, each source whose type
    and connection change to the synthetic store's, each table that will not be available and why, each command that will not be
    defined, by its source, and the kept mutations generating discards, by table id. Its
    ``digest`` names exactly this content: what the operator confirms."""
    tables = []
    for t in plan["tables"]:
        held_in, run_id = runs[t["tableId"]]
        (run,) = [r for r in t["runs"] if r["runId"] == run_id and r["env"] == held_in]
        tables.append(
            {
                "tableId": t["tableId"],
                "tableName": t["tableName"],
                "source": t["source"],
                "api": t["api"],
                "profile": {"runId": run_id, "env": held_in, "origin": run["origin"]},
                "scale": scale,
                "estimatedRows": round(run["rowCount"] * scale),
            }
        )
    total = sum(t["estimatedRows"] for t in tables)
    last = plan["lastGeneration"]
    body: dict[str, Any] = {
        "title": "Limitations of Synthetic Data",
        "estimatedRows": total,
        # From the environment's last finished generation; None: no estimate before the first.
        "estimatedSeconds": None if last is None or not last[0] else total * last[1] / last[0],
        "limitations": list(LIMITATIONS),
        "seed": seed,
        "scale": scale,
        "tables": tables,
        "sources": plan["sources"],
        "unavailable": plan["unavailable"],
        "commandsNotDefined": plan["commandsNotDefined"],
        "keptMutationsDiscarded": {str(tid): n for tid, n in sorted(kept_mutations.items())},
    }
    digest = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()
    return {**body, "digest": digest}


def start(
    state: Any,
    *,
    org_id: str,
    env: str,
    runs: dict[int, tuple[str, str]],
    seed: int,
    scale: float,
    rows: int,
) -> None:
    """Generate ``env``'s whole model in the background from ``runs`` -- each generated table's
    profile, as (the environment holding it, its run id) -- recording Generating, then Ready or
    Failed with the reason."""
    # Work that outlives its request runs on a background worker, never as a task of the
    # request's own loop, which ends with the request.
    from provisa.core.connection_loop import spawn_background

    spawn_background(
        _generate(
            state,
            org_id=org_id,
            env=env,
            runs=runs,
            seed=seed,
            scale=scale,
            generated_rows=rows,
        ),
        name=f"synthetic-model:{org_id}/{env}",
    )


async def _generate(
    state: Any,
    *,
    org_id: str,
    env: str,
    runs: dict[int, tuple[str, str]],
    seed: int,
    scale: float,
    generated_rows: int,
) -> None:
    import time

    from provisa.api.app import ensure_org_runtime
    from provisa.core.env_store import set_data
    from provisa.core.request_context import (
        reset_current_env,
        reset_current_org,
        set_current_env,
        set_current_org,
    )
    from provisa.synthetic.datasets import DatasetTableRow, define
    from provisa.synthetic.run import check_closure_of, generate, store_schema

    await ensure_org_runtime(org_id, env)
    org_token = set_current_org(org_id)
    env_token = set_current_env(env)
    try:
        rows = [
            DatasetTableRow(tid, held_in, run_id, None)
            for tid, (held_in, run_id) in sorted(runs.items())
        ]
        async with state.model_db.acquire() as conn:
            await check_closure_of(conn, [r.table_id for r in rows])
            await define(
                conn,
                dataset_id=DATASET_ID,
                seed=seed,
                scale=scale,
                store_schema=store_schema(DATASET_ID),
                tables=rows,
            )
        started = time.monotonic()
        await generate(state, DATASET_ID)
        async with state.model_db.acquire() as conn:
            await record_generation(conn, generated_rows, time.monotonic() - started)
        bound = await bind_generated(state, org_id, env)
        log.info("whole-model generation of %s/%s bound sources %s", org_id, env, bound)
    except Exception as exc:  # noqa: BLE001 -- REQ-1942: the environment shows Failed with the reason
        log.exception("whole-model generation of %s/%s failed", org_id, env)
        await set_data(
            state.admin_db,
            org_id,
            env,
            data_status="failed",
            data_error=f"{type(exc).__name__}: {exc}",
        )
        return
    finally:
        reset_current_env(env_token)
        reset_current_org(org_token)
    await set_data(state.admin_db, org_id, env, data_status="ready", data_error=None)
    # Its sources now point at the synthetic store, type and connection: the runtime is built
    # again from them on the next request.
    from provisa.api.org_runtime import runtime_key

    state.org_registry.invalidate(runtime_key(org_id, env))


async def bind_generated(state: Any, org_id: str, env: str) -> list[str]:
    """Bind every source of ``env``'s model to its synthetic store once its whole model is
    generated there (REQ-1942): type and connection, each naming the tables generated for it. A
    source none of whose tables was generated -- an API read only by a required parameter -- is
    bound too: nothing in the environment calls it. The ids bound."""
    from provisa.api.admin.db_queries import fetch_tables
    from provisa.core import env_data
    from provisa.core.environments import org_schema
    from provisa.core.models import BUILT_IN_SOURCE_IDS
    from provisa.core import model_change
    from provisa.synthetic.datasets import get_dataset

    # A binding is the environment's own and no part of the model a change commits (REQ-1524):
    # the type the model gives each source is kept, and is what a tree of it still carries.
    async with (
        model_change.committed_by_caller(),
        state.model_db.acquire() as conn,
        conn.transaction(),
    ):
        row = await get_dataset(conn, DATASET_ID)
        made = {t.table_id for t in row.tables}
        generated: dict[str, list[list[str]]] = {}
        for t in await fetch_tables(conn):
            if t["source_id"] in BUILT_IN_SOURCE_IDS:
                continue
            held = generated.setdefault(t["source_id"], [])
            if t["id"] in made:
                held.append([t["schema_name"], t["table_name"]])
        return await env_data.bind_synthetic(
            conn,
            org_schema(org_id, env),
            store_type=state.federation_engine.engine.replica_store_backend(),
            store_schema=row.store_schema,
            generated=generated,
        )
