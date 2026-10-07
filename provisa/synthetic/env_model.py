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
unsafe: every table that is not backed by an API, each from a profile run in the environment's
parent, the latest successful one unless the operator picks another. An API-backed table is never
generated: Provisa cannot repoint an API it does not own. The plan lists it with its key columns,
the rule its keys take on the synthetic side and the address the environment calls, and says
what a lookup returns: real records for keys drawn from real values, nothing for keys generated
fresh.

Generation runs in the background. The environment shows Generating, then Ready, or Failed with
the reason; until it finishes, its tables read what they read before.
"""

# Requirements: REQ-1942, REQ-1939

from __future__ import annotations

import asyncio
import logging
from typing import Any

log = logging.getLogger(__name__)

#: The dataset a Test (synthetic) environment's whole model is generated as.
DATASET_ID = "model"

#: The background generations running, kept until each finishes.
_RUNNING: set[asyncio.Task] = set()


def api_source_types() -> frozenset[str]:
    """The source types whose tables are backed by an API."""
    from provisa.api_source.models import ApiSourceType
    from provisa.compiler.complexity import REMOTE_API_SOURCE_TYPES

    return frozenset(e.value for e in ApiSourceType) | REMOTE_API_SOURCE_TYPES


def _copies_real_values(rule: str | None) -> bool:
    """Whether a key's rule draws its values from the real ones (a categories() naming none, a
    profile() fake), so a lookup by it reaches a real record."""
    from provisa.fakes.kinds import Categories, Profile, parse

    if rule is None:
        return False
    kind = parse(rule, rule=True)
    return isinstance(kind, Profile) or (isinstance(kind, Categories) and not kind.values)


def _api_entry(table: dict, source: dict) -> dict[str, Any]:
    keys = [c for c in table["columns"] if c.get("is_primary_key")]
    rules = {c["column_name"]: c.get("synthetic_rule") or c.get("fake") for c in keys}
    real = [name for name, rule in rules.items() if _copies_real_values(rule)]
    address = source.get("base_url") or (
        f"{source['host']}:{source['port']}" if source.get("host") else None
    )
    return {
        "tableId": table["id"],
        "tableName": table["table_name"],
        "keys": [c["column_name"] for c in keys],
        "keyRules": rules,
        "address": address,
        "lookup": (
            "a lookup by a key drawn from real values returns the real record; by a key "
            "generated fresh, nothing"
            if real
            else "every key is generated fresh, so a lookup returns nothing"
        ),
    }


async def model_plan(state: Any, conn: Any, parent: str, env: str) -> dict[str, Any]:
    """The tables a whole-model generation in ``env`` (the environment ``conn`` is scoped to)
    would generate -- each with the profiles it may be generated from, newest first: the parent's
    successful profile runs, measured or declared, and ``env``'s own declared profiles (REQ-1942)
    -- the one preselected: the parent's latest measured run, else the latest declared profile;
    and the API-backed tables it would not; ``ready`` when every generated table has one."""
    from provisa.api.admin.db_queries import fetch_tables
    from provisa.core.schema_org import sources
    from provisa.synthetic.run import profile_runs_in
    from sqlalchemy import select

    del state
    tables = await fetch_tables(conn)
    source_rows = {
        r._mapping["id"]: dict(r._mapping)
        for r in (await conn.execute_core(select(sources))).fetchall()
    }
    from provisa.core.models import BUILT_IN_SOURCE_IDS

    # The model's own tables: the built-in sources' (the catalog, the observability store) are the
    # platform's, not the operator's data.
    tables = [t for t in tables if t["source_id"] not in BUILT_IN_SOURCE_IDS]
    api_types = api_source_types()
    api = [t for t in tables if source_rows[t["source_id"]]["type"] in api_types]
    generated = [t for t in tables if source_rows[t["source_id"]]["type"] not in api_types]
    runs: dict[int, list[dict]] = {}
    for r in await profile_runs_in(conn, parent, generated):
        runs.setdefault(r["tableId"], []).extend(r["runs"])
    for r in await profile_runs_in(conn, env, generated):
        runs.setdefault(r["tableId"], []).extend(x for x in r["runs"] if x["origin"] == "declared")
    from provisa.api.admin.db_queries import fetch_relationships
    from provisa.core.request_context import require_current_org
    from provisa.synthetic.run import uncovered_columns

    relationships = await fetch_relationships(conn)
    entries = []
    for t in generated:
        held = sorted(runs.get(t["id"], []), key=lambda x: x["runTime"], reverse=True)
        preferred = [x for x in held if x["origin"] == "measured"] or held
        selected = preferred[0] if preferred else None
        entries.append(
            {
                "tableId": t["id"],
                "tableName": t["table_name"],
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
    return {
        "parent": parent,
        "tables": entries,
        "apiTables": [_api_entry(t, source_rows[t["source_id"]]) for t in api],
        "ready": all(e["selected"] is not None and not e["uncovered"] for e in entries),
    }


def start(
    state: Any,
    *,
    org_id: str,
    env: str,
    runs: dict[int, tuple[str, str]],
    seed: int,
    scale: float,
) -> None:
    """Generate ``env``'s whole model in the background from ``runs`` -- each generated table's
    profile, as (the environment holding it, its run id) -- recording Generating, then Ready or
    Failed with the reason."""
    task = asyncio.create_task(
        _generate(state, org_id=org_id, env=env, runs=runs, seed=seed, scale=scale)
    )
    _RUNNING.add(task)
    task.add_done_callback(_RUNNING.discard)


async def _generate(
    state: Any,
    *,
    org_id: str,
    env: str,
    runs: dict[int, tuple[str, str]],
    seed: int,
    scale: float,
) -> None:
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
        await generate(state, DATASET_ID)
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
