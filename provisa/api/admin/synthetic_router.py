# Copyright (c) 2026 Kenneth Stott
# Canary: 0f4c8b21-93d6-4e5a-a7b0-e2d1c6f8b359
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin routes of an environment's synthetic datasets (REQ-1939).

Every route acts in the environment the request selects (``x-provisa-env``); prod holds none.

* ``GET    /admin/synthetic-datasets`` — the environment's datasets and their tables.
* ``GET    /admin/synthetic-datasets/-/profile-runs?env=`` — the succeeded profile runs a dataset
  may name, per table, held by environment ``env``.
* ``PUT    /admin/synthetic-datasets/{id}`` — define or redefine a dataset.
* ``POST   /admin/synthetic-datasets/{id}/generate`` — generate it and report on it.
* ``GET    /admin/synthetic-datasets/{id}/report`` — how close it came to its profiles.
* ``DELETE /admin/synthetic-datasets/{id}`` — drop it: its tables read their bindings again.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel
from sqlalchemy import select

from provisa.api.admin.capabilities import require_capability_request
from provisa.api.app import state
from provisa.api.errors import ApiError

router = APIRouter(prefix="/admin/synthetic-datasets", tags=["admin", "synthetic"])

_RIGHT = "environment_management"


class TableIn(BaseModel):
    tableId: int
    profileEnv: str
    runId: str
    scale: float | None = None


class ConditionIn(BaseModel):
    relationship: str
    condition: str
    count: dict


class DatasetIn(BaseModel):
    seed: int
    scale: float
    tables: list[TableIn]
    # REQ-1939: conditional fan-out and assertions, each a field of the dataset's form.
    fanoutConditions: list[ConditionIn] = []
    assertions: list[str] = []
    # REQ-1939, DIFFERENTIAL PRIVACY: the privacy budget ε; omitted for a dataset not private.
    privateEpsilon: float | None = None
    # REQ-1939, NOT TOO CLOSE TO A REAL ROW (ruling Z2): both omitted, the check is off.
    closenessThreshold: float | None = None
    closenessDraws: int | None = None


def _db() -> Any:
    if state.model_db is None:
        raise ApiError(503, "synthetic.database_unavailable", "Database unavailable")
    return state.model_db


async def _require_read(request: Request) -> None:  # REQ-2155
    """Reading the datasets: environment_management, or environment_switch held by the owner of the
    environment the request acts in -- a sandbox visitor reads its own environment's generation
    report. Generating and changing them stay environment_management's."""
    from provisa.api.admin.capabilities import has_capability_request
    from provisa.api.admin.environments_router import _admin_pool, _caller_user_id
    from provisa.core.env_authority import owns_environment
    from provisa.core.request_context import active_env

    user_id = _caller_user_id(request)
    if (
        user_id is not None
        and has_capability_request(request, "environment_switch")
        and await owns_environment(
            _admin_pool(), request.state.active_org_id, active_env(), user_id
        )
    ):
        return
    require_capability_request(request, _RIGHT)


def _refused(exc: Exception, dataset: str | None = None) -> ApiError:
    return ApiError(422, "synthetic.refused", str(exc), dataset=dataset)


def _jsonable(v: Any) -> Any:
    return v.isoformat() if hasattr(v, "isoformat") else v


async def _serves_table(conn: Any, request: Request, table_id: int) -> bool:
    """Whether the caller is served a column of ``table_id`` (REQ-2152)."""
    from provisa.api.admin.read_rights import served_columns_request

    return await served_columns_request(conn, request, table_id) != set()


@router.get("")
async def list_datasets(request: Request) -> list[dict]:
    await _require_read(request)
    from provisa.api.admin.read_rights import reader_for_request, relationship_readable
    from provisa.synthetic.datasets import list_datasets as _list

    # REQ-2152: each dataset with only the tables, and the fan-out conditions over the
    # relationships, its caller reads.
    reader = reader_for_request(request)
    async with _db().acquire() as conn:
        rows = await _list(conn)
        served = {
            t.table_id: await _serves_table(conn, request, t.table_id)
            for r in rows
            for t in r.tables
        }
        readable = {
            c.relationship: await relationship_readable(conn, reader, c.relationship)
            for r in rows
            for c in r.fanout_conditions
        }
    return [
        {
            "id": r.id,
            "seed": r.seed,
            "scale": r.scale,
            "status": r.status,
            "storeSchema": r.store_schema,
            "error": r.error,
            "generatedAt": _jsonable(r.generated_at),
            "tables": [
                {
                    "tableId": t.table_id,
                    "profileEnv": t.profile_env,
                    "runId": t.run_id,
                    "scale": t.scale,
                }
                for t in r.tables
                if served[t.table_id]
            ],
            "fanoutConditions": [
                {"relationship": c.relationship, "condition": c.condition, "count": c.count}
                for c in r.fanout_conditions
                if readable[c.relationship]
            ],
            "assertions": list(r.assertions),
            "privateEpsilon": r.private_epsilon,
            "closenessThreshold": r.closeness_threshold,
            "closenessDraws": r.closeness_draws,
        }
        for r in rows
    ]


@router.get("/-/profile-runs")
async def profile_runs(request: Request, env: str) -> list[dict]:
    """``[{tableId, tableName, runs: [{runId, runTime, rowCount}]}]`` for each table of this
    environment that environment ``env`` has a succeeded profile run of, newest run first."""
    await _require_read(request)
    from provisa.api.admin.db_queries import fetch_tables
    from provisa.synthetic.run import profile_runs_in

    async with _db().acquire() as conn:
        # REQ-2152: only the tables the caller is served a column of.
        tables = [
            t for t in await fetch_tables(conn) if await _serves_table(conn, request, t["id"])
        ]
        return await profile_runs_in(conn, env, tables)


@router.get("/-/relationships")
async def relationships(request: Request) -> list[dict]:
    """``[{id, parentTableId, parentTable, childTableId, childTable}]``: the relationships whose
    child's rows a dataset may generate by, which a conditional fan-out names (REQ-1939)."""
    await _require_read(request)
    from provisa.api.admin.db_queries import fetch_relationships, fetch_tables
    from provisa.api.admin.read_rights import reader_for_request, relationship_readable
    from provisa.synthetic.plan import edges_of

    # REQ-2152: only the relationships whose joined columns the caller is served.
    reader = reader_for_request(request)
    async with _db().acquire() as conn:
        names = {t["id"]: t["table_name"] for t in await fetch_tables(conn)}
        edges = [
            e
            for e in edges_of(await fetch_relationships(conn))
            if e.relationship is not None
            and await relationship_readable(conn, reader, e.relationship)
        ]
    return [
        {
            "id": e.relationship,
            "parentTableId": e.parent_id,
            "parentTable": names[e.parent_id],
            "childTableId": e.child_id,
            "childTable": names[e.child_id],
        }
        for e in edges
        if e.relationship is not None
    ]


@router.put("/{dataset_id}")
async def define_dataset(request: Request, dataset_id: str, body: DatasetIn) -> dict:
    require_capability_request(request, _RIGHT)
    from provisa.synthetic.datasets import DatasetTableRow, FanoutCondition, check_name, define
    from provisa.synthetic.plan import DatasetRefused
    from provisa.synthetic.run import check_closure_of, check_conditions_of, store_schema

    try:
        check_name(dataset_id)
        schema = store_schema(dataset_id)
        tables = [DatasetTableRow(t.tableId, t.profileEnv, t.runId, t.scale) for t in body.tables]
        conditions = [
            FanoutCondition(c.relationship, c.condition, c.count) for c in body.fanoutConditions
        ]
        async with _db().acquire() as conn:
            # REQ-2152: a dataset generates only tables the caller is served; any other is
            # answered as one that is not registered.
            for t in tables:
                if not await _serves_table(conn, request, t.table_id):
                    raise ApiError(
                        404,
                        "synthetic.table_not_found",
                        f"table {t.table_id} is not registered",
                        table=t.table_id,
                    )
            # REQ-2152: a missing parent the caller is not served is not named.
            from provisa.api.admin.read_rights import reader_for_request

            reader = reader_for_request(request)
            await check_closure_of(
                conn,
                [t.table_id for t in tables],
                told=lambda table_id: reader.column(table_id, None),
            )
            await check_conditions_of(conn, [t.table_id for t in tables], conditions)
            await define(
                conn,
                dataset_id=dataset_id,
                seed=body.seed,
                scale=body.scale,
                store_schema=schema,
                tables=tables,
                fanout_conditions=conditions,
                assertions=body.assertions,
                private_epsilon=body.privateEpsilon,
                closeness_threshold=body.closenessThreshold,
                closeness_draws=body.closenessDraws,
            )
    except (ValueError, DatasetRefused) as exc:
        raise _refused(exc, dataset_id) from exc
    return {"id": dataset_id, "storeSchema": schema}


@router.post("/{dataset_id}/generate")
async def generate_dataset(request: Request, dataset_id: str) -> dict:
    require_capability_request(request, _RIGHT)
    from provisa.synthetic.plan import DatasetRefused
    from provisa.synthetic.run import generate

    try:
        await generate(state, dataset_id)
    except (LookupError, DatasetRefused) as exc:
        raise _refused(exc, dataset_id) from exc
    return {"id": dataset_id, "status": "generated"}


def _report_row(r: Any) -> dict:
    return {
        "table": r._mapping["table_name"],
        "column": r._mapping["column_name"],
        "measure": r._mapping["measure"],
        "source": r._mapping["source_value"],
        "synthetic": r._mapping["synthetic_value"],
        "delta": r._mapping["delta"],
        "note": r._mapping["note"],
    }


@router.get("/{dataset_id}/report")
async def dataset_report(request: Request, dataset_id: str) -> list[dict]:
    await _require_read(request)
    from provisa.core.schema_org import synthetic_datasets as sd
    from provisa.core.schema_org import synthetic_report as sr

    async with _db().acquire() as conn:
        failed = (
            await conn.execute_core(select(sd.c.report_error).where(sd.c.id == dataset_id))
        ).fetchone()
        if failed is not None and failed[0] is not None:
            # REQ-1942: the rows were generated and are served; their report failed.
            raise ApiError(
                409,
                "synthetic.report_failed",
                f"The report on synthetic dataset {dataset_id!r} failed: {failed[0]}",
                dataset=dataset_id,
            )
        result = await conn.execute_core(
            select(sr).where(sr.c.dataset_id == dataset_id).order_by(sr.c.id)
        )
        # REQ-2152: only the rows on columns the caller is served, by the dataset's own tables;
        # a report names a column as its profile publishes it, a table by its registered name.
        import types

        from provisa.api.admin.catalog_scope import catalog_scope
        from provisa.api.admin.profiler_checks_router import _physical_names
        from provisa.api.admin.read_rights import served_columns_of
        from provisa.core.schema_org import registered_tables as rt
        from provisa.core.schema_org import synthetic_dataset_tables as sdt

        report = result.fetchall()
        scope = catalog_scope(types.SimpleNamespace(context={"request": request}))
        if scope.whole:
            return [_report_row(r) for r in report]
        names = (
            await conn.execute_core(
                select(rt.c.id, rt.c.table_name)
                .join(sdt, sdt.c.table_id == rt.c.id)
                .where(sdt.c.dataset_id == dataset_id)
            )
        ).fetchall()
        readable: dict[str, set[str]] = {}
        for tid, name in names:
            served = await served_columns_of(conn, scope, tid)
            readable[name] = {
                published
                for published, physical in _physical_names(tid).items()
                if served is None or physical in served
            }
        # A row of the whole dataset (table "": its privacy budget, its closeness, an assertion,
        # whose text names its tables) is told to a caller served a column of each of them.
        whole_dataset = bool(readable) and all(readable.values())
        return [
            _report_row(r)
            for r in report
            if (r._mapping["table_name"] == "" and whole_dataset)
            or (
                r._mapping["table_name"] in readable
                and readable[r._mapping["table_name"]]
                and (
                    r._mapping["column_name"] is None
                    or r._mapping["column_name"] in readable[r._mapping["table_name"]]
                )
            )
        ]


@router.delete("/{dataset_id}")
async def drop_dataset(request: Request, dataset_id: str) -> dict:
    require_capability_request(request, _RIGHT)
    from provisa.synthetic.run import drop

    try:
        await drop(state, dataset_id)
    except LookupError as exc:
        raise ApiError(404, "synthetic.not_found", str(exc), dataset=dataset_id) from exc
    return {"id": dataset_id, "dropped": True}
