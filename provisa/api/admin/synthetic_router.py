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


class DatasetIn(BaseModel):
    seed: int
    scale: float
    tables: list[TableIn]


def _db() -> Any:
    if state.model_db is None:
        raise ApiError(503, "synthetic.database_unavailable", "Database unavailable")
    return state.model_db


def _refused(exc: Exception, dataset: str | None = None) -> ApiError:
    return ApiError(422, "synthetic.refused", str(exc), dataset=dataset)


def _jsonable(v: Any) -> Any:
    return v.isoformat() if hasattr(v, "isoformat") else v


@router.get("")
async def list_datasets(request: Request) -> list[dict]:
    require_capability_request(request, _RIGHT)
    from provisa.synthetic.datasets import list_datasets as _list

    async with _db().acquire() as conn:
        rows = await _list(conn)
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
            ],
        }
        for r in rows
    ]


@router.get("/-/profile-runs")
async def profile_runs(request: Request, env: str) -> list[dict]:
    """``[{tableId, tableName, runs: [{runId, runTime, rowCount}]}]`` for each table of this
    environment that environment ``env`` has a succeeded profile run of, newest run first."""
    require_capability_request(request, _RIGHT)
    from provisa.api.admin.db_queries import fetch_tables
    from provisa.synthetic.run import profile_runs_in

    async with _db().acquire() as conn:
        tables = await fetch_tables(conn)
        return await profile_runs_in(conn, env, tables)


@router.put("/{dataset_id}")
async def define_dataset(request: Request, dataset_id: str, body: DatasetIn) -> dict:
    require_capability_request(request, _RIGHT)
    from provisa.synthetic.datasets import DatasetTableRow, check_name, define
    from provisa.synthetic.plan import DatasetRefused
    from provisa.synthetic.run import check_closure_of, store_schema

    try:
        check_name(dataset_id)
        schema = store_schema(dataset_id)
        tables = [DatasetTableRow(t.tableId, t.profileEnv, t.runId, t.scale) for t in body.tables]
        async with _db().acquire() as conn:
            await check_closure_of(conn, [t.table_id for t in tables])
            await define(
                conn,
                dataset_id=dataset_id,
                seed=body.seed,
                scale=body.scale,
                store_schema=schema,
                tables=tables,
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


@router.get("/{dataset_id}/report")
async def dataset_report(request: Request, dataset_id: str) -> list[dict]:
    require_capability_request(request, _RIGHT)
    from provisa.core.schema_org import synthetic_report as sr

    async with _db().acquire() as conn:
        result = await conn.execute_core(
            select(sr).where(sr.c.dataset_id == dataset_id).order_by(sr.c.id)
        )
        return [
            {
                "table": r._mapping["table_name"],
                "column": r._mapping["column_name"],
                "measure": r._mapping["measure"],
                "source": r._mapping["source_value"],
                "synthetic": r._mapping["synthetic_value"],
                "delta": r._mapping["delta"],
                "note": r._mapping["note"],
            }
            for r in result.fetchall()
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
