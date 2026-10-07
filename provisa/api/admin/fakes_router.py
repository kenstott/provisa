# Copyright (c) 2026 Kenneth Stott
# Canary: f6fdc334-5ff5-4b31-8153-f86944f488db
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin routes of the table editor's test-data mode (REQ-1494).

* ``GET  /admin/fakes/catalog`` -- every kind of fake and fake method a column may declare, by
  name and category, with its arguments.
* ``POST /admin/fakes/check`` -- whether a column's fake, synthetic rule and stable flag would be
  saved: the table's other columns and the model's relationships as stored, this column as given.
  A refusal is answered 422 with the save's own message.
* ``POST /admin/fakes/propose`` -- Fill from profile: a fake for each identifying column and a
  synthetic rule for the others, from the table's latest profile run in the selected environment,
  for every column that declares neither. Nothing is saved.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel

from provisa.api.admin.capabilities import require_capability_request
from provisa.api.app import state
from provisa.api.errors import ApiError

router = APIRouter(prefix="/admin/fakes", tags=["admin", "fakes"])

_RIGHT = "table_registration"


class ColumnFakeIn(BaseModel):
    tableId: int | None = None
    tableName: str
    column: str
    dataType: str
    fake: str | None = None
    syntheticRule: str | None = None
    stable: bool = False


@router.get("/catalog")
async def fake_catalog(request: Request) -> dict[str, Any]:
    from provisa.api.admin._hiding_guard import require_hiding_editor_request

    require_hiding_editor_request(request)  # REQ-1944: the kinds a steward chooses a fake from
    from provisa.fakes.catalog import catalog

    return catalog()


@router.post("/check")
async def check_column_fake(request: Request, body: ColumnFakeIn) -> dict[str, Any]:
    from provisa.api.admin._hiding_guard import require_hiding_editor_request

    require_hiding_editor_request(request)  # REQ-1944: a steward checks the fake it saves
    from provisa.api.admin._fake_guard import check_model
    from provisa.api.admin.db_queries import fetch_relationships, fetch_tables
    from provisa.fakes.kinds import FakeRefused

    if state.model_db is None:
        raise ApiError(503, "fakes.database_unavailable", "Database unavailable")
    column = {
        "column_name": body.column,
        "data_type": body.dataType,
        "fake": body.fake or None,
        "fake_stable": body.stable,
        "synthetic_rule": body.syntheticRule or None,
    }
    async with state.model_db.acquire() as conn:
        tables = await fetch_tables(conn)
        relationships = await fetch_relationships(conn)
    target = next((t for t in tables if t["id"] == body.tableId), None)
    if target is None:
        tables.append({"id": None, "table_name": body.tableName, "columns": [column]})
    else:
        target["columns"] = [
            column if c["column_name"] == body.column else c for c in target["columns"]
        ]
        if not any(c["column_name"] == body.column for c in target["columns"]):
            target["columns"].append(column)
    try:
        check_model(tables, relationships)
    except FakeRefused as exc:
        raise ApiError(422, "schema.fake_refused", str(exc), column=body.column) from exc
    return {"ok": True}


class ProposeIn(BaseModel):
    tableId: int


@router.post("/propose")
async def propose_fakes(request: Request, body: ProposeIn) -> dict[str, Any]:
    require_capability_request(request, _RIGHT)
    from provisa.api.admin.db_queries import fetch_tables
    from provisa.core.request_context import current_env, require_current_org
    from provisa.fakes.propose import latest_facts, propose
    from provisa.profiler.run import column_tags

    if state.model_db is None:
        raise ApiError(503, "fakes.database_unavailable", "Database unavailable")
    async with state.model_db.acquire() as conn:
        reg = next((t for t in await fetch_tables(conn) if t["id"] == body.tableId), None)
        if reg is None:
            raise ApiError(404, "fakes.table_not_found", f"table {body.tableId} is not registered")
        latest = await latest_facts(
            conn, org_id=require_current_org(), env=current_env.get(), reg=reg
        )
        tags = await column_tags(conn, body.tableId)
    if latest is None:
        raise ApiError(
            422,
            "fakes.no_profile_run",
            f"table {reg['table_name']!r} has no succeeded profile run in this environment; "
            "run its profiler first",
            table=reg["table_name"],
        )
    run_id, facts = latest
    declared = {
        c["column_name"] for c in reg["columns"] if c.get("fake") or c.get("synthetic_rule")
    }
    pii = {c for c, tagged in tags.items() if "pii" in tagged}
    out = propose(run_id, facts, pii, declared)
    return {"runId": out.run_id, "columns": out.columns, "unmatchedPii": out.unmatched_pii}
