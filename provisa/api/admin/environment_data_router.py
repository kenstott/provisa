# Copyright (c) 2026 Kenneth Stott
# Canary: a1bc9d8c-f59d-4ab5-899f-3f4398ed08f4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An environment's data choices, over HTTP (REQ-1942).

The detail of an environment -- its parent, data mode, read-only or read-write, each source's
binding and its test data -- and the edit of its data choices: its data mode, one source's
binding, read-only or read-write. Every change needs the environment_data right; making an
environment show its parent's real rows also needs the right to read them there, and is audited
with what it makes visible and to how many members. Prod has a detail but no data mode.
"""

# Requirements: REQ-1942

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Request
from pydantic import BaseModel
from sqlalchemy import func, or_, select

from provisa.api.admin.environments_router import (
    DATA_CAPABILITY,
    MANAGE_CAPABILITY,
    _admin_pool,
    _audit,
    _confined,
    _known,
    _member,
    _pool,
    _reads_parent,
    _refresh,
    _state,
)
from provisa.api.errors import ApiError
from provisa.core import env_data, model_change
from provisa.core.env_classes import INHERITED, TEST_FAKE, TEST_SYNTHETIC
from provisa.core.env_store import set_data
from provisa.core.environments import PROD, org_schema

router = APIRouter(prefix="/admin/orgs/{org_id}/environments", tags=["admin"])


class DataChoicesBody(BaseModel):
    """An edit of an environment's data choices. A change of data mode that changes row keys --
    to or from test_synthetic -- discards the change log and needs ``confirm_discard``."""

    data_mode: Literal["inherit", "unbound", "test_fake", "test_synthetic"] | None = None
    mutation_handling: Literal["refused", "reversible", "direct"] | None = None
    confirm_discard: bool = False


class BindingBody(BaseModel):
    """One source's binding: through the parent's connection, none, or a connection of the
    environment's own -- given here, its password stored in the org's vault."""

    binding: Literal["inherited", "unbound", "own"]
    host: str | None = None
    port: int | None = None
    database: str | None = None
    username: str | None = None
    password: str | None = None
    path: str | None = None


def _refused(org_id: str, env: str, exc: env_data.DataChoiceRefused) -> ApiError:
    return ApiError(422, "environments.data_refused", str(exc), org=org_id, env=env)


async def _visible(org_id: str, env: str) -> dict[str, Any]:
    """What making ``env`` read its parent's real rows makes visible, and to how many members:
    its inherited sources, the tables reading them, and the members who may be served by it."""
    from provisa.core.schema_admin import user_org_memberships as m

    schema = org_schema(org_id, env)
    async with _pool().acquire() as conn:
        bindings = await env_data.source_bindings(conn, schema)
        inherited = [b["id"] for b in bindings if b["binding"] == INHERITED]
        rt = env_data._table("registered_tables", schema)
        tables = (
            await conn.execute_core(
                select(rt.c.table_name)
                .where(rt.c.source_id.in_(inherited))
                .order_by(rt.c.table_name)
            )
        ).fetchall()
    async with _admin_pool().acquire() as conn:
        row = (
            await conn.execute_core(
                select(func.count())
                .select_from(m)
                .where(m.c.org_id == org_id, or_(m.c.env_name.is_(None), m.c.env_name == env))
            )
        ).fetchone()
    assert row is not None  # COUNT over a table is a row
    return {
        "sources": inherited,
        "tables": [t[0] for t in tables],
        "members": int(row[0]),
    }


@router.get("/{name}/detail")
async def environment_detail(request: Request, org_id: str, name: str) -> dict:
    """The environment's detail (REQ-1942): its parent, data mode, read-only or read-write, each
    source's binding, and its test data -- the faked column count and the sensitive columns with no
    fake, or the synthetic dataset's status. Prod has no data mode: it is always real."""
    await _member(request, org_id, MANAGE_CAPABILITY)
    await _confined(request, org_id, name)
    row = await _known(org_id, name)
    schema = org_schema(org_id, name)
    async with _pool().acquire() as conn:
        bindings = await env_data.source_bindings(conn, schema)
        faked = await env_data.faked_column_count(conn, schema)
        uncovered = await env_data.uncovered_sensitive(conn, schema)
        kept = await kept_counts(conn, schema)
    return {
        "name": name,
        "parent": row["parent"],
        "data_mode": row["data_mode"],
        "mutation_handling": None if name == PROD else row["mutation_handling"],
        "sources": bindings,
        # REQ-1942: the mutations kept in its change log, by table id, as Reset mutations drops.
        "kept_mutations": kept,
        "test_data": {
            "faked_columns": faked,
            "sensitive_without_fake": uncovered,
            "synthetic": {
                "dataset": row["synthetic_dataset"],
                "status": row["data_status"],
                "error": row["data_error"],
            },
        },
    }


@router.patch("/{name}/data")
@model_change.commits_itself  # REQ-1524: writes the environment's model, which it commits itself
async def edit_data_choices(
    request: Request, org_id: str, name: str, body: DataChoicesBody
) -> dict:
    """Change the environment's data mode and whether it takes writes (REQ-1942). Inherit and
    Unbound set every source's binding; the Test modes leave each as it is. Test (fake) is refused
    while a sensitive column has no fake. A change to or from Test (synthetic) discards the change log
    and is refused without ``confirm_discard``."""
    await _confined(request, org_id, name)
    actor = await _member(request, org_id, DATA_CAPABILITY)
    row = await _known(org_id, name)
    if name == PROD:
        raise ApiError(
            409,
            "environments.prod_immutable",
            f"{PROD!r} has no data mode; it is always real.",
            org=org_id,
            env=name,
        )
    detail: dict[str, Any] = {}
    connectivity = False
    if body.data_mode is not None and body.data_mode != row["data_mode"]:
        if row["data_status"] == "generating":
            raise ApiError(
                409,
                "environments.generating",
                f"{name!r} is generating its model; change its data mode when it finishes.",
                org=org_id,
                env=name,
            )
        try:
            step = env_data.transition(row["data_mode"], body.data_mode)
        except env_data.DataChoiceRefused as exc:
            raise _refused(org_id, name, exc) from exc
        if step.discards_change_log and not body.confirm_discard:
            raise ApiError(
                409,
                "environments.confirm_discard",
                f"Changing {name!r} from {row['data_mode']} to {body.data_mode} changes its row "
                "keys and discards its change log. Confirm to go ahead.",
                org=org_id,
                env=name,
            )
        if step.reads_parent:
            await _reads_parent(request, org_id, row["parent"])
        schema = org_schema(org_id, name)
        async with _pool().acquire() as conn, conn.transaction():
            try:
                if body.data_mode == TEST_FAKE:
                    await env_data.refuse_uncovered_sensitive(conn, schema, name)
                if step.binding is not None:
                    await env_data.set_bindings(conn, schema, step.binding, None)
                    connectivity = True
            except env_data.DataChoiceRefused as exc:
                raise _refused(org_id, name, exc) from exc
        if row["data_mode"] == TEST_SYNTHETIC and row["synthetic_dataset"] is not None:
            # Its tables read their sources again: the generated model goes with the mode.
            await _drop_model(org_id, name, row["synthetic_dataset"])
            await set_data(
                _admin_pool(),
                org_id,
                name,
                synthetic_dataset=None,
                data_status=None,
                data_error=None,
            )
            detail["synthetic_dropped"] = row["synthetic_dataset"]
        await set_data(_admin_pool(), org_id, name, data_mode=body.data_mode)
        connectivity = True  # the runtime reads its data mode when it is built
        detail["data_mode"] = {"from": row["data_mode"], "to": body.data_mode}
        detail["change_log_discarded"] = (
            await _discard_kept(org_id, name) if step.discards_change_log else []
        )
        if step.reads_parent:
            detail["made_visible"] = await _visible(org_id, name)
    if body.mutation_handling is not None and body.mutation_handling != row["mutation_handling"]:
        await set_data(_admin_pool(), org_id, name, mutation_handling=body.mutation_handling)
        connectivity = True  # the runtime reads its mutation handling when it is built
        detail["mutation_handling"] = {
            "from": row["mutation_handling"],
            "to": body.mutation_handling,
        }
    refreshed = None
    if detail:
        await _audit(org_id, actor, "environment.data", name, detail)
        refreshed = await _refresh(org_id, name, connectivity=connectivity)
    return {"environment": await _known(org_id, name), "change": detail, "refreshed": refreshed}


@router.put("/{name}/sources/{source_id}/binding")
@model_change.commits_itself  # REQ-1524: writes the environment's model, which it commits itself
async def set_source_binding(
    request: Request, org_id: str, name: str, source_id: str, body: BindingBody
) -> dict:
    """Set one source's binding: through the parent's connection, or none (REQ-1942). A source
    becomes the environment's own by being given a connection. Inheriting shows the parent's
    real rows, so it needs the right to read them there, and is audited."""
    await _confined(request, org_id, name)
    actor = await _member(request, org_id, DATA_CAPABILITY)
    row = await _known(org_id, name)
    if name == PROD:
        raise ApiError(
            409,
            "environments.prod_immutable",
            f"{PROD!r} inherits from nothing: its sources are its own.",
            org=org_id,
            env=name,
        )
    if body.binding == INHERITED:
        await _reads_parent(request, org_id, row["parent"])
    connection = {
        k: v
        for k, v in body.model_dump(
            include={"host", "port", "database", "username", "path"}
        ).items()
        if v is not None
    }
    if body.binding == "own":
        if not connection:
            raise ApiError(
                422,
                "environments.data_refused",
                f"Binding {source_id!r} to a connection of {name!r}'s own needs the connection: "
                "host, port, database and username, or a path.",
                org=org_id,
                env=name,
            )
        from provisa.api.admin.schema_common import store_source_password

        # The environment's own credential, under a name of its own: the parent's is untouched.
        connection["password_ref"] = await store_source_password(
            actor, f"{source_id}__env_{name}", body.password or ""
        )
    elif connection or body.password is not None:
        raise ApiError(
            422,
            "environments.data_refused",
            f"A connection is given only when binding {source_id!r} to one of {name!r}'s own.",
            org=org_id,
            env=name,
        )
    async with _pool().acquire() as conn, conn.transaction():
        try:
            if body.binding == "own":
                await env_data.bind_own(conn, org_schema(org_id, name), source_id, connection)
            else:
                await env_data.set_bindings(
                    conn, org_schema(org_id, name), body.binding, [source_id]
                )
        except env_data.DataChoiceRefused as exc:
            raise _refused(org_id, name, exc) from exc
    detail: dict[str, Any] = {"source": source_id, "binding": body.binding}
    if body.binding == "own":
        detail["connection"] = {k: v for k, v in connection.items() if k != "password_ref"}
    if body.binding == INHERITED:
        detail["made_visible"] = await _visible(org_id, name)
    await _audit(org_id, actor, "environment.binding", name, detail)
    refreshed = await _refresh(org_id, name, connectivity=True)
    return {"source": source_id, "binding": body.binding, "refreshed": refreshed}


class GenerateBody(BaseModel):
    """A whole-model generation: each generated table's profile run in the parent, by table id
    (a table left out takes its latest successful run), the seed and the scale."""

    runs: dict[int, str] = {}
    seed: int = 0
    scale: float = 1.0
    confirm_discard: bool = False


def _not_synthetic(org_id: str, name: str, mode: str | None) -> ApiError:
    return ApiError(
        409,
        "environments.not_synthetic",
        f"{name!r} is {mode or 'prod'}, not Test (synthetic): switch its data mode first.",
        org=org_id,
        env=name,
    )


@router.get("/{name}/synthetic/plan")
async def synthetic_plan(request: Request, org_id: str, name: str) -> dict:
    """What a whole-model generation in a Test (synthetic) environment would generate (REQ-1942):
    every table not backed by an API with its parent's successful profile runs, the latest
    preselected; the API-backed tables, with their keys, key rules, address and what a lookup
    returns; and whether every generated table has a run."""
    from provisa.synthetic.env_model import model_plan

    await _member(request, org_id, MANAGE_CAPABILITY)
    await _confined(request, org_id, name)
    row = await _known(org_id, name)
    if row["data_mode"] != TEST_SYNTHETIC:
        raise _not_synthetic(org_id, name, row["data_mode"])
    db = (await _env_runtime(org_id, name)).model_db
    assert db is not None, "an environment's runtime holds its model store"
    async with db.acquire() as conn:
        return await model_plan(_state(), conn, row["parent"])


@router.post("/{name}/synthetic")
async def generate_model(request: Request, org_id: str, name: str, body: GenerateBody) -> dict:
    """Generate a Test (synthetic) environment's whole model in the background (REQ-1942). Every
    table not backed by an API needs a profile run in the parent; regenerating discards the kept
    mutations and needs ``confirm_discard``."""
    from provisa.synthetic.env_model import DATASET_ID, model_plan, start

    await _confined(request, org_id, name)
    actor = await _member(request, org_id, DATA_CAPABILITY)
    row = await _known(org_id, name)
    if row["data_mode"] != TEST_SYNTHETIC:
        raise _not_synthetic(org_id, name, row["data_mode"])
    if row["data_status"] == "generating":
        raise ApiError(
            409,
            "environments.generating",
            f"{name!r} is already generating its model.",
            org=org_id,
            env=name,
        )
    if row["synthetic_dataset"] is not None and not body.confirm_discard:
        raise ApiError(
            409,
            "environments.confirm_discard",
            f"Regenerating {name!r} changes its row keys and discards its kept mutations. "
            "Confirm to go ahead.",
            org=org_id,
            env=name,
        )
    db = (await _env_runtime(org_id, name)).model_db
    assert db is not None, "an environment's runtime holds its model store"
    async with db.acquire() as conn:
        plan = await model_plan(_state(), conn, row["parent"])
    runs: dict[int, str] = {}
    missing = []
    for t in plan["tables"]:
        chosen = body.runs.get(t["tableId"], t["selected"])
        if chosen is None:
            missing.append(t["tableName"])
        elif chosen not in {r["runId"] for r in t["runs"]}:
            raise ApiError(
                422,
                "environments.unknown_run",
                f"{t['tableName']!r} has no successful profile run {chosen!r} in "
                f"{row['parent']!r}.",
                org=org_id,
                env=name,
            )
        else:
            runs[t["tableId"]] = chosen
    if missing:
        raise ApiError(
            422,
            "environments.unprofiled",
            f"These tables have no successful profile run in {row['parent']!r} to generate them "
            f"from: {', '.join(missing)}.",
            org=org_id,
            env=name,
        )
    discarded = await _discard_kept(org_id, name)  # regenerating changes the row keys
    await set_data(
        _admin_pool(),
        org_id,
        name,
        data_status="generating",
        data_error=None,
        synthetic_dataset=DATASET_ID,
    )
    await _audit(
        org_id,
        actor,
        "environment.generate",
        name,
        {"runs": runs, "seed": body.seed, "scale": body.scale, "change_log_discarded": discarded},
    )
    start(
        _state(),
        org_id=org_id,
        env=name,
        parent=row["parent"],
        runs=runs,
        seed=body.seed,
        scale=body.scale,
    )
    return {"status": "generating", "tables": len(runs), "apiTables": len(plan["apiTables"])}


async def _env_runtime(org_id: str, name: str) -> Any:
    from provisa.api.app import ensure_org_runtime

    return await ensure_org_runtime(org_id, name)


async def _drop_model(org_id: str, name: str, dataset_id: str) -> None:
    """Drop ``name``'s generated model, in its own runtime."""
    from provisa.core.request_context import (
        reset_current_env,
        reset_current_org,
        set_current_env,
        set_current_org,
    )
    from provisa.synthetic.run import drop

    await _env_runtime(org_id, name)
    org_token = set_current_org(org_id)
    env_token = set_current_env(name)
    try:
        await drop(_state(), dataset_id)
    finally:
        reset_current_env(env_token)
        reset_current_org(org_token)


async def kept_counts(conn: Any, schema: str) -> dict[int, int]:
    """How many versions each table's change log keeps, by table id."""
    import sqlalchemy as sa

    from provisa.core.env_changes import log_name, logged

    out: dict[int, int] = {}
    for table_id in sorted(await logged(conn, schema)):
        quoted = '"' + schema.replace('"', '""') + '"'
        row = (
            await conn.execute_core(
                sa.text(f'SELECT COUNT(*) FROM {quoted}."{log_name(table_id)}"')
            )
        ).fetchone()
        out[table_id] = int(row[0])
    return out


async def _discard_kept(org_id: str, name: str) -> list[int]:
    """Drop ``name``'s kept mutations; the tables whose mutations were dropped."""
    from provisa.core.env_changes import reset

    async with _pool().acquire() as conn:
        return await reset(conn, org_schema(org_id, name))


@router.post("/{name}/mutations/reset")
async def reset_mutations(request: Request, org_id: str, name: str) -> dict:
    """Reset mutations (REQ-1942): drop the environment's kept mutations, returning it to its
    baseline -- the parent's real rows, the generated rows, or a database of its own."""
    await _confined(request, org_id, name)
    actor = await _member(request, org_id, DATA_CAPABILITY)
    await _known(org_id, name)
    if name == PROD:
        raise ApiError(
            409,
            "environments.prod_immutable",
            f"{PROD!r} keeps no mutations: its mutations change its own data.",
            org=org_id,
            env=name,
        )
    discarded = await _discard_kept(org_id, name)
    await _audit(org_id, actor, "environment.reset_mutations", name, {"tables": discarded})
    refreshed = await _refresh(org_id, name, connectivity=True)
    return {"tables": discarded, "refreshed": refreshed}
