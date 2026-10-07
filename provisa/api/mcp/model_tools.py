# Copyright (c) 2026 Kenneth Stott
# Canary: 9d1f6a3c-2e7b-4c58-b0a4-71e3c5d8f926
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Data Profiler, fakes, synthetic datasets and the config export as agent tools.

Each tool calls the admin route the UI calls (``profiler_router``, ``profiler_checks_router``,
``fakes_router``, ``synthetic_router``, ``orgs_router``) with the real request the chat turn or MCP
call carries, so the route's own guard decides: a caller without the right is refused by the route,
in the route's words. A change to a table (a column's fake, the profiler it belongs to) is saved
through the ``updateTable`` mutation the table editor calls (:mod:`provisa.api.mcp.table_edit`).

Every function takes ``(state, role, request, ...)``; ``role`` is the caller's pinned role, checked
first by :func:`provisa.api.mcp.tools.require_role`.
"""

# Requirements: REQ-1934, REQ-1494, REQ-1939, REQ-1919, REQ-1304

from __future__ import annotations

import json
from typing import Any

from provisa.api.mcp import table_edit
from provisa.api.mcp.tools import require_role

# The kinds of a profile run get_table_profile returns when the caller names none: the run, each
# column's measures, its frequent values, what it plausibly holds, the proposed constraints and the
# drift against earlier runs. The sketches (quantiles, histogram, fits) are asked for by name.
SUMMARY_KINDS: tuple[str, ...] = (
    "runs",
    "columns",
    "top_values",
    "plausible_type",
    "constraints",
    "constraint_checks",
    "drift",
)


# -- tables ------------------------------------------------------------------------------------


def _require_table_editor(request: Any) -> None:
    """The table editor's right (table_registration), asked before a table is read for a tool that
    reads or saves it; the save through ``updateTable`` asks it again."""
    from provisa.api.admin.capabilities import require_capability_request

    require_capability_request(request, "table_registration")


async def find_table_id(state: Any, role: str, request: Any, domain: str, table: str) -> list[dict]:
    """The registered tables of ``domain`` named or aliased ``table``, with their ids."""
    require_role(role, state)
    from sqlalchemy import or_, select

    from provisa.api.admin.capabilities import require_capability_request
    from provisa.api.admin.schema_helpers import _get_pool
    from provisa.core.schema_org import registered_tables as rt

    require_capability_request(request, "table_registration")  # the table editor's right
    pool = await _get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            select(
                rt.c.id,
                rt.c.source_id,
                rt.c.domain_id,
                rt.c.table_name,
                rt.c.alias,
                rt.c.profiler_source_id,
            )
            .where(rt.c.domain_id == domain, or_(rt.c.table_name == table, rt.c.alias == table))
            .order_by(rt.c.id)
        )
        rows = result.fetchall()
    return [
        {
            "id": r.id,
            "sourceId": r.source_id,
            "domain": r.domain_id,
            "table": r.table_name,
            "alias": r.alias,
            "profilerSourceId": r.profiler_source_id,
        }
        for r in rows
    ]


# -- profiler ----------------------------------------------------------------------------------


async def list_profilers(state: Any, role: str, request: Any) -> list[dict]:
    require_role(role, state)
    from provisa.api.admin.profiler_router import list_profilers as route

    return await route(request)


async def run_profiler(state: Any, role: str, request: Any, source_id: str) -> list[dict]:
    require_role(role, state)
    from provisa.api.admin.profiler_router import run_profiler as route

    return await route(request, source_id)


async def run_table_profile(state: Any, role: str, request: Any, table_id: int) -> dict:
    require_role(role, state)
    from provisa.api.admin.profiler_router import run_profile_now

    return await run_profile_now(request, table_id)


async def declare_table_profile(
    state: Any, role: str, request: Any, table_id: int, profile: dict
) -> dict:
    require_role(role, state)
    from provisa.api.admin.profiler_router import DeclaredProfileBody, declare_profile

    return await declare_profile(request, table_id, DeclaredProfileBody(profile=profile))


async def get_profile_run_as_declared(
    state: Any, role: str, request: Any, table_id: int, run_id: str
) -> dict:
    require_role(role, state)
    from provisa.api.admin.profiler_router import run_as_declared

    return await run_as_declared(request, table_id, run_id, x_provisa_role=role)


async def list_profile_runs(state: Any, role: str, request: Any, table_id: int) -> list[dict]:
    require_role(role, state)
    from provisa.api.admin.profiler_router import list_profile_runs as route

    return await route(request, table_id)


async def _latest_succeeded_run(request: Any, table_id: int) -> str:
    from provisa.api.admin.profiler_router import list_profile_runs as route

    runs = await route(request, table_id)  # newest first
    from provisa.profiler.declared import DECLARED

    # REQ-1942: the latest MEASURED run; a declared profile measured nothing.
    latest = next(
        (r for r in runs if r["status"] == "succeeded" and r["sample_method"] != DECLARED), None
    )
    if latest is None:
        raise ValueError(
            f"table {table_id} has no succeeded profile run; run run_table_profile first"
        )
    return latest["run_id"]


async def _safe_run(request: Any, role: str, table_id: int, run_id: str) -> dict:
    from provisa.api.admin.profiler_router import get_profile_run

    return await get_profile_run(request, table_id, run_id, x_provisa_role=role)


async def get_table_profile(
    state: Any,
    role: str,
    request: Any,
    table_id: int,
    run_id: str | None = None,
    kinds: list[str] | None = None,
) -> dict:
    """One run's results as the viewer may see them; the latest succeeded run when ``run_id`` is
    not named, and the :data:`SUMMARY_KINDS` when ``kinds`` is not."""
    require_role(role, state)
    from provisa.profiler.schema import RESULT_KINDS

    wanted = tuple(kinds) if kinds else SUMMARY_KINDS
    unknown = [k for k in wanted if k not in RESULT_KINDS]
    if unknown:
        raise ValueError(f"unknown profile result kinds {unknown}; expected some of {RESULT_KINDS}")
    chosen = run_id or await _latest_succeeded_run(request, table_id)
    run = await _safe_run(request, role, table_id, chosen)
    return {"runId": chosen, **{k: run[k] for k in wanted}}


async def set_table_profiler(
    state: Any, role: str, request: Any, table_id: int, profiler_source_id: str | None
) -> dict:
    """Join the table to a profiler, or leave the one it is in when ``profiler_source_id`` is None."""
    require_role(role, state)
    _require_table_editor(request)
    table = await table_edit.read_table(table_id)
    edited = table_edit.table_input(table)
    edited.profiler_source_id = profiler_source_id or None
    return await table_edit.save_table(request, edited)


def _proposal_doc(row: dict) -> dict:
    return {
        "constraint": row["constraint"],
        "column": row["column_name"],
        "otherColumn": row["other_column"],
        "definition": json.loads(row["definition"]),
        "evidence": row["evidence"],
        "share": row["share"],
        "sampled": row["sampled"],
        "status": row["status"],
    }


async def list_profile_constraints(state: Any, role: str, request: Any, table_id: int) -> dict:
    """The constraints the latest succeeded run proposes, the decisions taken on them, and the
    checker tables an accepted one can be exported to."""
    require_role(role, state)
    from provisa.api.admin.profiler_checks_router import list_constraints

    decided = await list_constraints(request, table_id)
    run_id = await _latest_succeeded_run(request, table_id)
    run = await _safe_run(request, role, table_id, run_id)
    return {
        "runId": run_id,
        "proposals": [_proposal_doc(r) for r in run["constraints"]],
        "decisions": decided["decisions"],
        "checkers": decided["checkers"],
    }


async def decide_profile_constraint(
    state: Any,
    role: str,
    request: Any,
    table_id: int,
    kind: str,
    column: str,
    status: str,
    other_column: str | None = None,
    definition: dict | None = None,
) -> dict:
    """Accept or dismiss one constraint the latest succeeded run proposes, with its evidence, share
    and sample as the run recorded them; ``definition`` replaces the proposed one (accept as
    edited)."""
    require_role(role, state)
    from provisa.api.admin.profiler_checks_router import DecisionInput, decide_constraint

    if status not in ("accepted", "dismissed"):
        raise ValueError(f"status must be 'accepted' or 'dismissed', not {status!r}")
    run_id = await _latest_succeeded_run(request, table_id)
    run = await _safe_run(request, role, table_id, run_id)
    found = next(
        (
            r
            for r in run["constraints"]
            if r["constraint"] == kind
            and r["column_name"] == column
            and (r["other_column"] or None) == (other_column or None)
        ),
        None,
    )
    if found is None:
        raise ValueError(
            f"run {run_id} proposes no {kind} constraint on {column!r}"
            + (f" and {other_column!r}" if other_column else "")
            + "; list_profile_constraints shows what it proposes"
        )
    body = DecisionInput(
        kind=kind,
        column=column,
        otherColumn=other_column or None,
        definition=definition if definition is not None else json.loads(found["definition"]),
        evidence=found["evidence"],
        share=found["share"],
        sampled=bool(found["sampled"]),
        status=status,
        runId=run_id,
    )
    return await decide_constraint(request, table_id, body)


async def forget_profile_constraint(
    state: Any, role: str, request: Any, table_id: int, constraint_id: str
) -> dict:
    require_role(role, state)
    from provisa.api.admin.profiler_checks_router import forget_constraint

    return await forget_constraint(request, table_id, constraint_id)


async def export_profile_constraint(
    state: Any,
    role: str,
    request: Any,
    table_id: int,
    constraint_id: str,
    checker_table_id: int | None = None,
) -> dict:
    require_role(role, state)
    from provisa.api.admin.profiler_checks_router import ExportInput, export_constraint

    return await export_constraint(
        request, table_id, constraint_id, ExportInput(checkerTableId=checker_table_id)
    )


async def list_profile_checks(state: Any, role: str, request: Any, table_id: int) -> dict:
    require_role(role, state)
    from provisa.api.admin.profiler_checks_router import check_candidates

    return await check_candidates(request, table_id)


async def create_drift_check(
    state: Any, role: str, request: Any, table_id: int, checker_table_id: int | None = None
) -> dict:
    require_role(role, state)
    from provisa.api.admin.profiler_checks_router import ExportInput
    from provisa.api.admin.profiler_checks_router import create_drift_check as route

    return await route(request, table_id, ExportInput(checkerTableId=checker_table_id))


async def create_expectation_check(
    state: Any,
    role: str,
    request: Any,
    table_id: int,
    expectations_table_id: int,
    checker_table_id: int | None = None,
) -> dict:
    require_role(role, state)
    from provisa.api.admin.profiler_checks_router import ExpectationInput
    from provisa.api.admin.profiler_checks_router import create_expectation_check as route

    body = ExpectationInput(
        expectationsTableId=expectations_table_id, checkerTableId=checker_table_id
    )
    return await route(request, table_id, body)


# -- fakes -------------------------------------------------------------------------------------


async def list_fake_kinds(state: Any, role: str, request: Any) -> dict:
    """The kinds of fake and the fake methods, by name and argument names. Argument defaults are
    left out: the editor's catalog is the place to read them."""
    require_role(role, state)
    from provisa.api.admin.fakes_router import fake_catalog

    full = await fake_catalog(request)
    return {
        "kinds": [
            {
                "name": k["name"],
                "category": k["category"],
                "args": [a["name"] for a in k["args"]],
                "syntheticRuleOnly": k["ruleOnly"],
            }
            for k in full["kinds"]
        ],
        "methods": [
            {"name": m["name"], "category": m["category"], "args": [p["name"] for p in m["params"]]}
            for m in full["methods"]
        ],
    }


async def get_table_fakes(state: Any, role: str, request: Any, table_id: int) -> dict:
    """Each column's fake, whether it is stable, its synthetic rule and whether it is tagged pii."""
    require_role(role, state)
    from provisa.api.admin.capabilities import require_capability_request

    require_capability_request(request, "table_registration")  # the fakes routes' right
    table = await table_edit.read_table(table_id)
    return {
        "tableId": table.id,
        "table": table.table_name,
        "columns": [
            {
                "column": c.column_name,
                "dataType": c.data_type,
                "fake": c.fake,
                "fakeStable": c.fake_stable,
                "syntheticRule": c.synthetic_rule,
                "isPii": c.is_pii,
            }
            for c in table.columns
        ],
    }


async def set_column_fake(
    state: Any,
    role: str,
    request: Any,
    table_id: int,
    column: str,
    fake: str | None = None,
    synthetic_rule: str | None = None,
    stable: bool | None = None,
) -> dict:
    """Set or clear a column's fake, synthetic rule and stability. None (or left out) keeps the
    current value; an empty string clears a fake or a synthetic rule. Checked by the editor's own
    check before the save."""
    require_role(role, state)
    from provisa.api.admin._hiding_guard import require_hiding_editor_request

    # REQ-1944: a fake is a hiding field -- a steward sets it too; the save checks the domain.
    require_hiding_editor_request(request)
    from provisa.api.admin.fakes_router import ColumnFakeIn, check_column_fake

    table = await table_edit.read_table(table_id)
    edited = table_edit.table_input(table)
    target = next((c for c in edited.columns if c.name == column), None)
    if target is None:
        raise ValueError(f"table {table.table_name!r} has no column {column!r}")
    if fake is not None:
        target.fake = fake.strip() or None
    if synthetic_rule is not None:
        target.synthetic_rule = synthetic_rule.strip() or None
    if stable is not None:
        target.fake_stable = stable
    current = next(c for c in table.columns if c.column_name == column)
    await check_column_fake(
        request,
        ColumnFakeIn(
            tableId=table_id,
            tableName=table.table_name,
            column=column,
            # The route takes a string; a column with no registered type is sent as "", exactly
            # as the editor's dialog sends it (ColumnFakeDialog.tsx), so both are checked alike.
            dataType=current.data_type or "",
            fake=target.fake,
            syntheticRule=target.synthetic_rule,
            stable=target.fake_stable,
        ),
    )
    saved = await table_edit.save_table(request, edited)
    return {
        "column": column,
        "fake": target.fake,
        "syntheticRule": target.synthetic_rule,
        "fakeStable": target.fake_stable,
        **saved,
    }


async def propose_fakes(state: Any, role: str, request: Any, table_id: int) -> dict:
    """Fill from profile: fakes and synthetic rules proposed from the latest succeeded run. Nothing
    is saved."""
    require_role(role, state)
    from provisa.api.admin.fakes_router import ProposeIn
    from provisa.api.admin.fakes_router import propose_fakes as route

    return await route(request, ProposeIn(tableId=table_id))


# -- synthetic datasets ------------------------------------------------------------------------


async def list_synthetic_datasets(state: Any, role: str, request: Any) -> list[dict]:
    require_role(role, state)
    from provisa.api.admin.synthetic_router import list_datasets

    return await list_datasets(request)


async def list_synthetic_profile_runs(state: Any, role: str, request: Any, env: str) -> list[dict]:
    require_role(role, state)
    from provisa.api.admin.synthetic_router import profile_runs

    return await profile_runs(request, env)


async def define_synthetic_dataset(
    state: Any,
    role: str,
    request: Any,
    dataset_id: str,
    seed: int,
    scale: float,
    tables: list[dict],
    fanoutConditions: list[dict] | None = None,  # noqa: N803 -- the tool's own argument names
    assertions: list[str] | None = None,
    privateEpsilon: float | None = None,  # noqa: N803
    closenessThreshold: float | None = None,  # noqa: N803
    closenessDraws: int | None = None,  # noqa: N803
) -> dict:
    require_role(role, state)
    from provisa.api.admin.synthetic_router import DatasetIn, define_dataset

    given = {
        "fanoutConditions": fanoutConditions,
        "assertions": assertions,
        "privateEpsilon": privateEpsilon,
        "closenessThreshold": closenessThreshold,
        "closenessDraws": closenessDraws,
    }
    body = DatasetIn(
        seed=seed,
        scale=scale,
        tables=tables,  # pyright: ignore[reportArgumentType]
        **{
            k: v for k, v in given.items() if v is not None
        },  # an argument left out takes DatasetIn's
    )
    return await define_dataset(request, dataset_id, body)


async def generate_synthetic_dataset(state: Any, role: str, request: Any, dataset_id: str) -> dict:
    require_role(role, state)
    from provisa.api.admin.synthetic_router import generate_dataset

    return await generate_dataset(request, dataset_id)


async def get_synthetic_report(state: Any, role: str, request: Any, dataset_id: str) -> list[dict]:
    require_role(role, state)
    from provisa.api.admin.synthetic_router import dataset_report

    return await dataset_report(request, dataset_id)


async def drop_synthetic_dataset(state: Any, role: str, request: Any, dataset_id: str) -> dict:
    require_role(role, state)
    from provisa.api.admin.synthetic_router import drop_dataset

    return await drop_dataset(request, dataset_id)


# -- environment data choices (REQ-1942) -------------------------------------------------------


async def get_environment_detail(state: Any, role: str, request: Any, env: str) -> dict:
    require_role(role, state)
    from provisa.api.admin._guards import require_active_org_id
    from provisa.api.admin.capabilities import require_capability_request
    from provisa.api.admin.environment_data_router import environment_detail

    require_capability_request(request, "environment_management")
    return await environment_detail(request, require_active_org_id(request), env)


async def set_environment_data(
    state: Any,
    role: str,
    request: Any,
    env: str,
    dataMode: str | None = None,  # noqa: N803 -- the tool's own argument names
    mutationHandling: str | None = None,  # noqa: N803
    confirmDiscard: bool = False,  # noqa: N803
) -> dict:
    require_role(role, state)
    from provisa.api.admin._guards import require_active_org_id
    from provisa.api.admin.capabilities import require_capability_request
    from provisa.api.admin.environment_data_router import DataChoicesBody, edit_data_choices

    require_capability_request(request, "environment_data")
    body = DataChoicesBody(
        data_mode=dataMode,  # pyright: ignore[reportArgumentType]
        mutation_handling=mutationHandling,  # pyright: ignore[reportArgumentType]
        confirm_discard=confirmDiscard,
    )
    return await edit_data_choices(request, require_active_org_id(request), env, body)


async def set_source_binding(
    state: Any,
    role: str,
    request: Any,
    env: str,
    sourceId: str,  # noqa: N803 -- the tool's own argument names
    binding: str,
    connection: dict | None = None,
) -> dict:
    require_role(role, state)
    from provisa.api.admin._guards import require_active_org_id
    from provisa.api.admin.capabilities import require_capability_request
    from provisa.api.admin.environment_data_router import BindingBody
    from provisa.api.admin.environment_data_router import set_source_binding as _set

    require_capability_request(request, "environment_data")
    body = BindingBody(binding=binding, **(connection or {}))  # pyright: ignore[reportArgumentType]
    return await _set(request, require_active_org_id(request), env, sourceId, body)


async def recopy_environment_sources(
    state: Any, role: str, request: Any, env: str, sources: list[str] | None = None
) -> dict:
    require_role(role, state)
    from provisa.api.admin._guards import require_active_org_id
    from provisa.api.admin.capabilities import require_capability_request
    from provisa.api.admin.environment_data_router import RecopyBody, recopy_sources

    require_capability_request(request, "environment_data")
    return await recopy_sources(
        request, require_active_org_id(request), env, RecopyBody(sources=sources)
    )


async def get_environment_synthetic_plan(state: Any, role: str, request: Any, env: str) -> dict:
    require_role(role, state)
    from provisa.api.admin._guards import require_active_org_id
    from provisa.api.admin.capabilities import require_capability_request
    from provisa.api.admin.environment_data_router import synthetic_plan

    require_capability_request(request, "environment_management")
    return await synthetic_plan(request, require_active_org_id(request), env)


async def generate_environment_model(
    state: Any,
    role: str,
    request: Any,
    env: str,
    runs: dict | None = None,
    seed: int = 0,
    scale: float = 1.0,
    digest: str | None = None,
) -> dict:
    require_role(role, state)
    from provisa.api.admin._guards import require_active_org_id
    from provisa.api.admin.capabilities import require_capability_request
    from provisa.api.admin.environment_data_router import (
        ConfirmBody,
        GenerateBody,
        confirm_generation,
        generate_model,
    )

    require_capability_request(request, "environment_data")
    given = {"runs": {int(k): v for k, v in runs.items()}} if runs is not None else {}
    org_id = require_active_org_id(request)
    if digest is None:  # REQ-1942: the first step answers the warning and generates nothing
        return await generate_model(
            request, org_id, env, GenerateBody(seed=seed, scale=scale, **given)
        )
    body = ConfirmBody(seed=seed, scale=scale, digest=digest, **given)
    return await confirm_generation(request, org_id, env, body)


async def reset_environment_mutations(state: Any, role: str, request: Any, env: str) -> dict:
    require_role(role, state)
    from provisa.api.admin._guards import require_active_org_id
    from provisa.api.admin.capabilities import require_capability_request
    from provisa.api.admin.environment_data_router import reset_mutations

    require_capability_request(request, "environment_data")
    return await reset_mutations(request, require_active_org_id(request), env)


# -- config ------------------------------------------------------------------------------------


async def export_model_config(state: Any, role: str, request: Any) -> dict:
    """The acting org's model as a config file (the org page's config export)."""
    require_role(role, state)
    from provisa.api.admin._guards import require_active_org_id
    from provisa.api.admin.orgs_router import export_org_config

    org_id = require_active_org_id(request)
    response = await export_org_config(org_id, request)
    return {"filename": f"{org_id}-config.yaml", "yaml": bytes(response.body).decode("utf-8")}
