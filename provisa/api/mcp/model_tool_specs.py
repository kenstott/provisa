# Copyright (c) 2026 Kenneth Stott
# Canary: 5a8c2e71-0b3d-4f96-9c1e-e4b7a20d6f38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Polly's tool schemas for the Data Profiler, fakes, synthetic datasets and the config export,
and their dispatch to :mod:`provisa.api.mcp.model_tools`.

The schemas carry no ``role``: the endpoint pins it (see chat.py). Every one of these tools needs
the verified request, because the admin route it calls reads the caller's rights from it.
"""

# Requirements: REQ-1934, REQ-1494, REQ-1939, REQ-1919, REQ-1304

from __future__ import annotations

from typing import Any

from provisa.api.mcp import model_tools

_TABLE_ID = {"type": "integer", "description": "The registered table's numeric id."}
_CHECKER_ID = {
    "type": "integer",
    "description": (
        "The checker table to add the check to. Leave it out where exactly one checker table "
        "scans the target; with several, the call is refused naming them."
    ),
}
_RIGHT_TABLE = "Needs table_registration; a caller without it is refused 'Missing capability'."
_RIGHT_ENV = "Needs environment_management; a caller without it is refused 'Missing capability'."


def _obj(properties: dict, required: list[str] | None = None) -> dict:
    return {"type": "object", "properties": properties, "required": required or []}


SPECS: list[dict] = [
    {
        "name": "find_table_id",
        "description": (
            "The numeric id of a registered table, which every profiler, constraint, check and "
            "fake tool takes. `domain` is the table's domain (the schema list_schemas/"
            "describe_table show) and `table` its registered name or alias. Returns every match "
            "with its id, source, domain, name and the profiler it belongs to; empty when none "
            "matches. Read only. " + _RIGHT_TABLE
        ),
        "input_schema": _obj(
            {"domain": {"type": "string"}, "table": {"type": "string"}}, ["domain", "table"]
        ),
    },
    {
        "name": "list_profilers",
        "description": (
            "REQ-1934: list the Data Profiler sources: each one's id, cron schedule, sample budget "
            "(sampleAboveCells), lowCardinalityMax and member tables. Read only. " + _RIGHT_TABLE
        ),
        "input_schema": _obj({}),
    },
    {
        "name": "set_table_profiler",
        "description": (
            "REQ-1934: add a table to a profiler (profiler_source_id = a profiler id from "
            "list_profilers) or remove it from the one it is in (profiler_source_id = null). "
            "Saved through the table editor's own save, so its refusals come back verbatim: a "
            "table whose rows need a required filter cannot join a profiler, and an unknown "
            "profiler id is refused. Removing a table stops its scheduled profiling: confirm with "
            "present_choice (mode='yes_no') before removing. " + _RIGHT_TABLE
        ),
        "input_schema": _obj(
            {
                "table_id": _TABLE_ID,
                "profiler_source_id": {"type": ["string", "null"]},
            },
            ["table_id", "profiler_source_id"],
        ),
    },
    {
        "name": "run_profiler",
        "description": (
            "REQ-1934: run a profiler now over every member table. Refused 'not_a_profiler' when "
            "the id is not a profiler source. Needs source_registration; a caller without it is "
            "refused 'Missing capability'."
        ),
        "input_schema": _obj({"source_id": {"type": "string"}}, ["source_id"]),
    },
    {
        "name": "run_table_profile",
        "description": (
            "REQ-1934: profile one table now (Run Profile Now). Refused when the table has not "
            "joined a profiler ('has not joined a profiler') or the run fails (the reason is "
            "returned). Returns runId, rowCount and profiledRows. " + _RIGHT_TABLE
        ),
        "input_schema": _obj({"table_id": _TABLE_ID}, ["table_id"]),
    },
    {
        "name": "list_profile_runs",
        "description": (
            "REQ-1934: a table's profile run history, newest first (run_id, run_time, status, "
            "row_count, sampling, error). Read only. Refused when the table has not joined a "
            "profiler. " + _RIGHT_TABLE
        ),
        "input_schema": _obj({"table_id": _TABLE_ID}, ["table_id"]),
    },
    {
        "name": "get_table_profile",
        "description": (
            "REQ-1934: one profile run's results, as YOUR role may see them (masked columns stay "
            "masked). run_id defaults to the latest succeeded run; refused when there is none. "
            "kinds picks result kinds; left out it returns runs, columns, top_values, "
            "plausible_type, constraints, constraint_checks and drift. Other kinds: quantiles, "
            "histogram, fits, fit_quality, fanout, fanout_runs, shapes, duplicates, correlations, "
            "dependencies, joint_counts, repeats. Read only. " + _RIGHT_TABLE
        ),
        "input_schema": _obj(
            {
                "table_id": _TABLE_ID,
                "run_id": {"type": "string"},
                "kinds": {"type": "array", "items": {"type": "string"}},
            },
            ["table_id"],
        ),
    },
    {
        "name": "list_profile_constraints",
        "description": (
            "REQ-1934: the constraints the latest succeeded profile run proposes (not_null, "
            "unique, value_set, range, ordering; each with evidence, share and whether it rests "
            "on a sample), the decisions already taken (accepted/dismissed, with their ids) and "
            "the checker tables an accepted one can be exported to. Read only. " + _RIGHT_TABLE
        ),
        "input_schema": _obj({"table_id": _TABLE_ID}, ["table_id"]),
    },
    {
        "name": "decide_profile_constraint",
        "description": (
            "REQ-1934: accept or dismiss ONE constraint the latest succeeded run proposes, named "
            "by kind + column (+ other_column for an ordering) exactly as list_profile_constraints "
            "shows it. status is 'accepted' or 'dismissed'. To accept an edited form pass "
            "definition (the kind's parameters, e.g. {values:[...]} or {min,max}); left out, the "
            "proposed definition is accepted. Every later run checks accepted constraints. "
            "Refused when the run proposes no such constraint, or the definition is invalid. "
            + _RIGHT_TABLE
        ),
        "input_schema": _obj(
            {
                "table_id": _TABLE_ID,
                "kind": {"type": "string"},
                "column": {"type": "string"},
                "other_column": {"type": "string"},
                "status": {"type": "string", "enum": ["accepted", "dismissed"]},
                "definition": {"type": "object"},
            },
            ["table_id", "kind", "column", "status"],
        ),
    },
    {
        "name": "forget_profile_constraint",
        "description": (
            "REQ-1934: withdraw an accept/dismiss decision by its id (from "
            "list_profile_constraints); the constraint is proposed as new again. " + _RIGHT_TABLE
        ),
        "input_schema": _obj(
            {"table_id": _TABLE_ID, "constraint_id": {"type": "string"}},
            ["table_id", "constraint_id"],
        ),
    },
    {
        "name": "export_profile_constraint",
        "description": (
            "REQ-1934: add an ACCEPTED constraint to a data-quality checker table whose contract "
            "scans the table. Refused when the constraint is not accepted, or no checker (or more "
            "than one, with checker_table_id left out) scans the table. " + _RIGHT_TABLE
        ),
        "input_schema": _obj(
            {
                "table_id": _TABLE_ID,
                "constraint_id": {"type": "string"},
                "checker_table_id": _CHECKER_ID,
            },
            ["table_id", "constraint_id"],
        ),
    },
    {
        "name": "list_profile_checks",
        "description": (
            "REQ-1934: what a drift or expectation check for a table can use: whether its drift "
            "result table is registered, the checker tables scanning it, and the registered "
            "tables of the expectations shape. Read only. " + _RIGHT_TABLE
        ),
        "input_schema": _obj({"table_id": _TABLE_ID}, ["table_id"]),
    },
    {
        "name": "create_drift_check",
        "description": (
            "REQ-1934: add a drift check over the table's registered drift result table to a "
            "checker. Refused when the drift table is not registered on the profiler ('register "
            "it on the profiler first') or no single checker scans it. " + _RIGHT_TABLE
        ),
        "input_schema": _obj(
            {"table_id": _TABLE_ID, "checker_table_id": _CHECKER_ID}, ["table_id"]
        ),
    },
    {
        "name": "create_expectation_check",
        "description": (
            "REQ-1934: add an expectation check comparing the table's drift results with an "
            "expectations table (expectations_table_id, one of list_profile_checks' "
            "expectationTables). Refused when that table is not of the expectations shape (the "
            "missing columns are named), the drift table is not registered, or no single checker "
            "scans it. " + _RIGHT_TABLE
        ),
        "input_schema": _obj(
            {
                "table_id": _TABLE_ID,
                "expectations_table_id": {"type": "integer"},
                "checker_table_id": _CHECKER_ID,
            },
            ["table_id", "expectations_table_id"],
        ),
    },
    {
        "name": "list_fake_kinds",
        "description": (
            "REQ-1494: the kinds of fake (categories, bool, distributions, relative fakes, sql, "
            "...) and the fake methods (email, name, phone, ...) a column can declare, with their "
            "argument names. Read this before writing a fake. A declaration is one call, e.g. "
            "categories((shoes, bra), (.4, .6)), normal(mean=50, sd=10, min=0), "
            "after(created_at, 1 to 10 days), email(). Kinds marked syntheticRuleOnly may only be "
            "a synthetic rule. Read only. " + _RIGHT_TABLE
        ),
        "input_schema": _obj({}),
    },
    {
        "name": "get_table_fakes",
        "description": (
            "REQ-1494: each column of a table with its fake, whether the fake is stable, its "
            "synthetic rule and whether it is tagged pii. Read only. " + _RIGHT_TABLE
        ),
        "input_schema": _obj({"table_id": _TABLE_ID}, ["table_id"]),
    },
    {
        "name": "set_column_fake",
        "description": (
            "REQ-1494: set or clear one column's fake, synthetic rule (used only by synthetic "
            "generation, laid over the fake) and stable flag. A field left out keeps its value; "
            "'' (empty string) clears a fake or a synthetic rule. Checked exactly as the table editor checks it, then saved "
            "through the editor's save; a refusal comes back verbatim, e.g. an unknown kind or "
            "method, arguments the kind does not take, shares that do not sum to one, a relative "
            "fake naming a missing column, a cycle, a kind wrong for the column's type, or a "
            "method no column can hold. " + _RIGHT_TABLE
        ),
        "input_schema": _obj(
            {
                "table_id": _TABLE_ID,
                "column": {"type": "string"},
                "fake": {"type": "string"},
                "synthetic_rule": {"type": "string"},
                "stable": {"type": "boolean"},
            },
            ["table_id", "column"],
        ),
    },
    {
        "name": "propose_fakes",
        "description": (
            "REQ-1494: fill from profile — propose a fake for each identifying column and a "
            "synthetic rule for the rest, from the table's latest succeeded profile run. Saves "
            "NOTHING: show the proposals, ask the user which to save (present_choice), then save "
            "each chosen one with set_column_fake. Refused 'no succeeded profile run' when the "
            "table has not been profiled in this environment. unmatchedPii lists pii columns with "
            "no confident proposal. " + _RIGHT_TABLE
        ),
        "input_schema": _obj({"table_id": _TABLE_ID}, ["table_id"]),
    },
    {
        "name": "list_synthetic_datasets",
        "description": (
            "REQ-1939: the synthetic datasets defined in this environment: id, seed, scale, "
            "status, the store schema they generate into, and their tables (each from a profile "
            "run). Read only. " + _RIGHT_ENV
        ),
        "input_schema": _obj({}),
    },
    {
        "name": "list_synthetic_profile_runs",
        "description": (
            "REQ-1939: the tables environment `env` has succeeded profile runs of, with their "
            "runs newest first — the runs a dataset can be generated from. Read only. " + _RIGHT_ENV
        ),
        "input_schema": _obj({"env": {"type": "string"}}, ["env"]),
    },
    {
        "name": "define_synthetic_dataset",
        "description": (
            "REQ-1939: define (or redefine) a synthetic dataset: dataset_id, seed, scale (rows "
            "relative to the profiled tables) and tables, each {tableId, profileEnv, runId, "
            "scale?} from list_synthetic_profile_runs. Refused verbatim for an invalid name, a "
            "table set missing a parent its relationships need, or a pii column with neither a "
            "fake nor a synthetic rule. Redefining an existing id replaces it. Optional: "
            "fanoutConditions [{relationship, condition, count: {fixed} | {low, high} | "
            "{measured: true}}], assertions [statement], privateEpsilon (makes it private), and "
            "closenessThreshold with closenessDraws (both or neither; neither: closeness is not "
            "checked). " + _RIGHT_ENV
        ),
        "input_schema": _obj(
            {
                "dataset_id": {"type": "string"},
                "seed": {"type": "integer"},
                "scale": {"type": "number"},
                "tables": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "tableId": {"type": "integer"},
                            "profileEnv": {"type": "string"},
                            "runId": {"type": "string"},
                            "scale": {"type": "number"},
                        },
                        "required": ["tableId", "profileEnv", "runId"],
                    },
                },
                "fanoutConditions": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "relationship": {"type": "string"},
                            "condition": {"type": "string"},
                            "count": {"type": "object"},
                        },
                        "required": ["relationship", "condition", "count"],
                    },
                },
                "assertions": {"type": "array", "items": {"type": "string"}},
                "privateEpsilon": {"type": "number"},
                "closenessThreshold": {"type": "number"},
                "closenessDraws": {"type": "integer"},
            },
            ["dataset_id", "seed", "scale", "tables"],
        ),
    },
    {
        "name": "generate_synthetic_dataset",
        "description": (
            "REQ-1939: generate a defined dataset's rows into its store schema (replacing rows "
            "generated before). Refused verbatim when the dataset is unknown or its definition "
            "cannot be generated. " + _RIGHT_ENV
        ),
        "input_schema": _obj({"dataset_id": {"type": "string"}}, ["dataset_id"]),
    },
    {
        "name": "get_synthetic_report",
        "description": (
            "REQ-1939: the comparison report of a generated dataset — per table, column and "
            "measure, the source value, the synthetic value, their delta and a note. Read only. "
            + _RIGHT_ENV
        ),
        "input_schema": _obj({"dataset_id": {"type": "string"}}, ["dataset_id"]),
    },
    {
        "name": "drop_synthetic_dataset",
        "description": (
            "REQ-1939: drop a synthetic dataset and its generated tables. Irreversible — always "
            "confirm with present_choice (mode='yes_no') first. Refused 'not_found' for an "
            "unknown id. " + _RIGHT_ENV
        ),
        "input_schema": _obj({"dataset_id": {"type": "string"}}, ["dataset_id"]),
    },
    {
        "name": "get_environment_detail",
        "description": (
            "REQ-1942: an environment's detail -- its parent, data mode (inherit, unbound, "
            "test_fake, test_synthetic; none for prod), mutation handling (refused, reversible, "
            "direct), each source's binding (own, inherited, unbound), and its test data: the "
            "faked column count, the sensitive columns with no fake, the synthetic dataset's "
            "status. Read only. " + _RIGHT_ENV
        ),
        "input_schema": _obj({"env": {"type": "string"}}, ["env"]),
    },
    {
        "name": "set_environment_data",
        "description": (
            "REQ-1942: change an environment's data mode and/or mutation handling. Inherit and "
            "unbound set every source's binding; the test modes leave them. A change to or from "
            "test_synthetic discards the kept mutations: refused unless confirmDiscard is true "
            "-- ask the user first with present_choice (mode='yes_no'). Test (fake) is refused "
            "while a sensitive column has no fake. Needs environment_data; a caller without it "
            "is refused 'Missing capability'."
        ),
        "input_schema": _obj(
            {
                "env": {"type": "string"},
                "dataMode": {
                    "type": "string",
                    "enum": ["inherit", "unbound", "test_fake", "test_synthetic"],
                },
                "mutationHandling": {"type": "string", "enum": ["refused", "reversible", "direct"]},
                "confirmDiscard": {"type": "boolean"},
            },
            ["env"],
        ),
    },
    {
        "name": "set_source_binding",
        "description": (
            "REQ-1942: set one source's binding in an environment: inherited (the parent's "
            "connection, by reference) or unbound. A source becomes the environment's own by "
            "being given a connection. Needs environment_data; a caller without it is refused "
            "'Missing capability'."
        ),
        "input_schema": _obj(
            {
                "env": {"type": "string"},
                "sourceId": {"type": "string"},
                "binding": {"type": "string", "enum": ["inherited", "unbound"]},
            },
            ["env", "sourceId", "binding"],
        ),
    },
    {
        "name": "get_environment_synthetic_plan",
        "description": (
            "REQ-1942: for a Test (synthetic) environment, what generating its whole model "
            "would generate: every table not backed by an API with its parent's successful "
            "profile runs (selected: the latest), the API-backed tables that are never generated "
            "with their keys, key rules, address and what a lookup returns, and ready (every "
            "table has a run). Read only. " + _RIGHT_ENV
        ),
        "input_schema": _obj({"env": {"type": "string"}}, ["env"]),
    },
    {
        "name": "generate_environment_model",
        "description": (
            "REQ-1942: generate a Test (synthetic) environment's whole model in the background; "
            "its detail then shows generating, then ready or failed with the reason. runs maps "
            "a table id to a profile run id (a table left out takes its latest). Regenerating "
            "discards the kept mutations: refused unless confirmDiscard is true -- ask first "
            "with present_choice (mode='yes_no'). Needs environment_data; a caller without it "
            "is refused 'Missing capability'."
        ),
        "input_schema": _obj(
            {
                "env": {"type": "string"},
                "runs": {"type": "object", "additionalProperties": {"type": "string"}},
                "seed": {"type": "integer"},
                "scale": {"type": "number"},
                "confirmDiscard": {"type": "boolean"},
            },
            ["env"],
        ),
    },
    {
        "name": "export_model_config",
        "description": (
            "REQ-1919/REQ-1304: export the acting org's governed model as a config file (YAML): "
            "returns {filename, yaml}. Read only. Needs user_management in this org (an "
            "org_admin); others are refused 'user_management in <org> required'."
        ),
        "input_schema": _obj({}),
    },
]

NAMES: frozenset[str] = frozenset(s["name"] for s in SPECS)


async def dispatch(state: Any, role: str, name: str, args: dict, request: Any) -> Any:
    """Run tool ``name`` with ``args`` as keywords (only the fields the model sent)."""
    if request is None:
        raise ValueError(f"{name} requires a verified request context")
    fn = getattr(model_tools, name)
    return await fn(state, role, request, **args)


SYSTEM_SECTION = (
    "REQ-1934/1494/1939/1919: the Data Profiler, fakes, synthetic datasets and the config export. "
    "Read tools act directly: find_table_id, list_profilers, list_profile_runs, get_table_profile, "
    "list_profile_constraints, list_profile_checks, list_fake_kinds, get_table_fakes, "
    "propose_fakes, list_synthetic_datasets, list_synthetic_profile_runs, get_synthetic_report, "
    "export_model_config. The write tools carry their own capability check and act directly once "
    "the user has asked for that change — no propose/confirm_required flow: set_table_profiler, "
    "run_profiler, run_table_profile, decide_profile_constraint, forget_profile_constraint, "
    "export_profile_constraint, create_drift_check, create_expectation_check, set_column_fake, "
    "define_synthetic_dataset, generate_synthetic_dataset, drop_synthetic_dataset, "
    "set_environment_data, set_source_binding, generate_environment_model "
    "(get_environment_detail and get_environment_synthetic_plan read). Confirm with "
    "present_choice (mode='yes_no') before drop_synthetic_dataset and before set_table_profiler "
    "removes a table from its profiler. propose_fakes saves nothing: show its proposals, ask which "
    "to save (present_choice, mode='multi'), then call set_column_fake once per chosen column. A "
    "refusal from any of these is the admin's own message — report it to the user as it is. "
    "Tables are named by numeric id: get it from find_table_id, never guess one. Per REQ-1834, navigate to /tables before a profiler, constraint, check or fake "
    "change, and to /admin/environments before a synthetic dataset change."
)
