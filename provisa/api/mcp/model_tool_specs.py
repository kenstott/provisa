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
_RIGHT_TABLE = "Needs table_registration."
_RIGHT_ENV = "Needs environment_management."


def _obj(properties: dict, required: list[str] | None = None) -> dict:
    return {"type": "object", "properties": properties, "required": required or []}


SPECS: list[dict] = [
    {
        "name": "find_table_id",
        "description": (
            "The numeric id of a registered table, which the profiler, constraint, check and fake"
            " tools take. domain: the table's domain (schema); table: its registered name or "
            "alias. Returns every match (id, source, domain, name, profiler); empty if none. Read"
            " only. " + _RIGHT_TABLE
        ),
        "input_schema": _obj(
            {"domain": {"type": "string"}, "table": {"type": "string"}}, ["domain", "table"]
        ),
    },
    {
        "name": "list_profilers",
        "description": (
            "List the Data Profiler sources: id, cron schedule, sample budget (sampleAboveCells),"
            " lowCardinalityMax, member tables. Read only. " + _RIGHT_TABLE
        ),
        "input_schema": _obj({}),
    },
    {
        "name": "set_table_profiler",
        "description": (
            "Add a table to a profiler (profiler_source_id from list_profilers) or remove it "
            "(null). A table whose rows need a required filter cannot join; refusals come back "
            "verbatim. Removing stops its scheduled profiling: confirm with the user "
            "(present_choice, mode='yes_no') first. " + _RIGHT_TABLE
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
            "Run a profiler now over all its member tables. Needs source_registration."
        ),
        "input_schema": _obj({"source_id": {"type": "string"}}, ["source_id"]),
    },
    {
        "name": "run_table_profile",
        "description": (
            "Profile one table now; it must have joined a profiler. Returns runId, rowCount, "
            "profiledRows. " + _RIGHT_TABLE
        ),
        "input_schema": _obj({"table_id": _TABLE_ID}, ["table_id"]),
    },
    {
        "name": "declare_table_profile",
        "description": (
            "Store a hand-written profile of a table in the active environment (a table with no "
            "data, or a what-if): {rowCount, columns: {<column>: {nullShare, distinctCount, range"
            " {min, max} | quantiles [101] | values [{value, weight}] | shapes [{shape, weight}],"
            " integerOnly}}, fanouts: {<relationship>: {range | quantiles}}, dependence}. An "
            "omitted column uses its fake or synthetic rule; one with neither is refused. Copy a "
            "run with get_profile_run_as_declared. Returns runId. " + _RIGHT_TABLE
        ),
        "input_schema": _obj(
            {"table_id": _TABLE_ID, "profile": {"type": "object"}}, ["table_id", "profile"]
        ),
    },
    {
        "name": "get_profile_run_as_declared",
        "description": (
            "A table's profile run (measured or declared) as a declared profile you may edit and "
            "store with declare_table_profile. Read only. " + _RIGHT_TABLE
        ),
        "input_schema": _obj(
            {"table_id": _TABLE_ID, "run_id": {"type": "string"}}, ["table_id", "run_id"]
        ),
    },
    {
        "name": "list_profile_runs",
        "description": (
            "A table's profile runs, newest first (run_id, run_time, status, row_count, sampling,"
            " error); it must have joined a profiler. Read only. " + _RIGHT_TABLE
        ),
        "input_schema": _obj({"table_id": _TABLE_ID}, ["table_id"]),
    },
    {
        "name": "get_table_profile",
        "description": (
            "One profile run's results as your role may see them (masked columns stay masked). "
            "run_id: default the latest succeeded run. kinds: default runs, columns, top_values, "
            "plausible_type, constraints, constraint_checks, classifications, drift; also "
            "quantiles, histogram, fits, fit_quality, fanout, fanout_runs, shapes, duplicates, "
            "correlations, dependencies, joint_counts, repeats. Read only. " + _RIGHT_TABLE
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
            "The constraints the latest succeeded run proposes (not_null, unique, value_set, "
            "range, ordering; with evidence, share, whether sampled), the decisions taken (with "
            "ids), and the checker tables an accepted one can be exported to. Read only. "
            + _RIGHT_TABLE
        ),
        "input_schema": _obj({"table_id": _TABLE_ID}, ["table_id"]),
    },
    {
        "name": "decide_profile_constraint",
        "description": (
            "Accept or dismiss one proposed constraint, named by kind + column (+ other_column "
            "for ordering) as list_profile_constraints shows it. definition: an edited form to "
            "accept (e.g. {values:[...]} or {min,max}); omitted, the proposed one. Later runs "
            "check accepted constraints. " + _RIGHT_TABLE
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
            "Withdraw an accept/dismiss decision by its id (from list_profile_constraints); the "
            "constraint is proposed again. " + _RIGHT_TABLE
        ),
        "input_schema": _obj(
            {"table_id": _TABLE_ID, "constraint_id": {"type": "string"}},
            ["table_id", "constraint_id"],
        ),
    },
    {
        "name": "export_profile_constraint",
        "description": (
            "Add an accepted constraint to a data-quality checker table that scans the table. "
            "checker_table_id: needed only when several checkers scan it. " + _RIGHT_TABLE
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
            "What a drift or expectation check for a table can use: whether its drift result "
            "table is registered, the checker tables scanning it, and the expectations-shaped "
            "tables. Read only. " + _RIGHT_TABLE
        ),
        "input_schema": _obj({"table_id": _TABLE_ID}, ["table_id"]),
    },
    {
        "name": "create_drift_check",
        "description": (
            "Add a drift check over the table's drift result table (registered on the profiler) "
            "to a checker. checker_table_id: needed only when several checkers scan it. "
            + _RIGHT_TABLE
        ),
        "input_schema": _obj(
            {"table_id": _TABLE_ID, "checker_table_id": _CHECKER_ID}, ["table_id"]
        ),
    },
    {
        "name": "create_expectation_check",
        "description": (
            "Add a check comparing the table's drift results with an expectations table "
            "(expectations_table_id, from list_profile_checks' expectationTables). The drift "
            "table must be registered; checker_table_id: needed only when several checkers scan "
            "it. " + _RIGHT_TABLE
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
            "The kinds of fake (categories, bool, distributions, relative fakes, sql, ...) and "
            "fake methods (email, name, phone, ...) a column can declare, with argument names. "
            "Read before writing a fake. A declaration is one call, e.g. categories((shoes, bra),"
            " (.4, .6)), normal(mean=50, sd=10, min=0), after(created_at, 1 to 10 days), email()."
            " syntheticRuleOnly kinds may only be a synthetic rule. Read only. " + _RIGHT_TABLE
        ),
        "input_schema": _obj({}),
    },
    {
        "name": "get_table_fakes",
        "description": (
            "Each column of a table with its fake, stable flag, synthetic rule and pii tag. Read "
            "only. " + _RIGHT_TABLE
        ),
        "input_schema": _obj({"table_id": _TABLE_ID}, ["table_id"]),
    },
    {
        "name": "set_column_fake",
        "description": (
            "Set or clear one column's fake, synthetic rule (used only by synthetic generation, "
            "over the fake) and stable flag. An omitted field keeps its value; '' clears a fake "
            "or rule. Checked as the table editor checks it; refusals come back verbatim. "
            + _RIGHT_TABLE
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
            "Propose a fake for each identifying column and a synthetic rule for the rest, from "
            "the table's latest succeeded profile run. Saves nothing: show the proposals, ask the"
            " user which to save (present_choice), then save each with set_column_fake. "
            "unmatchedPii: pii columns with no confident proposal. columns: only these, even if "
            "already declared; omitted, every column declaring neither. " + _RIGHT_TABLE
        ),
        "input_schema": _obj(
            {"table_id": _TABLE_ID, "columns": {"type": "array", "items": {"type": "string"}}},
            ["table_id"],
        ),
    },
    {
        "name": "list_synthetic_datasets",
        "description": (
            "The synthetic datasets in this environment: id, seed, scale, status, store schema, "
            "tables (each from a profile run). Read only. " + _RIGHT_ENV
        ),
        "input_schema": _obj({}),
    },
    {
        "name": "list_synthetic_profile_runs",
        "description": (
            "The tables environment env has succeeded profile runs of, runs newest first: what a "
            "dataset can be generated from. Read only. " + _RIGHT_ENV
        ),
        "input_schema": _obj({"env": {"type": "string"}}, ["env"]),
    },
    {
        "name": "define_synthetic_dataset",
        "description": (
            "Define or replace a synthetic dataset: dataset_id, seed, scale (rows relative to the"
            " profiled tables), tables [{tableId, profileEnv, runId, scale?}] from "
            "list_synthetic_profile_runs. Optional: fanoutConditions [{relationship, condition, "
            "count: {fixed} | {low, high} | {measured: true}}], assertions [statement], "
            "privateEpsilon (makes it private), closenessThreshold with closenessDraws (both or "
            "neither). Refused for an invalid name, a table missing a parent its relationships "
            "need, or a pii column with neither fake nor synthetic rule. " + _RIGHT_ENV
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
            "Generate a defined dataset's rows into its store schema, replacing rows generated "
            "before. " + _RIGHT_ENV
        ),
        "input_schema": _obj({"dataset_id": {"type": "string"}}, ["dataset_id"]),
    },
    {
        "name": "get_synthetic_report",
        "description": (
            "A generated dataset's comparison report: per table, column and measure, the source "
            "and synthetic values, their delta and a note. Read only. " + _RIGHT_ENV
        ),
        "input_schema": _obj({"dataset_id": {"type": "string"}}, ["dataset_id"]),
    },
    {
        "name": "drop_synthetic_dataset",
        "description": (
            "Drop a synthetic dataset and its generated tables. Irreversible: confirm with the "
            "user (present_choice, mode='yes_no') first. " + _RIGHT_ENV
        ),
        "input_schema": _obj({"dataset_id": {"type": "string"}}, ["dataset_id"]),
    },
    {
        "name": "get_environment_detail",
        "description": (
            "An environment's parent, data mode (inherit, unbound, test_fake, test_synthetic; "
            "none for prod), mutation handling (refused, reversible, direct), each source's "
            "binding (own, inherited, unbound), and test data (faked column count, sensitive "
            "columns with no fake, synthetic dataset status). Read only. " + _RIGHT_ENV
        ),
        "input_schema": _obj({"env": {"type": "string"}}, ["env"]),
    },
    {
        "name": "set_environment_data",
        "description": (
            "Change an environment's data mode and/or mutation handling. inherit and unbound set "
            "every source's binding; the test modes leave them. A change to or from "
            "test_synthetic discards kept mutations: refused unless confirmDiscard is true, so "
            "ask the user first (present_choice, mode='yes_no'). test_fake is refused while a "
            "sensitive column has no fake. Needs environment_data."
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
            "Set one source's binding in an environment: copied (the parent's connection, as "
            "written), unbound (cleared), or own, with connection {host, port, database, "
            "username, password} or {path}. Needs environment_data."
        ),
        "input_schema": _obj(
            {
                "env": {"type": "string"},
                "sourceId": {"type": "string"},
                "binding": {"type": "string", "enum": ["copied", "unbound", "own"]},
                "connection": {"type": "object"},
            },
            ["env", "sourceId", "binding"],
        ),
    },
    {
        "name": "recopy_environment_sources",
        "description": (
            "Replace the connection of each named source in an environment (every source when "
            "sources is omitted) with its parent's, as written. Needs environment_data."
        ),
        "input_schema": _obj(
            {"env": {"type": "string"}, "sources": {"type": "array", "items": {"type": "string"}}},
            ["env"],
        ),
    },
    {
        "name": "get_environment_synthetic_plan",
        "description": (
            "For a Test (synthetic) environment, what generating its whole model would do: each "
            "table with the profiles it may use (selected: the parent's latest measured run, else"
            " the latest declared profile), unavailable tables with why, commandsNotDefined, and "
            "ready. Read only. " + _RIGHT_ENV
        ),
        "input_schema": _obj({"env": {"type": "string"}}, ["env"]),
    },
    {
        "name": "generate_environment_model",
        "description": (
            "Generate a Test (synthetic) environment's whole model, in two calls. Without digest "
            "it generates nothing and returns the Limitations of Synthetic Data warning: show it "
            "and ask the user (present_choice, mode='yes_no'); once they confirm, call again with"
            " the same runs, seed and scale and the warning's digest to generate in the "
            "background (get_environment_detail shows progress). runs: {table id: profile run "
            "id}; an omitted table takes the preselected run. Needs environment_data."
        ),
        "input_schema": _obj(
            {
                "env": {"type": "string"},
                "runs": {"type": "object", "additionalProperties": {"type": "string"}},
                "seed": {"type": "integer"},
                "scale": {"type": "number"},
                "digest": {"type": "string"},
            },
            ["env"],
        ),
    },
    {
        "name": "reset_environment_mutations",
        "description": (
            "Drop the mutations a Reversible environment keeps, returning it to its baseline. "
            "Irreversible: confirm with the user (present_choice, mode='yes_no') first. Needs "
            "environment_data."
        ),
        "input_schema": _obj({"env": {"type": "string"}}, ["env"]),
    },
    {
        "name": "export_model_config",
        "description": (
            "Export the acting org's governed model as a YAML config: {filename, yaml}. Read "
            "only. Needs user_management in the org (org_admin)."
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
    "The Data Profiler, fakes, synthetic datasets and the config export. "
    "Read tools act directly: find_table_id, list_profilers, list_profile_runs, get_table_profile, "
    "get_profile_run_as_declared, "
    "list_profile_constraints, list_profile_checks, list_fake_kinds, get_table_fakes, "
    "propose_fakes, list_synthetic_datasets, list_synthetic_profile_runs, get_synthetic_report, "
    "export_model_config. The write tools carry their own capability check and act directly once "
    "the user has asked for that change — no propose/confirm_required flow: set_table_profiler, "
    "run_profiler, run_table_profile, declare_table_profile, decide_profile_constraint, forget_profile_constraint, "
    "export_profile_constraint, create_drift_check, create_expectation_check, set_column_fake, "
    "define_synthetic_dataset, generate_synthetic_dataset, drop_synthetic_dataset, "
    "set_environment_data, set_source_binding, recopy_environment_sources, "
    "generate_environment_model, "
    "reset_environment_mutations "
    "(get_environment_detail and get_environment_synthetic_plan read). Confirm with "
    "present_choice (mode='yes_no') before drop_synthetic_dataset and before set_table_profiler "
    "removes a table from its profiler. propose_fakes saves nothing: show its proposals, ask which "
    "to save (present_choice, mode='multi'), then call set_column_fake once per chosen column. A "
    "refusal from any of these is the admin's own message — report it to the user as it is. "
    "Tables are named by numeric id: get it from find_table_id, never guess one. Per the page-first rule, navigate to /tables before a profiler, constraint, check or fake "
    "change, and to /admin/environments before a synthetic dataset change."
)
