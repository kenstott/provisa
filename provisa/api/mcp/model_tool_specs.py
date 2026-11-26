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

# Requirements: REQ-1934, REQ-1494, REQ-1939, REQ-1919, REQ-1304, REQ-2253

from __future__ import annotations

from typing import Any

from provisa.api.mcp import model_tools

_TABLE_ID = {"type": "integer", "description": "From find_table_id."}
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
            "A registered table's numeric id, which the profiler, constraint, check and fake "
            "tools take. domain: its domain (schema); table: its name or alias. Returns every "
            "match (id, source, domain, name, profiler). Read only. " + _RIGHT_TABLE
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
            "verbatim. Removing stops its scheduled profiling: confirm with the user first. "
            + _RIGHT_TABLE
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
            + _RIGHT_TABLE
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
            "to a checker. " + _RIGHT_TABLE
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
            "table must be registered. " + _RIGHT_TABLE
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
            "fake methods (email, name, phone, ...) a column can declare, with argument names and"
            " the column families each fits (text, integer, numeric, boolean, date, timestamp, "
            "other). Read before writing a fake, and pick one whose families hold the column's "
            "family (get_table_fakes). A declaration is one call, e.g. categories((shoes, bra),"
            " (.4, .6)), normal(mean=50, sd=10, min=0), after(created_at, 1 to 10 days), email()."
            " syntheticRuleOnly kinds may only be a synthetic rule. Read only. " + _RIGHT_TABLE
        ),
        "input_schema": _obj({}),
    },
    {
        "name": "get_table_fakes",
        "description": (
            "Each column of a table with its fake, stable flag, synthetic rule, pii tag and "
            "family (the kind of values it holds; a json column is other). Read only. "
            + _RIGHT_TABLE
        ),
        "input_schema": _obj({"table_id": _TABLE_ID}, ["table_id"]),
    },
    {
        "name": "set_column_fake",
        "description": (
            "Set or clear one column's fake, synthetic rule (used only by synthetic generation, "
            "over the fake) and stable flag. An omitted field keeps its value; '' clears a fake "
            "or rule. column is a column of the table: a field inside a json column (contact.email)"
            " is none. The fake must fit the column's family (get_table_fakes): no fake method "
            'fits an other (json) column, whose fake is a template: {"email": email(), "n": '
            'pyint()::integer, "tags": [word()], "kind": "x"}. Checked as the table editor '
            "checks it; refusals come back verbatim, and a missing right is named in them. "
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
            "already declared; omitted, every column declaring neither. A table with no "
            "succeeded profile run is refused (fakes.no_profile_run). It is not a prerequisite: "
            "a column's fake can always be chosen from list_fake_kinds and saved with "
            "set_column_fake. " + _RIGHT_TABLE
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
            "Define or replace a synthetic dataset. scale: rows relative to the profiled tables;"
            " tables from list_synthetic_profile_runs. A fanout condition's count: {fixed} | "
            "{low, high} | {measured: true}. privateEpsilon makes it private; closenessThreshold "
            "and closenessDraws go together. Refused for an invalid name, a table missing a "
            "parent its relationships need, or a pii column with neither fake nor synthetic "
            "rule. " + _RIGHT_ENV
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
            "user first. " + _RIGHT_ENV
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
            "ask the user first. test_fake is refused while a "
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
            "Generate a Test (synthetic) environment's whole model in two calls. Without digest "
            "it generates nothing and returns the Limitations of Synthetic Data warning: show it "
            "and ask the user; once they confirm, call again with"
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
            "Irreversible: confirm with the user first. Needs environment_data."
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
    await _refuse_outside_selection(state, name, args)
    fn = getattr(model_tools, name)
    return await fn(state, role, request, **args)


#: The arguments that name a registered table by id.
_TABLE_ARGS = ("table_id", "expectations_table_id", "checker_table_id")

#: The tools that act on what spans every domain: an environment's data, the synthetic datasets
#: (each over tables of any domain), the whole model's config.
_WHOLE_ORG = {
    "list_synthetic_datasets": "The synthetic datasets",
    "list_synthetic_profile_runs": "An environment's synthetic profile runs",
    "define_synthetic_dataset": "A synthetic dataset",
    "generate_synthetic_dataset": "A synthetic dataset",
    "get_synthetic_report": "A synthetic dataset",
    "drop_synthetic_dataset": "A synthetic dataset",
    "get_environment_detail": "An environment's data",
    "set_environment_data": "An environment's data",
    "set_source_binding": "An environment's data",
    "recopy_environment_sources": "An environment's data",
    "get_environment_synthetic_plan": "An environment's data",
    "generate_environment_model": "An environment's data",
    "reset_environment_mutations": "An environment's data",
    "export_model_config": "The model's config",
}


async def _refuse_outside_selection(state: Any, name: str, args: dict) -> None:
    """REQ-2253: under a selection in the domain filter, a tool reads and changes nothing outside
    it -- a table named by id, a domain named, a profiler's member tables -- and a tool acting on
    what spans every domain is refused by name. No selection: nothing is asked."""
    from provisa.security.selected_domains import current, refuse_object, refuse_whole_org

    if current() is None:
        return
    if name in _WHOLE_ORG:
        refuse_whole_org(_WHOLE_ORG[name])
    if name == "find_table_id":
        # REQ-2253: list_schemas returns SQL-safe names (e.g. pet_store); the scope the domain
        # filter binds holds raw ids (e.g. pet-store). Normalize both sides before comparing so
        # the same domain is never refused just because its id contains a hyphen or other
        # non-alphanumeric character that domain_to_sql_name replaces with an underscore.
        from provisa.compiler.naming import domain_to_sql_name

        scope = current()
        sql_arg = domain_to_sql_name(args["domain"])
        if not any(domain_to_sql_name(d) == sql_arg for d in scope):  # type: ignore[union-attr]
            raise PermissionError(
                f"Domain {args['domain']!r} is outside the domains selected in the domain filter"
            )
    from provisa.api.admin.domain_guard import table_domain

    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as conn:
        for key in _TABLE_ARGS:
            if args.get(key) is not None:
                table_id = int(args[key])
                refuse_object(f"Table {table_id}", {await table_domain(conn, table_id)})
        if name == "run_profiler":
            from provisa.profiler.source import members

            for member in await members(conn, args["source_id"]):
                domain = await table_domain(conn, member["id"])
                refuse_object(f"Table {member['table_name']!r}", {domain})


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
    "(get_environment_detail and get_environment_synthetic_plan read). Where a tool says to "
    "confirm with or ask the user first, ask with present_choice (mode='yes_no'): before "
    "drop_synthetic_dataset, reset_environment_mutations, set_environment_data discarding kept "
    "mutations, generate_environment_model's second call, and before set_table_profiler "
    "removes a table from its profiler. propose_fakes saves nothing: show its proposals, ask which "
    "to save (present_choice, mode='multi'), then call set_column_fake once per chosen column. "
    'When the user names the columns to fake ("create fake defs for users.email, ..."), choose '
    "each fake yourself: read the column's family (get_table_fakes) and a kind or method that "
    "fits it (list_fake_kinds), save it with set_column_fake, and report each column saved or "
    "refused with the refusal's reason; propose_fakes is only for proposing from a profile run. A "
    "fake def is a column's fake, never a switch of environment. A "
    "refusal from any of these is the admin's own message — report it to the user as it is. "
    "Tables are named by numeric id: get it from find_table_id, never guess one. Per the page-first rule, navigate to /tables before a profiler, constraint, check or fake "
    "change, and to /admin/environments before a synthetic dataset change."
)


# -- tags: the tag assignment form's and the tag editor's edits ----------------------------------

_OBJECT = {
    "object_kind": {
        "type": "string",
        "enum": [
            "source",
            "table",
            "column",
            "relationship",
            "command",
            "product",
            "term",
            "rls_rule",
            "role",
            "metric",
            "document",
            "section",
        ],
    },
    "object_key": {
        "type": "string",
        "description": "As read_screen's tags give it: <kind>:<id>, column:<table id>:<column>.",
    },
    "tag_id": {
        "type": "string",
        "description": "Tag id with its parameter if any (entity:customer).",
    },
}
_CITATION = {
    "type": "object",
    "description": "A library document or one of its sections, by heading or by page.",
    "properties": {
        "document_id": {"type": "integer"},
        "section_heading": {"type": "string"},
        "section_page": {"type": "integer"},
    },
    "required": ["document_id"],
}
_CONFIRMED = " Over MCP, written at once."

TAG_SPECS: list[dict] = [
    {
        "name": "assign_tag",
        "description": (
            "Put a tag on an object or change that assignment (assignTag): reason, expiry (ISO "
            "date) and citations, the whole set, where the tag asks for them." + _CONFIRMED
        ),
        "input_schema": _obj(
            {
                **_OBJECT,
                "reason": {"type": "string"},
                "expires_on": {"type": "string"},
                "citations": {"type": "array", "items": _CITATION},
            },
            ["object_kind", "object_key", "tag_id"],
        ),
    },
    {
        "name": "unassign_tag",
        "description": (
            "Take a tag off an object (unassignTag); an inherited or derived tag is refused."
            + _CONFIRMED
        ),
        "input_schema": _obj(_OBJECT, ["object_kind", "object_key", "tag_id"]),
    },
    {
        "name": "cite_tag_assignment",
        "description": (
            "An assignment of a tag cites a document or section (citeTagAssignment); citing again "
            "re-records it, confirming one that needs review." + _CONFIRMED
        ),
        "input_schema": _obj(
            {**_OBJECT, "citation": _CITATION},
            ["object_kind", "object_key", "tag_id", "citation"],
        ),
    },
    {
        "name": "uncite_tag_assignment",
        "description": (
            "Take a citation off one assignment of a tag (unciteTagAssignment)." + _CONFIRMED
        ),
        "input_schema": _obj(
            {**_OBJECT, "citation": _CITATION},
            ["object_kind", "object_key", "tag_id", "citation"],
        ),
    },
    {
        "name": "cite_tag_definition",
        "description": (
            "A tag's definition cites the document or section saying why it exists "
            "(citeTagDefinition); citing again re-records it." + _CONFIRMED
        ),
        "input_schema": _obj(
            {"tag_id": _OBJECT["tag_id"], "citation": _CITATION}, ["tag_id", "citation"]
        ),
    },
    {
        "name": "uncite_tag_definition",
        "description": (
            "Take a citation off a tag's definition (unciteTagDefinition)." + _CONFIRMED
        ),
        "input_schema": _obj(
            {"tag_id": _OBJECT["tag_id"], "citation": _CITATION}, ["tag_id", "citation"]
        ),
    },
]

TAG_NAMES: frozenset[str] = frozenset(s["name"] for s in TAG_SPECS)

TAG_SYSTEM_SECTION = (
    "Tag edits: assign_tag, unassign_tag, cite_tag_assignment, uncite_tag_assignment, "
    "cite_tag_definition, uncite_tag_definition are the tag assignment form's and the tag "
    "editor's own changes. Name the object by object_kind and object_key exactly as read_screen's "
    "tags report it. Each is a proposal: the user sees the change and confirms it before it is "
    "written, so never ask a separate yes/no first; a refusal is the form's own message, report "
    "it as it is."
)
