# Copyright (c) 2026 Kenneth Stott
# Canary: 6e2a8d14-9c53-4f7b-8a06-1d5b3e7c9f42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every way of saving, and the guard each has (REQ-1557).

A held organization's member is refused an environment reference AT THE SAVE, so the rule holds
only while every save passes a guard. This file is the list. Each test reads the code for one
kind of save path and compares what it finds with the list written here; a path added without a
line here fails, and the line says which guard the path has or why it carries no text a member
entered.
"""

# Requirements: REQ-1557

from __future__ import annotations

import ast
import re
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_PROVISA = _ROOT / "provisa"


def _sources() -> dict[str, str]:
    return {str(p.relative_to(_ROOT)): p.read_text() for p in sorted(_PROVISA.rglob("*.py"))}


def _name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Call):
        return _name(node.func)
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Name):
        return node.id
    return None


def _calls(node: ast.AST) -> set[str]:
    return {n for c in ast.walk(node) if isinstance(c, ast.Call) and (n := _name(c.func))}


def _functions(tree: ast.AST):
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            yield node


# --- 1. the statements a control-plane connection executes ------------------------------------

#: What executes a statement on the connection.
_EXECUTES = {
    "_exec",
    "_run",
    "_execute_core",
    "_execute_script",
    "exec_driver_sql",
    "execute",
    "copy",
    "singlestore_load_data",
}

#: Each method of ``Connection`` that executes, and the calls that guard it. ``()``: it reaches
#: the database only through a guarded method named in its reason, or writes no row.
_CONNECTION: dict[str, tuple[tuple[str, ...], str]] = {
    "_exec": ((), "the executor itself: every caller is a method of this list"),
    "_run": (("check_raw",), "raw SQL with bound values"),
    "execute": ((), "through _run; with no bound values, a script (_execute_script)"),
    "executemany": (("check_raw",), "raw SQL, one row of bound values at a time"),
    "fetch": ((), "through _run"),
    "fetch_with_columns": ((), "through _run"),
    "fetch_described": ((), "through _run"),
    "fetchrow": ((), "through _run"),
    "fetchval": ((), "through _run"),
    "_execute_core": (
        ("guard_statement", "_refuse_edit_of_operator_row"),
        "the seam: a written value, and an UPDATE of a row that carries a reference",
    ),
    "execute_core": ((), "through _execute_core"),
    "execute_core_many": (("guard_statement", "guard_rows"), "the values travel beside it"),
    "bulk_copy": (("guard_rows",), "a bulk load's rows"),
    "_bulk_load_singlestore": ((), "bulk_copy's SingleStore path, after its guard"),
    "upsert": (("guard_statement",), "its INSERT; its UPDATE goes through _execute_core"),
    "upsert_returning": ((), "through upsert and _execute_core"),
    "insert_returning": ((), "through _execute_core"),
    "_exec_narrowed": ((), "the statement _execute_core guarded"),
    "_refuse_edit_of_operator_row": ((), "a SELECT: the guard's own read"),
    "_count_rows": ((), "a SELECT"),
    "_column_records": ((), "a SELECT"),
    "advisory_xact_lock": ((), "a lock, no row"),
    "advisory_lock": ((), "a lock, no row"),
    "_execute_script": ((), "a script with no bound values: the schema's own DDL"),
    "execute_script": ((), "through _execute_script"),
}


def _connection_methods() -> dict[str, ast.AST]:
    tree = ast.parse((_PROVISA / "core" / "database.py").read_text())
    (cls,) = [n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "Connection"]
    return {
        fn.name: fn
        for fn in cls.body
        if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)) and _calls(fn) & _EXECUTES
    }


def test_every_statement_a_connection_executes_is_listed_with_its_guard():
    methods = _connection_methods()
    assert sorted(methods) == sorted(_CONNECTION), (
        "Connection gained or lost a method that executes a statement: list it here with the "
        "guard it has (core.env_reference_guard)"
    )
    for name, (guards, reason) in _CONNECTION.items():
        missing = set(guards) - _calls(methods[name])
        assert not missing, f"Connection.{name} ({reason}) no longer calls {sorted(missing)}"


def test_the_seam_guards_call_the_environment_reference_guard():
    source = (_PROVISA / "core" / "env_secrets.py").read_text()
    tree = ast.parse(source)
    by_name = {fn.name: fn for fn in _functions(tree)}
    assert "check_written" in _calls(by_name["guard_statement"])
    assert "check_values" in _calls(by_name["guard_rows"])


# --- 2. raw SQL writes -------------------------------------------------------------------------

_RAW_WRITE = re.compile(
    r"""^\s*(INSERT\s+INTO|UPDATE)\s+(?:(?:"[^"]+"|[\w{}.]+)\s*\.\s*)?("[^"]+"|[\w{}]+)""",
    re.IGNORECASE,
)

#: Raw SQL that writes a table whose text is resolved, by file and table: none binds a value a
#: caller entered (Connection._run refuses one that does, whatever the table).
_RAW_MODEL_WRITES: dict[tuple[str, str], str] = {
    ("provisa/core/db.py", "roles"): "a standard role's capabilities, from constants",
    ("provisa/core/repositories/retired_roles.py", "user_role_assignments"): (
        "retired role ids carried to platform_admin, from the rows themselves"
    ),
    ("provisa/discovery/fk_introspect.py", "relationships"): (
        "a discovered foreign key: table ids and column names; bound values pass check_raw"
    ),
}

#: Files whose raw writes name their table by a placeholder, and what they write. A raw write is
#: held by its table's name (env_reference_guard.check_raw), which a placeholder gives at run
#: time: none of these names a table of the control plane whose text is resolved.
_RAW_TEMPLATED_WRITES: dict[str, str] = {
    "provisa/api/airport/server.py": "a client's rows into a source's table (data plane)",
    "provisa/api/flight/flightsql.py": "a client's rows into a source's table (data plane)",
    "provisa/api_source/engine_cache.py": "an API source's cached rows in the engine",
    "provisa/codehost/pgcatalog.py": "a code host's in-memory catalog",
    "provisa/compiler/mutation_gen.py": "a governed mutation's SQL (data plane)",
    "provisa/compiler/temp_tables.py": "a session's temporary table (data plane)",
    "provisa/compiler/write_admission.py": "a governed mutation's SQL (data plane)",
    "provisa/core/config_stamp.py": "the config stamp's own row and trigger",
    "provisa/core/env_copy.py": "an environment's copy of stored rows, table by table",
    "provisa/cypher/merge_writer.py": "a governed Cypher write's SQL (data plane)",
    "provisa/cypher/write_translator.py": "a governed Cypher write's SQL (data plane)",
    "provisa/federation/": "engine, replica and store tables (data plane)",
    "provisa/ingest/router.py": "ingested rows into an ingest table (data plane)",
    "provisa/mv/": "a materialized view's rows in its store",
    "provisa/pgwire/": "the session's catalog, COPY and temporary tables",
    "provisa/scheduler/jobs.py": "telemetry rows",
}


def _raw_writes() -> tuple[set[tuple[str, str]], set[str]]:
    from provisa.core.env_classes import CLASSIFIED, NEVER_RUNTIME, NEVER_SENSITIVE

    held = CLASSIFIED - NEVER_RUNTIME - NEVER_SENSITIVE
    named: set[tuple[str, str]] = set()
    templated: set[str] = set()
    for path, source in _sources().items():
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                text = node.value
            elif isinstance(node, ast.JoinedStr):
                text = "".join(
                    v.value if isinstance(v, ast.Constant) and isinstance(v.value, str) else "{}"
                    for v in node.values
                )
            else:
                continue
            match = _RAW_WRITE.match(text)
            if match is None:
                continue
            table = match.group(2).strip('"')
            if "{" in table:
                templated.add(path)
            elif table in held:
                named.add((path, table))
    return named, templated


def test_every_raw_write_of_a_table_whose_text_is_resolved_is_listed():
    named, templated = _raw_writes()
    assert named == set(_RAW_MODEL_WRITES), (
        "raw SQL writes a model table: write it through Connection.execute_core, or list it "
        "here with why it binds nothing a caller entered"
    )
    unlisted = sorted(
        path
        for path in templated
        if not any(path.startswith(known) for known in _RAW_TEMPLATED_WRITES)
    )
    assert not unlisted, (
        f"raw SQL writes a table named by a placeholder in {unlisted}: list the file here with "
        "what it writes"
    )


# --- 3. writes of the model as a whole, which the seam holds to no person ----------------------

_WHOLE_MODEL_MARKS = {"commits_itself", "committed_by_caller", "unnarrowed", "layout"}

#: Each function that writes the model with no person acting, what it writes, and the call that
#: guards it when what it writes may be text a caller entered or brought.
_WHOLE_MODEL: dict[tuple[str, str], tuple[str | None, str]] = {
    ("provisa/api/admin/environment_data_router.py", "edit_data_choices"): (
        None,
        "a data mode: stored bindings copied or cleared",
    ),
    ("provisa/api/admin/environment_data_router.py", "recopy_sources"): (
        None,
        "the parent's stored connections, copied again",
    ),
    ("provisa/api/admin/environment_data_router.py", "set_source_binding"): (
        "refuse_entered",
        "a connection the caller enters",
    ),
    ("provisa/api/admin/environments_router.py", "_move"): (
        None,
        "undo and redo: a commit of the organization's own history (env_deploy._load)",
    ),
    ("provisa/api/admin/environments_router.py", "decide_merge_request"): (
        None,
        "a reviewed merge of stored environments, or a load through env_deploy._load",
    ),
    ("provisa/api/admin/environments_router.py", "deploy_into_environment"): (
        None,
        "a tree by ref, loaded through env_deploy._load, which refuses a brought one",
    ),
    ("provisa/api/admin/environments_router.py", "merge_into_environment"): (
        None,
        "another environment's stored model",
    ),
    ("provisa/api/admin/environments_router.py", "pull_environment"): (
        None,
        "the remote's tree, loaded through env_deploy._load, which refuses a brought one",
    ),
    ("provisa/api/admin/environments_router.py", "rebuild_environment"): (
        None,
        "the stored model, serialized again",
    ),
    ("provisa/api/admin/orgs_router.py", "_provision_org_task"): (
        None,
        "an organization's creation: the operator's demo and standard rows",
    ),
    ("provisa/api/app.py", "_ensure_environment_baselines"): (None, "boot: stored models"),
    ("provisa/api/app.py", "_rebuild_schemas"): (None, "a runtime's build from the stored model"),
    ("provisa/api/demo_sandbox.py", "_build"): (None, "a sandbox's creation from its template"),
    ("provisa/api/demo_synthetic.py", "_give"): (None, "bindings to the demo's synthetic store"),
    ("provisa/core/config_loader.py", "_apply"): (
        None,
        "a configuration: the operator's file, or an import, refused at its route "
        "(test_the_import_route_refuses_before_it_resolves)",
    ),
    ("provisa/core/config_loader.py", "dry_run_config"): (None, "as _apply, rolled back"),
    ("provisa/core/db.py", "init_schema"): (None, "the schema's layout"),
    ("provisa/core/db.py", "write_layout_digest"): (None, "the layout's digest"),
    ("provisa/core/env_create.py", "create_environment"): (None, "a copy of a stored model"),
    ("provisa/core/model_change.py", "_wrapped"): (None, "commits_itself's own wrapper"),
    ("provisa/core/model_change.py", "commits_itself"): (None, "the decorator itself"),
    ("provisa/synthetic/env_model.py", "bind_generated"): (
        None,
        "bindings to the environment's synthetic store",
    ),
}


def _whole_model_writers() -> dict[tuple[str, str], ast.AST]:
    found: dict[tuple[str, str], ast.AST] = {}
    for path, source in _sources().items():
        if not _WHOLE_MODEL_MARKS & set(re.findall(r"\w+", source)):
            continue
        for fn in _functions(ast.parse(source)):
            marks = {_name(d) for d in fn.decorator_list}
            for node in ast.walk(fn):
                if isinstance(node, (ast.With, ast.AsyncWith)):
                    marks |= {
                        _name(item.context_expr)
                        for item in node.items
                        if isinstance(item.context_expr, ast.Call)
                    }
            if marks & _WHOLE_MODEL_MARKS:
                found[(path, fn.name)] = fn
    return found


def test_every_write_of_the_model_as_a_whole_is_listed_with_its_guard():
    writers = _whole_model_writers()
    assert sorted(writers) == sorted(_WHOLE_MODEL), (
        "a function writes the model with no person acting (commits_itself, "
        "committed_by_caller, unnarrowed, layout): the write seam does not hold it, so list it "
        "here with what it writes and, when that may be a caller's text, its guard"
    )
    for key, (guard, reason) in _WHOLE_MODEL.items():
        if guard is not None:
            assert guard in _calls(writers[key]), f"{key} ({reason}) no longer calls {guard}"


def test_a_brought_tree_is_refused_where_every_load_passes():
    tree = ast.parse((_PROVISA / "core" / "env_deploy.py").read_text())
    by_name = {fn.name: fn for fn in _functions(tree)}
    assert "_refuse_brought" in _calls(by_name["_load"])
    assert {"plan_deploy", "deploy_tree"} == {
        name for name, fn in by_name.items() if "_load" in _calls(fn)
    }


def test_the_import_route_refuses_before_it_resolves():
    source = (_PROVISA / "api" / "admin" / "import_router.py").read_text()
    (apply,) = [fn for fn in _functions(ast.parse(source)) if fn.name == "apply_import"]
    order = [
        _name(node.func)
        for node in sorted(
            (n for n in ast.walk(apply) if isinstance(n, ast.Call)),
            key=lambda n: (n.lineno, n.col_offset),
        )
    ]
    wanted = [
        "_require_an_import_right",
        "_require_section_rights_as_written",
        "refuse_entered",
        "parse_config_dict",
    ]
    assert [name for name in order if name in wanted] == wanted


# --- 4. statements executed beside the connection ----------------------------------------------

_DIRECT = re.compile(r"engine\.(begin|connect)\(|\bexec_driver_sql\(")

#: Files that execute on an engine directly, and what they write there.
_DIRECT_ENGINE: dict[str, str] = {
    "provisa/api/admin/settings_catalog_router.py": "a read",
    "provisa/api_source/engine_cache.py": "an API source's cached rows (data plane)",
    "provisa/api_source/oauth_store.py": "a sign-in's tokens, in its own table",
    "provisa/cli.py": "the operator's command line",
    "provisa/core/config_stamp.py": "a read of the stamp",
    "provisa/core/database.py": "the connection itself (section 1)",
    "provisa/core/db.py": "schema layout and its backfills",
    "provisa/core/deployment_settings.py": "the deployment's settings: a platform administrator's",
    "provisa/core/duckdb_store.py": "a store's own files",
    "provisa/core/org_registry_view.py": "a view's DDL",
    "provisa/core/platform_state/catalogs.py": "the engine's catalogs: names and hashes",
    "provisa/core/region_purge.py": "deletes",
    "provisa/core/repositories/retired_roles.py": "retired role ids (section 2)",
    "provisa/core/schema_admin.py": "the admin schema's DDL",
    "provisa/core/schema_create.py": "an org schema's DDL",
    "provisa/discovery/catalog_cache.py": "the catalog cache table's DDL",
    "provisa/executor/drivers/sqlalchemy_driver.py": "a source's own database (data plane)",
    "provisa/federation/query_residency.py": "a replica's reconcile (data plane)",
    "provisa/federation/replica_locks.py": "a lock",
    "provisa/federation/replica_target.py": "a replica's rows (data plane)",
    "provisa/federation/singlestore_kafka.py": "a pipeline in the engine (data plane)",
    "provisa/federation/sqlalchemy_runtime.py": "the engine (data plane)",
    "provisa/observability/ops_schema.py": "the telemetry store's DDL",
    "provisa/observability/otlp2sql.py": "telemetry rows",
    "provisa/profiler/carry.py": "profiles carried between stored models",
    "provisa/scheduler/holder.py": "the scheduler's own lock",
    "provisa/term_finding/carry.py": "term scans carried between stored models",
}


def test_every_file_that_executes_beside_the_connection_is_listed():
    found = sorted(path for path, source in _sources().items() if _DIRECT.search(source))
    assert found == sorted(_DIRECT_ENGINE), (
        "a file executes statements on an engine directly, beside Connection's guards: list it "
        "here with what it writes"
    )


# --- 5. what a member enters before, or without, a save -----------------------------------------


def test_the_admin_api_checks_what_is_entered_before_an_operation_runs():
    from provisa.api.admin.env_reference_check import EnteredEnvReference
    from provisa.api.admin.schema import admin_schema

    assert EnteredEnvReference in admin_schema.extensions
    app = (_PROVISA / "api" / "app.py").read_text()
    assert "dependencies=[_Depends(_entered_body)]" in app


def test_a_key_variable_is_refused_wherever_one_is_read():
    """Each read of the environment by the name an endpoint's settings give."""
    readers = {
        "provisa/llm/client.py": 2,
        "provisa/api/admin/ai_models_router.py": 1,
        "provisa/api/admin/discovery.py": 1,
        "provisa/core/secrets.py": 1,
        # Reads only variables it names itself (a vendor's own); an endpoint's key variable goes
        # through core.secrets.resolve_api_key_field.
        "provisa/api/mcp/chat.py": 0,
    }
    refusal = re.compile(r"(?<!def )(?:refuse_named_by_org|_refuse_key_variable_of_held_org)\(")
    for path, count in readers.items():
        guarded = len(refusal.findall((_ROOT / path).read_text()))
        assert guarded >= count, f"{path}: a key variable is read without its refusal"
    named = re.compile(r"api_key_env")
    read = re.compile(r"os\.environ(\.get\(|\[)(?![\"'])")
    reads = sorted(
        path for path, source in _sources().items() if named.search(source) and read.search(source)
    )
    # core.secrets reads the name it is handed (resolve_api_key_field) and names no field.
    assert reads == sorted(set(readers) - {"provisa/core/secrets.py"}), (
        "a file names api_key_env and reads the environment by a name it was given: refuse a "
        "held organization's own key variable there (refuse_named_by_org), and list it here"
    )
