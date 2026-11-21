# Copyright (c) 2026 Kenneth Stott
# Canary: fb5d1bb1-0914-4303-b3dc-0b98ed1ce6af
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The groups of the MCP server's tools, and which of them a session is listed (REQ-2246).

The one place a tool's group is named. Every registered tool belongs to exactly one group;
``check_registered`` refuses a server whose tools and this table differ, so a new tool fails
the build until it is given a group here.

A client connects in one of two modes, named by the ``tools`` parameter of the MCP address (or,
when it names none, by the operator setting ``mcp.tools``): ``full`` lists every tool; ``sip``
lists the groups nearly every question uses plus ``tool_groups``, the tool that names the other
groups and adds one to the session. Only the listing differs: every tool can be called in either
mode, and is allowed or refused by the caller's rights as in full mode.
"""

from __future__ import annotations

from typing import Any
from weakref import WeakKeyDictionary

# The address parameter and the operator setting name the mode with the same words.
MODE_PARAMETER = "tools"
FULL = "full"
SIP = "sip"
MODES = (FULL, SIP)

# The tool that names the groups and adds one to the session.
CATALOG_TOOL = "tool_groups"

# Each group's one line: what it is for. Shown to a client by ``tool_groups``.
PURPOSES: dict[str, str] = {
    "find": "Find what there is to ask about: schemas, tables, glossary terms, commands, "
    "reference documents, your role, and the groups of tools.",
    "describe_tables": "Describe a table: its columns, keys and the names each query surface uses.",
    "query": "Run or check SQL under your role.",
    "metrics": "List governed metrics and read one by name.",
    "statistics": "Statistics over a query's result without its rows: describe, correlate, "
    "regress, compare, forecast; and scored yes/no, choice and rubric decisions.",
    "graph": "Run Cypher, draw the graph it returns, and look up its node and property names.",
    "glossary": "Manage glossary terms: create, edit, approve, deprecate, retire, relate, tag "
    "and ground them on tables.",
    "data_products": "Manage data products: list, create, set members, publish, deprecate, "
    "retire and restore.",
    "metric_management": "Create, replace and delete governed metrics.",
    "proposals": "Propose a new source or table for a person to review and register.",
    "test_data": "Test data and environments: profiling and constraints, fake and synthetic "
    "data, environment data modes and bindings, and exporting the model's config.",
}

# The groups a sip session is always listed.
SIP_GROUPS = frozenset({"find", "describe_tables", "query", "metrics"})

# Each tool's group, by tool name.
GROUP_OF: dict[str, str] = {
    # find
    CATALOG_TOOL: "find",
    "describe_role": "find",
    "list_schemas": "find",
    "list_tables": "find",
    "search_catalog": "find",
    "search_terms": "find",
    "search_reference_library": "find",
    "list_commands": "find",
    "meta_resolve": "find",
    "generate_explore_queries": "find",
    "list_native_tables": "find",
    "list_glossary_terms": "find",
    # describe_tables
    "describe_table": "describe_tables",
    "graphql_field_names": "describe_tables",
    "describe_native_table": "describe_tables",
    # query
    "run_sql": "query",
    "explain_sql": "query",
    # metrics
    "list_metrics": "metrics",
    "query_metric": "metrics",
    # statistics
    "describe": "statistics",
    "correlate": "statistics",
    "regress": "statistics",
    "compare": "statistics",
    "forecast": "statistics",
    "jev_evaluate": "statistics",
    # graph
    "run_cypher": "graph",
    "render_graph": "graph",
    "clear_graph_session": "graph",
    "cypher_field_names": "graph",
    # glossary
    "create_glossary_term": "glossary",
    "update_glossary_term": "glossary",
    "approve_glossary_term": "glossary",
    "deprecate_glossary_term": "glossary",
    "retire_glossary_term": "glossary",
    "restore_glossary_term": "glossary",
    "list_glossary_findings": "glossary",
    "delete_glossary_term": "glossary",
    "add_glossary_term_edge": "glossary",
    "remove_glossary_term_edge": "glossary",
    "ground_glossary_term_to_table": "glossary",
    "unground_glossary_term_from_table": "glossary",
    "tag_glossary_term": "glossary",
    "untag_glossary_term": "glossary",
    # data_products
    "list_data_products": "data_products",
    "create_data_product": "data_products",
    "set_data_product_members": "data_products",
    "delete_data_product": "data_products",
    "publish_data_product": "data_products",
    "deprecate_data_product": "data_products",
    "retire_data_product": "data_products",
    "restore_data_product": "data_products",
    "list_data_product_findings": "data_products",
    # metric_management
    "upsert_metric": "metric_management",
    "delete_metric": "metric_management",
    # proposals
    "propose_source": "proposals",
    "propose_table": "proposals",
    # test_data
    "find_table_id": "test_data",
    "list_profilers": "test_data",
    "set_table_profiler": "test_data",
    "run_profiler": "test_data",
    "run_table_profile": "test_data",
    "declare_table_profile": "test_data",
    "get_profile_run_as_declared": "test_data",
    "list_profile_runs": "test_data",
    "get_table_profile": "test_data",
    "list_profile_constraints": "test_data",
    "decide_profile_constraint": "test_data",
    "forget_profile_constraint": "test_data",
    "export_profile_constraint": "test_data",
    "list_profile_checks": "test_data",
    "create_drift_check": "test_data",
    "create_expectation_check": "test_data",
    "list_fake_kinds": "test_data",
    "get_table_fakes": "test_data",
    "set_column_fake": "test_data",
    "propose_fakes": "test_data",
    "list_synthetic_datasets": "test_data",
    "list_synthetic_profile_runs": "test_data",
    "define_synthetic_dataset": "test_data",
    "generate_synthetic_dataset": "test_data",
    "get_synthetic_report": "test_data",
    "drop_synthetic_dataset": "test_data",
    "get_environment_detail": "test_data",
    "set_environment_data": "test_data",
    "set_source_binding": "test_data",
    "recopy_environment_sources": "test_data",
    "get_environment_synthetic_plan": "test_data",
    "generate_environment_model": "test_data",
    "reset_environment_mutations": "test_data",
    "export_model_config": "test_data",
}


# The rights each tool's call requires, any one of which allows it (REQ-2246): the rights the
# admin route or tool body it runs checks, so the list a principal is given and what its calls
# are allowed agree. An empty set: the call needs no right; what it reads is governed by the role
# it acts as. ``check_registered`` refuses a server whose tools and this table differ.
_NONE: frozenset[str] = frozenset()
_GLOSSARY_READ = frozenset({"glossary_read"})
_GLOSSARY_RW = frozenset({"glossary_rw"})
_PRODUCT_READ = frozenset({"data_product_read"})
_PRODUCT_RW = frozenset({"data_product_rw"})
_TABLE_EDITOR = frozenset({"table_registration"})
# require_dq_read_request: dq_read, which dq_rw implies.
_DQ_READ = frozenset({"dq_read", "dq_rw"})
_DQ_RW = frozenset({"dq_rw"})
# require_hiding_editor_request: the table editor's right or any governance right.
_HIDING_EDITOR = frozenset(
    {"table_registration", "column_grant", "access_config", "masking_config", "sensitive_data"}
)
_ENV_MANAGEMENT = frozenset({"environment_management"})
# synthetic_router._require_read: environment_management, or environment_switch held by the
# environment's owner (whose ownership the call decides).
_SYNTHETIC_READ = frozenset({"environment_management", "environment_switch"})
_ENV_DATA = frozenset({"environment_data"})

RIGHTS_OF: dict[str, frozenset[str]] = {
    **{t: _NONE for t, g in GROUP_OF.items() if g in SIP_GROUPS | {"statistics", "graph"}},
    "list_glossary_terms": _GLOSSARY_READ,
    # glossary
    "create_glossary_term": _GLOSSARY_RW,
    "update_glossary_term": _GLOSSARY_RW,
    "approve_glossary_term": _GLOSSARY_RW,
    "deprecate_glossary_term": _GLOSSARY_RW,
    "retire_glossary_term": _GLOSSARY_RW,
    "restore_glossary_term": _GLOSSARY_RW,
    "list_glossary_findings": _GLOSSARY_READ,
    "delete_glossary_term": _GLOSSARY_RW,
    "add_glossary_term_edge": _GLOSSARY_RW,
    "remove_glossary_term_edge": _GLOSSARY_RW,
    "ground_glossary_term_to_table": _GLOSSARY_RW,
    "unground_glossary_term_from_table": _GLOSSARY_RW,
    "tag_glossary_term": _GLOSSARY_RW,
    "untag_glossary_term": _GLOSSARY_RW,
    # data_products
    "list_data_products": _PRODUCT_READ,
    "create_data_product": _PRODUCT_RW,
    "set_data_product_members": _PRODUCT_RW,
    "delete_data_product": _PRODUCT_RW,
    "publish_data_product": _PRODUCT_RW,
    "deprecate_data_product": _PRODUCT_RW,
    "retire_data_product": _PRODUCT_RW,
    "restore_data_product": _PRODUCT_RW,
    "list_data_product_findings": _PRODUCT_READ,
    # metric_management
    "upsert_metric": _TABLE_EDITOR,
    "delete_metric": _TABLE_EDITOR,
    # proposals: anyone may propose; a person with the right reviews it
    "propose_source": _NONE,
    "propose_table": _NONE,
    # test_data
    "find_table_id": _TABLE_EDITOR,
    "list_profilers": _DQ_READ,
    "set_table_profiler": _TABLE_EDITOR,
    "run_profiler": frozenset({"source_registration"}),
    "run_table_profile": _TABLE_EDITOR,
    "declare_table_profile": _TABLE_EDITOR,
    "get_profile_run_as_declared": _DQ_READ,
    "list_profile_runs": _DQ_READ,
    "get_table_profile": _DQ_READ,
    "list_profile_constraints": _DQ_READ,
    "decide_profile_constraint": _DQ_RW,
    "forget_profile_constraint": _DQ_RW,
    "export_profile_constraint": _DQ_RW,
    "list_profile_checks": _DQ_READ,
    "create_drift_check": _DQ_RW,
    "create_expectation_check": _DQ_RW,
    "list_fake_kinds": _HIDING_EDITOR,
    "get_table_fakes": _TABLE_EDITOR,
    "set_column_fake": _HIDING_EDITOR,
    "propose_fakes": _TABLE_EDITOR,
    "list_synthetic_datasets": _SYNTHETIC_READ,
    "list_synthetic_profile_runs": _SYNTHETIC_READ,
    "define_synthetic_dataset": _ENV_MANAGEMENT,
    "generate_synthetic_dataset": _ENV_MANAGEMENT,
    "get_synthetic_report": _SYNTHETIC_READ,
    "drop_synthetic_dataset": _ENV_MANAGEMENT,
    "get_environment_detail": _ENV_MANAGEMENT,
    "set_environment_data": _ENV_DATA,
    "set_source_binding": _ENV_DATA,
    "recopy_environment_sources": _ENV_DATA,
    "get_environment_synthetic_plan": _ENV_MANAGEMENT,
    "generate_environment_model": _ENV_DATA,
    "reset_environment_mutations": _ENV_DATA,
    # orgs_router.export_org_config: the org admin's user_management, in the org acted in
    "export_model_config": frozenset({"user_management"}),
}


def offered(name: str, rights: set[str] | frozenset[str]) -> bool:
    """Whether the tool ``name`` is offered to a principal whose roles carry ``rights``."""
    needs = RIGHTS_OF[name]
    return not needs or bool(needs & rights)


def check_registered(registered: set[str]) -> None:
    """Refuse a server whose tools and these tables differ: a tool with no group or no rights
    entry, a group or rights entry naming a tool that is not registered, a group with no purpose,
    or a right that does not exist."""
    unknown_groups = sorted({g for g in GROUP_OF.values() if g not in PURPOSES})
    if unknown_groups:
        raise RuntimeError(f"MCP tool groups with no purpose: {unknown_groups}")
    if not SIP_GROUPS <= PURPOSES.keys():
        raise RuntimeError(f"sip groups not declared: {sorted(SIP_GROUPS - PURPOSES.keys())}")
    ungrouped = sorted(registered - GROUP_OF.keys())
    if ungrouped:
        raise RuntimeError(f"MCP tools with no group (tool_groups.GROUP_OF): {ungrouped}")
    missing = sorted(GROUP_OF.keys() - registered)
    if missing:
        raise RuntimeError(f"MCP tool groups name tools that are not registered: {missing}")
    unrighted = sorted(registered - RIGHTS_OF.keys())
    if unrighted:
        raise RuntimeError(f"MCP tools with no rights entry (tool_groups.RIGHTS_OF): {unrighted}")
    stale = sorted(RIGHTS_OF.keys() - registered)
    if stale:
        raise RuntimeError(f"MCP tool rights name tools that are not registered: {stale}")
    from provisa.security.rights import Capability

    named = {r for need in RIGHTS_OF.values() for r in need}
    unknown_rights = sorted(named - {c.value for c in Capability})
    if unknown_rights:
        raise RuntimeError(f"MCP tool rights name rights that do not exist: {unknown_rights}")


def check_command_names(commands: list[dict]) -> None:
    """Refuse a command whose MCP command binding names one of the server's own tools (REQ-2246:
    a command's MCP binding names its tool), naming both, as a binding shadowing a language's
    built-in is refused (REQ-2168)."""
    from provisa.core.command_bindings import CommandBindingRefused, binding_name

    for command in commands:
        name = binding_name(command, "mcp")
        if name is not None and name in GROUP_OF:
            raise CommandBindingRefused(
                f"command {command['name']!r}: its mcp command binding {name!r} would shadow "
                f"the built-in tool {name}"
            )


def mode_named(word: str | None) -> str:
    """The mode a connection asks for: the address's word, or the operator setting's when the
    address names none. A word that is not a mode is refused by name."""
    if word is None:
        from provisa.core import settings_registry

        word = settings_registry.value("mcp.tools")
    if word not in MODES:
        raise ValueError(f"unknown tool mode {word!r}: the modes are {' and '.join(MODES)}")
    return word


# The groups a sip session added, by its MCP session. Weak: an ended session takes its groups with it.
_added: WeakKeyDictionary[Any, set[str]] = WeakKeyDictionary()


def added_groups(session: Any) -> frozenset[str]:
    return frozenset(_added.get(session, ()))


def listed(mode: str, session: Any, name: str) -> bool:
    """Whether the tool ``name`` is listed to ``session`` in ``mode``."""
    if mode == FULL:
        return True
    group = GROUP_OF[name]
    return group in SIP_GROUPS or group in _added.get(session, ())


def answer(mode: str, session: Any, add: str | None, registered: set[str]) -> dict[str, Any]:
    """What ``tool_groups`` answers: the groups a sip session can add, each with its purpose, its
    tools (of ``registered``: the tools offered to the session's principal) and whether it is
    listed now; a group none of whose tools is offered is not named. With ``add``, that group is
    added to the session first."""
    if add is not None:
        if add not in PURPOSES:
            raise ValueError(f"unknown tool group {add!r}: the groups are {sorted(PURPOSES)}")
        if add not in SIP_GROUPS:
            _added.setdefault(session, set()).add(add)
    now = added_groups(session)
    groups = [
        {
            "group": group,
            "purpose": purpose,
            "tools": sorted(t for t, g in GROUP_OF.items() if g == group and t in registered),
            "listed": mode == FULL or group in now,
        }
        for group, purpose in PURPOSES.items()
        if group not in SIP_GROUPS
    ]
    groups = [g for g in groups if g["tools"]]
    result: dict[str, Any] = {"mode": mode, "groups": groups}
    if add is not None:
        result["added"] = add
    return result


# Where the transport middleware puts the connection's mode for the MCP handlers: the HTTP
# request's state (Starlette's ``request.state``), which each MCP request carries.
STATE_KEY = "provisa_mcp_tools"

CATALOG_DESCRIPTION = (
    "The other groups of tools this server has, each with what it is for and whether it is "
    "listed to you now. Name a group in ``add`` to add its tools to your tool list for this "
    "session."
)


def mode_of_address(query_string: bytes) -> str:
    """The mode the MCP address's query string names (``?tools=sip``), or the setting's when it
    names none. A word that is not a mode, or two words, are refused."""
    from urllib.parse import parse_qs

    words = parse_qs(query_string.decode("latin-1"), keep_blank_values=True).get(MODE_PARAMETER)
    if words is not None and len(words) > 1:
        raise ValueError(
            f"the MCP address names {MODE_PARAMETER} {len(words)} times: name one mode"
        )
    return mode_named(words[0] if words else None)
