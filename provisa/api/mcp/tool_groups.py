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

# Tools registered only under a condition (jev_evaluate: a Jev credential is configured). A
# server built without it still holds every other tool of this table.
CONDITIONAL = frozenset({"jev_evaluate"})


def check_registered(registered: set[str]) -> None:
    """Refuse a server whose tools and this table differ: a tool with no group, a group naming a
    tool that is not registered, or a group with no purpose."""
    unknown_groups = sorted({g for g in GROUP_OF.values() if g not in PURPOSES})
    if unknown_groups:
        raise RuntimeError(f"MCP tool groups with no purpose: {unknown_groups}")
    if not SIP_GROUPS <= PURPOSES.keys():
        raise RuntimeError(f"sip groups not declared: {sorted(SIP_GROUPS - PURPOSES.keys())}")
    ungrouped = sorted(registered - GROUP_OF.keys())
    if ungrouped:
        raise RuntimeError(f"MCP tools with no group (tool_groups.GROUP_OF): {ungrouped}")
    missing = sorted(GROUP_OF.keys() - registered - CONDITIONAL)
    if missing:
        raise RuntimeError(f"MCP tool groups name tools that are not registered: {missing}")


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
    tools (of ``registered``) and whether it is listed now; with ``add``, that group is added to
    the session first."""
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
