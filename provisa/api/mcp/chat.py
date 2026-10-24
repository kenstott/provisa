# Copyright (c) 2026 Kenneth Stott
# Canary: 794c8bae-86ac-4f4f-bba9-8d6b12bacb96
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""LLM chat agent over the MCP tools (REQ-1008), plus UI-actuation tools (REQ-1795).

A Claude model drives the same governed MCP tools an external agent would use —
list_schemas / list_tables / describe_table / run_sql / explain_sql /
search_catalog — via a manual tool-use loop (Anthropic Messages API). Every tool
runs under the caller's fixed role (never chosen by the model), so the agent is
bound by the same domain access + governance as any other client.

The loop yields discrete events (assistant text, tool_use, tool_result, done,
error) that the endpoint streams to the browser as SSE, and an artifact -- a picture
render_graph drew, a file run_sql delivered -- that the chat panel shows with its download
and that never enters the model's context.

CLIENT TOOLS (REQ-1795): a second tool category — `navigate` and admin-mutation
dispatch — that only make sense in the browser (router, Apollo client, confirm
dialogs). The server never executes these. When the model calls one, the loop
stops mid-turn and yields `awaiting_client_tools` carrying the verbatim assistant
content plus any ALREADY-computed server tool_results from the same turn. The
frontend executes the pending client tool(s) itself, appends the assistant
message and a matching tool_result message to its own history, and POSTs the
whole history back to /admin/mcp/chat to resume — the endpoint is already
stateless per-request (the caller resends full history each time), so resuming
needs no new server-side session state."""

# complexity-gate: allow-ble=1 reason="the agent loop reports any tool-execution failure back to the
# model as an is_error tool_result (so it can recover) instead of aborting the chat — a broad catch is
# the correct behaviour for an arbitrary tool call, and the error text is surfaced, never swallowed"

from __future__ import annotations

import json
import logging
import re
from collections.abc import AsyncIterator
from typing import Any

from provisa.api.mcp import admin_api, model_tool_specs, polly_usage
from provisa.api.mcp import tools as mcp_tools
from provisa.api.mcp.sql_quoting import quote_identifiers

# Tool schemas mirror the MCP tools, minus `role` — the role is pinned by the endpoint from the
# caller's identity and injected at execution, so the model can never select or escalate it.
_TOOLS: list[Any] = [
    {
        "name": "list_schemas",
        "description": "List catalog schemas with description and table count.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "list_tables",
        "description": "List tables in a schema with description and column count.",
        "input_schema": {
            "type": "object",
            "properties": {"schema": {"type": "string"}},
            "required": ["schema"],
        },
    },
    {
        "name": "describe_table",
        "description": "Describe a table: columns (name, type, description) and foreign keys.",
        "input_schema": {
            "type": "object",
            "properties": {"schema": {"type": "string"}, "table": {"type": "string"}},
            "required": ["schema", "table"],
        },
    },
    {
        "name": "graphql_field_names",
        "description": (
            "The REAL field names a GraphQL query against /query must use for "
            "this table and its columns — NOT a guessed transform of describe_table's SQL-plane "
            "names. The two planes apply DIFFERENT naming conventions and the table field "
            "additionally gets a domain-uniqueness prefix the compiled schema alone knows (e.g. "
            "registered table `iris_iris` in domain `shelter` can be GraphQL field `s__irisIris`, "
            "not `irisIris`). ALWAYS call this before writing a GraphQL query for a table; never "
            "assume camelCase-plus-prefix yourself — guessing wrong fails schema validation. Also "
            "returns the real gRPC method names (grpc_query_method/grpc_aggregate_method) for "
            "this same table — gRPC's names are a deterministic re-casing of this same field, "
            "always call this (not a guess) before a gRPC query too. also returns "
            "jsonapi_path/openapi_path — JSON:API and OpenAPI route on the table's RAW domain id, "
            "not the sql-safe schema name `schema`/`table` are — reassembling "
            "`/{schema}/{table}` yourself 404s for any domain id with a hyphen or space; always "
            "call this and use jsonapi_path/openapi_path verbatim instead."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"schema": {"type": "string"}, "table": {"type": "string"}},
            "required": ["schema", "table"],
        },
    },
    {
        "name": "cypher_field_names",
        "description": (
            "The REAL Cypher node label, id property, and column property names for "
            "this table — NOT a guessed PascalCase-plus-domain-prefix transform. A guessed label "
            "can collide with a different table's in another domain and MATCH the wrong node "
            "type silently, or simply not exist. ALWAYS call this before writing a Cypher query "
            "for a table. Returns node_pattern, the label as a MATCH writes it (unquoted), and "
            "relationships, the table's typed, directed relationship patterns."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"schema": {"type": "string"}, "table": {"type": "string"}},
            "required": ["schema", "table"],
        },
    },
    {
        "name": "generate_explore_queries",
        "description": (
            "Generate ready-to-run queries for ALL NINE Explore surfaces (sql, graphql, cypher, "
            "grpc, jsonapi, openapi, nl, python, notebook) from one natural-language question -- "
            "the "
            "SAME pipeline the NL Explore page runs, with its own LLM call resolving table "
            "selection, joins, and aggregation shape. Use this instead of graphql_field_names/"
            "cypher_field_names plus hand-built SQL for anything beyond a flat single-table "
            "browse: aggregation, GROUP BY, a business-term filter ('top customers by revenue'), "
            "or a question spanning more than one table. graphql_field_names/cypher_field_names "
            "stay the right, cheaper choice only for a plain single-table query with no "
            "aggregation. Returns {target: {query, error}} per surface — a target with `error` "
            "set could not answer this question there; say so plainly rather than guessing a "
            "query for it. Every other target's `query` is the exact, complete value to "
            "navigate with verbatim (for jsonapi/openapi it IS the full deep-link path/method, "
            "for grpc it IS the exact rpc call signature) — never rewrite, simplify, or "
            "re-derive it. nl's query is the question for /nl, python's is the script for "
            "/python (state.code), and notebook has `cells` for /notebook (state.cells)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"question": {"type": "string"}},
            "required": ["question"],
        },
    },
    {
        "name": "search_catalog",
        "description": (
            "Semantically search the catalog for datasets matching a natural-language query. "
            "Returns the best-matching table branches. Use when the table list is too large to scan."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "k": {"type": "integer", "description": "max results (default 5)"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "run_sql",
        "description": (
            "Execute SQL through the governed pipeline; returns row-capped JSON rows. Set "
            "file_format only when the user asked for the data as a file (CSV or Parquet): the "
            "result is then also delivered as a file the user downloads in the chat, and you "
            "still get its first rows. A result with more rows than the cap, asked for without a "
            "limit, is also delivered as a CSV file the user can download, alongside the first "
            "rows."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string"},
                "limit": {"type": "integer"},
                "offset": {"type": "integer"},
                "file_format": {"type": "string", "enum": ["csv", "parquet"]},
            },
            "required": ["sql"],
        },
    },
    {
        "name": "render_graph",
        "description": (
            "Draw a Cypher read as a picture IN THE CHAT: runs the statement under your role, "
            "draws its nodes and relationships the way the Graph page draws them, and shows the "
            "PNG to the user with a Download button and an 'Open on Graph page' button that runs "
            "the same statement there. Call cypher_field_names first for the real labels. The "
            "statement must bind and return the relationships along its path. Returns the counts "
            "of nodes and relationships drawn."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "cypher": {"type": "string"},
                "layout": {"type": "string", "enum": list(mcp_tools.GRAPH_LAYOUTS)},
            },
            "required": ["cypher"],
        },
    },
    {
        "name": "explain_sql",
        "description": "Validate and govern a query without executing it; confirms it plans cleanly for your role.",
        "input_schema": {
            "type": "object",
            "properties": {"sql": {"type": "string"}},
            "required": ["sql"],
        },
    },
    # REQ-2114: the admin API the UI uses, read and run as the user.
    {
        "name": "admin_schema",
        "description": (
            "Read the admin GraphQL schema the UI uses: with kind 'query' or 'mutation', its root "
            "fields as SDL signatures and descriptions, narrowed by `search` (a word in the name "
            "or description); with `type`, one input, object or enum type in full. Read this "
            "before admin_query or admin_mutation, so the operation you write is one the schema "
            "has, with its real argument and input names."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "kind": {"type": "string", "enum": ["query", "mutation"]},
                "search": {"type": "string"},
                "type": {"type": "string", "description": "A type name, e.g. SourceInput."},
            },
        },
    },
    {
        "name": "admin_query",
        "description": (
            "Run an admin GraphQL query as the user, through the UI's own API, to find what "
            "exists: sources, tables, relationships, roles, settings, anything the admin pages "
            "show. Queries only; a change is an admin_mutation, which the user confirms."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "A GraphQL query document."},
                "variables": {"type": "object"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "admin_rest_routes",
        "description": (
            "List the REST routes the UI's admin screens call for what is not a GraphQL mutation "
            "-- a file upload, a settings change, the organization folder's encryption, a code "
            "archive, relationship candidates, ontology edits and the like -- each with its "
            "method, path, what it does and its body. Narrow it with `search`. Reach one with "
            "admin_request."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"search": {"type": "string"}},
        },
    },
    {
        "name": "discover_relationships",
        "description": (
            "Run the Relationships page's discovery for one registered table, as the user, and "
            "list the relationship candidates it found (each with its id, the columns it would "
            "join, its confidence and why). Use it for 'create any relationships to <table>'; "
            "accept the candidates the user picks with admin_request POST "
            "/admin/discover/candidates/{id}/accept."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "schema": {"type": "string", "description": "The table's domain or schema."},
                "table": {"type": "string"},
            },
            "required": ["schema", "table"],
        },
    },
    {
        "name": "search_govdata_subjects",
        "description": (
            "Check GovData (askamerica) FIRST for a topic-matching source, before web_search or "
            "proposing a createSource — GovData and Kaggle are this org's subscription "
            "sources. Matches a free-text topic (e.g. 'inflation', 'unemployment', 'crime "
            "rates') against GovData's real schema/table catalog and reports whether each match's "
            "subject is already subscribed for this org."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
        },
    },
    {
        "name": "search_kaggle_datasets",
        "description": (
            "Check Kaggle SECOND (after search_govdata_subjects) for a topic-matching "
            "public dataset, before web_search or proposing a createSource. Reads the Kaggle API "
            "token from the operator setting 'Kaggle API token' — never a raw token argument and "
            "never a secret name you choose or ask the user to choose. If this tool errors "
            "because the token cannot be read, tell the user to enter it in that setting (Admin "
            "> Settings > Security): the token itself, or a reference to a secret they created, "
            "then retry."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
        },
    },
    {
        "name": "list_native_tables",
        "description": (
            "The REAL table names of a source, introspected directly from it — NOT "
            "the governed catalog list_tables/search_catalog read, which is EMPTY for a source "
            "with nothing registered on it yet (a source you just created, most commonly). "
            "REQUIRED before a registerTable for such a source: guessing a table_name instead "
            "means registerTable's own validation refuses it. schema_name is source-type-specific (e.g. a 'files' "
            "source's schema is its own id with hyphens turned to underscores) — if unsure, "
            "call with schema_name='public' first; the tool tells you the real one either way."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "source_id": {"type": "string"},
                "schema_name": {"type": "string", "description": "Default 'public'."},
            },
            "required": ["source_id"],
        },
    },
    {
        "name": "describe_native_table",
        "description": (
            "The REAL columns (name, data_type) of one table of a source, introspected "
            "directly from it. Call list_native_tables first if you don't already have the exact "
            "table_name from it. Building registerTable's columns list from "
            "this result is what makes registration actually succeed — a guessed columns list "
            "gets refused by the server's own validation, every time."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "source_id": {"type": "string"},
                "schema_name": {"type": "string"},
                "table_name": {"type": "string"},
            },
            "required": ["source_id", "schema_name", "table_name"],
        },
    },
    {
        "name": "list_glossary_terms",
        "description": (
            "Search/list the org's ontology terms. Each result's `status` is draft (not "
            "admitted: undefined, or grounded to no real column directly or through edges), "
            "live (admitted), deprecated or retired. Use this before create_glossary_term to "
            "avoid creating a duplicate."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "q": {"type": "string", "description": "Optional search string."},
                "include_deprecated": {"type": "boolean"},
            },
        },
    },
    {
        "name": "list_glossary_findings",
        "description": (
            "What a curator should look at in the ontology: each term with its findings, each "
            "{kind, message, params}. Kinds: unlinked_tables (a class's tables not all joined "
            "as one entity), class_and_attribute (a class holding non-key columns, or a table "
            "left without a class), tag_wrong_kind (a term tag on a term that is not a "
            "property), retirement_blocked (due for retirement but still holding data) and "
            "no_data (a concept that reaches no data). A plain draft term is not a finding."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "create_glossary_term",
        "description": (
            "Create a new ontology term (name + definition). It starts a draft — "
            "there is no separate flag to mark it finalized/live. A draft term becomes live "
            "either automatically (a real column gets registered and lands on it) or by calling "
            "add_glossary_term_edge to connect it to a term that is already live. If the org runs "
            "multi-domain, pass at least one domain id."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "definition": {"type": "string"},
                "domains": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["name"],
        },
    },
    {
        "name": "update_glossary_term",
        "description": (
            "Rename a ontology term, change its definition, or exclude it from exports. Only "
            "the fields you pass are changed. To withdraw a term that carries curator work (a "
            "definition, a relationship, or an expert), retire it with retire_glossary_term "
            "rather than deleting it."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "term_id": {"type": "integer"},
                "name": {"type": "string"},
                "definition": {"type": "string"},
                "export_excluded": {"type": "boolean"},
            },
            "required": ["term_id"],
        },
    },
    {
        "name": "deprecate_glossary_term",
        "description": (
            "Deprecate a live ontology term, with a reason and an optional planned retirement "
            "date (ISO, not in the past). It stays in service until then and is retired on it."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "term_id": {"type": "integer"},
                "reason": {"type": "string"},
                "retire_on": {"type": "string"},
            },
            "required": ["term_id", "reason"],
        },
    },
    {
        "name": "retire_glossary_term",
        "description": (
            "Retire an ontology term, with a reason. Its columns or tables move to the successor "
            "term id (required when it has any), which gets the REPLACES edge to it. A move that "
            "would take pii off a column is refused unless confirm_tag_loss names the tag or "
            "carry_tags puts it on the successor."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "term_id": {"type": "integer"},
                "reason": {"type": "string"},
                "successor": {"type": "integer"},
                "confirm_tag_loss": {"type": "array", "items": {"type": "string"}},
                "carry_tags": {"type": "boolean"},
            },
            "required": ["term_id", "reason"],
        },
    },
    {
        "name": "restore_glossary_term",
        "description": "Restore a retired or deprecated ontology term.",
        "input_schema": {
            "type": "object",
            "properties": {"term_id": {"type": "integer"}},
            "required": ["term_id"],
        },
    },
    {
        "name": "delete_glossary_term",
        "description": (
            "Permanently delete a ontology term. Irreversible — always confirm with "
            "the user first via present_choice (mode='yes_no'), the same as any other "
            "irreversible action, before calling this."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"term_id": {"type": "integer"}},
            "required": ["term_id"],
        },
    },
    {
        "name": "add_glossary_term_edge",
        "description": (
            "Connect two ontology terms with a typed relationship (e.g. 'broader', "
            "'narrower', 'synonym' — check list_glossary_terms/existing edges for the vocabulary "
            "this org already uses before inventing a new rel_type). This is also the mechanism "
            "for finalizing a draft term without waiting for a column: connect it to an "
            "already-live term and it becomes live through that edge."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "term_id": {"type": "integer"},
                "to_term_id": {"type": "integer"},
                "rel_type": {"type": "string"},
            },
            "required": ["term_id", "to_term_id", "rel_type"],
        },
    },
    {
        "name": "remove_glossary_term_edge",
        "description": "remove a relationship edge between two ontology terms.",
        "input_schema": {
            "type": "object",
            "properties": {
                "term_id": {"type": "integer"},
                "to_term_id": {"type": "integer"},
                "rel_type": {"type": "string"},
            },
            "required": ["term_id", "to_term_id", "rel_type"],
        },
    },
    {
        "name": "ground_glossary_term_to_table",
        "description": (
            "Make an ontology term the class term of a registered table or view: each of the "
            "table's rows is an instance of what the term names. The table must have a primary "
            "key, and a table has one class term — one another term already grounds is refused, "
            "naming that term."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"term_id": {"type": "integer"}, "table_id": {"type": "integer"}},
            "required": ["term_id", "table_id"],
        },
    },
    {
        "name": "unground_glossary_term_from_table",
        "description": "withdraw an ontology term as a table's class term.",
        "input_schema": {
            "type": "object",
            "properties": {"term_id": {"type": "integer"}, "table_id": {"type": "integer"}},
            "required": ["term_id", "table_id"],
        },
    },
    {
        "name": "tag_glossary_term",
        "description": (
            "Put a tag (pii) on an ontology term that is a Property, so every column the term "
            "holds, now and later, carries it; refused for a Class or a Concept. pii needs a "
            "reason. Says how many columns it covers."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "term_id": {"type": "integer"},
                "tag_id": {"type": "string"},
                "reason": {"type": "string"},
            },
            "required": ["term_id", "tag_id"],
        },
    },
    {
        "name": "untag_glossary_term",
        "description": "Take a tag off an ontology term; its columns no longer carry it.",
        "input_schema": {
            "type": "object",
            "properties": {"term_id": {"type": "integer"}, "tag_id": {"type": "string"}},
            "required": ["term_id", "tag_id"],
        },
    },
    {
        "name": "list_data_products",
        "description": "list the org's data products (id, domain, name, purpose, owner/team role, status, etc.).",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "create_data_product",
        "description": (
            "Create or replace a data product by id — an id that already exists is "
            "overwritten in full, so call list_data_products first if you mean to change only "
            "some fields of an existing one. `domain_id` must be a real, existing domain."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "domain_id": {"type": "string"},
                "name": {"type": "string"},
                "owner_role": {"type": "string"},
                "team_role": {"type": "string"},
                "purpose": {"type": "string"},
                "limitations": {"type": "string"},
                "usage": {"type": "string"},
                "version": {"type": "string"},
                "status": {"type": "string"},
                "sla": {"type": "string"},
                "support": {"type": "string"},
            },
            "required": ["id", "domain_id", "name"],
        },
    },
    {
        "name": "delete_data_product",
        "description": (
            "Delete a data product by id. Irreversible — always confirm with the user "
            "first (present_choice, mode='yes_no')."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"id": {"type": "string"}},
            "required": ["id"],
        },
    },
    {
        "name": "upsert_metric",
        "description": (
            "Create or replace a governed metric by name — a name that already exists "
            "is overwritten in full. `expression` is an aggregate ANSI-SQL expression over "
            'semantic table.column references (e.g. "SUM(orders.amount) - SUM(orders.refunds)") '
            "— must parse under sqlglot and contain at least one aggregate function, or this "
            "fails. Fill in `ai_context` with what the metric MEANS and when to use it, not just "
            "a repeat of the expression — it's the definition text other agents (and you, later) "
            "read to select this metric by meaning. `visible_to` defaults to every role if "
            "omitted."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "expression": {"type": "string"},
                "datatype": {"type": "string"},
                "description": {"type": "string"},
                "ai_context": {"type": "string"},
                "visible_to": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["name", "expression"],
        },
    },
    {
        "name": "delete_metric",
        "description": (
            "Delete a governed metric by name. Irreversible — always confirm with the "
            "user first (present_choice, mode='yes_no')."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "required": ["name"],
        },
    },
    # REQ-1934/1494/1939/1919: the profiler, fakes, synthetic datasets and the config export.
    *model_tool_specs.SPECS,
]

# REQ-1795: executed in the BROWSER, never on the server — see the module docstring. Kept as a
# short, explicit, hand-maintained set for this first slice of the UI-actuation plumbing; a later
# pass generalizes admin-mutation dispatch to the full admin_schema via introspection instead of
# one hardcoded tool per mutation.
_CLIENT_TOOLS: list[Any] = [
    {
        "name": "navigate",
        "description": (
            "Navigate the browser to a route within the Provisa app. Common ones: /sources, "
            "/tables, /relationships, /admin/ontology, /schema. Explore's query surfaces "
            "(never guess a route for these — NOT /explore, which is this same chat, and NOT "
            "/admin/explorer, which doesn't exist): /query (GraphQL), /graph (Cypher), /sql, "
            "/grpc, /jsonapi, /openapi, /nl (Natural Language). When you have already prepared a "
            "query for one of these, pass it via `state` instead of telling the user the text and "
            "asking them to paste it — each of these six reads its own state key (see the "
            "`state` field below) the SAME way an in-app hyperlink to it already does, and runs "
            "it immediately when state.autoRun is true, landing it in the editor with no "
            "copy-paste needed. See the system prompt's section on the query surfaces for which "
            "tool to call first to get each surface's real field/name identifiers before "
            "building the query. Explore's Python tab (/python) and Notebook tab (/notebook) run "
            "Python in the user's browser as the user, reading Provisa through provisa_client "
            "(provisa_client.query(sql_or_graphql_or_cypher) returns a pandas DataFrame; pandas, "
            "numpy, scikit-learn, matplotlib and networkx are available). They are two different tabs, "
            "never one for the other: the Python tab (state.code: one script, written into the "
            "active script tab -- or into a new tab with state.newTab true, titled state.title "
            "when given, when the user asks for a new one; every run starts from nothing, so the "
            "script must stand alone) and the Notebook tab (state.cells: a "
            "list of cells added to the open Jupyter notebook and run in turn; notebook state "
            "carries over between cells). Use the tab the user names; only when they name "
            "neither, use the Python tab. Never call the Python tab a notebook. Code you write "
            "for either starts with `import provisa_client` and `provisa_client.sign_in()`; in the "
            "Notebook tab a cell using %%sql, %%graphql or %%cypher needs `%load_ext "
            "provisa_client` run first. A "
            "navigate to either with autoRun waits for the run and its result says what the "
            "code printed, its last value (a DataFrame as its columns, first rows and row count) "
            "and any error, so read the result before you answer."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "route": {"type": "string"},
                "state": {
                    "type": "object",
                    "description": (
                        'Optional, one key per route: /query -> {"query": "query Name { ... }"} '
                        '(GraphQL — name the operation), /graph -> {"query": "MATCH ..."} '
                        '(Cypher), and for /graph optionally "view": {"groupBy": "<property>" '
                        "(the Group by control: a node property name, null for none), "
                        '"collapseGroups": true|false, "layout": "force"|"hierarchy", '
                        '"maxNodes": <n>} -- the Graph page\'s own view controls; a /graph '
                        "navigate with a view and NO query sets them on the graph already on "
                        'screen, keeping its query. /sql -> {"sql": "SELECT ..."}, '
                        '/grpc -> {"grpcMethod": "QuerySIrisIris()"} (the exact rpc name from '
                        "graphql_field_names' grpc_query_method, plus a literal trailing '()' — "
                        "required by the gRPC page's parser), "
                        '/jsonapi -> {"jsonapiUrl": "..."} — use the jsonapi_path returned by '
                        "graphql_field_names verbatim, never `/{schema}/{table}` yourself (that "
                        "is the sql-safe name, not the raw domain id these routes key on), "
                        '/openapi -> {"openApiUrl": "..."} — same, use the openapi_path returned '
                        "by graphql_field_names verbatim. "
                        '/python -> {"code": "<Python script>"} (the Python tab; add "newTab": true and '
                        'optionally "title" to write it into a new script tab), '
                        '/notebook -> {"cells": ["<cell>", ...]} (the Notebook tab); the run\'s '
                        "output comes back in the tool result. Always also set "
                        '"autoRun": true alongside whichever key applies.'
                    ),
                },
                # REQ-2115: steps taken on the screen the route opens, in order, each shown.
                "actions": {
                    "type": "array",
                    "description": (
                        "Optional: steps on the screen the route opens, taken in order and shown "
                        "to the user as each happens -- only when the user asked to be shown, "
                        "demoed or taught. Each names its element by `target`: "
                        '{"testId": "<data-testid>"} or {"role": "button"|"textbox"|"combobox"|'
                        '"checkbox"|"radio"|"switch"|"option"|"tab"|"link", "name": "<its '
                        'accessible name>"}. Steps: {"do": "fill", "target": ..., "value": '
                        '"<text>"} types into a field; {"do": "choose", "target": ..., "option": '
                        '"<option name>"} picks an option in a select or dropdown; {"do": '
                        '"press", "target": ...} presses a button (add "changes": true when '
                        "pressing it changes something the screen does not mark; a form's Save "
                        'or action always waits for the user\'s yes); {"do": "show", '
                        '"target": ...} shows an element, a list scrolled through end to end. '
                        "The steps stop at the first that cannot be done, and the result says "
                        "which, naming an element the screen does not have."
                    ),
                    "items": {
                        "type": "object",
                        "properties": {
                            "do": {"type": "string", "enum": ["fill", "choose", "press", "show"]},
                            "target": {
                                "type": "object",
                                "properties": {
                                    "testId": {"type": "string"},
                                    "role": {"type": "string"},
                                    "name": {"type": "string"},
                                },
                            },
                            "value": {"type": "string"},
                            "option": {"type": "string"},
                            "changes": {"type": "boolean"},
                        },
                        "required": ["do", "target"],
                    },
                },
            },
            "required": ["route"],
        },
    },
    {
        "name": "refresh_mv",
        "description": (
            "Trigger a manual refresh of a materialized view. Shows the user a confirmation "
            "dialog before running — if they decline, the tool result says so."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"mv_id": {"type": "string"}},
            "required": ["mv_id"],
        },
    },
    {
        "name": "present_choice",
        "description": (
            "Ask the user to pick from a small set of options via a real UI widget — a multiple "
            "choice list, a checklist, or a yes/no — instead of asking them to type a free-text "
            "reply. Use this whenever you're offering the user a decision among a handful of "
            "concrete options (e.g. 'which source?', 'which of these tables?', 'proceed?') rather "
            "than describing the options in prose and waiting for them to type one back. "
            "mode='single' renders radio buttons and returns the one option string picked; "
            "mode='multi' renders checkboxes and returns an array of the options picked (zero or "
            "more); mode='yes_no' renders Yes/No buttons and returns a boolean, no 'options' "
            "needed. The tool result names exactly which option(s) — or true/false — were picked, or "
            "says the user closed the choice without picking (selected null)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "question": {"type": "string", "description": "What you're asking."},
                "mode": {"type": "string", "enum": ["single", "multi", "yes_no"]},
                "options": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Required for 'single'/'multi'; omit for 'yes_no'.",
                },
            },
            "required": ["question", "mode"],
        },
    },
    # REQ-2114: a change through the admin API, as the user, once the user confirms in the chat.
    {
        "name": "admin_mutation",
        "description": (
            "Make a change through the admin GraphQL API the UI uses, as the user: the mutation "
            "is shown to the user in the chat with `summary`, and only when they confirm there is "
            "it sent, by their own browser session, so it meets every capability check the UI "
            "meets and is refused where the UI would refuse it. Any admin mutation the schema "
            "has (read it with admin_schema first): createSource, registerTable, "
            "createRelationship and every other. The result is the mutation's own answer, or "
            "'Cancelled by the user.'"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "mutation": {"type": "string", "description": "A GraphQL mutation document."},
                "variables": {"type": "object"},
                "summary": {
                    "type": "string",
                    "description": "One plain sentence: what this change does, for the user.",
                },
            },
            "required": ["mutation", "summary"],
        },
    },
    {
        "name": "admin_request",
        "description": (
            "Call one of the UI's admin REST routes (see admin_rest_routes) as the user, from "
            "their own browser session. A GET runs at once; any other method is shown to the "
            "user with `summary` and sent only when they confirm in the chat. For a route that "
            "uploads a file, set `upload` true: the user chooses the file in the chat and its "
            "bytes are the body, and {filename} in the path stands for its name (e.g. "
            "/admin/orgs/<org>/code-uploads/archive?filename={filename})."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "method": {"type": "string", "enum": ["GET", "POST", "PUT", "PATCH", "DELETE"]},
                "path": {
                    "type": "string",
                    "description": "e.g. /admin/discover/candidates/7/accept",
                },
                "body": {
                    "type": "object",
                    "description": "The JSON body, or a form's other fields.",
                },
                "upload": {"type": "boolean"},
                "summary": {"type": "string"},
            },
            "required": ["method", "path", "summary"],
        },
    },
    # REQ-2115: what the user is looking at, read in their own browser session.
    {
        "name": "read_screen",
        "description": (
            "Read the screen the user is looking at, from their own browser session: the current "
            "route; the open query editor's text (SQL, GraphQL, Cypher, SPARQL or Python) with "
            "its language and the query tab open; the visible labelled form fields with their values "
            "(a password shows as dots, as on screen); and each result grid's columns and row "
            "count, never its rows. Call it whenever the user refers to 'this', 'the query' or "
            "anything on the screen, before you answer or act on it."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
]
_CLIENT_TOOL_NAMES = frozenset(t["name"] for t in _CLIENT_TOOLS)

# REQ-1796: Anthropic's own HOSTED tools — executed entirely on Anthropic's servers within the
# same model turn, never by us or the browser. A third tool category alongside _TOOLS (executed
# here) and _CLIENT_TOOLS (executed by the browser): the SDK returns their results as ordinary
# content blocks (server_tool_use / web_search_tool_result / web_fetch_tool_result) in the SAME
# response, so `resp.stop_reason` stays "end_turn" and the existing loop needs no new branch to
# execute them — only to surface a UI badge so the user can see a search/fetch happened.
# max_uses bounds cost/latency per call, matching run_sql's row cap elsewhere in this module.
_WEB_TOOLS: list[Any] = [
    {"type": "web_search_20260209", "name": "web_search", "max_uses": 5},
    {
        "type": "web_fetch_20260309",
        "name": "web_fetch",
        "max_uses": 5,
        "max_content_tokens": 50_000,
    },
]
_SERVER_HOSTED_TOOL_NAMES = frozenset(t["name"] for t in _WEB_TOOLS)

_SYSTEM = (
    'You are Polly, Provisa\'s assistant — the name is a nod to "poly" (many): many sources, many '
    "engines, one governed catalog. You answer questions about a governed data catalog, run SQL on "
    "the user's behalf, and can navigate the app or trigger admin actions, using ONLY the provided "
    "tools. Every tool call runs under the user's role and its domain access + row/column "
    "governance — you cannot see or change the role. A question about the organisation's own "
    "systems, traffic, logs, performance, costs or people is a catalog question until a search "
    "says otherwise: call search_catalog with the question's own words (and list_glossary_terms "
    "for its business terms) before answering, and never answer that you have no tool or data "
    "for something you have not searched the catalog for in this turn. A search that fails (no "
    "embedding model is enabled, say) is no answer either: find the data with list_schemas, "
    "list_tables and describe_table instead. Prefer search_catalog to "
    "find relevant "
    "datasets, describe_table to confirm structure, and explain_sql before run_sql on non-trivial "
    "queries. Never invent table, column, or schema names — discover them with the tools. If a "
    "tool returns an error or empty result, say so plainly. Keep answers concise and lead with the "
    "answer. Never narrate your steps ('Let me look at...', 'I'll check...') before or between "
    "tool calls: call the tools, then write the answer. A large result the user did not ask to "
    "see in full (dozens of rows or more) is summarized -- the count, how it splits, a few "
    "examples -- with an offer of the rest as a file (run_sql with file_format), or its rows "
    "are put in a table on the SQL page via navigate; never paste hundreds of lines into the "
    "chat. Answer in the chat: opening a page is in addition to the answer, never instead of "
    "it. State only what a tool result "
    "confirms: when a result says only that something was requested or navigated to, say what "
    "you requested, never that it worked or how it now looks. Text you write in the same message "
    "as a tool call is written before that call's result exists, and the user never sees it: "
    "never say there that anything is done, open, running, shown or fixed, and never put there "
    "anything the user needs -- report the outcome only after the result says so. "
    "Explain a failure only from its error text and the tool results: never invent a cause, a "
    "mechanism, or UI state (a search box, a filter, a stuck setting) the error does not name, "
    "and never tell the user to change something you have not seen. An error from Provisa's "
    "own machinery -- an engine, binder, planner or compiler error ('Execution failed', "
    "'Binder Error', an internal column such as _nf_query) -- is a Provisa error: say so, quote "
    "it, and offer a working way to the same answer (e.g. the same question in SQL over a "
    "registered view), never a step for the user to fix it. When the user refers to 'this', "
    "'the query', 'these results' or anything else on their screen, call read_screen first and "
    "work from what it returns; never say you cannot see the screen. When you're offering the user a decision among a handful of concrete options — which "
    "source, which table, proceed or not, pick some of these — call present_choice rather than "
    "listing options in prose and waiting for a typed reply; it's a real UI widget (multiple "
    "choice, checklist, or yes/no) and reads better than typing a number or a name back. Its "
    "`question` is all the user sees beside the options: put in it everything they need to "
    "choose, since text you write beside the call is not shown. Never "
    "offer a choice between a picture in the chat and the Graph page, or between either and a "
    "Graph page view: do all that was asked (see PICTURES AND FILES). Once a "
    "tool call succeeds and accomplishes what the user actually asked for, the task is done — "
    "report the outcome and stop. Never re-run a tool you already called successfully in this "
    "same turn 'to be sure' or 'to do it properly' — if you notice partway through that an "
    "earlier step in THIS turn was done correctly, that is not a reason to redo it; only redo a "
    "step that actually failed or returned something wrong.\n\n"
    "If asked to find, add, connect, or register a new data source, or a new table from an "
    "existing source: first check search_catalog/list_schemas to avoid proposing a duplicate. "
    "Then, for a topical data request (e.g. 'find me inflation data'), check this org's "
    "subscription sources BEFORE the open web: ALWAYS call BOTH "
    "search_govdata_subjects AND search_kaggle_datasets — never stop after the first one finds a "
    "match and ask the user whether to check the other; check both unconditionally, in the same "
    "turn, then present whichever option(s) you found (there may be one, both, or neither). "
    "search_kaggle_datasets reads the Kaggle API token from the operator setting 'Kaggle API "
    "token'. Never ask the user to paste a token into chat or to choose a secret name; if the "
    "tool errors because the token cannot be read, just tell them to enter it in that setting "
    "(Admin > Settings > Security), as the token itself or a reference to a secret they "
    "created, then retry. Only once GovData and Kaggle have both been checked and neither has a "
    "match, fall back to web_search/web_fetch to find real connection "
    "details (API base URL, auth scheme, official docs) for a brand-new external source. If more "
    "than one candidate was found (multiple GovData subjects, multiple Kaggle datasets, or a mix "
    "of both), you do not yet know which one the user wants — ask via present_choice (mode="
    "'single') listing each candidate, and WAIT for their answer. Then propose that ONE chosen "
    "candidate with admin_mutation (createSource; registerTable for a table). admin_mutation "
    "itself shows the user the change and waits for them to confirm it, so never ask a separate "
    "'shall I?' question first: that is asking the same thing twice. If exactly one candidate was "
    "found, propose it with admin_mutation right away. Never propose a candidate from a "
    "multi-candidate result the user has not picked from. "
    "For a GovData match, the 'source' is the subject/schema itself (say what subscribing would "
    "require if the org isn't subscribed yet); for a Kaggle match, propose type='files' with "
    'federation_hints_json set to \'{"kaggle_owner": "<owner>", "kaggle_ref": "<ref>"}\' '
    "(split the dataset's ref on its '/') and any placeholder for path — the server downloads the "
    "real files and overwrites path itself when the source is actually created, so "
    "there is no separate staging/download step to call here or anywhere else. ALWAYS use the "
    "tool rather than only describing the source in prose. "
    "A request naming ONE file by a fully resolved path -- a file mount path, an https:// URL or "
    "an s3:// URL to a single .csv or .parquet file -- is a CSV or Parquet source given its file "
    "by download: work out csv or parquet from the file's name (ask if it says neither), and "
    "propose type='csv' or 'parquet' with download_from set to that path, path left out, an id "
    "from the file's name, register_table_draft true and table_domain_id the domain the user "
    "named (ask which one if they named none) -- the server copies the file into the "
    "organization's folder and sets up its table as a draft when the source is created. A server "
    "of many organizations refuses a file mount path by name: offer an https:// or s3:// address "
    "there. A directory, a glob or several files is a files source instead (type='files', path "
    "set to that directory or glob), proposed and confirmed the same way, its tables registered "
    "from what the source offers (list_native_tables, then registerTable). "
    "If none of these tools can find "
    "something (an API key, "
    "a customer-specific hostname), say what's still needed rather than inventing it.\n\n"
    "THE ADMIN API: you act as the user through the same admin API the UI calls. "
    "Read what exists with admin_query (and the schema with admin_schema, before writing any "
    "operation, so its fields, arguments and input names are real). Make every change with "
    "admin_mutation: it shows the user the change and its one-sentence summary in the chat and "
    "sends it from their own session only when they confirm there, so it meets exactly the "
    "capability checks the UI meets. Where the user's roles do not allow it, the mutation is "
    "refused, or queued as a request for approval, exactly as on the admin page -- report its "
    "answer as it came. A UI action that is a REST route and not a mutation (a file upload, a "
    "setting, the organization folder's encryption, a code archive) is found with "
    "admin_rest_routes and reached with admin_request. 'Create any relationships to <table>': "
    "run discover_relationships for it, show the candidates with present_choice (mode 'multi'), "
    "and accept each the user picks with admin_request POST "
    "/admin/discover/candidates/{id}/accept. A source's createSource input: at minimum id and "
    "type; a CSV or Parquet source given its file by download sets download_from and leaves path "
    "out, and register_table_draft with table_domain_id sets its table up as a draft. A "
    "registerTable input: source_id, domain_id, schema_name, table_name and columns (each at "
    "least name and visible_to); never set alias -- the server derives the right SQL name. When "
    "the user asks to be shown, demoed or taught how something is done, use the screen instead "
    "(navigate with actions), never these tools.\n\n"
    "After an admin_mutation or admin_request completes (success, refusal or cancel), ALWAYS say "
    "so in a short final message; never end the turn silently right after a tool call. After a "
    "source is created, suggest the natural next step -- e.g. 'Would you like me to register a "
    "table from this new source now?' -- rather than waiting to be asked.\n\n"
    "search_catalog/list_tables/describe_table only ever see the already-registered GOVERNED "
    "catalog — for a source that has no tables registered on it yet (most commonly one you or the "
    "user just created), that catalog is empty and those tools will never find anything on it, no "
    "matter how you search. For a registerTable on such a source, call "
    "list_native_tables (then describe_native_table for the chosen table) instead — these read the "
    "source's own real, native schema directly. Never guess a table_name or columns list from the "
    "source's name/description alone; registerTable's own validation will refuse a guessed "
    "columns list outright, and the tool call will fail. "
    "THE PAGE-FIRST RULE: before performing an action that corresponds to a specific admin page (creating or "
    "registering a source/table, editing a ontology term, upserting/deleting a relationship, "
    "running a GraphQL query in Explorer, or anything else with a dedicated screen), call navigate "
    "to that page FIRST (e.g. /admin/ontology for any ontology edit), then perform the action — so "
    "the user sees, on their own screen, the same page you're acting on, not just a chat "
    "transcript. Do this every single time that kind of action comes up, not just the first time "
    "in a conversation — a later ontology edit needs its own navigate call exactly as much as the "
    "first one did. Do this even when the action's own tool doesn't require the user to be on that "
    "page to work.\n\n"
    "If you cannot complete what was asked — no tool covers it, every avenue you tried came back "
    "empty or erroring, or you're missing information only the user can supply — say so plainly and "
    "specifically (e.g. 'I wasn't able to do that — <what's missing>. Can you give me more detail "
    "and I'll try again?'). Never just stop or trail off without a closing message; the user should "
    "never have to guess whether you're still working or already gave up.\n\n"
    "Manage the glossary with list_glossary_terms/list_glossary_findings/create_glossary_term/"
    "update_glossary_"
    "term/delete_glossary_term/add_glossary_term_edge/remove_glossary_term_edge, and ground a term "
    "to a whole table with ground_glossary_term_to_table/unground_glossary_term_from_table, "
    "and mark a Property term pii with tag_glossary_term/untag_glossary_term, "
    "and change a term's status with deprecate_glossary_term/retire_glossary_term/"
    "restore_glossary_term (each takes a reason, except restore), "
    "directly — these "
    "carry their own capability check (glossary_rw) and fail loud if the caller lacks it, so they "
    "need no admin_mutation. Still always confirm with the user "
    "(present_choice, mode='yes_no') before delete_glossary_term specifically, since it's "
    "irreversible; create/update/edge actions are safe to do directly once asked. Check list_"
    "glossary_terms first to avoid creating a duplicate term, and remember 'finalizing' a term "
    "has no direct flag — it happens by grounding it (a real column, or add_glossary_term_edge to "
    "an already-live term); say this plainly if a user asks you to just 'mark it live'. Per "
    "the page-first rule above: navigate to /admin/ontology before calling any of these — every time, for "
    "every term you touch in the turn, not only the first.\n\n"
    "Manage data products with list_data_products/create_data_product/"
    "delete_data_product, and governed metrics with upsert_metric/delete_metric, directly — no "
    "admin_mutation needed, they carry their own capability check and fail loud if the "
    "caller lacks it (data_product_rw / table_registration). create_data_product and upsert_metric "
    "are both upserts by id/name — an existing one is silently overwritten in full, so call "
    "list_data_products or check the metric first if you mean to change only some fields, not "
    "replace it. A metric's `expression` must be an aggregate SQL expression over semantic "
    "table.column references and fails validation with no aggregate function in it — write a real "
    "`ai_context` (what it means, when to use it), not a restatement of the expression. Always "
    "confirm with the user (present_choice, mode='yes_no') before delete_data_product/delete_"
    "metric specifically, since they're irreversible. Per the page-first rule: navigate to /data-products or "
    "/metrics before calling any of these.\n\n" + model_tool_specs.SYSTEM_SECTION + "\n\n"
    "THE QUERY SURFACES: Provisa has NINE query surfaces under Explore, each its "
    "own route and each with its own exact deep-link format — never assume one surface's names or "
    "state shape transfer to another. Route + navigate `state` key per surface: GraphQL /query "
    "(state.query), Cypher /graph (state.query), SQL /sql (state.sql), gRPC /grpc "
    "(state.grpcMethod), JSON:API /jsonapi (state.jsonapiUrl), OpenAPI /openapi (state.openApiUrl), "
    "NL /nl (no state deep-link — type the question directly if asked to use the NL surface "
    "itself), the Python tab /python (state.code into the active script tab; state.newTab=true "
    "and state.title for a new one when the user asks) and the Notebook tab /notebook (state.cells) "
    "-- use the one the user names, the Python tab only when they name neither, never call the "
    "Python tab a notebook, and start the code with `import provisa_client` and "
    "`provisa_client.sign_in()`; the run's output, value and error come back in the navigate "
    "result. Always pass state.autoRun=true alongside so the query runs immediately instead of "
    "sitting unrun in the editor.\n"
    "For anything beyond browsing one table's plain columns — aggregation, GROUP BY, a "
    "business-term filter, or a question spanning more than one table — call "
    "generate_explore_queries with the user's question FIRST; it runs the same pipeline the NL "
    "page does and hands back a ready query per surface. Only fall back to the per-table lookups "
    "below for a plain single-table browse with no aggregation.\n"
    "- SQL: your existing schema/table/column names (from list_tables/describe_table) ARE the "
    "real SQL-plane names already — no extra lookup needed. Give a query a name when the surface "
    "supports it (GraphQL: `query Name { ... }` instead of an anonymous `{ ... }`) — good practice, "
    "and it makes the request traceable in logs.\n"
    "- GraphQL: call graphql_field_names for the table first (its SQL-plane name is usually "
    "different, e.g. a domain-uniqueness prefix like `s__` that can't be guessed). Name the "
    "query operation, e.g. `query GetIris { s__irisIris { ... } }`.\n"
    "- Cypher: call cypher_field_names for each table first (its node label and property names "
    "are a separate derivation from GraphQL's, not the same names). Write each label exactly as "
    "its node_pattern gives it -- a domain table's label is two labels, `(n:Shop:Orders)`, never "
    "backticked -- and relate nodes only along the typed, directed relationships it lists, e.g. "
    "`(o:Shop:Orders)-[:PLACED_BY]->(c:Shop:Customers)`. Write a pattern as one path, e.g. "
    "`(o:Shop:Orders)<-[:ON_ORDER]-(i:Shop:OrderItems)-[:OF_PRODUCT]->(p:Shop:Products)`, or one "
    "MATCH per path: comma-separated patterns and a bare `--` do not parse here, an untyped "
    "`-[]-` matches every relationship, and a join on columns in WHERE draws no relationship. "
    "A statement for the Graph page that matches a path binds every relationship on it and "
    "returns it with the nodes, e.g. `MATCH (l:Poems:Line)-[e:EVOKES]->(t:Poems:Theme) RETURN l, "
    "e, t`: returning only the nodes draws no edges, so the page can neither connect nor group "
    "them. A /graph navigate is checked before it reaches the page -- one that does not run, or "
    "returns a path's nodes without its relationships, is refused: correct the statement from "
    "the error and navigate again.\n"
    "- gRPC: call graphql_field_names for the table — it also returns grpc_query_method/"
    "grpc_aggregate_method, the real rpc name. state.grpcMethod is that name plus `()`, e.g. "
    '`"QuerySIrisIris()"` — this is deterministic, not a guess; never omit the trailing `()`, '
    "the gRPC page's own parser requires it.\n"
    "- JSON:API: call graphql_field_names for the table and use its jsonapi_path verbatim as "
    "state.jsonapiUrl. These routes key on the table's RAW domain id, not the sql-safe schema "
    "name describe_table/list_tables give you — reassembling `/{schema}/{table}` yourself 404s "
    "for any domain id containing a hyphen or space (reproduced live).\n"
    "- OpenAPI: same as JSON:API — call graphql_field_names and use its openapi_path verbatim as "
    "state.openApiUrl, never reassembled from the plain schema/table names.\n"
    "PICTURES AND FILES: asked to show, draw or visualise a graph or network, call render_graph: "
    "the picture appears in the chat with a Download button and an Open on Graph page button "
    "that runs the same statement there, so never ask whether the user meant the chat or the "
    "Graph page -- they get both. Asked for a graph together with a Graph page view (grouped or "
    "collapsed by an attribute, a layout), do both in the same turn: render_graph, and navigate "
    "to /graph with the statement and state.view. Asked to open the Graph page, or to explore the graph there, "
    "navigate to /graph instead. Asked for data as a file (CSV or Parquet), call run_sql with "
    "file_format. A run_sql result that carries `redirect` was delivered as a file the user "
    "can download in the chat: say how many rows it holds and that the download is there, "
    "never list its rows.\n"
    "The Graph page's view -- group by, collapse every group, layout, max nodes -- is set through "
    "navigate's state.view, never by rewriting the query: asked to group the graph on screen by "
    "an attribute, navigate to /graph with only state.view.groupBy (the node property name) and "
    "no query, once the graph on screen was drawn with its relationships (if it was not, navigate "
    "with a query that returns them and the view together). Never change a query the user did not ask to change. A control you have no state "
    "key for, you say you cannot set; never guess a key.\n"
    "For any of these, do the lookup (if needed) once per table, build the query, navigate with "
    "state once, then stop — report the outcome; never re-run the naming lookup, search_catalog, "
    "or navigate again for the same table in the same turn just to double-check work that already "
    "succeeded.\n"
    # REQ-2115: the screen when asked to be shown; the admin API otherwise.
    "Asked to show, demo or teach something -- 'show me each step', 'show me how to add a "
    "source', 'teach me relationships' -- do it on the user's screen with navigate's actions, "
    "never by the admin API: open its page, fill in its form, choose its options and press its "
    "buttons, so the user watches each step happen there. Any other request to make a change you "
    "make through the admin API as usual. On the screen, name each element by its test id, or "
    "by its role and accessible name as the screen labels it; never guess one. An element the "
    "screen does not have is answered back by name: report it, and never carry on as if the step "
    "were done. A press of a button that changes something waits for the user's yes in the chat. "
    "Showing how to add a source, open the source picker (press sources-add-toggle), then show its "
    "whole list of source types (show source-type-picker-body) before pressing the one chosen "
    "(source-type-option-<type>), as a user would."
)


def _system_prompt(current_route: str | None) -> str:
    """_SYSTEM plus the frontend's current route, when it sent one (REQ-1800).

    There is no tool for "what page is the user on" — the browser already knows its own route on
    every turn, so the frontend just sends it (ChatPanel.tsx) rather than the model spending a
    tool round-trip to ask for something the caller could hand over for free. Kept OUT of _SYSTEM
    itself since it changes per turn, not per deployment."""
    if not current_route:
        return _SYSTEM
    return f"{_SYSTEM}\n\nThe user is currently viewing the app at route: {current_route!r}."


async def _effective_config(state: Any) -> dict:
    """The acting org's resolved config (REQ-1349) for vendor/model/endpoint lookups.

    Delegates to mcp_tools.effective_config when an org context (state.model_db) is bound, so an
    org's ai_models.mcp_chat/ai_endpoints choice made through the AI Models UI is honored on the
    very next chat turn, no restart. With no model_db bound, reads the bare deployment file
    (read_config()) — the same fallback this module's functions always used before REQ-1349 wiring,
    kept distinct from mcp_tools.effective_config's own fallback (state.config) because chat.py's
    tests configure the deployment file via PROVISA_CONFIG, not a fake state.config object."""
    model_db = getattr(state, "model_db", None)
    if model_db is None:
        from provisa.api.admin._config_io import read_config

        return read_config()
    return await mcp_tools.effective_config(state)


async def _resolve_vendor(state: Any) -> str:
    """The vendor configured for ai_models.mcp_chat, defaulting to 'anthropic'.

    REQ-1349: reads the ACTING ORG's config (_effective_config — deployment config with that
    org's org_settings overrides layered on, request-time resolved), not the static
    deployment-only config — an org that picks a vendor through the AI Models UI writes an
    org_settings row, and this must see it on the very next chat turn, no restart.

    REQ-1797: run_chat() branches on this — 'anthropic' uses Anthropic's own Messages API
    (including the hosted web_search/web_fetch tools, REQ-1796); anything else goes through
    aisuite's OpenAI-normalized tool-calling (openai/ollama/google/...), with no hosted web tools
    (each vendor's own equivalent, where one exists, is differently shaped — see _run_chat_aisuite)."""
    cfg = await _effective_config(state)
    op_cfg = (cfg.get("ai_models", {}) or {}).get("mcp_chat")
    vendor = op_cfg.get("vendor") if isinstance(op_cfg, dict) else None
    return vendor or "anthropic"


async def _llm_configured(state: Any) -> tuple[bool, str]:
    """Whether a usable credential (and, for a non-Anthropic vendor, an explicit model) exists
    for mcp_chat's configured vendor (REQ-1349, REQ-1794, REQ-1797).

    Mirrors ProvisaLLMClient's resolution (REQ-1790 custom endpoints, else the vendor's own
    default env var), but against the acting org's resolved config (mcp_tools.effective_config)
    rather than the deployment file alone — see _resolve_vendor. No fallback model guess for a
    non-Anthropic vendor (CLAUDE.md): the admin must name one explicitly in AI Models settings."""
    import os

    cfg = await _effective_config(state)
    vendor = await _resolve_vendor(state)

    endpoints = {
        ep["id"]: ep for ep in (cfg.get("ai_endpoints", []) or []) if ep.get("enabled", True)
    }
    endpoint = endpoints.get(vendor)
    if endpoint is not None:
        key_env = endpoint.get("api_key_env")
        from provisa.core.request_context import current_org
        from provisa.core.sandbox_ai import endpoint_key, key_org

        # REQ-1826: this must resolve the SAME grammar _resolve_api_key_field does at actual chat
        # time (${secret:NAME}/${env:NAME}/${user:NAME}, or a plain env-var name) — a bare
        # os.environ.get(key_env) check here always failed for a ${secret:...}/${env:...}
        # reference (that string is never itself a real env var name), permanently reporting "not
        # configured" for the exact vault-reference pattern this app's own AI Models UI recommends
        # (REQ-1808's whole point: every config field may carry that grammar, not just literals).
        if key_env:
            try:
                # REQ-2006: a sandbox's tier endpoint's key is the deployment org's.
                resolved = (
                    await endpoint_key(endpoint, current_org.get())
                    if key_org(endpoint, current_org.get())
                    else await _resolve_api_key_field(key_env)
                )
            except Exception as exc:
                # Never swallow the real cause into a generic "needs X set" — a ${secret:...}
                # that fails to resolve (unbound org, wrong org, the secret genuinely missing) is
                # a DIFFERENT, more diagnosable problem than "nothing was ever configured", and
                # reporting it as the latter hides exactly the information needed to fix it.
                return False, f"the {vendor!r} endpoint's {key_env} could not be resolved: {exc}"
            if not resolved:
                return False, f"the {vendor!r} endpoint needs {key_env} set"
    elif vendor == "ollama":
        pass  # REQ-1797: no credential — a local server, reachable at OLLAMA_API_URL or the default
    elif vendor == "google":
        # REQ-1797: aisuite's Google provider is Vertex AI, not the simpler Gemini API-key auth —
        # it needs a GCP project + service-account credentials, not one key.
        missing = [
            v
            for v in ("GOOGLE_PROJECT_ID", "GOOGLE_REGION", "GOOGLE_APPLICATION_CREDENTIALS")
            if not os.environ.get(v)
        ]
        if missing:
            return False, f"google (Vertex AI) needs {', '.join(missing)} set"
    else:
        env_var = {"anthropic": "ANTHROPIC_API_KEY", "openai": "OPENAI_API_KEY"}.get(vendor)
        if env_var and not os.environ.get(env_var):
            return False, f"{env_var} is not set"

    if vendor != "anthropic" and not await _resolve_model(state):
        return False, f"no model is set for vendor {vendor!r}"
    return True, ""


async def _resolve_model(state: Any) -> str | None:
    """The chat model: env override → the acting org's resolved ai_models.mcp_chat config
    (REQ-1349, via mcp_tools.effective_config — see _resolve_vendor) → Claude Opus 4.8 IF the
    vendor is (implicitly or explicitly) anthropic, else None (REQ-1797: no fallback guess for a
    vendor we don't know a safe default model for — the admin names one explicitly)."""
    from provisa.core import settings_registry  # REQ-1913: the operator setting `mcp.chat_model`

    override = settings_registry.value("mcp.chat_model")
    if override is not None:
        return override
    cfg = (await _effective_config(state)).get("ai_models", {}) or {}
    op_cfg = cfg.get("mcp_chat")
    if isinstance(op_cfg, str) and op_cfg.strip():
        return op_cfg.strip()
    if isinstance(op_cfg, dict) and op_cfg.get("model"):
        return str(op_cfg["model"])
    return "claude-opus-4-8" if await _resolve_vendor(state) == "anthropic" else None


async def _run_sql(state: Any, role: str, sql: str, args: dict) -> dict:
    """run_sql for the chat. The model always gets the result's first rows: a file delivery alone
    answers only how many rows it holds, and a model that asked for a file it did not need read
    that count as its answer (GPT-5.4 mini's "the shop has 1 customer", bake-off run 38625083413).
    A result asked for as a file (file_format) is also delivered as one; so is a result with more
    rows than the cap, asked for without a limit, as CSV. A delivered result -- those, or one the
    operator's large-result threshold delivered -- carries a download for the chat panel."""
    file_format = args.get("file_format")
    result = await mcp_tools.run_sql(
        state, role, sql, limit=args.get("limit"), offset=int(args.get("offset", 0))
    )
    deliver_as = (
        file_format
        if file_format is not None
        else "csv"
        if "redirect" not in result and args.get("limit") is None and result["truncated"]
        else None
    )
    if deliver_as is not None:
        delivered = await mcp_tools.run_sql(
            state, role, sql, redirect=True, redirect_format=deliver_as
        )
        result["redirect"] = delivered["redirect"]
    if "redirect" in result:
        result["artifact"] = {"kind": "file", **result["redirect"]}
        result["shown"] = "The user has a download for this file in the chat."
    return result


async def _render_graph(state: Any, role: str, cypher: str, layout: str) -> dict:
    """render_graph for the chat: the statement checked as a Graph page navigate is, run under
    the role, and drawn as a PNG the chat panel shows with its download and its way to the Graph
    page. The model gets what was drawn, never the picture."""
    import asyncio
    import base64

    from provisa.api.mcp.cypher_check import check_graph_read

    read = await check_graph_read(state, role, cypher)
    if read.problem is not None:
        raise ValueError(
            f"Not drawn: {read.problem}. Correct the statement and call render_graph again."
        )
    ran = await mcp_tools.run_cypher(state, role, cypher)
    elements = ran["elements"]
    png = await asyncio.to_thread(mcp_tools.render_graph, elements, layout)
    nodes, relationships = len(elements["nodes"]), len(elements["edges"])
    return {
        "nodes": nodes,
        "relationships": relationships,
        "rows": ran["row_count"],
        "truncated": ran["truncated"],
        "shown": (
            "The picture is shown in the chat, with a Download button and an Open on Graph page "
            "button that runs this statement there."
        ),
        "artifact": {
            "kind": "graph",
            "png": base64.b64encode(png).decode("ascii"),
            "cypher": cypher,
            "nodes": nodes,
            "relationships": relationships,
        },
    }


def _split_artifact(result: Any) -> tuple[Any, dict | None]:
    """A tool result as the model gets it, and the artifact the chat panel shows, when it has one
    (a picture, a download): the artifact goes to the browser alone, never into the model's
    context."""
    if not isinstance(result, dict) or "artifact" not in result:
        return result, None
    kept = dict(result)
    return kept, {"type": "artifact", **kept.pop("artifact")}


async def _execute_tool(
    state: Any, role: str, name: str, args: dict, *, request: Any = None
) -> Any:
    """Dispatch one tool call to the governed MCP tool, with the role pinned by the endpoint.

    `request` is the real, AuthMiddleware-verified HTTP request behind this chat turn -- passed
    to the tools that act as the user through the admin API (REQ-2114). Every other tool ignores
    it."""
    if name == "list_schemas":
        return await mcp_tools.list_schemas(state, role)
    if name == "list_tables":
        return await mcp_tools.list_tables(state, role, args["schema"])
    if name == "describe_table":
        return await mcp_tools.describe_table(state, role, args["schema"], args["table"])
    if name == "graphql_field_names":
        return await mcp_tools.graphql_field_names(state, role, args["schema"], args["table"])
    if name == "cypher_field_names":
        return await mcp_tools.cypher_field_names(state, role, args["schema"], args["table"])
    if name == "generate_explore_queries":
        return await mcp_tools.generate_explore_queries(state, role, args["question"])
    if name == "search_catalog":
        return await mcp_tools.search_catalog(state, role, args["query"], k=int(args.get("k", 5)))
    if name == "run_sql":
        # Polly's SQL runs with every identifier quoted; SQL that does not parse runs as written,
        # and the result says why it was not quoted.
        sql, quote_error = quote_identifiers(args["sql"])
        result = await _run_sql(state, role, sql, args)
        if quote_error is not None:
            result["sql_note"] = _unquoted_note(quote_error)
        return result
    if name == "render_graph":
        return await _render_graph(state, role, args["cypher"], args.get("layout", "force"))
    if name == "explain_sql":
        return await mcp_tools.explain_sql(state, role, args["sql"])
    # REQ-2114: the admin API the UI uses, read and run as the user behind this chat.
    if name == "admin_schema":
        if args.get("type"):
            return admin_api.schema_type(args["type"])
        return admin_api.schema_fields(args.get("kind") or "mutation", args.get("search"))
    if name == "admin_query":
        if request is None:
            raise ValueError("admin_query runs as the user, and this chat carries no request")
        return await admin_api.run_query(args["query"], args.get("variables"), request=request)
    if name == "admin_rest_routes":
        if request is None:
            raise ValueError("admin_rest_routes reads the app, and this chat carries no request")
        return admin_api.rest_routes(request.app, args.get("search"))
    if name == "discover_relationships":
        if request is None:
            raise ValueError("discovery runs as the user, and this chat carries no request")
        return await admin_api.discover_relationships(
            state, args["schema"], args["table"], request=request
        )
    if name == "search_govdata_subjects":
        return await mcp_tools.search_govdata_subjects(state, role, args["query"])
    if name == "search_kaggle_datasets":
        return await mcp_tools.search_kaggle_datasets(state, role, args["query"])
    if name == "list_native_tables":
        return await mcp_tools.list_native_tables(
            state, role, args["source_id"], args.get("schema_name", "public")
        )
    if name == "describe_native_table":
        return await mcp_tools.describe_native_table(
            state, role, args["source_id"], args["schema_name"], args["table_name"]
        )
    if name in (
        "list_glossary_terms",
        "list_glossary_findings",
        "create_glossary_term",
        "update_glossary_term",
        "delete_glossary_term",
        "add_glossary_term_edge",
        "remove_glossary_term_edge",
        "ground_glossary_term_to_table",
        "unground_glossary_term_from_table",
        "tag_glossary_term",
        "untag_glossary_term",
        "deprecate_glossary_term",
        "retire_glossary_term",
        "restore_glossary_term",
    ):
        if request is None:
            raise ValueError(f"{name} requires a verified request context")
        if name == "list_glossary_terms":
            return await mcp_tools.list_glossary_terms(
                state, role, request, args.get("q"), args.get("include_deprecated", True)
            )
        if name == "list_glossary_findings":
            return await mcp_tools.list_glossary_findings(state, role, request)
        if name == "create_glossary_term":
            return await mcp_tools.create_glossary_term(
                state, role, request, args["name"], args.get("definition"), args.get("domains")
            )
        if name == "update_glossary_term":
            return await mcp_tools.update_glossary_term(
                state,
                role,
                request,
                int(args["term_id"]),
                name=args.get("name"),
                definition=args.get("definition"),
                export_excluded=args.get("export_excluded"),
            )
        if name in ("deprecate_glossary_term", "retire_glossary_term", "restore_glossary_term"):
            action = name.split("_", 1)[0]
            body = {
                k: args[k]
                for k in ("reason", "retire_on", "successor", "confirm_tag_loss", "carry_tags")
                if k in args
            }
            return await mcp_tools.change_glossary_term_status(
                state, role, request, int(args["term_id"]), action, body
            )
        if name == "delete_glossary_term":
            return await mcp_tools.delete_glossary_term(state, role, request, int(args["term_id"]))
        if name == "add_glossary_term_edge":
            return await mcp_tools.add_glossary_term_edge(
                state,
                role,
                request,
                int(args["term_id"]),
                int(args["to_term_id"]),
                args["rel_type"],
            )
        if name == "ground_glossary_term_to_table":
            return await mcp_tools.ground_glossary_term_to_table(
                state, role, request, int(args["term_id"]), int(args["table_id"])
            )
        if name == "unground_glossary_term_from_table":
            return await mcp_tools.unground_glossary_term_from_table(
                state, role, request, int(args["term_id"]), int(args["table_id"])
            )
        if name == "tag_glossary_term":  # REQ-2145
            return await mcp_tools.tag_glossary_term(
                state, role, request, int(args["term_id"]), args["tag_id"], args.get("reason")
            )
        if name == "untag_glossary_term":
            return await mcp_tools.untag_glossary_term(
                state, role, request, int(args["term_id"]), args["tag_id"]
            )
        return await mcp_tools.remove_glossary_term_edge(
            state, role, request, int(args["term_id"]), int(args["to_term_id"]), args["rel_type"]
        )
    if name in (
        "list_data_products",
        "create_data_product",
        "delete_data_product",
        "upsert_metric",
        "delete_metric",
    ):
        if request is None:
            raise ValueError(f"{name} requires a verified request context")
        if name == "list_data_products":
            return await mcp_tools.list_data_products(state, role, request)
        if name == "create_data_product":
            return await mcp_tools.create_data_product(
                state,
                role,
                request,
                args["id"],
                args["domain_id"],
                args["name"],
                owner_role=args.get("owner_role"),
                team_role=args.get("team_role"),
                purpose=args.get("purpose", ""),
                limitations=args.get("limitations", ""),
                usage=args.get("usage", ""),
                version=args.get("version"),
                status=args.get("status"),
                sla=args.get("sla"),
                support=args.get("support"),
            )
        if name == "delete_data_product":
            return await mcp_tools.delete_data_product(state, role, request, args["id"])
        if name == "upsert_metric":
            return await mcp_tools.upsert_metric(
                state,
                role,
                request,
                args["name"],
                args["expression"],
                datatype=args.get("datatype"),
                description=args.get("description"),
                ai_context=args.get("ai_context"),
                visible_to=args.get("visible_to"),
            )
        return await mcp_tools.delete_metric(state, role, request, args["name"])
    if name in model_tool_specs.NAMES:
        return await model_tool_specs.dispatch(state, role, name, args, request)
    raise ValueError(f"unknown tool {name!r}")


def _to_openai_tool(t: dict) -> dict:
    """Our Anthropic-shaped tool def (name/description/input_schema) -> the OpenAI/aisuite
    function-tool shape every non-Anthropic aisuite provider expects (REQ-1797)."""
    return {
        "type": "function",
        "function": {
            "name": t["name"],
            "description": t["description"],
            "parameters": t["input_schema"],
        },
    }


def _wire_convo_to_openai_messages(convo: list[dict]) -> list[dict]:
    """The WIRE conversation format (Anthropic-shaped content blocks — what every vendor's SSE
    events and the frontend's resume POST already use, REQ-1795) -> aisuite/OpenAI chat messages
    (REQ-1797). Keeping the wire format vendor-neutral like this means useMcpChat.ts needs no
    vendor awareness at all: it always stores/replays wire blocks; only the SERVER, per vendor,
    translates them to what that vendor's API actually needs."""
    out: list[dict] = []
    for msg in convo:
        role, content = msg["role"], msg["content"]
        if isinstance(content, str):
            out.append({"role": role, "content": content})
            continue
        if role == "assistant":
            text = "\n".join(b["text"] for b in content if b.get("type") == "text")
            tool_calls = [
                {
                    "id": b["id"],
                    "type": "function",
                    "function": {"name": b["name"], "arguments": json.dumps(b.get("input", {}))},
                }
                for b in content
                if b.get("type") == "tool_use"
            ]
            entry: dict[str, Any] = {"role": "assistant", "content": text or None}
            if tool_calls:
                entry["tool_calls"] = tool_calls
            out.append(entry)
        else:
            # A user-role wire turn carrying tool_result blocks from a prior paused turn — each
            # becomes its own role="tool" message (OpenAI's convention; Anthropic bundles them
            # into one user-role message instead, which is what `content` still looks like here).
            for b in content:
                if isinstance(b, dict) and b.get("type") == "tool_result":
                    out.append(
                        {
                            "role": "tool",
                            "tool_call_id": b["tool_use_id"],
                            "content": b.get("content", ""),
                        }
                    )
                else:
                    out.append({"role": role, "content": content})
                    break
    return out


def _openai_message_to_wire_blocks(message: Any) -> list[dict]:
    """An aisuite/OpenAI-shaped response message -> the shared wire content-block format
    (REQ-1797) — so the SSE events this vendor path emits are indistinguishable, on the wire,
    from Anthropic's."""
    blocks: list[dict] = []
    text = getattr(message, "content", None)
    if text:
        blocks.append({"type": "text", "text": text})
    for tc in getattr(message, "tool_calls", None) or []:
        try:
            args = json.loads(tc.function.arguments or "{}")
        except (ValueError, TypeError):
            args = {}
        blocks.append({"type": "tool_use", "id": tc.id, "name": tc.function.name, "input": args})
    return blocks


def _unquoted_note(error: str) -> str:
    return f"The SQL could not be read to quote its names, so it is used as written: {error}"


def _prepare_client_call(call: dict, confirmation: str | None = None) -> tuple[dict, str | None]:
    """A client tool call as the browser receives it, and a note for the user when there is one.

    SQL handed to the SQL explorer has every identifier quoted (a reserved word as a column name
    otherwise breaks it). SQL that does not parse is handed on unchanged, and the note says why.
    A ``confirmation`` -- what the navigate's result can confirm -- travels with the call, and the
    browser adds it to the result it returns.
    """
    if confirmation is not None:
        call = {**call, "confirmation": confirmation}
    state = call["input"].get("state") if call["name"] == "navigate" else None
    if not isinstance(state, dict) or not isinstance(state.get("sql"), str):
        return call, None
    sql, error = quote_identifiers(state["sql"])
    prepared = {**call, "input": {**call["input"], "state": {**state, "sql": sql}}}
    return prepared, None if error is None else _unquoted_note(error)


async def _checked_graph_navigates(
    state: Any, role: str, calls: list[dict]
) -> tuple[dict[str, str], dict[str, str]]:
    """Each Graph page navigate, by call id: refused -- its Cypher statement could not run, or
    would draw no edges -- with the tool result the model gets instead (the statement never
    reaches the page, and the model corrects it); or let through, with what the navigate's result
    can confirm. The browser reports only that it navigated, never what the page drew, so the
    confirmation says what the page was handed and that the drawing is not reported back."""
    from provisa.api.mcp.cypher_check import check_graph_read, graph_query

    refused: dict[str, str] = {}
    confirmed: dict[str, str] = {}
    for call in calls:
        if call["name"] != "navigate" or not str(call["input"].get("route") or "").startswith(
            "/graph"
        ):
            continue
        query = graph_query(call)
        if query is None:
            view = (call["input"].get("state") or {}).get("view")
            if view:
                confirmed[call["id"]] = (
                    f"The Graph page was asked to set its view to {json.dumps(view)} on the graph "
                    "already on screen; the page does not report back what it drew."
                )
            continue
        read = await check_graph_read(state, role, query)
        if read.problem is not None:
            refused[call["id"]] = (
                "Not navigated: this Cypher statement does not run, so it was not put on the "
                f"Graph page. {read.problem}. Correct the statement and navigate again."
            )
            continue
        drawn = {
            kind: [name for name, k in read.columns.items() if k == kind]
            for kind in ("node", "edge", "path")
        }
        confirmed[call["id"]] = (
            "The Graph page was handed the statement, which checks clean and returns "
            + "; ".join(f"{kind} column(s) {', '.join(n)}" for kind, n in drawn.items() if n)
            + ". The page runs it; it does not report back the rows it drew."
        )
    return refused, confirmed


def _split_tool_calls(calls: list[dict]) -> tuple[list[dict], list[dict]]:
    """Normalized {id, name, input} tool calls -> (client_calls, server_calls)."""
    client_calls = [c for c in calls if c["name"] in _CLIENT_TOOL_NAMES]
    server_calls = [c for c in calls if c["name"] not in _CLIENT_TOOL_NAMES]
    return client_calls, server_calls


_EPHEMERAL = {"type": "ephemeral"}

# REQ-1838: the most one model call may generate, thinking included, on every vendor. A call
# with no cap lets a gateway reserve the model's whole output limit against the account's credit
# (OpenRouter refuses the call with 402 when that reservation exceeds it) and puts no bound on
# what one call can spend.
_MAX_TOKENS = 32000

# REQ-1838: a turn whose rounds all ran out still calling tools (the last one is asked to call
# none, but a provider may not honour that) ends saying so, never in silence.
# REQ-1838: a final round that answered nothing -- blank text and no tool call -- ends saying so.
# DeepSeek did, twice in bake-off run 38635124027, and the turn showed an empty reply.
_NO_ANSWER = {
    "type": "text",
    "text": "\n\n(The model returned no answer — please ask again.)",
}

# REQ-1838: the last round is told why it has no tools, so it answers instead of reaching for one:
# OpenRouter's Gemini and Qwen routes returned tool calls on a round sent with none, and Kimi wrote
# one out as text (bake-off run 38639158659).
_LAST_ROUND_INSTRUCTION = {
    "role": "user",
    "content": (
        "You have no tool calls left: answer the question now from the results above, in plain "
        "text."
    ),
}

# A tool call written as text instead of made -- Kimi's ` tool="run_sql" index="1" key="sql"
# type="string"SELECT ...`, or a model's own call markup -- is not an answer.
_TOOL_NAMES_PATTERN = "|".join(re.escape(t["name"]) for t in [*_TOOLS, *_CLIENT_TOOLS])
_TEXT_TOOL_CALL = re.compile(
    rf'tool="(?:{_TOOL_NAMES_PATTERN})"|<\|?tool_calls?\b|<function[=\s(]'
    rf'|"name"\s*:\s*"(?:{_TOOL_NAMES_PATTERN})"\s*,\s*"(?:arguments|parameters)"'
)


def _is_answer(text: str | None) -> bool:
    """Whether a final round's text answers: not blank, and not a tool call written as text."""
    if text is None or not text.strip():
        return False
    return _TEXT_TOOL_CALL.search(text) is None


_STEPS_SPENT = {
    "type": "text",
    "text": (
        "\n\n(Stopped after the most steps one question may take, before finishing — please "
        "ask a narrower question.)"
    ),
}


def _output_cap(provider_key: str, custom_endpoint: bool) -> dict[str, Any]:
    """_MAX_TOKENS as the aisuite provider's own call takes it. OpenAI's own API takes
    ``max_completion_tokens`` (its reasoning models refuse ``max_tokens``); an OpenAI-wire
    endpoint (OpenRouter, LiteLLM, vLLM) and Anthropic take ``max_tokens``; Ollama's own API takes
    ``options.num_predict``. aisuite's Google provider passes no generation limit on to Google
    (it builds its GenerationConfig from the temperature alone), so a cap for it is refused by
    name rather than sent to be dropped."""
    if provider_key == "openai" and not custom_endpoint:
        return {"max_completion_tokens": _MAX_TOKENS}
    if provider_key == "ollama":
        return {"options": {"num_predict": _MAX_TOKENS}}
    if provider_key == "google":
        raise ValueError(
            "Polly cannot cap a Google model's output through aisuite's Google provider; "
            "reach the model through an OpenAI-wire endpoint (e.g. OpenRouter) instead"
        )
    return {"max_tokens": _MAX_TOKENS}


def _cached_tools() -> list[Any]:
    """REQ-2012: the tool definitions, Anthropic's hosted tools first, with a prompt-cache
    breakpoint on the last. The definitions are the same on every call, so every call after the
    first reads them from the cache instead of paying for them again."""
    tools = [*_WEB_TOOLS, *_TOOLS, *_CLIENT_TOOLS]
    return [*tools[:-1], {**tools[-1], "cache_control": _EPHEMERAL}]


# REQ-2012: the models an OpenAI-wire gateway (OpenRouter) caches only where a block says so.
# Each round's conversation gets a breakpoint on its last message, as the Anthropic path's does
# (_with_cache_breakpoint), so the next round reads the turn so far from the cache: with the
# system prompt's, two of the four breakpoints a request may carry. Gemini uses the last one.
_GATEWAY_CACHED_MODELS = ("anthropic/", "google/")


def _with_openai_cache_breakpoint(messages: list[dict]) -> list[dict]:
    """``messages`` (OpenAI wire) with a prompt-cache breakpoint on the last one that has content:
    its text as a content part carrying cache_control. A copy; the conversation is unchanged."""
    for i in range(len(messages) - 1, -1, -1):
        content = messages[i].get("content")
        if not content:
            continue  # an assistant turn that only called tools carries no text to mark
        parts = (
            [{"type": "text", "text": content}]
            if isinstance(content, str)
            else [dict(part) for part in content]
        )
        parts[-1] = {**parts[-1], "cache_control": _EPHEMERAL}
        return [*messages[:i], {**messages[i], "content": parts}, *messages[i + 1 :]]
    raise ValueError("a conversation with no message that has content has nothing to cache")


def _cached_system(system: str) -> list[Any]:
    """REQ-2012: the system prompt as one cached block, read from the cache after the first call."""
    return [{"type": "text", "text": system, "cache_control": _EPHEMERAL}]


def _with_cache_breakpoint(convo: list[Any]) -> list[Any]:
    """REQ-2012: ``convo`` with a prompt-cache breakpoint on its last block, so a tool round trip
    (and the next turn) reads everything before it from the cache. A copy: ``convo`` itself, which
    goes back to the browser, is left as it is."""
    if not convo:
        return convo
    last = convo[-1]
    role = last["role"] if isinstance(last, dict) else last.role
    content = last["content"] if isinstance(last, dict) else last.content
    if isinstance(content, str):
        blocks: list[Any] = [{"type": "text", "text": content}]
    else:
        blocks = [
            b if isinstance(b, dict) else b.model_dump(exclude=getattr(b, "__api_exclude__", None))
            for b in content
        ]
    if not blocks:
        return convo
    blocks = [*blocks[:-1], {**blocks[-1], "cache_control": _EPHEMERAL}]
    return [*convo[:-1], {"role": role, "content": blocks}]


async def _run_chat_anthropic(
    state: Any,
    role: str,
    convo: list[Any],
    *,
    max_iterations: int,
    request: Any = None,
    current_route: str | None = None,
) -> AsyncIterator[dict]:
    """The original REQ-1008/1795/1796 loop: Anthropic's own Messages API, including the
    Anthropic-only hosted web_search/web_fetch tools. Unchanged behavior from before REQ-1797."""
    from anthropic import AsyncAnthropic, omit

    client = AsyncAnthropic()
    model = await _resolve_model(state)
    assert model is not None  # _llm_configured() guarantees this for vendor == "anthropic"
    system = _system_prompt(current_route)
    # REQ-1850: rounds that finish entirely server-side (server tool called, result fed back)
    # before a later round pauses on a CLIENT tool must still reach the frontend's resumed
    # `convo` — otherwise the model resumes having "forgotten" every tool call it made earlier
    # in this same turn and repeats them, forever, each time it reaches the client tool again.
    prior_messages: list[Any] = []
    # REQ-2012: the turn's token usage, summed over every call, tool round trips included.
    usage = polly_usage.Usage()

    for round_no in range(max_iterations):
        # REQ-1838: the last round the turn allows calls no tool, so the model answers from what
        # it has rather than the turn ending in silence mid-search.
        tool_choice: Any = {"type": "none"} if round_no == max_iterations - 1 else omit
        # REQ-1838: adaptive thinking's budget shares the max_tokens cap — 8192 let a heavily-
        # reasoning turn (many tools + a long system prompt) exhaust the budget mid-thought,
        # before the model ever emitted its tool_use block. stop_reason came back "max_tokens",
        # which the check below used to treat exactly like a deliberate "end_turn": the turn
        # silently ended with whatever text had already streamed ("let me fetch both now") and no
        # tool call, no error, nothing — verified live with Opus 4.6, reproducible on retry.
        # Raising max_tokens far enough to fix that pushes the SDK's own estimated generation time
        # over its 10-minute non-streaming ceiling ("Streaming is required for operations that may
        # take longer than 10 minutes" — anthropic-sdk-python#long-requests), also verified live —
        # so this now streams and takes the final assembled Message, identical in shape to what
        # .create() returned, rather than switching the rest of this loop to consume raw events.
        async with client.messages.stream(
            model=model,
            max_tokens=_MAX_TOKENS,
            system=_cached_system(system),
            tools=_cached_tools(),
            thinking={"type": "adaptive"},
            messages=_with_cache_breakpoint(convo),
            tool_choice=tool_choice,
        ) as stream:
            # REQ-1839: hosted-tool badges are yielded as their blocks complete, by walking the
            # raw event stream, so a search shows before the answer that depends on it. The walk
            # is presentation only: verified live, it once raised partway through a real Opus 4.6
            # round (an event shape it did not anticipate), so a failure here is logged and the
            # round's content still comes from get_final_message().
            # The round's text is not streamed as it comes: text the model writes beside a tool
            # call is written before that call's result exists ("Done -- the Graph page is now
            # running this"), so it is never shown. A round's text is sent once the round ends
            # without a tool call, from the complete response; the text beside a call stays in
            # assistant_content and the model's history.
            try:
                async for event in stream:
                    if event.type != "content_block_stop":
                        continue
                    block = stream.current_message_snapshot.content[event.index]
                    # REQ-1796: Anthropic already executed these (see _WEB_TOOLS' note) — no
                    # dispatch needed, just a UI badge so the user can see a search/fetch happened.
                    if block.type == "server_tool_use" and block.name in _SERVER_HOSTED_TOOL_NAMES:
                        yield {"type": "tool_use", "name": block.name, "input": block.input}
                    elif block.type == "web_search_tool_result":
                        content_type = getattr(getattr(block, "content", None), "type", "")
                        yield {
                            "type": "tool_result",
                            "name": "web_search",
                            "is_error": content_type == "web_search_tool_result_error",
                        }
                    elif block.type == "web_fetch_tool_result":
                        content_type = getattr(getattr(block, "content", None), "type", "")
                        yield {
                            "type": "tool_result",
                            "name": "web_fetch",
                            "is_error": content_type == "web_fetch_tool_result_error",
                        }
            except Exception:
                logging.getLogger(__name__).exception(
                    "REQ-1839 live event walk failed mid-stream; the round's text is still sent "
                    "from the complete response"
                )
            resp = await stream.get_final_message()
            usage = usage + polly_usage.from_anthropic(resp.usage)
            if not any(b.type == "tool_use" for b in resp.content):
                text = "".join(b.text for b in resp.content if b.type == "text" and b.text)
                if _is_answer(text):
                    yield {"type": "text", "text": text}
                elif resp.stop_reason != "max_tokens":  # that ends with its own note below
                    yield _NO_ANSWER

        if resp.stop_reason == "max_tokens":
            # REQ-1838: never end the turn silently on this — the user sees exactly the same
            # "said she'd do it, then nothing" symptom whether the cause is a truncated tool call
            # or the model genuinely giving up, and only one of those has a fix (raise the cap).
            yield {
                "type": "text",
                "text": (
                    "\n\n(Ran out of response budget before finishing — please retry, or ask a "
                    "narrower question.)"
                ),
            }
            break
        if resp.stop_reason != "tool_use":
            break
        if round_no == max_iterations - 1:
            # The last round was asked to call none; a call it made anyway is not run.
            yield _STEPS_SPENT
            break

        tool_use_blocks = [b for b in resp.content if b.type == "tool_use"]
        client_blocks = [b for b in tool_use_blocks if b.name in _CLIENT_TOOL_NAMES]
        server_blocks = [b for b in tool_use_blocks if b.name not in _CLIENT_TOOL_NAMES]

        tool_results = []
        for block in server_blocks:
            yield {"type": "tool_use", "name": block.name, "input": block.input}
            try:
                result, artifact = _split_artifact(
                    await _execute_tool(state, role, block.name, dict(block.input), request=request)
                )
                content = json.dumps(result, default=str)
                is_error = False
            except Exception as exc:  # noqa: BLE001 - reported back to the model, see pragma
                content = f"Error: {exc}"
                is_error = True
                artifact = None
            if artifact is not None:
                yield artifact
            yield {"type": "tool_result", "name": block.name, "is_error": is_error}
            tool_results.append(
                {
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": content,
                    "is_error": is_error,
                }
            )

        refused, confirmed = await _checked_graph_navigates(
            state, role, [{"id": b.id, "name": b.name, "input": b.input} for b in client_blocks]
        )
        for block in client_blocks:
            if block.id in refused:
                yield {"type": "tool_result", "name": block.name, "is_error": True}
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": refused[block.id],
                        "is_error": True,
                    }
                )
        client_blocks = [b for b in client_blocks if b.id not in refused]
        if client_blocks:
            # REQ-1795: stop here — the browser must execute these, then resume by POSTing the
            # full history again with the assistant content below plus a tool_result for every
            # block.id in `pending` (including the server ones already computed above).
            pending = []
            for block in client_blocks:
                call, note = _prepare_client_call(
                    {"id": block.id, "name": block.name, "input": block.input},
                    confirmed.get(block.id),
                )
                if note is not None:
                    yield {"type": "text", "text": f"\n\n{note}"}
                pending.append(call)
                yield {
                    "type": "tool_use",
                    "name": block.name,
                    "input": call["input"],
                    "client": True,
                }
            yield {
                "type": "awaiting_client_tools",
                # REQ-1838/1839 regression: resp now comes from stream.get_final_message()
                # (ParsedMessage), whose text blocks are ParsedTextBlock — carrying an extra
                # parsed_output field (always None here; only meaningful with the structured-
                # output param we never pass) that the SDK itself marks `__api_exclude__` but
                # plain model_dump() doesn't know to drop. Sent back verbatim in the resume POST's
                # assistant_content, Anthropic's API rejected it outright: 400 invalid_request_error
                # "messages.N.content.M.text.parsed_output: Extra inputs are not permitted" —
                # verified live, reproducible on every turn that paused for a client tool (e.g.
                # present_choice) after this switch to streaming. .create()'s plain TextBlock never
                # had this field, so the old non-streaming path never hit it.
                "assistant_content": [
                    b.model_dump(exclude=getattr(b, "__api_exclude__", None)) for b in resp.content
                ],
                "server_tool_results": tool_results,
                "pending": pending,
                # REQ-1850: every server-only round already completed earlier in THIS turn, in
                # wire order, so the frontend can splice them into its resumed `convo` ahead of
                # this round's own assistant_content/server_tool_results.
                "prior_messages": prior_messages,
            }
            yield polly_usage.usage_event(model, usage)
            yield {"type": "done"}
            return

        # No client tools this turn — append and continue exactly as before.
        convo.append({"role": "assistant", "content": resp.content})
        convo.append({"role": "user", "content": tool_results})
        # REQ-1850: prior_messages travels over SSE as JSON (unlike `convo`, which stays in-process
        # and can hold raw SDK block objects) — must be plain dicts, same exclusion as
        # assistant_content below (see its comment for why __api_exclude__ matters).
        prior_messages.append(
            {
                "role": "assistant",
                "content": [
                    b.model_dump(exclude=getattr(b, "__api_exclude__", None)) for b in resp.content
                ],
            }
        )
        prior_messages.append({"role": "user", "content": tool_results})

    yield polly_usage.usage_event(model, usage)
    yield {"type": "done"}


async def _resolve_aisuite_routing(state: Any, vendor: str, model: str) -> tuple[str, dict]:
    """(model_id, provider_configs) for aisuite, given the resolved vendor/model (REQ-1808).

    ``vendor`` is EITHER one of aisuite's own named providers (openai, ollama, google, ...) — in
    which case aisuite already knows how to reach it and needs no override — OR the `id` of a
    configured custom AI endpoint (REQ-1790: an OpenAI- or Anthropic-wire-protocol server aisuite
    has no name for, e.g. a local Ollama, LiteLLM, or OpenRouter gateway). aisuite's own model
    string can only ever name a provider IT recognizes (``Client.create`` looks `provider_key` up
    in its fixed registry and rejects anything else), so a custom endpoint is reached by
    overriding the underlying "openai"/"anthropic" provider's `base_url` (whichever the
    endpoint's `style` says it speaks) via `provider_configs`, and the model string names THAT
    provider, not the endpoint's own id.

    EVERY field taken from config here — base_url as much as the API key — is resolved through
    ``${secret:NAME}``/``${user:NAME}`` when its value uses that grammar (provisa.core.secrets),
    not just the key: an operator may just as reasonably want a private gateway's base_url held
    in the vault (it can itself carry embedded credentials, e.g. a signed URL or a ``user:pass@``
    authority) as they want the API key held there. The same indirection provisa.core.org_secrets
    already gives named vendor keys, generalized to every config field on a custom endpoint.
    """
    cfg = await _effective_config(state)
    endpoints = {ep["id"]: ep for ep in (cfg.get("ai_endpoints") or [])}
    endpoint = endpoints.get(vendor)
    if endpoint is None:
        return f"{vendor}:{model}", {}

    style = endpoint["style"]  # "openai" | "anthropic"
    from provisa.core.request_context import current_org
    from provisa.core.sandbox_ai import endpoint_key, key_org

    # REQ-2006: a sandbox's tier endpoint's key is the deployment org's; any other, the org's own.
    api_key = (
        await endpoint_key(endpoint, current_org.get())
        if key_org(endpoint, current_org.get())
        else await _resolve_api_key_field(endpoint.get("api_key_env"))
    )
    resolved_base_url = await _resolve_config_field(endpoint["base_url"])
    provider_config: dict[str, Any] = {"base_url": resolved_base_url}
    if api_key:
        provider_config["api_key"] = api_key
    elif style == "openai":
        # aisuite's OpenAI provider requires a non-empty api_key even when the server behind
        # base_url doesn't check one at all (e.g. Ollama's OpenAI-compatible endpoint) — a
        # placeholder here is not a credential, just satisfying that constructor guard.
        provider_config["api_key"] = "not-required"
    return f"{style}:{model}", {style: provider_config}


async def _resolve_config_field(value: str) -> str:
    """``value`` as-is, unless it contains a ``${env:NAME}``/``${secret:NAME}``/``${user:NAME}``
    reference (provisa.core.secrets' full reference grammar), in which case that's resolved
    first — so ANY custom-endpoint field can be a literal, an env-var indirection, or a vault
    reference, the caller's choice, not a fixed convention per field.

    Only ``${secret:...}``/``${user:...}`` need the org vault bound (see
    secrets_store.bound_to_request_org) — ``${env:...}``/``${scope:...}`` resolve against the
    process environment alone, so requiring org context for THOSE too would needlessly fail a
    deployment with no admin_db reachable from this call for a field that never touches the vault.
    """
    if "${" not in value:
        return value
    from provisa.core.secrets import resolve_secrets

    if "${secret:" in value or "${user:" in value:
        from provisa.core.secrets_store import bound_to_request_org

        async with bound_to_request_org():
            return resolve_secrets(value)
    return resolve_secrets(value)


async def _resolve_api_key_field(api_key_env: str | None) -> str | None:
    """The endpoint's API key, from its ``api_key_env`` field (REQ-1790, REQ-1808, REQ-1827).

    Delegates to provisa.core.secrets.resolve_api_key_field — the ONE shared implementation of
    this grammar, also used by provisa/vector/providers.py's embedding-model key. Kept as a thin
    wrapper (not a bare re-export) so this module's own call sites/tests don't need to know it
    moved."""
    from provisa.core.secrets import resolve_api_key_field

    return await resolve_api_key_field(api_key_env)


async def _run_chat_aisuite(
    state: Any,
    role: str,
    convo: list[Any],
    vendor: str,
    *,
    max_iterations: int,
    request: Any = None,
    current_route: str | None = None,
) -> AsyncIterator[dict]:
    """REQ-1797: the non-Anthropic path — openai/ollama/google (or any other aisuite-supported
    vendor), via aisuite's OpenAI-normalized tool-calling. No hosted web_search/web_fetch: those
    are Anthropic-proprietary (see _WEB_TOOLS); this path gets none, since each vendor's own
    equivalent (OpenAI's Responses-API web tool, Gemini's Search grounding, Ollama's none at all)
    is differently shaped and not unified by aisuite.

    Speaks the SAME wire format as the Anthropic path (Anthropic-shaped content blocks) for every
    SSE event and for `convo` itself — see _wire_convo_to_openai_messages/
    _openai_message_to_wire_blocks — so useMcpChat.ts on the frontend needs no vendor awareness.

    Calls the aisuite PROVIDER directly (``ProviderFactory.create_provider`` + its own
    ``chat_completions_create``), not ``aisuite.Client().chat.completions.create()`` — verified
    live (REQ-1809) that aisuite 0.1.14's own wrapper POPS ``tools`` out of kwargs and only ever
    forwards it back to the provider when ``max_turns`` is also given, which instead routes
    through aisuite's OWN automatic tool-execution loop (`Client._tool_runner`) — one that
    requires real Python callables, not the JSON tool schemas this loop's own governed dispatch
    needs. Passing `tools` with no `max_turns`, the documented-looking way, silently drops it: the
    model never saw a single tool definition and could never have called one, on ANY non-Anthropic
    vendor, since REQ-1797 first shipped. Calling the provider directly is exactly what
    ``Client.create()`` itself does in the max_turns-less case — except this actually keeps
    `tools` in the call."""
    import asyncio

    from aisuite.provider import ProviderFactory

    model = await _resolve_model(state)
    assert model is not None  # _llm_configured() guarantees this
    model_id, provider_configs = await _resolve_aisuite_routing(state, vendor, model)
    provider_key, model_name = model_id.split(":", 1)
    provider = ProviderFactory.create_provider(provider_key, provider_configs.get(provider_key, {}))
    cap = _output_cap(provider_key, custom_endpoint=bool(provider_configs))
    openai_tools = [_to_openai_tool(t) for t in _TOOLS + _CLIENT_TOOLS]
    system_text = _system_prompt(current_route)
    # REQ-2012: an Anthropic model behind an OpenAI-wire gateway (OpenRouter) caches only where a
    # block says so; a breakpoint on the system prompt caches it and the tool definitions ahead of
    # it. Other vendors cache a repeated prefix by themselves.
    system_message = {
        "role": "system",
        "content": (
            _cached_system(system_text) if model_name.startswith("anthropic/") else system_text
        ),
    }
    base_url = str((provider_configs.get(provider_key) or {}).get("base_url") or "")
    # REQ-2012: OpenRouter reports what it billed for a call when asked to.
    extra: dict[str, Any] = (
        {"extra_body": {"usage": {"include": True}}} if "openrouter.ai" in base_url else {}
    )
    prior_messages: list[Any] = []  # REQ-1850, same reasoning as the Anthropic loop above
    usage = polly_usage.Usage()
    billed: float | None = None

    for round_no in range(max_iterations):
        last_round = round_no == max_iterations - 1
        oa_messages = [system_message, *_wire_convo_to_openai_messages(convo)]
        if last_round:
            oa_messages.append(_LAST_ROUND_INSTRUCTION)
        if model_name.startswith(_GATEWAY_CACHED_MODELS):
            oa_messages = _with_openai_cache_breakpoint(oa_messages)
        # REQ-1838: the last round calls no tool (see the Anthropic loop). It is sent with no
        # tools at all: OpenRouter's Gemini and Qwen routes answered tool_choice "none" with more
        # tool calls (bake-off run 38630859730), and an OpenAI-wire call takes a history of tool
        # calls without their definitions. Anthropic requires the definitions whenever the
        # history holds tool_use blocks, so an Anthropic-wire endpoint keeps them, with
        # tool_choice none.
        tooling: dict[str, Any] = (
            {"tools": openai_tools}
            if not last_round
            else {"tools": openai_tools, "tool_choice": {"type": "none"}}
            if provider_key == "anthropic"
            else {}
        )
        # aisuite providers are synchronous (no async client) — off the event loop, same reasoning
        # as every other sync-SDK call in this codebase (e.g. provisa/llm/client.py).
        resp = await asyncio.to_thread(
            provider.chat_completions_create,
            model_name,
            oa_messages,
            **tooling,
            **cap,
            **extra,
        )
        usage = usage + polly_usage.from_openai(resp.usage)
        call_cost = getattr(resp.usage, "cost", None)
        if call_cost is not None:
            billed = (billed or 0.0) + float(call_cost)
        message = resp.choices[0].message
        calls_tools = resp.choices[0].finish_reason == "tool_calls" and bool(
            getattr(message, "tool_calls", None)
        )
        # Text beside a tool call is never shown (see the Anthropic loop); it stays in the
        # assistant content the model's history carries.
        if not calls_tools:
            if _is_answer(message.content):
                yield {"type": "text", "text": message.content}
            else:
                # A model that answers in its reasoning and leaves the reply blank: OpenRouter
                # returns that reasoning as the message's `reasoning`. It is sent as its own
                # event, which the chat panel does not show (a model's reasoning is not its
                # answer) and the bake-off records, so a blank reply can be read against it.
                reasoning = getattr(message, "reasoning", None)
                if reasoning:
                    yield {"type": "reasoning", "text": reasoning}
                yield _NO_ANSWER
            break
        if last_round:
            # The last round had no tools to call; a call it made anyway is not run.
            yield _STEPS_SPENT
            break

        wire_blocks = _openai_message_to_wire_blocks(message)
        calls = [b for b in wire_blocks if b["type"] == "tool_use"]
        client_calls, server_calls = _split_tool_calls(calls)

        tool_results = []
        for call in server_calls:
            yield {"type": "tool_use", "name": call["name"], "input": call["input"]}
            try:
                result, artifact = _split_artifact(
                    await _execute_tool(state, role, call["name"], call["input"], request=request)
                )
                content = json.dumps(result, default=str)
                is_error = False
            except Exception as exc:  # noqa: BLE001 - reported back to the model, see pragma
                content = f"Error: {exc}"
                is_error = True
                artifact = None
            if artifact is not None:
                yield artifact
            yield {"type": "tool_result", "name": call["name"], "is_error": is_error}
            tool_results.append(
                {
                    "type": "tool_result",
                    "tool_use_id": call["id"],
                    "content": content,
                    "is_error": is_error,
                }
            )

        refused, confirmed = await _checked_graph_navigates(state, role, client_calls)
        for call in client_calls:
            if call["id"] in refused:
                yield {"type": "tool_result", "name": call["name"], "is_error": True}
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": call["id"],
                        "content": refused[call["id"]],
                        "is_error": True,
                    }
                )
        client_calls = [c for c in client_calls if c["id"] not in refused]
        if client_calls:
            prepared_calls = []
            for raw_call in client_calls:
                call, note = _prepare_client_call(raw_call, confirmed.get(raw_call["id"]))
                if note is not None:
                    yield {"type": "text", "text": f"\n\n{note}"}
                prepared_calls.append(call)
                yield {
                    "type": "tool_use",
                    "name": call["name"],
                    "input": call["input"],
                    "client": True,
                }
            yield {
                "type": "awaiting_client_tools",
                "assistant_content": wire_blocks,
                "server_tool_results": tool_results,
                "pending": prepared_calls,
                "prior_messages": prior_messages,
            }
            yield polly_usage.usage_event(model_name, usage, billed)
            yield {"type": "done"}
            return

        convo.append({"role": "assistant", "content": wire_blocks})
        convo.append({"role": "user", "content": tool_results})
        prior_messages.append({"role": "assistant", "content": wire_blocks})
        prior_messages.append({"role": "user", "content": tool_results})

    yield polly_usage.usage_event(model_name, usage, billed)
    yield {"type": "done"}


async def run_chat(
    state: Any,
    role: str,
    messages: list[dict],
    *,
    max_iterations: int = 8,
    request: Any = None,
    current_route: str | None = None,
) -> AsyncIterator[dict]:
    """Drive the model's tool-use loop, yielding UI events until it stops calling tools.

    ``messages`` is the prior conversation ({role, content} with string content, or the richer
    wire-block shape when resuming after a client-tool pause — REQ-1795). The role is validated
    up front — an invalid role fails the whole chat rather than silently degrading. Dispatches to
    Anthropic's own API (REQ-1008/1796) or aisuite's multi-vendor path (REQ-1797) depending on
    ai_models.mcp_chat's configured vendor; both speak the identical wire format, so the caller
    (and the frontend) never need to know which one is active.

    ``request`` is the real, AuthMiddleware-verified HTTP request behind this call -- threaded
    through to the tools that act as the user through the admin API (REQ-2114). Optional so
    callers that use none of them keep working.

    ``current_route`` is the frontend's own current URL (REQ-1800), sent with every turn — this
    answers "what page am I on" without a tool round-trip, since the browser already knows its
    own route for free; see _system_prompt.
    """
    mcp_tools.require_role(role, state)  # raises ValueError / PermissionError

    configured, reason = await _llm_configured(state)
    if not configured:
        yield {
            "type": "error",
            "error": (
                "Polly isn't configured yet"
                + (f" ({reason})" if reason else "")
                + ". Set a vendor, model, and API key in Settings, then try again."
            ),
            "action": {"label": "Open AI Models settings", "route": "/admin/ai-models"},
        }
        yield {"type": "done"}
        return

    vendor = await _resolve_vendor(state)
    convo: list[Any] = list(messages)
    impl = (
        _run_chat_anthropic(
            state,
            role,
            convo,
            max_iterations=max_iterations,
            request=request,
            current_route=current_route,
        )
        if vendor == "anthropic"
        else _run_chat_aisuite(
            state,
            role,
            convo,
            vendor,
            max_iterations=max_iterations,
            request=request,
            current_route=current_route,
        )
    )
    async for event in impl:
        yield event
