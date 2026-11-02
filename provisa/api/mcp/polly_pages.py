# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7e12c9-40d6-4f8a-9e15-c7a2d86f0b41
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every route of the app, and what Polly can do with it: the one table.

A row says how a page is set (navigate's ``state`` keys, or query parameters on its route) and
what read_screen reports for it beyond the visible fields and result grids. The table is written
into Polly's system prompt, and tests/unit/test_polly_pages.py fails when a route of
provisa-ui/src/App.tsx has no row, or a route that publishes to screenState says it does not.
"""

from __future__ import annotations

from typing import NamedTuple

FIELDS = "visible fields"


class PollyPage(NamedTuple):
    what: str
    sets: str = "nothing beyond opening it"
    reads: str = FIELDS


PAGES: dict[str, PollyPage] = {
    "/": PollyPage("redirects to /query"),
    "/setup": PollyPage("redirects to /"),
    "/login": PollyPage("sign in; only a signed-out visitor sees it"),
    "/register": PollyPage("register; only a signed-out visitor sees it"),
    "/welcome": PollyPage("a new org's welcome"),
    "/docs": PollyPage("the product documentation"),
    "/demos/:name": PollyPage("a named demo's opening page"),
    "/sources": PollyPage("data sources", "?source= etc. as its links do"),
    "/tables": PollyPage(
        "the registered tables, as a list or the ERD",
        "query parameters: view=erd, table=<id> (its panel), domains=<a,b> (only these shown), "
        "objects=<domain.table,...> (highlighted), collapsed=<a,b> or collapsed=all",
        "inputs: view, table, domains, objects, collapsed",
    ),
    "/relationships": PollyPage(
        "relationships, as a list or the same ERD",
        "query parameters: view=erd, relationship=<id>, domains, objects, collapsed, as /tables",
        "inputs: view, relationship, domains, objects, collapsed",
    ),
    "/team": PollyPage("the org's members"),
    "/security": PollyPage("redirects to /security/roles"),
    "/security/roles": PollyPage("roles"),
    "/security/rls": PollyPage("row-level security rules"),
    "/query": PollyPage("GraphQL explorer", "state.query", "editor"),
    "/schema": PollyPage("the schema explorer"),
    "/graph": PollyPage("Cypher graph", "state.query, state.view", "editor"),
    "/sparql": PollyPage("SPARQL graph", "state.query", "editor"),
    "/sql": PollyPage("SQL editor", "state.sql", "editor, result grids"),
    "/nl": PollyPage("Natural Language; type the question with actions"),
    "/grpc": PollyPage(
        "gRPC explorer",
        "state.grpcMethod, state.grpcBody",
        "inputs: method, body, status, error",
    ),
    "/jsonapi": PollyPage(
        "JSON:API explorer",
        "state.jsonapiUrl",
        "inputs: url, table, fields, include, filter, sort, status, error",
    ),
    "/odata": PollyPage(
        "OData: the service root and metadata addresses to paste into Excel, Power BI or any "
        "OData client"
    ),
    "/openapi": PollyPage(
        "OpenAPI docs",
        "state.openApiUrl, state.openApiBody",
        "inputs: operation, parameters, body, requestUrl, status, error",
    ),
    "/python": PollyPage("Python tab", "state.code, state.newTab, state.title", "editor"),
    "/notebook": PollyPage("Notebook tab", "state.cells", "the run's output in navigate's result"),
    "/science": PollyPage("redirects to /python"),
    "/explore": PollyPage("this same chat, full page"),
    "/views": PollyPage("views"),
    "/metrics": PollyPage("governed metrics"),
    "/data-products": PollyPage(
        "data products",
        "query parameters: product=<id>, contract=<id>, search=<text>",
        "inputs: product, contract, search, toReviewOnly, creating, editing",
    ),
    "/commands": PollyPage("commands"),
    "/actions": PollyPage("redirects to /commands"),
    "/lineage": PollyPage("lineage"),
    "/admin": PollyPage("redirects to the first admin page the role may open"),
    "/admin/requests": PollyPage("approval requests"),
    "/admin/ontology": PollyPage(
        "the glossary's terms, or the ontology graph",
        "state.panel 'graph' or 'terms', state.term <id>, and state.graph, the graph's controls: "
        "flat true (2D) or false (3D), layout 'auto', 'force' or 'layered', preset 'ontology' or "
        "'taxonomy', layers from 'ontology', 'domains', 'data', 'columns'; or query parameters "
        "panel= and term=",
        "inputs: panel, term, termName, editing, graph",
    ),
    "/admin/overview": PollyPage("the admin dashboard"),
    "/admin/domains": PollyPage("domains"),
    "/admin/secrets": PollyPage("the org's secrets"),
    "/admin/my-secrets": PollyPage("the user's own secrets"),
    "/admin/cache": PollyPage("cache settings"),
    "/admin/scheduled-tasks": PollyPage("scheduled tasks"),
    "/admin/federation-engine": PollyPage("federation engine settings"),
    "/admin/org-engine": PollyPage("the org's engine"),
    "/admin/encryption": PollyPage("encryption settings"),
    "/admin/auth": PollyPage("authentication settings"),
    "/admin/system-health": PollyPage("system health"),
    "/admin/observability": PollyPage("observability settings"),
    "/admin/mcp-server": PollyPage("the MCP server's settings"),
    "/admin/local-users": PollyPage("local users"),
    "/admin/orgs": PollyPage("organizations"),
    "/admin/ai-models": PollyPage("AI model settings"),
    "/admin/package-registries": PollyPage("package registries"),
    "/admin/container-registries": PollyPage("container registries"),
    "/admin/catalogs": PollyPage("catalogs"),
    "/admin/reference-library": PollyPage("the reference library"),
    "/admin/metadata-export": PollyPage("metadata export"),
    "/admin/import": PollyPage("import"),
    "/admin/tags": PollyPage("tags"),
    "/admin/classification-kinds": PollyPage("classification kinds"),
    "/admin/reports": PollyPage("reports"),
    "/admin/security": PollyPage("security settings"),
    "/admin/maintenance": PollyPage("maintenance"),
    "/admin/deployment-settings": PollyPage("deployment settings"),
    "/admin/billing": PollyPage("billing"),
    "/admin/environments": PollyPage("environments"),
    "/admin/email": PollyPage("email settings"),
}


def _row(route: str, page: PollyPage) -> str:
    return f"{route}: {page.what}. Set: {page.sets}. read_screen: {page.reads}."


SYSTEM_SECTION = (
    "THE PAGES: every page of the app, how to set it and what read_screen reports. A navigate "
    "with state or query parameters on these reads the screen back before it answers. On the "
    "ERD (/tables or /relationships with view=erd) a table's panel, the focus and the collapsed "
    "domains are all in the link; its exports are the buttons erd-export-svg, erd-export-png, "
    "erd-export-jpeg and erd-export-json, pressed with navigate's actions by testId, and a "
    "whole domain collapses with collapsed=<domain id>.\n"
    + "\n".join(f"- {_row(route, page)}" for route, page in PAGES.items())
)
