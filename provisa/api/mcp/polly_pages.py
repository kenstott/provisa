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
    "/sources": PollyPage(
        "data sources. + Source opens the picker of every source type; the JDBC form picks a "
        "driver (jdbc-source-driver-select), fills its URL or host fields, and Detect "
        "(jdbc-source-detect) ticks the capabilities the source passes. A driver jar is a file on "
        "the user's machine: they choose it themselves",
        "?source= etc. as its links do",
    ),
    "/tables": PollyPage(
        "the registered tables, as a list or the ERD",
        "query parameters: view=erd, table=<id> (its panel), domains=<a,b> (only these shown), "
        "objects=<domain.table,...> (highlighted), collapsed=<a,b> or collapsed=all, "
        "layout=<saved layout name>. + Model (tables-model-toggle) opens the entity/fact form; "
        "its drop down (tables-model-menu) offers tables-model-entity-fact, tables-model-star "
        "and tables-model-vault, the Star Schema and Data Vault wizards, stepped with wizard-next "
        "and wizard-back and ended with wizard-create",
        "inputs: view, table, domains, objects, modeling (entityFact, star, vault or null), "
        "wizardStep (number, count, title), collapsed, layout, layoutUnsaved",
    ),
    "/relationships": PollyPage(
        "relationships, as a list or the same ERD",
        "query parameters: view=erd, relationship=<id>, domains, objects, collapsed, layout, "
        "as /tables",
        "inputs: view, relationship, domains, objects, collapsed, layout, layoutUnsaved",
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
        "OData explorer: the service root and metadata addresses to paste into Excel, Power BI or "
        "any OData client, and the role's entity sets, each with a query builder and its rows",
        "state.odataUrl, a read of one entity set as /data/odata/<set>?<options> with any of "
        "$filter, $select, $expand, $orderby, $top, $skip, $count=true, opened in its panel with "
        "those options set; state.autoRun true runs it",
        "inputs: entitySets, set, url, filter, select, expand, orderby, top, skip, count, status, "
        "error, rows, total, nextPage, warnings, handoffError",
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
        "panel= and term=. Find terms (glossary-generate-menu, then glossary-find-terms-btn) "
        "reads a document by URL or file, or a registered table; find-terms-start starts the "
        "scan. A term's delete is glossary-delete-btn, then glossary-delete-confirm-confirm",
        "inputs: panel, term, termName, editing, graph, findTerms (the dialog is open)",
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
    "/admin/observability": PollyPage("the org's traces and debug tracing"),
    "/admin/platform-observability": PollyPage("the deployment's telemetry and query engine"),
    "/admin/mcp-server": PollyPage("the MCP server's settings"),
    "/admin/local-users": PollyPage("local users"),
    "/admin/orgs": PollyPage("organizations"),
    "/admin/ai-models": PollyPage("AI model settings"),
    "/admin/package-registries": PollyPage("package registries"),
    "/admin/container-registries": PollyPage("container registries"),
    "/admin/catalogs": PollyPage("catalogs"),
    "/library": PollyPage("the reference library"),
    "/admin/metadata-export": PollyPage("metadata export"),
    "/admin/import": PollyPage("import"),
    "/admin/tags": PollyPage(
        "tags. The tag form's Required switch (tags-required) makes every object of the kinds "
        "in Applies to carry the tag; one without it is a missing row in the tags report. Its "
        "Allowed Domains field (tags-allowed-domains) offers the tag only on objects of those "
        "domains and requires it only there; none selected means all domains"
    ),
    "/admin/classification-kinds": PollyPage("classification kinds"),
    "/admin/reports": PollyPage("reports"),
    "/admin/security": PollyPage("security settings"),
    "/admin/maintenance": PollyPage("maintenance"),
    "/admin/deployment-settings": PollyPage("deployment settings"),
    "/admin/platform": PollyPage(
        "the platform's regions, the default region grant and each org's grant, applied on "
        "restart; read-only in a demo sandbox"
    ),
    "/admin/regions": PollyPage(
        "the regions the platform granted the org and the store it keeps each kind of data in "
        "there, read-only"
    ),
    "/admin/billing": PollyPage("billing"),
    "/admin/environments": PollyPage("environments"),
    "/admin/email": PollyPage("email settings"),
}


def _row(route: str, page: PollyPage) -> str:
    return f"{route}: {page.what}. Set: {page.sets}. read_screen: {page.reads}."


SYSTEM_SECTION = (
    "THE PAGES: every page of the app, how to set it and what read_screen reports. On every "
    "page read_screen also reports dataNode: the data node a coordinator-served UI sends its "
    "data calls to (the address chosen in the NavBar's data address control), or null when the "
    "UI is not coordinator-served. A navigate "
    "with state or query parameters on these reads the screen back before it answers. On the "
    "ERD (/tables or /relationships with view=erd) a table's panel, the focus, the collapsed "
    "domains and the saved layout shown are all in the link; its exports are in the canvas "
    "bar's Download menu: press erd-download, then erd-export-svg, erd-export-png, "
    "erd-export-jpeg or erd-export-json, with navigate's actions by testId, and a whole domain "
    "collapses with collapsed=<domain id>. Its Layout menu (erd-layout-menu) chooses "
    "erd-layout-automatic or erd-layout-choose-<name>, saves with erd-layout-save, saves under "
    "a new name with erd-layout-save-as (fill erd-layout-name, press erd-layout-save-as-ok) and "
    "deletes with erd-layout-delete, then erd-layout-delete-confirm-confirm.\n"
    + "\n".join(f"- {_row(route, page)}" for route, page in PAGES.items())
)
