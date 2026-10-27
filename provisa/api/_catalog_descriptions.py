# Copyright (c) 2026 Kenneth Stott
# Canary: 5f1c8b74-2d63-4a90-8e11-c47b0a9f6d32
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Curated descriptions for the built-in ``meta`` and ``ops`` catalog tables.

Every business table Provisa registers gets its description from whoever registered it. The
built-in domains have no such author: their tables are seeded by the platform itself, so without
this file every one of their columns arrives blank — and the REQ-609 ``stale_metadata`` report,
whose entire job is to name governance gaps, listed hundreds of them that no data steward could
ever close.

The seed applies these with a COALESCE (``startup_seed``), so a description edited in the UI wins
and only a blank one is filled. Keys are the registered ``table_name`` — the physical name for the
meta tables (their views are registered under it), the view name for the ops reports.
"""

# Requirements: REQ-609, REQ-884, REQ-1386

from __future__ import annotations

_TENANT = "Owning org — control-plane rows are isolated per tenant by this column"

_META: dict[str, str] = {
    "registered_tables": "Every table, view, and data product Provisa knows about, and the "
    "per-table governance and materialization settings that apply to it",
    "table_columns": "The columns of each registered table, with their masking rules, visibility "
    "grants, and semantic metadata",
    "domains": "Business domains — the unit of access control a role is granted or denied",
    "relationships": "Foreign-key and function edges between registered tables, which drive "
    "GraphQL nesting, Cypher traversal, and join planning",
    "rls_rules": "Row-level security predicates applied per (table, role) before any query runs",
    "roles": "The role hierarchy queries execute under; a role inherits its parent's grants",
    "roles_domain_access": "Which domains each role may reach — one row per role/domain pair",
    "roles_parents": "Which roles each role inherits from — one row per role/parent pair",
    "listed_names": "The names of listed columns in domains the reader does not reach — names only",
    "tracked_webhooks": "HTTP endpoints registered as callable functions in the model",
    "tracked_functions": "Source-native and Provisa-defined functions exposed as callable "
    "commands in the model",
    "tags": "The tag registry — governance labels (pii, deprecated, …) that can be attached to "
    "catalog objects, including the built-in system tags",
    "tag_assignments": "Every tag attached to a catalog object, with its justification and "
    "planned end date",
    "tag_param_values": "The closed set of values a parameterized tag accepts — an assignment "
    "naming a value outside this set is refused",
    "tag_expiry": "Tag assignments carrying a planned end date, with expiring/expired state "
    "derived at query time — the governance review queue",
    "derived_tags": "Tags computed from a table's own registration rather than assigned — its "
    "star-schema role and whether it holds data-quality scan results",
    # REQ-1443
    "sources": "Every registered source -- its identifier and its type",
    "dq_rules": "Each data-quality table's rules -- one row per check its contract declares, "
    "in the order written",
    # REQ-2125
    "data_products": "Every data product — one row per product with its own fields; its domain, "
    "member tables, commands, tags, glossary terms and profiles are reached by relationship",
    # REQ-2126
    "data_product_edges": "Each data product's links to the rest of the model — the tables and "
    "products it reaches through relationships — one row per product, target and kind",
    # REQ-2145
    "effective_column_tags": "Every tag each column carries, its own and those on the glossary "
    "term it means, each saying where it comes from",
    # REQ-2091
    "sensitive_coverage": "Every sensitive column, whether a fake covers it, the fake and synthetic "
    "rule it declares, and the latest adequacy checks on them",
    # REQ-1584: the business glossary as queryable metadata.
    "_terms": "The business vocabulary — one row per term, with the ref count and the "
    "live flag the export's admission rule computes",
    # REQ-1591
    "_domains": "Each glossary term's domains, by the one rule: its refs' domains "
    "while it holds refs, the domains declared for it otherwise",
    "_term_refs": "Each binding of a term to a physical column, resolved to that "
    "column's source, schema, table and domain",
    "_term_edges": "The term graph — how terms relate to one another, with both endpoint names",
    "_experts": "Who owns or authored each term, for routing a definition question",
    # REQ-2163
    "_axioms": "The ontology stated as RDFS, OWL and SKOS triples, one a row: each term's class "
    "or property kind, its super-classes and super-properties, a property's domain and range, "
    "and each term's label and definition",
    "_closure": "Each term with every term it is a kind of, as a class or as a property, "
    "through one or more kind-of relationships, and itself",
}

_OPS: dict[str, str] = {
    "query_audit_log": "One row per governed statement: who ran it, under which role, over which "
    "protocol surface, and how it ended",
    "usage_ranking": "Query volume and distinct consumers per registered table; a table with "
    "zero queries is a deprecation candidate",
    "deprecated_usage": "Accesses to objects tagged deprecated — the consumers who must migrate "
    "before the object can be removed",
    "judgment_review": "Every finding of the judgment review queue with the judgment's why -- "
    "reason, purpose, bases, review-by date, who decided it and when -- as audit evidence",
    "pii_access": "Accesses to objects tagged pii: who read them, under which role, over which "
    "surface",
    "policy_denials": "Statements governance refused (401 unauthenticated, 403 denied)",
    "surface_mix": "Daily query volume and distinct users per protocol surface",
    "query_health": "Daily latency and error profile per protocol surface",
    "stale_metadata": "Governance completeness gaps — catalog objects missing a description and "
    "domains missing a steward",
    "join_hotspots": "Table pairs read together in the same query, most frequent first — the "
    "materialization and caching candidates",
    "tag_usage": "One row per tag in the registry — how widely it is applied, across which kinds "
    "of object, and how much query traffic reaches what it marks",
    "traces": "One row per request — the request span, carrying its transport, role, route, "
    "engine, sources, cache result, row count, status and stage durations as attributes — and "
    "one row per piece of background work (a refresh, a scheduled job)",
    "mv_refresh_lineage": "One row per materialized-view refresh — the versions of the view's "
    "definition and of its inputs it was built from, and where each of its columns came from",
    "trace_details": "The spans under a request traced in debug detail — one row per stage, "
    "Redis command, source or engine statement and outbound call — joined to its request by "
    "trace_id and kept for a short retention",
    "metrics": "OpenTelemetry metric points emitted by Provisa and by the federation engine, "
    "compacted into Iceberg",
    "logs": "OpenTelemetry log records emitted by Provisa and by the federation engine, "
    "compacted into Iceberg",
    "org_registry": "The deployment's own tenancy — one row per org per org_admin, "
    "joining the platform-plane org record to the tenant-plane role assignment",
    "queries": "One row per statement Provisa executed, from the audit log — who ran it, under "
    "which role, over which transport, against which table, how long it took and how it ended",
    "replica_builds": "One row per replica of a source table Provisa keeps in its store — its "
    "build as it stands: requested, building, built, failed or retired, with the last build's "
    "rows, error and indexes",
    "replica_build_log": "One row per replica build that ended, completed or failed — when it "
    "started and finished, how it copied, its rows or its error; the last builds of each replica",
    "replica_build_health": "Replica builds and their failures per source and day, from the "
    "replica build log",
    "sla_breach_log": "One row per service-level breach — the latency or availability of the "
    "org's last hour past the operator's limit: when it opened, the worst and latest measure, "
    "the limit, and when it recovered",
    "service_level": "The service level of the org's last hour, from the audit log — how many "
    "queries, their latency percentiles and the share that succeeded",
}

# A model reading the catalog took ops.query_audit_log -- its route, status_code and duration_ms
# -- for the organisation's web log, and answered "the most requested page is `engine`" (Polly
# bake-off run 38630859730). Each built-in table says whose record it is.
_META_OWN = (
    ". Provisa's own catalog metadata about the data it governs, not the organisation's data"
)
_OPS_OWN = (
    ". Provisa's own record of the queries and activity run through it, not the "
    "organisation's application traffic or logs"
)

TABLE_DESCRIPTIONS: dict[str, str] = {
    **{name: text + _META_OWN for name, text in _META.items()},
    **{name: text + _OPS_OWN for name, text in _OPS.items()},
}

COLUMN_DESCRIPTIONS: dict[str, dict[str, str]] = {
    "registered_tables": {
        "id": "Surrogate key — what relationships, rls_rules, and the audit log reference",
        "source_id": "The registered source this table is read from",
        "domain_id": "Business domain the table belongs to, which decides who may reach it",
        "schema_name": "Schema (or equivalent namespace) the table lives in at the source",
        "table_name": "Table name at the source",
        "alias": "Name the table is exposed under, when it differs from the source name",
        "description": "What the table holds, shown to consumers in every catalog surface",
        "cache_ttl": "Seconds a result for this table may be served from cache",
        "gql_naming_convention": "Casing convention applied to this table's GraphQL names",
        "watermark_column": "Column whose high value marks how far incremental refresh has read",
        "view_sql": "Defining SQL, for a Provisa-managed virtual view rather than a source table",
        "product_id": "The DataProduct this table is published as a member of, if any",
        "materialize": "Whether Provisa maintains a materialization of this table",
        "mv_refresh_interval": "How often the materialization is refreshed",
        "l1_cluster": "Top-level semantic cluster, computed from the table's relationships",
        "l2_cluster": "Second-level semantic cluster within the L1 cluster",
        "l3_cluster": "Third-level semantic cluster within the L2 cluster",
        "clusters_computed_at": "When the semantic clusters were last recomputed",
        "tenant_id": _TENANT,
        # REQ-2125
        "dq_contract": "The data-quality contract a checker table's rows are the scan results "
        "of; empty for any other table",
        "profiled_table_id": "For a profile result table, the table it profiles; empty for any "
        "other table",
        "checked_table_id": "For a checker table, the table its data-quality contract scans; "
        "empty for any other table",
        # REQ-2130
        "class_term_id": "The glossary term this table's rows are instances of; empty when no "
        "term grounds it",
        # REQ-2132
        "latest_outcome": "For a checker table, its latest scan's outcome: pass when every check "
        "passed, fail otherwise; empty when it has never run",
        "latest_run_at": "For a checker table, when its latest scan ran",
    },
    # REQ-2126
    "data_product_edges": {
        "id": "Edge identifier: product, kind and target",
        "product_id": "The data product the edge starts from",
        "kind": "relationship (through tables in no product) or related (one relationship away)",
        "target_product_id": "The other data product the edge reaches, when its end is a member "
        "of one",
        "target_table_id": "The table the edge reaches, when its end is a registered table",
        "direct": "True when one relationship joins a member to the target",
        "hops": "The shortest number of relationships between a member and the target",
        "path_count": "How many paths connect the product to the target",
        "aliases": "The aliases of the relationships on those paths",
        "relationship_ids": "The relationships on those paths",
        "supporting_tables": "Tables in no product that a path runs through",
    },
    "dq_rules": {
        "id": "Rule identifier: the table and the check's position in its contract",
        "table_id": "Data-quality table whose contract declares the check",
        "domain_id": "Domain of that table",
        "checker": "Checker dialect the contract is written in: soda or great_expectations",
        "position": "The check's position in its contract, from 1",
        "column_name": "Column the check applies to; empty for a dataset-level check",
        "check_type": "The kind of check",
        "definition": "The check's arguments, as written in the contract",
        "tenant_id": _TENANT,
    },
    # REQ-1443
    "sources": {
        "id": "Source identifier",
        "type": "The source's type -- the connector it is reached through, or a checker's dialect",
    },
    # REQ-2125
    "data_products": {
        "id": "Data product identifier",
        "domain_id": "Domain the product is published in",
        "name": "Name the product is listed under",
        "owner_role": "Role that owns the product",
        "team_role": "Role of the team that maintains the product",
        "purpose": "What the product is for",
        "limitations": "What the product should not be used for, and its known gaps",
        "usage": "How to use the product",
        "version": "The product's version",
        "status": "The product's lifecycle status: draft, live, deprecated or retired",
        "status_reason": "Why the product was deprecated or retired",
        "retire_on": "When a deprecated product is planned to retire",
        "successor_id": "The product that took a retired product's members",
        "sla": "The service level the product is held to",
        "support": "Where to get support for the product",
        "support_contact": "Who to contact for support",
        "custom_properties": "The product's own named properties",
    },
    "table_columns": {
        "id": "Surrogate key for the column registration",
        "table_id": "The registered table this column belongs to",
        "column_name": "Column name at the source",
        "domain_id": "Domain of the owning table, mirrored for graph-sync consumers that key a "
        "column node without joining to registered_tables",
        # REQ-1584: the stable column identity the glossary binds to.
        "column_key": "Synthesized column identity (table_id:column_name), the join key from "
        "ontology._term_refs",
        "data_type": "Physical type as reflected from the source",
        "is_primary_key": "Whether the column is part of the table's primary key",
        "alias": "Name the column is exposed under, when it differs from the source name",
        "description": "What the column holds, shown to consumers in every catalog surface",
        "path": "Location of the value inside a nested/JSON column, when it is not top level",
        "scope": "Where the column may be used — projection, filtering, or both",
        "masks": "The ordered mask list: each entry names the roles it is for and the mask they "
        "read the value through, or none; a reader reads it through the first entry it holds",
        "native_filter_type": "Type the source expects in a filter on this column, when it "
        "differs from the projected type",
        "is_foreign_key": "Whether the column references another registered table",
        "is_alternate_key": "Whether the column uniquely identifies a row without being the "
        "primary key",
        "object_fields": "Field definitions for a structured (object) column",
        "visible_to": "Roles that may select this column; empty means no role",
        "tenant_id": _TENANT,
    },
    "domains": {
        "id": "Domain name — the identifier roles are granted access to",
        "description": "What the domain covers",
        "steward": "Role or principal accountable for the domain's metadata quality",
        "parent_domain_id": "Domain this one sits under; a role granted the parent reaches it too",
        "graphql_alias": "Name the domain is exposed under in GraphQL, when it differs",
        "org_id": "Org the domain was defined in",
        "tenant_id": _TENANT,
    },
    "relationships": {
        "id": "Relationship identifier, unique across the catalog",
        "source_table_id": "Table the edge starts at",
        "target_table_id": "Table the edge points to",
        "source_column": "Column on the source table that carries the key",
        "target_column": "Column on the target table the key matches",
        "cardinality": "one-to-one, one-to-many, or many-to-one",
        "materialize": "Whether the joined result is materialized",
        "refresh_interval": "How often the materialized join is refreshed",
        "target_function_name": "Function that resolves the target, for a function-backed edge",
        "function_arg": "Argument passed to the resolving function",
        "alias": "Name the edge is exposed under, when it differs from the target name",
        "graphql_alias": "Name the edge is exposed under in GraphQL, when it differs",
        "cypher_type": "Cypher relationship type the edge is served under",
        "graphql_field": "GraphQL field the edge is served under, in the naming convention in force",
        "disable_cypher": "Whether the edge is hidden from Cypher traversal",
        "hide_target_meta": "Whether the target's metadata is withheld from consumers of the edge",
        "same_entity": "Whether the rows the edge joins are the same entity, in two tables one term grounds",
        "source_json_key": "Key inside a JSON source column that carries the join value",
        "parameters": "Target parameters the edge feeds, each from a source column",
        "via_table_id": "Junction table the edge traverses through, for a junction-backed edge",
        "via_source_column": "Junction columns, in order, that match the source key",
        "via_target_column": "Junction columns, in order, that match the target key",
        "via_type_column": "Junction column that discriminates several relationship types",
        "via_type_value": "Discriminator value this edge is pinned to",
        "via_label_source": "Where the exposed type name comes from: column, table, or fixed",
        "owner": "Principal accountable for the edge definition",
        "version": "Revision counter, incremented on every edit",
        "needs_review": "Whether the edge was inferred and awaits confirmation",
        "tenant_id": _TENANT,
    },
    "rls_rules": {
        "id": "Rule identifier",
        "table_id": "Table the predicate is applied to",
        "domain_id": "Domain the rule belongs to",
        "role_id": "Role the predicate applies to",
        "filter_expr": "Predicate appended to every read of the table by that role",
        "description": "What the rule is for, as an access review reads it",
        "action_name": "Tracked function or webhook whose response rows the predicate filters",
        "tag_id": "Tag whose tables the predicate is applied to, beside each table's own rule",
        # REQ-2094
        "why": "Why the rule is what it is: reason, bases, review-by date, who decided it, when "
        "and on what profile evidence (JSON)",
        "tenant_id": _TENANT,
    },
    "roles": {
        "id": "Role name — what a caller executes as",
        "description": "What the role is for, as an access review reads it",
        "org_id": "Org the role was defined in",
        "capabilities": "Platform capabilities the role holds beyond data access",
        "domain_id": "Constant 'meta' — the domain this catalog view is exposed in",
        "tenant_id": _TENANT,
    },
    "listed_names": {
        "id": "The column's identity: its table's id and its name",
        "listed_domain": "The domain the listed table is in",
        "table_name": "The listed table's name",
        "column_name": "The listed column's name",
        "domain_id": "Constant 'meta' — the domain this catalog view is exposed in",
    },
    "roles_parents": {
        "id": "Composite key of the role and a role it inherits from",
        "role_id": "The role",
        "parent_role_id": "A role it inherits capabilities and grants from",
        "position": "Where the parent stands in the role's declared order, from 1",
        "domain_id": "Constant 'meta' — the domain this catalog view is exposed in",
    },
    "roles_domain_access": {
        "id": "Composite key of the role and the domain it reaches",
        "role_id": "The role",
        "domain_id": "Constant 'meta' — the domain this catalog view is exposed in",
        "accessed_domain_id": "Domain the role may reach",
    },
    "tracked_webhooks": {
        "id": "Webhook identifier",
        "name": "Name the webhook is callable under in the model",
        "url": "Endpoint invoked",
        "method": "HTTP method used",
        "timeout_ms": "Milliseconds to wait before the call is abandoned",
        "returns": "Type the call returns",
        "kind": "Whether the call reads or mutates",
        "inline_return_type": "Inline definition of the return shape, when it is not a named type",
        "arguments": "Declared arguments and their types",
        "visible_to": "Roles that may call the webhook",
        "domain_id": "Domain the webhook is exposed in",
        "description": "What the webhook does, shown to consumers",
        "created_at": "When the webhook was registered",
        "updated_at": "When the registration was last changed",
    },
    "tracked_functions": {
        "id": "Function identifier",
        "name": "Name the function is callable under in the model",
        "source_id": "Source that hosts the function",
        "schema_name": "Schema the function lives in at the source",
        "function_name": "Function name at the source",
        "returns": "Type the function returns",
        "kind": "Whether the function reads or mutates",
        "arguments": "Declared arguments and their types",
        "return_schema": "Column definitions of a table-valued return",
        "visible_to": "Roles the function is assigned to (empty: every role)",
        "domain_id": "Domain the function is exposed in",
        "description": "What the function does, shown to consumers",
        "created_at": "When the function was registered",
        "updated_at": "When the registration was last changed",
        "product_id": "The data product the function is a member of, if any",  # REQ-2125
    },
    "tags": {
        "id": "Tag name, e.g. pii or deprecated",
        "description": "What the tag asserts about the objects it is attached to",
        "applies_to": "Object kinds the tag may be attached to",
        "is_system": "Whether the tag is built in rather than defined by this deployment",
        "derived": "Whether the tag is computed from an object's own registration rather than "
        "assigned by a steward — a derived tag is read-only",
        "reason_policy": "Whether an assignment must carry a justification",
        "expires_policy": "Whether an assignment must carry a planned end date",
        "param_policy": "Whether assignments carry a parameter, attached as 'tag:value' — "
        "'entity:customer' names the entity type the column's values are",
        "tenant_id": _TENANT,
    },
    "tag_param_values": {
        "tag_id": "Parameterized tag the value belongs to",
        "value": "A permitted parameter value, e.g. customer for the entity tag",
        "description": "What the value means, shown to the steward choosing it",
        "tenant_id": _TENANT,
    },
    "tag_assignments": {
        "id": "Assignment identifier",
        "tag_id": "Tag attached, with its parameter when it has one (e.g. entity:customer)",
        "base_tag_id": "Tag attached, parameter stripped — the registry id",
        "object_type": "Kind of object tagged (table, column, relationship, command)",
        "source_id": "Source of the tagged object",
        "table_id": "Tagged table, or the table owning the tagged column",
        "column_name": "Tagged column, when the object is a column",
        "relationship_id": "Tagged relationship, when the object is a relationship",
        "command_name": "Tagged command, when the object is a command",
        "object_key": "Canonical key of the tagged object, unique across kinds",
        "reason": "Justification recorded with the assignment",
        "expires_on": "Planned end date of the assignment",
        # REQ-1585: the join key to table_columns and ontology._term_refs; null unless the
        # assignment is on a column.
        "column_key": "Synthesized column identity (table_id:column_name) of the tagged column",
        "tenant_id": _TENANT,
        "product_id": "Tagged data product, when the object is a data product",  # REQ-2125
        "term_id": "Tagged glossary term, when the object is a term",  # REQ-2145
    },
    # REQ-2145
    "effective_column_tags": {
        "id": "Identity of the row: the assignment, and for a term's tag the column it reaches",
        "column_key": "Synthesized column identity (table_id:column_name) of the column",
        "table_id": "Table the column belongs to",
        "column_name": "The column carrying the tag",
        "tag_id": "Tag carried, with its parameter when it has one",
        "base_tag_id": "Tag carried, parameter stripped — the registry id",
        "via": "column when the tag is on the column itself, term when it is on its glossary term",
        "term_id": "The glossary term the column carries the tag through, when via is term",
        "term": "That term's name",
        "reason": "Justification recorded with the assignment",
        "tenant_id": _TENANT,
    },
    "tag_expiry": {
        "id": "Assignment identifier",
        "tag_id": "Tag attached",
        "object_type": "Kind of object tagged (table, column, relationship, command)",
        "source_id": "Source of the tagged object",
        "table_id": "Tagged table, or the table owning the tagged column",
        "column_name": "Tagged column, when the object is a column",
        "relationship_id": "Tagged relationship, when the object is a relationship",
        "command_name": "Tagged command, when the object is a command",
        "object_key": "Canonical key of the tagged object, unique across kinds",
        "reason": "Justification recorded with the assignment",
        "expires_on": "Planned end date of the assignment",
        "status": "expired when the end date has passed, expiring while it is still ahead",
        "column_key": "Synthesized column identity (table_id:column_name) of the tagged "
        "column",  # REQ-1585
        "tenant_id": _TENANT,
    },
    # REQ-2091
    "sensitive_coverage": {
        "column_key": "Synthesized column identity (table_id:column_name) of the column",
        "table_id": "Table the column belongs to",
        "table_name": "Name of that table",
        "domain_id": "Domain the table is governed under",
        "column_name": "The sensitive column",
        "tags": "The sensitive tags the column carries, comma-separated",
        "has_fake": "True when the column declares a fake",
        "has_synthetic_rule": "True when the column declares a synthetic rule",
        "fake": "The fake as declared: its kind and parameters",
        "fake_kind": "The kind of fake (a method such as email, or hash, truncate, sql, ...)",
        "fake_deterministic": "True for every fake: one real value always gives one fake",
        "fake_stable": "True when the fake is also the same on every engine",
        "fake_derives_from_real": "True when the fake is computed from the real value itself "
        "(truncate, prefix, bucket, hash, encrypt, a relative or sql fake)",
        "synthetic_rule": "The synthetic rule as declared: the statement generation uses",
        "status": "covered, no_fake, or inadequate (a fake or rule failed an adequacy check)",
        "reason": "Why the column is not covered: no fake, or each failed check and its finding",
        "leak_rate": "Latest leak-rate check: the share of sampled values faked as themselves",
        "leak_rate_status": "passed, failed or not_run",
        "kind_mismatch_status": "Latest kind check against the column's profiled plausible "
        "type: passed, failed or not_run",
        "copies_real_status": "Latest check that no fake or rule reads a sensitive column real: "
        "passed, failed or not_run",
        "checked_at": "When the latest adequacy check of the column ran",
        "tenant_id": _TENANT,
    },
    "derived_tags": {
        "tag_id": "Computed tag (fact, dimension, data_quality)",
        "object_type": "Kind of object tagged; always table — derivation is a table-level fact",
        "source_id": "Source the table belongs to",
        "table_id": "Table the tag was derived for",
        "table_name": "Name of that table",
        "domain_id": "Domain the table is governed under",
        "object_key": "Canonical key of the tagged object, matching tag_assignments",
        "tenant_id": _TENANT,
    },
    # REQ-1584
    "_terms": {
        "id": "Term identifier",
        "name": "The term phrase, unique across the glossary",
        "definition": "What the term means; a term without one is a draft, never live",
        "is_abstract": "True when the term is a Concept: it grounds no column and no table "
        "(derived from its grounding)",
        "deprecated": "True when the derivation lost its last column but the row was kept",
        "retired": "True when a curator took the term out of service",
        "export_excluded": "True when the term is withheld from consuming surfaces",
        "ref_count": "How many physical columns the term binds to",
        "is_rooted": "True when the term holds at least one physical ref of its own, to a column "
        "or a whole table",
        "live": "True when the term is in service, defined, and connected over in-service "
        "edges to a term holding a physical ref",
        "status": "draft (not admitted), live (admitted), deprecated (its column went, or a "
        "curator deprecated it; in service until retired) or retired (out of service)",
        "status_reason": "Why a curator deprecated or retired the term",
        "retire_on": "The date a deprecated term is retired on, when a curator planned one",
        "tenant_id": _TENANT,
        # REQ-1387
        "grounded": "True when the term is connected over in-service edges to a term holding a "
        "physical ref, defined or not",
        "identity_gap": "True when the term grounds several tables that same-entity "
        "relationships do not all join: a curation gap",
        "term_domains": "Synthesized: the term's domains as |a|b| — what its readable-term scope tests; empty "
        "for a term with no domains",
        # REQ-2163
        "is_class": "True when the term is a Class: a table is grounded in it",
        "is_property": "True when the term is a Property: it is bound to a column",
    },
    # REQ-1591
    "_domains": {
        "id": "Term and domain",
        "term_id": "The term",
        "domain_id": "A domain the term belongs to: the domains of the tables its refs point at "
        "while it holds refs, its declared domains otherwise; * is the whole org",
        "tenant_id": _TENANT,
        "term_domains": "Synthesized: the term's domains as |a|b| — what its readable-term scope tests; empty "
        "for a term with no domains",
    },
    "_term_refs": {
        "id": "Ref identifier",
        "term_id": "Term bound by this ref",
        "table_id": "Table owning the bound column",
        "column_name": "The bound column",
        "column_key": "Synthesized column identity (table_id:column_name), the join key to "
        "table_columns",
        "source_id": "Source the bound column belongs to",
        "schema_name": "Schema of the bound column's table",
        "table_name": "Name of the bound column's table",
        "domain_id": "Domain the bound column is governed under",
        "tenant_id": _TENANT,
        "term_domains": "Synthesized: the term's domains as |a|b| — what its readable-term scope tests; empty "
        "for a term with no domains",
    },
    "_term_edges": {
        "id": "Edge identifier",
        "from_term_id": "Term the edge starts at",
        "to_term_id": "Term the edge ends at",
        "rel_type": "How the two relate (KIND_OF, RELATED_TO, PART_OF, SYNONYM_OF, …)",
        "from_term": "Name of the starting term",
        "to_term": "Name of the ending term",
        "tenant_id": _TENANT,
        "from_term_domains": "Synthesized: the from term's domains as |a|b| — what its readable-term scope tests; empty "
        "for a term with no domains",
        "to_term_domains": "Synthesized: the to term's domains as |a|b| — what its readable-term scope tests; empty "
        "for a term with no domains",
    },
    "_axioms": {
        "id": "The triple: its subject's id, predicate and object",
        "subject_term_id": "The term the triple is about",
        "subject": "That term's name; its IRI is its vocabulary IRI",
        "predicate": "The RDF, RDFS, OWL or SKOS property stated",
        "object_term_id": "The term the triple points at, when its object is a term",
        "object": "That term's name",
        "object_iri": "The object when it is an IRI: an OWL class or an XML Schema datatype",
        "object_literal": "The object when it is a literal: a label or a definition",
        "subject_domains": "Synthesized: the subject term's domains as |a|b| — what its "
        "readable-term scope tests",
        "object_domains": "Synthesized: the object term's domains as |a|b|; empty when the "
        "object is no term",
        "tenant_id": _TENANT,
    },
    "_closure": {
        "id": "The pair: its predicate, both terms' ids and its hops",
        "predicate": "rdfs:subClassOf between Classes, rdfs:subPropertyOf between Properties",
        "subject_term_id": "The narrower term",
        "subject": "That term's name",
        "object_term_id": "The broader term it is a kind of, or itself",
        "object": "That term's name",
        "hops": "0 for the term itself, 1 for one or more kind-of relationships",
        "subject_domains": "Synthesized: the narrower term's domains as |a|b| — what its "
        "readable-term scope tests",
        "object_domains": "Synthesized: the broader term's domains as |a|b|",
        "tenant_id": _TENANT,
    },
    "_experts": {
        "id": "Assignment identifier",
        "term_id": "Term this person owns or authored",
        "term_name": "Name of that term",
        "user_id": "The person; governed by ordinary column visibility",
        "kind": "expert or author",
        "tenant_id": _TENANT,
    },
    "query_audit_log": {
        "id": "Audit row identifier",
        "user_id": "Principal that issued the statement",
        "user_name": "Display name of that principal, from the tenant user directory",
        "role_id": "Role the statement executed under",
        "query_hash": "Hash of the statement text; the text itself is encrypted",
        "table_ids": "Registered tables the statement read",
        "table_names": "Those tables by qualified name (source.schema.table) as they were when "
        "the statement ran, in the same order",
        "source": "Protocol surface the statement arrived on (http, grpc, pgwire, bolt, …)",
        "status_code": "How the Provisa statement ended — 200 success, 403 denied by "
        "governance; not an application's HTTP status",
        "duration_ms": "Wall-clock milliseconds the statement took",
        "logged_at": "When the row was written",
        "route": "How Provisa answered the statement — cache, direct (one source) or engine; "
        "not a web page path",
        "row_count": "Rows the statement delivered",
        "route_reason": "Why the statement took its route",
        "sources": "Sources the statement read",
        "data_age": "How old the rows it was answered with were — a cache entry's age, or the "
        "time it was read as of; empty when read live",
        "region": "The region whose data answered it — the answering node's, or the one another "
        "region's data it read in place is kept in",
        "model_stamp": "The model version (stamp) the statement was governed under",
        "model_commit": "The environment repository commit that model equals, when proven",
        "enforced": "What was enforced on it — row filters (with the names of the session "
        "variables they read), masks by kind, row caps, and for a write its table and columns",
        "as_of": "The time the statement read bitemporal views as of, when it set one",
        "as_recorded": "The time the statement read bitemporal views as recorded at, when it set one",
    },
    "usage_ranking": {
        "id": "The registered table this row ranks",
        "source_id": "Source the table is read from",
        "domain_id": "Domain the table belongs to",
        "schema_name": "Schema the table lives in at the source",
        "table_name": "Table name at the source",
        "query_count": "Statements that read the table",
        "distinct_users": "Distinct principals that read it",
        "last_queried_at": "When it was last read",
    },
    "deprecated_usage": {
        "id": "Composite key of the audit row and the deprecation tag it hit",
        "table_id": "Table read",
        "table_name": "Name of the table read",
        "domain_id": "Domain the table belongs to",
        "object_type": "Whether the deprecation is on the table or on a column",
        "column_name": "Deprecated column, when the tag is on a column",
        "reason": "Justification recorded with the deprecation",
        "expires_on": "Date the deprecated object is planned for removal",
        "user_id": "Principal that read it",
        "user_name": "Display name of that principal, from the tenant user directory",
        "role_id": "Role the read executed under",
        "source": "Protocol surface the read arrived on",
        "logged_at": "When the read happened",
    },
    "judgment_review": {
        "id": "The finding",
        "object_type": "What the judgment is on: tag_assignment, column, mask_entry, row_rule, "
        "write_grant, posture",
        "object_key": "Which one",
        "judgment": "The kind of judgment: tag, fake, mask, row_rule, write_grant, posture",
        "category": "wrong, dated_by_data, dated_by_context, undecided or worth_a_look",
        "finding": "What was found (profile_drift, type_changed, basis_changed, review_due, ...)",
        "observed": "What changed, then and now, as JSON",
        "first_seen": "When the finding was first seen",
        "last_seen": "When a review last saw it",
        "resolved_at": "When it was resolved; null while open",
        "resolved_by": "Who resolved it",
        "resolution": "How: reaffirmed, dismissed with a note, or cleared when no longer seen",
        "reason": "The facts the decider relied on",
        "purpose": "What the decision is for",
        "bases": "The named bases it cites",
        "review_on": "Its review-by date",
        "decided_by": "Who made the decision",
        "decided_at": "When it was made",
        "domain_id": "The domain of the table it is on",
    },
    "pii_access": {
        "id": "Composite key of the audit row and the pii tag it hit",
        "table_id": "Table read",
        "table_name": "Name of the table read",
        "domain_id": "Domain the table belongs to",
        "object_type": "Whether the pii tag is on the table or on a column",
        "pii_column": "Column carrying the pii tag, when the tag is on a column",
        "user_id": "Principal that read it",
        "user_name": "Display name of that principal, from the tenant user directory",
        "role_id": "Role the read executed under",
        "source": "Protocol surface the read arrived on",
        "status_code": "How the read ended — 200 served, 403 denied by governance",
        "logged_at": "When the read happened",
    },
    "policy_denials": {
        "id": "Audit row identifier",
        "user_id": "Principal whose statement was refused",
        "user_name": "Display name of that principal, from the tenant user directory",
        "role_id": "Role the statement executed under",
        "query_hash": "Hash of the statement text; the text itself is encrypted",
        "table_ids": "Registered tables the statement referenced",
        "source": "Protocol surface the statement arrived on",
        "status_code": "401 unauthenticated, 403 denied by governance",
        "duration_ms": "Wall-clock milliseconds before the refusal",
        "logged_at": "When the refusal happened",
    },
    "surface_mix": {
        "id": "Composite key of the surface, the day, and the org",
        "source": "Protocol surface (http, grpc, pgwire, bolt, …)",
        "day": "Calendar day the counts cover",
        "query_count": "Statements that arrived on the surface that day",
        "distinct_users": "Distinct principals that used it that day",
    },
    "query_health": {
        "id": "Composite key of the surface, the day, and the org",
        "source": "Protocol surface (http, grpc, pgwire, bolt, …)",
        "day": "Calendar day the counts cover",
        "query_count": "Statements that arrived on the surface that day",
        "error_count": "How many of them ended at 400 or above",
        "avg_duration_ms": "Mean wall-clock milliseconds across them",
        "max_duration_ms": "Slowest statement that day, in milliseconds",
    },
    "stale_metadata": {
        "id": "Kind-prefixed key of the object with the gap",
        "object_type": "table, column, or domain",
        "object_name": "Name of the object with the gap",
        "domain_id": "Domain the object belongs to — who owns closing the gap",
        "issue": "missing_description, or missing_steward for a domain",
    },
    "join_hotspots": {
        "id": "Composite key of the two tables and the org",
        "table_id_a": "Lower-numbered table of the pair",
        "table_name_a": "Name of that table",
        "table_id_b": "Higher-numbered table of the pair",
        "table_name_b": "Name of that table",
        "co_occurrence_count": "Statements that read both tables together",
        "last_seen_at": "When the pair was last read together",
    },
    "tag_usage": {
        "id": "The tag name, which is its key",
        "tag_id": "The tag name",
        "is_system": "True for the tags Provisa defines in code, false for org-defined ones",
        "assignment_count": "Times the tag is applied, across every kind of object",
        "sources_tagged": "Distinct sources carrying the tag",
        "tables_tagged": "Distinct tables carrying the tag, at table or column level",
        "columns_tagged": "Distinct columns carrying the tag",
        "relationships_tagged": "Distinct relationships carrying the tag",
        "commands_tagged": "Distinct commands carrying the tag",
        "expiring_count": "Assignments with an expiry date set",
        "expired_count": "Assignments whose expiry date has passed",
        "query_count": "Statements that read a table carrying the tag",
        "distinct_users": "Users who ran those statements",
        "last_queried_at": "When a table carrying the tag was last read",
    },
    "traces": {
        "trace_id": "Identifier shared by every span of one end-to-end operation",
        "span_id": "Identifier of this span",
        "parent_span_id": "Span that this one ran inside, empty for the root",
        "span_name": "What the span measures, e.g. provisa.query.trino",
        "span_kind": "OpenTelemetry span kind — server, client, internal",
        "service_name": "Service that emitted the span (provisa, trino, …)",
        "service_namespace": "Namespace the emitting service belongs to",
        "timestamp": "Span start (UTC)",
        "end_timestamp": "Span end (UTC)",
        "duration": "Elapsed nanoseconds",
        "status_code": "OpenTelemetry status — 0 unset, 1 ok, 2 error",
        "status_message": "Error detail, when the span ended in error",
        "scope_name": "Instrumentation scope that emitted the span",
        "span_attributes": "All span attributes, as JSON",
        "resource_attributes": "Attributes of the emitting resource, as JSON",
        "table_name": "Registered table a debug-detail statement span read; empty on a request record",
        "domain_id": "Domain a debug-detail statement span ran in; empty on a request record",
        "role_id": "Role a debug-detail statement span ran under; empty on a request record",
        "query_text": "Statement text of a debug-detail statement span; empty on a request record",
        "tenant_id": "Org the span was emitted for",
        "_date": "Partition day the span was compacted into",
    },
    "mv_refresh_lineage": {
        "mv_id": "Materialized view that was refreshed",
        "target_table": "Table the refresh wrote",
        "definition_version": "Version of the view's definition the refresh ran",
        "input_version": "Version of the inputs the refresh read",
        "input_version_kind": "What the input version is: an Iceberg snapshot, a watermark, a "
        "freshness token or the refresh time",
        "trace_id": "Trace of the refresh",
        "refreshed_at": "When the refresh ran (UTC)",
        "column_derivations": "Each output column with the source columns and expression it "
        "comes from, as JSON",
        "tenant_id": "Org the refresh ran for",
    },
    "trace_details": {
        "trace_id": "Identifier shared by every span of one end-to-end operation",
        "span_id": "Identifier of this span",
        "parent_span_id": "Span that this one ran inside, empty for the root",
        "span_name": "What the span measures, e.g. provisa.query.trino",
        "span_kind": "OpenTelemetry span kind — server, client, internal",
        "service_name": "Service that emitted the span (provisa, trino, …)",
        "service_namespace": "Namespace the emitting service belongs to",
        "timestamp": "Span start (UTC)",
        "end_timestamp": "Span end (UTC)",
        "duration": "Elapsed nanoseconds",
        "status_code": "OpenTelemetry status — 0 unset, 1 ok, 2 error",
        "status_message": "Error detail, when the span ended in error",
        "scope_name": "Instrumentation scope that emitted the span",
        "span_attributes": "All span attributes, as JSON",
        "resource_attributes": "Attributes of the emitting resource, as JSON",
        "table_name": "Registered table a debug-detail statement span read; empty on a request record",
        "domain_id": "Domain a debug-detail statement span ran in; empty on a request record",
        "role_id": "Role a debug-detail statement span ran under; empty on a request record",
        "query_text": "Statement text of a debug-detail statement span; empty on a request record",
        "tenant_id": "Org the span was emitted for",
        "_date": "Partition day the span was compacted into",
    },
    "metrics": {
        "timestamp": "Point timestamp (UTC)",
        "start_timestamp": "Start of the interval the point covers, for cumulative instruments",
        "metric_name": "Instrument name",
        "metric_description": "What the instrument measures, as declared by its emitter",
        "metric_unit": "Unit of the value (ms, By, 1, …)",
        "metric_type": "Instrument type — sum, gauge, histogram",
        "service_name": "Service that emitted the point (provisa, trino, …)",
        "service_namespace": "Namespace the emitting service belongs to",
        "scope_name": "Instrumentation scope that emitted the point",
        "metric_attributes": "Attributes distinguishing this series, as JSON",
        "resource_attributes": "Attributes of the emitting resource, as JSON",
        "value": "The measurement",
        "tenant_id": "Org the point was emitted for",
        "_date": "Partition day the point was compacted into",
    },
    "logs": {
        "timestamp": "When the record was produced (UTC)",
        "observed_timestamp": "When the collector received it (UTC)",
        "trace_id": "Trace the record was emitted inside, when there was one",
        "span_id": "Span the record was emitted inside, when there was one",
        "severity_number": "OpenTelemetry severity, 1 (trace) through 24 (fatal)",
        "severity_text": "Severity as the emitter named it (INFO, ERROR, …)",
        "body": "The message",
        "service_name": "Service that emitted the record (provisa, trino, …)",
        "service_namespace": "Namespace the emitting service belongs to",
        "scope_name": "Instrumentation scope that emitted the record",
        "log_attributes": "Attributes attached to the record, as JSON — an exception's stack "
        "trace arrives here",
        "resource_attributes": "Attributes of the emitting resource, as JSON",
        "tenant_id": "Org the record was emitted for",
        "_date": "Partition day the record was compacted into",
    },
    "queries": {
        "id": "Identifier of the statement's audit row",
        "trace_id": "Trace of the request the statement ran in — joins to traces",
        "logged_at": "When the statement finished (UTC)",
        "duration_ms": "Elapsed milliseconds",
        "status_code": "Outcome of the Provisa statement — 200 served, 4xx refused, 5xx failed; "
        "not an application's HTTP status",
        "user_id": "User who ran the statement",
        "role_id": "Role the statement executed under",
        "source": "Transport the statement arrived on (http, pgwire, flight, bolt, grpc, …)",
        "route": "How Provisa answered the statement — cache, direct, engine or api (comma-joined "
        "when a GraphQL request's fields took different routes), not a web page path; empty "
        "when it was refused",
        "row_count": "Rows the statement delivered; empty when it failed or was refused",
        "query_hash": "SHA-256 of the statement text; the text itself is read per statement by "
        "an admin and is never a column",
        # REQ-2035: who saw which data, where -- one record across the org's regions.
        "region": "Region that answered the statement — the one whose copy it read",
        "table_id": "Registered table the statement read — its first, when it read several",
        "table_name": "Name of that table",
        "domain_id": "Domain of that table",
        "table_count": "Number of registered tables the statement read",
    },
    "replica_builds": {
        "id": "The replica's source, schema and table, dot-joined",
        "source_id": "The registered source the replicated table is read from",
        "schema_name": "Schema of the table at the source",
        "table_name": "The table at the source",
        "status": "requested, building, idle (built, nothing pending), failed, or retired (no "
        "longer declared; dropped after a grace period)",
        "requested_reason": "Why the last build was asked for: model, definition, hot, refresh, "
        "operator or read",
        "method": "How the running or last build copies: engine_statement or stream_batches",
        "load_kind": "bulk_stream or row_copy, of the running or last build",
        "started_at": "When the running or last build started",
        "finished_at": "When the standing replica's build completed; empty: there is no replica",
        "rows_copied": "Rows the running build has copied, or the last build's total",
        "completed_rows": "Rows of the last build that copied the whole table",
        "next_refresh_at": "When the replica is built again; empty: only on request",
        "failed_at": "When the last build failed",
        "failed_attempts": "Builds failed in a row since the last one that completed",
        "error_code": "The last failure's code, when its cause is a named one",
        "error_message": "The last failure, in English",
        "indexes_wanted": "Indexes the standing replica's landing asked for",
        "indexes_built": "Of those, the ones built",
        "indexes_failed": "Of those, the ones that could not be built",
    },
    "replica_build_log": {
        "id": "Surrogate key, in the order the builds ended",
        "source_id": "The registered source the replicated table is read from",
        "schema_name": "Schema of the table at the source",
        "table_name": "The table at the source",
        "started_at": "When the build started; empty when its start was not recorded",
        "finished_at": "When the build completed or failed",
        "outcome": "completed or failed",
        "method": "How a completed build copied: engine_statement, stream_batches or delta",
        "rows_copied": "Rows a completed build copied",
        "error_code": "A failed build's code, when its cause is a named one",
        "error_message": "A failed build's error, in English",
    },
    "sla_breach_log": {
        "id": "Surrogate key, in the order the breaches opened",
        "kind": "latency (the 99th percentile past its limit) or availability (below its floor)",
        "observed": "The measure when the breach opened: milliseconds, or percent available",
        "last_observed": "The measure at the latest evaluation while the breach lasted",
        "limit_value": "The limit in force when the breach opened",
        "window_start": "Start of the hour the opening evaluation measured",
        "window_end": "End of that hour",
        "started_at": "When the breach opened",
        "last_seen_at": "The latest evaluation that found it still in breach",
        "resolved_at": "When the measure came back within its limit; empty: still open",
    },
    "service_level": {
        "id": "last_hour",
        "window_start": "Start of the hour measured",
        "window_end": "End of the hour measured: now",
        "query_count": "Statements Provisa executed in the hour",
        "avg_ms": "Their mean duration, in milliseconds",
        "p50_ms": "Their median duration, in milliseconds",
        "p95_ms": "Their 95th-percentile duration, in milliseconds",
        "p99_ms": "Their 99th-percentile duration, in milliseconds",
        "availability_pct": "Percent of them answered without a server error",
    },
    "replica_build_health": {
        "id": "Composite key of the source and the day",
        "source_id": "The registered source whose replicas were built",
        "day": "Calendar day the builds ended",
        "build_count": "Replica builds of the source that ended that day",
        "failure_count": "How many of them failed",
        "failed_replicas": "Distinct replicas of the source with a failed build that day",
        "last_failed_at": "When the last of those failures was",
    },
    "org_registry": {
        "org_id": "Org the row is about",
        "org_name": "Display name the org was created under",
        "provisioning_state": "Where the org is in provisioning — a schema exists only once ready",
        "created_at": "When the org was created",
        "org_admin_user_id": "A user holding org_admin in that org, null when nobody does yet",
        "org_admin_email": "That admin's email",
        "org_admin_display_name": "That admin's display name",
    },
}
