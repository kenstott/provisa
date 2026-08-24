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

TABLE_DESCRIPTIONS: dict[str, str] = {
    # meta
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
    "tracked_webhooks": "HTTP endpoints registered as callable functions in the semantic layer",
    "tracked_functions": "Source-native and Provisa-defined functions exposed as callable "
    "commands in the semantic layer",
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
    # REQ-1584: the business glossary as queryable metadata.
    "glossary_terms": "The business vocabulary — one row per term, with the ref count and the "
    "live flag the export's admission rule computes",
    "glossary_term_refs": "Each binding of a term to a physical column, resolved to that "
    "column's source, schema, table and domain",
    "glossary_term_edges": "The term graph — how terms relate to one another, with both "
    "endpoint names",
    "glossary_term_experts": "Who owns or authored each term, for routing a definition question",
    # ops
    "query_audit_log": "One row per governed statement: who ran it, under which role, over which "
    "protocol surface, and how it ended",
    "usage_ranking": "Query volume and distinct consumers per registered table; a table with "
    "zero queries is a deprecation candidate",
    "deprecated_usage": "Accesses to objects tagged deprecated — the consumers who must migrate "
    "before the object can be removed",
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
    "traces": "OpenTelemetry spans emitted by Provisa and by the federation engine, compacted "
    "into Iceberg",
    "metrics": "OpenTelemetry metric points emitted by Provisa and by the federation engine, "
    "compacted into Iceberg",
    "logs": "OpenTelemetry log records emitted by Provisa and by the federation engine, "
    "compacted into Iceberg",
    "org_registry": "The deployment's own tenancy (REQ-1301) — one row per org per org_admin, "
    "joining the platform-plane org record to the tenant-plane role assignment",
    "queries": "The query spans of the trace stream — one row per statement Provisa executed, "
    "with the table, domain, role, and elapsed time it ran under",
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
        "column_presets": "Named column subsets consumers can select instead of listing columns",
        "view_sql": "Defining SQL, for a Provisa-managed virtual view rather than a source table",
        "data_product": "Whether the table is published as a data product",
        "materialize": "Whether Provisa maintains a materialization of this table",
        "mv_refresh_interval": "How often the materialization is refreshed",
        "l1_cluster": "Top-level semantic cluster, computed from the table's relationships",
        "l2_cluster": "Second-level semantic cluster within the L1 cluster",
        "l3_cluster": "Third-level semantic cluster within the L2 cluster",
        "clusters_computed_at": "When the semantic clusters were last recomputed",
        "tenant_id": _TENANT,
    },
    "table_columns": {
        "id": "Surrogate key for the column registration",
        "table_id": "The registered table this column belongs to",
        "column_name": "Column name at the source",
        # REQ-1584: the stable column identity the glossary binds to.
        "column_key": "Synthesized column identity (table_id:column_name), the join key from "
        "glossary_term_refs",
        "data_type": "Physical type as reflected from the source",
        "is_primary_key": "Whether the column is part of the table's primary key",
        "alias": "Name the column is exposed under, when it differs from the source name",
        "description": "What the column holds, shown to consumers in every catalog surface",
        "path": "Location of the value inside a nested/JSON column, when it is not top level",
        "scope": "Where the column may be used — projection, filtering, or both",
        "mask_type": "Masking applied to roles outside unmasked_to (hash, redact, partial, …)",
        "mask_pattern": "Pattern the mask preserves, for pattern-preserving mask types",
        "mask_replace": "Replacement character the mask substitutes",
        "mask_value": "Fixed value substituted by a constant mask",
        "mask_precision": "How much of the value a partial mask leaves visible",
        "native_filter_type": "Type the source expects in a filter on this column, when it "
        "differs from the projected type",
        "is_foreign_key": "Whether the column references another registered table",
        "is_alternate_key": "Whether the column uniquely identifies a row without being the "
        "primary key",
        "object_fields": "Field definitions for a structured (object) column",
        "visible_to": "Roles that may select this column; empty means unrestricted outside "
        "lockdown domains",
        "unmasked_to": "Roles that see the raw value rather than the masked one",
        "writable_by": "Roles that may write this column",
        "tenant_id": _TENANT,
    },
    "domains": {
        "id": "Domain name — the identifier roles are granted access to",
        "description": "What the domain covers",
        "steward": "Role or principal accountable for the domain's metadata quality",
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
        "disable_cypher": "Whether the edge is hidden from Cypher traversal",
        "hide_target_meta": "Whether the target's metadata is withheld from consumers of the edge",
        "source_json_key": "Key inside a JSON source column that carries the join value",
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
        "tenant_id": _TENANT,
    },
    "roles": {
        "id": "Role name — what a caller executes as",
        "parent_role_id": "Role this one inherits grants from",
        "org_id": "Org the role was defined in",
        "capabilities": "Platform capabilities the role holds beyond data access",
        "domain_id": "Constant 'meta' — the domain this catalog view is exposed in",
        "tenant_id": _TENANT,
    },
    "roles_domain_access": {
        "id": "Composite key of the role and the domain it reaches",
        "role_id": "The role",
        "domain_id": "Constant 'meta' — the domain this catalog view is exposed in",
        "accessed_domain_id": "Domain the role may reach",
    },
    "tracked_webhooks": {
        "id": "Webhook identifier",
        "name": "Name the webhook is callable under in the semantic layer",
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
        "name": "Name the function is callable under in the semantic layer",
        "source_id": "Source that hosts the function",
        "schema_name": "Schema the function lives in at the source",
        "function_name": "Function name at the source",
        "returns": "Type the function returns",
        "kind": "Whether the function reads or mutates",
        "arguments": "Declared arguments and their types",
        "return_schema": "Column definitions of a table-valued return",
        "visible_to": "Roles that may call the function",
        "writable_by": "Roles that may call it when it mutates",
        "domain_id": "Domain the function is exposed in",
        "description": "What the function does, shown to consumers",
        "created_at": "When the function was registered",
        "updated_at": "When the registration was last changed",
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
        # REQ-1585: the join key to table_columns and glossary_term_refs; null unless the
        # assignment is on a column.
        "column_key": "Synthesized column identity (table_id:column_name) of the tagged column",
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
    "glossary_terms": {
        "id": "Term identifier",
        "name": "The term phrase, unique across the glossary",
        "definition": "What the term means; a term without one is a proposal, never live",
        "is_abstract": "True when the term names a concept with no column of its own",
        "deprecated": "True when the derivation lost its last column but the row was kept",
        "retired": "True when a curator took the term out of service",
        "export_excluded": "True when the term is withheld from consuming surfaces",
        "ref_count": "How many physical columns the term binds to",
        "is_rooted": "True when the term holds at least one physical ref of its own",
        "live": "True when the term is in service, defined, and connected over in-service "
        "edges to a term holding a physical ref",
        "tenant_id": _TENANT,
    },
    "glossary_term_refs": {
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
    },
    "glossary_term_edges": {
        "id": "Edge identifier",
        "from_term_id": "Term the edge starts at",
        "to_term_id": "Term the edge ends at",
        "rel_type": "How the two relate (KIND_OF, RELATED_TO, PART_OF, SYNONYM_OF, …)",
        "from_term": "Name of the starting term",
        "to_term": "Name of the ending term",
        "tenant_id": _TENANT,
    },
    "glossary_term_experts": {
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
        "query_hash": "Hash of the statement text; the text itself is encrypted (REQ-689)",
        "table_ids": "Registered tables the statement read",
        "source": "Protocol surface the statement arrived on (http, grpc, pgwire, bolt, …)",
        "status_code": "How the statement ended — 200 success, 403 denied by governance",
        "duration_ms": "Wall-clock milliseconds the statement took",
        "logged_at": "When the row was written",
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
        "query_hash": "Hash of the statement text; the text itself is encrypted (REQ-689)",
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
        "table_name": "Registered table the statement read, extracted from the span attributes",
        "domain_id": "Domain the statement ran in, extracted from the span attributes",
        "role_id": "Role the statement executed under, extracted from the span attributes",
        "query_text": "Statement text, extracted from the span attributes",
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
        "trace_id": "Identifier shared by every span of one end-to-end operation",
        "span_id": "Identifier of this span",
        "parent_span_id": "Span that this one ran inside, empty for the root",
        "span_name": "What the span measures, e.g. provisa.query.trino",
        "service_name": "Service that emitted the span (provisa, trino, …)",
        "timestamp": "Statement start (UTC)",
        "end_timestamp": "Statement end (UTC)",
        "duration": "Elapsed nanoseconds",
        "status_code": "OpenTelemetry status — 0 unset, 1 ok, 2 error",
        "table_name": "Registered table the statement read",
        "domain_id": "Domain the statement ran in",
        "role_id": "Role the statement executed under",
        "query_text": "Statement text",
        "_date": "Partition day the span was compacted into",
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
