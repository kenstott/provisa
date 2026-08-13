# Copyright (c) 2026 Kenneth Stott
# Canary: b994d8ff-89f4-4e9a-a4e3-3747a04445ac
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Meta-domain SQL view definitions (extracted from api/app.py).

These views expose the admin tables to the engine surface. They are dialect-neutral:
the control plane's list-valued columns are ``JSON`` everywhere (portable SQLAlchemy
type — never PG ``TEXT[]``), so no ``array_to_json``/``::text`` normalizers are needed,
and ``DROP VIEW`` takes no ``CASCADE``. Runs verbatim on PostgreSQL, SQLite, MySQL, …
"""

from __future__ import annotations

# REQ-1132/REQ-1134: meta (catalog) CORE vs GOVERNANCE column split + the meta domain id. The single
# source of truth lives in provisa.security.rights so every query surface shares it; re-exported here
# for callers near the view definitions.
from provisa.security.rights import GOVERNANCE_META_COLUMNS, META_DOMAIN_ID  # noqa: E402,F401
from provisa.core.models import DERIVED_TAGS, SYSTEM_TAGS
from provisa.dq.contract import CHECKERS

# REQ-1375/REQ-1443: system and derived tags are code-defined intrinsics with no rows in the tags
# table, so the meta view unions them in from the constants — the registry a query surface sees is
# complete. Built at import time; never hand-edit the literals.
import json as _json


def _system_tag_rows_sql() -> str:
    rows = []
    for tag in SYSTEM_TAGS + DERIVED_TAGS:
        desc = tag.description.replace("'", "''")
        applies = _json.dumps(tag.applies_to).replace(" ", "")
        rows.append(
            f"SELECT '{tag.id}' AS id, '{desc}' AS description, "
            f"'{applies}' AS applies_to, TRUE AS is_system, "
            f"{'TRUE' if tag.derived else 'FALSE'} AS derived, "
            f"'{tag.reason_policy}' AS reason_policy, "
            f"'{tag.expires_policy}' AS expires_policy, NULL AS tenant_id"
        )
    return "\n        UNION ALL\n        ".join(rows)


# REQ-1443: the checker source types, as a SQL IN-list. Built from the constant so the view and
# provisa.core.derived_tags cannot disagree about what a data-quality source is.
_CHECKER_TYPES_SQL = ", ".join(f"'{name}'" for name in sorted(CHECKERS))


_META_TABLE_VIEWS: dict[str, str] = {
    # REQ-1373/1377: the tag registry and its assignments as queryable metadata, so
    # governance reporting (e.g. expiring deprecations) runs through the governed pipeline.
    # applies_to and tenant_id are CAST to TEXT: the system-tag branches supply applies_to
    # as a string literal and tenant_id as a bare NULL, and PG's pairwise UNION resolution
    # types those as text — refusing to match the table's JSONB/UUID columns. The casts
    # make every branch text, the same shape SQLite (JSON/UUID-as-TEXT) already returns.
    "tags": f"""
        CREATE OR REPLACE VIEW tags_meta AS
        {_system_tag_rows_sql()}
        UNION ALL
        SELECT id, description, CAST(applies_to AS TEXT) AS applies_to, is_system,
               FALSE AS derived, reason_policy, expires_policy,
               CAST(tenant_id AS TEXT) AS tenant_id
        FROM tags
    """,
    # REQ-1443: derived tags are computed, never assigned, so they have no rows in
    # tag_assignments and cannot be unioned into it — a governance query that treats a
    # derivation as an assignment would offer the operator an unassign that does nothing.
    # They get their own view, whose WHERE clauses restate provisa.core.derived_tags.
    "derived_tags": f"""
        CREATE OR REPLACE VIEW derived_tags AS
        SELECT rt.modeling_role AS tag_id, 'table' AS object_type, rt.source_id,
               rt.id AS table_id, rt.table_name, rt.domain_id,
               'table:' || CAST(rt.id AS TEXT) AS object_key,
               CAST(rt.tenant_id AS TEXT) AS tenant_id
        FROM registered_tables rt
        WHERE rt.modeling_role IN ('fact', 'dimension')
        UNION ALL
        SELECT 'data_quality', 'table', rt.source_id, rt.id, rt.table_name, rt.domain_id,
               'table:' || CAST(rt.id AS TEXT), CAST(rt.tenant_id AS TEXT)
        FROM registered_tables rt
        JOIN sources s ON s.id = rt.source_id
        WHERE s.type IN ({_CHECKER_TYPES_SQL}) AND rt.dq_contract IS NOT NULL
    """,
    "tag_assignments": """
        CREATE OR REPLACE VIEW tag_assignments_meta AS
        SELECT id, tag_id, object_type, source_id, table_id, column_name,
               relationship_id, command_name, object_key, reason, expires_on, tenant_id
        FROM tag_assignments
    """,
    # REQ-1375: the management report — every assignment with a planned end date, with its
    # state derived at query time. expires_on is an ISO date string, so lexicographic
    # comparison with CURRENT_DATE (cast to text) is correct on every dialect.
    "tag_expiry": """
        CREATE OR REPLACE VIEW tag_expiry AS
        SELECT id, tag_id, object_type, source_id, table_id, column_name,
               relationship_id, command_name, object_key, reason, expires_on,
               CASE WHEN expires_on < CAST(CURRENT_DATE AS TEXT)
                    THEN 'expired' ELSE 'expiring' END AS status,
               tenant_id
        FROM tag_assignments
        WHERE expires_on IS NOT NULL
    """,
    "registered_tables": """
        CREATE OR REPLACE VIEW registered_tables_meta AS
        SELECT id, source_id, domain_id, schema_name, table_name,
               alias, description, cache_ttl, gql_naming_convention, watermark_column,
               column_presets,
               view_sql, data_product, materialize, mv_refresh_interval,
               l1_cluster, l2_cluster, l3_cluster, clusters_computed_at,
               tenant_id
        FROM registered_tables
    """,
    "table_columns": """
        CREATE OR REPLACE VIEW table_columns_meta AS
        SELECT id, table_id, column_name, data_type, is_primary_key,
               alias, description, path, scope,
               mask_type, mask_pattern, mask_replace, mask_value, mask_precision,
               native_filter_type, is_foreign_key, is_alternate_key,
               object_fields,
               visible_to,
               unmasked_to,
               writable_by,
               tenant_id
        FROM table_columns
    """,
    "roles": """
        CREATE OR REPLACE VIEW roles_meta AS
        SELECT id, parent_role_id, org_id,
               capabilities,
               tenant_id,
               'meta' AS domain_id
        FROM roles
    """,
    "roles_domain_access": """
        CREATE OR REPLACE VIEW roles_domain_access AS
        SELECT r.id || ':' || d.id AS id,
               r.id AS role_id, 'meta' AS domain_id, d.id AS accessed_domain_id
        FROM roles r
        CROSS JOIN domains d
        WHERE d.id <> ''
    """,
    "tracked_webhooks": """
        CREATE OR REPLACE VIEW tracked_webhooks_meta AS
        SELECT id, name, url, method, timeout_ms, returns, kind,
               inline_return_type,
               arguments,
               visible_to,
               domain_id, description, created_at, updated_at
        FROM tracked_webhooks
    """,
    "tracked_functions": """
        CREATE OR REPLACE VIEW tracked_functions_meta AS
        SELECT id, name, source_id, schema_name, function_name, returns, kind,
               arguments,
               return_schema,
               visible_to,
               writable_by,
               domain_id, description, created_at, updated_at
        FROM tracked_functions
    """,
}

# REQ-884: Internal operational/observability logs exposed as first-class tables in
# the built-in ``ops`` domain, so telemetry is queryable through the governed pipeline
# (pgwire/SQL/GraphQL/Cypher) under role + domain access control — not only via the
# Python export path or raw control-plane JDBC, which bypass governance.
#
# Registry: to expose another internal log, add one ``source_table -> exposed_view``
# entry here plus its view DDL in ``_OPS_LOG_TABLE_VIEWS``. The seed
# (``startup_seed._seed_ops_domain``) and catalog population handle the rest — no new
# subsystem. The encrypted ``query_text_enc`` column is deliberately NOT exposed; its
# plaintext is only reachable via the authorised admin decrypt path (REQ-689).
_OPS_LOG_TABLE_ALIAS: dict[str, str] = {
    "query_audit_log": "query_audit_log_ops",
}

_OPS_LOG_TABLE_VIEWS: dict[str, str] = {
    "query_audit_log": """
        CREATE OR REPLACE VIEW query_audit_log_ops AS
        SELECT q.id, q.user_id, ud.display_name AS user_name, q.role_id, q.query_hash,
               q.table_ids, q.source, q.status_code, q.duration_ms, q.logged_at
        FROM query_audit_log q
        LEFT JOIN user_directory ud ON ud.user_id = q.user_id
    """,
}


# REQ-1386: helper view unnesting query_audit_log.table_ids to one row per
# (audit row, table id) — the join spine for the report views below. JSON array
# unnesting has no dialect-neutral spelling, so this is the single per-dialect
# DDL; every report view built on it stays portable. Not registered in the
# catalog — internal only.
def _ops_table_usage_ddl(dialect: str) -> str:
    header = "CREATE OR REPLACE VIEW ops_table_usage AS"
    if dialect == "sqlite":
        body = """
        SELECT q.id, q.user_id, q.role_id, q.source,
               q.status_code, q.duration_ms, q.logged_at,
               CAST(j.value AS INTEGER) AS table_id
        FROM query_audit_log q, json_each(q.table_ids) j
        """
    elif dialect == "mysql":
        body = """
        SELECT q.id, q.user_id, q.role_id, q.source,
               q.status_code, q.duration_ms, q.logged_at, jt.table_id
        FROM query_audit_log q,
             JSON_TABLE(q.table_ids, '$[*]' COLUMNS (table_id INT PATH '$')) AS jt
        """
    elif dialect == "duckdb":
        body = """
        SELECT q.id, q.user_id, q.role_id, q.source,
               q.status_code, q.duration_ms, q.logged_at,
               CAST(unnest(from_json(q.table_ids, '["VARCHAR"]')) AS INTEGER) AS table_id
        FROM query_audit_log q
        """
    else:
        # PostgreSQL (table_ids is JSONB per AUDIT_SCHEMA_SQL); any other dialect
        # fails loudly at CREATE rather than silently mis-parsing.
        body = """
        SELECT q.id, q.user_id, q.role_id, q.source,
               q.status_code, q.duration_ms, q.logged_at,
               CAST(j.elem AS INTEGER) AS table_id
        FROM query_audit_log q,
             jsonb_array_elements_text(q.table_ids) AS j(elem)
        """
    return f"{header}\n{body}"


# REQ-1386: management/operations report views in the built-in ``ops`` domain.
# Each is registered as a first-class ops-domain table (source ``provisa-admin``)
# by ``startup_seed._seed_ops_domain`` — always seeded on install, not demo data.
# Every view exposes an ``id`` column (its primary key at registration time).
# Table-level granularity throughout: the audit log records table_ids and the
# query text is encrypted (REQ-689), so element-level usage, masking outcome,
# and denying-rule id await the REQ-1386 log-write prerequisites.
_OPS_REPORT_VIEWS: dict[str, str] = {
    # Query count and distinct consumers per registered table; never-queried
    # tables surface with query_count = 0 (deprecation candidates).
    "usage_ranking": """
        CREATE OR REPLACE VIEW usage_ranking AS
        SELECT rt.id AS id, rt.source_id, rt.domain_id,
               rt.schema_name, rt.table_name,
               COUNT(u.id) AS query_count,
               COUNT(DISTINCT u.user_id) AS distinct_users,
               MAX(u.logged_at) AS last_queried_at
        FROM registered_tables rt
        LEFT JOIN ops_table_usage u ON u.table_id = rt.id
        GROUP BY rt.id, rt.source_id, rt.domain_id,
                 rt.schema_name, rt.table_name
    """,
    # One row per access to a table carrying (or containing a column carrying)
    # the 'deprecated' system tag — the consumers blocking safe removal.
    "deprecated_usage": """
        CREATE OR REPLACE VIEW deprecated_usage AS
        SELECT CAST(u.id AS TEXT) || ':' || CAST(ta.id AS TEXT) AS id,
               u.table_id, rt.table_name, rt.domain_id,
               ta.object_type, ta.column_name, ta.reason, ta.expires_on,
               u.user_id, ud.display_name AS user_name, u.role_id, u.source, u.logged_at
        FROM ops_table_usage u
        JOIN tag_assignments ta
          ON ta.table_id = u.table_id AND ta.tag_id = 'deprecated'
        JOIN registered_tables rt ON rt.id = u.table_id
        LEFT JOIN user_directory ud ON ud.user_id = u.user_id
    """,
    # One row per access to a table carrying (or containing a column carrying)
    # the 'pii' system tag: who, under which role, over which surface.
    "pii_access": """
        CREATE OR REPLACE VIEW pii_access AS
        SELECT CAST(u.id AS TEXT) || ':' || CAST(ta.id AS TEXT) AS id,
               u.table_id, rt.table_name, rt.domain_id,
               ta.object_type, ta.column_name AS pii_column,
               u.user_id, ud.display_name AS user_name, u.role_id, u.source, u.status_code,
               u.logged_at
        FROM ops_table_usage u
        JOIN tag_assignments ta
          ON ta.table_id = u.table_id AND ta.tag_id = 'pii'
        JOIN registered_tables rt ON rt.id = u.table_id
        LEFT JOIN user_directory ud ON ud.user_id = u.user_id
    """,
    # Access attempts rejected by governance (401 unauthenticated / 403 denied).
    "policy_denials": """
        CREATE OR REPLACE VIEW policy_denials AS
        SELECT q.id, q.user_id, ud.display_name AS user_name, q.role_id, q.query_hash, q.table_ids,
               q.source, q.status_code, q.duration_ms, q.logged_at
        FROM query_audit_log q
        LEFT JOIN user_directory ud ON ud.user_id = q.user_id
        WHERE q.status_code IN (401, 403)
    """,
    # Daily traffic per protocol surface (the audit log's ``source`` column).
    "surface_mix": """
        CREATE OR REPLACE VIEW surface_mix AS
        SELECT source || ':' || SUBSTR(CAST(logged_at AS TEXT), 1, 10) AS id,
               source, SUBSTR(CAST(logged_at AS TEXT), 1, 10) AS day,
               COUNT(*) AS query_count,
               COUNT(DISTINCT user_id) AS distinct_users
        FROM query_audit_log
        GROUP BY source, SUBSTR(CAST(logged_at AS TEXT), 1, 10)
    """,
    # Daily latency/error profile per protocol surface. Portable aggregates only
    # (percentile_cont is PG-only); p95 arrives with the REQ-1386 rollup tables.
    "query_health": """
        CREATE OR REPLACE VIEW query_health AS
        SELECT source || ':' || SUBSTR(CAST(logged_at AS TEXT), 1, 10) AS id,
               source, SUBSTR(CAST(logged_at AS TEXT), 1, 10) AS day,
               COUNT(*) AS query_count,
               SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) AS error_count,
               AVG(duration_ms) AS avg_duration_ms,
               MAX(duration_ms) AS max_duration_ms
        FROM query_audit_log
        GROUP BY source, SUBSTR(CAST(logged_at AS TEXT), 1, 10)
    """,
    # Governance-completeness gaps: tables/columns missing a description,
    # domains missing a steward (REQ-609 PENDING state).
    "stale_metadata": """
        CREATE OR REPLACE VIEW stale_metadata AS
        SELECT 'table:' || CAST(rt.id AS TEXT) AS id, 'table' AS object_type,
               rt.table_name AS object_name, rt.domain_id,
               'missing_description' AS issue
        FROM registered_tables rt
        WHERE rt.description IS NULL OR rt.description = ''
        UNION ALL
        SELECT 'column:' || CAST(tc.id AS TEXT), 'column',
               rt.table_name || '.' || tc.column_name, rt.domain_id,
               'missing_description'
        FROM table_columns tc
        JOIN registered_tables rt ON rt.id = tc.table_id
        WHERE tc.description IS NULL OR tc.description = ''
        UNION ALL
        SELECT 'domain:' || d.id, 'domain', d.id, d.id,
               'missing_steward'
        FROM domains d
        WHERE d.steward IS NULL AND d.id <> ''
    """,
    # Table pairs most often read in the same query — materialization/caching
    # candidates. Pairs ordered table_id_a < table_id_b to count each pair once.
    "join_hotspots": """
        CREATE OR REPLACE VIEW join_hotspots AS
        SELECT CAST(a.table_id AS TEXT) || ':' || CAST(b.table_id AS TEXT) AS id,
               a.table_id AS table_id_a, ra.table_name AS table_name_a,
               b.table_id AS table_id_b, rb.table_name AS table_name_b,
               COUNT(*) AS co_occurrence_count,
               MAX(a.logged_at) AS last_seen_at
        FROM ops_table_usage a
        JOIN ops_table_usage b
          ON b.id = a.id AND b.table_id > a.table_id
        JOIN registered_tables ra ON ra.id = a.table_id
        JOIN registered_tables rb ON rb.id = b.table_id
        GROUP BY a.table_id, ra.table_name, b.table_id, rb.table_name
    """,
    # One row per tag in the registry — how widely it is applied and how much traffic
    # reaches what it marks. Driven from tags_meta, not the tags table, so the code-defined
    # system tags (models.SYSTEM_TAGS, which have no rows) are present; a tag nobody has
    # applied still surfaces, with zeroes. The two aggregates are separate derived tables on
    # purpose: joining assignments to usage in one pass multiplies each assignment by the
    # statements that read it, and the counts stop meaning what their names say. Usage counts
    # DISTINCT statements over DISTINCT tagged tables, so a table carrying the same tag at
    # table level and on three columns still counts one statement once.
    "tag_usage": """
        CREATE OR REPLACE VIEW tag_usage AS
        SELECT t.id AS id, t.id AS tag_id, t.is_system,
               COALESCE(a.assignment_count, 0) AS assignment_count,
               COALESCE(a.sources_tagged, 0) AS sources_tagged,
               COALESCE(a.tables_tagged, 0) AS tables_tagged,
               COALESCE(a.columns_tagged, 0) AS columns_tagged,
               COALESCE(a.relationships_tagged, 0) AS relationships_tagged,
               COALESCE(a.commands_tagged, 0) AS commands_tagged,
               COALESCE(a.expiring_count, 0) AS expiring_count,
               COALESCE(a.expired_count, 0) AS expired_count,
               COALESCE(q.query_count, 0) AS query_count,
               COALESCE(q.distinct_users, 0) AS distinct_users,
               q.last_queried_at AS last_queried_at
        FROM tags_meta t
        LEFT JOIN (
            SELECT tag_id,
                   COUNT(*) AS assignment_count,
                   COUNT(DISTINCT source_id) AS sources_tagged,
                   COUNT(DISTINCT table_id) AS tables_tagged,
                   COUNT(DISTINCT CASE WHEN object_type = 'column'
                                       THEN object_key END) AS columns_tagged,
                   COUNT(DISTINCT relationship_id) AS relationships_tagged,
                   COUNT(DISTINCT command_name) AS commands_tagged,
                   SUM(CASE WHEN expires_on IS NOT NULL THEN 1 ELSE 0 END) AS expiring_count,
                   SUM(CASE WHEN expires_on < CAST(CURRENT_DATE AS TEXT)
                            THEN 1 ELSE 0 END) AS expired_count
            FROM tag_assignments
            GROUP BY tag_id
        ) a ON a.tag_id = t.id
        LEFT JOIN (
            SELECT tt.tag_id,
                   COUNT(DISTINCT u.id) AS query_count,
                   COUNT(DISTINCT u.user_id) AS distinct_users,
                   MAX(u.logged_at) AS last_queried_at
            FROM (SELECT DISTINCT tag_id, table_id FROM tag_assignments
                  WHERE table_id IS NOT NULL) tt
            JOIN ops_table_usage u ON u.table_id = tt.table_id
            GROUP BY tt.tag_id
        ) q ON q.tag_id = t.id
    """,
}
