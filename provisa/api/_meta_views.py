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
from provisa.core.models import SYSTEM_TAGS

# REQ-1375: system tags are code-defined intrinsics with no rows in the tags table, so the
# meta view unions them in from the constant — the registry a query surface sees is complete.
# Built from SYSTEM_TAGS at import time; never hand-edit the literals.
import json as _json


def _system_tag_rows_sql() -> str:
    rows = []
    for tag in SYSTEM_TAGS:
        desc = tag.description.replace("'", "''")
        applies = _json.dumps(tag.applies_to).replace(" ", "")
        rows.append(
            f"SELECT '{tag.id}' AS id, '{desc}' AS description, "
            f"'{applies}' AS applies_to, TRUE AS is_system, "
            f"'{tag.reason_policy}' AS reason_policy, "
            f"'{tag.expires_policy}' AS expires_policy, NULL AS tenant_id"
        )
    return "\n        UNION ALL\n        ".join(rows)


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
               reason_policy, expires_policy, CAST(tenant_id AS TEXT) AS tenant_id
        FROM tags
    """,
    "tag_assignments": """
        CREATE OR REPLACE VIEW tag_assignments_meta AS
        SELECT id, tag_id, object_type, source_id, table_id, column_name,
               relationship_id, object_key, reason, expires_on, tenant_id
        FROM tag_assignments
    """,
    # REQ-1375: the management report — every assignment with a planned end date, with its
    # state derived at query time. expires_on is an ISO date string, so lexicographic
    # comparison with CURRENT_DATE (cast to text) is correct on every dialect.
    "tag_expiry": """
        CREATE OR REPLACE VIEW tag_expiry AS
        SELECT id, tag_id, object_type, source_id, table_id, column_name,
               relationship_id, object_key, reason, expires_on,
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
        SELECT id, tenant_id, user_id, role_id, query_hash,
               table_ids, source, status_code, duration_ms, logged_at
        FROM query_audit_log
    """,
}
