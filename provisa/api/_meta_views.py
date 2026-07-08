# Copyright (c) 2026 Kenneth Stott
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

_META_TABLE_VIEWS: dict[str, str] = {
    "registered_tables": """
        DROP VIEW IF EXISTS registered_tables_meta;
        CREATE VIEW registered_tables_meta AS
        SELECT id, source_id, domain_id, schema_name, table_name,
               alias, description, cache_ttl, gql_naming_convention, watermark_column,
               column_presets,
               view_sql, data_product, materialize, mv_refresh_interval,
               l1_cluster, l2_cluster, l3_cluster, clusters_computed_at,
               tenant_id
        FROM registered_tables
    """,
    "table_columns": """
        DROP VIEW IF EXISTS table_columns_meta;
        CREATE VIEW table_columns_meta AS
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
        DROP VIEW IF EXISTS roles_meta;
        CREATE VIEW roles_meta AS
        SELECT id, parent_role_id, org_id,
               capabilities,
               tenant_id,
               'meta' AS domain_id
        FROM roles
    """,
    "roles_domain_access": """
        DROP VIEW IF EXISTS roles_domain_access;
        CREATE VIEW roles_domain_access AS
        SELECT r.id || ':' || d.id AS id,
               r.id AS role_id, 'meta' AS domain_id, d.id AS accessed_domain_id
        FROM roles r
        CROSS JOIN domains d
        WHERE d.id <> ''
    """,
    "tracked_webhooks": """
        DROP VIEW IF EXISTS tracked_webhooks_meta;
        CREATE VIEW tracked_webhooks_meta AS
        SELECT id, name, url, method, timeout_ms, returns, kind,
               inline_return_type,
               arguments,
               visible_to,
               domain_id, description, created_at, updated_at
        FROM tracked_webhooks
    """,
    "tracked_functions": """
        DROP VIEW IF EXISTS tracked_functions_meta;
        CREATE VIEW tracked_functions_meta AS
        SELECT id, name, source_id, schema_name, function_name, returns, kind,
               arguments,
               return_schema,
               visible_to,
               writable_by,
               domain_id, description, created_at, updated_at
        FROM tracked_functions
    """,
}
