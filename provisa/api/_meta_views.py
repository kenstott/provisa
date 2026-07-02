# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Meta-domain SQL view definitions (extracted from api/app.py).

Views replace admin tables that have text[] columns Trino cannot surface;
arrays are cast to JSON text. PostgreSQL-specific (pgwire/Trino surface).
"""

from __future__ import annotations

_META_TABLE_VIEWS: dict[str, str] = {
    "registered_tables": """
        DROP VIEW IF EXISTS registered_tables_meta CASCADE;
        CREATE VIEW registered_tables_meta AS
        SELECT id, source_id, domain_id, schema_name, table_name,
               alias, description, cache_ttl, gql_naming_convention, watermark_column,
               column_presets::text AS column_presets,
               view_sql, data_product, materialize, mv_refresh_interval,
               l1_cluster, l2_cluster, l3_cluster, clusters_computed_at,
               tenant_id
        FROM registered_tables
    """,
    "table_columns": """
        DROP VIEW IF EXISTS table_columns_meta CASCADE;
        CREATE VIEW table_columns_meta AS
        SELECT id, table_id, column_name, data_type, is_primary_key,
               alias, description, path, scope,
               mask_type, mask_pattern, mask_replace, mask_value, mask_precision,
               native_filter_type, is_foreign_key, is_alternate_key,
               object_fields::text AS object_fields,
               array_to_json(visible_to)::text  AS visible_to,
               array_to_json(unmasked_to)::text AS unmasked_to,
               array_to_json(writable_by)::text AS writable_by,
               tenant_id
        FROM table_columns
    """,
    "roles": """
        DROP VIEW IF EXISTS roles_meta CASCADE;
        CREATE VIEW roles_meta AS
        SELECT id, parent_role_id, org_id,
               array_to_json(capabilities)::text AS capabilities,
               tenant_id,
               'meta'::text AS domain_id
        FROM roles
    """,
    "roles_domain_access": """
        DROP VIEW IF EXISTS roles_domain_access CASCADE;
        CREATE VIEW roles_domain_access AS
        SELECT r.id || ':' || d.id AS id,
               r.id AS role_id, 'meta'::text AS domain_id, d.id AS accessed_domain_id
        FROM roles r
        CROSS JOIN domains d
        WHERE d.id <> ''
    """,
    "tracked_webhooks": """
        DROP VIEW IF EXISTS tracked_webhooks_meta CASCADE;
        CREATE VIEW tracked_webhooks_meta AS
        SELECT id, name, url, method, timeout_ms, returns, kind,
               inline_return_type::text AS inline_return_type,
               arguments::text AS arguments,
               array_to_json(visible_to)::text AS visible_to,
               domain_id, description, created_at, updated_at
        FROM tracked_webhooks
    """,
    "tracked_functions": """
        DROP VIEW IF EXISTS tracked_functions_meta CASCADE;
        CREATE VIEW tracked_functions_meta AS
        SELECT id, name, source_id, schema_name, function_name, returns, kind,
               arguments::text AS arguments,
               return_schema::text AS return_schema,
               array_to_json(visible_to)::text AS visible_to,
               array_to_json(writable_by)::text AS writable_by,
               domain_id, description, created_at, updated_at
        FROM tracked_functions
    """,
}
