# Copyright (c) 2026 Kenneth Stott
# Canary: b2c3d4e5-f6a7-8901-bcde-f12345678901
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL catalog proxy.

Intercepts information_schema and pg_catalog queries and answers them
from CompilationContext without a Trino round-trip. Uses DuckDB in-memory
as the query engine so clients can send arbitrary JOINs and WHERE clauses.
"""

# Requirements: REQ-127, REQ-128, REQ-363

from __future__ import annotations

import logging
import re
import time

log = logging.getLogger(__name__)

_SET_RE = re.compile(r"^\s*SET\b", re.IGNORECASE)
_SHOW_RE = re.compile(r"^\s*SHOW\b", re.IGNORECASE)
_TXN_RE = re.compile(
    r"^\s*(BEGIN|START\s+TRANSACTION|START|COMMIT|ROLLBACK|DISCARD|RESET|DEALLOCATE|SAVEPOINT|RELEASE)\b",
    re.IGNORECASE,
)

_SCALAR_FN_RE = re.compile(
    r"^\s*SELECT\s+(?:pg_catalog\.)?(current_user|session_user|current_database\(\)|current_schema\(\)|version\(\)|pg_backend_pid\(\))\s*$",
    re.IGNORECASE,
)

_INTERCEPT_SCHEMAS = frozenset({"information_schema", "pg_catalog"})

_TABLE_MAP: dict[tuple[str, str], str] = {
    ("information_schema", "schemata"): "_is_schemata",
    ("information_schema", "tables"): "_is_tables",
    ("information_schema", "columns"): "_is_columns",
    ("information_schema", "views"): "_is_views",
    ("pg_catalog", "pg_namespace"): "_pg_namespace",
    ("pg_catalog", "pg_class"): "_pg_class",
    ("pg_catalog", "pg_attribute"): "_pg_attribute",
    ("pg_catalog", "pg_type"): "_pg_type",
    ("pg_catalog", "pg_attrdef"): "_pg_attrdef",
    ("pg_catalog", "pg_description"): "_pg_description",
    ("pg_catalog", "pg_index"): "_pg_index",
    ("pg_catalog", "pg_constraint"): "_pg_constraint",
    ("pg_catalog", "pg_proc"): "_pg_proc",
    ("pg_catalog", "pg_roles"): "_pg_roles",
    ("pg_catalog", "pg_user"): "_pg_user",
    ("pg_catalog", "pg_auth_members"): "_pg_auth_members",
    ("pg_catalog", "pg_database"): "_pg_database",
    ("pg_catalog", "pg_settings"): "_pg_settings",
    ("pg_catalog", "pg_tables"): "_pg_tables",
    ("pg_catalog", "pg_stat_user_tables"): "_pg_stat_user_tables",
    ("pg_catalog", "pg_statio_user_tables"): "_pg_stat_user_tables",
    ("pg_catalog", "pg_am"): "_pg_am",
    ("pg_catalog", "pg_tablespace"): "_pg_tablespace",
    ("pg_catalog", "pg_conversion"): "_pg_conversion",
    ("pg_catalog", "pg_shdescription"): "_pg_shdescription",
    ("pg_catalog", "pg_extension"): "_pg_extension",
    ("pg_catalog", "pg_enum"): "_pg_enum",
    ("pg_catalog", "pg_stat_activity"): "_pg_stat_activity",
    ("pg_catalog", "pg_trigger"): "_pg_trigger",
    ("pg_catalog", "pg_inherits"): "_pg_inherits",
    ("pg_catalog", "pg_rewrite"): "_pg_rewrite",
    ("pg_catalog", "pg_depend"): "_pg_depend",
    ("pg_catalog", "pg_shdepend"): "_pg_shdepend",
    ("pg_catalog", "pg_aggregate"): "_pg_aggregate",
    ("pg_catalog", "pg_language"): "_pg_language",
    ("pg_catalog", "pg_operator"): "_pg_operator",
    ("pg_catalog", "pg_opfamily"): "_pg_opfamily",
    ("pg_catalog", "pg_opclass"): "_pg_opclass",
    ("pg_catalog", "pg_amop"): "_pg_amop",
    ("pg_catalog", "pg_amproc"): "_pg_amproc",
    ("pg_catalog", "pg_cast"): "_pg_cast",
    ("pg_catalog", "pg_collation"): "_pg_collation",
    ("pg_catalog", "pg_range"): "_pg_range",
    ("pg_catalog", "pg_foreign_table"): "_pg_foreign_table",
    ("pg_catalog", "pg_foreign_server"): "_pg_foreign_server",
    ("pg_catalog", "pg_user_mapping"): "_pg_user_mapping",
    ("pg_catalog", "pg_user_mappings"): "_pg_user_mappings",
    ("pg_catalog", "pg_foreign_data_wrapper"): "_pg_foreign_data_wrapper",
    ("pg_catalog", "pg_sequence"): "_pg_sequence",
    ("pg_catalog", "pg_policy"): "_pg_policy",
    ("pg_catalog", "pg_partitioned_table"): "_pg_partitioned_table",
    ("pg_catalog", "pg_publication"): "_pg_publication",
    ("pg_catalog", "pg_subscription"): "_pg_subscription",
    ("pg_catalog", "pg_event_trigger"): "_pg_event_trigger",
    ("pg_catalog", "pg_stat_user_indexes"): "_pg_stat_user_indexes",
    ("pg_catalog", "pg_locks"): "_pg_locks",
    ("pg_catalog", "pg_stat_ssl"): "_pg_stat_ssl",
    ("pg_catalog", "pg_timezone_names"): "_pg_timezone_names",
    ("pg_catalog", "pg_timezone_abbrevs"): "_pg_timezone_abbrevs",
    ("information_schema", "key_column_usage"): "_is_key_column_usage",
    ("information_schema", "table_constraints"): "_is_table_constraints",
    ("information_schema", "referential_constraints"): "_is_referential_constraints",
    ("information_schema", "role_table_grants"): "_is_role_table_grants",
    ("information_schema", "role_column_grants"): "_is_role_column_grants",
    ("information_schema", "triggers"): "_is_triggers",
    ("information_schema", "sequences"): "_is_sequences",
    ("information_schema", "routines"): "_is_routines",
    ("information_schema", "parameters"): "_is_parameters",
    ("information_schema", "enabled_roles"): "_is_enabled_roles",
    ("information_schema", "applicable_roles"): "_is_applicable_roles",
}

_CATALOG_TABLE_NAMES = frozenset(t for _, t in _TABLE_MAP)

# Stable OID assignments for system objects surfaced in pg_class/pg_attribute.
# 8001+ for pg_catalog tables, 9001+ for information_schema views.
_PG_CAT_TABLE_NAMES: list[str] = sorted(k[1] for k in _TABLE_MAP if k[0] == "pg_catalog")
_PG_CAT_TABLE_OIDS: dict[str, int] = {n: 8001 + i for i, n in enumerate(_PG_CAT_TABLE_NAMES)}

_IS_VIEW_NAMES: list[str] = [
    "schemata",
    "tables",
    "columns",
    "views",
    "key_column_usage",
    "table_constraints",
    "referential_constraints",
    "role_table_grants",
    "role_column_grants",
    "triggers",
    "sequences",
    "routines",
    "parameters",
    "enabled_roles",
    "applicable_roles",
]
_IS_VIEW_OIDS: dict[str, int] = {n: 9001 + i for i, n in enumerate(_IS_VIEW_NAMES)}

# Column definitions sourced from the live PostgreSQL 16 instance (information_schema.columns).
# Keys: information_schema view name OR pg_catalog table name.
# Values: ordered list of (column_name, pg_data_type).
_SYSTEM_TABLE_COLUMNS: dict[str, list[tuple[str, str]]] = {
    "schemata": [
        ("catalog_name", "name"),
        ("schema_name", "name"),
        ("schema_owner", "name"),
        ("default_character_set_catalog", "name"),
        ("default_character_set_schema", "name"),
        ("default_character_set_name", "name"),
        ("sql_path", "character varying"),
    ],
    "tables": [
        ("table_catalog", "name"),
        ("table_schema", "name"),
        ("table_name", "name"),
        ("table_type", "character varying"),
        ("self_referencing_column_name", "name"),
        ("reference_generation", "character varying"),
        ("user_defined_type_catalog", "name"),
        ("user_defined_type_schema", "name"),
        ("user_defined_type_name", "name"),
        ("is_insertable_into", "character varying"),
        ("is_typed", "character varying"),
        ("commit_action", "character varying"),
    ],
    "columns": [
        ("table_catalog", "name"),
        ("table_schema", "name"),
        ("table_name", "name"),
        ("column_name", "name"),
        ("ordinal_position", "integer"),
        ("column_default", "character varying"),
        ("is_nullable", "character varying"),
        ("data_type", "character varying"),
        ("character_maximum_length", "integer"),
        ("character_octet_length", "integer"),
        ("numeric_precision", "integer"),
        ("numeric_precision_radix", "integer"),
        ("numeric_scale", "integer"),
        ("datetime_precision", "integer"),
        ("interval_type", "character varying"),
        ("interval_precision", "integer"),
        ("character_set_catalog", "name"),
        ("character_set_schema", "name"),
        ("character_set_name", "name"),
        ("collation_catalog", "name"),
        ("collation_schema", "name"),
        ("collation_name", "name"),
        ("domain_catalog", "name"),
        ("domain_schema", "name"),
        ("domain_name", "name"),
        ("udt_catalog", "name"),
        ("udt_schema", "name"),
        ("udt_name", "name"),
        ("scope_catalog", "name"),
        ("scope_schema", "name"),
        ("scope_name", "name"),
        ("maximum_cardinality", "integer"),
        ("dtd_identifier", "name"),
        ("is_self_referencing", "character varying"),
        ("is_identity", "character varying"),
        ("identity_generation", "character varying"),
        ("identity_start", "character varying"),
        ("identity_increment", "character varying"),
        ("identity_maximum", "character varying"),
        ("identity_minimum", "character varying"),
        ("identity_cycle", "character varying"),
        ("is_generated", "character varying"),
        ("generation_expression", "character varying"),
        ("is_updatable", "character varying"),
    ],
    "views": [
        ("table_catalog", "name"),
        ("table_schema", "name"),
        ("table_name", "name"),
        ("view_definition", "character varying"),
        ("check_option", "character varying"),
        ("is_updatable", "character varying"),
        ("is_insertable_into", "character varying"),
        ("is_trigger_updatable", "character varying"),
        ("is_trigger_deletable", "character varying"),
        ("is_trigger_insertable_into", "character varying"),
    ],
    "key_column_usage": [
        ("constraint_catalog", "name"),
        ("constraint_schema", "name"),
        ("constraint_name", "name"),
        ("table_catalog", "name"),
        ("table_schema", "name"),
        ("table_name", "name"),
        ("column_name", "name"),
        ("ordinal_position", "integer"),
        ("position_in_unique_constraint", "integer"),
    ],
    "table_constraints": [
        ("constraint_catalog", "name"),
        ("constraint_schema", "name"),
        ("constraint_name", "name"),
        ("table_catalog", "name"),
        ("table_schema", "name"),
        ("table_name", "name"),
        ("constraint_type", "character varying"),
        ("is_deferrable", "character varying"),
        ("initially_deferred", "character varying"),
        ("enforced", "character varying"),
        ("nulls_distinct", "character varying"),
    ],
    "referential_constraints": [
        ("constraint_catalog", "name"),
        ("constraint_schema", "name"),
        ("constraint_name", "name"),
        ("unique_constraint_catalog", "name"),
        ("unique_constraint_schema", "name"),
        ("unique_constraint_name", "name"),
        ("match_option", "character varying"),
        ("update_rule", "character varying"),
        ("delete_rule", "character varying"),
    ],
    "role_table_grants": [
        ("grantor", "name"),
        ("grantee", "name"),
        ("table_catalog", "name"),
        ("table_schema", "name"),
        ("table_name", "name"),
        ("privilege_type", "character varying"),
        ("is_grantable", "character varying"),
        ("with_hierarchy", "character varying"),
    ],
    "role_column_grants": [
        ("grantor", "name"),
        ("grantee", "name"),
        ("table_catalog", "name"),
        ("table_schema", "name"),
        ("table_name", "name"),
        ("column_name", "name"),
        ("privilege_type", "character varying"),
        ("is_grantable", "character varying"),
    ],
    "triggers": [
        ("trigger_catalog", "name"),
        ("trigger_schema", "name"),
        ("trigger_name", "name"),
        ("event_manipulation", "character varying"),
        ("event_object_catalog", "name"),
        ("event_object_schema", "name"),
        ("event_object_table", "name"),
        ("action_order", "integer"),
        ("action_condition", "character varying"),
        ("action_statement", "character varying"),
        ("action_orientation", "character varying"),
        ("action_timing", "character varying"),
        ("action_reference_old_table", "name"),
        ("action_reference_new_table", "name"),
        ("action_reference_old_row", "name"),
        ("action_reference_new_row", "name"),
        ("created", "timestamp with time zone"),
    ],
    "sequences": [
        ("sequence_catalog", "name"),
        ("sequence_schema", "name"),
        ("sequence_name", "name"),
        ("data_type", "character varying"),
        ("numeric_precision", "integer"),
        ("numeric_precision_radix", "integer"),
        ("numeric_scale", "integer"),
        ("start_value", "character varying"),
        ("minimum_value", "character varying"),
        ("maximum_value", "character varying"),
        ("increment", "character varying"),
        ("cycle_option", "character varying"),
    ],
    "routines": [
        ("specific_catalog", "name"),
        ("specific_schema", "name"),
        ("specific_name", "name"),
        ("routine_catalog", "name"),
        ("routine_schema", "name"),
        ("routine_name", "name"),
        ("routine_type", "character varying"),
        ("data_type", "character varying"),
        ("numeric_precision", "integer"),
        ("numeric_precision_radix", "integer"),
        ("numeric_scale", "integer"),
        ("datetime_precision", "integer"),
        ("routine_body", "character varying"),
        ("routine_definition", "character varying"),
        ("external_name", "character varying"),
        ("external_language", "character varying"),
        ("parameter_style", "character varying"),
        ("is_deterministic", "character varying"),
        ("sql_data_access", "character varying"),
        ("security_type", "character varying"),
        ("created", "timestamp with time zone"),
        ("last_altered", "timestamp with time zone"),
    ],
    "parameters": [
        ("specific_catalog", "name"),
        ("specific_schema", "name"),
        ("specific_name", "name"),
        ("ordinal_position", "integer"),
        ("parameter_mode", "character varying"),
        ("is_result", "character varying"),
        ("as_locator", "character varying"),
        ("parameter_name", "name"),
        ("data_type", "character varying"),
        ("character_maximum_length", "integer"),
        ("character_octet_length", "integer"),
        ("numeric_precision", "integer"),
        ("numeric_precision_radix", "integer"),
        ("numeric_scale", "integer"),
        ("datetime_precision", "integer"),
        ("interval_type", "character varying"),
        ("interval_precision", "integer"),
        ("udt_catalog", "name"),
        ("udt_schema", "name"),
        ("udt_name", "name"),
        ("dtd_identifier", "name"),
        ("parameter_default", "character varying"),
    ],
    "enabled_roles": [("role_name", "name")],
    "applicable_roles": [
        ("grantee", "name"),
        ("role_name", "name"),
        ("is_grantable", "character varying"),
    ],
    "pg_namespace": [
        ("oid", "oid"),
        ("nspname", "name"),
        ("nspowner", "oid"),
        ("nspacl", "ARRAY"),
    ],
    "pg_class": [
        ("oid", "oid"),
        ("relname", "name"),
        ("relnamespace", "oid"),
        ("reltype", "oid"),
        ("reloftype", "oid"),
        ("relowner", "oid"),
        ("relam", "oid"),
        ("relfilenode", "oid"),
        ("reltablespace", "oid"),
        ("relpages", "integer"),
        ("reltuples", "real"),
        ("relallvisible", "integer"),
        ("reltoastrelid", "oid"),
        ("relhasindex", "boolean"),
        ("relisshared", "boolean"),
        ("relpersistence", "char"),
        ("relkind", "char"),
        ("relnatts", "smallint"),
        ("relchecks", "smallint"),
        ("relhasrules", "boolean"),
        ("relhastriggers", "boolean"),
        ("relhassubclass", "boolean"),
        ("relrowsecurity", "boolean"),
        ("relforcerowsecurity", "boolean"),
        ("relispopulated", "boolean"),
        ("relreplident", "char"),
        ("relispartition", "boolean"),
        ("relrewrite", "oid"),
        ("relfrozenxid", "xid"),
        ("relminmxid", "xid"),
        ("relacl", "ARRAY"),
        ("reloptions", "ARRAY"),
        ("relpartbound", "text"),
    ],
    "pg_attribute": [
        ("attrelid", "oid"),
        ("attname", "name"),
        ("atttypid", "oid"),
        ("attlen", "smallint"),
        ("attnum", "smallint"),
        ("attcacheoff", "integer"),
        ("atttypmod", "integer"),
        ("attndims", "smallint"),
        ("attbyval", "boolean"),
        ("attalign", "char"),
        ("attstorage", "char"),
        ("attnotnull", "boolean"),
        ("atthasdef", "boolean"),
        ("atthasmissing", "boolean"),
        ("attidentity", "char"),
        ("attgenerated", "char"),
        ("attisdropped", "boolean"),
        ("attislocal", "boolean"),
        ("attinhcount", "smallint"),
        ("attstattarget", "smallint"),
        ("attcollation", "oid"),
        ("attacl", "ARRAY"),
        ("attoptions", "ARRAY"),
        ("attfdwoptions", "ARRAY"),
    ],
    "pg_type": [
        ("oid", "oid"),
        ("typname", "name"),
        ("typnamespace", "oid"),
        ("typowner", "oid"),
        ("typlen", "smallint"),
        ("typbyval", "boolean"),
        ("typtype", "char"),
        ("typcategory", "char"),
        ("typispreferred", "boolean"),
        ("typisdefined", "boolean"),
        ("typdelim", "char"),
        ("typrelid", "oid"),
        ("typelem", "oid"),
        ("typarray", "oid"),
        ("typinput", "integer"),
        ("typoutput", "integer"),
        ("typreceive", "integer"),
        ("typsend", "integer"),
        ("typmodin", "integer"),
        ("typmodout", "integer"),
        ("typanalyze", "integer"),
        ("typalign", "char"),
        ("typstorage", "char"),
        ("typnotnull", "boolean"),
        ("typbasetype", "oid"),
        ("typtypmod", "integer"),
        ("typndims", "integer"),
        ("typcollation", "oid"),
        ("typdefaultbin", "text"),
        ("typdefault", "text"),
        ("typacl", "ARRAY"),
    ],
    "pg_attrdef": [
        ("oid", "oid"),
        ("adrelid", "oid"),
        ("adnum", "smallint"),
        ("adbin", "text"),
    ],
    "pg_description": [
        ("objoid", "oid"),
        ("classoid", "oid"),
        ("objsubid", "integer"),
        ("description", "text"),
    ],
    "pg_index": [
        ("indexrelid", "oid"),
        ("indrelid", "oid"),
        ("indnatts", "smallint"),
        ("indnkeyatts", "smallint"),
        ("indisunique", "boolean"),
        ("indisprimary", "boolean"),
        ("indisexclusion", "boolean"),
        ("indimmediate", "boolean"),
        ("indisclustered", "boolean"),
        ("indisvalid", "boolean"),
        ("indisready", "boolean"),
        ("indislive", "boolean"),
        ("indkey", "ARRAY"),
        ("indexprs", "text"),
        ("indpred", "text"),
    ],
    "pg_constraint": [
        ("oid", "oid"),
        ("conname", "name"),
        ("connamespace", "oid"),
        ("contype", "char"),
        ("condeferrable", "boolean"),
        ("condeferred", "boolean"),
        ("convalidated", "boolean"),
        ("conrelid", "oid"),
        ("contypid", "oid"),
        ("conindid", "oid"),
        ("conparentid", "oid"),
        ("confrelid", "oid"),
        ("confupdtype", "char"),
        ("confdeltype", "char"),
        ("confmatchtype", "char"),
        ("conislocal", "boolean"),
        ("coninhcount", "smallint"),
        ("connoinherit", "boolean"),
        ("conkey", "ARRAY"),
        ("confkey", "ARRAY"),
        ("conpfeqop", "ARRAY"),
        ("conppeqop", "ARRAY"),
        ("conffeqop", "ARRAY"),
        ("conexclop", "ARRAY"),
        ("conbin", "text"),
    ],
    "pg_proc": [
        ("oid", "oid"),
        ("proname", "name"),
        ("pronamespace", "oid"),
        ("proowner", "oid"),
        ("prolang", "oid"),
        ("procost", "real"),
        ("prorows", "real"),
        ("provariadic", "oid"),
        ("prokind", "char"),
        ("prosecdef", "boolean"),
        ("proisstrict", "boolean"),
        ("proretset", "boolean"),
        ("provolatile", "char"),
        ("proparallel", "char"),
        ("pronargs", "smallint"),
        ("pronargdefaults", "smallint"),
        ("prorettype", "oid"),
        ("proargtypes", "ARRAY"),
        ("proallargtypes", "ARRAY"),
        ("proargmodes", "ARRAY"),
        ("proargnames", "ARRAY"),
        ("prosrc", "text"),
        ("probin", "text"),
        ("proacl", "ARRAY"),
    ],
    "pg_roles": [
        ("rolname", "name"),
        ("rolsuper", "boolean"),
        ("rolinherit", "boolean"),
        ("rolcreaterole", "boolean"),
        ("rolcreatedb", "boolean"),
        ("rolcanlogin", "boolean"),
        ("rolreplication", "boolean"),
        ("rolconnlimit", "integer"),
        ("rolpassword", "text"),
        ("rolvaliduntil", "timestamp with time zone"),
        ("rolbypassrls", "boolean"),
        ("rolconfig", "ARRAY"),
        ("oid", "oid"),
    ],
    "pg_auth_members": [
        ("oid", "oid"),
        ("roleid", "oid"),
        ("member", "oid"),
        ("grantor", "oid"),
        ("admin_option", "boolean"),
        ("inherit_option", "boolean"),
        ("set_option", "boolean"),
    ],
    "pg_database": [
        ("oid", "oid"),
        ("datname", "name"),
        ("datdba", "oid"),
        ("encoding", "integer"),
        ("datistemplate", "boolean"),
        ("datallowconn", "boolean"),
        ("datconnlimit", "integer"),
        ("datfrozenxid", "xid"),
        ("datminmxid", "xid"),
        ("dattablespace", "oid"),
        ("datcollate", "text"),
        ("datctype", "text"),
        ("datacl", "ARRAY"),
    ],
    "pg_settings": [
        ("name", "text"),
        ("setting", "text"),
        ("unit", "text"),
        ("category", "text"),
        ("short_desc", "text"),
        ("extra_desc", "text"),
        ("context", "text"),
        ("vartype", "text"),
        ("source", "text"),
        ("min_val", "text"),
        ("max_val", "text"),
        ("enumvals", "ARRAY"),
        ("boot_val", "text"),
        ("reset_val", "text"),
        ("sourcefile", "text"),
        ("sourceline", "integer"),
        ("pending_restart", "boolean"),
    ],
    "pg_tables": [
        ("schemaname", "name"),
        ("tablename", "name"),
        ("tableowner", "name"),
        ("tablespace", "name"),
        ("hasindexes", "boolean"),
        ("hasrules", "boolean"),
        ("hastriggers", "boolean"),
        ("rowsecurity", "boolean"),
    ],
    "pg_stat_user_tables": [
        ("relid", "oid"),
        ("schemaname", "name"),
        ("relname", "name"),
        ("seq_scan", "bigint"),
        ("seq_tup_read", "bigint"),
        ("idx_scan", "bigint"),
        ("idx_tup_fetch", "bigint"),
        ("n_tup_ins", "bigint"),
        ("n_tup_upd", "bigint"),
        ("n_tup_del", "bigint"),
        ("n_tup_hot_upd", "bigint"),
        ("n_live_tup", "bigint"),
        ("n_dead_tup", "bigint"),
        ("n_mod_since_analyze", "bigint"),
        ("last_vacuum", "timestamp with time zone"),
        ("last_autovacuum", "timestamp with time zone"),
        ("last_analyze", "timestamp with time zone"),
        ("last_autoanalyze", "timestamp with time zone"),
        ("vacuum_count", "bigint"),
        ("autovacuum_count", "bigint"),
        ("analyze_count", "bigint"),
        ("autoanalyze_count", "bigint"),
    ],
    "pg_statio_user_tables": [
        ("relid", "oid"),
        ("schemaname", "name"),
        ("relname", "name"),
        ("heap_blks_read", "bigint"),
        ("heap_blks_hit", "bigint"),
        ("idx_blks_read", "bigint"),
        ("idx_blks_hit", "bigint"),
        ("toast_blks_read", "bigint"),
        ("toast_blks_hit", "bigint"),
        ("tidx_blks_read", "bigint"),
        ("tidx_blks_hit", "bigint"),
    ],
    "pg_am": [
        ("oid", "oid"),
        ("amname", "name"),
        ("amhandler", "integer"),
        ("amtype", "char"),
    ],
    "pg_tablespace": [
        ("oid", "oid"),
        ("spcname", "name"),
        ("spcowner", "oid"),
        ("spcacl", "ARRAY"),
        ("spcoptions", "ARRAY"),
    ],
    "pg_extension": [
        ("oid", "oid"),
        ("extname", "name"),
        ("extowner", "oid"),
        ("extnamespace", "oid"),
        ("extrelocatable", "boolean"),
        ("extversion", "text"),
        ("extconfig", "ARRAY"),
        ("extcondition", "ARRAY"),
    ],
    "pg_enum": [
        ("oid", "oid"),
        ("enumtypid", "oid"),
        ("enumsortorder", "real"),
        ("enumlabel", "name"),
    ],
    "pg_trigger": [
        ("oid", "oid"),
        ("tgrelid", "oid"),
        ("tgparentid", "oid"),
        ("tgname", "name"),
        ("tgfoid", "oid"),
        ("tgtype", "smallint"),
        ("tgenabled", "char"),
        ("tgisinternal", "boolean"),
        ("tgconstrrelid", "oid"),
        ("tgconstrindid", "oid"),
        ("tgconstraint", "oid"),
        ("tgdeferrable", "boolean"),
        ("tginitdeferred", "boolean"),
        ("tgnargs", "smallint"),
        ("tgattr", "ARRAY"),
        ("tgargs", "text"),
        ("tgqual", "text"),
        ("tgoldtable", "name"),
        ("tgnewtable", "name"),
    ],
    "pg_inherits": [
        ("inhrelid", "oid"),
        ("inhparent", "oid"),
        ("inhseqno", "integer"),
    ],
    "pg_depend": [
        ("classid", "oid"),
        ("objid", "oid"),
        ("objsubid", "integer"),
        ("refclassid", "oid"),
        ("refobjid", "oid"),
        ("refobjsubid", "integer"),
        ("deptype", "char"),
    ],
    "pg_aggregate": [
        ("aggfnoid", "integer"),
        ("aggkind", "char"),
        ("aggnumdirectargs", "smallint"),
        ("aggtransfn", "integer"),
        ("aggfinalfn", "integer"),
        ("aggsortop", "oid"),
        ("aggtranstype", "oid"),
        ("aggtransspace", "integer"),
        ("agginitval", "text"),
    ],
    "pg_language": [
        ("oid", "oid"),
        ("lanname", "name"),
        ("lanowner", "oid"),
        ("lanispl", "boolean"),
        ("lanpltrusted", "boolean"),
        ("lanplcallfoid", "oid"),
        ("laninline", "oid"),
        ("lanvalidator", "oid"),
        ("lanacl", "ARRAY"),
    ],
    "pg_operator": [
        ("oid", "oid"),
        ("oprname", "name"),
        ("oprnamespace", "oid"),
        ("oprowner", "oid"),
        ("oprkind", "char"),
        ("oprcanmerge", "boolean"),
        ("oprcanhash", "boolean"),
        ("oprleft", "oid"),
        ("oprright", "oid"),
        ("oprresult", "oid"),
        ("oprcom", "oid"),
        ("oprnegate", "oid"),
        ("oprcode", "integer"),
        ("oprrest", "integer"),
        ("oprjoin", "integer"),
    ],
    "pg_cast": [
        ("oid", "oid"),
        ("castsource", "oid"),
        ("casttarget", "oid"),
        ("castfunc", "oid"),
        ("castcontext", "char"),
        ("castmethod", "char"),
    ],
    "pg_collation": [
        ("oid", "oid"),
        ("collname", "name"),
        ("collnamespace", "oid"),
        ("collowner", "oid"),
        ("collprovider", "char"),
        ("collisdeterministic", "boolean"),
        ("collencoding", "integer"),
        ("collcollate", "text"),
        ("collctype", "text"),
    ],
    "pg_range": [
        ("rngtypid", "oid"),
        ("rngsubtype", "oid"),
        ("rngcollation", "oid"),
        ("rngsubopc", "oid"),
        ("rngcanonical", "integer"),
        ("rngsubdiff", "integer"),
    ],
    "pg_foreign_table": [
        ("ftrelid", "oid"),
        ("ftserver", "oid"),
        ("ftoptions", "ARRAY"),
    ],
    "pg_foreign_server": [
        ("oid", "oid"),
        ("srvname", "name"),
        ("srvowner", "oid"),
        ("srvfdw", "oid"),
        ("srvtype", "text"),
        ("srvversion", "text"),
        ("srvacl", "ARRAY"),
        ("srvoptions", "ARRAY"),
    ],
    "pg_sequence": [
        ("seqrelid", "oid"),
        ("seqtypid", "oid"),
        ("seqstart", "bigint"),
        ("seqincrement", "bigint"),
        ("seqmax", "bigint"),
        ("seqmin", "bigint"),
        ("seqcache", "bigint"),
        ("seqcycle", "boolean"),
    ],
    "pg_locks": [
        ("locktype", "text"),
        ("database", "oid"),
        ("relation", "oid"),
        ("page", "integer"),
        ("tuple", "smallint"),
        ("virtualxid", "text"),
        ("transactionid", "xid"),
        ("classid", "oid"),
        ("objid", "oid"),
        ("objsubid", "smallint"),
        ("virtualtransaction", "text"),
        ("pid", "integer"),
        ("mode", "text"),
        ("granted", "boolean"),
        ("fastpath", "boolean"),
        ("waitstart", "timestamp with time zone"),
    ],
    "pg_shdescription": [
        ("objoid", "oid"),
        ("classoid", "oid"),
        ("description", "text"),
    ],
    "pg_conversion": [
        ("oid", "oid"),
        ("conname", "name"),
        ("connamespace", "oid"),
        ("conowner", "oid"),
        ("conforencoding", "integer"),
        ("contoencoding", "integer"),
        ("conproc", "integer"),
        ("condefault", "boolean"),
    ],
    "pg_stat_activity": [
        ("datid", "oid"),
        ("datname", "name"),
        ("pid", "integer"),
        ("leader_pid", "integer"),
        ("usesysid", "oid"),
        ("usename", "name"),
        ("application_name", "text"),
        ("client_addr", "text"),
        ("client_hostname", "text"),
        ("client_port", "integer"),
        ("backend_start", "timestamp with time zone"),
        ("xact_start", "timestamp with time zone"),
        ("query_start", "timestamp with time zone"),
        ("state_change", "timestamp with time zone"),
        ("wait_event_type", "text"),
        ("wait_event", "text"),
        ("state", "text"),
        ("backend_xid", "xid"),
        ("backend_xmin", "xid"),
        ("query_id", "bigint"),
        ("query", "text"),
        ("backend_type", "text"),
    ],
    "pg_rewrite": [
        ("oid", "oid"),
        ("rulename", "name"),
        ("ev_class", "oid"),
        ("ev_type", "char"),
        ("ev_enabled", "char"),
        ("is_instead", "boolean"),
        ("ev_qual", "text"),
        ("ev_action", "text"),
    ],
    "pg_shdepend": [
        ("dbid", "oid"),
        ("classid", "oid"),
        ("objid", "oid"),
        ("objsubid", "integer"),
        ("refclassid", "oid"),
        ("refobjid", "oid"),
        ("deptype", "char"),
    ],
    "pg_partitioned_table": [
        ("partrelid", "oid"),
        ("partstrat", "char"),
        ("partnatts", "smallint"),
        ("partdefid", "oid"),
        ("partattrs", "ARRAY"),
        ("partclass", "ARRAY"),
        ("partcollation", "ARRAY"),
        ("partexprs", "text"),
    ],
    "pg_publication": [
        ("oid", "oid"),
        ("pubname", "name"),
        ("pubowner", "oid"),
        ("puballtables", "boolean"),
        ("pubinsert", "boolean"),
        ("pubupdate", "boolean"),
        ("pubdelete", "boolean"),
        ("pubtruncate", "boolean"),
        ("pubviaroot", "boolean"),
    ],
    "pg_subscription": [
        ("oid", "oid"),
        ("subdbid", "oid"),
        ("subskiplsn", "text"),
        ("subname", "name"),
        ("subowner", "oid"),
        ("subenabled", "boolean"),
        ("subbinary", "boolean"),
        ("substream", "char"),
        ("subtwophasestate", "char"),
        ("subdisableonerr", "boolean"),
        ("subconninfo", "text"),
        ("subslotname", "name"),
        ("subsynccommit", "text"),
        ("subpublications", "ARRAY"),
    ],
    "pg_event_trigger": [
        ("oid", "oid"),
        ("evtname", "name"),
        ("evtevent", "name"),
        ("evtowner", "oid"),
        ("evtfoid", "oid"),
        ("evtenabled", "char"),
        ("evttags", "ARRAY"),
    ],
    "pg_stat_user_indexes": [
        ("relid", "oid"),
        ("indexrelid", "oid"),
        ("schemaname", "name"),
        ("relname", "name"),
        ("indexrelname", "name"),
        ("idx_scan", "bigint"),
        ("last_idx_scan", "timestamp with time zone"),
        ("idx_tup_read", "bigint"),
        ("idx_tup_fetch", "bigint"),
    ],
    "pg_user_mapping": [
        ("oid", "oid"),
        ("umuser", "oid"),
        ("umserver", "oid"),
        ("umoptions", "ARRAY"),
    ],
    "pg_foreign_data_wrapper": [
        ("oid", "oid"),
        ("fdwname", "name"),
        ("fdwowner", "oid"),
        ("fdwhandler", "oid"),
        ("fdwvalidator", "oid"),
        ("fdwacl", "ARRAY"),
        ("fdwoptions", "ARRAY"),
    ],
    "pg_policy": [
        ("oid", "oid"),
        ("polname", "name"),
        ("polrelid", "oid"),
        ("polcmd", "char"),
        ("polpermissive", "boolean"),
        ("polroles", "ARRAY"),
        ("polqual", "text"),
        ("polwithcheck", "text"),
    ],
}

_PG_TYPE_ROWS = [
    # (oid, typname, typnamespace, typlen, typtype, typcategory, typnotnull, typbasetype, typbyval, typalign, typstorage)
    (16, "bool", 11, 1, "b", "B", False, 0, True, "c", "p"),
    (17, "bytea", 11, -1, "b", "U", False, 0, False, "i", "x"),
    (20, "int8", 11, 8, "b", "N", False, 0, True, "d", "p"),
    (21, "int2", 11, 2, "b", "N", False, 0, True, "s", "p"),
    (23, "int4", 11, 4, "b", "N", False, 0, True, "i", "p"),
    (25, "text", 11, -1, "b", "S", False, 0, False, "i", "x"),
    (114, "json", 11, -1, "b", "U", False, 0, False, "i", "x"),
    (700, "float4", 11, 4, "b", "N", False, 0, True, "i", "p"),
    (701, "float8", 11, 8, "b", "N", False, 0, True, "d", "p"),
    (1043, "varchar", 11, -1, "b", "S", False, 0, False, "i", "x"),
    (1082, "date", 11, 4, "b", "D", False, 0, True, "i", "p"),
    (1083, "time", 11, 8, "b", "D", False, 0, True, "d", "p"),
    (1114, "timestamp", 11, 8, "b", "D", False, 0, True, "d", "p"),
    (1184, "timestamptz", 11, 8, "b", "D", False, 0, True, "d", "p"),
    (1700, "numeric", 11, -1, "b", "N", False, 0, False, "i", "m"),
    (3802, "jsonb", 11, -1, "b", "U", False, 0, False, "i", "x"),
    (2950, "uuid", 11, 16, "b", "U", False, 0, False, "c", "p"),
]

# OID → (attlen, attbyval, attalign, attstorage)
_PG_OID_ATTR_META: dict[int, tuple[int, bool, str, str]] = {
    16: (1, True, "c", "p"),  # bool
    17: (-1, False, "i", "x"),  # bytea
    20: (8, True, "d", "p"),  # int8
    21: (2, True, "s", "p"),  # int2
    23: (4, True, "i", "p"),  # int4
    25: (-1, False, "i", "x"),  # text
    114: (-1, False, "i", "x"),  # json
    700: (4, True, "i", "p"),  # float4
    701: (8, True, "d", "p"),  # float8
    1043: (-1, False, "i", "x"),  # varchar
    1082: (4, True, "i", "p"),  # date
    1083: (8, True, "d", "p"),  # time
    1114: (8, True, "d", "p"),  # timestamp
    1184: (8, True, "d", "p"),  # timestamptz
    1700: (-1, False, "i", "m"),  # numeric
    3802: (-1, False, "i", "x"),  # jsonb
    2950: (16, False, "c", "p"),  # uuid
}

_TYPEINFO_COLS = [
    "oid",
    "ns",
    "name",
    "kind",
    "basetype",
    "elemtype",
    "elemdelim",
    "range_subtype",
    "attrtypoids",
    "attrnames",
    "depth",
    "basetype_name",
    "elemtype_name",
    "range_subtype_name",
]
# Declare attrtypoids/attrnames as VARCHAR so asyncpg uses built-in text OID (25)
# and doesn't recurse into array-type introspection for the schema.
_TYPEINFO_COL_TYPES = [
    "INTEGER",
    "VARCHAR",
    "VARCHAR",
    "VARCHAR",
    "INTEGER",
    "INTEGER",
    "VARCHAR",
    "INTEGER",
    "VARCHAR",
    "VARCHAR",
    "INTEGER",
    "VARCHAR",
    "VARCHAR",
    "VARCHAR",
]

# ns, name, kind, basetype_oid, elemtype_oid, elemdelim, range_subtype_oid
_TYPEINFO: dict[int, tuple] = {
    16: ("pg_catalog", "bool", "b", None, None, None, None),
    17: ("pg_catalog", "bytea", "b", None, None, None, None),
    18: ("pg_catalog", "char", "b", None, None, None, None),
    19: ("pg_catalog", "name", "b", None, None, None, None),
    20: ("pg_catalog", "int8", "b", None, None, None, None),
    21: ("pg_catalog", "int2", "b", None, None, None, None),
    23: ("pg_catalog", "int4", "b", None, None, None, None),
    25: ("pg_catalog", "text", "b", None, None, None, None),
    26: ("pg_catalog", "oid", "b", None, None, None, None),
    114: ("pg_catalog", "json", "b", None, None, None, None),
    700: ("pg_catalog", "float4", "b", None, None, None, None),
    701: ("pg_catalog", "float8", "b", None, None, None, None),
    705: ("pg_catalog", "unknown", "b", None, None, None, None),
    1042: ("pg_catalog", "bpchar", "b", None, None, None, None),
    1043: ("pg_catalog", "varchar", "b", None, None, None, None),
    1082: ("pg_catalog", "date", "b", None, None, None, None),
    1083: ("pg_catalog", "time", "b", None, None, None, None),
    1114: ("pg_catalog", "timestamp", "b", None, None, None, None),
    1184: ("pg_catalog", "timestamptz", "b", None, None, None, None),
    1700: ("pg_catalog", "numeric", "b", None, None, None, None),
    2950: ("pg_catalog", "uuid", "b", None, None, None, None),
    3802: ("pg_catalog", "jsonb", "b", None, None, None, None),
    # Array types
    199: ("pg_catalog", "_json", "b", None, 114, ",", None),
    1000: ("pg_catalog", "_bool", "b", None, 16, ",", None),
    1001: ("pg_catalog", "_bytea", "b", None, 17, ",", None),
    1002: ("pg_catalog", "_char", "b", None, 18, ",", None),
    1003: ("pg_catalog", "_name", "b", None, 19, ",", None),
    1005: ("pg_catalog", "_int2", "b", None, 21, ",", None),
    1007: ("pg_catalog", "_int4", "b", None, 23, ",", None),
    1009: ("pg_catalog", "_text", "b", None, 25, ",", None),
    1015: ("pg_catalog", "_varchar", "b", None, 1043, ",", None),
    1016: ("pg_catalog", "_int8", "b", None, 20, ",", None),
    1021: ("pg_catalog", "_float4", "b", None, 700, ",", None),
    1022: ("pg_catalog", "_float8", "b", None, 701, ",", None),
    1028: ("pg_catalog", "_oid", "b", None, 26, ",", None),
    1115: ("pg_catalog", "_timestamp", "b", None, 1114, ",", None),
    1182: ("pg_catalog", "_date", "b", None, 1082, ",", None),
    1183: ("pg_catalog", "_time", "b", None, 1083, ",", None),
    1185: ("pg_catalog", "_timestamptz", "b", None, 1184, ",", None),
    1231: ("pg_catalog", "_numeric", "b", None, 1700, ",", None),
    2951: ("pg_catalog", "_uuid", "b", None, 2950, ",", None),
    3807: ("pg_catalog", "_jsonb", "b", None, 3802, ",", None),
}


def _parse_typeinfo_oids(sql: str) -> list[int] | None:
    """Extract OIDs from ANY('{oid,...}'::oid[]) pattern; None if $1 still present."""
    m = re.search(r"ANY\s*\(\s*'\{([^}]*)\}'", sql, re.IGNORECASE)
    if m:
        raw = m.group(1).strip()
        return [int(x) for x in raw.split(",") if x.strip()] if raw else []
    return None


def _handle_typeinfo_tree(oids: list[int]):
    from provisa.executor.trino import QueryResult

    rows = []
    for oid in oids:
        info = _TYPEINFO.get(oid)
        if info is None:
            continue
        ns, name, kind, basetype, elemtype, elemdelim, range_subtype = info
        elem_name = _TYPEINFO[elemtype][1] if elemtype and elemtype in _TYPEINFO else None
        base_name = _TYPEINFO[basetype][1] if basetype and basetype in _TYPEINFO else None
        range_name = (
            _TYPEINFO[range_subtype][1] if range_subtype and range_subtype in _TYPEINFO else None
        )
        rows.append(
            (
                oid,
                ns,
                name,
                kind,
                basetype,
                elemtype,
                elemdelim,
                range_subtype,
                None,
                None,
                0,
                base_name,
                elem_name,
                range_name,
            )
        )
    return QueryResult(rows=rows, column_names=_TYPEINFO_COLS, column_types=_TYPEINFO_COL_TYPES)


_KNOWN_SETTINGS = {
    "server_version": "14.0.provisa",
    "server_version_num": "140000",
    "server_encoding": "UTF8",
    "client_encoding": "UTF8",
    "datestyle": "ISO, MDY",
    "timezone": "UTC",
    "integer_datetimes": "on",
    "standard_conforming_strings": "on",
    "intervalstyle": "postgres",
    "search_path": 'public, "$user"',
    "extra_float_digits": "0",
    "application_name": "",
    "is_superuser": "on",
    "session_authorization": "admin",
}


def _trino_to_pg_name(trino_type: str) -> str:
    t = trino_type.lower().split("(")[0].strip()
    return {
        "varchar": "character varying",
        "char": "character",
        "integer": "integer",
        "int": "integer",
        "bigint": "bigint",
        "smallint": "smallint",
        "boolean": "boolean",
        "double": "double precision",
        "real": "real",
        "date": "date",
        "time": "time without time zone",
        "timestamp": "timestamp without time zone",
        "timestamp with time zone": "timestamp with time zone",
        "decimal": "numeric",
        "json": "jsonb",
        "row": "jsonb",
        "array": "ARRAY",
        "varbinary": "bytea",
        "uuid": "uuid",
    }.get(t, "text")


def _trino_to_pg_oid(trino_type: str) -> int:
    t = trino_type.lower().split("(")[0].strip()
    return {
        "varchar": 1043,
        "char": 18,
        "integer": 23,
        "int": 23,
        "bigint": 20,
        "smallint": 21,
        "boolean": 16,
        "double": 701,
        "real": 700,
        "date": 1082,
        "time": 1083,
        "timestamp": 1114,
        "timestamp with time zone": 1184,
        "decimal": 1700,
        "json": 3802,
        "jsonb": 3802,
        "row": 3802,
        "array": 2277,
        "varbinary": 17,
        "uuid": 2950,
    }.get(t, 25)


_SCALAR_NAMES = frozenset(
    {
        "current_user",
        "session_user",
        "current_database",
        "current_schema",
        "version",
        "pg_backend_pid",
    }
)


def classify(sql: str) -> str:  # REQ-127, REQ-128, REQ-363
    """Return 'INTERCEPT' or 'PASS_THROUGH'."""
    stripped = sql.strip()
    if _SET_RE.match(stripped) or _SHOW_RE.match(stripped) or _TXN_RE.match(stripped):
        return "INTERCEPT"
    if _SCALAR_FN_RE.match(stripped):
        return "INTERCEPT"
    try:
        import sqlglot.expressions as exp
        import sqlglot

        tree = sqlglot.parse_one(stripped, read="postgres")
        for tbl in tree.find_all(exp.Table):
            db = tbl.db.lower() if tbl.db else ""
            tname = tbl.name.lower() if tbl.name else ""
            if db in _INTERCEPT_SCHEMAS:
                return "INTERCEPT"
            if not db and tname in _CATALOG_TABLE_NAMES:
                return "INTERCEPT"
        for func in tree.find_all(exp.Anonymous):
            fn = func.name.lower()
            if "current_setting" in fn or "set_config" in fn:
                return "INTERCEPT"
            if fn in _SCALAR_NAMES:
                return "INTERCEPT"
            if any(
                x in fn
                for x in (
                    "obj_description",
                    "col_description",
                    "shobj_description",
                    "pg_get_expr",
                    "pg_stat_get",
                )
            ):
                return "INTERCEPT"
        for col in tree.find_all(exp.Column):
            if col.name.lower() in _SCALAR_NAMES:
                return "INTERCEPT"
        for node in tree.walk():
            if type(node).__name__ in ("CurrentUser", "CurrentDatabase", "CurrentSchema"):
                return "INTERCEPT"
    except Exception:
        lower = stripped.lower()
        for name in _CATALOG_TABLE_NAMES:
            if re.search(r"\b" + re.escape(name) + r"\b", lower):
                return "INTERCEPT"
        for schema in _INTERCEPT_SCHEMAS:
            if schema in lower:
                return "INTERCEPT"
    return "PASS_THROUGH"


class CatalogIndex:
    """Single source of truth for all OID/attnum/name mappings used by catalog populate functions."""

    __slots__ = (
        "tables",
        "all_cols",
        "table_id_to_oid",
        "toid_to_table",
        "col_attnum",
        "attnum_to_col",
        "ns_map",
    )

    def __init__(self) -> None:
        self.tables: list[tuple] = []
        self.all_cols: list[tuple] = []
        self.table_id_to_oid: dict[int, int] = {}
        self.toid_to_table: dict[int, tuple] = {}
        self.col_attnum: dict[tuple[int, str], int] = {}
        self.attnum_to_col: dict[tuple[int, int], str] = {}
        self.ns_map: dict[str, int] = {"pg_catalog": 11, "information_schema": 12, "public": 2200}


def _build_catalog_index(ctx, col_types: dict) -> CatalogIndex:  # REQ-128, REQ-363
    """Build the CatalogIndex once. All populate functions read from it — nothing recomputes."""
    from provisa.compiler.naming import domain_to_sql_name, apply_sql_name
    from provisa.compiler.sql_gen import semantic_table_name

    idx = CatalogIndex()
    if not ctx:
        return idx

    # Assign stable namespace OIDs — sort extra schemas so OIDs don't depend on iteration order.
    extra_schemas: set[str] = set()
    for tm in ctx.tables.values():
        raw = tm.domain_id or tm.schema_name or "public"
        sch = domain_to_sql_name(raw)
        if sch not in idx.ns_map:
            extra_schemas.add(sch)
    _ns_extra = 2201
    for sch in sorted(extra_schemas):
        idx.ns_map[sch] = _ns_extra
        _ns_extra += 1

    seen_table_ids: set[int] = set()

    for tm in ctx.tables.values():
        if tm.table_id in seen_table_ids:
            continue
        seen_table_ids.add(tm.table_id)
        cat = "provisa"
        raw_schema = tm.domain_id or tm.schema_name or "public"
        sch = domain_to_sql_name(raw_schema)
        tname = semantic_table_name(tm)
        toid = 16384 + tm.table_id

        idx.tables.append((cat, sch, tname, tm.table_id, toid))
        idx.table_id_to_oid[tm.table_id] = toid
        idx.toid_to_table[toid] = (cat, sch, tname)

        _p2s_raw = getattr(ctx, "physical_to_sql", None)
        _p2s: dict = _p2s_raw if isinstance(_p2s_raw, dict) else {}
        real_cols = col_types.get(tm.table_id, [])
        for i, col in enumerate(real_cols, 1):
            phys = col.column_name
            exposed = _p2s.get((tm.table_id, phys)) or apply_sql_name(phys)
            idx.all_cols.append((toid, exposed, col.data_type, col.is_nullable, i))
            idx.col_attnum[(toid, exposed)] = i
            idx.attnum_to_col[(toid, i)] = exposed

        virtual = getattr(ctx, "virtual_columns", {}).get(tm.table_id, {})
        for j, vcol in enumerate(virtual, len(real_cols) + 1):
            idx.all_cols.append((toid, vcol, "varchar", True, j))
            idx.col_attnum[(toid, vcol)] = j
            idx.attnum_to_col[(toid, j)] = vcol

    return idx


def _populate_is_schemata(db, idx: CatalogIndex) -> None:
    db.execute("""CREATE TABLE _is_schemata (
        catalog_name VARCHAR, schema_name VARCHAR, schema_owner VARCHAR,
        default_character_set_catalog VARCHAR, default_character_set_schema VARCHAR,
        default_character_set_name VARCHAR, sql_path VARCHAR)""")
    seen_schemas: set[tuple] = {(c, s) for c, s, *_ in idx.tables}
    if seen_schemas:
        db.executemany(
            "INSERT INTO _is_schemata VALUES (?,?,'provisa',NULL,NULL,NULL,NULL)",
            list(seen_schemas),
        )


def _populate_is_tables(db, idx: CatalogIndex) -> None:
    db.execute("""CREATE TABLE _is_tables (
        table_catalog VARCHAR, table_schema VARCHAR, table_name VARCHAR, table_type VARCHAR,
        self_referencing_column_name VARCHAR, reference_generation VARCHAR,
        user_defined_type_catalog VARCHAR, user_defined_type_schema VARCHAR,
        user_defined_type_name VARCHAR, is_insertable_into VARCHAR,
        is_typed VARCHAR, commit_action VARCHAR)""")
    if idx.tables:
        db.executemany(
            "INSERT INTO _is_tables VALUES (?,?,?,'BASE TABLE',NULL,NULL,NULL,NULL,NULL,'YES','NO',NULL)",
            [(row[0], row[1], row[2]) for row in idx.tables],
        )


def _populate_is_columns(db, idx: CatalogIndex) -> None:
    db.execute("""CREATE TABLE _is_columns (
        table_catalog VARCHAR, table_schema VARCHAR, table_name VARCHAR,
        column_name VARCHAR, ordinal_position INTEGER, column_default VARCHAR,
        is_nullable VARCHAR, data_type VARCHAR,
        character_maximum_length INTEGER, character_octet_length INTEGER,
        numeric_precision INTEGER, numeric_precision_radix INTEGER, numeric_scale INTEGER,
        datetime_precision INTEGER, interval_type VARCHAR, interval_precision INTEGER,
        character_set_catalog VARCHAR, character_set_schema VARCHAR, character_set_name VARCHAR,
        collation_catalog VARCHAR, collation_schema VARCHAR, collation_name VARCHAR,
        domain_catalog VARCHAR, domain_schema VARCHAR, domain_name VARCHAR,
        udt_catalog VARCHAR, udt_schema VARCHAR, udt_name VARCHAR,
        scope_catalog VARCHAR, scope_schema VARCHAR, scope_name VARCHAR,
        maximum_cardinality INTEGER, dtd_identifier VARCHAR,
        is_self_referencing VARCHAR, is_identity VARCHAR, identity_generation VARCHAR,
        identity_start VARCHAR, identity_increment VARCHAR, identity_maximum VARCHAR,
        identity_minimum VARCHAR, identity_cycle VARCHAR, is_generated VARCHAR,
        generation_expression VARCHAR, is_updatable VARCHAR)""")
    col_rows = []
    for toid, col_name, col_type, is_nullable, ordinal in idx.all_cols:
        c, s, t = idx.toid_to_table.get(toid, ("provisa", "public", ""))
        pg_type = _trino_to_pg_name(col_type)
        null_str = "YES" if is_nullable else "NO"
        col_rows.append(
            (
                c,
                s,
                t,
                col_name,
                ordinal,
                None,
                null_str,
                pg_type,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                pg_type,
                None,
                None,
                None,
                None,
                str(ordinal),
                "NO",
                "NO",
                None,
                None,
                None,
                None,
                None,
                "NO",
                "NEVER",
                None,
                "YES",
            )
        )
    if col_rows:
        db.executemany(f"INSERT INTO _is_columns VALUES ({','.join(['?'] * 44)})", col_rows)


def _populate_pg_namespace(db, idx: CatalogIndex) -> None:
    db.execute("""CREATE TABLE _pg_namespace (
        oid INTEGER, nspname VARCHAR, nspowner INTEGER, nspacl VARCHAR)""")
    ns_rows = [(oid, name, 10, None) for name, oid in idx.ns_map.items()]
    db.executemany("INSERT INTO _pg_namespace VALUES (?,?,?,?)", ns_rows)


def _populate_pg_class(db, idx: CatalogIndex, row_counts: dict[int, float] | None = None) -> None:
    db.execute("""CREATE TABLE _pg_class (
        oid INTEGER, relname VARCHAR, relnamespace INTEGER, reltype INTEGER,
        reloftype INTEGER, relowner INTEGER, relam INTEGER, relfilenode INTEGER,
        reltablespace INTEGER, relpages INTEGER, reltuples REAL, relallvisible INTEGER,
        reltoastrelid INTEGER, relhasindex BOOLEAN, relisshared BOOLEAN,
        relpersistence VARCHAR, relkind VARCHAR, relnatts INTEGER, relchecks SMALLINT,
        relhasrules BOOLEAN, relhastriggers BOOLEAN, relhassubclass BOOLEAN,
        relrowsecurity BOOLEAN, relforcerowsecurity BOOLEAN, relispopulated BOOLEAN,
        relreplident VARCHAR, relispartition BOOLEAN, relrewrite INTEGER,
        relfrozenxid INTEGER, relminmxid INTEGER, relacl VARCHAR,
        reloptions VARCHAR, relpartbound VARCHAR)""")
    natts_by_toid: dict[int, int] = {}
    for col in idx.all_cols:
        natts_by_toid[col[0]] = natts_by_toid.get(col[0], 0) + 1
    pg_class_rows = []
    for _, s, t, _, toid in idx.tables:
        ns_oid = idx.ns_map.get(s, 2200)
        natts = natts_by_toid.get(toid, 0)
        reltuples = float(row_counts.get(toid, 0.0)) if row_counts else 0.0
        pg_class_rows.append(
            (
                toid,
                t,
                ns_oid,
                toid + 100000,
                0,
                10,
                0,
                toid,
                0,
                0,
                reltuples,
                0,
                0,
                False,
                False,
                "p",
                "r",
                natts,
                0,
                False,
                False,
                False,
                False,
                False,
                True,
                "d",
                False,
                0,
                0,
                0,
                None,
                None,
                None,
            )
        )
    for vname, oid in _IS_VIEW_OIDS.items():
        natts = len(_SYSTEM_TABLE_COLUMNS.get(vname, []))
        pg_class_rows.append(
            (
                oid,
                vname,
                12,
                oid + 100000,
                0,
                10,
                0,
                oid,
                0,
                0,
                0.0,
                0,
                0,
                False,
                False,
                "p",
                "v",
                natts,
                0,
                False,
                False,
                False,
                False,
                False,
                True,
                "d",
                False,
                0,
                0,
                0,
                None,
                None,
                None,
            )
        )
    for tname, oid in _PG_CAT_TABLE_OIDS.items():
        natts = len(_SYSTEM_TABLE_COLUMNS.get(tname, []))
        pg_class_rows.append(
            (
                oid,
                tname,
                11,
                oid + 100000,
                0,
                10,
                0,
                oid,
                0,
                0,
                0.0,
                0,
                0,
                False,
                False,
                "p",
                "r",
                natts,
                0,
                False,
                False,
                False,
                False,
                False,
                True,
                "d",
                False,
                0,
                0,
                0,
                None,
                None,
                None,
            )
        )
    if pg_class_rows:
        db.executemany(f"INSERT INTO _pg_class VALUES ({','.join(['?'] * 33)})", pg_class_rows)


def _populate_pg_description(
    db, idx: CatalogIndex, raw_tables: list, raw_domains: list | None = None
) -> None:
    from provisa.compiler.naming import domain_to_sql_name

    tid_desc: dict[int, str] = {}
    tid_col_desc: dict[int, dict[str, str]] = {}
    for rt in raw_tables:
        _tid = rt["id"] if isinstance(rt, dict) else getattr(rt, "id", None)
        _tdesc = rt["description"] if isinstance(rt, dict) else getattr(rt, "description", None)
        _cols = rt["columns"] if isinstance(rt, dict) else getattr(rt, "columns", [])
        if _tid is None:
            continue
        if _tdesc:
            tid_desc[_tid] = _tdesc
        cdesc: dict[str, str] = {}
        for col in _cols:
            _cname = col["column_name"] if isinstance(col, dict) else getattr(col, "name", "")
            _cdesc = (
                col["description"] if isinstance(col, dict) else getattr(col, "description", None)
            )
            if _cdesc:
                cdesc[_cname] = _cdesc
        if cdesc:
            tid_col_desc[_tid] = cdesc

    desc_rows: list[tuple] = []

    # Namespace (schema/domain) descriptions
    for dom in raw_domains or []:
        _did = dom["id"] if isinstance(dom, dict) else getattr(dom, "id", None)
        _ddesc = dom["description"] if isinstance(dom, dict) else getattr(dom, "description", None)
        if not _did or not _ddesc:
            continue
        ns_oid = idx.ns_map.get(domain_to_sql_name(_did))
        if ns_oid is not None:
            desc_rows.append((ns_oid, "pg_namespace", 0, _ddesc))

    for _, _, _, table_id, toid in idx.tables:
        tdesc = tid_desc.get(table_id)
        if tdesc:
            desc_rows.append((toid, "pg_class", 0, tdesc))
        for cname, cdesc_val in (tid_col_desc.get(table_id) or {}).items():
            attnum = idx.col_attnum.get((toid, cname))
            if attnum is not None:
                desc_rows.append((toid, "pg_class", attnum, cdesc_val))

    if desc_rows:
        db.executemany("INSERT INTO _pg_description VALUES (?,?,?,?)", desc_rows)


def _populate_pg_attribute(db, idx: CatalogIndex) -> None:
    db.execute("""CREATE TABLE _pg_attribute (
        attrelid INTEGER, attname VARCHAR, atttypid INTEGER, attstattarget INTEGER,
        attlen SMALLINT, attnum SMALLINT, attndims INTEGER, attcacheoff INTEGER,
        atttypmod INTEGER, attbyval BOOLEAN, attalign VARCHAR, attstorage VARCHAR,
        attnotnull BOOLEAN, atthasdef BOOLEAN, atthasmissing BOOLEAN,
        attidentity VARCHAR, attgenerated VARCHAR, attisdropped BOOLEAN,
        attislocal BOOLEAN, attinhcount INTEGER, attcollation INTEGER,
        attacl VARCHAR, attoptions VARCHAR, attfdwoptions VARCHAR)""")
    attr_rows = []
    for toid, col_name, col_type, is_nullable, ordinal in idx.all_cols:
        pg_oid = _trino_to_pg_oid(col_type)
        attlen, attbyval, attalign, attstorage = _PG_OID_ATTR_META.get(
            pg_oid, (-1, False, "i", "x")
        )
        attr_rows.append(
            (
                toid,
                col_name,
                pg_oid,
                -1,
                attlen,
                ordinal,
                0,
                -1,
                -1,
                attbyval,
                attalign,
                attstorage,
                not is_nullable,
                False,
                False,
                "",
                "",
                False,
                True,
                0,
                0,
                None,
                None,
                None,
            )
        )
    if attr_rows:
        db.executemany(f"INSERT INTO _pg_attribute VALUES ({','.join(['?'] * 24)})", attr_rows)


def _populate_system_attributes(db) -> None:
    """Add pg_attribute rows for pg_catalog and information_schema system objects."""
    _type_to_oid = {
        "varchar": 1043,
        "name": 25,
        "text": 25,
        "oid": 23,
        "integer": 23,
        "smallint": 21,
        "bigint": 20,
        "boolean": 16,
        "real": 700,
        "double": 701,
        "double precision": 701,
        "xid": 23,
        "array": 25,
        "char": 18,
        "timestamp with time zone": 1184,
        "timestamp": 1114,
    }
    attr_rows = []
    for is_name, oid in _IS_VIEW_OIDS.items():
        for attnum, (col_name, col_type) in enumerate(_SYSTEM_TABLE_COLUMNS.get(is_name, []), 1):
            pg_oid = _type_to_oid.get(col_type.lower().split("(")[0].strip(), 25)
            attlen, attbyval, attalign, attstorage = _PG_OID_ATTR_META.get(
                pg_oid, (-1, False, "i", "x")
            )
            attr_rows.append(
                (
                    oid,
                    col_name,
                    pg_oid,
                    -1,
                    attlen,
                    attnum,
                    0,
                    -1,
                    -1,
                    attbyval,
                    attalign,
                    attstorage,
                    False,
                    False,
                    False,
                    "",
                    "",
                    False,
                    True,
                    0,
                    0,
                    None,
                    None,
                    None,
                )
            )
    for pg_name, oid in _PG_CAT_TABLE_OIDS.items():
        for attnum, (col_name, col_type) in enumerate(_SYSTEM_TABLE_COLUMNS.get(pg_name, []), 1):
            pg_oid = _type_to_oid.get(col_type.lower().split("(")[0].strip(), 25)
            attlen, attbyval, attalign, attstorage = _PG_OID_ATTR_META.get(
                pg_oid, (-1, False, "i", "x")
            )
            attr_rows.append(
                (
                    oid,
                    col_name,
                    pg_oid,
                    -1,
                    attlen,
                    attnum,
                    0,
                    -1,
                    -1,
                    attbyval,
                    attalign,
                    attstorage,
                    False,
                    False,
                    False,
                    "",
                    "",
                    False,
                    True,
                    0,
                    0,
                    None,
                    None,
                    None,
                )
            )
    if attr_rows:
        db.executemany(f"INSERT INTO _pg_attribute VALUES ({','.join(['?'] * 24)})", attr_rows)


def _populate_pg_type(db) -> None:
    db.execute("""CREATE TABLE _pg_type (
        oid INTEGER, typname VARCHAR, typnamespace INTEGER, typowner INTEGER,
        typlen SMALLINT, typbyval BOOLEAN, typtype VARCHAR, typcategory VARCHAR,
        typispreferred BOOLEAN, typisdefined BOOLEAN, typdelim VARCHAR,
        typrelid INTEGER, typelem INTEGER, typarray INTEGER,
        typinput VARCHAR, typoutput VARCHAR, typreceive VARCHAR, typsend VARCHAR,
        typmodin VARCHAR, typmodout VARCHAR, typanalyze VARCHAR,
        typalign VARCHAR, typstorage VARCHAR, typnotnull BOOLEAN,
        typbasetype INTEGER, typtypmod INTEGER, typndims INTEGER, typcollation INTEGER,
        typdefaultbin VARCHAR, typdefault VARCHAR, typacl VARCHAR)""")
    db.executemany(
        f"INSERT INTO _pg_type VALUES ({','.join(['?'] * 31)})",
        [
            (
                oid,
                name,
                ns,
                10,
                ln,
                byval,
                tt,
                cat,
                False,
                True,
                ",",
                0,
                0,
                0,
                "-",
                "-",
                "-",
                "-",
                "-",
                "-",
                "-",
                align,
                storage,
                nn,
                base,
                -1,
                0,
                0,
                None,
                None,
                None,
            )
            for oid, name, ns, ln, tt, cat, nn, base, byval, align, storage in _PG_TYPE_ROWS
        ],
    )


def _populate_empty_system_tables(db) -> None:
    db.execute(
        "CREATE TABLE _pg_attrdef (oid INTEGER, adrelid INTEGER, adnum SMALLINT, adbin VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_description (objoid INTEGER, classoid VARCHAR, objsubid INTEGER, description VARCHAR)"
    )
    db.execute("""CREATE TABLE _pg_index (
        indexrelid INTEGER, indrelid INTEGER, indnatts SMALLINT, indnkeyatts SMALLINT,
        indisunique BOOLEAN, indisprimary BOOLEAN, indisexclusion BOOLEAN,
        indimmediate BOOLEAN, indisclustered BOOLEAN, indisvalid BOOLEAN,
        indcheckxmin BOOLEAN, indisready BOOLEAN, indislive BOOLEAN,
        indisreplident BOOLEAN, indkey INTEGER[], indcollation INTEGER[],
        indclass INTEGER[], indoption SMALLINT[], indexprs VARCHAR, indpred VARCHAR)""")
    db.execute("""CREATE TABLE _pg_proc (
        oid INTEGER, proname VARCHAR, pronamespace INTEGER, proowner INTEGER,
        prolang INTEGER, procost REAL, prorows REAL, provariadic INTEGER,
        prosupport VARCHAR, prokind VARCHAR, prosecdef BOOLEAN, proleakproof BOOLEAN,
        proisstrict BOOLEAN, proretset BOOLEAN, provolatile VARCHAR, proparallel VARCHAR,
        pronargs SMALLINT, pronargdefaults SMALLINT, prorettype INTEGER,
        proargtypes INTEGER[], proallargtypes INTEGER[], proargmodes VARCHAR[],
        proargnames VARCHAR, proargdefaults VARCHAR, protrftypes VARCHAR,
        prosrc VARCHAR, probin VARCHAR, prosqlbody VARCHAR,
        proconfig VARCHAR, proacl VARCHAR)""")
    db.execute(
        "CREATE TABLE _pg_auth_members (roleid INTEGER, member INTEGER, grantor INTEGER, admin_option BOOLEAN)"
    )
    db.execute(
        "CREATE TABLE _pg_tablespace (oid INTEGER, spcname VARCHAR, spcowner INTEGER, spcacl VARCHAR, spcoptions VARCHAR)"
    )
    db.execute("INSERT INTO _pg_tablespace VALUES (1663, 'pg_default', 10, NULL, NULL)")
    db.execute("INSERT INTO _pg_tablespace VALUES (1664, 'pg_global', 10, NULL, NULL)")
    db.execute(
        "CREATE TABLE _pg_conversion (oid INTEGER, conname VARCHAR, connamespace INTEGER, conowner INTEGER, conforencoding INTEGER, contoencoding INTEGER, conproc INTEGER, condefault BOOLEAN)"
    )
    db.execute(
        "CREATE TABLE _pg_shdescription (objoid INTEGER, classoid INTEGER, description VARCHAR)"
    )
    db.execute("""CREATE TABLE _pg_extension (
        oid INTEGER, extname VARCHAR, extowner INTEGER, extnamespace INTEGER,
        extrelocatable BOOLEAN, extversion VARCHAR, extconfig VARCHAR[], extcondition VARCHAR[])""")
    db.execute("""CREATE TABLE _pg_enum (
        oid INTEGER, enumtypid INTEGER, enumsortorder REAL, enumlabel VARCHAR)""")
    db.execute("""CREATE TABLE _pg_stat_activity (
        datid INTEGER, datname VARCHAR, pid INTEGER, usesysid INTEGER,
        usename VARCHAR, application_name VARCHAR, client_addr VARCHAR,
        client_hostname VARCHAR, client_port INTEGER, backend_start VARCHAR,
        xact_start VARCHAR, query_start VARCHAR, state_change VARCHAR,
        wait_event_type VARCHAR, wait_event VARCHAR, state VARCHAR,
        backend_xid INTEGER, backend_xmin INTEGER, query VARCHAR,
        backend_type VARCHAR)""")
    db.execute("""CREATE TABLE _pg_stat_user_tables (
        relid INTEGER, schemaname VARCHAR, relname VARCHAR,
        seq_scan BIGINT, seq_tup_read BIGINT, idx_scan BIGINT, idx_tup_fetch BIGINT,
        n_tup_ins BIGINT, n_tup_upd BIGINT, n_tup_del BIGINT, n_tup_hot_upd BIGINT,
        n_live_tup BIGINT, n_dead_tup BIGINT, n_mod_since_analyze BIGINT,
        n_ins_since_vacuum BIGINT, last_vacuum VARCHAR, last_autovacuum VARCHAR,
        last_analyze VARCHAR, last_autoanalyze VARCHAR, vacuum_count BIGINT,
        autovacuum_count BIGINT, analyze_count BIGINT, autoanalyze_count BIGINT)""")
    db.execute("""CREATE TABLE _is_views (
        table_catalog VARCHAR, table_schema VARCHAR, table_name VARCHAR,
        view_definition VARCHAR, check_option VARCHAR, is_updatable VARCHAR,
        is_insertable_into VARCHAR, is_trigger_updatable VARCHAR,
        is_trigger_deletable VARCHAR, is_trigger_insertable_into VARCHAR)""")
    db.execute("""CREATE TABLE _is_referential_constraints (
        constraint_catalog VARCHAR, constraint_schema VARCHAR, constraint_name VARCHAR,
        unique_constraint_catalog VARCHAR, unique_constraint_schema VARCHAR,
        unique_constraint_name VARCHAR, match_option VARCHAR,
        update_rule VARCHAR, delete_rule VARCHAR)""")
    db.execute(
        "CREATE TABLE _pg_trigger (oid INTEGER, tgrelid INTEGER, tgparentid INTEGER, tgname VARCHAR, tgfoid INTEGER, tgtype SMALLINT, tgenabled VARCHAR, tgisinternal BOOLEAN, tgconstrrelid INTEGER, tgconstrindid INTEGER, tgconstraint INTEGER, tgdeferrable BOOLEAN, tginitdeferred BOOLEAN, tgnargs SMALLINT, tgattr VARCHAR, tgargs VARCHAR, tgqual VARCHAR, tgoldtable VARCHAR, tgnewtable VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_inherits (inhrelid INTEGER, inhparent INTEGER, inhseqno INTEGER, inhdetachpending BOOLEAN)"
    )
    db.execute(
        "CREATE TABLE _pg_rewrite (oid INTEGER, rulename VARCHAR, ev_class INTEGER, ev_type VARCHAR, ev_enabled VARCHAR, is_instead BOOLEAN, ev_qual VARCHAR, ev_action VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_depend (classid INTEGER, objid INTEGER, objsubid INTEGER, refclassid INTEGER, refobjid INTEGER, refobjsubid INTEGER, deptype VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_shdepend (dbid INTEGER, classid INTEGER, objid INTEGER, objsubid INTEGER, refclassid INTEGER, refobjid INTEGER, deptype VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_aggregate (aggfnoid INTEGER, aggkind VARCHAR, aggnumdirectargs SMALLINT, aggtransfn INTEGER, aggfinalfn INTEGER, aggcombinefn INTEGER, aggserialfn INTEGER, aggdeserialfn INTEGER, aggmtransfn INTEGER, aggminvtransfn INTEGER, aggmfinalfn INTEGER, aggfinalextra BOOLEAN, aggmfinalextra BOOLEAN, aggfinalmodify VARCHAR, aggmfinalmodify VARCHAR, aggsortop INTEGER, aggtranstype INTEGER, aggtransspace INTEGER, aggmtranstype INTEGER, aggmtransspace INTEGER, agginitval VARCHAR, aggminitval VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_language (oid INTEGER, lanname VARCHAR, lanowner INTEGER, lanispl BOOLEAN, lanpltrusted BOOLEAN, lanplcallfoid INTEGER, laninline INTEGER, lanvalidator INTEGER, lanacl VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_operator (oid INTEGER, oprname VARCHAR, oprnamespace INTEGER, oprowner INTEGER, oprkind VARCHAR, oprcanmerge BOOLEAN, oprcanhash BOOLEAN, oprleft INTEGER, oprright INTEGER, oprresult INTEGER, oprcom INTEGER, oprnegate INTEGER, oprcode INTEGER, oprrest INTEGER, oprjoin INTEGER)"
    )
    db.execute(
        "CREATE TABLE _pg_cast (oid INTEGER, castsource INTEGER, casttarget INTEGER, castfunc INTEGER, castcontext VARCHAR, castmethod VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_opfamily (oid INTEGER, opfmethod INTEGER, opfname VARCHAR, opfnamespace INTEGER, opfowner INTEGER)"
    )
    db.execute(
        "CREATE TABLE _pg_opclass (oid INTEGER, opcmethod INTEGER, opcname VARCHAR, opcnamespace INTEGER, opcowner INTEGER, opcfamily INTEGER, opcintype INTEGER, opcdefault BOOLEAN, opckeytype INTEGER)"
    )
    db.execute(
        "CREATE TABLE _pg_amop (oid INTEGER, amopfamily INTEGER, amoplefttype INTEGER, amoprighttype INTEGER, amopstrategy SMALLINT, amoppurpose VARCHAR, amopopr INTEGER, amopmethod INTEGER, amopsortfamily INTEGER)"
    )
    db.execute(
        "CREATE TABLE _pg_amproc (oid INTEGER, amprocfamily INTEGER, amproclefttype INTEGER, amprocrighttype INTEGER, amprocnum SMALLINT, amproc INTEGER)"
    )
    db.execute(
        "CREATE TABLE _pg_collation (oid INTEGER, collname VARCHAR, collnamespace INTEGER, collowner INTEGER, collprovider VARCHAR, collisdeterministic BOOLEAN, collencoding INTEGER, collcollate VARCHAR, collctype VARCHAR, collversion VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_range (rngtypid INTEGER, rngsubtype INTEGER, rngmultitypid INTEGER, rngcollation INTEGER, rngsubopc INTEGER, rngcanonical INTEGER, rngsubdiff INTEGER)"
    )
    db.execute(
        "CREATE TABLE _pg_foreign_table (ftrelid INTEGER, ftserver INTEGER, ftoptions VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_foreign_server (oid INTEGER, srvname VARCHAR, srvowner INTEGER, srvfdw INTEGER, srvtype VARCHAR, srvversion VARCHAR, srvacl VARCHAR, srvoptions VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_user_mapping (oid INTEGER, umuser INTEGER, umserver INTEGER, umoptions VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_user_mappings (umid INTEGER, srvid INTEGER, srvname VARCHAR, umuser INTEGER, usename VARCHAR, umoptions VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_foreign_data_wrapper (oid INTEGER, fdwname VARCHAR, fdwowner INTEGER, fdwhandler INTEGER, fdwvalidator INTEGER, fdwacl VARCHAR, fdwoptions VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_sequence (seqrelid INTEGER, seqtypid INTEGER, seqstart BIGINT, seqincrement BIGINT, seqmax BIGINT, seqmin BIGINT, seqcache BIGINT, seqcycle BOOLEAN)"
    )
    db.execute(
        "CREATE TABLE _pg_policy (oid INTEGER, polname VARCHAR, polrelid INTEGER, polcmd VARCHAR, polpermissive BOOLEAN, polroles VARCHAR, polqual VARCHAR, polwithcheck VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_partitioned_table (partrelid INTEGER, partstrat VARCHAR, partnatts SMALLINT, partdefid INTEGER, partattrs VARCHAR, partclass VARCHAR, partcollation VARCHAR, partexprs VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_publication (oid INTEGER, pubname VARCHAR, pubowner INTEGER, puballtables BOOLEAN, pubinsert BOOLEAN, pubupdate BOOLEAN, pubdelete BOOLEAN, pubtruncate BOOLEAN, pubviaroot BOOLEAN)"
    )
    db.execute(
        "CREATE TABLE _pg_subscription (oid INTEGER, subdbid INTEGER, subskiplsn VARCHAR, subname VARCHAR, subowner INTEGER, subenabled BOOLEAN, subbinary BOOLEAN, substream VARCHAR, subtwophasestate VARCHAR, subdisableonerr BOOLEAN, subpasswordrequired BOOLEAN, subrunasowner BOOLEAN, subconninfo VARCHAR, subslotname VARCHAR, subsynccommit VARCHAR, subpublications VARCHAR, suborigin VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_event_trigger (oid INTEGER, evtname VARCHAR, evtevent VARCHAR, evtowner INTEGER, evtfoid INTEGER, evtenabled VARCHAR, evttags VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_stat_user_indexes (relid INTEGER, indexrelid INTEGER, schemaname VARCHAR, relname VARCHAR, indexrelname VARCHAR, idx_scan BIGINT, idx_tup_read BIGINT, idx_tup_fetch BIGINT)"
    )
    db.execute(
        "CREATE TABLE _pg_locks (locktype VARCHAR, database INTEGER, relation INTEGER, page INTEGER, tuple SMALLINT, virtualxid VARCHAR, transactionid INTEGER, classid INTEGER, objid INTEGER, objsubid SMALLINT, virtualtransaction VARCHAR, pid INTEGER, mode VARCHAR, granted BOOLEAN, fastpath BOOLEAN, waitstart VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_stat_ssl (pid INTEGER, ssl BOOLEAN, version VARCHAR, cipher VARCHAR, bits INTEGER, client_dn VARCHAR, client_serial VARCHAR, issuer_dn VARCHAR)"
    )
    db.execute("INSERT INTO _pg_stat_ssl VALUES (0, false, NULL, NULL, NULL, NULL, NULL, NULL)")
    db.execute(
        "CREATE TABLE _pg_timezone_names (name VARCHAR, abbrev VARCHAR, utc_offset VARCHAR, is_dst BOOLEAN)"
    )
    db.execute(
        "CREATE TABLE _pg_timezone_abbrevs (abbrev VARCHAR, utc_offset VARCHAR, is_dst BOOLEAN)"
    )
    db.execute(
        "CREATE TABLE _is_role_table_grants (grantor VARCHAR, grantee VARCHAR, table_catalog VARCHAR, table_schema VARCHAR, table_name VARCHAR, privilege_type VARCHAR, is_grantable VARCHAR, with_hierarchy VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _is_role_column_grants (grantor VARCHAR, grantee VARCHAR, table_catalog VARCHAR, table_schema VARCHAR, table_name VARCHAR, column_name VARCHAR, privilege_type VARCHAR, is_grantable VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _is_triggers (trigger_catalog VARCHAR, trigger_schema VARCHAR, trigger_name VARCHAR, event_manipulation VARCHAR, event_object_catalog VARCHAR, event_object_schema VARCHAR, event_object_table VARCHAR, action_order INTEGER, action_condition VARCHAR, action_statement VARCHAR, action_orientation VARCHAR, action_timing VARCHAR, action_reference_old_table VARCHAR, action_reference_new_table VARCHAR, action_reference_old_row VARCHAR, action_reference_new_row VARCHAR, created VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _is_sequences (sequence_catalog VARCHAR, sequence_schema VARCHAR, sequence_name VARCHAR, data_type VARCHAR, numeric_precision INTEGER, numeric_precision_radix INTEGER, numeric_scale INTEGER, start_value VARCHAR, minimum_value VARCHAR, maximum_value VARCHAR, increment VARCHAR, cycle_option VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _is_routines (specific_catalog VARCHAR, specific_schema VARCHAR, specific_name VARCHAR, routine_catalog VARCHAR, routine_schema VARCHAR, routine_name VARCHAR, routine_type VARCHAR, module_catalog VARCHAR, module_schema VARCHAR, module_name VARCHAR, udt_catalog VARCHAR, udt_schema VARCHAR, udt_name VARCHAR, data_type VARCHAR, character_maximum_length INTEGER, character_octet_length INTEGER, character_set_catalog VARCHAR, character_set_schema VARCHAR, character_set_name VARCHAR, collation_catalog VARCHAR, collation_schema VARCHAR, collation_name VARCHAR, numeric_precision INTEGER, numeric_precision_radix INTEGER, numeric_scale INTEGER, datetime_precision INTEGER, interval_type VARCHAR, interval_precision INTEGER, type_udt_catalog VARCHAR, type_udt_schema VARCHAR, type_udt_name VARCHAR, scope_catalog VARCHAR, scope_schema VARCHAR, scope_name VARCHAR, maximum_cardinality INTEGER, dtd_identifier VARCHAR, routine_body VARCHAR, routine_definition VARCHAR, external_name VARCHAR, external_language VARCHAR, parameter_style VARCHAR, is_deterministic VARCHAR, sql_data_access VARCHAR, is_null_call VARCHAR, sql_path VARCHAR, schema_level_routine VARCHAR, max_dynamic_result_sets INTEGER, is_user_defined_cast VARCHAR, is_implicitly_invocable VARCHAR, security_type VARCHAR, to_sql_specific_catalog VARCHAR, to_sql_specific_schema VARCHAR, to_sql_specific_name VARCHAR, as_locator VARCHAR, created VARCHAR, last_altered VARCHAR, new_savepoint_level VARCHAR, is_udt_dependent VARCHAR, result_cast_from_data_type VARCHAR, result_cast_as_locator VARCHAR, result_cast_char_max_length INTEGER, result_cast_char_octet_length INTEGER, result_cast_char_set_catalog VARCHAR, result_cast_char_set_schema VARCHAR, result_cast_char_set_name VARCHAR, result_cast_collation_catalog VARCHAR, result_cast_collation_schema VARCHAR, result_cast_collation_name VARCHAR, result_cast_numeric_precision INTEGER, result_cast_numeric_precision_radix INTEGER, result_cast_numeric_scale INTEGER, result_cast_datetime_precision INTEGER, result_cast_interval_type VARCHAR, result_cast_interval_precision INTEGER, result_cast_type_udt_catalog VARCHAR, result_cast_type_udt_schema VARCHAR, result_cast_type_udt_name VARCHAR, result_cast_scope_catalog VARCHAR, result_cast_scope_schema VARCHAR, result_cast_scope_name VARCHAR, result_cast_maximum_cardinality INTEGER, result_cast_dtd_identifier VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _is_parameters (specific_catalog VARCHAR, specific_schema VARCHAR, specific_name VARCHAR, ordinal_position INTEGER, parameter_mode VARCHAR, is_result VARCHAR, as_locator VARCHAR, parameter_name VARCHAR, data_type VARCHAR, character_maximum_length INTEGER, character_octet_length INTEGER, character_set_catalog VARCHAR, character_set_schema VARCHAR, character_set_name VARCHAR, collation_catalog VARCHAR, collation_schema VARCHAR, collation_name VARCHAR, numeric_precision INTEGER, numeric_precision_radix INTEGER, numeric_scale INTEGER, datetime_precision INTEGER, interval_type VARCHAR, interval_precision INTEGER, udt_catalog VARCHAR, udt_schema VARCHAR, udt_name VARCHAR, scope_catalog VARCHAR, scope_schema VARCHAR, scope_name VARCHAR, maximum_cardinality INTEGER, dtd_identifier VARCHAR, parameter_default VARCHAR)"
    )
    db.execute("CREATE TABLE _is_enabled_roles (role_name VARCHAR)")
    db.execute(
        "CREATE TABLE _is_applicable_roles (grantee VARCHAR, role_name VARCHAR, is_grantable VARCHAR)"
    )


_PG_SYSTEM_ROLES: list[tuple] = [
    # OID, name, super, inherit, createrole, createdb, canlogin, replication, connlimit, bypassrls
    (3386, "pg_monitor", False, True, False, False, False, False, -1, False),
    (3387, "pg_read_all_settings", False, True, False, False, False, False, -1, False),
    (3388, "pg_read_all_stats", False, True, False, False, False, False, -1, False),
    (3389, "pg_stat_scan_tables", False, True, False, False, False, False, -1, False),
    (4200, "pg_signal_backend", False, True, False, False, False, False, -1, False),
    (4569, "pg_read_server_files", False, True, False, False, False, False, -1, False),
    (4570, "pg_write_server_files", False, True, False, False, False, False, -1, False),
    (4571, "pg_execute_server_program", False, True, False, False, False, False, -1, False),
]


def _populate_pg_roles_and_database(db, role_id: str, state=None) -> None:
    db.execute("""CREATE TABLE _pg_roles (
        oid INTEGER, rolname VARCHAR, rolsuper BOOLEAN, rolinherit BOOLEAN,
        rolcreaterole BOOLEAN, rolcreatedb BOOLEAN, rolcanlogin BOOLEAN,
        rolreplication BOOLEAN, rolconnlimit INTEGER, rolpassword VARCHAR,
        rolvaliduntil VARCHAR, rolbypassrls BOOLEAN, rolconfig VARCHAR)""")

    rows: list[tuple] = []
    seen_names: set[str] = set()
    # Provisa roles from state (all defined roles, not just the connected one)
    _roles_attr = getattr(state, "roles", None)
    provisa_roles = list(_roles_attr.values()) if isinstance(_roles_attr, dict) else []
    for i, role in enumerate(provisa_roles):
        rname = role["id"] if isinstance(role, dict) else getattr(role, "id", None)
        if not rname or rname in seen_names:
            continue
        seen_names.add(rname)
        roid = 10 + i
        rows.append(
            (roid, rname, False, True, False, False, True, False, -1, None, None, False, None)
        )
    # Ensure the connected role is present even if state.roles is empty
    if role_id not in seen_names:
        rows.append(
            (10, role_id, False, True, False, False, True, False, -1, None, None, False, None)
        )
        seen_names.add(role_id)
    # Standard PG system roles
    for oid, name, sup, inh, crrole, crdb, login, repl, conn, byp in _PG_SYSTEM_ROLES:
        if name not in seen_names:
            rows.append(
                (oid, name, sup, inh, crrole, crdb, login, repl, conn, None, None, byp, None)
            )
            seen_names.add(name)

    db.executemany("INSERT INTO _pg_roles VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    db.execute("""CREATE TABLE _pg_user AS
        SELECT oid AS usesysid, rolname AS usename,
               rolcreatedb AS usecreatedb, rolsuper AS usesuper,
               rolreplication AS userepl, rolbypassrls AS usebypassrls,
               '********' AS passwd, rolvaliduntil AS valuntil,
               rolconfig AS useconfig
        FROM _pg_roles WHERE rolcanlogin""")
    db.execute("""CREATE TABLE _pg_database (
        oid INTEGER, datname VARCHAR, datdba INTEGER, encoding INTEGER,
        datlocprovider VARCHAR, datistemplate BOOLEAN, datallowconn BOOLEAN,
        datconnlimit INTEGER, datfrozenxid INTEGER, datminmxid INTEGER,
        dattablespace INTEGER, datcollate VARCHAR, datctype VARCHAR, datacl VARCHAR)""")
    db.execute(
        "INSERT INTO _pg_database VALUES (16384,'provisa',10,6,'c',FALSE,TRUE,-1,726,1,1663,'en_US.UTF-8','en_US.UTF-8',NULL)"
    )


_PG_SETTINGS_ROWS: list[tuple] = [
    (
        "server_version",
        "14.0.provisa",
        None,
        "Preset Options",
        "Shows the server version.",
        None,
        "internal",
        "string",
        "default",
        None,
        None,
        None,
        "14.0.provisa",
        "14.0.provisa",
        None,
        None,
        False,
    ),
    (
        "server_version_num",
        "140000",
        None,
        "Preset Options",
        "Shows the server version as an integer.",
        None,
        "internal",
        "integer",
        "default",
        None,
        None,
        None,
        "140000",
        "140000",
        None,
        None,
        False,
    ),
    (
        "server_encoding",
        "UTF8",
        None,
        "Preset Options",
        "Sets the server character set encoding.",
        None,
        "internal",
        "string",
        "default",
        None,
        None,
        None,
        "UTF8",
        "UTF8",
        None,
        None,
        False,
    ),
    (
        "client_encoding",
        "UTF8",
        None,
        "Client Connection Defaults",
        "Sets the client character set encoding.",
        None,
        "user",
        "string",
        "default",
        None,
        None,
        None,
        "SQL_ASCII",
        "UTF8",
        None,
        None,
        False,
    ),
    (
        "DateStyle",
        "ISO, MDY",
        None,
        "Client Connection Defaults",
        "Sets the display format for date and time values.",
        None,
        "user",
        "string",
        "default",
        None,
        None,
        None,
        "ISO, MDY",
        "ISO, MDY",
        None,
        None,
        False,
    ),
    (
        "TimeZone",
        "UTC",
        None,
        "Client Connection Defaults",
        "Sets the time zone for displaying and interpreting time stamps.",
        None,
        "user",
        "string",
        "default",
        None,
        None,
        None,
        "GMT",
        "UTC",
        None,
        None,
        False,
    ),
    (
        "max_connections",
        "100",
        None,
        "Connections and Authentication",
        "Sets the maximum number of concurrent connections.",
        None,
        "postmaster",
        "integer",
        "default",
        "1",
        "262143",
        None,
        "100",
        "100",
        None,
        None,
        False,
    ),
    (
        "standard_conforming_strings",
        "on",
        None,
        "Version and Platform Compatibility",
        "Causes strings to treat backslashes literally.",
        None,
        "user",
        "bool",
        "default",
        None,
        None,
        None,
        "on",
        "on",
        None,
        None,
        False,
    ),
    (
        "integer_datetimes",
        "on",
        None,
        "Preset Options",
        "Datetimes are integer based.",
        None,
        "internal",
        "bool",
        "default",
        None,
        None,
        None,
        "on",
        "on",
        None,
        None,
        False,
    ),
    (
        "IntervalStyle",
        "postgres",
        None,
        "Client Connection Defaults",
        "Sets the display format for interval values.",
        None,
        "user",
        "string",
        "default",
        None,
        None,
        None,
        "postgres",
        "postgres",
        None,
        None,
        False,
    ),
]


def _populate_pg_settings(db) -> None:
    db.execute("""CREATE TABLE _pg_settings (
        name VARCHAR, setting VARCHAR, unit VARCHAR, category VARCHAR,
        short_desc VARCHAR, extra_desc VARCHAR, context VARCHAR,
        vartype VARCHAR, source VARCHAR, min_val VARCHAR, max_val VARCHAR,
        enumvals VARCHAR, boot_val VARCHAR, reset_val VARCHAR,
        sourcefile VARCHAR, sourceline INTEGER, pending_restart BOOLEAN)""")
    db.executemany(
        f"INSERT INTO _pg_settings VALUES ({','.join(['?'] * 17)})",
        _PG_SETTINGS_ROWS,
    )


def _populate_pg_tables_and_am(db, idx: CatalogIndex) -> None:
    db.execute("""CREATE TABLE _pg_tables (
        schemaname VARCHAR, tablename VARCHAR, tableowner VARCHAR,
        tablespace VARCHAR, hasindexes BOOLEAN, hasrules BOOLEAN,
        hastriggers BOOLEAN, rowsecurity BOOLEAN)""")
    if idx.tables:
        db.executemany(
            "INSERT INTO _pg_tables VALUES (?,?,'provisa',NULL,FALSE,FALSE,FALSE,FALSE)",
            [(row[1], row[2]) for row in idx.tables],
        )
    db.execute("""CREATE TABLE _pg_am (
        oid INTEGER, amname VARCHAR, amhandler VARCHAR, amtype VARCHAR)""")
    db.executemany(
        "INSERT INTO _pg_am VALUES (?,?,?,?)",
        [
            (2, "heap", "heap_tableam_handler", "t"),
            (403, "btree", "bthandler", "i"),
            (405, "hash", "hashhandler", "i"),
            (783, "gist", "gisthandler", "i"),
            (2742, "gin", "ginhandler", "i"),
            (4000, "spgist", "spghandler", "i"),
            (3580, "brin", "brinhandler", "i"),
        ],
    )


def _build_pk_constraint_rows(
    ctx,
    idx: CatalogIndex,
    con_oid_start: int,
) -> tuple[list[tuple], int]:
    from provisa.compiler.sql_gen import semantic_table_name

    rows: list[tuple] = []
    con_oid = con_oid_start
    seen_table_ids: set[int] = set()
    for _, tm in ctx.tables.items():
        if tm.table_id in seen_table_ids:
            continue
        toid_pk = idx.table_id_to_oid.get(tm.table_id)
        if toid_pk is None:
            continue
        pk_cols = ctx.pk_columns.get(tm.table_id, [])
        if not pk_cols:
            continue
        seen_table_ids.add(tm.table_id)
        ns_oid_pk = idx.ns_map.get(idx.toid_to_table[toid_pk][1], 2200)
        conkey = [idx.col_attnum.get((toid_pk, c), 0) for c in pk_cols]
        rows.append(
            (
                con_oid,
                f"pk_{semantic_table_name(tm)}",
                ns_oid_pk,
                "p",
                False,
                False,
                True,
                toid_pk,
                0,
                0,
                0,
                0,
                None,
                None,
                None,
                True,
                0,
                True,
                conkey,
                None,
                None,
                None,
                None,
                None,
                None,
            )
        )
        con_oid += 1
    return rows, con_oid


def _build_fk_constraint_rows(
    ctx,
    idx: CatalogIndex,
    con_oid_start: int,
) -> tuple[list[tuple], int]:
    from provisa.compiler.sql_gen import semantic_table_name

    rows: list[tuple] = []
    con_oid = con_oid_start
    seen_joins: set[tuple] = set()
    used_names: set[str] = set()
    for (src_type, join_field), jm in ctx.joins.items():
        if not jm.target_column:
            continue
        if jm.cardinality != "many-to-one":
            continue
        src_tm = next((tm for tm in ctx.tables.values() if tm.type_name == src_type), None)
        if src_tm is None:
            continue
        dedup_key = (src_tm.table_id, jm.source_column, jm.target.table_id, jm.target_column)
        if dedup_key in seen_joins:
            continue
        seen_joins.add(dedup_key)
        src_toid = idx.table_id_to_oid.get(src_tm.table_id)
        tgt_toid = idx.table_id_to_oid.get(jm.target.table_id)
        if src_toid is None or tgt_toid is None:
            continue
        ns_oid_fk = idx.ns_map.get(idx.toid_to_table[src_toid][1], 2200)
        is_synthetic = (
            jm.source_constant is not None
            or jm.source_expr is not None
            or jm.source_column.startswith("__")
        )
        from provisa.compiler.naming import apply_sql_name

        src_col_sql = apply_sql_name(jm.source_column)
        tgt_col_sql = apply_sql_name(jm.target_column)
        col_label = join_field if is_synthetic else src_col_sql
        src_sem_name = semantic_table_name(src_tm)
        base_name = f"fk_{src_sem_name}__{col_label}"
        tgt_sem_name = semantic_table_name(jm.target)
        con_name = base_name if base_name not in used_names else f"{base_name}__{tgt_sem_name}"
        used_names.add(con_name)
        attnum_col = src_col_sql
        if jm.source_column.startswith("__"):
            attnum_col = "_name_"
        src_attnum = idx.col_attnum.get((src_toid, attnum_col), 0)
        tgt_attnum = idx.col_attnum.get((tgt_toid, tgt_col_sql), 0)
        if src_attnum == 0:
            continue
        rows.append(
            (
                con_oid,
                con_name,
                ns_oid_fk,
                "f",
                False,
                False,
                True,
                src_toid,
                0,
                0,
                0,
                tgt_toid,
                "a",
                "a",
                "s",
                True,
                0,
                True,
                [src_attnum],
                [tgt_attnum],
                None,
                None,
                None,
                None,
                None,
            )
        )
        con_oid += 1
    return rows, con_oid


def _populate_pg_constraint(db, ctx, idx: CatalogIndex) -> list[tuple]:
    db.execute("""CREATE TABLE _pg_constraint (
        oid INTEGER, conname VARCHAR, connamespace INTEGER, contype VARCHAR,
        condeferrable BOOLEAN, condeferred BOOLEAN, convalidated BOOLEAN,
        conrelid INTEGER, contypid INTEGER, conindid INTEGER, conparentid INTEGER,
        confrelid INTEGER, confupdtype VARCHAR, confdeltype VARCHAR, confmatchtype VARCHAR,
        conislocal BOOLEAN, coninhcount INTEGER, connoinherit BOOLEAN,
        conkey INTEGER[], confkey INTEGER[], conpfeqop INTEGER[], conppeqop INTEGER[],
        conffeqop INTEGER[], conexclop INTEGER[], conbin VARCHAR)""")
    constraint_rows: list[tuple] = []
    if ctx:
        pk_rows, next_oid = _build_pk_constraint_rows(ctx, idx, 20000)
        constraint_rows.extend(pk_rows)
        fk_rows, _ = _build_fk_constraint_rows(ctx, idx, next_oid)
        constraint_rows.extend(fk_rows)
    if constraint_rows:
        db.executemany(
            f"INSERT INTO _pg_constraint VALUES ({','.join(['?'] * 25)})",
            constraint_rows,
        )
    return constraint_rows


def _populate_is_constraints(db, constraint_rows: list[tuple], idx: CatalogIndex) -> None:
    db.execute("""CREATE TABLE _is_table_constraints (
        constraint_catalog VARCHAR, constraint_schema VARCHAR, constraint_name VARCHAR,
        table_catalog VARCHAR, table_schema VARCHAR, table_name VARCHAR,
        constraint_type VARCHAR, is_deferrable VARCHAR, initially_deferred VARCHAR,
        enforced VARCHAR, nulls_distinct VARCHAR)""")
    db.execute("""CREATE TABLE _is_key_column_usage (
        constraint_catalog VARCHAR, constraint_schema VARCHAR, constraint_name VARCHAR,
        table_catalog VARCHAR, table_schema VARCHAR, table_name VARCHAR,
        column_name VARCHAR, ordinal_position INTEGER, position_in_unique_constraint INTEGER)""")
    if not constraint_rows:
        return
    oid_to_ns: dict[int, str] = {v: k for k, v in idx.ns_map.items()}
    is_tc_rows: list[tuple] = []
    is_kcu_rows: list[tuple] = []
    for con_row in constraint_rows:
        conname_v: str = con_row[1]
        conns_oid_v: int = con_row[2]
        contype_v: str = con_row[3]
        conrelid_v: int = con_row[7]
        c_v, c_sch_v, c_tname_v = idx.toid_to_table.get(conrelid_v, ("provisa", "public", ""))
        con_schema_v = oid_to_ns.get(conns_oid_v, "public")
        ctype_str = "PRIMARY KEY" if contype_v == "p" else "FOREIGN KEY"
        is_tc_rows.append(
            (
                "provisa",
                con_schema_v,
                conname_v,
                c_v,
                c_sch_v,
                c_tname_v,
                ctype_str,
                "NO",
                "NO",
                "YES",
                "YES",
            )
        )
        conkeys_raw = con_row[18]
        conkeys_list: list[int] = list(conkeys_raw) if conkeys_raw else []
        for pos, attnum_v in enumerate(conkeys_list, 1):
            col_name_v = idx.attnum_to_col.get((conrelid_v, int(attnum_v)), "")
            if col_name_v:
                is_kcu_rows.append(
                    (
                        "provisa",
                        con_schema_v,
                        conname_v,
                        c_v,
                        c_sch_v,
                        c_tname_v,
                        col_name_v,
                        pos,
                        pos if contype_v == "p" else None,
                    )
                )
    if is_tc_rows:
        db.executemany(
            f"INSERT INTO _is_table_constraints VALUES ({','.join(['?'] * 11)})",
            is_tc_rows,
        )
    if is_kcu_rows:
        db.executemany(
            f"INSERT INTO _is_key_column_usage VALUES ({','.join(['?'] * 9)})",
            is_kcu_rows,
        )


_row_count_cache: dict[str, tuple[float, dict[int, float]]] = {}
_ROW_COUNT_TTL = 300.0


def _fetch_row_counts(ctx, idx: CatalogIndex, trino_conn) -> dict[int, float]:
    """Fetch row count estimates via SHOW STATS FOR. Returns {toid: row_count}."""
    if ctx is None or trino_conn is None:
        return {}
    table_id_to_meta: dict[int, tuple[str, str, str]] = {
        tm.table_id: (tm.catalog_name, tm.schema_name, tm.table_name) for tm in ctx.tables.values()
    }
    result: dict[int, float] = {}
    for _, _, _, table_id, toid in idx.tables:
        ref = table_id_to_meta.get(table_id)
        if not ref:
            continue
        cat, sch, tname = ref
        try:
            cur = trino_conn.cursor()
            cur.execute(f'SHOW STATS FOR "{cat}"."{sch}"."{tname}"')
            for row in cur.fetchall():
                if row[0] is None and row[4] is not None:
                    result[toid] = float(row[4])
                    break
        except Exception:
            pass
    return result


def _build_catalog_db(role_id: str, state):  # REQ-127, REQ-128, REQ-363
    import duckdb

    db = duckdb.connect(":memory:")
    db.execute("CREATE MACRO pg_backend_pid() AS 0")
    db.execute("CREATE MACRO age(x) AS 0")
    db.execute("CREATE MACRO quote_ident(x) AS '\"' || replace(x, '\"', '\"\"') || '\"'")
    db.execute("""CREATE MACRO pg_available_extensions() AS TABLE
        SELECT CAST(NULL AS VARCHAR) AS name, CAST(NULL AS VARCHAR) AS default_version,
               CAST(NULL AS VARCHAR) AS installed_version, CAST(NULL AS VARCHAR) AS comment
        LIMIT 0""")
    db.execute("""CREATE MACRO pg_available_extension_versions() AS TABLE
        SELECT CAST(NULL AS VARCHAR) AS name, CAST(NULL AS VARCHAR) AS version,
               FALSE AS installed, FALSE AS superuser, FALSE AS trusted,
               FALSE AS relocatable, CAST(NULL AS VARCHAR) AS schema,
               CAST(NULL AS VARCHAR[]) AS requires, CAST(NULL AS VARCHAR) AS comment
        LIMIT 0""")
    ctx = state.contexts.get(role_id)
    col_types: dict = state.schema_build_cache.get("column_types", {})
    idx = _build_catalog_index(ctx, col_types)

    now = time.monotonic()
    cached = _row_count_cache.get(role_id)
    if cached and now - cached[0] < _ROW_COUNT_TTL:
        row_counts = cached[1]
    else:
        row_counts = _fetch_row_counts(ctx, idx, getattr(state, "trino_conn", None))
        _row_count_cache[role_id] = (now, row_counts)

    _populate_is_schemata(db, idx)
    _populate_is_tables(db, idx)
    _populate_is_columns(db, idx)
    _populate_pg_namespace(db, idx)
    _populate_pg_class(db, idx, row_counts)
    _populate_pg_attribute(db, idx)
    _populate_system_attributes(db)
    _populate_pg_type(db)
    _populate_empty_system_tables(db)
    raw_tables = state.schema_build_cache.get("tables", []) if state else []
    raw_domains = state.schema_build_cache.get("domains", []) if state else []
    _populate_pg_description(db, idx, raw_tables, raw_domains)
    constraint_rows = _populate_pg_constraint(db, ctx, idx)
    _populate_pg_roles_and_database(db, role_id, state)
    _populate_pg_settings(db)
    _populate_pg_tables_and_am(db, idx)
    _populate_is_constraints(db, constraint_rows, idx)

    return db


def _rewrite_for_duckdb(sql: str, role_id: str = "") -> str:
    """Rewrite catalog table refs for DuckDB and transpile from postgres dialect."""
    import sqlglot
    import sqlglot.expressions as exp
    import re as _pre_re

    # Convert PG array literals with type casts to DuckDB array syntax before
    # sqlglot parses — sqlglot misparses e.g. '{16395}'::oid[] (treats [] as
    # bracket indexing, not array type), causing a silent fallback to original SQL.
    # '{a,b,c}'::type[] → [a,b,c]
    sql = _pre_re.sub(
        r"'\{([^}]*)\}'::\w+(?:\[\])+",
        lambda m: "[" + m.group(1) + "]",
        sql,
    )
    # ARRAY[a,b]::type[] → ARRAY[a,b] (strip redundant cast; DuckDB infers type)
    sql = _pre_re.sub(r"(ARRAY\[[^\]]*\])::\w+(?:\[\])+", r"\1", sql, flags=_pre_re.IGNORECASE)

    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except Exception:
        return sql

    def _transform(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        if isinstance(node, exp.Table):
            # Schema-qualified scalar function used as a TVF (e.g. pg_catalog.pg_indexam_has_property(...) amcanorder).
            # Rewrite to a lateral subquery so DuckDB can parse it.
            if isinstance(node.this, exp.Anonymous):
                fn_result = _transform(node.this)
                col_name = node.alias if node.alias else node.this.name.lower()
                if fn_result is node.this:
                    # Function not rewritten to a scalar — strip schema qualifier so
                    # DuckDB can call its own TABLE macro (e.g. pg_available_extensions).
                    new_tbl = exp.Table(this=node.this)
                    if node.alias:
                        new_tbl.set("alias", node.args.get("alias"))
                    return new_tbl
                # Function rewritten to a scalar — wrap in a derived table so it is
                # valid in FROM/JOIN position (e.g. pg_indexam_has_property → false).
                inner = exp.select(exp.alias_(fn_result, col_name))
                return exp.Subquery(
                    this=inner,
                    alias=exp.TableAlias(
                        this=exp.Identifier(this=f"_tvf_{col_name}", quoted=False)
                    ),
                )
            db = node.db.lower() if node.db else ""
            name = node.name.lower() if node.name else ""
            mapped = _TABLE_MAP.get((db, name)) or (
                _TABLE_MAP.get(("pg_catalog", name))
                if not db and name in _CATALOG_TABLE_NAMES
                else None
            )
            if mapped:
                new_tbl = exp.Table(this=exp.Identifier(this=mapped, quoted=False))
                if node.alias:
                    new_tbl.set("alias", node.args.get("alias"))
                else:
                    # Preserve original name as alias so unqualified column refs
                    # like `pg_opclass.oid` continue to resolve after rename.
                    new_tbl.set(
                        "alias", exp.TableAlias(this=exp.Identifier(this=name, quoted=False))
                    )
                return new_tbl
        if isinstance(node, exp.Anonymous):
            fn = node.name.lower()
            if fn == "array_length":
                args = node.args.get("expressions", [])
                return exp.Anonymous(this="len", expressions=[args[0]] if args else [exp.null()])
            if fn == "current_schemas":
                args = node.args.get("expressions", [])
                include_implicit = True
                if args and isinstance(args[0], exp.Boolean):
                    include_implicit = args[0].this
                elif args and isinstance(args[0], exp.false().__class__):
                    include_implicit = False
                base = exp.select(
                    exp.Anonymous(this="list", expressions=[exp.column("nspname")])
                ).from_("_pg_namespace")
                if not include_implicit:
                    base = base.where("nspname NOT IN ('pg_catalog', 'information_schema')")
                return exp.Subquery(this=base)
            if "pg_get_userbyid" in fn or "pg_get_role_name" in fn:
                return exp.Literal.string("provisa")
            if fn.startswith("pg_get_") or "pg_tablespace_location" in fn:
                return exp.null()
            if "pg_encoding_to_char" in fn:
                return exp.Literal.string("UTF8")
            if "format_type" in fn:
                args = node.args.get("expressions", [])
                typid_expr = args[0] if args else exp.null()
                subq = (
                    exp.select(exp.column("typname"))
                    .from_("_pg_type")
                    .where(exp.EQ(this=exp.column("oid"), expression=typid_expr))
                )
                return exp.Subquery(this=subq)
            if "obj_description" in fn or "shobj_description" in fn:
                args = node.args.get("expressions", [])
                oid_expr = args[0] if args else exp.null()
                subq = (
                    exp.select(exp.column("description"))
                    .from_("_pg_description")
                    .where(exp.EQ(this=exp.column("objoid"), expression=oid_expr))
                    .where(exp.EQ(this=exp.column("objsubid"), expression=exp.Literal.number(0)))
                )
                return exp.Subquery(this=subq)
            if "col_description" in fn:
                args = node.args.get("expressions", [])
                oid_expr = args[0] if args else exp.null()
                attnum_expr = args[1] if len(args) > 1 else exp.null()
                subq = (
                    exp.select(exp.column("description"))
                    .from_("_pg_description")
                    .where(exp.EQ(this=exp.column("objoid"), expression=oid_expr))
                    .where(exp.EQ(this=exp.column("objsubid"), expression=attnum_expr))
                )
                return exp.Subquery(this=subq)
            if any(
                p in fn
                for p in (
                    "pg_get_constraintdef",
                    "pg_get_expr",
                    "pg_get_indexdef",
                    "pg_get_partkeydef",
                    "pg_get_partition",
                    "pg_get_serial_sequence",
                    "pg_get_userbyid",
                    "pg_get_ruledef",
                    "pg_get_triggerdef",
                    "pg_get_viewdef",
                )
            ):
                return exp.null()
            if "pg_postmaster_start_time" in fn or "pg_conf_load_time" in fn:
                return exp.null()
            if "pg_is_other_temp_schema" in fn:
                return exp.false()
            if (
                "pg_function_is_visible" in fn
                or "pg_opclass_is_visible" in fn
                or "pg_type_is_visible" in fn
                or "pg_ts_config_is_visible" in fn
                or "pg_ts_dict_is_visible" in fn
                or "pg_ts_parser_is_visible" in fn
                or "pg_ts_template_is_visible" in fn
                or "pg_operator_is_visible" in fn
            ):
                return exp.true()
            if (
                "pg_relation_size" in fn
                or "pg_total_relation_size" in fn
                or "pg_indexes_size" in fn
                or "pg_stat_get" in fn
            ):
                return exp.Literal.number(0)
            if "pg_table_is_visible" in fn or "pg_has_role" in fn:
                return exp.true()
            if fn == "encode":
                # PG encode(bytea, format) → return NULL; our catalog columns are VARCHAR not bytea
                return exp.null()
            if fn in (
                "pg_indexam_has_property",
                "pg_am_has_property",
                "pg_index_has_property",
                "pg_index_column_has_property",
            ):
                return exp.false()
            if fn in ("current_user", "session_user"):
                return exp.Literal.string(role_id)
            if fn in ("current_database",):
                return exp.Literal.string("provisa")
            if fn == "version":
                return exp.Literal.string("PostgreSQL 14.0 on Provisa")
            if "set_config" in fn:
                return exp.null()
            if "current_setting" in fn:
                args = node.args.get("expressions", [])
                key = args[0].name.lower() if args and isinstance(args[0], exp.Literal) else ""
                return exp.Literal.string(_KNOWN_SETTINGS.get(key, ""))
        if type(node).__name__ == "CurrentUser":
            return exp.Literal.string(role_id)
        if type(node).__name__ == "CurrentDatabase":
            return exp.Literal.string("provisa")
        if type(node).__name__ == "CurrentSchema":
            return exp.Literal.string("public")
        if isinstance(node, exp.Dot):
            # Strip schema qualifier from schema-qualified expressions: pg_catalog.TRUE → TRUE
            left = node.this
            if isinstance(left, exp.Identifier) and left.name.lower() in _INTERCEPT_SCHEMAS:
                # Re-apply transform to inner node so schema-qualified function calls
                # like pg_catalog.pg_encoding_to_char(...) are fully handled
                return _transform(node.expression)
        if isinstance(node, exp.EQ):
            # Rewrite `val = ANY(arr)` → `list_contains(arr, val)` so DuckDB
            # does not expand ANY into an internal subquery, which it rejects
            # on the outer side of non-inner JOINs.
            lhs, rhs = node.this, node.expression
            if isinstance(rhs, exp.Any):
                arr = rhs.this
                return exp.Anonymous(
                    this="list_contains",
                    expressions=[arr.transform(_transform), lhs.transform(_transform)],
                )
            if isinstance(lhs, exp.Any):
                arr = lhs.this
                return exp.Anonymous(
                    this="list_contains",
                    expressions=[arr.transform(_transform), rhs.transform(_transform)],
                )
        if isinstance(node, exp.Cast):
            dtype = node.args.get("to")
            dtype_str = str(dtype).lower() if dtype else ""
            if dtype_str in (
                "regclass",
                "regtype",
                "regproc",
                "regprocedure",
                "regoper",
                "regoperator",
                "regconfig",
                "regdictionary",
                "regrole",
                "regnamespace",
            ):
                return node.this
            if dtype_str in ("oid", "xid", "tid", "cid"):
                return exp.Literal.number(0)
            if dtype_str == "name":
                return exp.cast(node.this, "VARCHAR")
        if isinstance(node, exp.Column):
            if node.name.lower() in ("xmin", "xmax", "cmin", "cmax", "ctid"):
                return exp.cast(exp.Literal.number(0), "INTEGER")
            if node.name.lower() in ("current_user", "session_user"):
                return exp.Literal.string(role_id)
            # Rewrite schema-qualified column refs: pg_catalog.pg_class.col → _pg_class.col
            db_node = node.args.get("db") or node.args.get("catalog")
            db = db_node.name.lower() if db_node and hasattr(db_node, "name") else ""
            tbl = node.args.get("table")
            tname = tbl.name.lower() if tbl and hasattr(tbl, "name") else ""
            if db in _INTERCEPT_SCHEMAS and tname:
                return exp.column(node.name, table=tname)
        return node

    try:
        rewritten = tree.transform(_transform)
        # Move INNER JOINs before LEFT/RIGHT/FULL JOINs so DuckDB does not reject
        # forward alias references (e.g. `LEFT JOIN dsc ON c.oid=...` before
        # `INNER JOIN pg_class c` — c is not yet in scope).
        _outer_sides = {"LEFT", "RIGHT", "FULL"}
        for _sel in rewritten.find_all(exp.Select):
            _joins = _sel.args.get("joins") or []
            if len(_joins) > 1:
                _inner = [
                    j for j in _joins if (j.args.get("side") or "").upper() not in _outer_sides
                ]
                _outer = [j for j in _joins if (j.args.get("side") or "").upper() in _outer_sides]
                if _inner and _outer:
                    _sel.set("joins", _inner + _outer)
        sql_out = rewritten.sql(dialect="duckdb")
        # In real PG, oid is a hidden system column excluded from *. In our DuckDB tables,
        # oid is an explicit regular column, so "x.oid, x.*" returns oid twice. Remove the
        # duplicate by adding EXCLUDE on the star expression.
        import re as _re

        sql_out = _re.sub(
            r"(\w+)\.oid\s*,\s*\1\.\*",
            lambda m: f"{m.group(1)}.oid, {m.group(1)}.* EXCLUDE (oid)",
            sql_out,
        )
        return sql_out
    except Exception:
        return sql


def _handle_show(sql: str):
    """Answer SHOW commands without DuckDB."""
    from provisa.executor.trino import QueryResult

    normalized = sql.strip().rstrip(";")
    if re.match(r"^\s*SHOW\s+TRANSACTION\s+ISOLATION\s+LEVEL\s*$", normalized, re.IGNORECASE):
        return QueryResult(rows=[("read committed",)], column_names=["transaction_isolation"])
    parts = normalized.split()
    if len(parts) < 2:
        return QueryResult(rows=[], column_names=[])
    setting = parts[1].lower()
    if setting == "all":
        rows = [(k, v) for k, v in _KNOWN_SETTINGS.items()]
        return QueryResult(rows=rows, column_names=["name", "setting"])
    value = _KNOWN_SETTINGS.get(setting, "")
    return QueryResult(rows=[(value,)], column_names=[setting])


def _handle_scalar(sql: str, role_id: str):
    from provisa.executor.trino import QueryResult

    s = sql.strip().lower()
    if "current_user" in s or "session_user" in s:
        return QueryResult(rows=[(role_id,)], column_names=["current_user"])
    if "current_database" in s:
        return QueryResult(rows=[("provisa",)], column_names=["current_database"])
    if "version()" in s:
        return QueryResult(rows=[("PostgreSQL 14.0 on Provisa",)], column_names=["version"])
    if "current_schema()" in s:
        return QueryResult(rows=[("public",)], column_names=["current_schema"])
    if "pg_backend_pid()" in s:
        return QueryResult(rows=[(0,)], column_names=["pg_backend_pid"])
    return None


def _handle_current_setting(sql: str):
    """Answer SELECT current_setting(...) [+ set_config(...)] without DuckDB."""
    from provisa.executor.trino import QueryResult

    lower = sql.lower()
    if "current_setting" not in lower:
        return None

    # Multi-expression startup query: SELECT current_setting('x') AS a, set_config(...) AS b
    # Detect alias names from the SQL so we return the right column names for asyncpg.
    if "set_config" in lower:
        m1 = re.search(
            r"current_setting\s*\(\s*['\"]([^'\"]+)['\"]\s*\)(?:\s+AS\s+(\w+))?",
            sql,
            re.IGNORECASE,
        )
        m2 = re.search(r"set_config\s*\([^)]+\)(?:\s+AS\s+(\w+))?", sql, re.IGNORECASE)
        col1 = (m1.group(2) or "current_setting") if m1 else "current_setting"
        col2 = (m2.group(1) or "set_config") if m2 else "set_config"
        key = m1.group(1).lower() if m1 else ""
        val1 = _KNOWN_SETTINGS.get(key, "")
        return QueryResult(rows=[(val1, None)], column_names=[col1, col2])

    m = re.search(r"current_setting\s*\(\s*['\"]([^'\"]+)['\"]\s*\)", sql, re.IGNORECASE)
    if not m:
        return None
    key = m.group(1).lower()
    value = _KNOWN_SETTINGS.get(key, "")
    return QueryResult(rows=[(value,)], column_names=["current_setting"])


def answer(sql: str, role_id: str, state):
    """Return a synthetic QueryResult for intercepted catalog/SET/SHOW queries."""
    from provisa.executor.trino import QueryResult

    stripped = sql.strip().rstrip(";")

    if _TXN_RE.match(stripped) or _SET_RE.match(stripped):
        return QueryResult(rows=[], column_names=[])

    if _SHOW_RE.match(stripped):
        return _handle_show(stripped)

    if _SCALAR_FN_RE.match(stripped):
        result = _handle_scalar(stripped, role_id)
        if result is not None:
            return result

    if "current_setting" in stripped.lower():
        result = _handle_current_setting(stripped)
        if result is not None:
            return result

    if "set_config" in stripped.lower() and "current_setting" not in stripped.lower():
        from provisa.executor.trino import QueryResult

        return QueryResult(rows=[("on",)], column_names=["set_config"], column_types=["VARCHAR"])

    # asyncpg type-introspection recursive CTE. During describe ($1 not yet bound)
    # return schema-only. During execute, return rows from _TYPEINFO for the requested OIDs
    # so asyncpg can cache types and stop introspecting.
    if "typeinfo_tree" in stripped.lower():
        oids = _parse_typeinfo_oids(stripped)
        if oids is None:
            # Describe phase: $1 still present — return schema, 0 rows
            return QueryResult(
                rows=[], column_names=_TYPEINFO_COLS, column_types=_TYPEINFO_COL_TYPES
            )
        return _handle_typeinfo_tree(oids)

    if "pg_get_keywords" in stripped.lower():
        # pg_get_keywords() is a SRF — rewriter turns it into scalar NULL, breaking FROM clause.
        # DBeaver uses it only for SQL autocomplete keyword exclusion; return empty string.
        return QueryResult(rows=[(None,)], column_names=["string_agg"], column_types=["VARCHAR"])

    rewritten = stripped
    db = None
    try:
        db = _build_catalog_db(role_id, state)
        # Substitute $N params before rewriting so SQLGlot can parse the SQL.
        # Queries with $N::type[] (e.g. asyncpg type introspection) would otherwise
        # fail to parse, preventing table-name rewrites.
        import re as _re

        # Strip $N params AND any trailing PG type cast (e.g. $1::oid[]) so SQLGlot
        # can parse the query without failing on array-type annotations.
        pre_subst = _re.sub(r"\$\d+(?:::[^\s,)]+)?", "NULL", stripped)
        rewritten = _rewrite_for_duckdb(pre_subst, role_id)
        cur = db.execute(rewritten)
        rows = [tuple(r) for r in cur.fetchall()]
        col_names = [desc[0] for desc in (cur.description or [])]
        col_types = [str(desc[1]) for desc in (cur.description or [])]
        return QueryResult(rows=rows, column_names=col_names, column_types=col_types)
    except Exception as exc:
        log.error(
            "[CATALOG] DuckDB error sql=%r rewritten=%r: %s", stripped[:200], rewritten[:200], exc
        )
        raise
    finally:
        if db is not None:
            db.close()
