# Copyright (c) 2026 Kenneth Stott
# Canary: 9579d636-4b4f-4d6b-a006-aad464e70171
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse's forms carried over from DuckDB (REQ-2167, REQ-2236): an overload ClickHouse had no
form of, written on ClickHouse in the form DuckDB answers it with.

:data:`PORTED` lists those overloads, each by PostgreSQL's name and argument types. A call of one
is taken out of the statement before anything else reads it, written for DuckDB alone, as
``SELECT <call>``, and that one expression, read back in DuckDB's SQL, is written in ClickHouse's.
What ClickHouse writes of every other part of the statement is unchanged: a placeholder holds the
call's place until the statement is written, and is then replaced by the carried form.

A call is carried over only where DuckDB's form is one expression and calls no function DuckDB
alone has (a ``provisa_`` function of provisa.federation.duckdb_pg_scalars); otherwise it is left
where it was, to be refused by name as before. The function support measure compares every
carried overload with PostgreSQL's answer: an overload it found wrong, or failing on the engine,
is not listed here, and stays refused by name."""

# Requirements: REQ-2167, REQ-2236

from __future__ import annotations

import functools
import itertools
import re
from dataclasses import dataclass, field

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import SqlglotError

_CLICKHOUSE = "clickhouse"

#: Each overload carried over: PostgreSQL's name of the function and the types of its arguments,
#: as pg_proc lists them. The comment is its family on the function support report.
PORTED: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("bit_and", ("bit",)),  # Aggregate and window
    ("bit_or", ("bit",)),  # Aggregate and window
    ("bit_xor", ("bit",)),  # Aggregate and window
    ("max", ("anyenum",)),  # Aggregate and window
    ("max", ("character",)),  # Aggregate and window
    ("max", ("tid",)),  # Aggregate and window
    ("min", ("anyenum",)),  # Aggregate and window
    ("min", ("character",)),  # Aggregate and window
    ("min", ("tid",)),  # Aggregate and window
    ("mode", ("anyelement",)),  # Aggregate and window
    ("ntile", ("integer",)),  # Aggregate and window
    ("string_agg", ("bytea", "bytea")),  # Aggregate and window
    ("array_agg", ("anyarray",)),  # Array
    ("array_agg", ("anynonarray",)),  # Array
    ("array_cat", ("anycompatiblearray", "anycompatiblearray")),  # Array
    ("array_position", ("anycompatiblearray", "anycompatible", "integer")),  # Array
    ("array_remove", ("anycompatiblearray", "anycompatible")),  # Array
    ("arraycontained", ("anyarray", "anyarray")),  # Array
    ("arraycontains", ("anyarray", "anyarray")),  # Array
    ("arrayoverlap", ("anyarray", "anyarray")),  # Array
    ("generate_subscripts", ("anyarray", "integer")),  # Array
    ("generate_subscripts", ("anyarray", "integer", "boolean")),  # Array
    ("age", ("timestamp with time zone",)),  # Date
    ("age", ("timestamp with time zone", "timestamp with time zone")),  # Date
    ("age", ("timestamp without time zone",)),  # Date
    ("age", ("timestamp without time zone", "timestamp without time zone")),  # Date
    ("clock_timestamp", ()),  # Date
    ("date_add", ("timestamp with time zone", "interval")),  # Date
    ("date_add", ("timestamp with time zone", "interval", "text")),  # Date
    ("date_part", ("text", "time with time zone")),  # Date
    ("date_part", ("text", "time without time zone")),  # Date
    ("date_trunc", ("text", "interval")),  # Date
    ("date_trunc", ("text", "timestamp with time zone", "text")),  # Date
    ("datetime_pl", ("date", "time without time zone")),  # Date
    ("datetimetz_pl", ("date", "time with time zone")),  # Date
    ("extract", ("text", "time with time zone")),  # Date
    ("extract", ("text", "time without time zone")),  # Date
    ("in_range", ("date", "date", "interval", "boolean", "boolean")),  # Date
    ("in_range", ("interval", "interval", "interval", "boolean", "boolean")),  # Date
    (
        "in_range",
        ("time with time zone", "time with time zone", "interval", "boolean", "boolean"),
    ),  # Date
    (
        "in_range",
        ("time without time zone", "time without time zone", "interval", "boolean", "boolean"),
    ),  # Date
    (
        "in_range",
        ("timestamp with time zone", "timestamp with time zone", "interval", "boolean", "boolean"),
    ),  # Date
    (
        "in_range",
        (
            "timestamp without time zone",
            "timestamp without time zone",
            "interval",
            "boolean",
            "boolean",
        ),
    ),  # Date
    ("integer_pl_date", ("integer", "date")),  # Date
    ("interval", ("interval", "integer")),  # Date
    ("interval", ("time without time zone",)),  # Date
    (
        "make_interval",
        ("integer", "integer", "integer", "integer", "integer", "integer", "double precision"),
    ),  # Date
    ("max", ("time with time zone",)),  # Date
    ("max", ("time without time zone",)),  # Date
    ("min", ("time with time zone",)),  # Date
    ("min", ("time without time zone",)),  # Date
    (
        "overlaps",
        (
            "time with time zone",
            "time with time zone",
            "time with time zone",
            "time with time zone",
        ),
    ),  # Date
    (
        "overlaps",
        ("time without time zone", "interval", "time without time zone", "interval"),
    ),  # Date
    (
        "overlaps",
        ("time without time zone", "interval", "time without time zone", "time without time zone"),
    ),  # Date
    (
        "overlaps",
        ("time without time zone", "time without time zone", "time without time zone", "interval"),
    ),  # Date
    (
        "overlaps",
        (
            "time without time zone",
            "time without time zone",
            "time without time zone",
            "time without time zone",
        ),
    ),  # Date
    (
        "overlaps",
        ("timestamp with time zone", "interval", "timestamp with time zone", "interval"),
    ),  # Date
    (
        "overlaps",
        (
            "timestamp with time zone",
            "interval",
            "timestamp with time zone",
            "timestamp with time zone",
        ),
    ),  # Date
    (
        "overlaps",
        (
            "timestamp with time zone",
            "timestamp with time zone",
            "timestamp with time zone",
            "interval",
        ),
    ),  # Date
    (
        "overlaps",
        ("timestamp without time zone", "interval", "timestamp without time zone", "interval"),
    ),  # Date
    (
        "overlaps",
        (
            "timestamp without time zone",
            "interval",
            "timestamp without time zone",
            "timestamp without time zone",
        ),
    ),  # Date
    (
        "overlaps",
        (
            "timestamp without time zone",
            "timestamp without time zone",
            "timestamp without time zone",
            "interval",
        ),
    ),  # Date
    ("percentile_cont", ("double precision", "interval")),  # Date
    ("percentile_cont", ("double precision[]", "interval")),  # Date
    ("time", ("interval",)),  # Date
    ("time", ("time with time zone",)),  # Date
    ("time", ("time without time zone", "integer")),  # Date
    ("time", ("timestamp with time zone",)),  # Date
    ("time", ("timestamp without time zone",)),  # Date
    ("timedate_pl", ("time without time zone", "date")),  # Date
    ("timestamp", ("date", "time without time zone")),  # Date
    ("timestamp", ("timestamp without time zone", "integer")),  # Date
    ("timestamptz", ("date",)),  # Date
    ("timestamptz", ("date", "time with time zone")),  # Date
    ("timestamptz", ("date", "time without time zone")),  # Date
    ("timestamptz", ("timestamp without time zone",)),  # Date
    ("timetz", ("time with time zone", "integer")),  # Date
    ("timetz", ("time without time zone",)),  # Date
    ("timetz", ("timestamp with time zone",)),  # Date
    ("timetzdate_pl", ("time with time zone", "date")),  # Date
    ("timezone", ("interval", "time with time zone")),  # Date
    ("timezone", ("interval", "timestamp with time zone")),  # Date
    ("timezone", ("interval", "timestamp without time zone")),  # Date
    ("timezone", ("text", "time with time zone")),  # Date
    ("timezone", ("text", "timestamp without time zone")),  # Date
    ("timezone", ("time with time zone",)),  # Date
    ("timezone", ("timestamp without time zone",)),  # Date
    ("enum_cmp", ("anyenum", "anyenum")),  # Enum
    ("enum_eq", ("anyenum", "anyenum")),  # Enum
    ("enum_ge", ("anyenum", "anyenum")),  # Enum
    ("enum_gt", ("anyenum", "anyenum")),  # Enum
    ("enum_le", ("anyenum", "anyenum")),  # Enum
    ("enum_lt", ("anyenum", "anyenum")),  # Enum
    ("enum_ne", ("anyenum", "anyenum")),  # Enum
    ("length", ("lseg",)),  # Geometric
    ("length", ("path",)),  # Geometric
    ("array_to_json", ("anyarray",)),  # JSON
    ("array_to_json", ("anyarray", "boolean")),  # JSON
    ("json_agg", ("anyelement",)),  # JSON
    ("json_agg_strict", ("anyelement",)),  # JSON
    ("json_array_elements", ("json",)),  # JSON
    ("json_array_elements_text", ("json",)),  # JSON
    ("json_build_array", ()),  # JSON
    ("json_build_array", ('"any"',)),  # JSON
    ("json_build_object", ()),  # JSON
    ("json_build_object", ('"any"',)),  # JSON
    ("json_each", ("json",)),  # JSON
    ("json_each_text", ("json",)),  # JSON
    ("json_extract_path", ("json", "text[]")),  # JSON
    ("json_extract_path_text", ("json", "text[]")),  # JSON
    ("json_object", ("text[]",)),  # JSON
    ("json_object", ("text[]", "text[]")),  # JSON
    ("json_object_agg", ('"any"', '"any"')),  # JSON
    ("json_object_agg_strict", ('"any"', '"any"')),  # JSON
    ("json_object_agg_unique", ('"any"', '"any"')),  # JSON
    ("json_object_agg_unique_strict", ('"any"', '"any"')),  # JSON
    ("json_object_keys", ("json",)),  # JSON
    ("json_strip_nulls", ("json",)),  # JSON
    ("json_to_record", ("json",)),  # JSON
    ("json_to_recordset", ("json",)),  # JSON
    ("jsonb_agg", ("anyelement",)),  # JSON
    ("jsonb_agg_strict", ("anyelement",)),  # JSON
    ("jsonb_array_elements", ("jsonb",)),  # JSON
    ("jsonb_array_elements_text", ("jsonb",)),  # JSON
    ("jsonb_build_array", ()),  # JSON
    ("jsonb_build_array", ('"any"',)),  # JSON
    ("jsonb_build_object", ()),  # JSON
    ("jsonb_build_object", ('"any"',)),  # JSON
    ("jsonb_cmp", ("jsonb", "jsonb")),  # JSON
    ("jsonb_concat", ("jsonb", "jsonb")),  # JSON
    ("jsonb_contained", ("jsonb", "jsonb")),  # JSON
    ("jsonb_contains", ("jsonb", "jsonb")),  # JSON
    ("jsonb_delete", ("jsonb", "text[]")),  # JSON
    ("jsonb_delete", ("jsonb", "integer")),  # JSON
    ("jsonb_delete", ("jsonb", "text")),  # JSON
    ("jsonb_delete_path", ("jsonb", "text[]")),  # JSON
    ("jsonb_each", ("jsonb",)),  # JSON
    ("jsonb_each_text", ("jsonb",)),  # JSON
    ("jsonb_eq", ("jsonb", "jsonb")),  # JSON
    ("jsonb_exists", ("jsonb", "text")),  # JSON
    ("jsonb_exists_all", ("jsonb", "text[]")),  # JSON
    ("jsonb_exists_any", ("jsonb", "text[]")),  # JSON
    ("jsonb_extract_path", ("jsonb", "text[]")),  # JSON
    ("jsonb_extract_path_text", ("jsonb", "text[]")),  # JSON
    ("jsonb_ge", ("jsonb", "jsonb")),  # JSON
    ("jsonb_gt", ("jsonb", "jsonb")),  # JSON
    ("jsonb_hash", ("jsonb",)),  # JSON
    ("jsonb_hash_extended", ("jsonb", "bigint")),  # JSON
    ("jsonb_insert", ("jsonb", "text[]", "jsonb", "boolean")),  # JSON
    ("jsonb_le", ("jsonb", "jsonb")),  # JSON
    ("jsonb_lt", ("jsonb", "jsonb")),  # JSON
    ("jsonb_ne", ("jsonb", "jsonb")),  # JSON
    ("jsonb_object", ("text[]",)),  # JSON
    ("jsonb_object", ("text[]", "text[]")),  # JSON
    ("jsonb_object_agg", ('"any"', '"any"')),  # JSON
    ("jsonb_object_agg_strict", ('"any"', '"any"')),  # JSON
    ("jsonb_object_agg_unique", ('"any"', '"any"')),  # JSON
    ("jsonb_object_agg_unique_strict", ('"any"', '"any"')),  # JSON
    ("jsonb_object_keys", ("jsonb",)),  # JSON
    ("jsonb_path_exists", ("jsonb", "jsonpath", "jsonb", "boolean")),  # JSON
    ("jsonb_path_exists_opr", ("jsonb", "jsonpath")),  # JSON
    ("jsonb_path_exists_tz", ("jsonb", "jsonpath", "jsonb", "boolean")),  # JSON
    ("jsonb_path_match", ("jsonb", "jsonpath", "jsonb", "boolean")),  # JSON
    ("jsonb_path_match_opr", ("jsonb", "jsonpath")),  # JSON
    ("jsonb_path_match_tz", ("jsonb", "jsonpath", "jsonb", "boolean")),  # JSON
    ("jsonb_path_query", ("jsonb", "jsonpath", "jsonb", "boolean")),  # JSON
    ("jsonb_path_query_array", ("jsonb", "jsonpath", "jsonb", "boolean")),  # JSON
    ("jsonb_path_query_array_tz", ("jsonb", "jsonpath", "jsonb", "boolean")),  # JSON
    ("jsonb_path_query_first", ("jsonb", "jsonpath", "jsonb", "boolean")),  # JSON
    ("jsonb_path_query_first_tz", ("jsonb", "jsonpath", "jsonb", "boolean")),  # JSON
    ("jsonb_path_query_tz", ("jsonb", "jsonpath", "jsonb", "boolean")),  # JSON
    ("jsonb_pretty", ("jsonb",)),  # JSON
    ("jsonb_set", ("jsonb", "text[]", "jsonb", "boolean")),  # JSON
    ("jsonb_set_lax", ("jsonb", "text[]", "jsonb", "boolean", "text")),  # JSON
    ("jsonb_strip_nulls", ("jsonb",)),  # JSON
    ("jsonb_to_record", ("jsonb",)),  # JSON
    ("jsonb_to_recordset", ("jsonb",)),  # JSON
    ("row_to_json", ("record",)),  # JSON
    ("row_to_json", ("record", "boolean")),  # JSON
    ("to_json", ("anyelement",)),  # JSON
    ("to_jsonb", ("anyelement",)),  # JSON
    ("abbrev", ("cidr",)),  # Network
    ("abbrev", ("inet",)),  # Network
    ("broadcast", ("inet",)),  # Network
    ("family", ("inet",)),  # Network
    ("host", ("inet",)),  # Network
    ("max", ("inet",)),  # Network
    ("min", ("inet",)),  # Network
    ("netmask", ("inet",)),  # Network
    ("network", ("inet",)),  # Network
    ("avg", ("numeric",)),  # Numeric
    ("cash_words", ("money",)),  # Numeric
    ("dexp", ("double precision",)),  # Numeric
    ("div", ("numeric", "numeric")),  # Numeric
    ("dlog1", ("double precision",)),  # Numeric
    ("exp", ("double precision",)),  # Numeric
    ("exp", ("numeric",)),  # Numeric
    ("float4", ("jsonb",)),  # Numeric
    ("float8", ("jsonb",)),  # Numeric
    ("gcd", ("numeric", "numeric")),  # Numeric
    ("generate_series", ("numeric", "numeric")),  # Numeric
    ("generate_series", ("numeric", "numeric", "numeric")),  # Numeric
    ("int2", ("jsonb",)),  # Numeric
    ("int2int4_sum", ("bigint[]",)),  # Numeric
    ("int4", ('"char"',)),  # Numeric
    ("int4", ("bit",)),  # Numeric
    ("int4", ("jsonb",)),  # Numeric
    ("int8", ("bit",)),  # Numeric
    ("int8", ("jsonb",)),  # Numeric
    ("int8", ("oid",)),  # Numeric
    ("lcm", ("numeric", "numeric")),  # Numeric
    ("ln", ("double precision",)),  # Numeric
    ("ln", ("numeric",)),  # Numeric
    ("log", ("numeric",)),  # Numeric
    ("log", ("numeric", "numeric")),  # Numeric
    ("log10", ("numeric",)),  # Numeric
    ("make_time", ("integer", "integer", "double precision")),  # Numeric
    (
        "make_timestamp",
        ("integer", "integer", "integer", "integer", "integer", "double precision"),
    ),  # Numeric
    (
        "make_timestamptz",
        ("integer", "integer", "integer", "integer", "integer", "double precision"),
    ),  # Numeric
    (
        "make_timestamptz",
        ("integer", "integer", "integer", "integer", "integer", "double precision", "text"),
    ),  # Numeric
    ("max", ("money",)),  # Numeric
    ("max", ("numeric",)),  # Numeric
    ("min", ("money",)),  # Numeric
    ("min", ("numeric",)),  # Numeric
    ("numeric", ("double precision",)),  # Numeric
    ("numeric", ("numeric", "integer")),  # Numeric
    ("numeric", ("real",)),  # Numeric
    ("pg_lsn", ("numeric",)),  # Numeric
    ("pg_size_pretty", ("numeric",)),  # Numeric
    ("pow", ("numeric", "numeric")),  # Numeric
    ("power", ("numeric", "numeric")),  # Numeric
    ("random", ("numeric", "numeric")),  # Numeric
    ("sqrt", ("numeric",)),  # Numeric
    ("stddev", ("numeric",)),  # Numeric
    ("stddev", ("real",)),  # Numeric
    ("stddev_pop", ("numeric",)),  # Numeric
    ("stddev_pop", ("real",)),  # Numeric
    ("stddev_samp", ("numeric",)),  # Numeric
    ("stddev_samp", ("real",)),  # Numeric
    ("tanh", ("double precision",)),  # Numeric
    ("to_timestamp", ("double precision",)),  # Numeric
    ("var_pop", ("numeric",)),  # Numeric
    ("var_pop", ("real",)),  # Numeric
    ("var_samp", ("numeric",)),  # Numeric
    ("var_samp", ("real",)),  # Numeric
    ("variance", ("numeric",)),  # Numeric
    ("variance", ("real",)),  # Numeric
    ("bit", ("bigint", "integer")),  # Other
    ("bit", ("integer", "integer")),  # Other
    ("char", ("integer",)),  # Other
    ("int8dec_any", ("bigint", '"any"')),  # Other
    ("int8inc_any", ("bigint", '"any"')),  # Other
    ("text", ("xml",)),  # Other
    ("uuid_extract_timestamp", ("uuid",)),  # Other
    ("uuid_extract_version", ("uuid",)),  # Other
    ("lower", ("anymultirange",)),  # Range
    ("lower", ("anyrange",)),  # Range
    ("unnest", ("anymultirange",)),  # Range
    ("upper", ("anymultirange",)),  # Range
    ("upper", ("anyrange",)),  # Range
    ("bit", ("bit", "integer", "boolean")),  # String
    ("bit_count", ("bit",)),  # String
    ("bit_length", ("bit",)),  # String
    ("bitcat", ("bit varying", "bit varying")),  # String
    ("bitshiftleft", ("bit", "integer")),  # String
    ("bitshiftright", ("bit", "integer")),  # String
    ("bpchar", ("character", "integer", "boolean")),  # String
    ("btrim", ("text", "text")),  # String
    ("char", ("text",)),  # String
    ("concat_ws", ("text", '"any"')),  # String
    ("convert", ("bytea", "name", "name")),  # String
    ("convert_from", ("bytea", "name")),  # String
    ("convert_to", ("text", "name")),  # String
    ("format", ("text",)),  # String
    ("format", ("text", '"any"')),  # String
    ("get_bit", ("bit", "integer")),  # String
    ("is_normalized", ("text", "text")),  # String
    ("length", ("bit",)),  # String
    ("length", ("bytea", "name")),  # String
    ("like_escape", ("text", "text")),  # String
    ("ltrim", ("text", "text")),  # String
    ("nameconcatoid", ("name", "oid")),  # String
    ("normalize", ("text", "text")),  # String
    ("octet_length", ("bit",)),  # String
    ("overlay", ("bit", "bit", "integer")),  # String
    ("overlay", ("bit", "bit", "integer", "integer")),  # String
    ("overlay", ("text", "text", "integer")),  # String
    ("overlay", ("text", "text", "integer", "integer")),  # String
    ("parse_ident", ("text", "boolean")),  # String
    ("position", ("bit", "bit")),  # String
    ("regexp_count", ("text", "text", "integer")),  # String
    ("regexp_count", ("text", "text", "integer", "text")),  # String
    ("regexp_instr", ("text", "text")),  # String
    ("regexp_instr", ("text", "text", "integer")),  # String
    ("regexp_instr", ("text", "text", "integer", "integer")),  # String
    ("regexp_instr", ("text", "text", "integer", "integer", "integer")),  # String
    ("regexp_instr", ("text", "text", "integer", "integer", "integer", "text")),  # String
    (
        "regexp_instr",
        ("text", "text", "integer", "integer", "integer", "text", "integer"),
    ),  # String
    ("regexp_like", ("text", "text", "text")),  # String
    ("regexp_match", ("text", "text")),  # String
    ("regexp_match", ("text", "text", "text")),  # String
    ("regexp_split_to_array", ("text", "text")),  # String
    ("regexp_split_to_array", ("text", "text", "text")),  # String
    ("regexp_split_to_table", ("text", "text")),  # String
    ("regexp_split_to_table", ("text", "text", "text")),  # String
    ("regexp_substr", ("text", "text")),  # String
    ("regexp_substr", ("text", "text", "integer")),  # String
    ("regexp_substr", ("text", "text", "integer", "integer")),  # String
    ("regexp_substr", ("text", "text", "integer", "integer", "text")),  # String
    ("regexp_substr", ("text", "text", "integer", "integer", "text", "integer")),  # String
    ("replace", ("text", "text", "text")),  # String
    ("rtrim", ("text", "text")),  # String
    ("set_bit", ("bit", "integer", "integer")),  # String
    ("similar_escape", ("text", "text")),  # String
    ("similar_to_escape", ("text",)),  # String
    ("similar_to_escape", ("text", "text")),  # String
    ("string_to_array", ("text", "text")),  # String
    ("string_to_array", ("text", "text", "text")),  # String
    ("string_to_table", ("text", "text")),  # String
    ("string_to_table", ("text", "text", "text")),  # String
    ("substring", ("text", "text", "text")),  # String
    ("to_ascii", ("text", "integer")),  # String
    ("to_ascii", ("text", "name")),  # String
    ("to_date", ("text", "text")),  # String
    ("to_timestamp", ("text", "text")),  # String
    ("translate", ("text", "text", "text")),  # String
    ("unicode_assigned", ("text",)),  # String
    ("unistr", ("text",)),  # String
    ("varbit", ("bit varying", "integer", "boolean")),  # String
    ("varchar", ("character varying", "integer", "boolean")),  # String
    ("varchar", ("name",)),  # String
    ("xpath", ("text", "xml")),  # String
    ("xpath", ("text", "xml", "text[]")),  # String
    ("xpath_exists", ("text", "xml")),  # String
    ("xpath_exists", ("text", "xml", "text[]")),  # String
    ("current_schemas", ("boolean",)),  # System
    ("pg_conf_load_time", ()),  # System
    ("pg_encoding_max_length", ("integer",)),  # System
    ("pg_indexam_has_property", ("oid", "text")),  # System
    ("pg_my_temp_schema", ()),  # System
    ("pg_options_to_table", ("text[]",)),  # System
    ("pg_postmaster_start_time", ()),  # System
    ("pg_size_bytes", ("text",)),  # System
    ("pg_size_pretty", ("bigint",)),  # System
    ("pg_split_walfile_name", ("text",)),  # System
    ("pg_timezone_abbrevs", ()),  # System
    ("pg_timezone_names", ()),  # System
    ("pg_typeof", ('"any"',)),  # System
    ("pg_wal_lsn_diff", ("pg_lsn", "pg_lsn")),  # System
    ("pg_walfile_name", ("pg_lsn",)),  # System
    ("pg_walfile_name_offset", ("pg_lsn",)),  # System
    ("pg_xact_commit_timestamp", ("xid",)),  # System
    ("length", ("tsvector",)),  # Text search
    ("strip", ("tsvector",)),  # Text search
    ("unnest", ("tsvector",)),  # Text search
    ("anyenum_out", ("anyenum",)),  # Type operators
    ("bitand", ("bit", "bit")),  # Type operators
    ("bitcmp", ("bit", "bit")),  # Type operators
    ("biteq", ("bit", "bit")),  # Type operators
    ("bitge", ("bit", "bit")),  # Type operators
    ("bitgt", ("bit", "bit")),  # Type operators
    ("bitle", ("bit", "bit")),  # Type operators
    ("bitlt", ("bit", "bit")),  # Type operators
    ("bitne", ("bit", "bit")),  # Type operators
    ("bitnot", ("bit",)),  # Type operators
    ("bitor", ("bit", "bit")),  # Type operators
    ("bitxor", ("bit", "bit")),  # Type operators
    ("bpchar_larger", ("character", "character")),  # Type operators
    ("bpchar_smaller", ("character", "character")),  # Type operators
    ("bpcharcmp", ("character", "character")),  # Type operators
    ("bpcharge", ("character", "character")),  # Type operators
    ("bpchargt", ("character", "character")),  # Type operators
    ("bpcharle", ("character", "character")),  # Type operators
    ("bpcharlt", ("character", "character")),  # Type operators
    ("cash_out", ("money",)),  # Type operators
    ("cash_send", ("money",)),  # Type operators
    (
        "date_bin",
        ("interval", "timestamp with time zone", "timestamp with time zone"),
    ),  # Type operators
    (
        "date_bin",
        ("interval", "timestamp without time zone", "timestamp without time zone"),
    ),  # Type operators
    ("date_subtract", ("timestamp with time zone", "interval")),  # Type operators
    ("date_subtract", ("timestamp with time zone", "interval", "text")),  # Type operators
    ("enum_out", ("anyenum",)),  # Type operators
    ("enum_send", ("anyenum",)),  # Type operators
    ("int2vectorsend", ("int2vector",)),  # Type operators
    ("interval_in", ("cstring", "oid", "integer")),  # Type operators
    ("interval_pl_time", ("interval", "time without time zone")),  # Type operators
    ("interval_pl_timestamptz", ("interval", "timestamp with time zone")),  # Type operators
    ("interval_pl_timetz", ("interval", "time with time zone")),  # Type operators
    ("jsonb_in", ("cstring",)),  # Type operators
    ("numeric_pl_pg_lsn", ("numeric", "pg_lsn")),  # Type operators
    ("oidvectoreq", ("oidvector", "oidvector")),  # Type operators
    ("oidvectorge", ("oidvector", "oidvector")),  # Type operators
    ("oidvectorgt", ("oidvector", "oidvector")),  # Type operators
    ("oidvectorle", ("oidvector", "oidvector")),  # Type operators
    ("oidvectorlt", ("oidvector", "oidvector")),  # Type operators
    ("oidvectorne", ("oidvector", "oidvector")),  # Type operators
    ("oidvectorsend", ("oidvector",)),  # Type operators
    ("oidvectortypes", ("oidvector",)),  # Type operators
    ("pg_lsn_mii", ("pg_lsn", "numeric")),  # Type operators
    ("pg_lsn_pli", ("pg_lsn", "numeric")),  # Type operators
    ("record_eq", ("record", "record")),  # Type operators
    ("record_ge", ("record", "record")),  # Type operators
    ("record_gt", ("record", "record")),  # Type operators
    ("record_image_eq", ("record", "record")),  # Type operators
    ("record_image_ge", ("record", "record")),  # Type operators
    ("record_image_gt", ("record", "record")),  # Type operators
    ("record_image_le", ("record", "record")),  # Type operators
    ("record_image_lt", ("record", "record")),  # Type operators
    ("record_image_ne", ("record", "record")),  # Type operators
    ("record_le", ("record", "record")),  # Type operators
    ("record_lt", ("record", "record")),  # Type operators
    ("record_ne", ("record", "record")),  # Type operators
    ("text_ge", ("text", "text")),  # Type operators
    ("text_gt", ("text", "text")),  # Type operators
    ("text_larger", ("text", "text")),  # Type operators
    ("text_le", ("text", "text")),  # Type operators
    ("text_lt", ("text", "text")),  # Type operators
    ("text_smaller", ("text", "text")),  # Type operators
    ("tideq", ("tid", "tid")),  # Type operators
    ("tidge", ("tid", "tid")),  # Type operators
    ("tidgt", ("tid", "tid")),  # Type operators
    ("tidlarger", ("tid", "tid")),  # Type operators
    ("tidle", ("tid", "tid")),  # Type operators
    ("tidlt", ("tid", "tid")),  # Type operators
    ("tidne", ("tid", "tid")),  # Type operators
    ("tidsmaller", ("tid", "tid")),  # Type operators
    ("time_mi_interval", ("time without time zone", "interval")),  # Type operators
    ("time_mi_time", ("time without time zone", "time without time zone")),  # Type operators
    ("time_pl_interval", ("time without time zone", "interval")),  # Type operators
    (
        "timestamp_mi",
        ("timestamp without time zone", "timestamp without time zone"),
    ),  # Type operators
    ("timestamptz_mi", ("timestamp with time zone", "timestamp with time zone")),  # Type operators
    ("timestamptz_mi_interval", ("timestamp with time zone", "interval")),  # Type operators
    ("timestamptz_pl_interval", ("timestamp with time zone", "interval")),  # Type operators
    ("timetz_cmp", ("time with time zone", "time with time zone")),  # Type operators
    ("timetz_eq", ("time with time zone", "time with time zone")),  # Type operators
    ("timetz_ge", ("time with time zone", "time with time zone")),  # Type operators
    ("timetz_gt", ("time with time zone", "time with time zone")),  # Type operators
    ("timetz_larger", ("time with time zone", "time with time zone")),  # Type operators
    ("timetz_le", ("time with time zone", "time with time zone")),  # Type operators
    ("timetz_lt", ("time with time zone", "time with time zone")),  # Type operators
    ("timetz_mi_interval", ("time with time zone", "interval")),  # Type operators
    ("timetz_ne", ("time with time zone", "time with time zone")),  # Type operators
    ("timetz_pl_interval", ("time with time zone", "interval")),  # Type operators
    ("timetz_smaller", ("time with time zone", "time with time zone")),  # Type operators
    ("varbitcmp", ("bit varying", "bit varying")),  # Type operators
    ("varbiteq", ("bit varying", "bit varying")),  # Type operators
    ("varbitge", ("bit varying", "bit varying")),  # Type operators
    ("varbitgt", ("bit varying", "bit varying")),  # Type operators
    ("varbitle", ("bit varying", "bit varying")),  # Type operators
    ("varbitlt", ("bit varying", "bit varying")),  # Type operators
    ("varbitne", ("bit varying", "bit varying")),  # Type operators
    ("xml_send", ("xml",)),  # Type operators
    ("xml", ("text",)),  # XML
    ("xml_is_well_formed", ("text",)),  # XML
    ("xml_is_well_formed_content", ("text",)),  # XML
    ("xml_is_well_formed_document", ("text",)),  # XML
    ("xmlagg", ("xml",)),  # XML
    ("xmlcomment", ("text",)),  # XML
    ("xmlconcat2", ("xml", "xml")),  # XML
    ("xmlexists", ("text", "xml")),  # XML
)

#: A function and argument count whose every pg_catalog overload is listed: an argument whose type
#: the statement does not say matches any of them.
ALONE: frozenset[tuple[str, int]] = frozenset(
    {
        ("abbrev", 1),
        ("age", 2),
        ("anyenum_out", 1),
        ("array_agg", 1),
        ("array_cat", 2),
        ("array_position", 3),
        ("array_remove", 2),
        ("array_to_json", 1),
        ("array_to_json", 2),
        ("arraycontained", 2),
        ("arraycontains", 2),
        ("arrayoverlap", 2),
        ("bit", 2),
        ("bit", 3),
        ("bitand", 2),
        ("bitcat", 2),
        ("bitcmp", 2),
        ("biteq", 2),
        ("bitge", 2),
        ("bitgt", 2),
        ("bitle", 2),
        ("bitlt", 2),
        ("bitne", 2),
        ("bitnot", 1),
        ("bitor", 2),
        ("bitshiftleft", 2),
        ("bitshiftright", 2),
        ("bitxor", 2),
        ("bpchar", 3),
        ("bpchar_larger", 2),
        ("bpchar_smaller", 2),
        ("bpcharcmp", 2),
        ("bpcharge", 2),
        ("bpchargt", 2),
        ("bpcharle", 2),
        ("bpcharlt", 2),
        ("broadcast", 1),
        ("cash_out", 1),
        ("cash_send", 1),
        ("cash_words", 1),
        ("char", 1),
        ("clock_timestamp", 0),
        ("concat_ws", 2),
        ("convert", 3),
        ("convert_from", 2),
        ("convert_to", 2),
        ("current_schemas", 1),
        ("date_add", 2),
        ("date_add", 3),
        ("date_bin", 3),
        ("date_subtract", 2),
        ("date_subtract", 3),
        ("date_trunc", 3),
        ("datetime_pl", 2),
        ("datetimetz_pl", 2),
        ("dexp", 1),
        ("div", 2),
        ("dlog1", 1),
        ("enum_cmp", 2),
        ("enum_eq", 2),
        ("enum_ge", 2),
        ("enum_gt", 2),
        ("enum_le", 2),
        ("enum_lt", 2),
        ("enum_ne", 2),
        ("enum_out", 1),
        ("enum_send", 1),
        ("exp", 1),
        ("family", 1),
        ("format", 1),
        ("format", 2),
        ("generate_subscripts", 2),
        ("generate_subscripts", 3),
        ("host", 1),
        ("int2int4_sum", 1),
        ("int2vectorsend", 1),
        ("int8dec_any", 2),
        ("int8inc_any", 2),
        ("integer_pl_date", 2),
        ("interval", 1),
        ("interval", 2),
        ("interval_in", 3),
        ("interval_pl_time", 2),
        ("interval_pl_timestamptz", 2),
        ("interval_pl_timetz", 2),
        ("is_normalized", 2),
        ("json_agg", 1),
        ("json_agg_strict", 1),
        ("json_array_elements", 1),
        ("json_array_elements_text", 1),
        ("json_build_array", 0),
        ("json_build_array", 1),
        ("json_build_object", 0),
        ("json_build_object", 1),
        ("json_each", 1),
        ("json_each_text", 1),
        ("json_extract_path", 2),
        ("json_extract_path_text", 2),
        ("json_object", 1),
        ("json_object", 2),
        ("json_object_agg", 2),
        ("json_object_agg_strict", 2),
        ("json_object_agg_unique", 2),
        ("json_object_agg_unique_strict", 2),
        ("json_object_keys", 1),
        ("json_strip_nulls", 1),
        ("json_to_record", 1),
        ("json_to_recordset", 1),
        ("jsonb_agg", 1),
        ("jsonb_agg_strict", 1),
        ("jsonb_array_elements", 1),
        ("jsonb_array_elements_text", 1),
        ("jsonb_build_array", 0),
        ("jsonb_build_array", 1),
        ("jsonb_build_object", 0),
        ("jsonb_build_object", 1),
        ("jsonb_cmp", 2),
        ("jsonb_concat", 2),
        ("jsonb_contained", 2),
        ("jsonb_contains", 2),
        ("jsonb_delete", 2),
        ("jsonb_delete_path", 2),
        ("jsonb_each", 1),
        ("jsonb_each_text", 1),
        ("jsonb_eq", 2),
        ("jsonb_exists", 2),
        ("jsonb_exists_all", 2),
        ("jsonb_exists_any", 2),
        ("jsonb_extract_path", 2),
        ("jsonb_extract_path_text", 2),
        ("jsonb_ge", 2),
        ("jsonb_gt", 2),
        ("jsonb_hash", 1),
        ("jsonb_hash_extended", 2),
        ("jsonb_in", 1),
        ("jsonb_insert", 4),
        ("jsonb_le", 2),
        ("jsonb_lt", 2),
        ("jsonb_ne", 2),
        ("jsonb_object", 1),
        ("jsonb_object", 2),
        ("jsonb_object_agg", 2),
        ("jsonb_object_agg_strict", 2),
        ("jsonb_object_agg_unique", 2),
        ("jsonb_object_agg_unique_strict", 2),
        ("jsonb_object_keys", 1),
        ("jsonb_path_exists", 4),
        ("jsonb_path_exists_opr", 2),
        ("jsonb_path_exists_tz", 4),
        ("jsonb_path_match", 4),
        ("jsonb_path_match_opr", 2),
        ("jsonb_path_match_tz", 4),
        ("jsonb_path_query", 4),
        ("jsonb_path_query_array", 4),
        ("jsonb_path_query_array_tz", 4),
        ("jsonb_path_query_first", 4),
        ("jsonb_path_query_first_tz", 4),
        ("jsonb_path_query_tz", 4),
        ("jsonb_pretty", 1),
        ("jsonb_set", 4),
        ("jsonb_set_lax", 5),
        ("jsonb_strip_nulls", 1),
        ("jsonb_to_record", 1),
        ("jsonb_to_recordset", 1),
        ("length", 2),
        ("ln", 1),
        ("log", 2),
        ("make_interval", 7),
        ("make_time", 3),
        ("make_timestamp", 6),
        ("make_timestamptz", 6),
        ("make_timestamptz", 7),
        ("mode", 1),
        ("nameconcatoid", 2),
        ("netmask", 1),
        ("network", 1),
        ("normalize", 2),
        ("ntile", 1),
        ("numeric", 2),
        ("numeric_pl_pg_lsn", 2),
        ("oidvectoreq", 2),
        ("oidvectorge", 2),
        ("oidvectorgt", 2),
        ("oidvectorle", 2),
        ("oidvectorlt", 2),
        ("oidvectorne", 2),
        ("oidvectorsend", 1),
        ("oidvectortypes", 1),
        ("parse_ident", 2),
        ("pg_conf_load_time", 0),
        ("pg_encoding_max_length", 1),
        ("pg_indexam_has_property", 2),
        ("pg_lsn", 1),
        ("pg_lsn_mii", 2),
        ("pg_lsn_pli", 2),
        ("pg_my_temp_schema", 0),
        ("pg_options_to_table", 1),
        ("pg_postmaster_start_time", 0),
        ("pg_size_bytes", 1),
        ("pg_size_pretty", 1),
        ("pg_split_walfile_name", 1),
        ("pg_timezone_abbrevs", 0),
        ("pg_timezone_names", 0),
        ("pg_typeof", 1),
        ("pg_wal_lsn_diff", 2),
        ("pg_walfile_name", 1),
        ("pg_walfile_name_offset", 1),
        ("pg_xact_commit_timestamp", 1),
        ("record_eq", 2),
        ("record_ge", 2),
        ("record_gt", 2),
        ("record_image_eq", 2),
        ("record_image_ge", 2),
        ("record_image_gt", 2),
        ("record_image_le", 2),
        ("record_image_lt", 2),
        ("record_image_ne", 2),
        ("record_le", 2),
        ("record_lt", 2),
        ("record_ne", 2),
        ("regexp_count", 3),
        ("regexp_count", 4),
        ("regexp_instr", 2),
        ("regexp_instr", 3),
        ("regexp_instr", 4),
        ("regexp_instr", 5),
        ("regexp_instr", 6),
        ("regexp_instr", 7),
        ("regexp_like", 3),
        ("regexp_match", 2),
        ("regexp_match", 3),
        ("regexp_split_to_array", 2),
        ("regexp_split_to_array", 3),
        ("regexp_split_to_table", 2),
        ("regexp_split_to_table", 3),
        ("regexp_substr", 2),
        ("regexp_substr", 3),
        ("regexp_substr", 4),
        ("regexp_substr", 5),
        ("regexp_substr", 6),
        ("replace", 3),
        ("row_to_json", 1),
        ("row_to_json", 2),
        ("similar_escape", 2),
        ("similar_to_escape", 1),
        ("similar_to_escape", 2),
        ("string_to_array", 2),
        ("string_to_array", 3),
        ("string_to_table", 2),
        ("string_to_table", 3),
        ("strip", 1),
        ("tanh", 1),
        ("text_ge", 2),
        ("text_gt", 2),
        ("text_larger", 2),
        ("text_le", 2),
        ("text_lt", 2),
        ("text_smaller", 2),
        ("tideq", 2),
        ("tidge", 2),
        ("tidgt", 2),
        ("tidlarger", 2),
        ("tidle", 2),
        ("tidlt", 2),
        ("tidne", 2),
        ("tidsmaller", 2),
        ("time", 1),
        ("time", 2),
        ("time_mi_interval", 2),
        ("time_mi_time", 2),
        ("time_pl_interval", 2),
        ("timedate_pl", 2),
        ("timestamp", 2),
        ("timestamp_mi", 2),
        ("timestamptz", 1),
        ("timestamptz_mi", 2),
        ("timestamptz_mi_interval", 2),
        ("timestamptz_pl_interval", 2),
        ("timetz", 1),
        ("timetz", 2),
        ("timetz_cmp", 2),
        ("timetz_eq", 2),
        ("timetz_ge", 2),
        ("timetz_gt", 2),
        ("timetz_larger", 2),
        ("timetz_le", 2),
        ("timetz_lt", 2),
        ("timetz_mi_interval", 2),
        ("timetz_ne", 2),
        ("timetz_pl_interval", 2),
        ("timetz_smaller", 2),
        ("timetzdate_pl", 2),
        ("to_ascii", 2),
        ("to_date", 2),
        ("to_json", 1),
        ("to_jsonb", 1),
        ("to_timestamp", 1),
        ("to_timestamp", 2),
        ("translate", 3),
        ("unicode_assigned", 1),
        ("unistr", 1),
        ("uuid_extract_timestamp", 1),
        ("uuid_extract_version", 1),
        ("varbit", 3),
        ("varbitcmp", 2),
        ("varbiteq", 2),
        ("varbitge", 2),
        ("varbitgt", 2),
        ("varbitle", 2),
        ("varbitlt", 2),
        ("varbitne", 2),
        ("varchar", 1),
        ("varchar", 3),
        ("xml", 1),
        ("xml_is_well_formed", 1),
        ("xml_is_well_formed_content", 1),
        ("xml_is_well_formed_document", 1),
        ("xml_send", 1),
        ("xmlagg", 1),
        ("xmlcomment", 1),
        ("xmlconcat2", 2),
        ("xmlexists", 2),
        ("xpath", 2),
        ("xpath", 3),
        ("xpath_exists", 2),
        ("xpath_exists", 3),
    }
)

#: Argument types any said type fits.
_POLYMORPHIC = re.compile(r"^(any\w*|\"any\"|record|internal)$")
#: The placeholder a carried call's place is held by, until the statement is written.
_PLACEHOLDER = "provisa_ported_"
_PLACED = re.compile(rf"\b{_PLACEHOLDER}(\d+)\(\)", re.IGNORECASE)


@functools.cache
def _said(pg_type: str) -> str | None:
    """A pg_proc argument type as said_type reads a statement's cast to it; None for a type any
    argument fits."""
    from provisa.transpiler.function_macros import said_type

    if _POLYMORPHIC.match(pg_type):
        return None
    tree = sqlglot.parse_one(f"SELECT NULL::{pg_type}", read="postgres")
    assert isinstance(tree, exp.Select)
    return said_type(tree.selects[0])


@functools.cache
def _overloads() -> dict[tuple[str, int], tuple[tuple[str | None, ...], ...]]:
    table: dict[tuple[str, int], list[tuple[str | None, ...]]] = {}
    for name, types in PORTED:
        table.setdefault((name, len(types)), []).append(tuple(_said(t) for t in types))
    return {key: tuple(rows) for key, rows in table.items()}


@functools.cache
def _mentioned() -> re.Pattern[str]:
    names = sorted({name for name, _ in PORTED}, key=len, reverse=True)
    return re.compile(r"\b(?:" + "|".join(map(re.escape, names)) + r")\b", re.IGNORECASE)


#: The carried overloads whose last argument is VARIADIC, by name: their fixed arguments' types and
#: the type of each further argument (an array of them, where the call says VARIADIC).
VARIADIC: dict[str, tuple[tuple[str, ...], str]] = {
    "concat_ws": (("text",), '"any"'),
    "format": (("text",), '"any"'),
    "json_build_array": ((), '"any"'),
    "json_build_object": ((), '"any"'),
    "jsonb_build_array": ((), '"any"'),
    "jsonb_build_object": ((), '"any"'),
    "json_extract_path": (("json",), "text"),
    "json_extract_path_text": (("json",), "text"),
    "jsonb_extract_path": (("jsonb",), "text"),
    "jsonb_extract_path_text": (("jsonb",), "text"),
    "jsonb_delete": (("jsonb",), "text"),
}


def _name_and_args(node: exp.Expr) -> tuple[str, list[exp.Expr]] | None:
    from provisa.transpiler import engine_refusals, function_macros

    if isinstance(node, exp.WithinGroup):
        inner = _name_and_args(node.this)
        order = node.expression
        ordered = [o.this for o in order.expressions] if isinstance(order, exp.Order) else []
        return None if inner is None else (inner[0], [*inner[1], *ordered])
    if isinstance(node, exp.Anonymous):
        name = node.name.lower()
        return engine_refusals._NAMED.get(name, name), list(node.expressions)
    called = function_macros.call_of(node)
    if called is not None:
        return called
    if isinstance(node, exp.Func):
        found = engine_refusals._called(node)
        if found is not None:
            name, args = found
            return engine_refusals._NAMED.get(name, name), args
    return None


def _fits(want: str | None, got: str | None) -> bool:
    return want is None or got is None or want == got.split("(")[0]


def _variadic(name: str, args: list[exp.Expr]) -> bool:
    from provisa.transpiler.function_macros import said_type

    fixed, each = VARIADIC[name]
    if len(args) <= len(fixed):
        return False
    said = [said_type(a) for a in args]
    if not all(_fits(_said(t), s) for t, s in zip(fixed, said)):
        return False
    rest = args[len(fixed) :]
    if len(rest) == 1 and isinstance(rest[0], exp.Variadic):
        return True  # VARIADIC ARRAY[..]: its elements are the further arguments
    return all(_fits(_said(each), said_type(a)) for a in rest)


def is_ported(node: exp.Expr) -> bool:
    """Whether ``node`` calls an overload of :data:`PORTED`: its name, its argument count, and each
    argument's said type that overload's (an unsaid one fits where the name and count are
    :data:`ALONE`)."""
    from provisa.transpiler.function_macros import said_type

    found = _name_and_args(node)
    if found is None:
        return False
    name, args = found
    if name in VARIADIC and _variadic(name, args):
        return True
    rows = _overloads().get((name, len(args)))
    if rows is None:
        return False
    said = [said_type(a) for a in args]
    if any(s is None for s in said) and (name, len(args)) not in ALONE:
        return False
    return any(all(_fits(want, got) for want, got in zip(row, said)) for row in rows)


def duckdb_form(call: exp.Expr) -> exp.Expr | None:
    """DuckDB's form of ``call`` as one expression, or None where DuckDB's is not one expression
    ClickHouse can be given."""
    from provisa.transpiler.function_macros import FunctionRefused
    from provisa.transpiler.transpile import transpile

    try:
        written = transpile(f"SELECT {call.sql(dialect='postgres')} AS v", "duckdb")
        tree = sqlglot.parse_one(written, read="duckdb")
    except (FunctionRefused, SqlglotError, ValueError):
        return None
    if not isinstance(tree, exp.Select) or len(tree.selects) != 1:
        return None
    if any(tree.args.get(key) for key in ("from_", "from", "with_", "with", "where", "joins")):
        return None
    form = tree.selects[0].unalias()
    if any(f.name.lower().startswith("provisa_") for f in form.find_all(exp.Anonymous)):
        return None
    return form


def clickhouse_text(form: exp.Expr) -> str:
    """``form`` written in ClickHouse's SQL, with what ClickHouse would read otherwise said for it
    (transpile.clickhouse_semantics)."""
    from provisa.transpiler.transpile import clickhouse_semantics

    tree = clickhouse_semantics(exp.select(form.copy()))
    assert isinstance(tree, exp.Select)
    said = tree.selects[0]
    if not isinstance(said, (exp.Func, exp.Column, exp.Literal, exp.Paren)):
        said = exp.Paren(this=said)
    return said.sql(dialect=_CLICKHOUSE)


@dataclass
class HeldOut:
    """The carried calls of one statement, by placeholder number."""

    forms: dict[int, str] = field(default_factory=dict)

    def restored(self, written: str) -> str:
        """``written`` with each placeholder replaced by its call's ClickHouse form."""
        if not self.forms:
            return written
        return _PLACED.sub(lambda m: self.forms[int(m.group(1))], written)


def held_out(pg_sql: str, dialect: str) -> tuple[str, HeldOut]:
    """``pg_sql`` with each outermost call of a carried overload replaced by a placeholder, and
    the forms that replace them once the statement is written. Unchanged on any dialect but
    ClickHouse, and where no carried overload is called."""
    held = HeldOut()
    if dialect != _CLICKHOUSE or not _mentioned().search(pg_sql):
        return pg_sql, held
    try:
        tree = sqlglot.parse_one(pg_sql, read="postgres")
    except SqlglotError:
        return pg_sql, held
    numbers = itertools.count()

    def carried(node: exp.Expr) -> exp.Expr:
        target = node
        if (
            isinstance(node, exp.Dot)
            and isinstance(node.this, exp.Identifier)
            and node.this.name.lower() == "pg_catalog"
        ):
            target = node.expression
        if not is_ported(target):
            return node
        form = duckdb_form(target)
        if form is None:
            return node
        number = next(numbers)
        held.forms[number] = clickhouse_text(form)
        placeholder = exp.Anonymous(this=f"{_PLACEHOLDER}{number}", expressions=[])
        if isinstance(node.parent, exp.Select) and node.arg_key == "expressions":
            return exp.alias_(placeholder, target.name or "v")
        return placeholder

    rewritten = tree.transform(carried)
    if not held.forms:
        return pg_sql, held
    return rewritten.sql(dialect="postgres"), held
