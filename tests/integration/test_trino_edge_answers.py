# Copyright (c) 2026 Kenneth Stott
# Canary: 1d7a5e39-b2c4-4f86-9a03-e6c8f5b2d714
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Trino's wrong answers at edge values (the function support measure, run 44670380355): each
statement as Provisa writes it for Trino, run there, against postgres:17.11's answer or error. A
call answers as PostgreSQL answers it, raises PostgreSQL's error where it raised, or is refused by
name where Trino holds no exact answer (REQ-2236, REQ-2171): never another value."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import datetime
import math
from decimal import Decimal
from typing import Any

import pytest

from provisa.core.pg_error import PostgresError, from_engine
from provisa.transpiler.function_macros import FunctionRefused, RowRefused
from provisa.transpiler.transpile import transpile

pytestmark = [pytest.mark.integration, pytest.mark.requires_trino]

#: (statement, kind, PostgreSQL's): kind is "value" (its text, or None), "rows" (each row's text)
#: or "err" (its SQLSTATE).
EDGES: list[tuple[str, str, Any]] = [
    (
        "SELECT pg_catalog.\"abs\"('123456789012345678901234567890.123456789012345678901234567890'::numeric) AS v",
        "value",
        "123456789012345678901234567890.123456789012345678901234567890",
    ),
    ("SELECT pg_catalog.\"acos\"('-Infinity'::double precision) AS v", "err", "22003"),
    (
        "SELECT pg_catalog.\"acosh\"('1.7976931348623157e+308'::double precision) AS v",
        "value",
        "710.4758600739439",
    ),
    (
        "SELECT pg_catalog.\"array_to_string\"('{1,2,3}'::integer[], 'abc'::text, NULL::text) AS v",
        "value",
        "1abc2abc3",
    ),
    ("SELECT pg_catalog.\"asin\"('-Infinity'::double precision) AS v", "err", "22003"),
    (
        "SELECT pg_catalog.\"asinh\"('1.7976931348623157e+308'::double precision) AS v",
        "value",
        "710.4758600739439",
    ),
    (
        "SELECT pg_catalog.\"avg\"(c1) AS v FROM (VALUES ('1.7976931348623157e+308'::double precision), ('1.7976931348623157e+308'::double precision)) AS t(c1)",
        "err",
        "22003",
    ),
    ('SELECT pg_catalog."bpchar"(\'\'::"char") AS v', "value", ""),
    ("SELECT pg_catalog.\"btequalimage\"('4294967296'::oid) AS v", "err", "22003"),
    ("SELECT pg_catalog.\"btoidcmp\"('4294967296'::oid, 12345::oid) AS v", "err", "22003"),
    ("SELECT pg_catalog.\"cardinality\"('{{1,2},{3,4}}'::integer[]) AS v", "value", "4"),
    (
        "SELECT pg_catalog.\"cash_mul_flt4\"('-92233720368547758.08'::money, 0.5::real) AS v",
        "value",
        "-$46,116,860,184,273,879.04",
    ),
    (
        "SELECT pg_catalog.\"cash_mul_flt8\"('-92233720368547758.08'::money, 0.5::double precision) AS v",
        "value",
        "-$46,116,860,184,273,879.04",
    ),
    (
        "SELECT pg_catalog.\"ceil\"('123456789012345678901234567890.123456789012345678901234567890'::numeric) AS v",
        "value",
        "123456789012345678901234567891",
    ),
    (
        "SELECT pg_catalog.\"ceiling\"('123456789012345678901234567890.123456789012345678901234567890'::numeric) AS v",
        "value",
        "123456789012345678901234567891",
    ),
    ("SELECT pg_catalog.\"char_length\"(''::character) AS v", "value", "0"),
    ("SELECT pg_catalog.\"character_length\"(''::character) AS v", "value", "0"),
    ("SELECT pg_catalog.\"chr\"('0'::integer) AS v", "err", "54000"),
    ("SELECT pg_catalog.\"cos\"('-Infinity'::double precision) AS v", "err", "22003"),
    ("SELECT pg_catalog.\"cot\"('-Infinity'::double precision) AS v", "err", "22003"),
    (
        "SELECT pg_catalog.\"date_cmp_timestamp\"('2024-06-15'::date, '294276-12-31 23:59:59.999999'::timestamp without time zone) AS v",
        "value",
        "-1",
    ),
    (
        "SELECT pg_catalog.\"date_ge_timestamp\"('2024-06-15'::date, '294276-12-31 23:59:59.999999'::timestamp without time zone) AS v",
        "value",
        "f",
    ),
    (
        "SELECT pg_catalog.\"date_gt_timestamp\"('2024-06-15'::date, '294276-12-31 23:59:59.999999'::timestamp without time zone) AS v",
        "value",
        "f",
    ),
    (
        "SELECT pg_catalog.\"date_le_timestamp\"('2024-06-15'::date, '294276-12-31 23:59:59.999999'::timestamp without time zone) AS v",
        "value",
        "t",
    ),
    (
        "SELECT pg_catalog.\"date_lt_timestamp\"('2024-06-15'::date, '294276-12-31 23:59:59.999999'::timestamp without time zone) AS v",
        "value",
        "t",
    ),
    ("SELECT pg_catalog.\"date_part\"('year'::text, '5874897-12-31'::date) AS v", "err", "22008"),
    (
        "SELECT pg_catalog.\"date_part\"('year'::text, '294276-12-31 23:59:59.999999'::timestamp without time zone) AS v",
        "value",
        "294276",
    ),
    (
        "SELECT pg_catalog.\"datetime_pl\"('2024-06-15'::date, '23:59:59.999999'::time without time zone) AS v",
        "value",
        "2024-06-15 23:59:59.999999",
    ),
    (
        "SELECT pg_catalog.\"degrees\"('1.7976931348623157e+308'::double precision) AS v",
        "err",
        "22003",
    ),
    ("SELECT pg_catalog.\"exp\"('1.7976931348623157e+308'::double precision) AS v", "err", "22003"),
    ("SELECT pg_catalog.\"extract\"('year'::text, '5874897-12-31'::date) AS v", "value", "5874897"),
    (
        "SELECT pg_catalog.\"extract\"('year'::text, '294276-12-31 23:59:59.999999'::timestamp without time zone) AS v",
        "value",
        "294276",
    ),
    ("SELECT pg_catalog.\"float48ge\"('NaN'::real, 0.5::double precision) AS v", "value", "t"),
    ("SELECT pg_catalog.\"float48gt\"('NaN'::real, 0.5::double precision) AS v", "value", "t"),
    ("SELECT pg_catalog.\"float48le\"(0.5::real, 'NaN'::double precision) AS v", "value", "t"),
    ("SELECT pg_catalog.\"float48lt\"(0.5::real, 'NaN'::double precision) AS v", "value", "t"),
    ("SELECT pg_catalog.\"float4ge\"('NaN'::real, 0.5::real) AS v", "value", "t"),
    ("SELECT pg_catalog.\"float4gt\"('NaN'::real, 0.5::real) AS v", "value", "t"),
    ("SELECT pg_catalog.\"float4larger\"('NaN'::real, 0.5::real) AS v", "value", "NaN"),
    ("SELECT pg_catalog.\"float4le\"(0.5::real, 'NaN'::real) AS v", "value", "t"),
    ("SELECT pg_catalog.\"float4lt\"(0.5::real, 'NaN'::real) AS v", "value", "t"),
    ("SELECT pg_catalog.\"float4smaller\"('NaN'::real, 0.5::real) AS v", "value", "0.5"),
    ("SELECT pg_catalog.\"float84ge\"('NaN'::double precision, 0.5::real) AS v", "value", "t"),
    ("SELECT pg_catalog.\"float84gt\"('NaN'::double precision, 0.5::real) AS v", "value", "t"),
    ("SELECT pg_catalog.\"float84le\"(0.5::double precision, 'NaN'::real) AS v", "value", "t"),
    ("SELECT pg_catalog.\"float84lt\"(0.5::double precision, 'NaN'::real) AS v", "value", "t"),
    (
        "SELECT pg_catalog.\"float8ge\"('NaN'::double precision, 0.5::double precision) AS v",
        "value",
        "t",
    ),
    (
        "SELECT pg_catalog.\"float8gt\"('NaN'::double precision, 0.5::double precision) AS v",
        "value",
        "t",
    ),
    (
        "SELECT pg_catalog.\"float8larger\"('NaN'::double precision, 0.5::double precision) AS v",
        "value",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"float8le\"(0.5::double precision, 'NaN'::double precision) AS v",
        "value",
        "t",
    ),
    (
        "SELECT pg_catalog.\"float8lt\"(0.5::double precision, 'NaN'::double precision) AS v",
        "value",
        "t",
    ),
    (
        "SELECT pg_catalog.\"float8smaller\"('NaN'::double precision, 0.5::double precision) AS v",
        "value",
        "0.5",
    ),
    (
        "SELECT pg_catalog.\"flt4_mul_cash\"(0.5::real, '-92233720368547758.08'::money) AS v",
        "value",
        "-$46,116,860,184,273,879.04",
    ),
    (
        "SELECT pg_catalog.\"flt8_mul_cash\"(0.5::double precision, '-92233720368547758.08'::money) AS v",
        "value",
        "-$46,116,860,184,273,879.04",
    ),
    ("SELECT pg_catalog.\"hashoid\"('4294967296'::oid) AS v", "err", "22003"),
    ("SELECT pg_catalog.\"hashoidextended\"('4294967296'::oid, 3::bigint) AS v", "err", "22003"),
    (
        "SELECT pg_catalog.\"in_range\"(0.5::double precision, 'NaN'::double precision, 0.5::double precision, true::boolean, true::boolean) AS v",
        "value",
        "t",
    ),
    (
        "SELECT pg_catalog.\"in_range\"(0.5::real, 'NaN'::real, 0.5::double precision, true::boolean, true::boolean) AS v",
        "value",
        "t",
    ),
    (
        "SELECT pg_catalog.\"int8_sum\"('-0.000000000000000000001'::numeric, 3::bigint) AS v",
        "value",
        "2.999999999999999999999",
    ),
    ("SELECT pg_catalog.\"isfinite\"('5874898-01-01'::date) AS v", "err", "22008"),
    (
        "SELECT pg_catalog.\"isfinite\"('294277-01-01 00:00:00'::timestamp without time zone) AS v",
        "err",
        "22008",
    ),
    ("SELECT pg_catalog.\"length\"(''::character) AS v", "value", "0"),
    ("SELECT pg_catalog.\"ln\"('-Infinity'::double precision) AS v", "err", "2201E"),
    ("SELECT pg_catalog.\"log\"('-Infinity'::double precision) AS v", "err", "2201E"),
    ("SELECT pg_catalog.\"log10\"('-Infinity'::double precision) AS v", "err", "2201E"),
    (
        "SELECT pg_catalog.\"max\"(c1) AS v FROM (VALUES ('4294967296'::oid), ('4294967296'::oid)) AS t(c1)",
        "err",
        "22003",
    ),
    (
        "SELECT pg_catalog.\"min\"(c1) AS v FROM (VALUES ('4294967296'::oid), ('4294967296'::oid)) AS t(c1)",
        "err",
        "22003",
    ),
    (
        "SELECT pg_catalog.\"min_scale\"('123456789012345678901234567890.123456789012345678901234567890'::numeric) AS v",
        "value",
        "29",
    ),
    (
        "SELECT pg_catalog.\"mod\"('123456789012345678901234567890.123456789012345678901234567890'::numeric, 2.5::numeric) AS v",
        "value",
        "0.123456789012345678901234567890",
    ),
    ("SELECT pg_catalog.\"name\"(''::character) AS v", "value", ""),
    (
        "SELECT pg_catalog.\"numeric_abs\"('123456789012345678901234567890.123456789012345678901234567890'::numeric) AS v",
        "value",
        "123456789012345678901234567890.123456789012345678901234567890",
    ),
    (
        "SELECT pg_catalog.\"numeric_add\"('-0.000000000000000000001'::numeric, 2.5::numeric) AS v",
        "value",
        "2.499999999999999999999",
    ),
    (
        "SELECT pg_catalog.\"numeric_inc\"('123456789012345678901234567890.123456789012345678901234567890'::numeric) AS v",
        "value",
        "123456789012345678901234567891.123456789012345678901234567890",
    ),
    (
        "SELECT pg_catalog.\"numeric_larger\"('123456789012345678901234567890.123456789012345678901234567890'::numeric, 2.5::numeric) AS v",
        "value",
        "123456789012345678901234567890.123456789012345678901234567890",
    ),
    (
        "SELECT pg_catalog.\"numeric_mod\"('-0.000000000000000000001'::numeric, 2.5::numeric) AS v",
        "value",
        "-0.000000000000000000001",
    ),
    (
        "SELECT pg_catalog.\"numeric_mul\"('-0.000000000000000000001'::numeric, 2.5::numeric) AS v",
        "value",
        "-0.0000000000000000000025",
    ),
    (
        "SELECT pg_catalog.\"numeric_sub\"('-0.000000000000000000001'::numeric, 2.5::numeric) AS v",
        "value",
        "-2.500000000000000000001",
    ),
    (
        "SELECT pg_catalog.\"numeric_uminus\"('123456789012345678901234567890.123456789012345678901234567890'::numeric) AS v",
        "value",
        "-123456789012345678901234567890.123456789012345678901234567890",
    ),
    (
        "SELECT pg_catalog.\"numeric_uplus\"('123456789012345678901234567890.123456789012345678901234567890'::numeric) AS v",
        "value",
        "123456789012345678901234567890.123456789012345678901234567890",
    ),
    ("SELECT pg_catalog.\"oideq\"('4294967296'::oid, 12345::oid) AS v", "err", "22003"),
    ("SELECT pg_catalog.\"oidge\"('4294967296'::oid, 12345::oid) AS v", "err", "22003"),
    ("SELECT pg_catalog.\"oidgt\"('4294967296'::oid, 12345::oid) AS v", "err", "22003"),
    ("SELECT pg_catalog.\"oidlarger\"('4294967296'::oid, 12345::oid) AS v", "err", "22003"),
    ("SELECT pg_catalog.\"oidle\"('4294967296'::oid, 12345::oid) AS v", "err", "22003"),
    ("SELECT pg_catalog.\"oidlt\"('4294967296'::oid, 12345::oid) AS v", "err", "22003"),
    ("SELECT pg_catalog.\"oidne\"('4294967296'::oid, 12345::oid) AS v", "err", "22003"),
    ("SELECT pg_catalog.\"oidsmaller\"('4294967296'::oid, 12345::oid) AS v", "err", "22003"),
    ("SELECT pg_catalog.\"pg_column_compression\"('2147483648'::integer) AS v", "err", "22003"),
    ("SELECT pg_catalog.\"pg_column_toast_chunk_id\"('2147483648'::integer) AS v", "err", "22003"),
    ("SELECT pg_catalog.\"position\"(''::text, 'abc'::text) AS v", "value", "0"),
    (
        'SELECT pg_catalog."postgresql_fdw_validator"(\'{"",日本語😀,é}\'::text[], 12345::oid) AS v',
        "err",
        "42601",
    ),
    (
        "SELECT pg_catalog.\"pow\"('-Infinity'::double precision, 0.5::double precision) AS v",
        "err",
        "2201F",
    ),
    (
        "SELECT pg_catalog.\"power\"('-Infinity'::double precision, 0.5::double precision) AS v",
        "err",
        "2201F",
    ),
    (
        "SELECT pg_catalog.\"regr_intercept\"(c1, c2) AS v FROM (VALUES (0.5::double precision, 'NaN'::double precision), (0.5::double precision, 'NaN'::double precision)) AS t(c1, c2)",
        "value",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"regr_slope\"(c1, c2) AS v FROM (VALUES (0.5::double precision, 'NaN'::double precision), (0.5::double precision, 'NaN'::double precision)) AS t(c1, c2)",
        "value",
        "NaN",
    ),
    ("SELECT pg_catalog.\"replace\"('abc'::text, ''::text, 'abc'::text) AS v", "value", "abc"),
    (
        "SELECT pg_catalog.\"scale\"('123456789012345678901234567890.123456789012345678901234567890'::numeric) AS v",
        "value",
        "30",
    ),
    ("SELECT pg_catalog.\"sign\"('NaN'::double precision) AS v", "value", "0"),
    ("SELECT pg_catalog.\"sin\"('-Infinity'::double precision) AS v", "err", "22003"),
    ("SELECT pg_catalog.\"sqrt\"('-Infinity'::double precision) AS v", "err", "2201F"),
    (
        "SELECT pg_catalog.\"stddev\"(c1) AS v FROM (VALUES ('1.7976931348623157e+308'::double precision), ('1.7976931348623157e+308'::double precision)) AS t(c1)",
        "err",
        "22003",
    ),
    (
        "SELECT pg_catalog.\"stddev_pop\"(c1) AS v FROM (VALUES ('1.7976931348623157e+308'::double precision), ('1.7976931348623157e+308'::double precision)) AS t(c1)",
        "err",
        "22003",
    ),
    (
        "SELECT pg_catalog.\"stddev_samp\"(c1) AS v FROM (VALUES ('1.7976931348623157e+308'::double precision), ('1.7976931348623157e+308'::double precision)) AS t(c1)",
        "err",
        "22003",
    ),
    (
        "SELECT pg_catalog.\"substr\"('abc'::text, 3::integer, '-2147483648'::integer) AS v",
        "err",
        "22011",
    ),
    (
        "SELECT pg_catalog.\"sum\"(c1) AS v FROM (VALUES ('1.7976931348623157e+308'::double precision), ('1.7976931348623157e+308'::double precision)) AS t(c1)",
        "err",
        "22003",
    ),
    (
        "SELECT pg_catalog.\"sum\"(c1) AS v FROM (VALUES ('123456789012345678901234567890.123456789012345678901234567890'::numeric), ('123456789012345678901234567890.123456789012345678901234567890'::numeric)) AS t(c1)",
        "value",
        "246913578024691357802469135780.246913578024691357802469135780",
    ),
    (
        "SELECT pg_catalog.\"sum\"(c1) AS v FROM (VALUES ('3.4028235e+38'::real), ('3.4028235e+38'::real)) AS t(c1)",
        "err",
        "22003",
    ),
    ("SELECT pg_catalog.\"tan\"('-Infinity'::double precision) AS v", "err", "22003"),
    ('SELECT pg_catalog."text"(\'\'::"char") AS v', "value", ""),
    ("SELECT pg_catalog.\"text\"(''::character) AS v", "value", ""),
    (
        "SELECT pg_catalog.\"time\"('294276-12-31 23:59:59.999999'::timestamp without time zone) AS v",
        "value",
        "23:59:59.999999",
    ),
    (
        "SELECT pg_catalog.\"time_cmp\"('23:59:59.999999'::time without time zone, '12:30:45'::time without time zone) AS v",
        "value",
        "1",
    ),
    (
        "SELECT pg_catalog.\"time_ge\"('23:59:59.999999'::time without time zone, '12:30:45'::time without time zone) AS v",
        "value",
        "t",
    ),
    (
        "SELECT pg_catalog.\"time_gt\"('23:59:59.999999'::time without time zone, '12:30:45'::time without time zone) AS v",
        "value",
        "t",
    ),
    (
        "SELECT pg_catalog.\"time_larger\"('23:59:59.999999'::time without time zone, '12:30:45'::time without time zone) AS v",
        "value",
        "23:59:59.999999",
    ),
    (
        "SELECT pg_catalog.\"time_le\"('23:59:59.999999'::time without time zone, '12:30:45'::time without time zone) AS v",
        "value",
        "f",
    ),
    (
        "SELECT pg_catalog.\"time_lt\"('23:59:59.999999'::time without time zone, '12:30:45'::time without time zone) AS v",
        "value",
        "f",
    ),
    (
        "SELECT pg_catalog.\"time_smaller\"('23:59:59.999999'::time without time zone, '12:30:45'::time without time zone) AS v",
        "value",
        "12:30:45",
    ),
    (
        "SELECT pg_catalog.\"timedate_pl\"('23:59:59.999999'::time without time zone, '2024-06-15'::date) AS v",
        "value",
        "2024-06-15 23:59:59.999999",
    ),
    (
        "SELECT pg_catalog.\"timestamp\"('2024-06-15'::date, '23:59:59.999999'::time without time zone) AS v",
        "value",
        "2024-06-15 23:59:59.999999",
    ),
    (
        "SELECT pg_catalog.\"timestamp_cmp\"('294276-12-31 23:59:59.999999'::timestamp without time zone, '2024-06-15 12:30:45'::timestamp without time zone) AS v",
        "value",
        "1",
    ),
    (
        "SELECT pg_catalog.\"timestamp_cmp_date\"('294276-12-31 23:59:59.999999'::timestamp without time zone, '2024-06-15'::date) AS v",
        "value",
        "1",
    ),
    (
        "SELECT pg_catalog.\"timestamp_cmp_timestamptz\"('294276-12-31 23:59:59.999999'::timestamp without time zone, '2024-06-15 12:30:45+00'::timestamp with time zone) AS v",
        "value",
        "1",
    ),
    (
        "SELECT pg_catalog.\"timestamp_ge\"('294276-12-31 23:59:59.999999'::timestamp without time zone, '2024-06-15 12:30:45'::timestamp without time zone) AS v",
        "value",
        "t",
    ),
    (
        "SELECT pg_catalog.\"timestamp_ge_date\"('294276-12-31 23:59:59.999999'::timestamp without time zone, '2024-06-15'::date) AS v",
        "value",
        "t",
    ),
    (
        "SELECT pg_catalog.\"timestamp_ge_timestamptz\"('294276-12-31 23:59:59.999999'::timestamp without time zone, '2024-06-15 12:30:45+00'::timestamp with time zone) AS v",
        "value",
        "t",
    ),
    (
        "SELECT pg_catalog.\"timestamp_gt\"('294276-12-31 23:59:59.999999'::timestamp without time zone, '2024-06-15 12:30:45'::timestamp without time zone) AS v",
        "value",
        "t",
    ),
    (
        "SELECT pg_catalog.\"timestamp_gt_date\"('294276-12-31 23:59:59.999999'::timestamp without time zone, '2024-06-15'::date) AS v",
        "value",
        "t",
    ),
    (
        "SELECT pg_catalog.\"timestamp_gt_timestamptz\"('294276-12-31 23:59:59.999999'::timestamp without time zone, '2024-06-15 12:30:45+00'::timestamp with time zone) AS v",
        "value",
        "t",
    ),
    (
        "SELECT pg_catalog.\"timestamp_larger\"('294276-12-31 23:59:59.999999'::timestamp without time zone, '2024-06-15 12:30:45'::timestamp without time zone) AS v",
        "value",
        "294276-12-31 23:59:59.999999",
    ),
    (
        "SELECT pg_catalog.\"timestamp_le\"('294276-12-31 23:59:59.999999'::timestamp without time zone, '2024-06-15 12:30:45'::timestamp without time zone) AS v",
        "value",
        "f",
    ),
    (
        "SELECT pg_catalog.\"timestamp_le_date\"('294276-12-31 23:59:59.999999'::timestamp without time zone, '2024-06-15'::date) AS v",
        "value",
        "f",
    ),
    (
        "SELECT pg_catalog.\"timestamp_le_timestamptz\"('294276-12-31 23:59:59.999999'::timestamp without time zone, '2024-06-15 12:30:45+00'::timestamp with time zone) AS v",
        "value",
        "f",
    ),
    (
        "SELECT pg_catalog.\"timestamp_lt\"('294276-12-31 23:59:59.999999'::timestamp without time zone, '2024-06-15 12:30:45'::timestamp without time zone) AS v",
        "value",
        "f",
    ),
    (
        "SELECT pg_catalog.\"timestamp_lt_date\"('294276-12-31 23:59:59.999999'::timestamp without time zone, '2024-06-15'::date) AS v",
        "value",
        "f",
    ),
    (
        "SELECT pg_catalog.\"timestamp_lt_timestamptz\"('294276-12-31 23:59:59.999999'::timestamp without time zone, '2024-06-15 12:30:45+00'::timestamp with time zone) AS v",
        "value",
        "f",
    ),
    (
        "SELECT pg_catalog.\"timestamptz_cmp_timestamp\"('2024-06-15 12:30:45+00'::timestamp with time zone, '294276-12-31 23:59:59.999999'::timestamp without time zone) AS v",
        "value",
        "-1",
    ),
    (
        "SELECT pg_catalog.\"timestamptz_ge_timestamp\"('2024-06-15 12:30:45+00'::timestamp with time zone, '294276-12-31 23:59:59.999999'::timestamp without time zone) AS v",
        "value",
        "f",
    ),
    (
        "SELECT pg_catalog.\"timestamptz_gt_timestamp\"('2024-06-15 12:30:45+00'::timestamp with time zone, '294276-12-31 23:59:59.999999'::timestamp without time zone) AS v",
        "value",
        "f",
    ),
    (
        "SELECT pg_catalog.\"timestamptz_le_timestamp\"('2024-06-15 12:30:45+00'::timestamp with time zone, '294276-12-31 23:59:59.999999'::timestamp without time zone) AS v",
        "value",
        "t",
    ),
    (
        "SELECT pg_catalog.\"timestamptz_lt_timestamp\"('2024-06-15 12:30:45+00'::timestamp with time zone, '294276-12-31 23:59:59.999999'::timestamp without time zone) AS v",
        "value",
        "t",
    ),
    (
        "SELECT pg_catalog.\"to_char\"('294276-12-31 23:59:59.999999'::timestamp without time zone, 'YYYY-MM-DD'::text) AS v",
        "value",
        "294276-12-31",
    ),
    (
        "SELECT pg_catalog.\"trim_scale\"('123456789012345678901234567890.123456789012345678901234567890'::numeric) AS v",
        "value",
        "123456789012345678901234567890.12345678901234567890123456789",
    ),
    ("SELECT pg_catalog.\"unnest\"('{{1,2},{3,4}}'::integer[]) AS v", "rows", ["1", "2", "3", "4"]),
    (
        "SELECT pg_catalog.\"var_pop\"(c1) AS v FROM (VALUES ('1.7976931348623157e+308'::double precision), ('1.7976931348623157e+308'::double precision)) AS t(c1)",
        "err",
        "22003",
    ),
    (
        "SELECT pg_catalog.\"var_samp\"(c1) AS v FROM (VALUES ('1.7976931348623157e+308'::double precision), ('1.7976931348623157e+308'::double precision)) AS t(c1)",
        "err",
        "22003",
    ),
    (
        "SELECT pg_catalog.\"variance\"(c1) AS v FROM (VALUES ('1.7976931348623157e+308'::double precision), ('1.7976931348623157e+308'::double precision)) AS t(c1)",
        "err",
        "22003",
    ),
]


def _text(value: Any) -> str | None:
    """A Trino value in PostgreSQL's text form."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return str(int(value)) if value.is_integer() else repr(value)
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime.datetime):
        return value.isoformat(" ")
    if isinstance(value, datetime.time):
        return value.isoformat()
    return str(value)


def _money(text: str) -> Decimal:
    """PostgreSQL's money text as its value: Trino holds money as its numeric base."""
    return Decimal(text.replace("$", "").replace(",", ""))


def _agrees(got: str | None, want: str | None) -> bool:
    if want is not None and want.lstrip("-").startswith("$"):
        return got is not None and Decimal(got) == _money(want)
    return got == want


def _refused_by_name(error: Exception) -> bool:
    return isinstance(error, RowRefused) and error.sqlstate == "0A000"


@pytest.mark.parametrize(("sql", "kind", "want"), EDGES, ids=[e[0][18:70] for e in EDGES])
def test_trino_answers_an_edge_as_postgresql_does(trino_conn, sql, kind, want):
    try:
        written = transpile(sql, "trino")
    except FunctionRefused as refused:
        state = getattr(refused, "sqlstate", None)
        assert (kind == "err" and state == want) or _refused_by_name(refused), (
            f"{sql}: {state} {refused}"
        )
        return
    except PostgresError as error:
        assert kind == "err" and error.sqlstate == want, f"{sql}: {error.sqlstate} {error}"
        return
    cursor = trino_conn.cursor()
    try:
        cursor.execute(written)
        rows = cursor.fetchall()
    except Exception as error:  # noqa: BLE001 -- the engine's error, read as PostgreSQL's
        postgres = from_engine(error)
        assert postgres is not None, f"{written}: {str(error)[:300]}"
        assert kind == "err" and postgres.sqlstate == want, (
            f"{written}: {postgres.sqlstate} {postgres}, PostgreSQL {kind} {want}"
        )
        return
    assert kind != "err", f"{written} answered {rows}, PostgreSQL raised {want}"
    got = [_text(row[-1]) for row in rows]
    if kind == "rows":
        assert got == want, f"{written}: {got}"
    else:
        assert len(got) == 1 and _agrees(got[0], want), f"{written}: {got}, PostgreSQL {want!r}"
