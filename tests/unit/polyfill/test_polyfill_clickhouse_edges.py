# Copyright (c) 2026 Kenneth Stott
# Canary: 86c0f00b-6056-4479-8345-9237b5e2a583
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse's answers at the function support measure's edge values (REQ-2236; REQ-2171): each
answered on the embedded engine (chdb), against PostgreSQL's answer. On the base ClickHouse
answered each of these wrong:

- sind, cosd, tand and cotd of a double past 2^53 (ClickHouse's float % is inexact there), and a
  minus zero's sign;
- a double literal's text, which ClickHouse's cast reads a unit off its nearest double;
- percentile_cont and percentile_disc of an array of fractions over only NULL keys (NULL, not an
  array of NULLs);
- btree's byte-order comparisons (memcmp's and strncmp's difference, not -1 or 1);
- cardinality, unnest and array_to_string of an array of more than one dimension;
- hashbpchar of a character(n) literal past ASCII (padded by bytes);
- circle's text of a radius of a three-digit exponent;
- interval_out of an interval past its fields (22008);
- daterange of an infinite bound (made exclusive), numrange of a numeric of an exponent;
- numeric_mod of digits past 18 places.

The statements are the measure's own calls of each overload (function_support.calls and
edge_calls) and the edges the rows read, answered by the embedded PostgreSQL 16.11."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import math

import pytest

from provisa.core.pg_error import PostgresError, from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import RowRefused
from provisa.transpiler.transpile import transpile

ANSWERED = [
    ('SELECT pg_catalog."sind"(NULL::double precision) AS v', None),
    ('SELECT pg_catalog."sind"(0.5::double precision) AS v', "0.008726535498373936"),
    ('SELECT pg_catalog."sind"((-2.25)::double precision) AS v', "-0.03925981575906862"),
    ('SELECT pg_catalog."sind"(100::double precision) AS v', "0.984807753012208"),
    ("SELECT pg_catalog.\"sind\"('NaN'::double precision) AS v", "NaN"),
    ("SELECT pg_catalog.\"sind\"('-0'::double precision) AS v", "-0"),
    (
        "SELECT pg_catalog.\"sind\"('1.7976931348623157e+308'::double precision) AS v",
        "0.7880107536067218",
    ),
    ('SELECT pg_catalog."cosd"(NULL::double precision) AS v', None),
    ('SELECT pg_catalog."cosd"(0.5::double precision) AS v', "0.9999619230641713"),
    ('SELECT pg_catalog."cosd"((-2.25)::double precision) AS v', "0.9992290362407229"),
    ('SELECT pg_catalog."cosd"(100::double precision) AS v', "-0.17364817766693036"),
    ("SELECT pg_catalog.\"cosd\"('NaN'::double precision) AS v", "NaN"),
    ("SELECT pg_catalog.\"cosd\"('-0'::double precision) AS v", "1"),
    (
        "SELECT pg_catalog.\"cosd\"('1.7976931348623157e+308'::double precision) AS v",
        "-0.6156614753256582",
    ),
    ('SELECT pg_catalog."tand"(NULL::double precision) AS v', None),
    ('SELECT pg_catalog."tand"(0.5::double precision) AS v', "0.008726867790758791"),
    ('SELECT pg_catalog."tand"((-2.25)::double precision) AS v', "-0.03929010700766965"),
    ('SELECT pg_catalog."tand"(100::double precision) AS v', "-5.671281819617709"),
    ("SELECT pg_catalog.\"tand\"('NaN'::double precision) AS v", "NaN"),
    ("SELECT pg_catalog.\"tand\"('-0'::double precision) AS v", "0"),
    (
        "SELECT pg_catalog.\"tand\"('1.7976931348623157e+308'::double precision) AS v",
        "-1.2799416321930788",
    ),
    ('SELECT pg_catalog."cotd"(NULL::double precision) AS v', None),
    ('SELECT pg_catalog."cotd"(0.5::double precision) AS v', "114.58865012930958"),
    ('SELECT pg_catalog."cotd"((-2.25)::double precision) AS v', "-25.451699579357072"),
    ('SELECT pg_catalog."cotd"(100::double precision) AS v', "-0.176326980708465"),
    ("SELECT pg_catalog.\"cotd\"('NaN'::double precision) AS v", "NaN"),
    ("SELECT pg_catalog.\"cotd\"('-0'::double precision) AS v", "-Infinity"),
    (
        "SELECT pg_catalog.\"cotd\"('1.7976931348623157e+308'::double precision) AS v",
        "-0.7812856265067174",
    ),
    (
        'SELECT pg_catalog."percentile_cont"(NULL::double precision[]) WITHIN GROUP (ORDER BY NULL::double precision) AS v',
        None,
    ),
    (
        "SELECT pg_catalog.\"percentile_cont\"('{0.25,0.5}'::double precision[]) WITHIN GROUP (ORDER BY c1) AS v FROM (VALUES (0.5::double precision), ((-2.25)::double precision), (100::double precision)) AS t(c1)",
        "{-0.875,0.5}",
    ),
    (
        "SELECT pg_catalog.\"percentile_cont\"('{0.25,0.5}'::double precision[]) WITHIN GROUP (ORDER BY c1) AS v FROM (VALUES (NULL::double precision), (NULL::double precision)) AS t(c1)",
        None,
    ),
    (
        "SELECT pg_catalog.\"percentile_cont\"('{0.25,0.5}'::double precision[]) WITHIN GROUP (ORDER BY c1) AS v FROM (VALUES ('NaN'::double precision), ('NaN'::double precision)) AS t(c1)",
        "{NaN,NaN}",
    ),
    (
        "SELECT pg_catalog.\"percentile_cont\"('{0.25,0.5}'::double precision[]) WITHIN GROUP (ORDER BY c1) AS v FROM (VALUES ('-Infinity'::double precision), ('-Infinity'::double precision)) AS t(c1)",
        "{NaN,NaN}",
    ),
    (
        "SELECT pg_catalog.\"percentile_cont\"('{0.25,0.5}'::double precision[]) WITHIN GROUP (ORDER BY c1) AS v FROM (VALUES ('-0'::double precision), ('-0'::double precision)) AS t(c1)",
        "{0,0}",
    ),
    (
        "SELECT pg_catalog.\"percentile_cont\"('{0.25,0.5}'::double precision[]) WITHIN GROUP (ORDER BY c1) AS v FROM (VALUES ('1.7976931348623157e+308'::double precision), ('1.7976931348623157e+308'::double precision)) AS t(c1)",
        "{1.7976931348623157e+308,1.7976931348623157e+308}",
    ),
    (
        'SELECT pg_catalog."percentile_disc"(NULL::double precision[]) WITHIN GROUP (ORDER BY NULL::integer) AS v',
        None,
    ),
    (
        "SELECT pg_catalog.\"percentile_disc\"('{0.25,0.5}'::double precision[]) WITHIN GROUP (ORDER BY c1) AS v FROM (VALUES (3::integer), ((-7)::integer), (42::integer)) AS t(c1)",
        "{-7,3}",
    ),
    (
        "SELECT pg_catalog.\"percentile_disc\"('{0.25,0.5}'::double precision[]) WITHIN GROUP (ORDER BY c1) AS v FROM (VALUES (NULL::integer), (NULL::integer)) AS t(c1)",
        None,
    ),
    (
        "SELECT pg_catalog.\"percentile_disc\"('{0.25,0.5}'::double precision[]) WITHIN GROUP (ORDER BY c1) AS v FROM (VALUES ('-2147483648'::integer), ('-2147483648'::integer)) AS t(c1)",
        "{-2147483648,-2147483648}",
    ),
    (
        "SELECT pg_catalog.\"percentile_disc\"('{0.25,0.5}'::double precision[]) WITHIN GROUP (ORDER BY c1) AS v FROM (VALUES ('2147483647'::integer), ('2147483647'::integer)) AS t(c1)",
        "{2147483647,2147483647}",
    ),
    (
        "SELECT pg_catalog.\"percentile_disc\"('{0.25,0.5}'::double precision[]) WITHIN GROUP (ORDER BY c1) AS v FROM (VALUES ('0'::integer), ('0'::integer)) AS t(c1)",
        "{0,0}",
    ),
    (
        'SELECT pg_catalog."percentile_cont"(NULL::double precision) WITHIN GROUP (ORDER BY NULL::double precision) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."percentile_cont"(0.25::double precision) WITHIN GROUP (ORDER BY c1) AS v FROM (VALUES (0.5::double precision), ((-2.25)::double precision), (100::double precision)) AS t(c1)',
        "-0.875",
    ),
    (
        'SELECT pg_catalog."percentile_cont"(0.25::double precision) WITHIN GROUP (ORDER BY c1) AS v FROM (VALUES (NULL::double precision), (NULL::double precision)) AS t(c1)',
        None,
    ),
    (
        "SELECT pg_catalog.\"percentile_cont\"(0.25::double precision) WITHIN GROUP (ORDER BY c1) AS v FROM (VALUES ('NaN'::double precision), ('NaN'::double precision)) AS t(c1)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"percentile_cont\"(0.25::double precision) WITHIN GROUP (ORDER BY c1) AS v FROM (VALUES ('-Infinity'::double precision), ('-Infinity'::double precision)) AS t(c1)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"percentile_cont\"(0.25::double precision) WITHIN GROUP (ORDER BY c1) AS v FROM (VALUES ('-0'::double precision), ('-0'::double precision)) AS t(c1)",
        "0",
    ),
    (
        "SELECT pg_catalog.\"percentile_cont\"(0.25::double precision) WITHIN GROUP (ORDER BY c1) AS v FROM (VALUES ('1.7976931348623157e+308'::double precision), ('1.7976931348623157e+308'::double precision)) AS t(c1)",
        "1.7976931348623157e+308",
    ),
    (
        'SELECT pg_catalog."percentile_disc"(NULL::double precision) WITHIN GROUP (ORDER BY NULL::integer) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."percentile_disc"(0.25::double precision) WITHIN GROUP (ORDER BY c1) AS v FROM (VALUES (3::integer), ((-7)::integer), (42::integer)) AS t(c1)',
        "-7",
    ),
    (
        'SELECT pg_catalog."percentile_disc"(0.25::double precision) WITHIN GROUP (ORDER BY c1) AS v FROM (VALUES (NULL::integer), (NULL::integer)) AS t(c1)',
        None,
    ),
    (
        "SELECT pg_catalog.\"percentile_disc\"(0.25::double precision) WITHIN GROUP (ORDER BY c1) AS v FROM (VALUES ('-2147483648'::integer), ('-2147483648'::integer)) AS t(c1)",
        "-2147483648",
    ),
    (
        "SELECT pg_catalog.\"percentile_disc\"(0.25::double precision) WITHIN GROUP (ORDER BY c1) AS v FROM (VALUES ('2147483647'::integer), ('2147483647'::integer)) AS t(c1)",
        "2147483647",
    ),
    (
        "SELECT pg_catalog.\"percentile_disc\"(0.25::double precision) WITHIN GROUP (ORDER BY c1) AS v FROM (VALUES ('0'::integer), ('0'::integer)) AS t(c1)",
        "0",
    ),
    ('SELECT pg_catalog."btbpchar_pattern_cmp"(NULL::character, NULL::character) AS v', None),
    ("SELECT pg_catalog.\"btbpchar_pattern_cmp\"('abc'::character, 'abc'::character) AS v", "0"),
    (
        "SELECT pg_catalog.\"btbpchar_pattern_cmp\"('Hello World'::character, 'Hello World'::character) AS v",
        "0",
    ),
    ("SELECT pg_catalog.\"btbpchar_pattern_cmp\"('12'::character, '12'::character) AS v", "0"),
    ("SELECT pg_catalog.\"btbpchar_pattern_cmp\"(NULL::character, 'abc'::character) AS v", None),
    ("SELECT pg_catalog.\"btbpchar_pattern_cmp\"('abc'::character, NULL::character) AS v", None),
    ("SELECT pg_catalog.\"btbpchar_pattern_cmp\"(''::character, 'abc'::character) AS v", "-1"),
    ("SELECT pg_catalog.\"btbpchar_pattern_cmp\"('abc'::character, ''::character) AS v", "1"),
    (
        "SELECT pg_catalog.\"btbpchar_pattern_cmp\"('日本語テキスト😀'::character, 'abc'::character) AS v",
        "133",
    ),
    (
        "SELECT pg_catalog.\"btbpchar_pattern_cmp\"('abc'::character, '日本語テキスト😀'::character) AS v",
        "-133",
    ),
    ("SELECT pg_catalog.\"btbpchar_pattern_cmp\"('été'::character, 'abc'::character) AS v", "4"),
    ("SELECT pg_catalog.\"btbpchar_pattern_cmp\"('abc'::character, 'été'::character) AS v", "-4"),
    ('SELECT pg_catalog."btnamecmp"(NULL::name, NULL::name) AS v', None),
    ("SELECT pg_catalog.\"btnamecmp\"('abc'::name, 'abc'::name) AS v", "0"),
    ("SELECT pg_catalog.\"btnamecmp\"('Hello World'::name, 'Hello World'::name) AS v", "0"),
    ("SELECT pg_catalog.\"btnamecmp\"('12'::name, '12'::name) AS v", "0"),
    ("SELECT pg_catalog.\"btnamecmp\"(NULL::name, 'abc'::name) AS v", None),
    ("SELECT pg_catalog.\"btnamecmp\"('abc'::name, NULL::name) AS v", None),
    ("SELECT pg_catalog.\"btnamecmp\"(''::name, 'abc'::name) AS v", "-97"),
    ("SELECT pg_catalog.\"btnamecmp\"('abc'::name, ''::name) AS v", "97"),
    ("SELECT pg_catalog.\"btnamecmp\"('日本語テキスト😀'::name, 'abc'::name) AS v", "133"),
    ("SELECT pg_catalog.\"btnamecmp\"('abc'::name, '日本語テキスト😀'::name) AS v", "-133"),
    ("SELECT pg_catalog.\"btnamecmp\"('été'::name, 'abc'::name) AS v", "4"),
    ("SELECT pg_catalog.\"btnamecmp\"('abc'::name, 'été'::name) AS v", "-4"),
    ('SELECT pg_catalog."btnametextcmp"(NULL::name, NULL::text) AS v', None),
    ("SELECT pg_catalog.\"btnametextcmp\"('abc'::name, 'abc'::text) AS v", "0"),
    ("SELECT pg_catalog.\"btnametextcmp\"('Hello World'::name, 'Hello World'::text) AS v", "0"),
    ("SELECT pg_catalog.\"btnametextcmp\"('12'::name, '12'::text) AS v", "0"),
    ("SELECT pg_catalog.\"btnametextcmp\"(NULL::name, 'abc'::text) AS v", None),
    ("SELECT pg_catalog.\"btnametextcmp\"('abc'::name, NULL::text) AS v", None),
    ("SELECT pg_catalog.\"btnametextcmp\"(''::name, 'abc'::text) AS v", "-1"),
    ("SELECT pg_catalog.\"btnametextcmp\"('abc'::name, ''::text) AS v", "1"),
    ("SELECT pg_catalog.\"btnametextcmp\"('日本語テキスト😀'::name, 'abc'::text) AS v", "133"),
    ("SELECT pg_catalog.\"btnametextcmp\"('abc'::name, '日本語テキスト😀'::text) AS v", "-133"),
    ("SELECT pg_catalog.\"btnametextcmp\"('été'::name, 'abc'::text) AS v", "4"),
    ("SELECT pg_catalog.\"btnametextcmp\"('abc'::name, 'été'::text) AS v", "-4"),
    ('SELECT pg_catalog."bttext_pattern_cmp"(NULL::text, NULL::text) AS v', None),
    ("SELECT pg_catalog.\"bttext_pattern_cmp\"('abc'::text, 'abc'::text) AS v", "0"),
    (
        "SELECT pg_catalog.\"bttext_pattern_cmp\"('Hello World'::text, 'Hello World'::text) AS v",
        "0",
    ),
    ("SELECT pg_catalog.\"bttext_pattern_cmp\"('12'::text, '12'::text) AS v", "0"),
    ("SELECT pg_catalog.\"bttext_pattern_cmp\"(NULL::text, 'abc'::text) AS v", None),
    ("SELECT pg_catalog.\"bttext_pattern_cmp\"('abc'::text, NULL::text) AS v", None),
    ("SELECT pg_catalog.\"bttext_pattern_cmp\"(''::text, 'abc'::text) AS v", "-1"),
    ("SELECT pg_catalog.\"bttext_pattern_cmp\"('abc'::text, ''::text) AS v", "1"),
    ("SELECT pg_catalog.\"bttext_pattern_cmp\"('日本語テキスト😀'::text, 'abc'::text) AS v", "133"),
    (
        "SELECT pg_catalog.\"bttext_pattern_cmp\"('abc'::text, '日本語テキスト😀'::text) AS v",
        "-133",
    ),
    ("SELECT pg_catalog.\"bttext_pattern_cmp\"('été'::text, 'abc'::text) AS v", "4"),
    ("SELECT pg_catalog.\"bttext_pattern_cmp\"('abc'::text, 'été'::text) AS v", "-4"),
    ('SELECT pg_catalog."bttextnamecmp"(NULL::text, NULL::name) AS v', None),
    ("SELECT pg_catalog.\"bttextnamecmp\"('abc'::text, 'abc'::name) AS v", "0"),
    ("SELECT pg_catalog.\"bttextnamecmp\"('Hello World'::text, 'Hello World'::name) AS v", "0"),
    ("SELECT pg_catalog.\"bttextnamecmp\"('12'::text, '12'::name) AS v", "0"),
    ("SELECT pg_catalog.\"bttextnamecmp\"(NULL::text, 'abc'::name) AS v", None),
    ("SELECT pg_catalog.\"bttextnamecmp\"('abc'::text, NULL::name) AS v", None),
    ("SELECT pg_catalog.\"bttextnamecmp\"(''::text, 'abc'::name) AS v", "-1"),
    ("SELECT pg_catalog.\"bttextnamecmp\"('abc'::text, ''::name) AS v", "1"),
    ("SELECT pg_catalog.\"bttextnamecmp\"('日本語テキスト😀'::text, 'abc'::name) AS v", "133"),
    ("SELECT pg_catalog.\"bttextnamecmp\"('abc'::text, '日本語テキスト😀'::name) AS v", "-133"),
    ("SELECT pg_catalog.\"bttextnamecmp\"('été'::text, 'abc'::name) AS v", "4"),
    ("SELECT pg_catalog.\"bttextnamecmp\"('abc'::text, 'été'::name) AS v", "-4"),
    ('SELECT pg_catalog."byteacmp"(NULL::bytea, NULL::bytea) AS v', None),
    ("SELECT pg_catalog.\"byteacmp\"('\\xdeadbeef'::bytea, '\\xdeadbeef'::bytea) AS v", "0"),
    ("SELECT pg_catalog.\"byteacmp\"('abc'::bytea, 'abc'::bytea) AS v", "0"),
    ("SELECT pg_catalog.\"byteacmp\"('\\x00ff'::bytea, '\\x00ff'::bytea) AS v", "0"),
    ("SELECT pg_catalog.\"byteacmp\"(NULL::bytea, '\\xdeadbeef'::bytea) AS v", None),
    ("SELECT pg_catalog.\"byteacmp\"('\\xdeadbeef'::bytea, NULL::bytea) AS v", None),
    ("SELECT pg_catalog.\"byteacmp\"('\\x'::bytea, '\\xdeadbeef'::bytea) AS v", "-1"),
    ("SELECT pg_catalog.\"byteacmp\"('\\xdeadbeef'::bytea, '\\x'::bytea) AS v", "1"),
    ("SELECT pg_catalog.\"byteacmp\"('\\x00ff00'::bytea, '\\xdeadbeef'::bytea) AS v", "-222"),
    ("SELECT pg_catalog.\"byteacmp\"('\\xdeadbeef'::bytea, '\\x00ff00'::bytea) AS v", "222"),
    ('SELECT pg_catalog."gin_cmp_tslexeme"(NULL::text, NULL::text) AS v', None),
    ("SELECT pg_catalog.\"gin_cmp_tslexeme\"('abc'::text, 'abc'::text) AS v", "0"),
    ("SELECT pg_catalog.\"gin_cmp_tslexeme\"('Hello World'::text, 'Hello World'::text) AS v", "0"),
    ("SELECT pg_catalog.\"gin_cmp_tslexeme\"('12'::text, '12'::text) AS v", "0"),
    ("SELECT pg_catalog.\"gin_cmp_tslexeme\"(NULL::text, 'abc'::text) AS v", None),
    ("SELECT pg_catalog.\"gin_cmp_tslexeme\"('abc'::text, NULL::text) AS v", None),
    ("SELECT pg_catalog.\"gin_cmp_tslexeme\"(''::text, 'abc'::text) AS v", "-1"),
    ("SELECT pg_catalog.\"gin_cmp_tslexeme\"('abc'::text, ''::text) AS v", "1"),
    ("SELECT pg_catalog.\"gin_cmp_tslexeme\"('日本語テキスト😀'::text, 'abc'::text) AS v", "133"),
    ("SELECT pg_catalog.\"gin_cmp_tslexeme\"('abc'::text, '日本語テキスト😀'::text) AS v", "-133"),
    ("SELECT pg_catalog.\"gin_cmp_tslexeme\"('été'::text, 'abc'::text) AS v", "4"),
    ("SELECT pg_catalog.\"gin_cmp_tslexeme\"('abc'::text, 'été'::text) AS v", "-4"),
    ('SELECT pg_catalog."gin_compare_jsonb"(NULL::text, NULL::text) AS v', None),
    ("SELECT pg_catalog.\"gin_compare_jsonb\"('abc'::text, 'abc'::text) AS v", "0"),
    ("SELECT pg_catalog.\"gin_compare_jsonb\"('Hello World'::text, 'Hello World'::text) AS v", "0"),
    ("SELECT pg_catalog.\"gin_compare_jsonb\"('12'::text, '12'::text) AS v", "0"),
    ("SELECT pg_catalog.\"gin_compare_jsonb\"(NULL::text, 'abc'::text) AS v", None),
    ("SELECT pg_catalog.\"gin_compare_jsonb\"('abc'::text, NULL::text) AS v", None),
    ("SELECT pg_catalog.\"gin_compare_jsonb\"(''::text, 'abc'::text) AS v", "-1"),
    ("SELECT pg_catalog.\"gin_compare_jsonb\"('abc'::text, ''::text) AS v", "1"),
    ("SELECT pg_catalog.\"gin_compare_jsonb\"('日本語テキスト😀'::text, 'abc'::text) AS v", "133"),
    ("SELECT pg_catalog.\"gin_compare_jsonb\"('abc'::text, '日本語テキスト😀'::text) AS v", "-133"),
    ("SELECT pg_catalog.\"gin_compare_jsonb\"('été'::text, 'abc'::text) AS v", "4"),
    ("SELECT pg_catalog.\"gin_compare_jsonb\"('abc'::text, 'été'::text) AS v", "-4"),
    ('SELECT pg_catalog."array_to_string"(NULL::integer[], NULL::text) AS v', None),
    ("SELECT pg_catalog.\"array_to_string\"('{1,2,3}'::integer[], 'abc'::text) AS v", "1abc2abc3"),
    ("SELECT pg_catalog.\"array_to_string\"('{}'::integer[], 'Hello World'::text) AS v", ""),
    ("SELECT pg_catalog.\"array_to_string\"('{-7,NULL,42}'::integer[], '12'::text) AS v", "-71242"),
    ("SELECT pg_catalog.\"array_to_string\"(NULL::integer[], 'abc'::text) AS v", None),
    ("SELECT pg_catalog.\"array_to_string\"('{1,2,3}'::integer[], NULL::text) AS v", None),
    (
        "SELECT pg_catalog.\"array_to_string\"('{2147483647,-2147483648}'::integer[], 'abc'::text) AS v",
        "2147483647abc-2147483648",
    ),
    ("SELECT pg_catalog.\"array_to_string\"('{1,2,3}'::integer[], ''::text) AS v", "123"),
    ("SELECT pg_catalog.\"array_to_string\"('{NULL}'::integer[], 'abc'::text) AS v", ""),
    (
        "SELECT pg_catalog.\"array_to_string\"('{1,2,3}'::integer[], '日本語テキスト😀'::text) AS v",
        "1日本語テキスト😀2日本語テキスト😀3",
    ),
    (
        "SELECT pg_catalog.\"array_to_string\"('{{1,2},{3,4}}'::integer[], 'abc'::text) AS v",
        "1abc2abc3abc4",
    ),
    ("SELECT pg_catalog.\"array_to_string\"('{1,2,3}'::integer[], 'été'::text) AS v", "1été2été3"),
    ('SELECT pg_catalog."array_to_string"(NULL::integer[], NULL::text, NULL::text) AS v', None),
    (
        "SELECT pg_catalog.\"array_to_string\"('{1,2,3}'::integer[], 'abc'::text, 'abc'::text) AS v",
        "1abc2abc3",
    ),
    (
        "SELECT pg_catalog.\"array_to_string\"('{}'::integer[], 'Hello World'::text, 'Hello World'::text) AS v",
        "",
    ),
    (
        "SELECT pg_catalog.\"array_to_string\"('{-7,NULL,42}'::integer[], '12'::text, '12'::text) AS v",
        "-712121242",
    ),
    ("SELECT pg_catalog.\"array_to_string\"(NULL::integer[], 'abc'::text, 'abc'::text) AS v", None),
    (
        "SELECT pg_catalog.\"array_to_string\"('{1,2,3}'::integer[], NULL::text, 'abc'::text) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"array_to_string\"('{1,2,3}'::integer[], 'abc'::text, NULL::text) AS v",
        "1abc2abc3",
    ),
    (
        "SELECT pg_catalog.\"array_to_string\"('{2147483647,-2147483648}'::integer[], 'abc'::text, 'abc'::text) AS v",
        "2147483647abc-2147483648",
    ),
    (
        "SELECT pg_catalog.\"array_to_string\"('{1,2,3}'::integer[], ''::text, 'abc'::text) AS v",
        "123",
    ),
    (
        "SELECT pg_catalog.\"array_to_string\"('{1,2,3}'::integer[], 'abc'::text, ''::text) AS v",
        "1abc2abc3",
    ),
    (
        "SELECT pg_catalog.\"array_to_string\"('{NULL}'::integer[], 'abc'::text, 'abc'::text) AS v",
        "abc",
    ),
    (
        "SELECT pg_catalog.\"array_to_string\"('{1,2,3}'::integer[], '日本語テキスト😀'::text, 'abc'::text) AS v",
        "1日本語テキスト😀2日本語テキスト😀3",
    ),
    ('SELECT pg_catalog."cardinality"(NULL::integer[]) AS v', None),
    ("SELECT pg_catalog.\"cardinality\"('{1,2,3}'::integer[]) AS v", "3"),
    ("SELECT pg_catalog.\"cardinality\"('{}'::integer[]) AS v", "0"),
    ("SELECT pg_catalog.\"cardinality\"('{-7,NULL,42}'::integer[]) AS v", "3"),
    ("SELECT pg_catalog.\"cardinality\"('{2147483647,-2147483648}'::integer[]) AS v", "2"),
    ("SELECT pg_catalog.\"cardinality\"('{NULL}'::integer[]) AS v", "1"),
    ("SELECT pg_catalog.\"cardinality\"('{{1,2},{3,4}}'::integer[]) AS v", "4"),
    ("SELECT pg_catalog.\"unnest\"('{NULL}'::integer[]) AS v", None),
    ('SELECT pg_catalog."hashbpchar"(NULL::character) AS v', None),
    ("SELECT pg_catalog.\"hashbpchar\"('abc'::character) AS v", "1075015857"),
    ("SELECT pg_catalog.\"hashbpchar\"('Hello World'::character) AS v", "-1180053438"),
    ("SELECT pg_catalog.\"hashbpchar\"('12'::character) AS v", "-631133447"),
    ("SELECT pg_catalog.\"hashbpchar\"(''::character) AS v", "-1477818771"),
    ("SELECT pg_catalog.\"hashbpchar\"('日本語テキスト😀'::character) AS v", "-1779400605"),
    ("SELECT pg_catalog.\"hashbpchar\"('été'::character) AS v", "2035879394"),
    ("SELECT pg_catalog.\"hashbpchar\"('  padded  '::character) AS v", "-1477818771"),
    ('SELECT pg_catalog."hashbpcharextended"(NULL::character, NULL::bigint) AS v', None),
    (
        "SELECT pg_catalog.\"hashbpcharextended\"('abc'::character, 3::bigint) AS v",
        "-3249926563169018939",
    ),
    (
        "SELECT pg_catalog.\"hashbpcharextended\"('Hello World'::character, (-7)::bigint) AS v",
        "-1519033063351763176",
    ),
    (
        "SELECT pg_catalog.\"hashbpcharextended\"('12'::character, 5000000000::bigint) AS v",
        "-3612005053184030960",
    ),
    ('SELECT pg_catalog."hashbpcharextended"(NULL::character, 3::bigint) AS v', None),
    ("SELECT pg_catalog.\"hashbpcharextended\"('abc'::character, NULL::bigint) AS v", None),
    (
        "SELECT pg_catalog.\"hashbpcharextended\"(''::character, 3::bigint) AS v",
        "1628731699460038297",
    ),
    (
        "SELECT pg_catalog.\"hashbpcharextended\"('abc'::character, '-9223372036854775808'::bigint) AS v",
        "6837730478958681016",
    ),
    (
        "SELECT pg_catalog.\"hashbpcharextended\"('日本語テキスト😀'::character, 3::bigint) AS v",
        "-8292297051826092624",
    ),
    (
        "SELECT pg_catalog.\"hashbpcharextended\"('abc'::character, '9223372036854775807'::bigint) AS v",
        "-4948564415594343765",
    ),
    (
        "SELECT pg_catalog.\"hashbpcharextended\"('été'::character, 3::bigint) AS v",
        "-2879252015487380380",
    ),
    (
        "SELECT pg_catalog.\"hashbpcharextended\"('abc'::character, '0'::bigint) AS v",
        "3591986179850072241",
    ),
    ('SELECT pg_catalog."interval_out"(NULL::interval) AS v', None),
    ("SELECT pg_catalog.\"interval_out\"('1 day 02:03:04'::interval) AS v", "1 day 02:03:04"),
    ("SELECT pg_catalog.\"interval_out\"('3 mons'::interval) AS v", "3 mons"),
    ("SELECT pg_catalog.\"interval_out\"('-1 year 2 days'::interval) AS v", "-1 years +2 days"),
    ("SELECT pg_catalog.\"interval_out\"('00:00:00'::interval) AS v", "00:00:00"),
    ("SELECT pg_catalog.\"interval_out\"('-00:00:00.000001'::interval) AS v", "-00:00:00.000001"),
    ("SELECT pg_catalog.\"interval_out\"('178956970 years'::interval) AS v", "178956970 years"),
    ("SELECT pg_catalog.\"interval_out\"('-178956970 years'::interval) AS v", "-178956970 years"),
    ('SELECT pg_catalog."datemultirange"(NULL::daterange) AS v', None),
    (
        "SELECT pg_catalog.\"datemultirange\"('[2024-01-01,2024-02-01)'::daterange) AS v",
        "{[2024-01-01,2024-02-01)}",
    ),
    ("SELECT pg_catalog.\"datemultirange\"('empty'::daterange) AS v", "{}"),
    ("SELECT pg_catalog.\"datemultirange\"('(,2000-01-01]'::daterange) AS v", "{(,2000-01-02)}"),
    (
        "SELECT pg_catalog.\"datemultirange\"('[-infinity,infinity]'::daterange) AS v",
        "{[-infinity,infinity]}",
    ),
    ("SELECT pg_catalog.\"datemultirange\"('(,)'::daterange) AS v", "{(,)}"),
    ('SELECT pg_catalog."daterange_canonical"(NULL::daterange) AS v', None),
    (
        "SELECT pg_catalog.\"daterange_canonical\"('[2024-01-01,2024-02-01)'::daterange) AS v",
        "[2024-01-01,2024-02-01)",
    ),
    ("SELECT pg_catalog.\"daterange_canonical\"('empty'::daterange) AS v", "empty"),
    ("SELECT pg_catalog.\"daterange_canonical\"('(,2000-01-01]'::daterange) AS v", "(,2000-01-02)"),
    (
        "SELECT pg_catalog.\"daterange_canonical\"('[-infinity,infinity]'::daterange) AS v",
        "[-infinity,infinity]",
    ),
    ("SELECT pg_catalog.\"daterange_canonical\"('(,)'::daterange) AS v", "(,)"),
    ('SELECT pg_catalog."numeric_mod"(NULL::numeric, NULL::numeric) AS v', None),
    ('SELECT pg_catalog."numeric_mod"(2.5::numeric, 2.5::numeric) AS v', "0.0"),
    ('SELECT pg_catalog."numeric_mod"((-7.125)::numeric, (-7.125)::numeric) AS v', "0.000"),
    ('SELECT pg_catalog."numeric_mod"(1000::numeric, 1000::numeric) AS v', "0"),
    ('SELECT pg_catalog."numeric_mod"(NULL::numeric, 2.5::numeric) AS v', None),
    ('SELECT pg_catalog."numeric_mod"(2.5::numeric, NULL::numeric) AS v', None),
    ("SELECT pg_catalog.\"numeric_mod\"('0'::numeric, 2.5::numeric) AS v", "0.0"),
    (
        "SELECT pg_catalog.\"numeric_mod\"('-0.000000000000000000001'::numeric, 2.5::numeric) AS v",
        "-0.000000000000000000001",
    ),
    (
        "SELECT pg_catalog.\"numeric_mod\"(2.5::numeric, '-0.000000000000000000001'::numeric) AS v",
        "0.000000000000000000000",
    ),
    ('SELECT pg_catalog."numrange"(NULL::numeric, NULL::numeric) AS v', "(,)"),
    ('SELECT pg_catalog."numrange"(2.5::numeric, 2.5::numeric) AS v', "empty"),
    ('SELECT pg_catalog."numrange"((-7.125)::numeric, (-7.125)::numeric) AS v', "empty"),
    ('SELECT pg_catalog."numrange"(1000::numeric, 1000::numeric) AS v', "empty"),
    ('SELECT pg_catalog."numrange"(NULL::numeric, 2.5::numeric) AS v', "(,2.5)"),
    ('SELECT pg_catalog."numrange"(2.5::numeric, NULL::numeric) AS v', "[2.5,)"),
    ("SELECT pg_catalog.\"numrange\"('0'::numeric, 2.5::numeric) AS v", "[0,2.5)"),
    (
        "SELECT pg_catalog.\"numrange\"('-0.000000000000000000001'::numeric, 2.5::numeric) AS v",
        "[-0.000000000000000000001,2.5)",
    ),
    (
        "SELECT pg_catalog.\"numrange\"(2.5::numeric, '123456789012345678901234567890.123456789012345678901234567890'::numeric) AS v",
        "[2.5,123456789012345678901234567890.123456789012345678901234567890)",
    ),
    ("SELECT pg_catalog.sind('0'::double precision)", "0"),
    ("SELECT pg_catalog.sind('-0.0'::double precision)", "-0"),
    ("SELECT pg_catalog.sind('30'::double precision)", "0.5"),
    ("SELECT pg_catalog.sind('45'::double precision)", "0.7071067811865475"),
    ("SELECT pg_catalog.sind('60'::double precision)", "0.8660254037844387"),
    ("SELECT pg_catalog.sind('90'::double precision)", "1"),
    ("SELECT pg_catalog.sind('120'::double precision)", "0.8660254037844387"),
    ("SELECT pg_catalog.sind('135'::double precision)", "0.7071067811865475"),
    ("SELECT pg_catalog.sind('150'::double precision)", "0.5"),
    ("SELECT pg_catalog.sind('180'::double precision)", "0"),
    ("SELECT pg_catalog.sind('210'::double precision)", "-0.5"),
    ("SELECT pg_catalog.sind('225'::double precision)", "-0.7071067811865475"),
    ("SELECT pg_catalog.sind('270'::double precision)", "-1"),
    ("SELECT pg_catalog.sind('300'::double precision)", "-0.8660254037844387"),
    ("SELECT pg_catalog.sind('315'::double precision)", "-0.7071067811865475"),
    ("SELECT pg_catalog.sind('360'::double precision)", "0"),
    ("SELECT pg_catalog.sind('-30'::double precision)", "-0.5"),
    ("SELECT pg_catalog.sind('-45'::double precision)", "-0.7071067811865475"),
    ("SELECT pg_catalog.sind('-90'::double precision)", "-1"),
    ("SELECT pg_catalog.sind('-180'::double precision)", "-0"),
    ("SELECT pg_catalog.sind('-360'::double precision)", "-0"),
    ("SELECT pg_catalog.sind('-720'::double precision)", "-0"),
    ("SELECT pg_catalog.sind('720'::double precision)", "0"),
    ("SELECT pg_catalog.sind('1e+16'::double precision)", "-0.984807753012208"),
    ("SELECT pg_catalog.sind('-1e+16'::double precision)", "0.984807753012208"),
    ("SELECT pg_catalog.sind('9007199254740992'::double precision)", "0.5299192642332048"),
    ("SELECT pg_catalog.sind('9007199254740994'::double precision)", "0.5591929034707467"),
    ("SELECT pg_catalog.sind('1e+20'::double precision)", "-0.984807753012208"),
    ("SELECT pg_catalog.sind('1e+300'::double precision)", "0"),
    ("SELECT pg_catalog.sind('-1e+300'::double precision)", "-0"),
    ("SELECT pg_catalog.sind('1.7976931348623157e+308'::double precision)", "0.7880107536067218"),
    ("SELECT pg_catalog.sind('-1.7976931348623157e+308'::double precision)", "-0.7880107536067218"),
    ("SELECT pg_catalog.sind('5e-324'::double precision)", "0"),
    ("SELECT pg_catalog.sind('1e-10'::double precision)", "1.7453292519943298e-12"),
    ("SELECT pg_catalog.sind('359.99999999999994'::double precision)", "-9.921048172113444e-16"),
    ("SELECT pg_catalog.sind('450'::double precision)", "1"),
    ("SELECT pg_catalog.sind('-450'::double precision)", "-1"),
    ("SELECT pg_catalog.sind('3600030'::double precision)", "0.5"),
    ("SELECT pg_catalog.sind('-352.3344703336753'::double precision)", "0.13338996508412657"),
    ("SELECT pg_catalog.sind('-698.3016521509962'::double precision)", "0.3697199651414246"),
    ("SELECT pg_catalog.sind('301.8689460797075'::double precision)", "-0.8492579728797427"),
    ("SELECT pg_catalog.sind('-855.1274266649145'::double precision)", "-0.7055324177357662"),
    ("SELECT pg_catalog.sind('71.76400861337834'::double precision)", "0.9497756652470978"),
    ("SELECT pg_catalog.sind('-268.622166174829'::double precision)", "0.9997108671115748"),
    ("SELECT pg_catalog.sind('-884.0021504505864'::double precision)", "-0.27560127712292415"),
    ("SELECT pg_catalog.sind('14.871466378840523'::double precision)", "0.25665150055416536"),
    ("SELECT pg_catalog.sind('-925.0086831160303'::double precision)", "0.4227556068932642"),
    ("SELECT pg_catalog.sind('-132.70863267522827'::double precision)", "-0.7348124094365909"),
    ("SELECT pg_catalog.sind('-860.2891528507621'::double precision)", "-0.6389134676130814"),
    ("SELECT pg_catalog.sind('-818.5739733122699'::double precision)", "-0.9888242056707887"),
    ("SELECT pg_catalog.sind('-150.96162171497213'::double precision)", "-0.48539535571809755"),
    ("SELECT pg_catalog.sind('653.7042493440761'::double precision)", "-0.9156327803462938"),
    ("SELECT pg_catalog.sind('-752.3960777007088'::double precision)", "-0.5357689935355666"),
    ("SELECT pg_catalog.sind('-553.522070785971'::double precision)", "0.23381991108594055"),
    ("SELECT pg_catalog.sind('254.86644481117855'::double precision)", "-0.9653199012355046"),
    ("SELECT pg_catalog.sind('895.4178849140112'::double precision)", "0.07988777526590629"),
    ("SELECT pg_catalog.sind('154.20589723499734'::double precision)", "0.43513843070647706"),
    ("SELECT pg_catalog.sind('-206.63905069843963'::double precision)", "0.44836840647243004"),
    ("SELECT pg_catalog.sind('952.51021118584'::double precision)", "-0.7934618204309849"),
    ("SELECT pg_catalog.sind('-906.8346387644874'::double precision)", "0.11900425434526822"),
    ("SELECT pg_catalog.sind('716.936918097359'::double precision)", "-0.053435402335952877"),
    ("SELECT pg_catalog.sind('-420.7814273366474'::double precision)", "-0.8727638896894464"),
    ("SELECT pg_catalog.sind('-711.4898332851249'::double precision)", "0.14798490239611814"),
    ("SELECT pg_catalog.sind('-764.4155238432633'::double precision)", "-0.6998568956135611"),
    ("SELECT pg_catalog.sind('-383.03635179613127'::double precision)", "-0.3913150719656679"),
    ("SELECT pg_catalog.sind('632.2527182400629'::double precision)", "-0.9992271725426476"),
    ("SELECT pg_catalog.sind('-638.547240152125'::double precision)", "0.9888936589153586"),
    ("SELECT pg_catalog.sind('163.20032732493246'::double precision)", "0.2890263278713082"),
    ("SELECT pg_catalog.sind('277.826937852368'::double precision)", "-0.9906839237251492"),
    ("SELECT pg_catalog.sind('-255.20491454853754'::double precision)", "0.9668452959212829"),
    ("SELECT pg_catalog.sind('95.48893141911572'::double precision)", "0.9954146955803564"),
    ("SELECT pg_catalog.sind('-874.4220500533537'::double precision)", "-0.4317386501445524"),
    ("SELECT pg_catalog.sind('-880.7976600675347'::double precision)", "-0.3289052143474509"),
    ("SELECT pg_catalog.sind('-588.0825743613469'::double precision)", "0.7441084007108087"),
    ("SELECT pg_catalog.sind('360.79994636357173'::double precision)", "0.013961244298117337"),
    ("SELECT pg_catalog.sind('-144.81538866119422'::double precision)", "-0.5762128243123206"),
    ("SELECT pg_catalog.sind('-371.705659246417'::double precision)", "-0.2028840146357286"),
    ("SELECT pg_catalog.sind('171.12372701527738'::double precision)", "0.15430124450182028"),
    ("SELECT pg_catalog.sind('-93.63124725844932'::double precision)", "-0.9979923360992389"),
    ("SELECT pg_catalog.sind('-400.46600627263524'::double precision)", "-0.648996783315537"),
    ("SELECT pg_catalog.sind('588.7589630449822'::double precision)", "-0.7519429428082607"),
    ("SELECT pg_catalog.sind('397.9888674591425'::double precision)", "0.6155083536161293"),
    ("SELECT pg_catalog.sind('-511.80697855569423'::double precision)", "-0.4724434196842943"),
    ("SELECT pg_catalog.sind('148.84742051734202'::double precision)", "0.5173188947959214"),
    ("SELECT pg_catalog.sind('50.393007622902815'::double precision)", "0.7704354458125127"),
    ("SELECT pg_catalog.sind('750.2749911468577'::double precision)", "0.5041507149790231"),
    ("SELECT pg_catalog.sind('458.89057887843524'::double precision)", "0.9879852913789653"),
    ("SELECT pg_catalog.sind('-424.1244702196269'::double precision)", "-0.8997442487941965"),
    ("SELECT pg_catalog.sind('960.3496949851642'::double precision)", "-0.8690609193298577"),
    ("SELECT pg_catalog.sind('-763.8684434900758'::double precision)", "-0.6930048687948751"),
    ("SELECT pg_catalog.sind('-163.75435642954562'::double precision)", "-0.27975601675392764"),
    ("SELECT pg_catalog.sind('514.2818591304988'::double precision)", "0.43394435996229436"),
    ("SELECT pg_catalog.sind('-696.0309306789904'::double precision)", "0.4062434134532802"),
    ("SELECT pg_catalog.sind('-22.073799048388764'::double precision)", "-0.375800528877415"),
    ("SELECT pg_catalog.sind('-921.5854859051246'::double precision)", "0.3678890110646884"),
    ("SELECT pg_catalog.sind('336.4317130687905'::double precision)", "-0.3998417668084112"),
    ("SELECT pg_catalog.sind('529.1417324256263'::double precision)", "0.1883801653621645"),
    ("SELECT pg_catalog.sind('146.05188055476788'::double precision)", "0.5584419920019265"),
    ("SELECT pg_catalog.sind('7.509556236617764e+17'::double precision)", "0.587785252292473"),
    ("SELECT pg_catalog.sind('-3.7250497430380646e+17'::double precision)", "0.9945218953682733"),
    ("SELECT pg_catalog.sind('3.905907325473185e+17'::double precision)", "-0.9510565162951535"),
    ("SELECT pg_catalog.sind('1.8873975421003674e+17'::double precision)", "-0.898794046299167"),
    ("SELECT pg_catalog.sind('1.5979040856498432e+17'::double precision)", "0.8660254037844387"),
    ("SELECT pg_catalog.sind('-8.758933739717389e+16'::double precision)", "0.9271838545667874"),
    ("SELECT pg_catalog.sind('6.799355610250829e+17'::double precision)", "0.8660254037844387"),
    ("SELECT pg_catalog.sind('8.893621902158748e+17'::double precision)", "0.2756373558169992"),
    ("SELECT pg_catalog.sind('-5.180332516071104e+16'::double precision)", "0.34202014332566877"),
    ("SELECT pg_catalog.sind('3.2830441094934886e+17'::double precision)", "-0.6946583704589973"),
    ("SELECT pg_catalog.sind('-8.786611448055606e+17'::double precision)", "-0.9945218953682733"),
    ("SELECT pg_catalog.sind('4.029840426088479e+17'::double precision)", "-0.9993908270190958"),
    ("SELECT pg_catalog.sind('2.942577090553375e+17'::double precision)", "0.9510565162951535"),
    ("SELECT pg_catalog.sind('9.861918789332682e+17'::double precision)", "0.40673664307580026"),
    ("SELECT pg_catalog.sind('6.438495732194299e+17'::double precision)", "-0.46947156278589086"),
    ("SELECT pg_catalog.sind('-4.3080893581170157e+17'::double precision)", "0.9271838545667874"),
    ("SELECT pg_catalog.sind('-2.284171151065783e+17'::double precision)", "0.2756373558169992"),
    ("SELECT pg_catalog.sind('3.373054317683763e+17'::double precision)", "0.984807753012208"),
    ("SELECT pg_catalog.sind('-9.548741438888229e+17'::double precision)", "0.20791169081775937"),
    ("SELECT pg_catalog.sind('-7.660942740046822e+16'::double precision)", "0.6946583704589973"),
    ("SELECT pg_catalog.sind('-6.63903242186911e+17'::double precision)", "-0.6946583704589973"),
    ("SELECT pg_catalog.sind('-7.658084110365362e+17'::double precision)", "0.20791169081775937"),
    ("SELECT pg_catalog.sind('-8.820911613373792e+17'::double precision)", "-0.34202014332566877"),
    ("SELECT pg_catalog.sind('5.364659769450414e+17'::double precision)", "-0.9271838545667874"),
    ("SELECT pg_catalog.sind('-7.413195559626316e+17'::double precision)", "0.7880107536067218"),
    ("SELECT pg_catalog.sind('-5.0477033260617146e+17'::double precision)", "0.9702957262759965"),
    ("SELECT pg_catalog.sind('-2.1810059373354586e+17'::double precision)", "0.40673664307580026"),
    ("SELECT pg_catalog.sind('7.428439482525988e+17'::double precision)", "0.587785252292473"),
    ("SELECT pg_catalog.sind('-8.388373975997228e+17'::double precision)", "-0.5299192642332048"),
    ("SELECT pg_catalog.sind('-1.0162519810133811e+17'::double precision)", "-0.46947156278589086"),
    ("SELECT pg_catalog.sind('1.6884648201487328e+169'::double precision)", "0.34202014332566877"),
    ("SELECT pg_catalog.sind('-2.1786629775058796e+252'::double precision)", "-0.7431448254773942"),
    ("SELECT pg_catalog.sind('-3.7169483385108185e+217'::double precision)", "0.9993908270190958"),
    (
        "SELECT pg_catalog.sind('-1.8997755382139974e+210'::double precision)",
        "-0.20791169081775937",
    ),
    ("SELECT pg_catalog.sind('9.576588377613846e+294'::double precision)", "-0.8290375725550417"),
    ("SELECT pg_catalog.sind('3.6248159126262636e+25'::double precision)", "-0.5299192642332048"),
    ("SELECT pg_catalog.sind('2.771500622134285e+71'::double precision)", "-0.13917310096006547"),
    ("SELECT pg_catalog.sind('5193.049136405663'::double precision)", "0.45322621276453506"),
    ("SELECT pg_catalog.sind('8.432546485063132e+80'::double precision)", "0.9702957262759965"),
    ("SELECT pg_catalog.sind('-7.38463232585639e+44'::double precision)", "-0.7431448254773942"),
    ("SELECT pg_catalog.sind('-6.640947686814204e+187'::double precision)", "-0.9993908270190958"),
    ("SELECT pg_catalog.sind('3.582292647804163e+293'::double precision)", "0.34202014332566877"),
    ("SELECT pg_catalog.sind('-4.428599824348172e+140'::double precision)", "-0.9993908270190958"),
    ("SELECT pg_catalog.sind('-4.031310141940825e+122'::double precision)", "-0.9510565162951535"),
    ("SELECT pg_catalog.sind('-7.752198110990332e+31'::double precision)", "0.13917310096006547"),
    ("SELECT pg_catalog.sind('1.4870619405533841e+19'::double precision)", "-0.9510565162951535"),
    ("SELECT pg_catalog.sind('-1.895051570540204e+303'::double precision)", "0.6946583704589973"),
    ("SELECT pg_catalog.sind('-9.75847230260282e+49'::double precision)", "-0.5299192642332048"),
    ("SELECT pg_catalog.sind('1.0568083483815113e+185'::double precision)", "-0.8290375725550417"),
    ("SELECT pg_catalog.sind('1.1799152808224647'::double precision)", "0.02059195100502797"),
    ("SELECT pg_catalog.sind('-1.8991352476087505e+165'::double precision)", "-0.8290375725550417"),
    ("SELECT pg_catalog.sind('1.0741798377043402e+189'::double precision)", "0.34202014332566877"),
    ("SELECT pg_catalog.sind('-1.969573675995471e+269'::double precision)", "-0.46947156278589086"),
    ("SELECT pg_catalog.sind('-5.669561647417182e+45'::double precision)", "-0.898794046299167"),
    ("SELECT pg_catalog.sind('-1.9237621148721974e+294'::double precision)", "0.06975647374412532"),
    ("SELECT pg_catalog.sind('1.093076796630543e+146'::double precision)", "0.9702957262759965"),
    ("SELECT pg_catalog.sind('-2.9687481563339654e+261'::double precision)", "0.898794046299167"),
    ("SELECT pg_catalog.sind('-9.155714075948541e+147'::double precision)", "-0.9271838545667874"),
    ("SELECT pg_catalog.sind('2.8344954180934757e+26'::double precision)", "-0.2756373558169992"),
    ("SELECT pg_catalog.sind('-7.935375755522184e+230'::double precision)", "0.34202014332566877"),
    ("SELECT pg_catalog.cosd('0'::double precision)", "1"),
    ("SELECT pg_catalog.cosd('-0.0'::double precision)", "1"),
    ("SELECT pg_catalog.cosd('30'::double precision)", "0.8660254037844387"),
    ("SELECT pg_catalog.cosd('45'::double precision)", "0.7071067811865475"),
    ("SELECT pg_catalog.cosd('60'::double precision)", "0.5"),
    ("SELECT pg_catalog.cosd('90'::double precision)", "0"),
    ("SELECT pg_catalog.cosd('120'::double precision)", "-0.5"),
    ("SELECT pg_catalog.cosd('135'::double precision)", "-0.7071067811865475"),
    ("SELECT pg_catalog.cosd('150'::double precision)", "-0.8660254037844387"),
    ("SELECT pg_catalog.cosd('180'::double precision)", "-1"),
    ("SELECT pg_catalog.cosd('210'::double precision)", "-0.8660254037844387"),
    ("SELECT pg_catalog.cosd('225'::double precision)", "-0.7071067811865475"),
    ("SELECT pg_catalog.cosd('270'::double precision)", "0"),
    ("SELECT pg_catalog.cosd('300'::double precision)", "0.5"),
    ("SELECT pg_catalog.cosd('315'::double precision)", "0.7071067811865475"),
    ("SELECT pg_catalog.cosd('360'::double precision)", "1"),
    ("SELECT pg_catalog.cosd('-30'::double precision)", "0.8660254037844387"),
    ("SELECT pg_catalog.cosd('-45'::double precision)", "0.7071067811865475"),
    ("SELECT pg_catalog.cosd('-90'::double precision)", "0"),
    ("SELECT pg_catalog.cosd('-180'::double precision)", "-1"),
    ("SELECT pg_catalog.cosd('-360'::double precision)", "1"),
    ("SELECT pg_catalog.cosd('-720'::double precision)", "1"),
    ("SELECT pg_catalog.cosd('720'::double precision)", "1"),
    ("SELECT pg_catalog.cosd('1e+16'::double precision)", "0.17364817766693036"),
    ("SELECT pg_catalog.cosd('-1e+16'::double precision)", "0.17364817766693036"),
    ("SELECT pg_catalog.cosd('9007199254740992'::double precision)", "0.848048096156426"),
    ("SELECT pg_catalog.cosd('9007199254740994'::double precision)", "0.8290375725550417"),
    ("SELECT pg_catalog.cosd('1e+20'::double precision)", "0.17364817766693036"),
    ("SELECT pg_catalog.cosd('1e+300'::double precision)", "1"),
    ("SELECT pg_catalog.cosd('-1e+300'::double precision)", "1"),
    ("SELECT pg_catalog.cosd('1.7976931348623157e+308'::double precision)", "-0.6156614753256582"),
    ("SELECT pg_catalog.cosd('-1.7976931348623157e+308'::double precision)", "-0.6156614753256582"),
    ("SELECT pg_catalog.cosd('5e-324'::double precision)", "1"),
    ("SELECT pg_catalog.cosd('1e-10'::double precision)", "1"),
    ("SELECT pg_catalog.cosd('359.99999999999994'::double precision)", "1"),
    ("SELECT pg_catalog.cosd('450'::double precision)", "0"),
    ("SELECT pg_catalog.cosd('-450'::double precision)", "0"),
    ("SELECT pg_catalog.cosd('3600030'::double precision)", "0.8660254037844387"),
    ("SELECT pg_catalog.cosd('-352.3344703336753'::double precision)", "0.991063629246304"),
    ("SELECT pg_catalog.cosd('-698.3016521509962'::double precision)", "0.9291432329710118"),
    ("SELECT pg_catalog.cosd('301.8689460797075'::double precision)", "0.5279781202854812"),
    ("SELECT pg_catalog.cosd('-855.1274266649145'::double precision)", "-0.7086776471174494"),
    ("SELECT '0'::double precision", "0"),
    ("SELECT '-0'::double precision", "-0"),
    ("SELECT '-0.0'::double precision", "-0"),
    ("SELECT '1.5'::double precision", "1.5"),
    ("SELECT ' 2.5 '::double precision", "2.5"),
    ("SELECT '1e-320'::double precision", "1e-320"),
    ("SELECT '4.9e-324'::double precision", "5e-324"),
    ("SELECT '1.7976931348623157e+308'::double precision", "1.7976931348623157e+308"),
    ("SELECT 'NaN'::double precision", "NaN"),
    ("SELECT 'Infinity'::double precision", "Infinity"),
    ("SELECT '-Infinity'::double precision", "-Infinity"),
    ("SELECT 'inf'::double precision", "Infinity"),
    ("SELECT '0.1'::double precision", "0.1"),
    ("SELECT '0.30000000000000004'::double precision", "0.30000000000000004"),
    ("SELECT '123456789012345678901234567890'::double precision", "1.2345678901234568e+29"),
    ("SELECT '9007199254740993'::double precision", "9.007199254740992e+15"),
    ("SELECT '.5'::double precision", "0.5"),
    ("SELECT '5.'::double precision", "5"),
    ("SELECT '-.5e3'::double precision", "-500"),
    ("SELECT '0.1000000000000000055511151231257827021181583404541015625'::double precision", "0.1"),
    ("SELECT '2.2250738585072011e-308'::double precision", "2.225073858507201e-308"),
    ("SELECT '2.7126267136355693e+57'::double precision", "2.7126267136355693e+57"),
    ("SELECT '2.9176505403442687e-228'::double precision", "2.9176505403442687e-228"),
    ("SELECT '5.5705027106665415e+256'::double precision", "5.5705027106665415e+256"),
    ("SELECT '2.0668446752292503e-16'::double precision", "2.0668446752292503e-16"),
    ("SELECT '1.4826236772844953e+50'::double precision", "1.4826236772844953e+50"),
    ("SELECT '2.670405756230503e+65'::double precision", "2.670405756230503e+65"),
    ("SELECT '2.3095972247362096e+252'::double precision", "2.3095972247362096e+252"),
    ("SELECT '2.1552158160265848e-19'::double precision", "2.1552158160265848e-19"),
    ("SELECT '4.084669271327047e+31'::double precision", "4.084669271327047e+31"),
    ("SELECT '1.89867543353284e-190'::double precision", "1.89867543353284e-190"),
    ("SELECT '1.5619592075495646e+134'::double precision", "1.5619592075495646e+134"),
    ("SELECT '3.703210565721954e+25'::double precision", "3.703210565721954e+25"),
    ("SELECT '8.339285569756503e+30'::double precision", "8.339285569756503e+30"),
    ("SELECT '8.092383615358505e-64'::double precision", "8.092383615358505e-64"),
    ("SELECT '7.942454533308265e+222'::double precision", "7.942454533308265e+222"),
    ("SELECT '1.0724487151562167e-165'::double precision", "1.0724487151562167e-165"),
    ("SELECT '3.5334869493505085e-215'::double precision", "3.5334869493505085e-215"),
    ("SELECT '6.997119006180081e+262'::double precision", "6.997119006180081e+262"),
    ("SELECT '2.9495629468048806e-68'::double precision", "2.9495629468048806e-68"),
    ("SELECT '2.438173398544643e-299'::double precision", "2.438173398544643e-299"),
    ("SELECT '1.7164744666665028e+171'::double precision", "1.7164744666665028e+171"),
    ("SELECT '2.206889037164534e-210'::double precision", "2.206889037164534e-210"),
    ("SELECT '2.2700200993576141e+282'::double precision", "2.2700200993576141e+282"),
    ("SELECT '2.809489096561197e-282'::double precision", "2.809489096561197e-282"),
    ("SELECT '9.872569543527427e+172'::double precision", "9.872569543527427e+172"),
    ("SELECT '6.40067725527354e+199'::double precision", "6.40067725527354e+199"),
    ("SELECT '1.472862824289832e-142'::double precision", "1.472862824289832e-142"),
    ("SELECT '5.257740645155257e+58'::double precision", "5.257740645155257e+58"),
    ("SELECT '2.2767127185184196e+259'::double precision", "2.2767127185184196e+259"),
    ("SELECT '1.0561848313607126e-69'::double precision", "1.0561848313607126e-69"),
    ("SELECT '8.484860439896298e+177'::double precision", "8.484860439896298e+177"),
    ("SELECT '1.9094844770358533e-45'::double precision", "1.9094844770358533e-45"),
    ("SELECT '9.160910978862201e+140'::double precision", "9.160910978862201e+140"),
    ("SELECT '5.520782241090869e+47'::double precision", "5.520782241090869e+47"),
    ("SELECT '2.7865401617620176e+286'::double precision", "2.7865401617620176e+286"),
    ("SELECT '5.867164912993042e-226'::double precision", "5.867164912993042e-226"),
    ("SELECT '2.6096236267456295e-83'::double precision", "2.6096236267456295e-83"),
    ("SELECT '1.5717641235735652e-286'::double precision", "1.5717641235735652e-286"),
    ("SELECT '0.0014855807050772596'::double precision", "0.0014855807050772596"),
    ("SELECT '1.3153820747201239e-149'::double precision", "1.3153820747201239e-149"),
    ("SELECT '2.4594391437744508e+106'::double precision", "2.4594391437744508e+106"),
    ("SELECT '2.0649728324670836e+172'::double precision", "2.0649728324670836e+172"),
    ("SELECT '2.1208297357696158e+219'::double precision", "2.1208297357696158e+219"),
    ("SELECT '5.1732786776342e-49'::double precision", "5.1732786776342e-49"),
    ("SELECT '7.870371814630122e+205'::double precision", "7.870371814630122e+205"),
    ("SELECT '9.126217290657043e+45'::double precision", "9.126217290657043e+45"),
    ("SELECT '2.1759731018492944e+21'::double precision", "2.1759731018492944e+21"),
    ("SELECT '2.186200672701022e-57'::double precision", "2.186200672701022e-57"),
    ("SELECT '2.0919683891942585e-165'::double precision", "2.0919683891942585e-165"),
    ("SELECT '4.8426612616317165e-101'::double precision", "4.8426612616317165e-101"),
    ("SELECT '4.254239532951741e+256'::double precision", "4.254239532951741e+256"),
    ("SELECT '5.4399058643825485e-291'::double precision", "5.4399058643825485e-291"),
    ("SELECT '3.0892616715701608e-136'::double precision", "3.0892616715701608e-136"),
    ("SELECT '3.6180631706739256e+65'::double precision", "3.6180631706739256e+65"),
    ("SELECT '7.839441234777491e+120'::double precision", "7.839441234777491e+120"),
    ("SELECT '5.8348229605055436e+122'::double precision", "5.8348229605055436e+122"),
    ("SELECT '1.9720194468613592e-107'::double precision", "1.9720194468613592e-107"),
    ("SELECT '1.188922144244817e+26'::double precision", "1.188922144244817e+26"),
    ("SELECT '4.8707470306248364e+44'::double precision", "4.8707470306248364e+44"),
    ("SELECT '1.680199175962828e-244'::double precision", "1.680199175962828e-244"),
    ("SELECT '1.5880199927324664e+96'::double precision", "1.5880199927324664e+96"),
    ("SELECT '2.025617903062222e+82'::double precision", "2.025617903062222e+82"),
    ("SELECT '1.68390505033116e+301'::double precision", "1.68390505033116e+301"),
    ("SELECT '5.0994209331428703e-144'::double precision", "5.0994209331428703e-144"),
    ("SELECT '5.9340588167922786e-232'::double precision", "5.9340588167922786e-232"),
    ("SELECT '1.65732020254796e-11'::double precision", "1.65732020254796e-11"),
    ("SELECT '7.318622386610436e+85'::double precision", "7.318622386610436e+85"),
    ("SELECT '1.4271272939026165e-10'::double precision", "1.4271272939026165e-10"),
    ("SELECT '1.5695307196560543e-96'::double precision", "1.5695307196560543e-96"),
    ("SELECT '1.3775633422018118e-267'::double precision", "1.3775633422018118e-267"),
    ("SELECT '1.3688239748367356e+245'::double precision", "1.3688239748367356e+245"),
    ("SELECT '2.976719324065102e-296'::double precision", "2.976719324065102e-296"),
    ("SELECT '2.503170163693795e-45'::double precision", "2.503170163693795e-45"),
    ("SELECT '1.1059210343564406e-52'::double precision", "1.1059210343564406e-52"),
    ("SELECT '2.4256304317392993e-235'::double precision", "2.4256304317392993e-235"),
    ("SELECT '1.0703161938045496e+65'::double precision", "1.0703161938045496e+65"),
    ("SELECT '4.110948927857099e+161'::double precision", "4.110948927857099e+161"),
    ("SELECT '9.640234079587065e-76'::double precision", "9.640234079587065e-76"),
    ("SELECT '3.6324138281705476e+53'::double precision", "3.6324138281705476e+53"),
    ("SELECT '4.639607119427315e+31'::double precision", "4.639607119427315e+31"),
    ("SELECT '2.4970326632386235e+260'::double precision", "2.4970326632386235e+260"),
    ("SELECT '1.3263893499713182e-136'::double precision", "1.3263893499713182e-136"),
    ("SELECT '3.4141291310850915e-163'::double precision", "3.4141291310850915e-163"),
    ("SELECT '1.8433528872235103e-286'::double precision", "1.8433528872235103e-286"),
    ("SELECT '3.3040579076216197e-304'::double precision", "3.3040579076216197e-304"),
    ("SELECT '5.355188910804082e-242'::double precision", "5.355188910804082e-242"),
    ("SELECT '1.7605891396665585e+22'::double precision", "1.7605891396665585e+22"),
    ("SELECT '1.1858851602510638e+277'::double precision", "1.1858851602510638e+277"),
    ("SELECT '9.267542600832176e+290'::double precision", "9.267542600832176e+290"),
    ("SELECT '7.052813038700566e-129'::double precision", "7.052813038700566e-129"),
    ("SELECT '2.779544562592642e-146'::double precision", "2.779544562592642e-146"),
    ("SELECT '1.9837687900134493e+117'::double precision", "1.9837687900134493e+117"),
    ("SELECT '1.4772867266963093e+296'::double precision", "1.4772867266963093e+296"),
    ("SELECT '3.545486103380911e-99'::double precision", "3.545486103380911e-99"),
    ("SELECT '1.353331456777137e-86'::double precision", "1.353331456777137e-86"),
    ("SELECT '2.2334547670421077e-223'::double precision", "2.2334547670421077e-223"),
    ("SELECT '2.0551120628857523e+223'::double precision", "2.0551120628857523e+223"),
    ("SELECT '2.1921965313714476e-76'::double precision", "2.1921965313714476e-76"),
    ("SELECT '2.33156587171458e+228'::double precision", "2.33156587171458e+228"),
    ("SELECT '1.3932340513798106e-70'::double precision", "1.3932340513798106e-70"),
    ("SELECT '3.015768220396252e+226'::double precision", "3.015768220396252e+226"),
    ("SELECT '7.831708783209169e+111'::double precision", "7.831708783209169e+111"),
    ("SELECT '1.9572598910425757e-245'::double precision", "1.9572598910425757e-245"),
    ("SELECT '7.310173314464406e+291'::double precision", "7.310173314464406e+291"),
    ("SELECT '2.2545883563934224e+192'::double precision", "2.2545883563934224e+192"),
    ("SELECT '2.1304172560279723e-141'::double precision", "2.1304172560279723e-141"),
    ("SELECT '1.246662133008564e+83'::double precision", "1.246662133008564e+83"),
    ("SELECT '1.8086933691088156e+133'::double precision", "1.8086933691088156e+133"),
    ("SELECT '2.404703570445695e+269'::double precision", "2.404703570445695e+269"),
    ("SELECT '5.550986871330449e-39'::double precision", "5.550986871330449e-39"),
    ("SELECT '1.8192058202142612e-149'::double precision", "1.8192058202142612e-149"),
    ("SELECT '7.406070010420298e-122'::double precision", "7.406070010420298e-122"),
    ("SELECT '9.909337119350466e-100'::double precision", "9.909337119350466e-100"),
    ("SELECT '1.3258656378474553e+178'::double precision", "1.3258656378474553e+178"),
    ("SELECT '7.017057315329988e+300'::double precision", "7.017057315329988e+300"),
    ("SELECT '1.5161527136293113e-114'::double precision", "1.5161527136293113e-114"),
    ("SELECT '1.528140375961946e-76'::double precision", "1.528140375961946e-76"),
    ("SELECT '2.0695267912968487e+55'::double precision", "2.0695267912968487e+55"),
    ("SELECT '1.6325555578354879e-226'::double precision", "1.6325555578354879e-226"),
    ("SELECT '4.478076852633851e+82'::double precision", "4.478076852633851e+82"),
    ("SELECT '1.0376896301782233e-103'::double precision", "1.0376896301782233e-103"),
    ("SELECT '4.348646398617984e-91'::double precision", "4.348646398617984e-91"),
    ("SELECT '4.106786011534582e+257'::double precision", "4.106786011534582e+257"),
    ("SELECT '2.633035287396492e+67'::double precision", "2.633035287396492e+67"),
    ("SELECT '1.0638474419909528e-136'::double precision", "1.0638474419909528e-136"),
    ("SELECT '7.161573821732815e-07'::double precision", "7.161573821732815e-07"),
    ("SELECT '2.5599195517679576e+55'::double precision", "2.5599195517679576e+55"),
    ("SELECT '5.498054260140333e+280'::double precision", "5.498054260140333e+280"),
    ("SELECT '1.4926167324219073e-295'::double precision", "1.4926167324219073e-295"),
    ("SELECT '4.425914104679962e-81'::double precision", "4.425914104679962e-81"),
    ("SELECT '1.6025207483661596e+79'::double precision", "1.6025207483661596e+79"),
    ("SELECT '1.5194827027949456e-124'::double precision", "1.5194827027949456e-124"),
    ("SELECT '7.319051947172803e+62'::double precision", "7.319051947172803e+62"),
    ("SELECT '2.839130205064358e-199'::double precision", "2.839130205064358e-199"),
    ("SELECT '1.895787292811924e-194'::double precision", "1.895787292811924e-194"),
    ("SELECT '2.817802198345164e+159'::double precision", "2.817802198345164e+159"),
    ("SELECT '1.9967605734722827e+212'::double precision", "1.9967605734722827e+212"),
    ("SELECT '7.82047134354126e-146'::double precision", "7.82047134354126e-146"),
    ("SELECT '2.8250857168374716e+177'::double precision", "2.8250857168374716e+177"),
    ("SELECT '5.2058511246531964e-244'::double precision", "5.2058511246531964e-244"),
    ("SELECT '2.0812354614917164e+193'::double precision", "2.0812354614917164e+193"),
    ("SELECT '8.575751580740835e+290'::double precision", "8.575751580740835e+290"),
    ("SELECT '3.955211591590012e+113'::double precision", "3.955211591590012e+113"),
    ("SELECT '1.2133672358152645e-227'::double precision", "1.2133672358152645e-227"),
    ("SELECT '2.043907756837262'::double precision", "2.043907756837262"),
    ("SELECT '1.2183615442872851e+95'::double precision", "1.2183615442872851e+95"),
    ("SELECT '1.3829536037733014e-142'::double precision", "1.3829536037733014e-142"),
    ("SELECT '1.4242639051736814e-106'::double precision", "1.4242639051736814e-106"),
    ("SELECT '1.2582779796896832e+110'::double precision", "1.2582779796896832e+110"),
    ("SELECT '3.274504088208013e+92'::double precision", "3.274504088208013e+92"),
    ("SELECT 0.1::double precision", "0.1"),
    ("SELECT 1e308::double precision", "1e+308"),
    ("SELECT 12345678901234567890.5::double precision", "1.2345678901234567e+19"),
    ("SELECT 4.029840426088479e+17::double precision", "4.029840426088479e+17"),
    (
        "SELECT pg_catalog.circle('(1,2)'::point, '1.7976931348623157e+308'::double precision)",
        "<(1,2),1.7976931348623157e+308>",
    ),
    ("SELECT pg_catalog.circle('(1,2)'::point, '1e-300'::double precision)", "<(1,2),1e-300>"),
    ("SELECT pg_catalog.btnamecmp('abc'::name, 'abcd'::name)", "-100"),
    ("SELECT pg_catalog.bttext_pattern_cmp('abc'::text, 'abcd'::text)", "-1"),
    ("SELECT pg_catalog.btbpchar_pattern_cmp('ab  '::character(4), 'ab'::character(2))", "0"),
    ("SELECT pg_catalog.byteacmp('\\x01'::bytea, '\\x0102'::bytea)", "-1"),
    ("SELECT pg_catalog.cardinality('{{{1,2},{3,4}},{{5,6},{7,8}}}'::integer[])", "8"),
    ("SELECT pg_catalog.array_to_string('{{1,NULL},{3,4}}'::integer[], ',', '*')", "1,*,3,4"),
    ("SELECT pg_catalog.array_to_string('{{1,NULL},{3,4}}'::integer[], ',')", "1,3,4"),
    (
        "SELECT pg_catalog.interval_out('178956970 years 7 mons'::interval)",
        "178956970 years 7 mons",
    ),
    (
        "SELECT pg_catalog.daterange_canonical('(-infinity,infinity)'::daterange)",
        "(-infinity,infinity)",
    ),
    (
        "SELECT pg_catalog.daterange_canonical('(2020-01-01,infinity]'::daterange)",
        "[2020-01-02,infinity]",
    ),
    (
        "SELECT pg_catalog.numeric_mod('99999999999999999999999999999999999999'::numeric, 7::numeric)",
        "1",
    ),
    ("SELECT pg_catalog.numeric_mod('-7.5'::numeric, '2'::numeric)", "-1.5"),
    (
        "SELECT pg_catalog.numrange('0.000000000000000000001'::numeric, '1e3'::numeric)",
        "[0.000000000000000000001,1000)",
    ),
]
RAISED = [
    ("SELECT pg_catalog.\"sind\"('-Infinity'::double precision) AS v", "22003"),
    ("SELECT pg_catalog.\"cosd\"('-Infinity'::double precision) AS v", "22003"),
    ("SELECT pg_catalog.\"tand\"('-Infinity'::double precision) AS v", "22003"),
    ("SELECT pg_catalog.\"cotd\"('-Infinity'::double precision) AS v", "22003"),
    (
        "SELECT pg_catalog.\"percentile_disc\"('{0.25,0.5}'::double precision[]) WITHIN GROUP (ORDER BY c1) AS v FROM (VALUES ('2147483648'::integer), ('2147483648'::integer)) AS t(c1)",
        "22003",
    ),
    (
        "SELECT pg_catalog.\"percentile_disc\"(0.25::double precision) WITHIN GROUP (ORDER BY c1) AS v FROM (VALUES ('2147483648'::integer), ('2147483648'::integer)) AS t(c1)",
        "22003",
    ),
    ("SELECT pg_catalog.\"interval_out\"('178956971 years'::interval) AS v", "22008"),
    ("SELECT pg_catalog.\"numeric_mod\"(2.5::numeric, '0'::numeric) AS v", "22012"),
    ("SELECT pg_catalog.\"numrange\"(2.5::numeric, '0'::numeric) AS v", "22000"),
    (
        "SELECT pg_catalog.\"numrange\"(2.5::numeric, '-0.000000000000000000001'::numeric) AS v",
        "22000",
    ),
    (
        "SELECT pg_catalog.\"numrange\"('123456789012345678901234567890.123456789012345678901234567890'::numeric, 2.5::numeric) AS v",
        "22000",
    ),
    ("SELECT '2e-324'::double precision", "22003"),
    ("SELECT '1e-400'::double precision", "22003"),
    ("SELECT '1e309'::double precision", "22003"),
    ("SELECT '1.7976931348623159e+308'::double precision", "22003"),
    ("SELECT pg_catalog.interval_out('-178956971 years'::interval)", "22008"),
]

#: scripts/conformance/function_support.py's FLOAT_TOLERANCE.
_TOLERANCE = 1e-14
_FLOAT_WORDS = {"Infinity": "inf", "-Infinity": "-inf"}


def _el(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, float):
        return _float_text(value)
    return str(value)


def _float_text(value: float) -> str:
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    return repr(value)


def _same(want: str | None, got: object) -> bool:
    """PostgreSQL's text of a value against the engine's: a float by its value and its zero's sign
    (NaN equal to NaN, a double within the measure's tolerance), an array by its elements, bytes
    by their hex, a text as it is."""
    if want is None or got is None:
        return want is got
    if isinstance(got, bytes):
        return want == "\\x" + got.hex()
    if isinstance(got, float):
        expected = float(_FLOAT_WORDS.get(want, want))
        if math.isnan(expected) or math.isnan(got):
            return math.isnan(expected) and math.isnan(got)
        if expected == 0 or got == 0:
            return expected == got and math.copysign(1, expected) == math.copysign(1, got)
        return expected == got or abs(expected - got) <= _TOLERANCE * max(abs(expected), abs(got))
    if isinstance(got, (list, tuple)):
        if any(isinstance(v, float) for v in got):
            wanted = want.strip("{}").split(",")
            return len(wanted) == len(got) and all(
                _same(None if w == "NULL" else w, v) for w, v in zip(wanted, got)
            )
        return want == "{" + ",".join(_el(v) for v in got) + "}"
    if isinstance(got, bool):
        return want == ("t" if got else "f")
    return want == str(got)


@pytest.fixture(scope="module")
def runtime() -> ClickHouseFederationRuntime:
    return ClickHouseFederationRuntime.embedded()


def _answers(runtime: ClickHouseFederationRuntime, sql: str) -> list[object]:
    return [row[0] for row in runtime.run_sync(transpile(sql, "clickhouse")).iter_rows()]


@pytest.mark.parametrize(("sql", "want"), ANSWERED)
def test_clickhouse_answers_postgresql_s(runtime, sql, want):
    got = _answers(runtime, sql)
    assert len(got) == 1 and _same(want, got[0]), (want, got)


@pytest.mark.parametrize(("sql", "state"), RAISED)
def test_clickhouse_raises_postgresql_s_error(runtime, sql, state):
    with pytest.raises(Exception) as raised:
        _answers(runtime, sql)
    if isinstance(raised.value, (RowRefused, PostgresError)):
        assert raised.value.sqlstate == state
        return
    error = from_engine(raised.value)
    assert error is not None and error.sqlstate == state, str(raised.value)[:300]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT numeric_mod('123456789012345678901234567890.123456789012345678901234567890'::numeric,"
        " 2.5::numeric)",
        "SELECT numeric_mod(2.5::numeric,"
        " '123456789012345678901234567890.123456789012345678901234567890'::numeric)",
    ],
)
def test_a_numeric_past_38_digits_is_refused_by_name(runtime, sql):
    """The grid's stated limitation: a Decimal holds 38 digits, PostgreSQL's numeric more."""
    with pytest.raises(RowRefused) as raised:
        _answers(runtime, sql)
    assert raised.value.sqlstate == "0A000"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT pg_catalog.unnest('{{1,2},{3,4}}'::integer[]) AS v",
        "SELECT unnest(ARRAY[[1,2],[3,4]]) AS v",
    ],
)
def test_unnest_of_dimensions_answers_each_element(runtime, sql):
    assert _answers(runtime, sql) == [1, 2, 3, 4]


@pytest.mark.parametrize(
    ("sql", "want"),
    [
        ("SELECT cardinality(ARRAY[[1,2],[3,4]]) AS v", 4),
        ("SELECT array_to_string(ARRAY[[1,2],[3,NULL]], '-', '*') AS v", "1-2-3-*"),
    ],
)
def test_an_array_constructor_of_arrays_is_of_dimensions(runtime, sql, want):
    assert _answers(runtime, sql) == [want]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT sind(x) FROM t",
        "SELECT btnamecmp(a, b) FROM t",
        "SELECT cardinality('{{1,2},{3,4}}'::integer[])",
    ],
)
def test_trino_keeps_its_form(sql):
    written = transpile(sql, "trino")
    assert "reinterpretAsUInt64" not in written and "arrayFlatten" not in written
    assert "reinterpretAsUInt8" not in written
