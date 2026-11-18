# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a6c21-7d4e-4b95-9e02-1c5d7a8b4f63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse's rows of JSON built and of a json or jsonb value's members (REQ-2236; REQ-2171):
to_json, to_jsonb, json_build_array, jsonb_build_array, array_to_json, the json_agg family,
json_array_elements and json_each and their kin, the extract_path functions, json_build_object,
the object aggregates, jsonb_concat, jsonb_delete, row_to_json, and the functions computed of
literal arguments (jsonb's comparisons, hashes, containment, changes at a path, jsonb_pretty,
strip_nulls, json_object and the jsonpath functions). Each was refused by name on ClickHouse; each
call here is the functions measure's own (its samples and edges) or an edge the rows read, answered
on the embedded engine (chdb) and compared with the embedded PostgreSQL's recorded answer as the
measure compares JSON: parsed, so whitespace and key order are no difference."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.transpile import transpile

ANSWERED = [
    ('SELECT pg_catalog."array_to_json"(NULL::integer[]) AS v', [None]),
    ("SELECT pg_catalog.\"array_to_json\"('{1,2,3}'::integer[]) AS v", ["[1,2,3]"]),
    ("SELECT pg_catalog.\"array_to_json\"('{}'::integer[]) AS v", ["[]"]),
    ("SELECT pg_catalog.\"array_to_json\"('{-7,NULL,42}'::integer[]) AS v", ["[-7,null,42]"]),
    (
        "SELECT pg_catalog.\"array_to_json\"('{2147483647,-2147483648}'::integer[]) AS v LIMIT 1001",
        ["[2147483647,-2147483648]"],
    ),
    ("SELECT pg_catalog.\"array_to_json\"('{NULL}'::integer[]) AS v LIMIT 1001", ["[null]"]),
    (
        "SELECT pg_catalog.\"array_to_json\"('{{1,2},{3,4}}'::integer[]) AS v LIMIT 1001",
        ["[[1,2],[3,4]]"],
    ),
    ('SELECT pg_catalog."array_to_json"(NULL::integer[], NULL::boolean) AS v', [None]),
    (
        "SELECT pg_catalog.\"array_to_json\"('{1,2,3}'::integer[], true::boolean) AS v",
        ["[1,\n 2,\n 3]"],
    ),
    ("SELECT pg_catalog.\"array_to_json\"('{}'::integer[], false::boolean) AS v", ["[]"]),
    (
        "SELECT pg_catalog.\"array_to_json\"('{-7,NULL,42}'::integer[], true::boolean) AS v",
        ["[-7,\n null,\n 42]"],
    ),
    ('SELECT pg_catalog."array_to_json"(NULL::integer[], true::boolean) AS v LIMIT 1001', [None]),
    (
        "SELECT pg_catalog.\"array_to_json\"('{1,2,3}'::integer[], NULL::boolean) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"array_to_json\"('{2147483647,-2147483648}'::integer[], true::boolean) AS v LIMIT 1001",
        ["[2147483647,\n -2147483648]"],
    ),
    (
        "SELECT pg_catalog.\"array_to_json\"('{NULL}'::integer[], true::boolean) AS v LIMIT 1001",
        ["[null]"],
    ),
    (
        "SELECT pg_catalog.\"array_to_json\"('{{1,2},{3,4}}'::integer[], true::boolean) AS v LIMIT 1001",
        ["[[1,2],\n [3,4]]"],
    ),
    ('SELECT pg_catalog."json_agg"(NULL::integer) AS v', ["[null]"]),
    (
        'SELECT pg_catalog."json_agg"(c1 ORDER BY c1) AS v FROM (VALUES (3::integer), ((-7)::integer), (42::integer)) AS t(c1)',
        ["[-7, 3, 42]"],
    ),
    (
        'SELECT pg_catalog."json_agg"(c1 ORDER BY c1) AS v FROM (VALUES (NULL::integer), (NULL::integer)) AS t(c1) LIMIT 1001',
        ["[null, null]"],
    ),
    (
        "SELECT pg_catalog.\"json_agg\"(c1 ORDER BY c1) AS v FROM (VALUES ('-2147483648'::integer), ('-2147483648'::integer)) AS t(c1) LIMIT 1001",
        ["[-2147483648, -2147483648]"],
    ),
    (
        "SELECT pg_catalog.\"json_agg\"(c1 ORDER BY c1) AS v FROM (VALUES ('2147483647'::integer), ('2147483647'::integer)) AS t(c1) LIMIT 1001",
        ["[2147483647, 2147483647]"],
    ),
    (
        "SELECT pg_catalog.\"json_agg\"(c1 ORDER BY c1) AS v FROM (VALUES ('0'::integer), ('0'::integer)) AS t(c1) LIMIT 1001",
        ["[0, 0]"],
    ),
    ('SELECT pg_catalog."json_agg_strict"(NULL::integer) AS v', ["[]"]),
    (
        'SELECT pg_catalog."json_agg_strict"(c1 ORDER BY c1) AS v FROM (VALUES (3::integer), ((-7)::integer), (42::integer)) AS t(c1)',
        ["[-7, 3, 42]"],
    ),
    (
        'SELECT pg_catalog."json_agg_strict"(c1 ORDER BY c1) AS v FROM (VALUES (NULL::integer), (NULL::integer)) AS t(c1) LIMIT 1001',
        ["[]"],
    ),
    (
        "SELECT pg_catalog.\"json_agg_strict\"(c1 ORDER BY c1) AS v FROM (VALUES ('-2147483648'::integer), ('-2147483648'::integer)) AS t(c1) LIMIT 1001",
        ["[-2147483648, -2147483648]"],
    ),
    (
        "SELECT pg_catalog.\"json_agg_strict\"(c1 ORDER BY c1) AS v FROM (VALUES ('2147483647'::integer), ('2147483647'::integer)) AS t(c1) LIMIT 1001",
        ["[2147483647, 2147483647]"],
    ),
    (
        "SELECT pg_catalog.\"json_agg_strict\"(c1 ORDER BY c1) AS v FROM (VALUES ('0'::integer), ('0'::integer)) AS t(c1) LIMIT 1001",
        ["[0, 0]"],
    ),
    ('SELECT pg_catalog."json_array_elements"(NULL::json) AS v', []),
    (
        'SELECT pg_catalog."json_array_elements"(\'[1, "two", null]\'::json) AS v',
        ["1", '"two"', "null"],
    ),
    ("SELECT pg_catalog.\"json_array_elements\"('[]'::json) AS v LIMIT 1001", []),
    ('SELECT pg_catalog."json_array_elements_text"(NULL::json) AS v', []),
    (
        'SELECT pg_catalog."json_array_elements_text"(\'[1, "two", null]\'::json) AS v',
        ["1", "two", None],
    ),
    ("SELECT pg_catalog.\"json_array_elements_text\"('[]'::json) AS v LIMIT 1001", []),
    ('SELECT pg_catalog."json_build_array"() AS v', ["[]"]),
    ('SELECT pg_catalog."json_build_array"(NULL::integer) AS v', ["[null]"]),
    ('SELECT pg_catalog."json_build_array"(3::integer) AS v', ["[3]"]),
    ('SELECT pg_catalog."json_build_array"((-7)::integer) AS v', ["[-7]"]),
    ('SELECT pg_catalog."json_build_array"(42::integer) AS v', ["[42]"]),
    (
        "SELECT pg_catalog.\"json_build_array\"('-2147483648'::integer) AS v LIMIT 1001",
        ["[-2147483648]"],
    ),
    (
        "SELECT pg_catalog.\"json_build_array\"('2147483647'::integer) AS v LIMIT 1001",
        ["[2147483647]"],
    ),
    ("SELECT pg_catalog.\"json_build_array\"('0'::integer) AS v LIMIT 1001", ["[0]"]),
    ('SELECT pg_catalog."json_build_object"() AS v', ["{}"]),
    ('SELECT pg_catalog."json_build_object"(1::integer, 2::integer) AS v', ['{"1" : 2}']),
    ("SELECT pg_catalog.\"json_build_object\"('a'::text, 3::integer) AS v", ['{"a" : 3}']),
    ("SELECT pg_catalog.\"json_build_object\"('k'::text, NULL::integer) AS v", ['{"k" : null}']),
    ('SELECT pg_catalog."json_each"(NULL::json) AS v', []),
    (
        'SELECT pg_catalog."json_each"(\'{"a": 1, "b": [1, 2]}\'::json) AS v',
        ["(a,1)", '(b,"[1, 2]")'],
    ),
    ("SELECT pg_catalog.\"json_each\"('{}'::json) AS v LIMIT 1001", []),
    ('SELECT pg_catalog."json_each_text"(NULL::json) AS v', []),
    (
        'SELECT pg_catalog."json_each_text"(\'{"a": 1, "b": [1, 2]}\'::json) AS v',
        ["(a,1)", '(b,"[1, 2]")'],
    ),
    ("SELECT pg_catalog.\"json_each_text\"('{}'::json) AS v LIMIT 1001", []),
    ('SELECT pg_catalog."json_extract_path"(NULL::json, VARIADIC NULL::text[]) AS v', [None]),
    (
        'SELECT pg_catalog."json_extract_path"(\'{"a": 1, "b": [1, 2]}\'::json, VARIADIC \'{a}\'::text[]) AS v',
        ["1"],
    ),
    (
        "SELECT pg_catalog.\"json_extract_path\"('[1, \"two\", null]'::json, VARIADIC '{0}'::text[]) AS v",
        ["1"],
    ),
    ("SELECT pg_catalog.\"json_extract_path\"('3'::json, VARIADIC '{a}'::text[]) AS v", [None]),
    (
        "SELECT pg_catalog.\"json_extract_path\"(NULL::json, VARIADIC '{a}'::text[]) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"json_extract_path\"('{}'::json, VARIADIC '{a}'::text[]) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"json_extract_path\"('[]'::json, VARIADIC '{a}'::text[]) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"json_extract_path\"('\"日本語😀\"'::json, VARIADIC '{a}'::text[]) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"json_extract_path\"('12345678901234567890123456789'::json, VARIADIC '{a}'::text[]) AS v LIMIT 1001",
        [None],
    ),
    ('SELECT pg_catalog."json_extract_path_text"(NULL::json, VARIADIC NULL::text[]) AS v', [None]),
    (
        'SELECT pg_catalog."json_extract_path_text"(\'{"a": 1, "b": [1, 2]}\'::json, VARIADIC \'{a}\'::text[]) AS v',
        ["1"],
    ),
    (
        "SELECT pg_catalog.\"json_extract_path_text\"('[1, \"two\", null]'::json, VARIADIC '{0}'::text[]) AS v",
        ["1"],
    ),
    (
        "SELECT pg_catalog.\"json_extract_path_text\"('3'::json, VARIADIC '{a}'::text[]) AS v",
        [None],
    ),
    (
        "SELECT pg_catalog.\"json_extract_path_text\"(NULL::json, VARIADIC '{a}'::text[]) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"json_extract_path_text\"('{}'::json, VARIADIC '{a}'::text[]) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"json_extract_path_text\"('[]'::json, VARIADIC '{a}'::text[]) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"json_extract_path_text\"('\"日本語😀\"'::json, VARIADIC '{a}'::text[]) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"json_extract_path_text\"('12345678901234567890123456789'::json, VARIADIC '{a}'::text[]) AS v LIMIT 1001",
        [None],
    ),
    ('SELECT pg_catalog."json_object"(NULL::text[]) AS v', [None]),
    ("SELECT pg_catalog.\"json_object\"('{}'::text[]) AS v", ["{}"]),
    (
        "SELECT pg_catalog.\"json_object\"('{{a,b},{c,d}}'::text[]) AS v LIMIT 1001",
        ['{"a" : "b", "c" : "d"}'],
    ),
    ('SELECT pg_catalog."json_object"(NULL::text[], NULL::text[]) AS v', [None]),
    (
        "SELECT pg_catalog.\"json_object\"('{a,b,c}'::text[], '{a,b,c}'::text[]) AS v",
        ['{"a" : "a", "b" : "b", "c" : "c"}'],
    ),
    ("SELECT pg_catalog.\"json_object\"('{}'::text[], '{}'::text[]) AS v", ["{}"]),
    ("SELECT pg_catalog.\"json_object\"(NULL::text[], '{a,b,c}'::text[]) AS v LIMIT 1001", [None]),
    ("SELECT pg_catalog.\"json_object\"('{a,b,c}'::text[], NULL::text[]) AS v LIMIT 1001", [None]),
    (
        "SELECT pg_catalog.\"json_object\"('{\"\",日本語😀,é}'::text[], '{a,b,c}'::text[]) AS v LIMIT 1001",
        ['{"" : "a", "日本語😀" : "b", "é" : "c"}'],
    ),
    (
        "SELECT pg_catalog.\"json_object\"('{a,b,c}'::text[], '{\"\",日本語😀,é}'::text[]) AS v LIMIT 1001",
        ['{"a" : "", "b" : "日本語😀", "c" : "é"}'],
    ),
    (
        'SELECT pg_catalog."json_object_agg"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, 3::integer), ((-7)::integer, (-7)::integer), (42::integer, 42::integer)) AS t(c1, c2)',
        ['{ "-7" : -7, "3" : 3, "42" : 42 }'],
    ),
    (
        'SELECT pg_catalog."json_object_agg"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, NULL::integer), (3::integer, NULL::integer)) AS t(c1, c2) LIMIT 1001',
        ['{ "3" : null, "3" : null }'],
    ),
    (
        "SELECT pg_catalog.\"json_object_agg\"(c1, c2 ORDER BY c1) AS v FROM (VALUES ('-2147483648'::integer, 3::integer), ('-2147483648'::integer, 3::integer)) AS t(c1, c2) LIMIT 1001",
        ['{ "-2147483648" : 3, "-2147483648" : 3 }'],
    ),
    (
        "SELECT pg_catalog.\"json_object_agg\"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, '-2147483648'::integer), (3::integer, '-2147483648'::integer)) AS t(c1, c2) LIMIT 1001",
        ['{ "3" : -2147483648, "3" : -2147483648 }'],
    ),
    (
        "SELECT pg_catalog.\"json_object_agg\"(c1, c2 ORDER BY c1) AS v FROM (VALUES ('2147483647'::integer, 3::integer), ('2147483647'::integer, 3::integer)) AS t(c1, c2) LIMIT 1001",
        ['{ "2147483647" : 3, "2147483647" : 3 }'],
    ),
    (
        "SELECT pg_catalog.\"json_object_agg\"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, '2147483647'::integer), (3::integer, '2147483647'::integer)) AS t(c1, c2) LIMIT 1001",
        ['{ "3" : 2147483647, "3" : 2147483647 }'],
    ),
    (
        "SELECT pg_catalog.\"json_object_agg\"(c1, c2 ORDER BY c1) AS v FROM (VALUES ('0'::integer, 3::integer), ('0'::integer, 3::integer)) AS t(c1, c2) LIMIT 1001",
        ['{ "0" : 3, "0" : 3 }'],
    ),
    (
        "SELECT pg_catalog.\"json_object_agg\"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, '0'::integer), (3::integer, '0'::integer)) AS t(c1, c2) LIMIT 1001",
        ['{ "3" : 0, "3" : 0 }'],
    ),
    (
        'SELECT pg_catalog."json_object_agg_strict"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, 3::integer), ((-7)::integer, (-7)::integer), (42::integer, 42::integer)) AS t(c1, c2)',
        ['{ "-7" : -7, "3" : 3, "42" : 42 }'],
    ),
    (
        'SELECT pg_catalog."json_object_agg_strict"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, NULL::integer), (3::integer, NULL::integer)) AS t(c1, c2) LIMIT 1001',
        ["{  }"],
    ),
    (
        "SELECT pg_catalog.\"json_object_agg_strict\"(c1, c2 ORDER BY c1) AS v FROM (VALUES ('-2147483648'::integer, 3::integer), ('-2147483648'::integer, 3::integer)) AS t(c1, c2) LIMIT 1001",
        ['{ "-2147483648" : 3, "-2147483648" : 3 }'],
    ),
    (
        "SELECT pg_catalog.\"json_object_agg_strict\"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, '-2147483648'::integer), (3::integer, '-2147483648'::integer)) AS t(c1, c2) LIMIT 1001",
        ['{ "3" : -2147483648, "3" : -2147483648 }'],
    ),
    (
        "SELECT pg_catalog.\"json_object_agg_strict\"(c1, c2 ORDER BY c1) AS v FROM (VALUES ('2147483647'::integer, 3::integer), ('2147483647'::integer, 3::integer)) AS t(c1, c2) LIMIT 1001",
        ['{ "2147483647" : 3, "2147483647" : 3 }'],
    ),
    (
        "SELECT pg_catalog.\"json_object_agg_strict\"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, '2147483647'::integer), (3::integer, '2147483647'::integer)) AS t(c1, c2) LIMIT 1001",
        ['{ "3" : 2147483647, "3" : 2147483647 }'],
    ),
    (
        "SELECT pg_catalog.\"json_object_agg_strict\"(c1, c2 ORDER BY c1) AS v FROM (VALUES ('0'::integer, 3::integer), ('0'::integer, 3::integer)) AS t(c1, c2) LIMIT 1001",
        ['{ "0" : 3, "0" : 3 }'],
    ),
    (
        "SELECT pg_catalog.\"json_object_agg_strict\"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, '0'::integer), (3::integer, '0'::integer)) AS t(c1, c2) LIMIT 1001",
        ['{ "3" : 0, "3" : 0 }'],
    ),
    (
        'SELECT pg_catalog."json_object_agg_unique"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, 3::integer), ((-7)::integer, (-7)::integer), (42::integer, 42::integer)) AS t(c1, c2)',
        ['{ "-7" : -7, "3" : 3, "42" : 42 }'],
    ),
    (
        'SELECT pg_catalog."json_object_agg_unique_strict"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, 3::integer), ((-7)::integer, (-7)::integer), (42::integer, 42::integer)) AS t(c1, c2)',
        ['{ "-7" : -7, "3" : 3, "42" : 42 }'],
    ),
    ('SELECT pg_catalog."json_strip_nulls"(NULL::json) AS v', [None]),
    (
        'SELECT pg_catalog."json_strip_nulls"(\'{"a": 1, "b": [1, 2]}\'::json) AS v',
        ['{"a":1,"b":[1,2]}'],
    ),
    ('SELECT pg_catalog."json_strip_nulls"(\'[1, "two", null]\'::json) AS v', ['[1,"two",null]']),
    ("SELECT pg_catalog.\"json_strip_nulls\"('3'::json) AS v", ["3"]),
    ("SELECT pg_catalog.\"json_strip_nulls\"('{}'::json) AS v LIMIT 1001", ["{}"]),
    ("SELECT pg_catalog.\"json_strip_nulls\"('[]'::json) AS v LIMIT 1001", ["[]"]),
    ('SELECT pg_catalog."json_strip_nulls"(\'"日本語😀"\'::json) AS v LIMIT 1001', ['"日本語😀"']),
    (
        "SELECT pg_catalog.\"json_strip_nulls\"('12345678901234567890123456789'::json) AS v LIMIT 1001",
        ["12345678901234567890123456789"],
    ),
    ('SELECT pg_catalog."jsonb_agg"(NULL::integer) AS v', ["[null]"]),
    (
        'SELECT pg_catalog."jsonb_agg"(c1 ORDER BY c1) AS v FROM (VALUES (3::integer), ((-7)::integer), (42::integer)) AS t(c1)',
        ["[-7, 3, 42]"],
    ),
    (
        'SELECT pg_catalog."jsonb_agg"(c1 ORDER BY c1) AS v FROM (VALUES (NULL::integer), (NULL::integer)) AS t(c1) LIMIT 1001',
        ["[null, null]"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_agg\"(c1 ORDER BY c1) AS v FROM (VALUES ('-2147483648'::integer), ('-2147483648'::integer)) AS t(c1) LIMIT 1001",
        ["[-2147483648, -2147483648]"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_agg\"(c1 ORDER BY c1) AS v FROM (VALUES ('2147483647'::integer), ('2147483647'::integer)) AS t(c1) LIMIT 1001",
        ["[2147483647, 2147483647]"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_agg\"(c1 ORDER BY c1) AS v FROM (VALUES ('0'::integer), ('0'::integer)) AS t(c1) LIMIT 1001",
        ["[0, 0]"],
    ),
    ('SELECT pg_catalog."jsonb_agg_strict"(NULL::integer) AS v', ["[]"]),
    (
        'SELECT pg_catalog."jsonb_agg_strict"(c1 ORDER BY c1) AS v FROM (VALUES (3::integer), ((-7)::integer), (42::integer)) AS t(c1)',
        ["[-7, 3, 42]"],
    ),
    (
        'SELECT pg_catalog."jsonb_agg_strict"(c1 ORDER BY c1) AS v FROM (VALUES (NULL::integer), (NULL::integer)) AS t(c1) LIMIT 1001',
        ["[]"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_agg_strict\"(c1 ORDER BY c1) AS v FROM (VALUES ('-2147483648'::integer), ('-2147483648'::integer)) AS t(c1) LIMIT 1001",
        ["[-2147483648, -2147483648]"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_agg_strict\"(c1 ORDER BY c1) AS v FROM (VALUES ('2147483647'::integer), ('2147483647'::integer)) AS t(c1) LIMIT 1001",
        ["[2147483647, 2147483647]"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_agg_strict\"(c1 ORDER BY c1) AS v FROM (VALUES ('0'::integer), ('0'::integer)) AS t(c1) LIMIT 1001",
        ["[0, 0]"],
    ),
    ('SELECT pg_catalog."jsonb_array_elements"(NULL::jsonb) AS v', []),
    (
        'SELECT pg_catalog."jsonb_array_elements"(\'[1, "two", null]\'::jsonb) AS v',
        ["1", '"two"', "null"],
    ),
    ("SELECT pg_catalog.\"jsonb_array_elements\"('[]'::jsonb) AS v LIMIT 1001", []),
    ('SELECT pg_catalog."jsonb_array_elements_text"(NULL::jsonb) AS v', []),
    (
        'SELECT pg_catalog."jsonb_array_elements_text"(\'[1, "two", null]\'::jsonb) AS v',
        ["1", "two", None],
    ),
    ("SELECT pg_catalog.\"jsonb_array_elements_text\"('[]'::jsonb) AS v LIMIT 1001", []),
    ('SELECT pg_catalog."jsonb_build_array"() AS v', ["[]"]),
    ('SELECT pg_catalog."jsonb_build_array"(NULL::integer) AS v', ["[null]"]),
    ('SELECT pg_catalog."jsonb_build_array"(3::integer) AS v', ["[3]"]),
    ('SELECT pg_catalog."jsonb_build_array"((-7)::integer) AS v', ["[-7]"]),
    ('SELECT pg_catalog."jsonb_build_array"(42::integer) AS v', ["[42]"]),
    (
        "SELECT pg_catalog.\"jsonb_build_array\"('-2147483648'::integer) AS v LIMIT 1001",
        ["[-2147483648]"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_build_array\"('2147483647'::integer) AS v LIMIT 1001",
        ["[2147483647]"],
    ),
    ("SELECT pg_catalog.\"jsonb_build_array\"('0'::integer) AS v LIMIT 1001", ["[0]"]),
    ('SELECT pg_catalog."jsonb_build_object"() AS v', ["{}"]),
    ('SELECT pg_catalog."jsonb_build_object"(1::integer, 2::integer) AS v', ['{"1": 2}']),
    ("SELECT pg_catalog.\"jsonb_build_object\"('a'::text, 3::integer) AS v", ['{"a": 3}']),
    ("SELECT pg_catalog.\"jsonb_build_object\"('k'::text, NULL::integer) AS v", ['{"k": null}']),
    ('SELECT pg_catalog."jsonb_cmp"(NULL::jsonb, NULL::jsonb) AS v', [None]),
    (
        'SELECT pg_catalog."jsonb_cmp"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v',
        ["0"],
    ),
    (
        'SELECT pg_catalog."jsonb_cmp"(\'[1, "two", null]\'::jsonb, \'[1, "two", null]\'::jsonb) AS v',
        ["0"],
    ),
    ("SELECT pg_catalog.\"jsonb_cmp\"('3'::jsonb, '3'::jsonb) AS v", ["0"]),
    (
        'SELECT pg_catalog."jsonb_cmp"(NULL::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_cmp"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::jsonb) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_cmp"(\'{}\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["-1"],
    ),
    (
        'SELECT pg_catalog."jsonb_cmp"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{}\'::jsonb) AS v LIMIT 1001',
        ["1"],
    ),
    (
        'SELECT pg_catalog."jsonb_cmp"(\'[]\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["-1"],
    ),
    (
        'SELECT pg_catalog."jsonb_cmp"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'[]\'::jsonb) AS v LIMIT 1001',
        ["1"],
    ),
    (
        'SELECT pg_catalog."jsonb_cmp"(\'"日本語😀"\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["-1"],
    ),
    (
        'SELECT pg_catalog."jsonb_cmp"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'"日本語😀"\'::jsonb) AS v LIMIT 1001',
        ["1"],
    ),
    ('SELECT pg_catalog."jsonb_concat"(NULL::jsonb, NULL::jsonb) AS v', [None]),
    (
        'SELECT pg_catalog."jsonb_concat"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v',
        ['{"a": 1, "b": [1, 2]}'],
    ),
    (
        'SELECT pg_catalog."jsonb_concat"(\'[1, "two", null]\'::jsonb, \'[1, "two", null]\'::jsonb) AS v',
        ['[1, "two", null, 1, "two", null]'],
    ),
    ("SELECT pg_catalog.\"jsonb_concat\"('3'::jsonb, '3'::jsonb) AS v", ["[3, 3]"]),
    (
        'SELECT pg_catalog."jsonb_concat"(NULL::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_concat"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::jsonb) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_concat"(\'{}\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ['{"a": 1, "b": [1, 2]}'],
    ),
    (
        'SELECT pg_catalog."jsonb_concat"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{}\'::jsonb) AS v LIMIT 1001',
        ['{"a": 1, "b": [1, 2]}'],
    ),
    (
        'SELECT pg_catalog."jsonb_concat"(\'[]\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ['[{"a": 1, "b": [1, 2]}]'],
    ),
    (
        'SELECT pg_catalog."jsonb_concat"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'[]\'::jsonb) AS v LIMIT 1001',
        ['[{"a": 1, "b": [1, 2]}]'],
    ),
    (
        'SELECT pg_catalog."jsonb_concat"(\'"日本語😀"\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ['["日本語😀", {"a": 1, "b": [1, 2]}]'],
    ),
    (
        'SELECT pg_catalog."jsonb_concat"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'"日本語😀"\'::jsonb) AS v LIMIT 1001',
        ['[{"a": 1, "b": [1, 2]}, "日本語😀"]'],
    ),
    ('SELECT pg_catalog."jsonb_contained"(NULL::jsonb, NULL::jsonb) AS v', [None]),
    (
        'SELECT pg_catalog."jsonb_contained"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v',
        ["t"],
    ),
    (
        'SELECT pg_catalog."jsonb_contained"(\'[1, "two", null]\'::jsonb, \'[1, "two", null]\'::jsonb) AS v',
        ["t"],
    ),
    ("SELECT pg_catalog.\"jsonb_contained\"('3'::jsonb, '3'::jsonb) AS v", ["t"]),
    (
        'SELECT pg_catalog."jsonb_contained"(NULL::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_contained"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::jsonb) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_contained"(\'{}\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["t"],
    ),
    (
        'SELECT pg_catalog."jsonb_contained"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{}\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_contained"(\'[]\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_contained"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'[]\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_contained"(\'"日本語😀"\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_contained"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'"日本語😀"\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    ('SELECT pg_catalog."jsonb_contains"(NULL::jsonb, NULL::jsonb) AS v', [None]),
    (
        'SELECT pg_catalog."jsonb_contains"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v',
        ["t"],
    ),
    (
        'SELECT pg_catalog."jsonb_contains"(\'[1, "two", null]\'::jsonb, \'[1, "two", null]\'::jsonb) AS v',
        ["t"],
    ),
    ("SELECT pg_catalog.\"jsonb_contains\"('3'::jsonb, '3'::jsonb) AS v", ["t"]),
    (
        'SELECT pg_catalog."jsonb_contains"(NULL::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_contains"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::jsonb) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_contains"(\'{}\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_contains"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{}\'::jsonb) AS v LIMIT 1001',
        ["t"],
    ),
    (
        'SELECT pg_catalog."jsonb_contains"(\'[]\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_contains"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'[]\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_contains"(\'"日本語😀"\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_contains"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'"日本語😀"\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    ('SELECT pg_catalog."jsonb_delete"(NULL::jsonb, VARIADIC NULL::text[]) AS v', [None]),
    (
        'SELECT pg_catalog."jsonb_delete"(\'{"a": 1, "b": [1, 2]}\'::jsonb, VARIADIC \'{a,b,c}\'::text[]) AS v',
        ["{}"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_delete\"('[1, \"two\", null]'::jsonb, VARIADIC '{}'::text[]) AS v",
        ['[1, "two", null]'],
    ),
    (
        "SELECT pg_catalog.\"jsonb_delete\"(NULL::jsonb, VARIADIC '{a,b,c}'::text[]) AS v LIMIT 1001",
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_delete"(\'{"a": 1, "b": [1, 2]}\'::jsonb, VARIADIC NULL::text[]) AS v LIMIT 1001',
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_delete\"('{}'::jsonb, VARIADIC '{a,b,c}'::text[]) AS v LIMIT 1001",
        ["{}"],
    ),
    (
        'SELECT pg_catalog."jsonb_delete"(\'{"a": 1, "b": [1, 2]}\'::jsonb, VARIADIC \'{"",日本語😀,é}\'::text[]) AS v LIMIT 1001',
        ['{"a": 1, "b": [1, 2]}'],
    ),
    (
        "SELECT pg_catalog.\"jsonb_delete\"('[]'::jsonb, VARIADIC '{a,b,c}'::text[]) AS v LIMIT 1001",
        ["[]"],
    ),
    (
        'SELECT pg_catalog."jsonb_delete"(\'{"a": 1, "b": [1, 2]}\'::jsonb, VARIADIC \'{NULL}\'::text[]) AS v LIMIT 1001',
        ['{"a": 1, "b": [1, 2]}'],
    ),
    ('SELECT pg_catalog."jsonb_delete"(NULL::jsonb, NULL::integer) AS v', [None]),
    (
        'SELECT pg_catalog."jsonb_delete"(\'[1, "two", null]\'::jsonb, (-7)::integer) AS v',
        ['[1, "two", null]'],
    ),
    ('SELECT pg_catalog."jsonb_delete"(NULL::jsonb, 3::integer) AS v LIMIT 1001', [None]),
    (
        'SELECT pg_catalog."jsonb_delete"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::integer) AS v LIMIT 1001',
        [None],
    ),
    ("SELECT pg_catalog.\"jsonb_delete\"('[]'::jsonb, 3::integer) AS v LIMIT 1001", ["[]"]),
    ('SELECT pg_catalog."jsonb_delete"(NULL::jsonb, NULL::text) AS v', [None]),
    (
        'SELECT pg_catalog."jsonb_delete"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'abc\'::text) AS v',
        ['{"a": 1, "b": [1, 2]}'],
    ),
    (
        "SELECT pg_catalog.\"jsonb_delete\"('[1, \"two\", null]'::jsonb, 'Hello World'::text) AS v",
        ['[1, "two", null]'],
    ),
    ("SELECT pg_catalog.\"jsonb_delete\"(NULL::jsonb, 'abc'::text) AS v LIMIT 1001", [None]),
    (
        'SELECT pg_catalog."jsonb_delete"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::text) AS v LIMIT 1001',
        [None],
    ),
    ("SELECT pg_catalog.\"jsonb_delete\"('{}'::jsonb, 'abc'::text) AS v LIMIT 1001", ["{}"]),
    (
        'SELECT pg_catalog."jsonb_delete"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'\'::text) AS v LIMIT 1001',
        ['{"a": 1, "b": [1, 2]}'],
    ),
    ("SELECT pg_catalog.\"jsonb_delete\"('[]'::jsonb, 'abc'::text) AS v LIMIT 1001", ["[]"]),
    (
        'SELECT pg_catalog."jsonb_delete"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'日本語テキスト😀\'::text) AS v LIMIT 1001',
        ['{"a": 1, "b": [1, 2]}'],
    ),
    (
        'SELECT pg_catalog."jsonb_delete"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'été\'::text) AS v LIMIT 1001',
        ['{"a": 1, "b": [1, 2]}'],
    ),
    ('SELECT pg_catalog."jsonb_delete_path"(NULL::jsonb, NULL::text[]) AS v', [None]),
    (
        'SELECT pg_catalog."jsonb_delete_path"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{a,b,c}\'::text[]) AS v',
        ['{"a": 1, "b": [1, 2]}'],
    ),
    (
        "SELECT pg_catalog.\"jsonb_delete_path\"('[1, \"two\", null]'::jsonb, '{}'::text[]) AS v",
        ['[1, "two", null]'],
    ),
    (
        "SELECT pg_catalog.\"jsonb_delete_path\"(NULL::jsonb, '{a,b,c}'::text[]) AS v LIMIT 1001",
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_delete_path"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::text[]) AS v LIMIT 1001',
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_delete_path\"('{}'::jsonb, '{a,b,c}'::text[]) AS v LIMIT 1001",
        ["{}"],
    ),
    (
        'SELECT pg_catalog."jsonb_delete_path"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{"",日本語😀,é}\'::text[]) AS v LIMIT 1001',
        ['{"a": 1, "b": [1, 2]}'],
    ),
    (
        "SELECT pg_catalog.\"jsonb_delete_path\"('[]'::jsonb, '{a,b,c}'::text[]) AS v LIMIT 1001",
        ["[]"],
    ),
    ('SELECT pg_catalog."jsonb_each"(NULL::jsonb) AS v', []),
    (
        'SELECT pg_catalog."jsonb_each"(\'{"a": 1, "b": [1, 2]}\'::jsonb) AS v',
        ["(a,1)", '(b,"[1, 2]")'],
    ),
    ("SELECT pg_catalog.\"jsonb_each\"('{}'::jsonb) AS v LIMIT 1001", []),
    ('SELECT pg_catalog."jsonb_each_text"(NULL::jsonb) AS v', []),
    (
        'SELECT pg_catalog."jsonb_each_text"(\'{"a": 1, "b": [1, 2]}\'::jsonb) AS v',
        ["(a,1)", '(b,"[1, 2]")'],
    ),
    ("SELECT pg_catalog.\"jsonb_each_text\"('{}'::jsonb) AS v LIMIT 1001", []),
    ('SELECT pg_catalog."jsonb_eq"(NULL::jsonb, NULL::jsonb) AS v', [None]),
    (
        'SELECT pg_catalog."jsonb_eq"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v',
        ["t"],
    ),
    (
        'SELECT pg_catalog."jsonb_eq"(\'[1, "two", null]\'::jsonb, \'[1, "two", null]\'::jsonb) AS v',
        ["t"],
    ),
    ("SELECT pg_catalog.\"jsonb_eq\"('3'::jsonb, '3'::jsonb) AS v", ["t"]),
    (
        'SELECT pg_catalog."jsonb_eq"(NULL::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_eq"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::jsonb) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_eq"(\'{}\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_eq"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{}\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_eq"(\'[]\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_eq"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'[]\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_eq"(\'"日本語😀"\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_eq"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'"日本語😀"\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    ('SELECT pg_catalog."jsonb_extract_path"(NULL::jsonb, VARIADIC NULL::text[]) AS v', [None]),
    (
        'SELECT pg_catalog."jsonb_extract_path"(\'{"a": 1, "b": [1, 2]}\'::jsonb, VARIADIC \'{a}\'::text[]) AS v',
        ["1"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_extract_path\"('[1, \"two\", null]'::jsonb, VARIADIC '{0}'::text[]) AS v",
        ["1"],
    ),
    ("SELECT pg_catalog.\"jsonb_extract_path\"('3'::jsonb, VARIADIC '{a}'::text[]) AS v", [None]),
    (
        "SELECT pg_catalog.\"jsonb_extract_path\"(NULL::jsonb, VARIADIC '{a}'::text[]) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_extract_path\"('{}'::jsonb, VARIADIC '{a}'::text[]) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_extract_path\"('[]'::jsonb, VARIADIC '{a}'::text[]) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_extract_path\"('\"日本語😀\"'::jsonb, VARIADIC '{a}'::text[]) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_extract_path\"('12345678901234567890123456789'::jsonb, VARIADIC '{a}'::text[]) AS v LIMIT 1001",
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_extract_path_text"(NULL::jsonb, VARIADIC NULL::text[]) AS v',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_extract_path_text"(\'{"a": 1, "b": [1, 2]}\'::jsonb, VARIADIC \'{a}\'::text[]) AS v',
        ["1"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_extract_path_text\"('[1, \"two\", null]'::jsonb, VARIADIC '{0}'::text[]) AS v",
        ["1"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_extract_path_text\"('3'::jsonb, VARIADIC '{a}'::text[]) AS v",
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_extract_path_text\"(NULL::jsonb, VARIADIC '{a}'::text[]) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_extract_path_text\"('{}'::jsonb, VARIADIC '{a}'::text[]) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_extract_path_text\"('[]'::jsonb, VARIADIC '{a}'::text[]) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_extract_path_text\"('\"日本語😀\"'::jsonb, VARIADIC '{a}'::text[]) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_extract_path_text\"('12345678901234567890123456789'::jsonb, VARIADIC '{a}'::text[]) AS v LIMIT 1001",
        [None],
    ),
    ('SELECT pg_catalog."jsonb_ge"(NULL::jsonb, NULL::jsonb) AS v', [None]),
    (
        'SELECT pg_catalog."jsonb_ge"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v',
        ["t"],
    ),
    (
        'SELECT pg_catalog."jsonb_ge"(\'[1, "two", null]\'::jsonb, \'[1, "two", null]\'::jsonb) AS v',
        ["t"],
    ),
    ("SELECT pg_catalog.\"jsonb_ge\"('3'::jsonb, '3'::jsonb) AS v", ["t"]),
    (
        'SELECT pg_catalog."jsonb_ge"(NULL::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_ge"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::jsonb) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_ge"(\'{}\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_ge"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{}\'::jsonb) AS v LIMIT 1001',
        ["t"],
    ),
    (
        'SELECT pg_catalog."jsonb_ge"(\'[]\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_ge"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'[]\'::jsonb) AS v LIMIT 1001',
        ["t"],
    ),
    (
        'SELECT pg_catalog."jsonb_ge"(\'"日本語😀"\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_ge"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'"日本語😀"\'::jsonb) AS v LIMIT 1001',
        ["t"],
    ),
    ('SELECT pg_catalog."jsonb_gt"(NULL::jsonb, NULL::jsonb) AS v', [None]),
    (
        'SELECT pg_catalog."jsonb_gt"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v',
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_gt"(\'[1, "two", null]\'::jsonb, \'[1, "two", null]\'::jsonb) AS v',
        ["f"],
    ),
    ("SELECT pg_catalog.\"jsonb_gt\"('3'::jsonb, '3'::jsonb) AS v", ["f"]),
    (
        'SELECT pg_catalog."jsonb_gt"(NULL::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_gt"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::jsonb) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_gt"(\'{}\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_gt"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{}\'::jsonb) AS v LIMIT 1001',
        ["t"],
    ),
    (
        'SELECT pg_catalog."jsonb_gt"(\'[]\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_gt"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'[]\'::jsonb) AS v LIMIT 1001',
        ["t"],
    ),
    (
        'SELECT pg_catalog."jsonb_gt"(\'"日本語😀"\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_gt"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'"日本語😀"\'::jsonb) AS v LIMIT 1001',
        ["t"],
    ),
    ('SELECT pg_catalog."jsonb_hash"(NULL::jsonb) AS v', [None]),
    ('SELECT pg_catalog."jsonb_hash"(\'{"a": 1, "b": [1, 2]}\'::jsonb) AS v', ["-228480614"]),
    ('SELECT pg_catalog."jsonb_hash"(\'[1, "two", null]\'::jsonb) AS v', ["-2042232898"]),
    ("SELECT pg_catalog.\"jsonb_hash\"('3'::jsonb) AS v", ["-2006842805"]),
    ("SELECT pg_catalog.\"jsonb_hash\"('{}'::jsonb) AS v LIMIT 1001", ["0"]),
    ("SELECT pg_catalog.\"jsonb_hash\"('[]'::jsonb) AS v LIMIT 1001", ["0"]),
    ('SELECT pg_catalog."jsonb_hash"(\'"日本語😀"\'::jsonb) AS v LIMIT 1001', ["736765119"]),
    (
        "SELECT pg_catalog.\"jsonb_hash\"('12345678901234567890123456789'::jsonb) AS v LIMIT 1001",
        ["-1869070166"],
    ),
    ('SELECT pg_catalog."jsonb_hash_extended"(NULL::jsonb, NULL::bigint) AS v', [None]),
    (
        'SELECT pg_catalog."jsonb_hash_extended"(\'{"a": 1, "b": [1, 2]}\'::jsonb, 3::bigint) AS v',
        ["-5510033560091052886"],
    ),
    (
        'SELECT pg_catalog."jsonb_hash_extended"(\'[1, "two", null]\'::jsonb, (-7)::bigint) AS v',
        ["7874654012264134311"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_hash_extended\"('3'::jsonb, 5000000000::bigint) AS v",
        ["2373355654910935665"],
    ),
    ('SELECT pg_catalog."jsonb_hash_extended"(NULL::jsonb, 3::bigint) AS v LIMIT 1001', [None]),
    (
        'SELECT pg_catalog."jsonb_hash_extended"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::bigint) AS v LIMIT 1001',
        [None],
    ),
    ("SELECT pg_catalog.\"jsonb_hash_extended\"('{}'::jsonb, 3::bigint) AS v LIMIT 1001", ["3"]),
    (
        'SELECT pg_catalog."jsonb_hash_extended"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'-9223372036854775808\'::bigint) AS v LIMIT 1001',
        ["5236938222197272216"],
    ),
    ("SELECT pg_catalog.\"jsonb_hash_extended\"('[]'::jsonb, 3::bigint) AS v LIMIT 1001", ["3"]),
    (
        'SELECT pg_catalog."jsonb_hash_extended"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'9223372036854775807\'::bigint) AS v LIMIT 1001',
        ["-6034227564380837915"],
    ),
    (
        'SELECT pg_catalog."jsonb_hash_extended"(\'"日本語😀"\'::jsonb, 3::bigint) AS v LIMIT 1001',
        ["-181794054192910347"],
    ),
    (
        'SELECT pg_catalog."jsonb_hash_extended"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'0\'::bigint) AS v LIMIT 1001',
        ["-7058221187140048486"],
    ),
    (
        'SELECT pg_catalog."jsonb_insert"(NULL::jsonb, NULL::text[], NULL::jsonb, NULL::boolean) AS v',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_insert"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{a,b,c}\'::text[], \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v',
        ['{"a": 1, "b": [1, 2]}'],
    ),
    (
        "SELECT pg_catalog.\"jsonb_insert\"('[1, \"two\", null]'::jsonb, '{}'::text[], '[1, \"two\", null]'::jsonb, false::boolean) AS v",
        ['[1, "two", null]'],
    ),
    (
        'SELECT pg_catalog."jsonb_insert"(NULL::jsonb, \'{a,b,c}\'::text[], \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_insert"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::text[], \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_insert"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{a,b,c}\'::text[], NULL::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_insert"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{a,b,c}\'::text[], \'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_insert\"('{}'::jsonb, '{a,b,c}'::text[], '{\"a\": 1, \"b\": [1, 2]}'::jsonb, true::boolean) AS v LIMIT 1001",
        ["{}"],
    ),
    (
        'SELECT pg_catalog."jsonb_insert"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{"",日本語😀,é}\'::text[], \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v LIMIT 1001',
        ['{"a": 1, "b": [1, 2]}'],
    ),
    (
        "SELECT pg_catalog.\"jsonb_insert\"('{\"a\": 1, \"b\": [1, 2]}'::jsonb, '{a,b,c}'::text[], '{}'::jsonb, true::boolean) AS v LIMIT 1001",
        ['{"a": 1, "b": [1, 2]}'],
    ),
    ('SELECT pg_catalog."jsonb_le"(NULL::jsonb, NULL::jsonb) AS v', [None]),
    (
        'SELECT pg_catalog."jsonb_le"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v',
        ["t"],
    ),
    (
        'SELECT pg_catalog."jsonb_le"(\'[1, "two", null]\'::jsonb, \'[1, "two", null]\'::jsonb) AS v',
        ["t"],
    ),
    ("SELECT pg_catalog.\"jsonb_le\"('3'::jsonb, '3'::jsonb) AS v", ["t"]),
    (
        'SELECT pg_catalog."jsonb_le"(NULL::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_le"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::jsonb) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_le"(\'{}\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["t"],
    ),
    (
        'SELECT pg_catalog."jsonb_le"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{}\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_le"(\'[]\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["t"],
    ),
    (
        'SELECT pg_catalog."jsonb_le"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'[]\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_le"(\'"日本語😀"\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["t"],
    ),
    (
        'SELECT pg_catalog."jsonb_le"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'"日本語😀"\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    ('SELECT pg_catalog."jsonb_lt"(NULL::jsonb, NULL::jsonb) AS v', [None]),
    (
        'SELECT pg_catalog."jsonb_lt"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v',
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_lt"(\'[1, "two", null]\'::jsonb, \'[1, "two", null]\'::jsonb) AS v',
        ["f"],
    ),
    ("SELECT pg_catalog.\"jsonb_lt\"('3'::jsonb, '3'::jsonb) AS v", ["f"]),
    (
        'SELECT pg_catalog."jsonb_lt"(NULL::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_lt"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::jsonb) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_lt"(\'{}\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["t"],
    ),
    (
        'SELECT pg_catalog."jsonb_lt"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{}\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_lt"(\'[]\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["t"],
    ),
    (
        'SELECT pg_catalog."jsonb_lt"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'[]\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_lt"(\'"日本語😀"\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["t"],
    ),
    (
        'SELECT pg_catalog."jsonb_lt"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'"日本語😀"\'::jsonb) AS v LIMIT 1001',
        ["f"],
    ),
    ('SELECT pg_catalog."jsonb_ne"(NULL::jsonb, NULL::jsonb) AS v', [None]),
    (
        'SELECT pg_catalog."jsonb_ne"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v',
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_ne"(\'[1, "two", null]\'::jsonb, \'[1, "two", null]\'::jsonb) AS v',
        ["f"],
    ),
    ("SELECT pg_catalog.\"jsonb_ne\"('3'::jsonb, '3'::jsonb) AS v", ["f"]),
    (
        'SELECT pg_catalog."jsonb_ne"(NULL::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_ne"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::jsonb) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_ne"(\'{}\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["t"],
    ),
    (
        'SELECT pg_catalog."jsonb_ne"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{}\'::jsonb) AS v LIMIT 1001',
        ["t"],
    ),
    (
        'SELECT pg_catalog."jsonb_ne"(\'[]\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["t"],
    ),
    (
        'SELECT pg_catalog."jsonb_ne"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'[]\'::jsonb) AS v LIMIT 1001',
        ["t"],
    ),
    (
        'SELECT pg_catalog."jsonb_ne"(\'"日本語😀"\'::jsonb, \'{"a": 1, "b": [1, 2]}\'::jsonb) AS v LIMIT 1001',
        ["t"],
    ),
    (
        'SELECT pg_catalog."jsonb_ne"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'"日本語😀"\'::jsonb) AS v LIMIT 1001',
        ["t"],
    ),
    ('SELECT pg_catalog."jsonb_object"(NULL::text[]) AS v', [None]),
    ("SELECT pg_catalog.\"jsonb_object\"('{}'::text[]) AS v", ["{}"]),
    (
        "SELECT pg_catalog.\"jsonb_object\"('{{a,b},{c,d}}'::text[]) AS v LIMIT 1001",
        ['{"a": "b", "c": "d"}'],
    ),
    ('SELECT pg_catalog."jsonb_object"(NULL::text[], NULL::text[]) AS v', [None]),
    (
        "SELECT pg_catalog.\"jsonb_object\"('{a,b,c}'::text[], '{a,b,c}'::text[]) AS v",
        ['{"a": "a", "b": "b", "c": "c"}'],
    ),
    ("SELECT pg_catalog.\"jsonb_object\"('{}'::text[], '{}'::text[]) AS v", ["{}"]),
    ("SELECT pg_catalog.\"jsonb_object\"(NULL::text[], '{a,b,c}'::text[]) AS v LIMIT 1001", [None]),
    ("SELECT pg_catalog.\"jsonb_object\"('{a,b,c}'::text[], NULL::text[]) AS v LIMIT 1001", [None]),
    (
        "SELECT pg_catalog.\"jsonb_object\"('{\"\",日本語😀,é}'::text[], '{a,b,c}'::text[]) AS v LIMIT 1001",
        ['{"": "a", "é": "c", "日本語😀": "b"}'],
    ),
    (
        "SELECT pg_catalog.\"jsonb_object\"('{a,b,c}'::text[], '{\"\",日本語😀,é}'::text[]) AS v LIMIT 1001",
        ['{"a": "", "b": "日本語😀", "c": "é"}'],
    ),
    (
        'SELECT pg_catalog."jsonb_object_agg"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, 3::integer), ((-7)::integer, (-7)::integer), (42::integer, 42::integer)) AS t(c1, c2)',
        ['{"3": 3, "-7": -7, "42": 42}'],
    ),
    (
        'SELECT pg_catalog."jsonb_object_agg"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, NULL::integer), (3::integer, NULL::integer)) AS t(c1, c2) LIMIT 1001',
        ['{"3": null}'],
    ),
    (
        "SELECT pg_catalog.\"jsonb_object_agg\"(c1, c2 ORDER BY c1) AS v FROM (VALUES ('-2147483648'::integer, 3::integer), ('-2147483648'::integer, 3::integer)) AS t(c1, c2) LIMIT 1001",
        ['{"-2147483648": 3}'],
    ),
    (
        "SELECT pg_catalog.\"jsonb_object_agg\"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, '-2147483648'::integer), (3::integer, '-2147483648'::integer)) AS t(c1, c2) LIMIT 1001",
        ['{"3": -2147483648}'],
    ),
    (
        "SELECT pg_catalog.\"jsonb_object_agg\"(c1, c2 ORDER BY c1) AS v FROM (VALUES ('2147483647'::integer, 3::integer), ('2147483647'::integer, 3::integer)) AS t(c1, c2) LIMIT 1001",
        ['{"2147483647": 3}'],
    ),
    (
        "SELECT pg_catalog.\"jsonb_object_agg\"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, '2147483647'::integer), (3::integer, '2147483647'::integer)) AS t(c1, c2) LIMIT 1001",
        ['{"3": 2147483647}'],
    ),
    (
        "SELECT pg_catalog.\"jsonb_object_agg\"(c1, c2 ORDER BY c1) AS v FROM (VALUES ('0'::integer, 3::integer), ('0'::integer, 3::integer)) AS t(c1, c2) LIMIT 1001",
        ['{"0": 3}'],
    ),
    (
        "SELECT pg_catalog.\"jsonb_object_agg\"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, '0'::integer), (3::integer, '0'::integer)) AS t(c1, c2) LIMIT 1001",
        ['{"3": 0}'],
    ),
    (
        'SELECT pg_catalog."jsonb_object_agg_strict"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, 3::integer), ((-7)::integer, (-7)::integer), (42::integer, 42::integer)) AS t(c1, c2)',
        ['{"3": 3, "-7": -7, "42": 42}'],
    ),
    (
        'SELECT pg_catalog."jsonb_object_agg_strict"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, NULL::integer), (3::integer, NULL::integer)) AS t(c1, c2) LIMIT 1001',
        ["{}"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_object_agg_strict\"(c1, c2 ORDER BY c1) AS v FROM (VALUES ('-2147483648'::integer, 3::integer), ('-2147483648'::integer, 3::integer)) AS t(c1, c2) LIMIT 1001",
        ['{"-2147483648": 3}'],
    ),
    (
        "SELECT pg_catalog.\"jsonb_object_agg_strict\"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, '-2147483648'::integer), (3::integer, '-2147483648'::integer)) AS t(c1, c2) LIMIT 1001",
        ['{"3": -2147483648}'],
    ),
    (
        "SELECT pg_catalog.\"jsonb_object_agg_strict\"(c1, c2 ORDER BY c1) AS v FROM (VALUES ('2147483647'::integer, 3::integer), ('2147483647'::integer, 3::integer)) AS t(c1, c2) LIMIT 1001",
        ['{"2147483647": 3}'],
    ),
    (
        "SELECT pg_catalog.\"jsonb_object_agg_strict\"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, '2147483647'::integer), (3::integer, '2147483647'::integer)) AS t(c1, c2) LIMIT 1001",
        ['{"3": 2147483647}'],
    ),
    (
        "SELECT pg_catalog.\"jsonb_object_agg_strict\"(c1, c2 ORDER BY c1) AS v FROM (VALUES ('0'::integer, 3::integer), ('0'::integer, 3::integer)) AS t(c1, c2) LIMIT 1001",
        ['{"0": 3}'],
    ),
    (
        "SELECT pg_catalog.\"jsonb_object_agg_strict\"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, '0'::integer), (3::integer, '0'::integer)) AS t(c1, c2) LIMIT 1001",
        ['{"3": 0}'],
    ),
    (
        'SELECT pg_catalog."jsonb_object_agg_unique"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, 3::integer), ((-7)::integer, (-7)::integer), (42::integer, 42::integer)) AS t(c1, c2)',
        ['{"3": 3, "-7": -7, "42": 42}'],
    ),
    (
        'SELECT pg_catalog."jsonb_object_agg_unique_strict"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, 3::integer), ((-7)::integer, (-7)::integer), (42::integer, 42::integer)) AS t(c1, c2)',
        ['{"3": 3, "-7": -7, "42": 42}'],
    ),
    (
        'SELECT pg_catalog."jsonb_path_exists"(NULL::jsonb, NULL::jsonpath, NULL::jsonb, NULL::boolean) AS v',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_exists"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v',
        ["t"],
    ),
    (
        'SELECT pg_catalog."jsonb_path_exists"(NULL::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_exists"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_exists"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, NULL::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_exists"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_exists\"('{}'::jsonb, '$.a'::jsonpath, '{\"a\": 1, \"b\": [1, 2]}'::jsonb, true::boolean) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_exists\"('{\"a\": 1, \"b\": [1, 2]}'::jsonb, '$.a'::jsonpath, '{}'::jsonb, true::boolean) AS v LIMIT 1001",
        ["t"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_exists\"('[]'::jsonb, '$.a'::jsonpath, '{\"a\": 1, \"b\": [1, 2]}'::jsonb, true::boolean) AS v LIMIT 1001",
        ["f"],
    ),
    ('SELECT pg_catalog."jsonb_path_exists_opr"(NULL::jsonb, NULL::jsonpath) AS v', [None]),
    (
        'SELECT pg_catalog."jsonb_path_exists_opr"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath) AS v',
        ["t"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_exists_opr\"('[1, \"two\", null]'::jsonb, '$[0]'::jsonpath) AS v",
        ["t"],
    ),
    ("SELECT pg_catalog.\"jsonb_path_exists_opr\"('3'::jsonb, '$'::jsonpath) AS v", ["t"]),
    (
        "SELECT pg_catalog.\"jsonb_path_exists_opr\"(NULL::jsonb, '$.a'::jsonpath) AS v LIMIT 1001",
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_exists_opr"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::jsonpath) AS v LIMIT 1001',
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_exists_opr\"('{}'::jsonb, '$.a'::jsonpath) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_exists_opr\"('[]'::jsonb, '$.a'::jsonpath) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_exists_opr\"('\"日本語😀\"'::jsonb, '$.a'::jsonpath) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_exists_opr\"('12345678901234567890123456789'::jsonb, '$.a'::jsonpath) AS v LIMIT 1001",
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_path_exists_tz"(NULL::jsonb, NULL::jsonpath, NULL::jsonb, NULL::boolean) AS v',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_exists_tz"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v',
        ["t"],
    ),
    (
        'SELECT pg_catalog."jsonb_path_exists_tz"(NULL::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_exists_tz"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_exists_tz"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, NULL::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_exists_tz"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_exists_tz\"('{}'::jsonb, '$.a'::jsonpath, '{\"a\": 1, \"b\": [1, 2]}'::jsonb, true::boolean) AS v LIMIT 1001",
        ["f"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_exists_tz\"('{\"a\": 1, \"b\": [1, 2]}'::jsonb, '$.a'::jsonpath, '{}'::jsonb, true::boolean) AS v LIMIT 1001",
        ["t"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_exists_tz\"('[]'::jsonb, '$.a'::jsonpath, '{\"a\": 1, \"b\": [1, 2]}'::jsonb, true::boolean) AS v LIMIT 1001",
        ["f"],
    ),
    (
        'SELECT pg_catalog."jsonb_path_match"(NULL::jsonb, NULL::jsonpath, NULL::jsonb, NULL::boolean) AS v',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_match"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_match"(NULL::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_match"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_match"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, NULL::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_match"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_match\"('{}'::jsonb, '$.a'::jsonpath, '{\"a\": 1, \"b\": [1, 2]}'::jsonb, true::boolean) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_match\"('{\"a\": 1, \"b\": [1, 2]}'::jsonb, '$.a'::jsonpath, '{}'::jsonb, true::boolean) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_match\"('[]'::jsonb, '$.a'::jsonpath, '{\"a\": 1, \"b\": [1, 2]}'::jsonb, true::boolean) AS v LIMIT 1001",
        [None],
    ),
    ('SELECT pg_catalog."jsonb_path_match_opr"(NULL::jsonb, NULL::jsonpath) AS v', [None]),
    (
        'SELECT pg_catalog."jsonb_path_match_opr"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath) AS v',
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_match_opr\"('[1, \"two\", null]'::jsonb, '$[0]'::jsonpath) AS v",
        [None],
    ),
    ("SELECT pg_catalog.\"jsonb_path_match_opr\"('3'::jsonb, '$'::jsonpath) AS v", [None]),
    (
        "SELECT pg_catalog.\"jsonb_path_match_opr\"(NULL::jsonb, '$.a'::jsonpath) AS v LIMIT 1001",
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_match_opr"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::jsonpath) AS v LIMIT 1001',
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_match_opr\"('{}'::jsonb, '$.a'::jsonpath) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_match_opr\"('[]'::jsonb, '$.a'::jsonpath) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_match_opr\"('\"日本語😀\"'::jsonb, '$.a'::jsonpath) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_match_opr\"('12345678901234567890123456789'::jsonb, '$.a'::jsonpath) AS v LIMIT 1001",
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_match_tz"(NULL::jsonb, NULL::jsonpath, NULL::jsonb, NULL::boolean) AS v',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_match_tz"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_match_tz"(NULL::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_match_tz"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_match_tz"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, NULL::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_match_tz"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_match_tz\"('{}'::jsonb, '$.a'::jsonpath, '{\"a\": 1, \"b\": [1, 2]}'::jsonb, true::boolean) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_match_tz\"('{\"a\": 1, \"b\": [1, 2]}'::jsonb, '$.a'::jsonpath, '{}'::jsonb, true::boolean) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_match_tz\"('[]'::jsonb, '$.a'::jsonpath, '{\"a\": 1, \"b\": [1, 2]}'::jsonb, true::boolean) AS v LIMIT 1001",
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query"(NULL::jsonb, NULL::jsonpath, NULL::jsonb, NULL::boolean) AS v',
        [],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v',
        ["1"],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query"(NULL::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v LIMIT 1001',
        [],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v LIMIT 1001',
        [],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, NULL::jsonb, true::boolean) AS v LIMIT 1001',
        [],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::boolean) AS v LIMIT 1001',
        [],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query\"('{}'::jsonb, '$.a'::jsonpath, '{\"a\": 1, \"b\": [1, 2]}'::jsonb, true::boolean) AS v LIMIT 1001",
        [],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query\"('{\"a\": 1, \"b\": [1, 2]}'::jsonb, '$.a'::jsonpath, '{}'::jsonb, true::boolean) AS v LIMIT 1001",
        ["1"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query\"('[]'::jsonb, '$.a'::jsonpath, '{\"a\": 1, \"b\": [1, 2]}'::jsonb, true::boolean) AS v LIMIT 1001",
        [],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_array"(NULL::jsonb, NULL::jsonpath, NULL::jsonb, NULL::boolean) AS v',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_array"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v',
        ["[1]"],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_array"(NULL::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_array"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_array"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, NULL::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_array"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_array\"('{}'::jsonb, '$.a'::jsonpath, '{\"a\": 1, \"b\": [1, 2]}'::jsonb, true::boolean) AS v LIMIT 1001",
        ["[]"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_array\"('{\"a\": 1, \"b\": [1, 2]}'::jsonb, '$.a'::jsonpath, '{}'::jsonb, true::boolean) AS v LIMIT 1001",
        ["[1]"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_array\"('[]'::jsonb, '$.a'::jsonpath, '{\"a\": 1, \"b\": [1, 2]}'::jsonb, true::boolean) AS v LIMIT 1001",
        ["[]"],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_array_tz"(NULL::jsonb, NULL::jsonpath, NULL::jsonb, NULL::boolean) AS v',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_array_tz"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v',
        ["[1]"],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_array_tz"(NULL::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_array_tz"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_array_tz"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, NULL::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_array_tz"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_array_tz\"('{}'::jsonb, '$.a'::jsonpath, '{\"a\": 1, \"b\": [1, 2]}'::jsonb, true::boolean) AS v LIMIT 1001",
        ["[]"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_array_tz\"('{\"a\": 1, \"b\": [1, 2]}'::jsonb, '$.a'::jsonpath, '{}'::jsonb, true::boolean) AS v LIMIT 1001",
        ["[1]"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_array_tz\"('[]'::jsonb, '$.a'::jsonpath, '{\"a\": 1, \"b\": [1, 2]}'::jsonb, true::boolean) AS v LIMIT 1001",
        ["[]"],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_first"(NULL::jsonb, NULL::jsonpath, NULL::jsonb, NULL::boolean) AS v',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_first"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v',
        ["1"],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_first"(NULL::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_first"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_first"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, NULL::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_first"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_first\"('{}'::jsonb, '$.a'::jsonpath, '{\"a\": 1, \"b\": [1, 2]}'::jsonb, true::boolean) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_first\"('{\"a\": 1, \"b\": [1, 2]}'::jsonb, '$.a'::jsonpath, '{}'::jsonb, true::boolean) AS v LIMIT 1001",
        ["1"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_first\"('[]'::jsonb, '$.a'::jsonpath, '{\"a\": 1, \"b\": [1, 2]}'::jsonb, true::boolean) AS v LIMIT 1001",
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_first_tz"(NULL::jsonb, NULL::jsonpath, NULL::jsonb, NULL::boolean) AS v',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_first_tz"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v',
        ["1"],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_first_tz"(NULL::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_first_tz"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_first_tz"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, NULL::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_first_tz"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_first_tz\"('{}'::jsonb, '$.a'::jsonpath, '{\"a\": 1, \"b\": [1, 2]}'::jsonb, true::boolean) AS v LIMIT 1001",
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_first_tz\"('{\"a\": 1, \"b\": [1, 2]}'::jsonb, '$.a'::jsonpath, '{}'::jsonb, true::boolean) AS v LIMIT 1001",
        ["1"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_first_tz\"('[]'::jsonb, '$.a'::jsonpath, '{\"a\": 1, \"b\": [1, 2]}'::jsonb, true::boolean) AS v LIMIT 1001",
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_tz"(NULL::jsonb, NULL::jsonpath, NULL::jsonb, NULL::boolean) AS v',
        [],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_tz"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v',
        ["1"],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_tz"(NULL::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v LIMIT 1001',
        [],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_tz"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v LIMIT 1001',
        [],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_tz"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, NULL::jsonb, true::boolean) AS v LIMIT 1001',
        [],
    ),
    (
        'SELECT pg_catalog."jsonb_path_query_tz"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'$.a\'::jsonpath, \'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::boolean) AS v LIMIT 1001',
        [],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_tz\"('{}'::jsonb, '$.a'::jsonpath, '{\"a\": 1, \"b\": [1, 2]}'::jsonb, true::boolean) AS v LIMIT 1001",
        [],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_tz\"('{\"a\": 1, \"b\": [1, 2]}'::jsonb, '$.a'::jsonpath, '{}'::jsonb, true::boolean) AS v LIMIT 1001",
        ["1"],
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_tz\"('[]'::jsonb, '$.a'::jsonpath, '{\"a\": 1, \"b\": [1, 2]}'::jsonb, true::boolean) AS v LIMIT 1001",
        [],
    ),
    ('SELECT pg_catalog."jsonb_pretty"(NULL::jsonb) AS v', [None]),
    (
        'SELECT pg_catalog."jsonb_pretty"(\'{"a": 1, "b": [1, 2]}\'::jsonb) AS v',
        ['{\n    "a": 1,\n    "b": [\n        1,\n        2\n    ]\n}'],
    ),
    (
        'SELECT pg_catalog."jsonb_pretty"(\'[1, "two", null]\'::jsonb) AS v',
        ['[\n    1,\n    "two",\n    null\n]'],
    ),
    ("SELECT pg_catalog.\"jsonb_pretty\"('3'::jsonb) AS v", ["3"]),
    ("SELECT pg_catalog.\"jsonb_pretty\"('{}'::jsonb) AS v LIMIT 1001", ["{\n}"]),
    ("SELECT pg_catalog.\"jsonb_pretty\"('[]'::jsonb) AS v LIMIT 1001", ["[\n]"]),
    ('SELECT pg_catalog."jsonb_pretty"(\'"日本語😀"\'::jsonb) AS v LIMIT 1001', ['"日本語😀"']),
    (
        "SELECT pg_catalog.\"jsonb_pretty\"('12345678901234567890123456789'::jsonb) AS v LIMIT 1001",
        ["12345678901234567890123456789"],
    ),
    (
        'SELECT pg_catalog."jsonb_set"(NULL::jsonb, NULL::text[], NULL::jsonb, NULL::boolean) AS v',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_set"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{a,b,c}\'::text[], \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v',
        ['{"a": 1, "b": [1, 2]}'],
    ),
    (
        "SELECT pg_catalog.\"jsonb_set\"('[1, \"two\", null]'::jsonb, '{}'::text[], '[1, \"two\", null]'::jsonb, false::boolean) AS v",
        ['[1, "two", null]'],
    ),
    (
        'SELECT pg_catalog."jsonb_set"(NULL::jsonb, \'{a,b,c}\'::text[], \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_set"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::text[], \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_set"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{a,b,c}\'::text[], NULL::jsonb, true::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_set"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{a,b,c}\'::text[], \'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::boolean) AS v LIMIT 1001',
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_set\"('{}'::jsonb, '{a,b,c}'::text[], '{\"a\": 1, \"b\": [1, 2]}'::jsonb, true::boolean) AS v LIMIT 1001",
        ["{}"],
    ),
    (
        'SELECT pg_catalog."jsonb_set"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{"",日本語😀,é}\'::text[], \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean) AS v LIMIT 1001',
        ['{"a": 1, "b": [1, 2]}'],
    ),
    (
        "SELECT pg_catalog.\"jsonb_set\"('{\"a\": 1, \"b\": [1, 2]}'::jsonb, '{a,b,c}'::text[], '{}'::jsonb, true::boolean) AS v LIMIT 1001",
        ['{"a": 1, "b": [1, 2]}'],
    ),
    (
        'SELECT pg_catalog."jsonb_set_lax"(NULL::jsonb, NULL::text[], NULL::jsonb, NULL::boolean, NULL::text) AS v',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_set_lax"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{a,b,c}\'::text[], \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean, \'abc\'::text) AS v',
        ['{"a": 1, "b": [1, 2]}'],
    ),
    (
        "SELECT pg_catalog.\"jsonb_set_lax\"('[1, \"two\", null]'::jsonb, '{}'::text[], '[1, \"two\", null]'::jsonb, false::boolean, 'Hello World'::text) AS v",
        ['[1, "two", null]'],
    ),
    (
        "SELECT pg_catalog.\"jsonb_set_lax\"(NULL::jsonb, '{a,b,c}'::text[], '{\"a\": 1, \"b\": [1, 2]}'::jsonb, true::boolean, 'abc'::text) AS v LIMIT 1001",
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_set_lax"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::text[], \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean, \'abc\'::text) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."jsonb_set_lax"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{a,b,c}\'::text[], \'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::boolean, \'abc\'::text) AS v LIMIT 1001',
        [None],
    ),
    (
        "SELECT pg_catalog.\"jsonb_set_lax\"('{}'::jsonb, '{a,b,c}'::text[], '{\"a\": 1, \"b\": [1, 2]}'::jsonb, true::boolean, 'abc'::text) AS v LIMIT 1001",
        ["{}"],
    ),
    (
        'SELECT pg_catalog."jsonb_set_lax"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{"",日本語😀,é}\'::text[], \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean, \'abc\'::text) AS v LIMIT 1001',
        ['{"a": 1, "b": [1, 2]}'],
    ),
    (
        "SELECT pg_catalog.\"jsonb_set_lax\"('{\"a\": 1, \"b\": [1, 2]}'::jsonb, '{a,b,c}'::text[], '{}'::jsonb, true::boolean, 'abc'::text) AS v LIMIT 1001",
        ['{"a": 1, "b": [1, 2]}'],
    ),
    ('SELECT pg_catalog."jsonb_strip_nulls"(NULL::jsonb) AS v', [None]),
    (
        'SELECT pg_catalog."jsonb_strip_nulls"(\'{"a": 1, "b": [1, 2]}\'::jsonb) AS v',
        ['{"a": 1, "b": [1, 2]}'],
    ),
    (
        'SELECT pg_catalog."jsonb_strip_nulls"(\'[1, "two", null]\'::jsonb) AS v',
        ['[1, "two", null]'],
    ),
    ("SELECT pg_catalog.\"jsonb_strip_nulls\"('3'::jsonb) AS v", ["3"]),
    ("SELECT pg_catalog.\"jsonb_strip_nulls\"('{}'::jsonb) AS v LIMIT 1001", ["{}"]),
    ("SELECT pg_catalog.\"jsonb_strip_nulls\"('[]'::jsonb) AS v LIMIT 1001", ["[]"]),
    (
        'SELECT pg_catalog."jsonb_strip_nulls"(\'"日本語😀"\'::jsonb) AS v LIMIT 1001',
        ['"日本語😀"'],
    ),
    (
        "SELECT pg_catalog.\"jsonb_strip_nulls\"('12345678901234567890123456789'::jsonb) AS v LIMIT 1001",
        ["12345678901234567890123456789"],
    ),
    ('SELECT pg_catalog."row_to_json"(NULL::record) AS v', [None]),
    ("SELECT pg_catalog.\"row_to_json\"(ROW(1, 'a'::text)::record) AS v", ['{"f1":1,"f2":"a"}']),
    ("SELECT pg_catalog.\"row_to_json\"(ROW(2, 'b'::text)::record) AS v", ['{"f1":2,"f2":"b"}']),
    (
        "SELECT pg_catalog.\"row_to_json\"(ROW(NULL::integer, 'c'::text)::record) AS v",
        ['{"f1":null,"f2":"c"}'],
    ),
    ('SELECT pg_catalog."row_to_json"(NULL::record, NULL::boolean) AS v', [None]),
    (
        "SELECT pg_catalog.\"row_to_json\"(ROW(1, 'a'::text)::record, true::boolean) AS v",
        ['{"f1":1,\n "f2":"a"}'],
    ),
    (
        "SELECT pg_catalog.\"row_to_json\"(ROW(2, 'b'::text)::record, false::boolean) AS v",
        ['{"f1":2,"f2":"b"}'],
    ),
    (
        "SELECT pg_catalog.\"row_to_json\"(ROW(NULL::integer, 'c'::text)::record, true::boolean) AS v",
        ['{"f1":null,\n "f2":"c"}'],
    ),
    ('SELECT pg_catalog."row_to_json"(NULL::record, true::boolean) AS v LIMIT 1001', [None]),
    (
        "SELECT pg_catalog.\"row_to_json\"(ROW(1, 'a'::text)::record, NULL::boolean) AS v LIMIT 1001",
        [None],
    ),
    ('SELECT pg_catalog."to_json"(NULL::integer) AS v', [None]),
    ('SELECT pg_catalog."to_json"(3::integer) AS v', ["3"]),
    ('SELECT pg_catalog."to_json"((-7)::integer) AS v', ["-7"]),
    ('SELECT pg_catalog."to_json"(42::integer) AS v', ["42"]),
    ("SELECT pg_catalog.\"to_json\"('-2147483648'::integer) AS v LIMIT 1001", ["-2147483648"]),
    ("SELECT pg_catalog.\"to_json\"('2147483647'::integer) AS v LIMIT 1001", ["2147483647"]),
    ("SELECT pg_catalog.\"to_json\"('0'::integer) AS v LIMIT 1001", ["0"]),
    ('SELECT pg_catalog."to_jsonb"(NULL::integer) AS v', [None]),
    ('SELECT pg_catalog."to_jsonb"(3::integer) AS v', ["3"]),
    ('SELECT pg_catalog."to_jsonb"((-7)::integer) AS v', ["-7"]),
    ('SELECT pg_catalog."to_jsonb"(42::integer) AS v', ["42"]),
    ("SELECT pg_catalog.\"to_jsonb\"('-2147483648'::integer) AS v LIMIT 1001", ["-2147483648"]),
    ("SELECT pg_catalog.\"to_jsonb\"('2147483647'::integer) AS v LIMIT 1001", ["2147483647"]),
    ("SELECT pg_catalog.\"to_jsonb\"('0'::integer) AS v LIMIT 1001", ["0"]),
    (
        "SELECT json_extract_path_text('{\"a\":[1, 2]}'::json, VARIADIC '{}'::text[])",
        ['{"a":[1, 2]}'],
    ),
    ("SELECT json_extract_path('[1,2,3]'::json, VARIADIC '{-1}'::text[])", ["3"]),
    ("SELECT jsonb_extract_path('[1,2,3]'::jsonb, VARIADIC '{-1}'::text[])", ["3"]),
    (
        'SELECT json_extract_path(\'{"a":{"b":[1,{"c":null}]}}\'::json, VARIADIC \'{a,b,1,c}\'::text[])',
        ["null"],
    ),
    (
        'SELECT json_extract_path_text(\'{"a":{"b":[1,{"c":null}]}}\'::json, VARIADIC \'{a,b,1,c}\'::text[])',
        [None],
    ),
    ("SELECT json_extract_path('{\"a\":1}'::json, VARIADIC '{a,NULL}'::text[])", [None]),
    ("SELECT jsonb_delete('[1,2,3]'::jsonb, -5)", ["[1, 2, 3]"]),
    ("SELECT jsonb_delete('{\"a\":1}'::jsonb, VARIADIC '{}'::text[])", ['{"a": 1}']),
    ("SELECT jsonb_delete(NULL::jsonb, VARIADIC '{{a,b},{c,d}}'::text[])", [None]),
]
RAISED = [
    (
        "SELECT pg_catalog.\"json_agg\"(c1 ORDER BY c1) AS v FROM (VALUES ('2147483648'::integer), ('2147483648'::integer)) AS t(c1) LIMIT 1001",
        "22003",
    ),
    (
        "SELECT pg_catalog.\"json_agg_strict\"(c1 ORDER BY c1) AS v FROM (VALUES ('2147483648'::integer), ('2147483648'::integer)) AS t(c1) LIMIT 1001",
        "22003",
    ),
    ('SELECT pg_catalog."json_array_elements"(\'{"a": 1, "b": [1, 2]}\'::json) AS v', "22023"),
    ("SELECT pg_catalog.\"json_array_elements\"('3'::json) AS v", "22023"),
    ("SELECT pg_catalog.\"json_array_elements\"('{}'::json) AS v LIMIT 1001", "22023"),
    ('SELECT pg_catalog."json_array_elements"(\'"日本語😀"\'::json) AS v LIMIT 1001', "22023"),
    (
        "SELECT pg_catalog.\"json_array_elements\"('12345678901234567890123456789'::json) AS v LIMIT 1001",
        "22023",
    ),
    ('SELECT pg_catalog."json_array_elements_text"(\'{"a": 1, "b": [1, 2]}\'::json) AS v', "22023"),
    ("SELECT pg_catalog.\"json_array_elements_text\"('3'::json) AS v", "22023"),
    ("SELECT pg_catalog.\"json_array_elements_text\"('{}'::json) AS v LIMIT 1001", "22023"),
    ('SELECT pg_catalog."json_array_elements_text"(\'"日本語😀"\'::json) AS v LIMIT 1001', "22023"),
    (
        "SELECT pg_catalog.\"json_array_elements_text\"('12345678901234567890123456789'::json) AS v LIMIT 1001",
        "22023",
    ),
    ("SELECT pg_catalog.\"json_build_array\"('2147483648'::integer) AS v LIMIT 1001", "22003"),
    ('SELECT pg_catalog."json_each"(\'[1, "two", null]\'::json) AS v', "22023"),
    ("SELECT pg_catalog.\"json_each\"('3'::json) AS v", "22023"),
    ("SELECT pg_catalog.\"json_each\"('[]'::json) AS v LIMIT 1001", "22023"),
    ('SELECT pg_catalog."json_each"(\'"日本語😀"\'::json) AS v LIMIT 1001', "22023"),
    (
        "SELECT pg_catalog.\"json_each\"('12345678901234567890123456789'::json) AS v LIMIT 1001",
        "22023",
    ),
    ('SELECT pg_catalog."json_each_text"(\'[1, "two", null]\'::json) AS v', "22023"),
    ("SELECT pg_catalog.\"json_each_text\"('3'::json) AS v", "22023"),
    ("SELECT pg_catalog.\"json_each_text\"('[]'::json) AS v LIMIT 1001", "22023"),
    ('SELECT pg_catalog."json_each_text"(\'"日本語😀"\'::json) AS v LIMIT 1001', "22023"),
    (
        "SELECT pg_catalog.\"json_each_text\"('12345678901234567890123456789'::json) AS v LIMIT 1001",
        "22023",
    ),
    ("SELECT pg_catalog.\"json_object\"('{a,b,c}'::text[]) AS v", "2202E"),
    ("SELECT pg_catalog.\"json_object\"('{x,NULL,z}'::text[]) AS v", "2202E"),
    ('SELECT pg_catalog."json_object"(\'{"",日本語😀,é}\'::text[]) AS v LIMIT 1001', "2202E"),
    ("SELECT pg_catalog.\"json_object\"('{NULL}'::text[]) AS v LIMIT 1001", "2202E"),
    ("SELECT pg_catalog.\"json_object\"('{x,NULL,z}'::text[], '{x,NULL,z}'::text[]) AS v", "22004"),
    (
        "SELECT pg_catalog.\"json_object\"('{NULL}'::text[], '{a,b,c}'::text[]) AS v LIMIT 1001",
        "2202E",
    ),
    (
        "SELECT pg_catalog.\"json_object\"('{a,b,c}'::text[], '{NULL}'::text[]) AS v LIMIT 1001",
        "2202E",
    ),
    (
        "SELECT pg_catalog.\"json_object\"('{{a,b},{c,d}}'::text[], '{a,b,c}'::text[]) AS v LIMIT 1001",
        "2202E",
    ),
    (
        "SELECT pg_catalog.\"json_object\"('{a,b,c}'::text[], '{{a,b},{c,d}}'::text[]) AS v LIMIT 1001",
        "2202E",
    ),
    ('SELECT pg_catalog."json_object_agg"(NULL::integer, NULL::integer) AS v', "22004"),
    (
        'SELECT pg_catalog."json_object_agg"(c1, c2 ORDER BY c1) AS v FROM (VALUES (NULL::integer, 3::integer), (NULL::integer, 3::integer)) AS t(c1, c2) LIMIT 1001',
        "22004",
    ),
    ('SELECT pg_catalog."json_object_agg_strict"(NULL::integer, NULL::integer) AS v', "22004"),
    (
        'SELECT pg_catalog."json_object_agg_strict"(c1, c2 ORDER BY c1) AS v FROM (VALUES (NULL::integer, 3::integer), (NULL::integer, 3::integer)) AS t(c1, c2) LIMIT 1001',
        "22004",
    ),
    ('SELECT pg_catalog."json_object_agg_unique"(NULL::integer, NULL::integer) AS v', "22004"),
    (
        'SELECT pg_catalog."json_object_agg_unique"(c1, c2 ORDER BY c1) AS v FROM (VALUES (NULL::integer, 3::integer), (NULL::integer, 3::integer)) AS t(c1, c2) LIMIT 1001',
        "22004",
    ),
    (
        'SELECT pg_catalog."json_object_agg_unique"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, NULL::integer), (3::integer, NULL::integer)) AS t(c1, c2) LIMIT 1001',
        "22030",
    ),
    (
        "SELECT pg_catalog.\"json_object_agg_unique\"(c1, c2 ORDER BY c1) AS v FROM (VALUES ('-2147483648'::integer, 3::integer), ('-2147483648'::integer, 3::integer)) AS t(c1, c2) LIMIT 1001",
        "22030",
    ),
    (
        "SELECT pg_catalog.\"json_object_agg_unique\"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, '-2147483648'::integer), (3::integer, '-2147483648'::integer)) AS t(c1, c2) LIMIT 1001",
        "22030",
    ),
    (
        "SELECT pg_catalog.\"json_object_agg_unique\"(c1, c2 ORDER BY c1) AS v FROM (VALUES ('2147483647'::integer, 3::integer), ('2147483647'::integer, 3::integer)) AS t(c1, c2) LIMIT 1001",
        "22030",
    ),
    (
        "SELECT pg_catalog.\"json_object_agg_unique\"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, '2147483647'::integer), (3::integer, '2147483647'::integer)) AS t(c1, c2) LIMIT 1001",
        "22030",
    ),
    (
        "SELECT pg_catalog.\"json_object_agg_unique\"(c1, c2 ORDER BY c1) AS v FROM (VALUES ('0'::integer, 3::integer), ('0'::integer, 3::integer)) AS t(c1, c2) LIMIT 1001",
        "22030",
    ),
    (
        "SELECT pg_catalog.\"json_object_agg_unique\"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, '0'::integer), (3::integer, '0'::integer)) AS t(c1, c2) LIMIT 1001",
        "22030",
    ),
    (
        'SELECT pg_catalog."json_object_agg_unique_strict"(NULL::integer, NULL::integer) AS v',
        "22004",
    ),
    (
        'SELECT pg_catalog."json_object_agg_unique_strict"(c1, c2 ORDER BY c1) AS v FROM (VALUES (NULL::integer, 3::integer), (NULL::integer, 3::integer)) AS t(c1, c2) LIMIT 1001',
        "22004",
    ),
    (
        'SELECT pg_catalog."json_object_agg_unique_strict"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, NULL::integer), (3::integer, NULL::integer)) AS t(c1, c2) LIMIT 1001',
        "22030",
    ),
    (
        "SELECT pg_catalog.\"json_object_agg_unique_strict\"(c1, c2 ORDER BY c1) AS v FROM (VALUES ('-2147483648'::integer, 3::integer), ('-2147483648'::integer, 3::integer)) AS t(c1, c2) LIMIT 1001",
        "22030",
    ),
    (
        "SELECT pg_catalog.\"json_object_agg_unique_strict\"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, '-2147483648'::integer), (3::integer, '-2147483648'::integer)) AS t(c1, c2) LIMIT 1001",
        "22030",
    ),
    (
        "SELECT pg_catalog.\"json_object_agg_unique_strict\"(c1, c2 ORDER BY c1) AS v FROM (VALUES ('2147483647'::integer, 3::integer), ('2147483647'::integer, 3::integer)) AS t(c1, c2) LIMIT 1001",
        "22030",
    ),
    (
        "SELECT pg_catalog.\"json_object_agg_unique_strict\"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, '2147483647'::integer), (3::integer, '2147483647'::integer)) AS t(c1, c2) LIMIT 1001",
        "22030",
    ),
    (
        "SELECT pg_catalog.\"json_object_agg_unique_strict\"(c1, c2 ORDER BY c1) AS v FROM (VALUES ('0'::integer, 3::integer), ('0'::integer, 3::integer)) AS t(c1, c2) LIMIT 1001",
        "22030",
    ),
    (
        "SELECT pg_catalog.\"json_object_agg_unique_strict\"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, '0'::integer), (3::integer, '0'::integer)) AS t(c1, c2) LIMIT 1001",
        "22030",
    ),
    (
        "SELECT pg_catalog.\"jsonb_agg\"(c1 ORDER BY c1) AS v FROM (VALUES ('2147483648'::integer), ('2147483648'::integer)) AS t(c1) LIMIT 1001",
        "22003",
    ),
    (
        "SELECT pg_catalog.\"jsonb_agg_strict\"(c1 ORDER BY c1) AS v FROM (VALUES ('2147483648'::integer), ('2147483648'::integer)) AS t(c1) LIMIT 1001",
        "22003",
    ),
    ('SELECT pg_catalog."jsonb_array_elements"(\'{"a": 1, "b": [1, 2]}\'::jsonb) AS v', "22023"),
    ("SELECT pg_catalog.\"jsonb_array_elements\"('3'::jsonb) AS v", "22023"),
    ("SELECT pg_catalog.\"jsonb_array_elements\"('{}'::jsonb) AS v LIMIT 1001", "22023"),
    ('SELECT pg_catalog."jsonb_array_elements"(\'"日本語😀"\'::jsonb) AS v LIMIT 1001', "22023"),
    (
        "SELECT pg_catalog.\"jsonb_array_elements\"('12345678901234567890123456789'::jsonb) AS v LIMIT 1001",
        "22023",
    ),
    (
        'SELECT pg_catalog."jsonb_array_elements_text"(\'{"a": 1, "b": [1, 2]}\'::jsonb) AS v',
        "22023",
    ),
    ("SELECT pg_catalog.\"jsonb_array_elements_text\"('3'::jsonb) AS v", "22023"),
    ("SELECT pg_catalog.\"jsonb_array_elements_text\"('{}'::jsonb) AS v LIMIT 1001", "22023"),
    (
        'SELECT pg_catalog."jsonb_array_elements_text"(\'"日本語😀"\'::jsonb) AS v LIMIT 1001',
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_array_elements_text\"('12345678901234567890123456789'::jsonb) AS v LIMIT 1001",
        "22023",
    ),
    ("SELECT pg_catalog.\"jsonb_build_array\"('2147483648'::integer) AS v LIMIT 1001", "22003"),
    ("SELECT pg_catalog.\"jsonb_delete\"('3'::jsonb, VARIADIC '{x,NULL,z}'::text[]) AS v", "22023"),
    (
        "SELECT pg_catalog.\"jsonb_delete\"('\"日本語😀\"'::jsonb, VARIADIC '{a,b,c}'::text[]) AS v LIMIT 1001",
        "22023",
    ),
    (
        'SELECT pg_catalog."jsonb_delete"(\'{"a": 1, "b": [1, 2]}\'::jsonb, VARIADIC \'{{a,b},{c,d}}\'::text[]) AS v LIMIT 1001',
        "2202E",
    ),
    (
        'SELECT pg_catalog."jsonb_delete"(\'{"a": 1, "b": [1, 2]}\'::jsonb, 3::integer) AS v',
        "22023",
    ),
    ("SELECT pg_catalog.\"jsonb_delete\"('3'::jsonb, 42::integer) AS v", "22023"),
    ("SELECT pg_catalog.\"jsonb_delete\"('{}'::jsonb, 3::integer) AS v LIMIT 1001", "22023"),
    (
        'SELECT pg_catalog."jsonb_delete"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'-2147483648\'::integer) AS v LIMIT 1001',
        "22023",
    ),
    (
        'SELECT pg_catalog."jsonb_delete"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'2147483647\'::integer) AS v LIMIT 1001',
        "22023",
    ),
    (
        'SELECT pg_catalog."jsonb_delete"(\'"日本語😀"\'::jsonb, 3::integer) AS v LIMIT 1001',
        "22023",
    ),
    (
        'SELECT pg_catalog."jsonb_delete"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'0\'::integer) AS v LIMIT 1001',
        "22023",
    ),
    ("SELECT pg_catalog.\"jsonb_delete\"('3'::jsonb, '12'::text) AS v", "22023"),
    (
        "SELECT pg_catalog.\"jsonb_delete\"('\"日本語😀\"'::jsonb, 'abc'::text) AS v LIMIT 1001",
        "22023",
    ),
    ("SELECT pg_catalog.\"jsonb_delete_path\"('3'::jsonb, '{x,NULL,z}'::text[]) AS v", "22023"),
    (
        'SELECT pg_catalog."jsonb_delete_path"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{NULL}\'::text[]) AS v LIMIT 1001',
        "22004",
    ),
    (
        "SELECT pg_catalog.\"jsonb_delete_path\"('\"日本語😀\"'::jsonb, '{a,b,c}'::text[]) AS v LIMIT 1001",
        "22023",
    ),
    (
        'SELECT pg_catalog."jsonb_delete_path"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{{a,b},{c,d}}\'::text[]) AS v LIMIT 1001',
        "2202E",
    ),
    ('SELECT pg_catalog."jsonb_each"(\'[1, "two", null]\'::jsonb) AS v', "22023"),
    ("SELECT pg_catalog.\"jsonb_each\"('3'::jsonb) AS v", "22023"),
    ("SELECT pg_catalog.\"jsonb_each\"('[]'::jsonb) AS v LIMIT 1001", "22023"),
    ('SELECT pg_catalog."jsonb_each"(\'"日本語😀"\'::jsonb) AS v LIMIT 1001', "22023"),
    (
        "SELECT pg_catalog.\"jsonb_each\"('12345678901234567890123456789'::jsonb) AS v LIMIT 1001",
        "22023",
    ),
    ('SELECT pg_catalog."jsonb_each_text"(\'[1, "two", null]\'::jsonb) AS v', "22023"),
    ("SELECT pg_catalog.\"jsonb_each_text\"('3'::jsonb) AS v", "22023"),
    ("SELECT pg_catalog.\"jsonb_each_text\"('[]'::jsonb) AS v LIMIT 1001", "22023"),
    ('SELECT pg_catalog."jsonb_each_text"(\'"日本語😀"\'::jsonb) AS v LIMIT 1001', "22023"),
    (
        "SELECT pg_catalog.\"jsonb_each_text\"('12345678901234567890123456789'::jsonb) AS v LIMIT 1001",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_insert\"('3'::jsonb, '{x,NULL,z}'::text[], '3'::jsonb, true::boolean) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_insert\"('[]'::jsonb, '{a,b,c}'::text[], '{\"a\": 1, \"b\": [1, 2]}'::jsonb, true::boolean) AS v LIMIT 1001",
        "22P02",
    ),
    ("SELECT pg_catalog.\"jsonb_object\"('{a,b,c}'::text[]) AS v", "2202E"),
    ("SELECT pg_catalog.\"jsonb_object\"('{x,NULL,z}'::text[]) AS v", "2202E"),
    ('SELECT pg_catalog."jsonb_object"(\'{"",日本語😀,é}\'::text[]) AS v LIMIT 1001', "2202E"),
    ("SELECT pg_catalog.\"jsonb_object\"('{NULL}'::text[]) AS v LIMIT 1001", "2202E"),
    (
        "SELECT pg_catalog.\"jsonb_object\"('{x,NULL,z}'::text[], '{x,NULL,z}'::text[]) AS v",
        "22004",
    ),
    (
        "SELECT pg_catalog.\"jsonb_object\"('{NULL}'::text[], '{a,b,c}'::text[]) AS v LIMIT 1001",
        "2202E",
    ),
    (
        "SELECT pg_catalog.\"jsonb_object\"('{a,b,c}'::text[], '{NULL}'::text[]) AS v LIMIT 1001",
        "2202E",
    ),
    (
        "SELECT pg_catalog.\"jsonb_object\"('{{a,b},{c,d}}'::text[], '{a,b,c}'::text[]) AS v LIMIT 1001",
        "2202E",
    ),
    (
        "SELECT pg_catalog.\"jsonb_object\"('{a,b,c}'::text[], '{{a,b},{c,d}}'::text[]) AS v LIMIT 1001",
        "2202E",
    ),
    ('SELECT pg_catalog."jsonb_object_agg"(NULL::integer, NULL::integer) AS v', "22023"),
    (
        'SELECT pg_catalog."jsonb_object_agg"(c1, c2 ORDER BY c1) AS v FROM (VALUES (NULL::integer, 3::integer), (NULL::integer, 3::integer)) AS t(c1, c2) LIMIT 1001',
        "22023",
    ),
    ('SELECT pg_catalog."jsonb_object_agg_strict"(NULL::integer, NULL::integer) AS v', "22023"),
    (
        'SELECT pg_catalog."jsonb_object_agg_strict"(c1, c2 ORDER BY c1) AS v FROM (VALUES (NULL::integer, 3::integer), (NULL::integer, 3::integer)) AS t(c1, c2) LIMIT 1001',
        "22023",
    ),
    ('SELECT pg_catalog."jsonb_object_agg_unique"(NULL::integer, NULL::integer) AS v', "22023"),
    (
        'SELECT pg_catalog."jsonb_object_agg_unique"(c1, c2 ORDER BY c1) AS v FROM (VALUES (NULL::integer, 3::integer), (NULL::integer, 3::integer)) AS t(c1, c2) LIMIT 1001',
        "22023",
    ),
    (
        'SELECT pg_catalog."jsonb_object_agg_unique"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, NULL::integer), (3::integer, NULL::integer)) AS t(c1, c2) LIMIT 1001',
        "22030",
    ),
    (
        "SELECT pg_catalog.\"jsonb_object_agg_unique\"(c1, c2 ORDER BY c1) AS v FROM (VALUES ('-2147483648'::integer, 3::integer), ('-2147483648'::integer, 3::integer)) AS t(c1, c2) LIMIT 1001",
        "22030",
    ),
    (
        "SELECT pg_catalog.\"jsonb_object_agg_unique\"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, '-2147483648'::integer), (3::integer, '-2147483648'::integer)) AS t(c1, c2) LIMIT 1001",
        "22030",
    ),
    (
        "SELECT pg_catalog.\"jsonb_object_agg_unique\"(c1, c2 ORDER BY c1) AS v FROM (VALUES ('2147483647'::integer, 3::integer), ('2147483647'::integer, 3::integer)) AS t(c1, c2) LIMIT 1001",
        "22030",
    ),
    (
        "SELECT pg_catalog.\"jsonb_object_agg_unique\"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, '2147483647'::integer), (3::integer, '2147483647'::integer)) AS t(c1, c2) LIMIT 1001",
        "22030",
    ),
    (
        "SELECT pg_catalog.\"jsonb_object_agg_unique\"(c1, c2 ORDER BY c1) AS v FROM (VALUES ('0'::integer, 3::integer), ('0'::integer, 3::integer)) AS t(c1, c2) LIMIT 1001",
        "22030",
    ),
    (
        "SELECT pg_catalog.\"jsonb_object_agg_unique\"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, '0'::integer), (3::integer, '0'::integer)) AS t(c1, c2) LIMIT 1001",
        "22030",
    ),
    (
        'SELECT pg_catalog."jsonb_object_agg_unique_strict"(NULL::integer, NULL::integer) AS v',
        "22023",
    ),
    (
        'SELECT pg_catalog."jsonb_object_agg_unique_strict"(c1, c2 ORDER BY c1) AS v FROM (VALUES (NULL::integer, 3::integer), (NULL::integer, 3::integer)) AS t(c1, c2) LIMIT 1001',
        "22023",
    ),
    (
        'SELECT pg_catalog."jsonb_object_agg_unique_strict"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, NULL::integer), (3::integer, NULL::integer)) AS t(c1, c2) LIMIT 1001',
        "22030",
    ),
    (
        "SELECT pg_catalog.\"jsonb_object_agg_unique_strict\"(c1, c2 ORDER BY c1) AS v FROM (VALUES ('-2147483648'::integer, 3::integer), ('-2147483648'::integer, 3::integer)) AS t(c1, c2) LIMIT 1001",
        "22030",
    ),
    (
        "SELECT pg_catalog.\"jsonb_object_agg_unique_strict\"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, '-2147483648'::integer), (3::integer, '-2147483648'::integer)) AS t(c1, c2) LIMIT 1001",
        "22030",
    ),
    (
        "SELECT pg_catalog.\"jsonb_object_agg_unique_strict\"(c1, c2 ORDER BY c1) AS v FROM (VALUES ('2147483647'::integer, 3::integer), ('2147483647'::integer, 3::integer)) AS t(c1, c2) LIMIT 1001",
        "22030",
    ),
    (
        "SELECT pg_catalog.\"jsonb_object_agg_unique_strict\"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, '2147483647'::integer), (3::integer, '2147483647'::integer)) AS t(c1, c2) LIMIT 1001",
        "22030",
    ),
    (
        "SELECT pg_catalog.\"jsonb_object_agg_unique_strict\"(c1, c2 ORDER BY c1) AS v FROM (VALUES ('0'::integer, 3::integer), ('0'::integer, 3::integer)) AS t(c1, c2) LIMIT 1001",
        "22030",
    ),
    (
        "SELECT pg_catalog.\"jsonb_object_agg_unique_strict\"(c1, c2 ORDER BY c1) AS v FROM (VALUES (3::integer, '0'::integer), (3::integer, '0'::integer)) AS t(c1, c2) LIMIT 1001",
        "22030",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_exists\"('[1, \"two\", null]'::jsonb, '$[0]'::jsonpath, '[1, \"two\", null]'::jsonb, false::boolean) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_exists\"('3'::jsonb, '$'::jsonpath, '3'::jsonb, true::boolean) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_exists\"('{\"a\": 1, \"b\": [1, 2]}'::jsonb, '$.a'::jsonpath, '[]'::jsonb, true::boolean) AS v LIMIT 1001",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_exists_tz\"('[1, \"two\", null]'::jsonb, '$[0]'::jsonpath, '[1, \"two\", null]'::jsonb, false::boolean) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_exists_tz\"('3'::jsonb, '$'::jsonpath, '3'::jsonb, true::boolean) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_exists_tz\"('{\"a\": 1, \"b\": [1, 2]}'::jsonb, '$.a'::jsonpath, '[]'::jsonb, true::boolean) AS v LIMIT 1001",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_match\"('[1, \"two\", null]'::jsonb, '$[0]'::jsonpath, '[1, \"two\", null]'::jsonb, false::boolean) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_match\"('3'::jsonb, '$'::jsonpath, '3'::jsonb, true::boolean) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_match\"('{\"a\": 1, \"b\": [1, 2]}'::jsonb, '$.a'::jsonpath, '[]'::jsonb, true::boolean) AS v LIMIT 1001",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_match_tz\"('[1, \"two\", null]'::jsonb, '$[0]'::jsonpath, '[1, \"two\", null]'::jsonb, false::boolean) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_match_tz\"('3'::jsonb, '$'::jsonpath, '3'::jsonb, true::boolean) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_match_tz\"('{\"a\": 1, \"b\": [1, 2]}'::jsonb, '$.a'::jsonpath, '[]'::jsonb, true::boolean) AS v LIMIT 1001",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query\"('[1, \"two\", null]'::jsonb, '$[0]'::jsonpath, '[1, \"two\", null]'::jsonb, false::boolean) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query\"('3'::jsonb, '$'::jsonpath, '3'::jsonb, true::boolean) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query\"('{\"a\": 1, \"b\": [1, 2]}'::jsonb, '$.a'::jsonpath, '[]'::jsonb, true::boolean) AS v LIMIT 1001",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_array\"('[1, \"two\", null]'::jsonb, '$[0]'::jsonpath, '[1, \"two\", null]'::jsonb, false::boolean) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_array\"('3'::jsonb, '$'::jsonpath, '3'::jsonb, true::boolean) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_array\"('{\"a\": 1, \"b\": [1, 2]}'::jsonb, '$.a'::jsonpath, '[]'::jsonb, true::boolean) AS v LIMIT 1001",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_array_tz\"('[1, \"two\", null]'::jsonb, '$[0]'::jsonpath, '[1, \"two\", null]'::jsonb, false::boolean) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_array_tz\"('3'::jsonb, '$'::jsonpath, '3'::jsonb, true::boolean) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_array_tz\"('{\"a\": 1, \"b\": [1, 2]}'::jsonb, '$.a'::jsonpath, '[]'::jsonb, true::boolean) AS v LIMIT 1001",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_first\"('[1, \"two\", null]'::jsonb, '$[0]'::jsonpath, '[1, \"two\", null]'::jsonb, false::boolean) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_first\"('3'::jsonb, '$'::jsonpath, '3'::jsonb, true::boolean) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_first\"('{\"a\": 1, \"b\": [1, 2]}'::jsonb, '$.a'::jsonpath, '[]'::jsonb, true::boolean) AS v LIMIT 1001",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_first_tz\"('[1, \"two\", null]'::jsonb, '$[0]'::jsonpath, '[1, \"two\", null]'::jsonb, false::boolean) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_first_tz\"('3'::jsonb, '$'::jsonpath, '3'::jsonb, true::boolean) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_first_tz\"('{\"a\": 1, \"b\": [1, 2]}'::jsonb, '$.a'::jsonpath, '[]'::jsonb, true::boolean) AS v LIMIT 1001",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_tz\"('[1, \"two\", null]'::jsonb, '$[0]'::jsonpath, '[1, \"two\", null]'::jsonb, false::boolean) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_tz\"('3'::jsonb, '$'::jsonpath, '3'::jsonb, true::boolean) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_path_query_tz\"('{\"a\": 1, \"b\": [1, 2]}'::jsonb, '$.a'::jsonpath, '[]'::jsonb, true::boolean) AS v LIMIT 1001",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_set\"('3'::jsonb, '{x,NULL,z}'::text[], '3'::jsonb, true::boolean) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_set\"('[]'::jsonb, '{a,b,c}'::text[], '{\"a\": 1, \"b\": [1, 2]}'::jsonb, true::boolean) AS v LIMIT 1001",
        "22P02",
    ),
    (
        "SELECT pg_catalog.\"jsonb_set_lax\"('3'::jsonb, '{x,NULL,z}'::text[], '3'::jsonb, true::boolean, '12'::text) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"jsonb_set_lax\"('{\"a\": 1, \"b\": [1, 2]}'::jsonb, '{a,b,c}'::text[], NULL::jsonb, true::boolean, 'abc'::text) AS v LIMIT 1001",
        "22023",
    ),
    (
        'SELECT pg_catalog."jsonb_set_lax"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{a,b,c}\'::text[], \'{"a": 1, "b": [1, 2]}\'::jsonb, true::boolean, NULL::text) AS v LIMIT 1001',
        "22023",
    ),
    ("SELECT pg_catalog.\"to_json\"('2147483648'::integer) AS v LIMIT 1001", "22003"),
    ("SELECT pg_catalog.\"to_jsonb\"('2147483648'::integer) AS v LIMIT 1001", "22003"),
    ("SELECT json_build_object(NULL::text, 1)", "22004"),
    ("SELECT json_build_object('a'::text)", "22023"),
    ("SELECT json_build_object('{\"a\":1}'::json, 1)", "22023"),
    ("SELECT json_object_agg(NULL::text, 1::integer)", "22004"),
    (
        "SELECT json_object_agg_unique(c1, c2) FROM (VALUES ('k'::text, 1::integer), ('k'::text, 2::integer)) t(c1, c2)",
        "22030",
    ),
    (
        "SELECT jsonb_object_agg_unique(c1, c2) FROM (VALUES ('k'::text, 1::integer), ('k'::text, 2::integer)) t(c1, c2)",
        "22030",
    ),
    ("SELECT jsonb_delete('3'::jsonb, VARIADIC '{{a,b},{c,d}}'::text[])", "2202E"),
]


@pytest.fixture(scope="module")
def runtime() -> ClickHouseFederationRuntime:
    return ClickHouseFederationRuntime.embedded()


def _answers(runtime: ClickHouseFederationRuntime, sql: str) -> list[object]:
    return [row[0] for row in runtime.run_sync(transpile(sql, "clickhouse")).iter_rows()]


def _text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "t" if value else "f"
    return str(value)


def _record(text: str) -> list[str] | None:
    """A record's fields, unquoted as record_in reads them; None for no record."""
    if len(text) < 2 or text[0] != "(" or text[-1] != ")":
        return None
    fields, field, quoted, i, body = [], "", False, 1, text[:-1]
    while i < len(body):
        pair = body[i : i + 2]
        if quoted and pair in ('""', "\\\\"):
            field += body[i]
            i += 2
            continue
        if body[i] == '"':
            quoted = not quoted
        elif body[i] == "," and not quoted:
            fields.append(field)
            field = ""
        else:
            field += body[i]
        i += 1
    return [*fields, field]


def _same(want: str | None, got: str | None) -> bool:
    """The measure's comparison of a JSON answer: parsed, numbers by value; a record field by
    field."""
    if want == got:
        return True
    if want is None or got is None:
        return False
    try:
        return json.loads(want, parse_float=Decimal) == json.loads(got, parse_float=Decimal)
    except ValueError:
        pass
    a, b = _record(want), _record(got)
    return a is not None and b is not None and len(a) == len(b) and all(map(_same, a, b))


@pytest.mark.parametrize(("sql", "want"), ANSWERED)
def test_clickhouse_answers_postgresql(runtime, sql, want):
    got = [_text(v) for v in _answers(runtime, sql)]
    assert len(got) == len(want), (got, want)
    if "ORDER BY" not in sql and len(want) > 1:  # rows of no order compare as a set
        assert sorted(map(str, got)) == sorted(map(str, want)) or all(map(_same, want, got))
    else:
        assert all(map(_same, want, got)), (got, want)


@pytest.mark.parametrize(("sql", "sqlstate"), RAISED)
def test_clickhouse_raises_postgresqls_class(runtime, sql, sqlstate):
    try:
        _answers(runtime, sql)
    except FunctionRefused as refused:  # refused as the statement is read: the row's own error
        assert "no form" not in str(refused)
        assert getattr(refused, "sqlstate", "")[:2] == sqlstate[:2]
        return
    except Exception as raised:  # noqa: BLE001 -- the engine's raise carries PostgreSQL's error
        assert f"postgresql-error {sqlstate[:2]}" in str(raised), raised
        return
    pytest.fail(f"answered where PostgreSQL raises {sqlstate}")
