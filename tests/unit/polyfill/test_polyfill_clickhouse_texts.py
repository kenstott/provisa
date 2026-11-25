# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6b9c21-8d4e-4a57-b0e2-71c5d9a8e463
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse's rows of text functions, regular expressions, a jsonb number's integers and double,
int8 of an oid and an oid's literal (REQ-2236; REQ-2167, AN ANSWER WITHIN A STATED BOUND COMES
BEFORE A REFUSAL): each answered on the embedded engine (chdb), against PostgreSQL's answer. Each
overload was refused by name on ClickHouse on the base.

The statements are the function support measure's own calls of each overload (its NULL call,
samples and edge values, function_support.calls and edge_calls) and the edges the rows read
(a multi-byte set, a start past the text, an exponent), answered by the embedded PostgreSQL the
unit tests boot (PostgreSQL 16.11, its text output)."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import pytest

from provisa.core.pg_error import from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import FunctionRefused, RowRefused
from provisa.transpiler.transpile import transpile

ANSWERED = [  # (statement, PostgreSQL's text of its one row)
    ('SELECT pg_catalog."btrim"(NULL::text, NULL::text) AS v', None),
    ("SELECT pg_catalog.\"btrim\"('abc'::text, 'abc'::text) AS v", ""),
    ("SELECT pg_catalog.\"btrim\"('Hello World'::text, 'Hello World'::text) AS v", ""),
    ("SELECT pg_catalog.\"btrim\"('12'::text, '12'::text) AS v", ""),
    ("SELECT pg_catalog.\"btrim\"(NULL::text, 'abc'::text) AS v", None),
    ("SELECT pg_catalog.\"btrim\"('abc'::text, NULL::text) AS v", None),
    ("SELECT pg_catalog.\"btrim\"(''::text, 'abc'::text) AS v", ""),
    ("SELECT pg_catalog.\"btrim\"('abc'::text, ''::text) AS v", "abc"),
    ("SELECT pg_catalog.\"btrim\"('日本語テキスト😀'::text, 'abc'::text) AS v", "日本語テキスト😀"),
    ("SELECT pg_catalog.\"btrim\"('abc'::text, '日本語テキスト😀'::text) AS v", "abc"),
    ("SELECT pg_catalog.\"btrim\"('été'::text, 'abc'::text) AS v", "été"),
    ("SELECT pg_catalog.\"btrim\"('abc'::text, 'été'::text) AS v", "abc"),
    ('SELECT pg_catalog."ltrim"(NULL::text, NULL::text) AS v', None),
    ("SELECT pg_catalog.\"ltrim\"('abc'::text, 'abc'::text) AS v", ""),
    ("SELECT pg_catalog.\"ltrim\"('Hello World'::text, 'Hello World'::text) AS v", ""),
    ("SELECT pg_catalog.\"ltrim\"('12'::text, '12'::text) AS v", ""),
    ("SELECT pg_catalog.\"ltrim\"(NULL::text, 'abc'::text) AS v", None),
    ("SELECT pg_catalog.\"ltrim\"('abc'::text, NULL::text) AS v", None),
    ("SELECT pg_catalog.\"ltrim\"(''::text, 'abc'::text) AS v", ""),
    ("SELECT pg_catalog.\"ltrim\"('abc'::text, ''::text) AS v", "abc"),
    ("SELECT pg_catalog.\"ltrim\"('日本語テキスト😀'::text, 'abc'::text) AS v", "日本語テキスト😀"),
    ("SELECT pg_catalog.\"ltrim\"('abc'::text, '日本語テキスト😀'::text) AS v", "abc"),
    ("SELECT pg_catalog.\"ltrim\"('été'::text, 'abc'::text) AS v", "été"),
    ("SELECT pg_catalog.\"ltrim\"('abc'::text, 'été'::text) AS v", "abc"),
    ('SELECT pg_catalog."rtrim"(NULL::text, NULL::text) AS v', None),
    ("SELECT pg_catalog.\"rtrim\"('abc'::text, 'abc'::text) AS v", ""),
    ("SELECT pg_catalog.\"rtrim\"('Hello World'::text, 'Hello World'::text) AS v", ""),
    ("SELECT pg_catalog.\"rtrim\"('12'::text, '12'::text) AS v", ""),
    ("SELECT pg_catalog.\"rtrim\"(NULL::text, 'abc'::text) AS v", None),
    ("SELECT pg_catalog.\"rtrim\"('abc'::text, NULL::text) AS v", None),
    ("SELECT pg_catalog.\"rtrim\"(''::text, 'abc'::text) AS v", ""),
    ("SELECT pg_catalog.\"rtrim\"('abc'::text, ''::text) AS v", "abc"),
    ("SELECT pg_catalog.\"rtrim\"('日本語テキスト😀'::text, 'abc'::text) AS v", "日本語テキスト😀"),
    ("SELECT pg_catalog.\"rtrim\"('abc'::text, '日本語テキスト😀'::text) AS v", "abc"),
    ("SELECT pg_catalog.\"rtrim\"('été'::text, 'abc'::text) AS v", "été"),
    ("SELECT pg_catalog.\"rtrim\"('abc'::text, 'été'::text) AS v", "abc"),
    ('SELECT pg_catalog."translate"(NULL::text, NULL::text, NULL::text) AS v', None),
    ("SELECT pg_catalog.\"translate\"('abc'::text, 'abc'::text, 'abc'::text) AS v", "abc"),
    (
        "SELECT pg_catalog.\"translate\"('Hello World'::text, 'Hello World'::text, 'Hello World'::text) AS v",
        "Hello World",
    ),
    ("SELECT pg_catalog.\"translate\"('12'::text, '12'::text, '12'::text) AS v", "12"),
    ("SELECT pg_catalog.\"translate\"(NULL::text, 'abc'::text, 'abc'::text) AS v", None),
    ("SELECT pg_catalog.\"translate\"('abc'::text, NULL::text, 'abc'::text) AS v", None),
    ("SELECT pg_catalog.\"translate\"('abc'::text, 'abc'::text, NULL::text) AS v", None),
    ("SELECT pg_catalog.\"translate\"(''::text, 'abc'::text, 'abc'::text) AS v", ""),
    ("SELECT pg_catalog.\"translate\"('abc'::text, ''::text, 'abc'::text) AS v", "abc"),
    ("SELECT pg_catalog.\"translate\"('abc'::text, 'abc'::text, ''::text) AS v", ""),
    (
        "SELECT pg_catalog.\"translate\"('日本語テキスト😀'::text, 'abc'::text, 'abc'::text) AS v",
        "日本語テキスト😀",
    ),
    (
        "SELECT pg_catalog.\"translate\"('abc'::text, '日本語テキスト😀'::text, 'abc'::text) AS v",
        "abc",
    ),
    ('SELECT pg_catalog."regexp_count"(NULL::text, NULL::text, NULL::integer) AS v', None),
    ("SELECT pg_catalog.\"regexp_count\"('abc'::text, 'b'::text, 3::integer) AS v", "0"),
    ("SELECT pg_catalog.\"regexp_count\"('12'::text, '^1'::text, 42::integer) AS v", "0"),
    ("SELECT pg_catalog.\"regexp_count\"(NULL::text, 'b'::text, 3::integer) AS v", None),
    ("SELECT pg_catalog.\"regexp_count\"('abc'::text, 'b'::text, NULL::integer) AS v", None),
    ("SELECT pg_catalog.\"regexp_count\"(''::text, 'b'::text, 3::integer) AS v", "0"),
    (
        "SELECT pg_catalog.\"regexp_count\"('日本語テキスト😀'::text, 'b'::text, 3::integer) AS v",
        "0",
    ),
    ("SELECT pg_catalog.\"regexp_count\"('abc'::text, 'b'::text, '2147483647'::integer) AS v", "0"),
    ("SELECT pg_catalog.\"regexp_count\"('été'::text, 'b'::text, 3::integer) AS v", "0"),
    (
        'SELECT pg_catalog."regexp_count"(NULL::text, NULL::text, NULL::integer, NULL::text) AS v',
        None,
    ),
    ("SELECT pg_catalog.\"regexp_count\"('abc'::text, 'b'::text, 1::integer, 'i'::text) AS v", "1"),
    (
        "SELECT pg_catalog.\"regexp_count\"('Hello World'::text, '[a-z]+'::text, 2::integer, 'c'::text) AS v",
        "2",
    ),
    ("SELECT pg_catalog.\"regexp_count\"('12'::text, '^1'::text, 1::integer, 'i'::text) AS v", "1"),
    ("SELECT pg_catalog.\"regexp_count\"(NULL::text, 'b'::text, 1::integer, 'i'::text) AS v", None),
    ("SELECT pg_catalog.\"regexp_count\"(''::text, 'b'::text, 1::integer, 'i'::text) AS v", "0"),
    (
        "SELECT pg_catalog.\"regexp_count\"('日本語テキスト😀'::text, 'b'::text, 1::integer, 'i'::text) AS v",
        "0",
    ),
    ("SELECT pg_catalog.\"regexp_count\"('été'::text, 'b'::text, 1::integer, 'i'::text) AS v", "0"),
    (
        "SELECT pg_catalog.\"regexp_count\"('  padded  '::text, 'b'::text, 1::integer, 'i'::text) AS v",
        "0",
    ),
    ('SELECT pg_catalog."regexp_instr"(NULL::text, NULL::text) AS v', None),
    ("SELECT pg_catalog.\"regexp_instr\"('abc'::text, 'b'::text) AS v", "2"),
    ("SELECT pg_catalog.\"regexp_instr\"('Hello World'::text, '[a-z]+'::text) AS v", "2"),
    ("SELECT pg_catalog.\"regexp_instr\"('12'::text, '^1'::text) AS v", "1"),
    ("SELECT pg_catalog.\"regexp_instr\"(NULL::text, 'b'::text) AS v", None),
    ("SELECT pg_catalog.\"regexp_instr\"(''::text, 'b'::text) AS v", "0"),
    ("SELECT pg_catalog.\"regexp_instr\"('日本語テキスト😀'::text, 'b'::text) AS v", "0"),
    ("SELECT pg_catalog.\"regexp_instr\"('été'::text, 'b'::text) AS v", "0"),
    ("SELECT pg_catalog.\"regexp_instr\"('  padded  '::text, 'b'::text) AS v", "0"),
    ('SELECT pg_catalog."regexp_instr"(NULL::text, NULL::text, NULL::integer) AS v', None),
    ("SELECT pg_catalog.\"regexp_instr\"('abc'::text, 'b'::text, 3::integer) AS v", "0"),
    ("SELECT pg_catalog.\"regexp_instr\"('12'::text, '^1'::text, 42::integer) AS v", "0"),
    ("SELECT pg_catalog.\"regexp_instr\"(NULL::text, 'b'::text, 3::integer) AS v", None),
    ("SELECT pg_catalog.\"regexp_instr\"('abc'::text, 'b'::text, NULL::integer) AS v", None),
    ("SELECT pg_catalog.\"regexp_instr\"(''::text, 'b'::text, 3::integer) AS v", "0"),
    (
        "SELECT pg_catalog.\"regexp_instr\"('日本語テキスト😀'::text, 'b'::text, 3::integer) AS v",
        "0",
    ),
    ("SELECT pg_catalog.\"regexp_instr\"('abc'::text, 'b'::text, '2147483647'::integer) AS v", "0"),
    ("SELECT pg_catalog.\"regexp_instr\"('été'::text, 'b'::text, 3::integer) AS v", "0"),
    (
        'SELECT pg_catalog."regexp_instr"(NULL::text, NULL::text, NULL::integer, NULL::integer) AS v',
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('abc'::text, 'b'::text, 3::integer, 3::integer) AS v",
        "0",
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('12'::text, '^1'::text, 42::integer, 42::integer) AS v",
        "0",
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"(NULL::text, 'b'::text, 3::integer, 3::integer) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('abc'::text, 'b'::text, NULL::integer, 3::integer) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('abc'::text, 'b'::text, 3::integer, NULL::integer) AS v",
        None,
    ),
    ("SELECT pg_catalog.\"regexp_instr\"(''::text, 'b'::text, 3::integer, 3::integer) AS v", "0"),
    (
        "SELECT pg_catalog.\"regexp_instr\"('日本語テキスト😀'::text, 'b'::text, 3::integer, 3::integer) AS v",
        "0",
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('abc'::text, 'b'::text, '2147483647'::integer, 3::integer) AS v",
        "0",
    ),
    (
        'SELECT pg_catalog."regexp_instr"(NULL::text, NULL::text, NULL::integer, NULL::integer, NULL::integer) AS v',
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('abc'::text, 'b'::text, 1::integer, 1::integer, 0::integer) AS v",
        "2",
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('Hello World'::text, '[a-z]+'::text, 2::integer, 1::integer, 1::integer) AS v",
        "6",
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('12'::text, '^1'::text, 1::integer, 2::integer, 0::integer) AS v",
        "0",
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"(NULL::text, 'b'::text, 1::integer, 1::integer, 0::integer) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"(''::text, 'b'::text, 1::integer, 1::integer, 0::integer) AS v",
        "0",
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('日本語テキスト😀'::text, 'b'::text, 1::integer, 1::integer, 0::integer) AS v",
        "0",
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('été'::text, 'b'::text, 1::integer, 1::integer, 0::integer) AS v",
        "0",
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('  padded  '::text, 'b'::text, 1::integer, 1::integer, 0::integer) AS v",
        "0",
    ),
    (
        'SELECT pg_catalog."regexp_instr"(NULL::text, NULL::text, NULL::integer, NULL::integer, NULL::integer, NULL::text) AS v',
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('abc'::text, 'b'::text, 1::integer, 1::integer, 0::integer, 'i'::text) AS v",
        "2",
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('Hello World'::text, '[a-z]+'::text, 2::integer, 1::integer, 1::integer, 'c'::text) AS v",
        "6",
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('12'::text, '^1'::text, 1::integer, 2::integer, 0::integer, 'i'::text) AS v",
        "0",
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"(NULL::text, 'b'::text, 1::integer, 1::integer, 0::integer, 'i'::text) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"(''::text, 'b'::text, 1::integer, 1::integer, 0::integer, 'i'::text) AS v",
        "0",
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('日本語テキスト😀'::text, 'b'::text, 1::integer, 1::integer, 0::integer, 'i'::text) AS v",
        "0",
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('été'::text, 'b'::text, 1::integer, 1::integer, 0::integer, 'i'::text) AS v",
        "0",
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('  padded  '::text, 'b'::text, 1::integer, 1::integer, 0::integer, 'i'::text) AS v",
        "0",
    ),
    (
        'SELECT pg_catalog."regexp_instr"(NULL::text, NULL::text, NULL::integer, NULL::integer, NULL::integer, NULL::text, NULL::integer) AS v',
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('abc'::text, 'b'::text, 1::integer, 1::integer, 0::integer, 'i'::text, 0::integer) AS v",
        "2",
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('Hello World'::text, '[a-z]+'::text, 2::integer, 1::integer, 1::integer, 'c'::text, 0::integer) AS v",
        "6",
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('12'::text, '^1'::text, 1::integer, 2::integer, 0::integer, 'i'::text, 0::integer) AS v",
        "0",
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"(NULL::text, 'b'::text, 1::integer, 1::integer, 0::integer, 'i'::text, 0::integer) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"(''::text, 'b'::text, 1::integer, 1::integer, 0::integer, 'i'::text, 0::integer) AS v",
        "0",
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('日本語テキスト😀'::text, 'b'::text, 1::integer, 1::integer, 0::integer, 'i'::text, 0::integer) AS v",
        "0",
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('été'::text, 'b'::text, 1::integer, 1::integer, 0::integer, 'i'::text, 0::integer) AS v",
        "0",
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('  padded  '::text, 'b'::text, 1::integer, 1::integer, 0::integer, 'i'::text, 0::integer) AS v",
        "0",
    ),
    ('SELECT pg_catalog."regexp_like"(NULL::text, NULL::text, NULL::text) AS v', None),
    ("SELECT pg_catalog.\"regexp_like\"('abc'::text, 'b'::text, 'i'::text) AS v", "t"),
    ("SELECT pg_catalog.\"regexp_like\"('Hello World'::text, '[a-z]+'::text, 'c'::text) AS v", "t"),
    ("SELECT pg_catalog.\"regexp_like\"('12'::text, '^1'::text, 'i'::text) AS v", "t"),
    ("SELECT pg_catalog.\"regexp_like\"(NULL::text, 'b'::text, 'i'::text) AS v", None),
    ("SELECT pg_catalog.\"regexp_like\"(''::text, 'b'::text, 'i'::text) AS v", "f"),
    ("SELECT pg_catalog.\"regexp_like\"('日本語テキスト😀'::text, 'b'::text, 'i'::text) AS v", "f"),
    ("SELECT pg_catalog.\"regexp_like\"('été'::text, 'b'::text, 'i'::text) AS v", "f"),
    ("SELECT pg_catalog.\"regexp_like\"('  padded  '::text, 'b'::text, 'i'::text) AS v", "f"),
    ('SELECT pg_catalog."regexp_split_to_array"(NULL::text, NULL::text) AS v', None),
    ("SELECT pg_catalog.\"regexp_split_to_array\"('abc'::text, 'b'::text) AS v", "{a,c}"),
    (
        "SELECT pg_catalog.\"regexp_split_to_array\"('Hello World'::text, '[a-z]+'::text) AS v",
        '{H," W",""}',
    ),
    ("SELECT pg_catalog.\"regexp_split_to_array\"('12'::text, '^1'::text) AS v", '{"",2}'),
    ("SELECT pg_catalog.\"regexp_split_to_array\"(NULL::text, 'b'::text) AS v", None),
    ("SELECT pg_catalog.\"regexp_split_to_array\"(''::text, 'b'::text) AS v", '{""}'),
    (
        "SELECT pg_catalog.\"regexp_split_to_array\"('日本語テキスト😀'::text, 'b'::text) AS v",
        "{日本語テキスト😀}",
    ),
    ("SELECT pg_catalog.\"regexp_split_to_array\"('été'::text, 'b'::text) AS v", "{été}"),
    (
        "SELECT pg_catalog.\"regexp_split_to_array\"('  padded  '::text, 'b'::text) AS v",
        '{"  padded  "}',
    ),
    ('SELECT pg_catalog."regexp_split_to_array"(NULL::text, NULL::text, NULL::text) AS v', None),
    (
        "SELECT pg_catalog.\"regexp_split_to_array\"('abc'::text, 'b'::text, 'i'::text) AS v",
        "{a,c}",
    ),
    (
        "SELECT pg_catalog.\"regexp_split_to_array\"('Hello World'::text, '[a-z]+'::text, 'c'::text) AS v",
        '{H," W",""}',
    ),
    (
        "SELECT pg_catalog.\"regexp_split_to_array\"('12'::text, '^1'::text, 'i'::text) AS v",
        '{"",2}',
    ),
    ("SELECT pg_catalog.\"regexp_split_to_array\"(NULL::text, 'b'::text, 'i'::text) AS v", None),
    ("SELECT pg_catalog.\"regexp_split_to_array\"(''::text, 'b'::text, 'i'::text) AS v", '{""}'),
    (
        "SELECT pg_catalog.\"regexp_split_to_array\"('日本語テキスト😀'::text, 'b'::text, 'i'::text) AS v",
        "{日本語テキスト😀}",
    ),
    (
        "SELECT pg_catalog.\"regexp_split_to_array\"('été'::text, 'b'::text, 'i'::text) AS v",
        "{été}",
    ),
    (
        "SELECT pg_catalog.\"regexp_split_to_array\"('  padded  '::text, 'b'::text, 'i'::text) AS v",
        '{"  padded  "}',
    ),
    ('SELECT pg_catalog."regexp_substr"(NULL::text, NULL::text) AS v', None),
    ("SELECT pg_catalog.\"regexp_substr\"('abc'::text, 'b'::text) AS v", "b"),
    ("SELECT pg_catalog.\"regexp_substr\"('Hello World'::text, '[a-z]+'::text) AS v", "ello"),
    ("SELECT pg_catalog.\"regexp_substr\"('12'::text, '^1'::text) AS v", "1"),
    ("SELECT pg_catalog.\"regexp_substr\"(NULL::text, 'b'::text) AS v", None),
    ("SELECT pg_catalog.\"regexp_substr\"(''::text, 'b'::text) AS v", None),
    ("SELECT pg_catalog.\"regexp_substr\"('日本語テキスト😀'::text, 'b'::text) AS v", None),
    ("SELECT pg_catalog.\"regexp_substr\"('été'::text, 'b'::text) AS v", None),
    ("SELECT pg_catalog.\"regexp_substr\"('  padded  '::text, 'b'::text) AS v", None),
    ('SELECT pg_catalog."regexp_substr"(NULL::text, NULL::text, NULL::integer) AS v', None),
    ("SELECT pg_catalog.\"regexp_substr\"('abc'::text, 'b'::text, 3::integer) AS v", None),
    ("SELECT pg_catalog.\"regexp_substr\"('12'::text, '^1'::text, 42::integer) AS v", None),
    ("SELECT pg_catalog.\"regexp_substr\"(NULL::text, 'b'::text, 3::integer) AS v", None),
    ("SELECT pg_catalog.\"regexp_substr\"('abc'::text, 'b'::text, NULL::integer) AS v", None),
    ("SELECT pg_catalog.\"regexp_substr\"(''::text, 'b'::text, 3::integer) AS v", None),
    (
        "SELECT pg_catalog.\"regexp_substr\"('日本語テキスト😀'::text, 'b'::text, 3::integer) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"('abc'::text, 'b'::text, '2147483647'::integer) AS v",
        None,
    ),
    ("SELECT pg_catalog.\"regexp_substr\"('été'::text, 'b'::text, 3::integer) AS v", None),
    (
        'SELECT pg_catalog."regexp_substr"(NULL::text, NULL::text, NULL::integer, NULL::integer) AS v',
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"('abc'::text, 'b'::text, 3::integer, 3::integer) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"('12'::text, '^1'::text, 42::integer, 42::integer) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"(NULL::text, 'b'::text, 3::integer, 3::integer) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"('abc'::text, 'b'::text, NULL::integer, 3::integer) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"('abc'::text, 'b'::text, 3::integer, NULL::integer) AS v",
        None,
    ),
    ("SELECT pg_catalog.\"regexp_substr\"(''::text, 'b'::text, 3::integer, 3::integer) AS v", None),
    (
        "SELECT pg_catalog.\"regexp_substr\"('日本語テキスト😀'::text, 'b'::text, 3::integer, 3::integer) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"('abc'::text, 'b'::text, '2147483647'::integer, 3::integer) AS v",
        None,
    ),
    (
        'SELECT pg_catalog."regexp_substr"(NULL::text, NULL::text, NULL::integer, NULL::integer, NULL::text) AS v',
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"('abc'::text, 'b'::text, 1::integer, 1::integer, 'i'::text) AS v",
        "b",
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"('Hello World'::text, '[a-z]+'::text, 2::integer, 1::integer, 'c'::text) AS v",
        "ello",
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"('12'::text, '^1'::text, 1::integer, 2::integer, 'i'::text) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"(NULL::text, 'b'::text, 1::integer, 1::integer, 'i'::text) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"(''::text, 'b'::text, 1::integer, 1::integer, 'i'::text) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"('日本語テキスト😀'::text, 'b'::text, 1::integer, 1::integer, 'i'::text) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"('été'::text, 'b'::text, 1::integer, 1::integer, 'i'::text) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"('  padded  '::text, 'b'::text, 1::integer, 1::integer, 'i'::text) AS v",
        None,
    ),
    (
        'SELECT pg_catalog."regexp_substr"(NULL::text, NULL::text, NULL::integer, NULL::integer, NULL::text, NULL::integer) AS v',
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"('abc'::text, 'b'::text, 1::integer, 1::integer, 'i'::text, 0::integer) AS v",
        "b",
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"('Hello World'::text, '[a-z]+'::text, 2::integer, 1::integer, 'c'::text, 0::integer) AS v",
        "ello",
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"('12'::text, '^1'::text, 1::integer, 2::integer, 'i'::text, 0::integer) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"(NULL::text, 'b'::text, 1::integer, 1::integer, 'i'::text, 0::integer) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"(''::text, 'b'::text, 1::integer, 1::integer, 'i'::text, 0::integer) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"('日本語テキスト😀'::text, 'b'::text, 1::integer, 1::integer, 'i'::text, 0::integer) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"('été'::text, 'b'::text, 1::integer, 1::integer, 'i'::text, 0::integer) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"('  padded  '::text, 'b'::text, 1::integer, 1::integer, 'i'::text, 0::integer) AS v",
        None,
    ),
    (
        'SELECT pg_catalog."overlay"(NULL::text, NULL::text, NULL::integer, NULL::integer) AS v',
        None,
    ),
    (
        "SELECT pg_catalog.\"overlay\"('abc'::text, 'abc'::text, 1::integer, 1::integer) AS v",
        "abcbc",
    ),
    (
        "SELECT pg_catalog.\"overlay\"('Hello World'::text, 'Hello World'::text, 2::integer, 2::integer) AS v",
        "HHello Worldlo World",
    ),
    ("SELECT pg_catalog.\"overlay\"('12'::text, '12'::text, 1::integer, 1::integer) AS v", "122"),
    ("SELECT pg_catalog.\"overlay\"(NULL::text, 'abc'::text, 1::integer, 1::integer) AS v", None),
    ("SELECT pg_catalog.\"overlay\"('abc'::text, NULL::text, 1::integer, 1::integer) AS v", None),
    ("SELECT pg_catalog.\"overlay\"(''::text, 'abc'::text, 1::integer, 1::integer) AS v", "abc"),
    ("SELECT pg_catalog.\"overlay\"('abc'::text, ''::text, 1::integer, 1::integer) AS v", "bc"),
    (
        "SELECT pg_catalog.\"overlay\"('日本語テキスト😀'::text, 'abc'::text, 1::integer, 1::integer) AS v",
        "abc本語テキスト😀",
    ),
    (
        "SELECT pg_catalog.\"overlay\"('abc'::text, '日本語テキスト😀'::text, 1::integer, 1::integer) AS v",
        "日本語テキスト😀bc",
    ),
    (
        "SELECT pg_catalog.\"overlay\"('été'::text, 'abc'::text, 1::integer, 1::integer) AS v",
        "abćté",
    ),
    (
        "SELECT pg_catalog.\"overlay\"('abc'::text, 'été'::text, 1::integer, 1::integer) AS v",
        "étébc",
    ),
    ('SELECT pg_catalog."overlay"(NULL::text, NULL::text, NULL::integer) AS v', None),
    ("SELECT pg_catalog.\"overlay\"('abc'::text, 'abc'::text, 1::integer) AS v", "abc"),
    (
        "SELECT pg_catalog.\"overlay\"('Hello World'::text, 'Hello World'::text, 2::integer) AS v",
        "HHello World",
    ),
    ("SELECT pg_catalog.\"overlay\"('12'::text, '12'::text, 1::integer) AS v", "12"),
    ("SELECT pg_catalog.\"overlay\"(NULL::text, 'abc'::text, 1::integer) AS v", None),
    ("SELECT pg_catalog.\"overlay\"('abc'::text, NULL::text, 1::integer) AS v", None),
    ("SELECT pg_catalog.\"overlay\"(''::text, 'abc'::text, 1::integer) AS v", "abc"),
    ("SELECT pg_catalog.\"overlay\"('abc'::text, ''::text, 1::integer) AS v", "abc"),
    (
        "SELECT pg_catalog.\"overlay\"('日本語テキスト😀'::text, 'abc'::text, 1::integer) AS v",
        "abcテキスト😀",
    ),
    (
        "SELECT pg_catalog.\"overlay\"('abc'::text, '日本語テキスト😀'::text, 1::integer) AS v",
        "日本語テキスト😀",
    ),
    ("SELECT pg_catalog.\"overlay\"('été'::text, 'abc'::text, 1::integer) AS v", "abcé"),
    ("SELECT pg_catalog.\"overlay\"('abc'::text, 'été'::text, 1::integer) AS v", "été"),
    ('SELECT pg_catalog."string_to_array"(NULL::text, NULL::text, NULL::text) AS v', None),
    (
        "SELECT pg_catalog.\"string_to_array\"('abc'::text, 'abc'::text, 'abc'::text) AS v",
        '{"",""}',
    ),
    (
        "SELECT pg_catalog.\"string_to_array\"('Hello World'::text, 'Hello World'::text, 'Hello World'::text) AS v",
        '{"",""}',
    ),
    ("SELECT pg_catalog.\"string_to_array\"('12'::text, '12'::text, '12'::text) AS v", '{"",""}'),
    ("SELECT pg_catalog.\"string_to_array\"(NULL::text, 'abc'::text, 'abc'::text) AS v", None),
    ("SELECT pg_catalog.\"string_to_array\"('abc'::text, NULL::text, 'abc'::text) AS v", "{a,b,c}"),
    ("SELECT pg_catalog.\"string_to_array\"('abc'::text, 'abc'::text, NULL::text) AS v", '{"",""}'),
    ("SELECT pg_catalog.\"string_to_array\"(''::text, 'abc'::text, 'abc'::text) AS v", "{}"),
    ("SELECT pg_catalog.\"string_to_array\"('abc'::text, ''::text, 'abc'::text) AS v", "{NULL}"),
    (
        "SELECT pg_catalog.\"string_to_array\"('abc'::text, 'abc'::text, ''::text) AS v",
        "{NULL,NULL}",
    ),
    (
        "SELECT pg_catalog.\"string_to_array\"('日本語テキスト😀'::text, 'abc'::text, 'abc'::text) AS v",
        "{日本語テキスト😀}",
    ),
    (
        "SELECT pg_catalog.\"string_to_array\"('abc'::text, '日本語テキスト😀'::text, 'abc'::text) AS v",
        "{NULL}",
    ),
    ('SELECT pg_catalog."string_to_array"(NULL::text, NULL::text) AS v', None),
    ("SELECT pg_catalog.\"string_to_array\"('abc'::text, 'abc'::text) AS v", '{"",""}'),
    (
        "SELECT pg_catalog.\"string_to_array\"('Hello World'::text, 'Hello World'::text) AS v",
        '{"",""}',
    ),
    ("SELECT pg_catalog.\"string_to_array\"('12'::text, '12'::text) AS v", '{"",""}'),
    ("SELECT pg_catalog.\"string_to_array\"(NULL::text, 'abc'::text) AS v", None),
    ("SELECT pg_catalog.\"string_to_array\"('abc'::text, NULL::text) AS v", "{a,b,c}"),
    ("SELECT pg_catalog.\"string_to_array\"(''::text, 'abc'::text) AS v", "{}"),
    ("SELECT pg_catalog.\"string_to_array\"('abc'::text, ''::text) AS v", "{abc}"),
    (
        "SELECT pg_catalog.\"string_to_array\"('日本語テキスト😀'::text, 'abc'::text) AS v",
        "{日本語テキスト😀}",
    ),
    ("SELECT pg_catalog.\"string_to_array\"('abc'::text, '日本語テキスト😀'::text) AS v", "{abc}"),
    ("SELECT pg_catalog.\"string_to_array\"('été'::text, 'abc'::text) AS v", "{été}"),
    ("SELECT pg_catalog.\"string_to_array\"('abc'::text, 'été'::text) AS v", "{abc}"),
    ('SELECT pg_catalog."nameconcatoid"(NULL::name, NULL::oid) AS v', None),
    ("SELECT pg_catalog.\"nameconcatoid\"('abc'::name, 12345::oid) AS v", "abc_12345"),
    ("SELECT pg_catalog.\"nameconcatoid\"('Hello World'::name, 0::oid) AS v", "Hello World_0"),
    ("SELECT pg_catalog.\"nameconcatoid\"('12'::name, 1::oid) AS v", "12_1"),
    ('SELECT pg_catalog."nameconcatoid"(NULL::name, 12345::oid) AS v', None),
    ("SELECT pg_catalog.\"nameconcatoid\"('abc'::name, NULL::oid) AS v", None),
    ("SELECT pg_catalog.\"nameconcatoid\"(''::name, 12345::oid) AS v", "_12345"),
    ("SELECT pg_catalog.\"nameconcatoid\"('abc'::name, '4294967295'::oid) AS v", "abc_4294967295"),
    (
        "SELECT pg_catalog.\"nameconcatoid\"('日本語テキスト😀'::name, 12345::oid) AS v",
        "日本語テキスト😀_12345",
    ),
    ("SELECT pg_catalog.\"nameconcatoid\"('été'::name, 12345::oid) AS v", "été_12345"),
    (
        "SELECT pg_catalog.\"nameconcatoid\"('  padded  '::name, 12345::oid) AS v",
        "  padded  _12345",
    ),
    (
        'SELECT pg_catalog."varchar"(NULL::character varying, NULL::integer, NULL::boolean) AS v',
        None,
    ),
    (
        "SELECT pg_catalog.\"varchar\"('abc'::character varying, 3::integer, true::boolean) AS v",
        "abc",
    ),
    (
        "SELECT pg_catalog.\"varchar\"('Hello World'::character varying, (-7)::integer, false::boolean) AS v",
        "Hello World",
    ),
    (
        "SELECT pg_catalog.\"varchar\"('12'::character varying, 42::integer, true::boolean) AS v",
        "12",
    ),
    ('SELECT pg_catalog."varchar"(NULL::character varying, 3::integer, true::boolean) AS v', None),
    (
        "SELECT pg_catalog.\"varchar\"('abc'::character varying, NULL::integer, true::boolean) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"varchar\"('abc'::character varying, 3::integer, NULL::boolean) AS v",
        None,
    ),
    ("SELECT pg_catalog.\"varchar\"(''::character varying, 3::integer, true::boolean) AS v", ""),
    (
        "SELECT pg_catalog.\"varchar\"('abc'::character varying, '-2147483648'::integer, true::boolean) AS v",
        "abc",
    ),
    (
        "SELECT pg_catalog.\"varchar\"('日本語テキスト😀'::character varying, 3::integer, true::boolean) AS v",
        "日本語テキスト😀",
    ),
    (
        "SELECT pg_catalog.\"varchar\"('abc'::character varying, '2147483647'::integer, true::boolean) AS v",
        "abc",
    ),
    (
        "SELECT pg_catalog.\"varchar\"('été'::character varying, 3::integer, true::boolean) AS v",
        "été",
    ),
    ('SELECT pg_catalog."varchar"(NULL::name) AS v', None),
    ("SELECT pg_catalog.\"varchar\"('abc'::name) AS v", "abc"),
    ("SELECT pg_catalog.\"varchar\"('Hello World'::name) AS v", "Hello World"),
    ("SELECT pg_catalog.\"varchar\"('12'::name) AS v", "12"),
    ("SELECT pg_catalog.\"varchar\"(''::name) AS v", ""),
    ("SELECT pg_catalog.\"varchar\"('日本語テキスト😀'::name) AS v", "日本語テキスト😀"),
    ("SELECT pg_catalog.\"varchar\"('été'::name) AS v", "été"),
    ("SELECT pg_catalog.\"varchar\"('  padded  '::name) AS v", "  padded  "),
    ('SELECT pg_catalog."bpchar"(NULL::character, NULL::integer, NULL::boolean) AS v', None),
    ("SELECT pg_catalog.\"bpchar\"('abc'::character, 3::integer, true::boolean) AS v", "a"),
    (
        "SELECT pg_catalog.\"bpchar\"('Hello World'::character, (-7)::integer, false::boolean) AS v",
        "H",
    ),
    (
        "SELECT pg_catalog.\"bpchar\"('12'::character, 42::integer, true::boolean) AS v",
        "1                                     ",
    ),
    ('SELECT pg_catalog."bpchar"(NULL::character, 3::integer, true::boolean) AS v', None),
    ("SELECT pg_catalog.\"bpchar\"('abc'::character, NULL::integer, true::boolean) AS v", None),
    ("SELECT pg_catalog.\"bpchar\"('abc'::character, 3::integer, NULL::boolean) AS v", None),
    ("SELECT pg_catalog.\"bpchar\"(''::character, 3::integer, true::boolean) AS v", " "),
    (
        "SELECT pg_catalog.\"bpchar\"('abc'::character, '-2147483648'::integer, true::boolean) AS v",
        "a",
    ),
    (
        "SELECT pg_catalog.\"bpchar\"('日本語テキスト😀'::character, 3::integer, true::boolean) AS v",
        "日",
    ),
    ("SELECT pg_catalog.\"bpchar\"('été'::character, 3::integer, true::boolean) AS v", "e"),
    ('SELECT pg_catalog."int2"(NULL::jsonb) AS v', None),
    ("SELECT pg_catalog.\"int2\"('3'::jsonb) AS v", "3"),
    ('SELECT pg_catalog."int4"(NULL::jsonb) AS v', None),
    ("SELECT pg_catalog.\"int4\"('3'::jsonb) AS v", "3"),
    ('SELECT pg_catalog."int8"(NULL::jsonb) AS v', None),
    ("SELECT pg_catalog.\"int8\"('3'::jsonb) AS v", "3"),
    ('SELECT pg_catalog."float8"(NULL::jsonb) AS v', None),
    ("SELECT pg_catalog.\"float8\"('3'::jsonb) AS v", "3"),
    (
        "SELECT pg_catalog.\"float8\"('12345678901234567890123456789'::jsonb) AS v",
        "1.2345678901234568e+28",
    ),
    ('SELECT pg_catalog."int8"(NULL::oid) AS v', None),
    ('SELECT pg_catalog."int8"(12345::oid) AS v', "12345"),
    ('SELECT pg_catalog."int8"(0::oid) AS v', "0"),
    ('SELECT pg_catalog."int8"(1::oid) AS v', "1"),
    ("SELECT pg_catalog.\"int8\"('4294967295'::oid) AS v", "4294967295"),
    ("SELECT pg_catalog.btrim('xx]a-b^c\\]x'::text, ']x\\'::text) AS v", "a-b^c"),
    ("SELECT pg_catalog.rtrim('ab日本日'::text, '日'::text) AS v", "ab日本"),
    ("SELECT pg_catalog.btrim('[[a]]'::text, '[]'::text) AS v", "a"),
    ("SELECT pg_catalog.btrim(E'\\n a \\n'::text, E' \\n'::text) AS v", "a"),
    ("SELECT pg_catalog.translate('abcabc'::text, 'aba'::text, 'xyz'::text) AS v", "xycxyc"),
    ("SELECT pg_catalog.translate('日本語'::text, '本語'::text, 'x'::text) AS v", "日x"),
    ("SELECT pg_catalog.overlay('abc'::text, 'x'::text, 2, -1) AS v", "axabc"),
    ("SELECT pg_catalog.overlay('abc'::text, 'x'::text, 9, 1) AS v", "abcx"),
    ("SELECT pg_catalog.varchar('abcdef'::character varying, 7, true) AS v", "abc"),
    ("SELECT pg_catalog.bpchar('ab'::character(2), 9, false) AS v", "ab   "),
    (
        "SELECT pg_catalog.nameconcatoid(repeat('é', 40)::name, 12::oid) AS v",
        "éééééééééééééééééééééééééééééé_12",
    ),
    ("SELECT '-1'::oid AS v", "4294967295"),
    ("SELECT pg_catalog.regexp_count('abcabcABC'::text, 'b'::text, 1, 'i') AS v", "3"),
    ("SELECT pg_catalog.regexp_count(E'a\\nb'::text, 'a.b'::text, 1) AS v", "1"),
    # A two-argument call of a literal pattern: a newline an ordinary character, as PostgreSQL's
    ("SELECT pg_catalog.regexp_count(E'a\\nb'::text, 'a.b'::text) AS v", "1"),
    ("SELECT pg_catalog.regexp_count('abcb'::text, 'b'::text) AS v", "2"),
    ("SELECT pg_catalog.regexp_instr('日本語日本語'::text, '本'::text, 1, 2) AS v", "5"),
    ("SELECT pg_catalog.regexp_instr('日本語日本語'::text, '本'::text, 3, 1, 1) AS v", "6"),
    ("SELECT pg_catalog.regexp_instr('abcabc'::text, 'b'::text, 1, 3) AS v", "0"),
    ("SELECT pg_catalog.regexp_substr('foo123bar45'::text, '[0-9]+'::text, 1, 2) AS v", "45"),
    ("SELECT pg_catalog.regexp_substr('foo123bar45'::text, '\\d+'::text, 5) AS v", "23"),
    ("SELECT pg_catalog.regexp_substr('it''s'::text, '''s'::text) AS v", "'s"),
    # The empty pattern matches the empty text before each character and at the end (REQ-2236).
    ("SELECT pg_catalog.\"regexp_substr\"('abc'::text, ''::text) AS v", ""),
    ("SELECT pg_catalog.\"regexp_substr\"('abc'::text, ''::text, 3::integer) AS v", ""),
    ("SELECT pg_catalog.regexp_substr('abc'::text, ''::text, 4) AS v", ""),
    ("SELECT pg_catalog.regexp_substr('abc'::text, ''::text, 5) AS v", None),
    ("SELECT pg_catalog.regexp_substr('abc'::text, ''::text, 1, 4) AS v", ""),
    ("SELECT pg_catalog.regexp_substr('abc'::text, ''::text, 1, 5) AS v", None),
    ("SELECT pg_catalog.regexp_substr('日本'::text, ''::text, 2, 2) AS v", ""),
    ("SELECT pg_catalog.regexp_substr('日本'::text, ''::text, 2, 3) AS v", None),
    ("SELECT pg_catalog.regexp_substr(''::text, ''::text) AS v", ""),
    ("SELECT pg_catalog.regexp_substr(NULL::text, ''::text) AS v", None),
    (
        "SELECT pg_catalog.\"regexp_substr\"('abc'::text, ''::text, 1::integer, 1::integer, 'i'::text) AS v",
        "",
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"('abc'::text, ''::text, 1::integer, 1::integer, 'i'::text, 0::integer) AS v",
        "",
    ),
    ("SELECT pg_catalog.\"regexp_replace\"('abc'::text, ''::text, 'abc'::text) AS v", "abcabc"),
    (
        "SELECT pg_catalog.\"regexp_replace\"('abc'::text, ''::text, 'abc'::text, 'g'::text) AS v",
        "abcaabcbabccabc",
    ),
    ("SELECT pg_catalog.regexp_replace('日本'::text, ''::text, '-'::text, 'g') AS v", "-日-本-"),
    ("SELECT pg_catalog.regexp_replace(''::text, ''::text, 'x'::text, 'g') AS v", "x"),
    ("SELECT pg_catalog.regexp_replace(''::text, ''::text, 'x'::text) AS v", "x"),
    ("SELECT pg_catalog.regexp_replace('abc'::text, ''::text, 'x'::text, 'i') AS v", "xabc"),
    ("SELECT pg_catalog.regexp_replace(NULL::text, ''::text, 'x'::text, 'g') AS v", None),
    ("SELECT pg_catalog.regexp_replace(NULL::text, ''::text, 'x'::text) AS v", None),
    # A pattern that is not empty but matches the empty text (REQ-2236): PostgreSQL's g flag
    # replaces each empty match and goes on one character; ClickHouse's replaceRegexpAll stops at
    # the first. The measure's edges, then anchors and the text's ends.
    (
        "SELECT pg_catalog.\"regexp_replace\"('abc'::text, 'x*'::text, 'abc'::text, 'g'::text) AS v",
        "abcaabcbabccabc",
    ),
    (
        "SELECT pg_catalog.\"regexp_replace\"('abc'::text, 'a|'::text, 'abc'::text, 'g'::text) AS v",
        "abcabcbabccabc",
    ),
    ("SELECT pg_catalog.\"regexp_replace\"('abc'::text, 'x*'::text, 'abc'::text) AS v", "abcabc"),
    ("SELECT pg_catalog.\"regexp_replace\"('abc'::text, 'a|'::text, 'abc'::text) AS v", "abcbc"),
    ("SELECT pg_catalog.regexp_replace('axxb'::text, 'x*'::text, '-'::text, 'g') AS v", "-a--b-"),
    ("SELECT pg_catalog.regexp_replace('axx'::text, 'x*'::text, '-'::text, 'g') AS v", "-a--"),
    ("SELECT pg_catalog.regexp_replace('aab'::text, 'a|'::text, '-'::text, 'g') AS v", "---b-"),
    ("SELECT pg_catalog.regexp_replace('abc'::text, '^'::text, '-'::text, 'g') AS v", "-abc"),
    ("SELECT pg_catalog.regexp_replace('abc'::text, '$'::text, '-'::text, 'g') AS v", "abc-"),
    ("SELECT pg_catalog.regexp_replace('abc'::text, '^|$'::text, '-'::text, 'g') AS v", "-abc-"),
    ("SELECT pg_catalog.regexp_replace('abc'::text, '^a*'::text, '-'::text, 'g') AS v", "-bc"),
    ("SELECT pg_catalog.regexp_replace('abc'::text, 'b*$'::text, '-'::text, 'g') AS v", "abc-"),
    (
        "SELECT pg_catalog.regexp_replace('日本x'::text, 'x*'::text, '-'::text, 'g') AS v",
        "-日-本--",
    ),
    ("SELECT pg_catalog.regexp_replace(''::text, 'x*'::text, '-'::text, 'g') AS v", "-"),
    ("SELECT pg_catalog.regexp_replace(NULL::text, 'x*'::text, '-'::text, 'g') AS v", None),
    ("SELECT pg_catalog.regexp_replace('aBc'::text, 'b*'::text, '-'::text, 'gi') AS v", "-a--c-"),
    ("SELECT pg_catalog.regexp_replace('abc'::text, 'b'::text, '-'::text, 'g') AS v", "a-c"),
    ("SELECT pg_catalog.regexp_replace('abc'::text, lower('b'), '-'::text, 'g') AS v", "a-c"),
    # The Nth match from start of a pattern that matches the empty text (REQ-2236): PostgreSQL's
    # search copies the character past an empty match and goes on; the measure's edges first.
    (
        "SELECT pg_catalog.\"regexp_replace\"('abc'::text, ''::text, 'abc'::text, 3::integer, 3::integer) AS v",
        "abc",
    ),
    (
        "SELECT pg_catalog.\"regexp_replace\"('abc'::text, 'x*'::text, 'abc'::text, 3::integer, 3::integer) AS v",
        "abc",
    ),
    (
        "SELECT pg_catalog.\"regexp_replace\"('abc'::text, 'a|'::text, 'abc'::text, 3::integer, 3::integer) AS v",
        "abc",
    ),
    (
        "SELECT pg_catalog.\"regexp_replace\"('abc'::text, lower('')::text, 'abc'::text, 3::integer, 3::integer) AS v",
        "abc",
    ),
    (
        "SELECT pg_catalog.\"regexp_replace\"('abc'::text, 'x*'::text, '-'::text, 1, 2) AS v",
        "a-bc",
    ),
    (
        "SELECT pg_catalog.\"regexp_replace\"('abc'::text, 'x*'::text, '-'::text, 2, 0) AS v",
        "a-b-c-",
    ),
    (
        "SELECT pg_catalog.\"regexp_replace\"('abc'::text, 'x*'::text, '-'::text, 2, 1) AS v",
        "a-bc",
    ),
    (
        "SELECT pg_catalog.\"regexp_replace\"('abc'::text, '^x*'::text, '-'::text, 2, 0) AS v",
        "abc",
    ),
    (
        "SELECT pg_catalog.\"regexp_replace\"('abc'::text, '^x*'::text, '-'::text, 1, 0) AS v",
        "-abc",
    ),
    (
        "SELECT pg_catalog.\"regexp_replace\"('abc'::text, 'x*'::text, '-'::text, 4, 0) AS v",
        "abc-",
    ),
    ("SELECT pg_catalog.\"regexp_replace\"('abc'::text, 'x*'::text, '-'::text, 5, 0) AS v", "abc"),
    (
        "SELECT pg_catalog.\"regexp_replace\"('abc'::text, 'x*'::text, '-'::text, 4, 1) AS v",
        "abc-",
    ),
    (
        "SELECT pg_catalog.\"regexp_replace\"('abc'::text, 'x*'::text, '-'::text, 1, 4) AS v",
        "abc-",
    ),
    (
        "SELECT pg_catalog.\"regexp_replace\"('abc'::text, 'a|'::text, '-'::text, 1, 2) AS v",
        "a-bc",
    ),
    (
        "SELECT pg_catalog.\"regexp_replace\"('axxb'::text, 'x*'::text, '-'::text, 1, 3) AS v",
        "axx-b",
    ),
    (
        "SELECT pg_catalog.\"regexp_replace\"('axxb'::text, 'x*'::text, '-'::text, 2, 1) AS v",
        "a-b",
    ),
    (
        "SELECT pg_catalog.\"regexp_replace\"('日本x'::text, 'x*'::text, '-'::text, 2, 0) AS v",
        "日-本--",
    ),
    (
        "SELECT pg_catalog.\"regexp_replace\"('abc'::text, '(b?)'::text, '[\\1]'::text, 1, 0) AS v",
        "[]a[b][]c[]",
    ),
    (
        "SELECT pg_catalog.\"regexp_replace\"('abc'::text, 'x*$'::text, '-'::text, 1, 0) AS v",
        "abc-",
    ),
    ("SELECT pg_catalog.\"regexp_replace\"(''::text, 'x*'::text, '-'::text, 1, 0) AS v", "-"),
    ("SELECT pg_catalog.\"regexp_replace\"('abc'::text, ''::text, '-'::text, 2, 2) AS v", "ab-c"),
    ("SELECT pg_catalog.\"regexp_replace\"(NULL::text, 'x*'::text, '-'::text, 1, 0) AS v", None),
    (
        "SELECT pg_catalog.\"regexp_replace\"('abc'::text, 'x*'::text, '-'::text, 1, NULL::integer) AS v",
        None,
    ),
    ("SELECT pg_catalog.regexp_like('ABC'::text, 'b'::text, 'ic') AS v", "f"),
    ("SELECT pg_catalog.regexp_split_to_array('a1b22c'::text, '[0-9]+'::text) AS v", "{a,b,c}"),
    ("SELECT pg_catalog.regexp_split_to_array('aXbxc'::text, 'x'::text, 'i') AS v", "{a,b,c}"),
    ("SELECT pg_catalog.int4('-2.5'::jsonb) AS v", "-3"),
    ("SELECT pg_catalog.int8('0.0005e3'::jsonb) AS v", "1"),
    ("SELECT pg_catalog.int4('1e-46'::jsonb) AS v", "0"),
    ("SELECT pg_catalog.float8('0.49999'::jsonb) AS v", "0.49999"),
    (
        "SELECT pg_catalog.float8('12345678901234567890123456789'::jsonb) AS v",
        "1.2345678901234568e+28",
    ),
]
RAISED = [
    (
        "SELECT pg_catalog.\"regexp_count\"('Hello World'::text, '[a-z]+'::text, (-7)::integer) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"regexp_count\"('abc'::text, 'b'::text, '-2147483648'::integer) AS v",
        "22023",
    ),
    ("SELECT pg_catalog.\"regexp_count\"('abc'::text, 'b'::text, '0'::integer) AS v", "22023"),
    (
        "SELECT pg_catalog.\"regexp_instr\"('Hello World'::text, '[a-z]+'::text, (-7)::integer) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('abc'::text, 'b'::text, '-2147483648'::integer) AS v",
        "22023",
    ),
    ("SELECT pg_catalog.\"regexp_instr\"('abc'::text, 'b'::text, '0'::integer) AS v", "22023"),
    (
        "SELECT pg_catalog.\"regexp_instr\"('Hello World'::text, '[a-z]+'::text, (-7)::integer, (-7)::integer) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('abc'::text, 'b'::text, '-2147483648'::integer, 3::integer) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('abc'::text, 'b'::text, 3::integer, '-2147483648'::integer) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"('Hello World'::text, '[a-z]+'::text, (-7)::integer) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"('abc'::text, 'b'::text, '-2147483648'::integer) AS v",
        "22023",
    ),
    ("SELECT pg_catalog.\"regexp_substr\"('abc'::text, 'b'::text, '0'::integer) AS v", "22023"),
    (
        "SELECT pg_catalog.\"regexp_substr\"('Hello World'::text, '[a-z]+'::text, (-7)::integer, (-7)::integer) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"('abc'::text, 'b'::text, '-2147483648'::integer, 3::integer) AS v",
        "22023",
    ),
    (
        "SELECT pg_catalog.\"regexp_substr\"('abc'::text, 'b'::text, 3::integer, '-2147483648'::integer) AS v",
        "22023",
    ),
    ("SELECT pg_catalog.\"nameconcatoid\"('abc'::name, '4294967296'::oid) AS v", "22003"),
    (
        "SELECT pg_catalog.\"bpchar\"('abc'::character, '2147483647'::integer, true::boolean) AS v",
        "XX000",
    ),
    ('SELECT pg_catalog."int2"(\'{"a": 1, "b": [1, 2]}\'::jsonb) AS v', "22023"),
    ('SELECT pg_catalog."int2"(\'[1, "two", null]\'::jsonb) AS v', "22023"),
    ("SELECT pg_catalog.\"int2\"('{}'::jsonb) AS v", "22023"),
    ("SELECT pg_catalog.\"int2\"('[]'::jsonb) AS v", "22023"),
    ('SELECT pg_catalog."int2"(\'"日本語😀"\'::jsonb) AS v', "22023"),
    ("SELECT pg_catalog.\"int2\"('12345678901234567890123456789'::jsonb) AS v", "22003"),
    ('SELECT pg_catalog."int4"(\'{"a": 1, "b": [1, 2]}\'::jsonb) AS v', "22023"),
    ('SELECT pg_catalog."int4"(\'[1, "two", null]\'::jsonb) AS v', "22023"),
    ("SELECT pg_catalog.\"int4\"('{}'::jsonb) AS v", "22023"),
    ("SELECT pg_catalog.\"int4\"('[]'::jsonb) AS v", "22023"),
    ('SELECT pg_catalog."int4"(\'"日本語😀"\'::jsonb) AS v', "22023"),
    ("SELECT pg_catalog.\"int4\"('12345678901234567890123456789'::jsonb) AS v", "22003"),
    ('SELECT pg_catalog."int8"(\'{"a": 1, "b": [1, 2]}\'::jsonb) AS v', "22023"),
    ('SELECT pg_catalog."int8"(\'[1, "two", null]\'::jsonb) AS v', "22023"),
    ("SELECT pg_catalog.\"int8\"('{}'::jsonb) AS v", "22023"),
    ("SELECT pg_catalog.\"int8\"('[]'::jsonb) AS v", "22023"),
    ('SELECT pg_catalog."int8"(\'"日本語😀"\'::jsonb) AS v', "22023"),
    ("SELECT pg_catalog.\"int8\"('12345678901234567890123456789'::jsonb) AS v", "22003"),
    ('SELECT pg_catalog."float8"(\'{"a": 1, "b": [1, 2]}\'::jsonb) AS v', "22023"),
    ('SELECT pg_catalog."float8"(\'[1, "two", null]\'::jsonb) AS v', "22023"),
    ("SELECT pg_catalog.\"float8\"('{}'::jsonb) AS v", "22023"),
    ("SELECT pg_catalog.\"float8\"('[]'::jsonb) AS v", "22023"),
    ('SELECT pg_catalog."float8"(\'"日本語😀"\'::jsonb) AS v', "22023"),
    ("SELECT pg_catalog.\"int8\"('4294967296'::oid) AS v", "22003"),
    ("SELECT pg_catalog.overlay('abc'::text, 'x'::text, 0, 1) AS v", "22011"),
    ("SELECT pg_catalog.overlay('abc'::text, 'x'::text, 2, 2147483647) AS v", "22003"),
    ("SELECT pg_catalog.overlay('abc'::text, 'x'::text, 2147483647) AS v", "22003"),
    ("SELECT pg_catalog.varchar('abcdef'::character varying, 7, false) AS v", "22001"),
    ("SELECT pg_catalog.varchar('abc   '::character varying, 6, false) AS v", "22001"),
    ("SELECT pg_catalog.bpchar('abcdef'::character(6), 7, false) AS v", "22001"),
    ("SELECT '4294967296'::oid AS v", "22003"),
    ("SELECT 'x'::oid AS v", "22P02"),
    # PostgreSQL 17 counts each empty match (regexp_count('abc', '') is 4); ClickHouse's
    # countMatches skips them, so a pattern that matches the empty text is its stated 0A000.
    ("SELECT pg_catalog.regexp_count('abc'::text, ''::text) AS v", "0A000"),
    ("SELECT pg_catalog.regexp_count('abc'::text, 'x*'::text) AS v", "0A000"),
    # extractAll skips an empty match too: regexp_substr('abc', 'b*') is '' on PostgreSQL.
    ("SELECT pg_catalog.regexp_substr('abc'::text, 'b*'::text) AS v", "0A000"),
    ("SELECT pg_catalog.regexp_substr('abc'::text, '^'::text) AS v", "0A000"),
    # A pattern no form here searches as PostgreSQL does (no literal, or a replacement that is no
    # literal) raises 0A000 where it matches the empty text.
    (
        "SELECT pg_catalog.\"regexp_replace\"('abc'::text, lower('')::text, 'abc'::text, 'g'::text) AS v",
        "0A000",
    ),
    (
        "SELECT pg_catalog.regexp_replace('abc'::text, 'x*'::text, c, 'g') AS v "
        "FROM (VALUES ('a'::text)) AS t(c)",
        "0A000",
    ),
    ("SELECT pg_catalog.regexp_instr('abc'::text, 'b'::text, 1, 1, 2) AS v", "22023"),
    ("SELECT pg_catalog.regexp_instr('abc'::text, 'b'::text, 1, 0) AS v", "22023"),
    ("SELECT pg_catalog.regexp_substr('foo'::text, 'o'::text, 1, 1, 'g') AS v", "22023"),
    ("SELECT pg_catalog.int4('2147483647.5'::jsonb) AS v", "22003"),
    ("SELECT pg_catalog.int2('32767.5'::jsonb) AS v", "22003"),
    ("SELECT pg_catalog.int4('\"x\"'::jsonb) AS v", "22023"),
    ("SELECT pg_catalog.float8('1e400'::jsonb) AS v", "22003"),
    ("SELECT pg_catalog.float8('1e-400'::jsonb) AS v", "22003"),
]

#: Calls a row refuses by name, each within its stated bound.
REFUSED = [
    "SELECT pg_catalog.regexp_substr('ab'::text, '\\w'::text) AS v",
    "SELECT pg_catalog.regexp_substr('a1'::text, '[[:digit:]]'::text) AS v",
    "SELECT pg_catalog.regexp_substr('aa'::text, '(a)\\1'::text) AS v",
    "SELECT pg_catalog.regexp_substr('foo'::text, 'o'::text, 1, 1, 'x') AS v",
    "SELECT pg_catalog.regexp_substr('foo'::text, '(o)'::text, 1, 1, '', 1) AS v",
    "SELECT pg_catalog.regexp_count(c, c, 1) AS v FROM (VALUES ('a'::text)) AS t(c)",
    # The empty pattern's replacement is read here: an escape PostgreSQL reads in it is refused.
    "SELECT pg_catalog.regexp_replace('abc'::text, ''::text, '\\&x'::text, 'g') AS v",
    "SELECT pg_catalog.regexp_replace('abc'::text, ''::text, c, 'g') AS v "
    "FROM (VALUES ('a'::text)) AS t(c)",
]


def _element(value: object) -> str:
    if value is None:
        return "NULL"
    text = _text(value)
    if text == "" or text.upper() == "NULL" or any(ch in text for ch in ',{}"\\ '):
        return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return text


def _text(value: object) -> str:
    """The engine's value as PostgreSQL's text output writes it: a boolean t or f, an array in
    braces."""
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, (list, tuple)):
        return "{" + ",".join(_element(v) for v in value) + "}"
    return str(value)


def _same(want: str | None, got: object) -> bool:
    if want is None or got is None:
        return want is got
    if isinstance(got, float):
        return float(want) == got
    return want == _text(got)


@pytest.fixture(scope="module")
def runtime() -> ClickHouseFederationRuntime:
    return ClickHouseFederationRuntime.embedded()


def _answer(runtime: ClickHouseFederationRuntime, sql: str) -> object:
    ((value,),) = list(runtime.run_sync(transpile(sql, "clickhouse", generated=False)).iter_rows())
    return value


@pytest.mark.parametrize(("sql", "want"), ANSWERED)
def test_clickhouse_answers_postgresql_s(runtime, sql, want):
    got = _answer(runtime, sql)
    assert _same(want, got), (want, got)


@pytest.mark.parametrize(("sql", "state"), RAISED)
def test_clickhouse_raises_postgresql_s_error(runtime, sql, state):
    with pytest.raises(Exception) as raised:
        _answer(runtime, sql)
    if isinstance(raised.value, RowRefused):
        assert raised.value.sqlstate == state
        return
    error = from_engine(raised.value)
    assert error is not None and error.sqlstate == state, str(raised.value)[:300]


@pytest.mark.parametrize("sql", REFUSED)
def test_a_call_past_the_row_s_bound_is_refused_by_name(sql):
    with pytest.raises(FunctionRefused):
        transpile(sql, "clickhouse", generated=False)


def test_a_native_two_argument_regexp_like_keeps_clickhouse_s_match():
    sql = "SELECT regexp_like(a, b) AS v FROM (VALUES ('x', 'y')) AS t(a, b)"
    assert "match(a, b)" in transpile(sql, "clickhouse", generated=False)


def test_a_two_argument_regexp_count_of_a_column_pattern_keeps_clickhouse_s_count():
    sql = "SELECT regexp_count(a, b) AS v FROM (VALUES ('x', 'y')) AS t(a, b)"
    assert "countMatches(a, b)" in transpile(sql, "clickhouse", generated=False)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT pg_catalog.\"overlay\"('abc'::text, 'x'::text, 1) AS v",
        "SELECT pg_catalog.regexp_instr('abc'::text, 'b'::text) AS v",
        'SELECT pg_catalog."int8"(12345::oid) AS v',
    ],
)
def test_trino_still_refuses_what_clickhouse_now_answers(sql):
    with pytest.raises(FunctionRefused):
        transpile(sql, "trino", generated=False)
