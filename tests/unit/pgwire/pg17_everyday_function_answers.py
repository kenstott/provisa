# Copyright (c) 2026 Kenneth Stott
# Canary: 6d7e18b2-c45a-4f09-8e31-b2a4c97f0d15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What PostgreSQL 17 (postgres:17.11, UTF8, en_US.utf8, UTC) answered for
everyday_function_statements.py: statement -> ("ok", type oid, rows) or ("err", SQLSTATE, message).
Real values, each statement over the extended protocol with text results. Written by
record_pg17_everyday_functions.py, never by hand."""

# Requirements: REQ-2167, REQ-2171
# fmt: off

from __future__ import annotations

from typing import Any

RECORDED: dict[str, tuple[Any, Any, Any]] = {'SELECT array_ndims(ARRAY[1,2,3]) AS v': ('ok', 23, ['1']),
 'SELECT array_ndims(ARRAY[[1,2,3],[4,5,6]]) AS v': ('ok', 23, ['2']),
 'SELECT array_ndims(ARRAY[]::integer[]) AS v': ('ok', 23, [None]),
 'SELECT array_ndims(NULL::integer[]) AS v': ('ok', 23, [None]),
 'SELECT array_ndims(ARRAY[[[1],[2]]]) AS v': ('ok', 23, ['3']),
 'SELECT array_dims(ARRAY[1,2,3]) AS v': ('ok', 25, ['[1:3]']),
 'SELECT array_dims(ARRAY[[1,2,3],[4,5,6]]) AS v': ('ok', 25, ['[1:2][1:3]']),
 'SELECT array_dims(ARRAY[]::integer[]) AS v': ('ok', 25, [None]),
 'SELECT array_dims(NULL::integer[]) AS v': ('ok', 25, [None]),
 'SELECT array_dims(ARRAY[[[1],[2]]]) AS v': ('ok', 25, ['[1:1][1:2][1:1]']),
 'SELECT array_lower(ARRAY[1,2,3], 1) AS v': ('ok', 23, ['1']),
 'SELECT array_lower(ARRAY[1,2,3], 2) AS v': ('ok', 23, [None]),
 'SELECT array_lower(ARRAY[1,2,3], 0) AS v': ('ok', 23, [None]),
 'SELECT array_lower(ARRAY[[1,2,3],[4,5,6]], 1) AS v': ('ok', 23, ['1']),
 'SELECT array_lower(ARRAY[[1,2,3],[4,5,6]], 2) AS v': ('ok', 23, ['1']),
 'SELECT array_lower(ARRAY[]::integer[], 1) AS v': ('ok', 23, [None]),
 'SELECT array_lower(NULL::integer[], 1) AS v': ('ok', 23, [None]),
 'SELECT array_lower(ARRAY[1,2,3], NULL::integer) AS v': ('ok', 23, [None]),
 'SELECT array_upper(ARRAY[1,2,3], 1) AS v': ('ok', 23, ['3']),
 'SELECT array_upper(ARRAY[1,2,3], 2) AS v': ('ok', 23, [None]),
 'SELECT array_upper(ARRAY[1,2,3], 0) AS v': ('ok', 23, [None]),
 'SELECT array_upper(ARRAY[[1,2,3],[4,5,6]], 1) AS v': ('ok', 23, ['2']),
 'SELECT array_upper(ARRAY[[1,2,3],[4,5,6]], 2) AS v': ('ok', 23, ['3']),
 'SELECT array_upper(ARRAY[]::integer[], 1) AS v': ('ok', 23, [None]),
 'SELECT array_upper(NULL::integer[], 1) AS v': ('ok', 23, [None]),
 'SELECT array_upper(ARRAY[1,2,3], NULL::integer) AS v': ('ok', 23, [None]),
 'SELECT array_fill(7, ARRAY[3]) AS v': ('ok', 1007, ['{7,7,7}']),
 'SELECT array_fill(7, ARRAY[0]) AS v': ('ok', 1007, ['{}']),
 'SELECT array_fill(NULL::integer, ARRAY[2]) AS v': ('ok', 1007, ['{NULL,NULL}']),
 "SELECT array_fill('x'::text, ARRAY[2]) AS v": ('ok', 1009, ['{x,x}']),
 'SELECT array_fill(7, NULL::integer[]) AS v': ('err',
                                                '22004',
                                                'dimension array or low bound array cannot be '
                                                'null'),
 'SELECT array_fill(7, ARRAY[2], ARRAY[1]) AS v': ('ok', 1007, ['{7,7}']),
 'SELECT array_fill(7, ARRAY[2], NULL::integer[]) AS v': ('err',
                                                          '22004',
                                                          'dimension array or low bound array '
                                                          'cannot be null'),
 'SELECT array_positions(ARRAY[1,2,1], 1) AS v': ('ok', 1007, ['{1,3}']),
 'SELECT array_positions(ARRAY[1,NULL,1], NULL::integer) AS v': ('ok', 1007, ['{2}']),
 'SELECT array_positions(ARRAY[1,2], 9) AS v': ('ok', 1007, ['{}']),
 'SELECT array_positions(NULL::integer[], 1) AS v': ('ok', 1007, [None]),
 'SELECT array_replace(ARRAY[1,2,1], 1, 9) AS v': ('ok', 1007, ['{9,2,9}']),
 'SELECT array_replace(ARRAY[1,NULL], NULL::integer, 9) AS v': ('ok', 1007, ['{1,9}']),
 'SELECT array_replace(ARRAY[1,2], 1, NULL::integer) AS v': ('ok', 1007, ['{NULL,2}']),
 'SELECT array_replace(NULL::integer[], 1, 9) AS v': ('ok', 1007, [None]),
 'SELECT array_sample(ARRAY[7], 1) AS v': ('ok', 1007, ['{7}']),
 'SELECT array_sample(ARRAY[1,2], 0) AS v': ('ok', 1007, ['{}']),
 'SELECT array_sample(ARRAY[1,2], 3) AS v': ('err', '22023', 'sample size must be between 0 and 2'),
 'SELECT array_sample(ARRAY[1,2], -1) AS v': ('err',
                                              '22023',
                                              'sample size must be between 0 and 2'),
 'SELECT array_sample(NULL::integer[], 1) AS v': ('ok', 1007, [None]),
 'SELECT array_shuffle(ARRAY[7]) AS v': ('ok', 1007, ['{7}']),
 'SELECT array_shuffle(NULL::integer[]) AS v': ('ok', 1007, [None]),
 'SELECT trim_array(ARRAY[1,2,3], 1) AS v': ('ok', 1007, ['{1,2}']),
 'SELECT trim_array(ARRAY[1,2,3], 3) AS v': ('ok', 1007, ['{}']),
 'SELECT trim_array(ARRAY[1,2,3], 0) AS v': ('ok', 1007, ['{1,2,3}']),
 'SELECT trim_array(ARRAY[1,2,3], 4) AS v': ('err',
                                             '2202E',
                                             'number of elements to trim must be between 0 and 3'),
 'SELECT trim_array(ARRAY[1,2,3], -1) AS v': ('err',
                                              '2202E',
                                              'number of elements to trim must be between 0 and 3'),
 'SELECT trim_array(NULL::integer[], 1) AS v': ('ok', 1007, [None]),
 'SELECT trim_array(ARRAY[1,2], NULL::integer) AS v': ('ok', 1007, [None]),
 'SELECT array_agg(a) AS v FROM (VALUES (ARRAY[1,2]), (ARRAY[3,4])) t(a)': ('ok',
                                                                            1007,
                                                                            ['{{1,2},{3,4}}']),
 'SELECT array_agg(ARRAY[1]) AS v': ('ok', 1007, ['{{1}}']),
 'SELECT generate_subscripts(ARRAY[7,8,9], 1, true) AS v': ('ok', 23, ['3', '2', '1']),
 'SELECT generate_subscripts(ARRAY[7,8,9], 1, false) AS v': ('ok', 23, ['1', '2', '3']),
 'SELECT generate_subscripts(ARRAY[[1,2,3],[4,5,6]], 2, true) AS v': ('ok', 23, ['3', '2', '1']),
 'SELECT generate_subscripts(NULL::integer[], NULL::integer, NULL::boolean) AS v': ('ok', 23, []),
 'SELECT pg_catalog."array_agg"(\'{1}\'::integer[]) AS v': ('ok', 1007, ['{{1}}']),
 'SELECT pg_catalog."array_dims"(NULL::integer[]) AS v': ('ok', 25, [None]),
 'SELECT pg_catalog."array_fill"(NULL::integer, \'{1}\'::integer[]) AS v': ('ok', 1007, ['{NULL}']),
 'SELECT pg_catalog."array_fill"(NULL::integer, \'{1}\'::integer[], \'{1}\'::integer[]) AS v': ('ok',
                                                                                                1007,
                                                                                                ['{NULL}']),
 'SELECT pg_catalog."array_lower"(NULL::integer[], NULL::integer) AS v': ('ok', 23, [None]),
 'SELECT pg_catalog."array_ndims"(NULL::integer[]) AS v': ('ok', 23, [None]),
 'SELECT pg_catalog."array_positions"(NULL::integer[], NULL::integer) AS v': ('ok', 1007, [None]),
 'SELECT pg_catalog."array_replace"(NULL::integer[], NULL::integer, NULL::integer) AS v': ('ok',
                                                                                           1007,
                                                                                           [None]),
 'SELECT pg_catalog."array_sample"(NULL::integer[], NULL::integer) AS v': ('ok', 1007, [None]),
 'SELECT pg_catalog."array_shuffle"(NULL::integer[]) AS v': ('ok', 1007, [None]),
 'SELECT pg_catalog."array_upper"(NULL::integer[], NULL::integer) AS v': ('ok', 23, [None]),
 'SELECT pg_catalog."generate_subscripts"(NULL::integer[], NULL::integer, NULL::boolean) AS v': ('ok',
                                                                                                 23,
                                                                                                 [])}
