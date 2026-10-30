# Copyright (c) 2026 Kenneth Stott
# Canary: 92f1c6e3-a05d-4e71-b8d4-6c30e7a9f158
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Range and multirange functions over pgwire (REQ-2167, REQ-2171): each statement run as the
conformance probe runs it, against what PostgreSQL 17 answered (docker postgres:17; ("ok", type
OID, the rows' text) or ("err", SQLSTATE, message)). Recorded, not derived. The conformance
group's functions are here in every overload, with NULL arguments, as the probe calls them."""

# Requirements: REQ-2167, REQ-2171
# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

from typing import Any

import pytest

from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401

FLOAT8 = 701

# fmt: off
RECORDED: dict[str, tuple] = {'SELECT pg_catalog."daterange"(NULL::date, NULL::date) AS v': ('ok', 3912, ['(,)']),
 'SELECT pg_catalog."daterange"(NULL::date, NULL::date, NULL::text) AS v': ('err',
                                                                            '22000',
                                                                            'range constructor flags '
                                                                            'argument must not be null'),
 'SELECT pg_catalog."daterange_canonical"(NULL::daterange) AS v': ('ok', 3912, [None]),
 'SELECT pg_catalog."daterange_subdiff"(NULL::date, NULL::date) AS v': ('ok', 701, [None]),
 'SELECT pg_catalog."in_range"(NULL::bigint, NULL::bigint, NULL::bigint, NULL::boolean, NULL::boolean) AS v': ('ok',
                                                                                                               16,
                                                                                                               [None]),
 'SELECT pg_catalog."in_range"(NULL::date, NULL::date, NULL::interval, NULL::boolean, NULL::boolean) AS v': ('ok',
                                                                                                             16,
                                                                                                             [None]),
 'SELECT pg_catalog."in_range"(NULL::double precision, NULL::double precision, NULL::double precision, NULL::boolean, NULL::boolean) AS v': ('ok',
                                                                                                                                             16,
                                                                                                                                             [None]),
 'SELECT pg_catalog."in_range"(NULL::integer, NULL::integer, NULL::bigint, NULL::boolean, NULL::boolean) AS v': ('ok',
                                                                                                                 16,
                                                                                                                 [None]),
 'SELECT pg_catalog."in_range"(NULL::integer, NULL::integer, NULL::integer, NULL::boolean, NULL::boolean) AS v': ('ok',
                                                                                                                  16,
                                                                                                                  [None]),
 'SELECT pg_catalog."in_range"(NULL::integer, NULL::integer, NULL::smallint, NULL::boolean, NULL::boolean) AS v': ('ok',
                                                                                                                   16,
                                                                                                                   [None]),
 'SELECT pg_catalog."in_range"(NULL::interval, NULL::interval, NULL::interval, NULL::boolean, NULL::boolean) AS v': ('ok',
                                                                                                                     16,
                                                                                                                     [None]),
 'SELECT pg_catalog."in_range"(NULL::numeric, NULL::numeric, NULL::numeric, NULL::boolean, NULL::boolean) AS v': ('ok',
                                                                                                                  16,
                                                                                                                  [None]),
 'SELECT pg_catalog."in_range"(NULL::real, NULL::real, NULL::double precision, NULL::boolean, NULL::boolean) AS v': ('ok',
                                                                                                                     16,
                                                                                                                     [None]),
 'SELECT pg_catalog."in_range"(NULL::smallint, NULL::smallint, NULL::bigint, NULL::boolean, NULL::boolean) AS v': ('ok',
                                                                                                                   16,
                                                                                                                   [None]),
 'SELECT pg_catalog."in_range"(NULL::smallint, NULL::smallint, NULL::integer, NULL::boolean, NULL::boolean) AS v': ('ok',
                                                                                                                    16,
                                                                                                                    [None]),
 'SELECT pg_catalog."in_range"(NULL::smallint, NULL::smallint, NULL::smallint, NULL::boolean, NULL::boolean) AS v': ('ok',
                                                                                                                     16,
                                                                                                                     [None]),
 'SELECT pg_catalog."in_range"(NULL::time with time zone, NULL::time with time zone, NULL::interval, NULL::boolean, NULL::boolean) AS v': ('ok',
                                                                                                                                           16,
                                                                                                                                           [None]),
 'SELECT pg_catalog."in_range"(NULL::time without time zone, NULL::time without time zone, NULL::interval, NULL::boolean, NULL::boolean) AS v': ('ok',
                                                                                                                                                 16,
                                                                                                                                                 [None]),
 'SELECT pg_catalog."in_range"(NULL::timestamp with time zone, NULL::timestamp with time zone, NULL::interval, NULL::boolean, NULL::boolean) AS v': ('ok',
                                                                                                                                                     16,
                                                                                                                                                     [None]),
 'SELECT pg_catalog."in_range"(NULL::timestamp without time zone, NULL::timestamp without time zone, NULL::interval, NULL::boolean, NULL::boolean) AS v': ('ok',
                                                                                                                                                           16,
                                                                                                                                                           [None]),
 'SELECT pg_catalog."int4range"(NULL::integer, NULL::integer) AS v': ('ok', 3904, ['(,)']),
 'SELECT pg_catalog."int4range"(NULL::integer, NULL::integer, NULL::text) AS v': ('err',
                                                                                  '22000',
                                                                                  'range constructor flags '
                                                                                  'argument must not be '
                                                                                  'null'),
 'SELECT pg_catalog."int4range_canonical"(NULL::int4range) AS v': ('ok', 3904, [None]),
 'SELECT pg_catalog."int4range_subdiff"(NULL::integer, NULL::integer) AS v': ('ok', 701, [None]),
 'SELECT pg_catalog."int8range"(NULL::bigint, NULL::bigint) AS v': ('ok', 3926, ['(,)']),
 'SELECT pg_catalog."int8range"(NULL::bigint, NULL::bigint, NULL::text) AS v': ('err',
                                                                                '22000',
                                                                                'range constructor flags '
                                                                                'argument must not be null'),
 'SELECT pg_catalog."int8range_canonical"(NULL::int8range) AS v': ('ok', 3926, [None]),
 'SELECT pg_catalog."int8range_subdiff"(NULL::bigint, NULL::bigint) AS v': ('ok', 701, [None]),
 'SELECT pg_catalog."nummultirange"() AS v': ('ok', 4532, ['{}']),
 'SELECT pg_catalog."nummultirange"(NULL::numrange) AS v': ('ok', 4532, [None]),
 'SELECT pg_catalog."numrange_subdiff"(NULL::numeric, NULL::numeric) AS v': ('ok', 701, [None]),
 'SELECT pg_catalog."tsmultirange"() AS v': ('ok', 4533, ['{}']),
 'SELECT pg_catalog."tsmultirange"(NULL::tsrange) AS v': ('ok', 4533, [None]),
 'SELECT pg_catalog."tsrange"(NULL::timestamp without time zone, NULL::timestamp without time zone) AS v': ('ok',
                                                                                                            3908,
                                                                                                            ['(,)']),
 'SELECT pg_catalog."tsrange"(NULL::timestamp without time zone, NULL::timestamp without time zone, NULL::text) AS v': ('err',
                                                                                                                        '22000',
                                                                                                                        'range '
                                                                                                                        'constructor '
                                                                                                                        'flags '
                                                                                                                        'argument '
                                                                                                                        'must '
                                                                                                                        'not '
                                                                                                                        'be '
                                                                                                                        'null'),
 'SELECT pg_catalog."tsrange_subdiff"(NULL::timestamp without time zone, NULL::timestamp without time zone) AS v': ('ok',
                                                                                                                    701,
                                                                                                                    [None]),
 'SELECT pg_catalog."tstzmultirange"() AS v': ('ok', 4534, ['{}']),
 'SELECT pg_catalog."tstzmultirange"(NULL::tstzrange) AS v': ('ok', 4534, [None]),
 'SELECT pg_catalog."tstzrange"(NULL::timestamp with time zone, NULL::timestamp with time zone) AS v': ('ok',
                                                                                                        3910,
                                                                                                        ['(,)']),
 'SELECT pg_catalog."tstzrange"(NULL::timestamp with time zone, NULL::timestamp with time zone, NULL::text) AS v': ('err',
                                                                                                                    '22000',
                                                                                                                    'range '
                                                                                                                    'constructor '
                                                                                                                    'flags '
                                                                                                                    'argument '
                                                                                                                    'must '
                                                                                                                    'not '
                                                                                                                    'be '
                                                                                                                    'null'),
 'SELECT pg_catalog."tstzrange_subdiff"(NULL::timestamp with time zone, NULL::timestamp with time zone) AS v': ('ok',
                                                                                                                701,
                                                                                                                [None]),
 'SELECT pg_catalog."datemultirange"() AS v': ('ok', 4535, ['{}']),
 'SELECT pg_catalog."datemultirange"(NULL::daterange) AS v': ('ok', 4535, [None]),
 'SELECT pg_catalog."int4multirange"() AS v': ('ok', 4451, ['{}']),
 'SELECT pg_catalog."int4multirange"(NULL::int4range) AS v': ('ok', 4451, [None]),
 'SELECT pg_catalog."int8multirange"() AS v': ('ok', 4536, ['{}']),
 'SELECT pg_catalog."int8multirange"(NULL::int8range) AS v': ('ok', 4536, [None]),
 'SELECT pg_catalog."numrange"(NULL::numeric, NULL::numeric) AS v': ('ok', 3906, ['(,)']),
 'SELECT pg_catalog."numrange"(NULL::numeric, NULL::numeric, NULL::text) AS v': ('err',
                                                                                 '22000',
                                                                                 'range constructor flags '
                                                                                 'argument must not be null'),
 "SELECT '[1,10]'::int4range AS v": ('ok', 3904, ['[1,11)']),
 "SELECT '(1,10]'::int8range AS v": ('ok', 3926, ['[2,11)']),
 "SELECT '[1.5,2.50)'::numrange AS v": ('ok', 3906, ['[1.5,2.50)']),
 "SELECT '[2020-01-01,2020-02-01]'::daterange AS v": ('ok', 3912, ['[2020-01-01,2020-02-02)']),
 "SELECT '[2020-01-01,2020-02-01]'::tsrange AS v": ('ok',
                                                    3908,
                                                    ['["2020-01-01 00:00:00","2020-02-01 00:00:00"]']),
 "SELECT '[2020-01-01,2020-02-01]'::tstzrange AS v": ('ok',
                                                      3910,
                                                      ['["2020-01-01 00:00:00+00","2020-02-01 '
                                                       '00:00:00+00"]']),
 "SELECT int4range(1, 10, '[]') AS v": ('ok', 3904, ['[1,11)']),
 'SELECT int4range(5, 1) AS v': ('err',
                                 '22000',
                                 'range lower bound must be less than or equal to range upper bound'),
 "SELECT daterange('2020-01-01', '2020-01-05', '(]') AS v": ('ok', 3912, ['[2020-01-02,2020-01-06)']),
 'SELECT numrange(1.5, 2.50) AS v': ('ok', 3906, ['[1.5,2.50)']),
 "SELECT '[1,10)'::int4range && '[9,20)'::int4range AS v": ('ok', 16, ['t']),
 "SELECT '[1,10)'::int4range @> 5 AS v": ('ok', 16, ['t']),
 "SELECT 5 <@ '[1,10)'::int4range AS v": ('ok', 16, ['t']),
 "SELECT '[1,10)'::int4range -|- '[10,20)'::int4range AS v": ('ok', 16, ['t']),
 "SELECT '[1,10)'::int4range + '[5,20)'::int4range AS v": ('ok', 3904, ['[1,20)']),
 "SELECT '[1,10)'::int4range * '[5,20)'::int4range AS v": ('ok', 3904, ['[5,10)']),
 "SELECT '[1,10)'::int4range - '[5,20)'::int4range AS v": ('ok', 3904, ['[1,5)']),
 "SELECT '[1,2)'::int4range + '[5,20)'::int4range AS v": ('err',
                                                          '22000',
                                                          'result of range union would not be contiguous'),
 "SELECT lower('[1,10)'::int4range) AS v": ('ok', 23, ['1']),
 "SELECT upper('[1.5,2.50)'::numrange) AS v": ('ok', 1700, ['2.50']),
 "SELECT lower('[2020-01-01,2020-02-01)'::daterange) AS v": ('ok', 1082, ['2020-01-01']),
 "SELECT lower('[2020-01-01,2020-02-01)'::tsrange) AS v": ('ok', 1114, ['2020-01-01 00:00:00']),
 "SELECT lower('[2020-01-01,2020-02-01)'::tstzrange) AS v": ('ok', 1184, ['2020-01-01 00:00:00+00']),
 "SELECT isempty('empty'::int4range) AS v": ('ok', 16, ['t']),
 "SELECT lower_inc('[1,2)'::int4range) AS v": ('ok', 16, ['t']),
 "SELECT upper_inc('[1,2)'::int4range) AS v": ('ok', 16, ['f']),
 "SELECT lower_inf('(,2)'::int4range) AS v": ('ok', 16, ['t']),
 "SELECT upper_inf('(,2)'::int4range) AS v": ('ok', 16, ['f']),
 "SELECT range_merge('[1,2)'::int4range, '[5,6)'::int4range) AS v": ('ok', 3904, ['[1,6)']),
 'SELECT int4range_subdiff(5, 2) AS v': ('ok', 701, ['3']),
 'SELECT numrange_subdiff(5.5, 2) AS v': ('ok', 701, ['3.5']),
 "SELECT daterange_subdiff('2020-01-10', '2020-01-01') AS v": ('ok', 701, ['9']),
 "SELECT tsrange_subdiff('2020-01-02', '2020-01-01') AS v": ('ok', 701, ['86400']),
 "SELECT tstzrange_subdiff('2020-01-02', '2020-01-01') AS v": ('ok', 701, ['86400']),
 "SELECT int4range_canonical('(1,5]'::int4range) AS v": ('ok', 3904, ['[2,6)']),
 'SELECT in_range(5, 1, 4, false, true) AS v': ('ok', 16, ['t']),
 'SELECT in_range(5, 1, 3, false, true) AS v': ('ok', 16, ['f']),
 'SELECT in_range(5, 10, 5, true, false) AS v': ('ok', 16, ['t']),
 'SELECT in_range(5, 1, -4, false, true) AS v': ('err',
                                                 '22013',
                                                 'invalid preceding or following size in window function'),
 "SELECT in_range('2020-01-05'::date, '2020-01-01'::date, '4 days'::interval, false, true) AS v": ('ok',
                                                                                                   16,
                                                                                                   ['t']),
 "SELECT '{[1,3),[2,7),empty,(9,10]}'::int4multirange AS v": ('ok', 4451, ['{[1,7),[10,11)}']),
 "SELECT '{[1.5,2)}'::nummultirange AS v": ('ok', 4532, ['{[1.5,2)}']),
 "SELECT '{[2020-01-01,2020-01-02)}'::tsmultirange AS v": ('ok',
                                                           4533,
                                                           ['{["2020-01-01 00:00:00","2020-01-02 '
                                                            '00:00:00")}']),
 "SELECT '{[2020-01-01,2020-01-02)}'::tstzmultirange AS v": ('ok',
                                                             4534,
                                                             ['{["2020-01-01 00:00:00+00","2020-01-02 '
                                                              '00:00:00+00")}']),
 "SELECT '{[2020-01-01,2020-01-02]}'::datemultirange AS v": ('ok', 4535, ['{[2020-01-01,2020-01-03)}']),
 "SELECT '{[1,3)}'::int8multirange AS v": ('ok', 4536, ['{[1,3)}']),
 'SELECT int4multirange() AS v': ('ok', 4451, ['{}']),
 "SELECT int4multirange('[1,2)'::int4range, '[2,3)'::int4range) AS v": ('ok', 4451, ['{[1,3)}']),
 "SELECT int4multirange(VARIADIC ARRAY['[1,2)'::int4range, '[2,3)'::int4range]) AS v": ('ok',
                                                                                        4451,
                                                                                        ['{[1,3)}']),
 'SELECT int4multirange(\'{"[1,2)","[2,3)"}\'::int4range[]) AS v': ('err',
                                                                    '42883',
                                                                    'function int4multirange(int4range[]) '
                                                                    'does not exist'),
 "SELECT multirange('[1,2)'::int4range) AS v": ('ok', 4451, ['{[1,2)}']),
 "SELECT '{[1,3),[5,7)}'::int4multirange @> 5 AS v": ('ok', 16, ['t']),
 "SELECT '{[1,3),[5,7)}'::int4multirange && '[2,6)'::int4range AS v": ('ok', 16, ['t']),
 "SELECT '{[1,3),[5,7)}'::int4multirange + '{[3,5)}'::int4multirange AS v": ('ok', 4451, ['{[1,7)}']),
 "SELECT '{[1,3),[5,7)}'::int4multirange * '{[2,6)}'::int4multirange AS v": ('ok', 4451, ['{[2,3),[5,6)}']),
 "SELECT '{[1,10)}'::int4multirange - '{[3,4)}'::int4multirange AS v": ('ok', 4451, ['{[1,3),[4,10)}']),
 "SELECT lower('{[1,3),[5,7)}'::int4multirange) AS v": ('ok', 23, ['1']),
 "SELECT upper('{[1,3),[5,7)}'::int4multirange) AS v": ('ok', 23, ['7']),
 "SELECT isempty('{}'::int4multirange) AS v": ('ok', 16, ['t']),
 "SELECT range_merge('{[1,3),[5,7)}'::int4multirange) AS v": ('ok', 3904, ['[1,7)']),
 "SELECT unnest('{[1,3),[5,7)}'::int4multirange) AS v": ('ok', 3904, ['[1,3)', '[5,7)']),
 "SELECT range_eq('[1,2)'::int4range, '[1,2)'::int4range) AS v": ('ok', 16, ['t']),
 "SELECT range_contains_elem('[1,2)'::int4range, 1) AS v": ('ok', 16, ['t']),
 "SELECT range_union('[1,2)'::int4range, '[2,3)'::int4range) AS v": ('ok', 3904, ['[1,3)']),
 "SELECT range_cmp('[1,2)'::int4range, '[2,3)'::int4range) AS v": ('ok', 23, ['-1']),
 "SELECT multirange_contains_elem('{[1,2)}'::int4multirange, 1) AS v": ('ok', 16, ['t']),
 "SELECT multirange_union('{[1,2)}'::int4multirange, '{[2,3)}'::int4multirange) AS v": ('ok',
                                                                                        4451,
                                                                                        ['{[1,3)}']),
 "SELECT range_overlaps_multirange('[1,2)'::int4range, '{[1,2)}'::int4multirange) AS v": ('ok', 16, ['t']),
 "SELECT elem_contained_by_multirange(1, '{[1,2)}'::int4multirange) AS v": ('ok', 16, ['t'])}
# fmt: on


@pytest.mark.parametrize("sql", list(RECORDED))
def test_a_range_function_is_answered_as_postgresql_answers_it(conn: Any, sql: str):
    res = conn.conn.pgconn.exec_params(sql.encode(), [], result_format=0)
    recorded = RECORDED[sql]
    if recorded[0] == "err":
        assert res.error_field(ord("C")).decode() == recorded[1], sql
        assert res.error_field(ord("M")).decode() == recorded[2], sql
        return
    assert res.error_message == b"", res.error_message
    assert res.ftype(0) == recorded[1], sql
    rows = [
        None if (value := res.get_value(i, 0)) is None else value.decode()
        for i in range(res.ntuples)
    ]
    if (
        recorded[1] == FLOAT8
    ):  # the wire's digits are a known gap of their own: the value is compared
        assert [v and float(v) for v in rows] == [v and float(v) for v in recorded[2]], sql
        return
    assert rows == recorded[2], sql
