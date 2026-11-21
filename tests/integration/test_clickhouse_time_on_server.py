# Copyright (c) 2026 Kenneth Stott
# Canary: 58d77957-f6fb-420e-a98e-6f33276f2993
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A time of day on ClickHouse's server, the 24.3 CI runs (REQ-2236, amended 2026-11-15; job J1391).

Each statement that takes or answers a time -- a literal, a cast, time arithmetic, time_in and
time_out, and every pg_catalog overload that takes or returns a time, called as the functions
measure calls it (scripts/conformance/function_support.calls) -- is written by Provisa for
ClickHouse, run on the server and compared with PostgreSQL's answer. ``RECORDED`` holds that
answer, (result type OID, the value's text) or (None, "SQLSTATE: message"), recorded on the
embedded PostgreSQL 16.11 under the functions measure's session (UTC, ISO, IntervalStyle
postgres); the statements tests/unit/pgwire/pg17_type_io_answers.py also recorded on 17.11 agree.

A value is read as pgwire reads it: where the statement's root says its type
(provisa.pgwire.cast_types.result_casts), a text the engine serves for it must be one pgwire reads
back (``of_text``), and the engine's own value is brought to the type (``coerce``)."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import os
import re
from typing import Any

import pytest

from provisa.core.pg_error import from_engine
from provisa.pgwire.cast_types import result_casts
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.polyfill import clocks
from provisa.transpiler.polyfill.notation import Refused
from provisa.transpiler.transpile import transpile
from tests.unit.pgwire.type_io_corpus import same_answer

pytestmark = [pytest.mark.integration]

_BYTEA, _BOOLEAN = 17, 16

#: Each statement and PostgreSQL's answer to it: (OID, text), or (None, "SQLSTATE: message").
RECORDED: dict[str, tuple[int | None, str | None]] = {
    "SELECT '12:30'::time + interval '1 hour' AS v": (1083, "13:30:00"),
    "SELECT '12:30'::time - '10:00:15.5'::time AS v": (1186, "02:29:44.5"),
    "SELECT '12:30'::time - interval '13 hours' AS v": (1083, "23:30:00"),
    "SELECT '12:30'::time < '13:00'::time AS v": (16, "true"),
    "SELECT '12:30'::time = '12:30:00'::time AS v": (16, "true"),
    "SELECT '12:30'::time AS v": (1083, "12:30:00"),
    "SELECT '12:30:45'::time::text AS v": (25, "12:30:45"),
    "SELECT '2024-06-15 12:30:45.5'::timestamp::time AS v": (1083, "12:30:45.5"),
    "SELECT '23:59:59.9999995'::time AS v": (1083, "24:00:00"),
    "SELECT '24:00:00'::time AS v": (1083, "24:00:00"),
    "SELECT '25:00'::time AS v": (None, '22008: date/time field value out of range: "25:00"'),
    "SELECT CAST('12:30:45.5' AS time) AS v": (1083, "12:30:45.5"),
    "SELECT NULL::time AS v": (1083, None),
    "SELECT date '2024-06-15' + time '12:30' AS v": (1114, "2024-06-15 12:30:00"),
    "SELECT date_part('second', time '12:30:45.25') AS v": (701, "45.25"),
    "SELECT extract(hour from time '12:30:45') AS v": (1700, "12"),
    "SELECT interval '90 minutes' + time '23:00' AS v": (1083, "00:30:00"),
    "SELECT pg_catalog.\"date_part\"('hour'::text, '12:30:45'::time without time zone) AS v": (
        701,
        "12",
    ),
    "SELECT pg_catalog.\"date_part\"('minute'::text, '00:00:00'::time without time zone) AS v": (
        701,
        "0",
    ),
    "SELECT pg_catalog.\"date_part\"('second'::text, '23:59:59.5'::time without time zone) AS v": (
        701,
        "59.5",
    ),
    'SELECT pg_catalog."date_part"(NULL::text, NULL::time without time zone) AS v': (701, None),
    "SELECT pg_catalog.\"datetime_pl\"('1999-12-31'::date, '00:00:00'::time without time zone) AS v": (
        1114,
        "1999-12-31 00:00:00",
    ),
    "SELECT pg_catalog.\"datetime_pl\"('2000-02-29'::date, '23:59:59.5'::time without time zone) AS v": (
        1114,
        "2000-02-29 23:59:59.5",
    ),
    "SELECT pg_catalog.\"datetime_pl\"('2024-06-15'::date, '12:30:45'::time without time zone) AS v": (
        1114,
        "2024-06-15 12:30:45",
    ),
    'SELECT pg_catalog."datetime_pl"(NULL::date, NULL::time without time zone) AS v': (1114, None),
    "SELECT pg_catalog.\"extract\"('hour'::text, '12:30:45'::time without time zone) AS v": (
        1700,
        "12",
    ),
    "SELECT pg_catalog.\"extract\"('minute'::text, '00:00:00'::time without time zone) AS v": (
        1700,
        "0",
    ),
    "SELECT pg_catalog.\"extract\"('second'::text, '23:59:59.5'::time without time zone) AS v": (
        1700,
        "59.500000",
    ),
    'SELECT pg_catalog."extract"(NULL::text, NULL::time without time zone) AS v': (1700, None),
    "SELECT pg_catalog.\"in_range\"('00:00:00'::time without time zone, '00:00:00'::time without time zone, '3 mons'::interval, false::boolean, false::boolean) AS v": (
        16,
        "true",
    ),
    "SELECT pg_catalog.\"in_range\"('12:30:45'::time without time zone, '12:30:45'::time without time zone, '1 day 02:03:04'::interval, true::boolean, true::boolean) AS v": (
        16,
        "false",
    ),
    "SELECT pg_catalog.\"in_range\"('23:59:59.5'::time without time zone, '23:59:59.5'::time without time zone, '-1 year 2 days'::interval, true::boolean, true::boolean) AS v": (
        16,
        "true",
    ),
    'SELECT pg_catalog."in_range"(NULL::time without time zone, NULL::time without time zone, NULL::interval, NULL::boolean, NULL::boolean) AS v': (
        16,
        None,
    ),
    "SELECT pg_catalog.\"interval\"('00:00:00'::time without time zone) AS v": (1186, "00:00:00"),
    "SELECT pg_catalog.\"interval\"('12:30:45'::time without time zone) AS v": (1186, "12:30:45"),
    "SELECT pg_catalog.\"interval\"('23:59:59.5'::time without time zone) AS v": (
        1186,
        "23:59:59.5",
    ),
    'SELECT pg_catalog."interval"(NULL::time without time zone) AS v': (1186, None),
    "SELECT pg_catalog.\"interval_pl_time\"('-1 year 2 days'::interval, '23:59:59.5'::time without time zone) AS v": (
        1083,
        "23:59:59.5",
    ),
    "SELECT pg_catalog.\"interval_pl_time\"('1 day 02:03:04'::interval, '12:30:45'::time without time zone) AS v": (
        1083,
        "14:33:49",
    ),
    "SELECT pg_catalog.\"interval_pl_time\"('3 mons'::interval, '00:00:00'::time without time zone) AS v": (
        1083,
        "00:00:00",
    ),
    'SELECT pg_catalog."interval_pl_time"(NULL::interval, NULL::time without time zone) AS v': (
        1083,
        None,
    ),
    'SELECT pg_catalog."make_time"(0::integer, 0::integer, 0::double precision) AS v': (
        1083,
        "00:00:00",
    ),
    'SELECT pg_catalog."make_time"(12::integer, 30::integer, 45.5::double precision) AS v': (
        1083,
        "12:30:45.5",
    ),
    'SELECT pg_catalog."make_time"(23::integer, 59::integer, 59.5::double precision) AS v': (
        1083,
        "23:59:59.5",
    ),
    'SELECT pg_catalog."make_time"(NULL::integer, NULL::integer, NULL::double precision) AS v': (
        1083,
        None,
    ),
    'SELECT pg_catalog."max"(NULL::time without time zone) AS v': (1083, None),
    "SELECT pg_catalog.\"max\"(c1) AS v FROM (VALUES ('12:30:45'::time without time zone), ('00:00:00'::time without time zone), ('23:59:59.5'::time without time zone)) AS t(c1)": (
        1083,
        "23:59:59.5",
    ),
    'SELECT pg_catalog."min"(NULL::time without time zone) AS v': (1083, None),
    "SELECT pg_catalog.\"min\"(c1) AS v FROM (VALUES ('12:30:45'::time without time zone), ('00:00:00'::time without time zone), ('23:59:59.5'::time without time zone)) AS t(c1)": (
        1083,
        "00:00:00",
    ),
    "SELECT pg_catalog.\"overlaps\"('00:00:00'::time without time zone, '00:00:00'::time without time zone, '00:00:00'::time without time zone, '00:00:00'::time without time zone) AS v": (
        16,
        "true",
    ),
    "SELECT pg_catalog.\"overlaps\"('00:00:00'::time without time zone, '00:00:00'::time without time zone, '00:00:00'::time without time zone, '3 mons'::interval) AS v": (
        16,
        "true",
    ),
    "SELECT pg_catalog.\"overlaps\"('00:00:00'::time without time zone, '3 mons'::interval, '00:00:00'::time without time zone, '00:00:00'::time without time zone) AS v": (
        16,
        "true",
    ),
    "SELECT pg_catalog.\"overlaps\"('00:00:00'::time without time zone, '3 mons'::interval, '00:00:00'::time without time zone, '3 mons'::interval) AS v": (
        16,
        "true",
    ),
    "SELECT pg_catalog.\"overlaps\"('12:30:45'::time without time zone, '1 day 02:03:04'::interval, '12:30:45'::time without time zone, '1 day 02:03:04'::interval) AS v": (
        16,
        "true",
    ),
    "SELECT pg_catalog.\"overlaps\"('12:30:45'::time without time zone, '1 day 02:03:04'::interval, '12:30:45'::time without time zone, '12:30:45'::time without time zone) AS v": (
        16,
        "true",
    ),
    "SELECT pg_catalog.\"overlaps\"('12:30:45'::time without time zone, '12:30:45'::time without time zone, '12:30:45'::time without time zone, '1 day 02:03:04'::interval) AS v": (
        16,
        "true",
    ),
    "SELECT pg_catalog.\"overlaps\"('12:30:45'::time without time zone, '12:30:45'::time without time zone, '12:30:45'::time without time zone, '12:30:45'::time without time zone) AS v": (
        16,
        "true",
    ),
    "SELECT pg_catalog.\"overlaps\"('23:59:59.5'::time without time zone, '-1 year 2 days'::interval, '23:59:59.5'::time without time zone, '-1 year 2 days'::interval) AS v": (
        16,
        "true",
    ),
    "SELECT pg_catalog.\"overlaps\"('23:59:59.5'::time without time zone, '-1 year 2 days'::interval, '23:59:59.5'::time without time zone, '23:59:59.5'::time without time zone) AS v": (
        16,
        "true",
    ),
    "SELECT pg_catalog.\"overlaps\"('23:59:59.5'::time without time zone, '23:59:59.5'::time without time zone, '23:59:59.5'::time without time zone, '-1 year 2 days'::interval) AS v": (
        16,
        "true",
    ),
    "SELECT pg_catalog.\"overlaps\"('23:59:59.5'::time without time zone, '23:59:59.5'::time without time zone, '23:59:59.5'::time without time zone, '23:59:59.5'::time without time zone) AS v": (
        16,
        "true",
    ),
    'SELECT pg_catalog."overlaps"(NULL::time without time zone, NULL::interval, NULL::time without time zone, NULL::interval) AS v': (
        16,
        None,
    ),
    'SELECT pg_catalog."overlaps"(NULL::time without time zone, NULL::interval, NULL::time without time zone, NULL::time without time zone) AS v': (
        16,
        None,
    ),
    'SELECT pg_catalog."overlaps"(NULL::time without time zone, NULL::time without time zone, NULL::time without time zone, NULL::interval) AS v': (
        16,
        None,
    ),
    'SELECT pg_catalog."overlaps"(NULL::time without time zone, NULL::time without time zone, NULL::time without time zone, NULL::time without time zone) AS v': (
        16,
        None,
    ),
    "SELECT pg_catalog.\"time\"('-1 year 2 days'::interval) AS v": (1083, "00:00:00"),
    "SELECT pg_catalog.\"time\"('00:00:00'::time without time zone, (-7)::integer) AS v": (
        1083,
        "00:00:00",
    ),
    "SELECT pg_catalog.\"time\"('00:00:00+00'::time with time zone) AS v": (1083, "00:00:00"),
    "SELECT pg_catalog.\"time\"('1 day 02:03:04'::interval) AS v": (1083, "02:03:04"),
    "SELECT pg_catalog.\"time\"('12:30:45'::time without time zone, 3::integer) AS v": (
        1083,
        "12:30:45",
    ),
    "SELECT pg_catalog.\"time\"('12:30:45+02'::time with time zone) AS v": (1083, "12:30:45"),
    "SELECT pg_catalog.\"time\"('1999-12-31 23:59:59'::timestamp without time zone) AS v": (
        1083,
        "23:59:59",
    ),
    "SELECT pg_catalog.\"time\"('1999-12-31 23:59:59+02'::timestamp with time zone) AS v": (
        1083,
        "21:59:59",
    ),
    "SELECT pg_catalog.\"time\"('2000-02-29 00:00:00'::timestamp without time zone) AS v": (
        1083,
        "00:00:00",
    ),
    "SELECT pg_catalog.\"time\"('2000-02-29 00:00:00-05'::timestamp with time zone) AS v": (
        1083,
        "05:00:00",
    ),
    "SELECT pg_catalog.\"time\"('2024-06-15 12:30:45'::timestamp without time zone) AS v": (
        1083,
        "12:30:45",
    ),
    "SELECT pg_catalog.\"time\"('2024-06-15 12:30:45+00'::timestamp with time zone) AS v": (
        1083,
        "12:30:45",
    ),
    "SELECT pg_catalog.\"time\"('23:59:59.5'::time without time zone, 42::integer) AS v": (
        1083,
        "23:59:59.5",
    ),
    "SELECT pg_catalog.\"time\"('23:59:59.5-05'::time with time zone) AS v": (1083, "23:59:59.5"),
    "SELECT pg_catalog.\"time\"('3 mons'::interval) AS v": (1083, "00:00:00"),
    'SELECT pg_catalog."time"(NULL::interval) AS v': (1083, None),
    'SELECT pg_catalog."time"(NULL::time with time zone) AS v': (1083, None),
    'SELECT pg_catalog."time"(NULL::time without time zone, NULL::integer) AS v': (1083, None),
    'SELECT pg_catalog."time"(NULL::timestamp with time zone) AS v': (1083, None),
    'SELECT pg_catalog."time"(NULL::timestamp without time zone) AS v': (1083, None),
    "SELECT pg_catalog.\"time_cmp\"('00:00:00'::time without time zone, '00:00:00'::time without time zone) AS v": (
        23,
        "0",
    ),
    "SELECT pg_catalog.\"time_cmp\"('12:30:45'::time without time zone, '12:30:45'::time without time zone) AS v": (
        23,
        "0",
    ),
    "SELECT pg_catalog.\"time_cmp\"('23:59:59.5'::time without time zone, '23:59:59.5'::time without time zone) AS v": (
        23,
        "0",
    ),
    'SELECT pg_catalog."time_cmp"(NULL::time without time zone, NULL::time without time zone) AS v': (
        23,
        None,
    ),
    "SELECT pg_catalog.\"time_eq\"('00:00:00'::time without time zone, '00:00:00'::time without time zone) AS v": (
        16,
        "true",
    ),
    "SELECT pg_catalog.\"time_eq\"('12:30:45'::time without time zone, '12:30:45'::time without time zone) AS v": (
        16,
        "true",
    ),
    "SELECT pg_catalog.\"time_eq\"('23:59:59.5'::time without time zone, '23:59:59.5'::time without time zone) AS v": (
        16,
        "true",
    ),
    'SELECT pg_catalog."time_eq"(NULL::time without time zone, NULL::time without time zone) AS v': (
        16,
        None,
    ),
    "SELECT pg_catalog.\"time_ge\"('00:00:00'::time without time zone, '00:00:00'::time without time zone) AS v": (
        16,
        "true",
    ),
    "SELECT pg_catalog.\"time_ge\"('12:30:45'::time without time zone, '12:30:45'::time without time zone) AS v": (
        16,
        "true",
    ),
    "SELECT pg_catalog.\"time_ge\"('23:59:59.5'::time without time zone, '23:59:59.5'::time without time zone) AS v": (
        16,
        "true",
    ),
    'SELECT pg_catalog."time_ge"(NULL::time without time zone, NULL::time without time zone) AS v': (
        16,
        None,
    ),
    "SELECT pg_catalog.\"time_gt\"('00:00:00'::time without time zone, '00:00:00'::time without time zone) AS v": (
        16,
        "false",
    ),
    "SELECT pg_catalog.\"time_gt\"('12:30:45'::time without time zone, '12:30:45'::time without time zone) AS v": (
        16,
        "false",
    ),
    "SELECT pg_catalog.\"time_gt\"('23:59:59.5'::time without time zone, '23:59:59.5'::time without time zone) AS v": (
        16,
        "false",
    ),
    'SELECT pg_catalog."time_gt"(NULL::time without time zone, NULL::time without time zone) AS v': (
        16,
        None,
    ),
    "SELECT pg_catalog.\"time_hash\"('00:00:00'::time without time zone) AS v": (23, "-272711505"),
    "SELECT pg_catalog.\"time_hash\"('12:30:45'::time without time zone) AS v": (23, "662686461"),
    "SELECT pg_catalog.\"time_hash\"('23:59:59.5'::time without time zone) AS v": (
        23,
        "1136122426",
    ),
    'SELECT pg_catalog."time_hash"(NULL::time without time zone) AS v': (23, None),
    "SELECT pg_catalog.\"time_hash_extended\"('00:00:00'::time without time zone, (-7)::bigint) AS v": (
        20,
        "6070635583126202392",
    ),
    "SELECT pg_catalog.\"time_hash_extended\"('12:30:45'::time without time zone, 3::bigint) AS v": (
        20,
        "2910291444485371408",
    ),
    "SELECT pg_catalog.\"time_hash_extended\"('23:59:59.5'::time without time zone, 5000000000::bigint) AS v": (
        20,
        "-5158964459425867543",
    ),
    'SELECT pg_catalog."time_hash_extended"(NULL::time without time zone, NULL::bigint) AS v': (
        20,
        None,
    ),
    "SELECT pg_catalog.\"time_in\"('00:00:00'::cstring, 'time without time zone'::regtype::oid, (-1)::integer) AS v": (
        1083,
        "00:00:00",
    ),
    "SELECT pg_catalog.\"time_in\"('12:30:45'::cstring, 'time without time zone'::regtype::oid, (-1)::integer) AS v": (
        1083,
        "12:30:45",
    ),
    "SELECT pg_catalog.\"time_in\"('23:59:59.5'::cstring, 'time without time zone'::regtype::oid, (-1)::integer) AS v": (
        1083,
        "23:59:59.5",
    ),
    'SELECT pg_catalog."time_in"(NULL::cstring, NULL::oid, NULL::integer) AS v': (1083, None),
    "SELECT pg_catalog.\"time_larger\"('00:00:00'::time without time zone, '00:00:00'::time without time zone) AS v": (
        1083,
        "00:00:00",
    ),
    "SELECT pg_catalog.\"time_larger\"('12:30:45'::time without time zone, '12:30:45'::time without time zone) AS v": (
        1083,
        "12:30:45",
    ),
    "SELECT pg_catalog.\"time_larger\"('23:59:59.5'::time without time zone, '23:59:59.5'::time without time zone) AS v": (
        1083,
        "23:59:59.5",
    ),
    'SELECT pg_catalog."time_larger"(NULL::time without time zone, NULL::time without time zone) AS v': (
        1083,
        None,
    ),
    "SELECT pg_catalog.\"time_le\"('00:00:00'::time without time zone, '00:00:00'::time without time zone) AS v": (
        16,
        "true",
    ),
    "SELECT pg_catalog.\"time_le\"('12:30:45'::time without time zone, '12:30:45'::time without time zone) AS v": (
        16,
        "true",
    ),
    "SELECT pg_catalog.\"time_le\"('23:59:59.5'::time without time zone, '23:59:59.5'::time without time zone) AS v": (
        16,
        "true",
    ),
    'SELECT pg_catalog."time_le"(NULL::time without time zone, NULL::time without time zone) AS v': (
        16,
        None,
    ),
    "SELECT pg_catalog.\"time_lt\"('00:00:00'::time without time zone, '00:00:00'::time without time zone) AS v": (
        16,
        "false",
    ),
    "SELECT pg_catalog.\"time_lt\"('12:30:45'::time without time zone, '12:30:45'::time without time zone) AS v": (
        16,
        "false",
    ),
    "SELECT pg_catalog.\"time_lt\"('23:59:59.5'::time without time zone, '23:59:59.5'::time without time zone) AS v": (
        16,
        "false",
    ),
    'SELECT pg_catalog."time_lt"(NULL::time without time zone, NULL::time without time zone) AS v': (
        16,
        None,
    ),
    "SELECT pg_catalog.\"time_mi_interval\"('00:00:00'::time without time zone, '3 mons'::interval) AS v": (
        1083,
        "00:00:00",
    ),
    "SELECT pg_catalog.\"time_mi_interval\"('12:30:45'::time without time zone, '1 day 02:03:04'::interval) AS v": (
        1083,
        "10:27:41",
    ),
    "SELECT pg_catalog.\"time_mi_interval\"('23:59:59.5'::time without time zone, '-1 year 2 days'::interval) AS v": (
        1083,
        "23:59:59.5",
    ),
    'SELECT pg_catalog."time_mi_interval"(NULL::time without time zone, NULL::interval) AS v': (
        1083,
        None,
    ),
    "SELECT pg_catalog.\"time_mi_time\"('00:00:00'::time without time zone, '00:00:00'::time without time zone) AS v": (
        1186,
        "00:00:00",
    ),
    "SELECT pg_catalog.\"time_mi_time\"('12:30:45'::time without time zone, '12:30:45'::time without time zone) AS v": (
        1186,
        "00:00:00",
    ),
    "SELECT pg_catalog.\"time_mi_time\"('23:59:59.5'::time without time zone, '23:59:59.5'::time without time zone) AS v": (
        1186,
        "00:00:00",
    ),
    'SELECT pg_catalog."time_mi_time"(NULL::time without time zone, NULL::time without time zone) AS v': (
        1186,
        None,
    ),
    "SELECT pg_catalog.\"time_ne\"('00:00:00'::time without time zone, '00:00:00'::time without time zone) AS v": (
        16,
        "false",
    ),
    "SELECT pg_catalog.\"time_ne\"('12:30:45'::time without time zone, '12:30:45'::time without time zone) AS v": (
        16,
        "false",
    ),
    "SELECT pg_catalog.\"time_ne\"('23:59:59.5'::time without time zone, '23:59:59.5'::time without time zone) AS v": (
        16,
        "false",
    ),
    'SELECT pg_catalog."time_ne"(NULL::time without time zone, NULL::time without time zone) AS v': (
        16,
        None,
    ),
    "SELECT pg_catalog.\"time_out\"('00:00:00'::time without time zone) AS v": (2275, "00:00:00"),
    "SELECT pg_catalog.\"time_out\"('12:30:45'::time without time zone) AS v": (2275, "12:30:45"),
    "SELECT pg_catalog.\"time_out\"('23:59:59.5'::time without time zone) AS v": (
        2275,
        "23:59:59.5",
    ),
    'SELECT pg_catalog."time_out"(NULL::time without time zone) AS v': (2275, None),
    "SELECT pg_catalog.\"time_pl_interval\"('00:00:00'::time without time zone, '3 mons'::interval) AS v": (
        1083,
        "00:00:00",
    ),
    "SELECT pg_catalog.\"time_pl_interval\"('12:30:45'::time without time zone, '1 day 02:03:04'::interval) AS v": (
        1083,
        "14:33:49",
    ),
    "SELECT pg_catalog.\"time_pl_interval\"('23:59:59.5'::time without time zone, '-1 year 2 days'::interval) AS v": (
        1083,
        "23:59:59.5",
    ),
    'SELECT pg_catalog."time_pl_interval"(NULL::time without time zone, NULL::interval) AS v': (
        1083,
        None,
    ),
    "SELECT pg_catalog.\"time_send\"('00:00:00'::time without time zone) AS v": (
        17,
        "\\x0000000000000000",
    ),
    "SELECT pg_catalog.\"time_send\"('12:30:45'::time without time zone) AS v": (
        17,
        "\\x0000000a7ce42740",
    ),
    "SELECT pg_catalog.\"time_send\"('23:59:59.5'::time without time zone) AS v": (
        17,
        "\\x000000141dcfbee0",
    ),
    'SELECT pg_catalog."time_send"(NULL::time without time zone) AS v': (17, None),
    "SELECT pg_catalog.\"time_smaller\"('00:00:00'::time without time zone, '00:00:00'::time without time zone) AS v": (
        1083,
        "00:00:00",
    ),
    "SELECT pg_catalog.\"time_smaller\"('12:30:45'::time without time zone, '12:30:45'::time without time zone) AS v": (
        1083,
        "12:30:45",
    ),
    "SELECT pg_catalog.\"time_smaller\"('23:59:59.5'::time without time zone, '23:59:59.5'::time without time zone) AS v": (
        1083,
        "23:59:59.5",
    ),
    'SELECT pg_catalog."time_smaller"(NULL::time without time zone, NULL::time without time zone) AS v': (
        1083,
        None,
    ),
    "SELECT pg_catalog.\"timedate_pl\"('00:00:00'::time without time zone, '1999-12-31'::date) AS v": (
        1114,
        "1999-12-31 00:00:00",
    ),
    "SELECT pg_catalog.\"timedate_pl\"('12:30:45'::time without time zone, '2024-06-15'::date) AS v": (
        1114,
        "2024-06-15 12:30:45",
    ),
    "SELECT pg_catalog.\"timedate_pl\"('23:59:59.5'::time without time zone, '2000-02-29'::date) AS v": (
        1114,
        "2000-02-29 23:59:59.5",
    ),
    'SELECT pg_catalog."timedate_pl"(NULL::time without time zone, NULL::date) AS v': (1114, None),
    "SELECT pg_catalog.\"timestamp\"('1999-12-31'::date, '00:00:00'::time without time zone) AS v": (
        1114,
        "1999-12-31 00:00:00",
    ),
    "SELECT pg_catalog.\"timestamp\"('2000-02-29'::date, '23:59:59.5'::time without time zone) AS v": (
        1114,
        "2000-02-29 23:59:59.5",
    ),
    "SELECT pg_catalog.\"timestamp\"('2024-06-15'::date, '12:30:45'::time without time zone) AS v": (
        1114,
        "2024-06-15 12:30:45",
    ),
    'SELECT pg_catalog."timestamp"(NULL::date, NULL::time without time zone) AS v': (1114, None),
    "SELECT pg_catalog.\"timestamptz\"('1999-12-31'::date, '00:00:00'::time without time zone) AS v": (
        1184,
        "1999-12-31 00:00:00+00",
    ),
    "SELECT pg_catalog.\"timestamptz\"('2000-02-29'::date, '23:59:59.5'::time without time zone) AS v": (
        1184,
        "2000-02-29 23:59:59.5+00",
    ),
    "SELECT pg_catalog.\"timestamptz\"('2024-06-15'::date, '12:30:45'::time without time zone) AS v": (
        1184,
        "2024-06-15 12:30:45+00",
    ),
    'SELECT pg_catalog."timestamptz"(NULL::date, NULL::time without time zone) AS v': (1184, None),
    "SELECT pg_catalog.\"timetz\"('00:00:00'::time without time zone) AS v": (1266, "00:00:00+00"),
    "SELECT pg_catalog.\"timetz\"('12:30:45'::time without time zone) AS v": (1266, "12:30:45+00"),
    "SELECT pg_catalog.\"timetz\"('23:59:59.5'::time without time zone) AS v": (
        1266,
        "23:59:59.5+00",
    ),
    'SELECT pg_catalog."timetz"(NULL::time without time zone) AS v': (1266, None),
    "SELECT time '12:30' + date '2024-06-15' AS v": (1114, "2024-06-15 12:30:00"),
    "SELECT time '12:30' AS v": (1083, "12:30:00"),
}


def _outcome(client: Any, sql: str) -> str | None:
    """How Provisa's answer to ``sql`` on the server differs from PostgreSQL's; None where it
    does not."""
    oid, want = RECORDED[sql]
    try:
        written = transpile(sql, "clickhouse")
    except Refused as refused:
        said = f"{refused.refusal.sqlstate}: {refused.refusal.message}"
        return None if oid is None and said == want else f"refused: {said}"
    except FunctionRefused as refused:
        said = f"{getattr(refused, 'sqlstate', '0A000')}: {refused}"
        return None if oid is None and said == want else f"refused: {said}"
    try:
        if oid == _BYTEA:
            # ClickHouse has no bytes but a String, which a text reader decodes.
            digits = client.query(f"SELECT hex(v) FROM ({written})").result_rows[0][0]
            got: Any = None if digits is None else bytes.fromhex(digits)
        else:
            got = client.query(written).result_rows[0][0]
    except Exception as raised:  # noqa: BLE001 -- every statement is reported, not the first
        error = from_engine(raised)
        if error is None:
            return f"engine error: {str(raised).splitlines()[0][:200]}"
        said = f"{error.sqlstate}: {error}"
        return None if oid is None and said == want else f"raised {said}"
    if oid is None:
        return f"answered {got!r}, PostgreSQL raised {want}"
    cast = (result_casts(sql) or (None,))[0]
    if cast is not None and isinstance(got, str) and cast.of_text is not None:
        try:
            cast.of_text(got)  # the text pgwire reads back as the value it is
        except ValueError as unread:
            return f"pgwire does not read {got!r}: {unread}"
    elif cast is not None and cast.coerce is not None and got is not None:
        got = cast.coerce(got)
    if oid == _BOOLEAN and got is not None:
        got = bool(got)
        return None if ("true" if got else "false") == want else f"{got!r}, PostgreSQL {want!r}"
    return None if same_answer(got, oid, want) else f"{got!r}, PostgreSQL {want!r}"


#: The functions still refused by name on ClickHouse, each for a reason stated on its grid row or
#: the old table's refusal: timetz (a pair of numbers, its own job), a timestamptz or a timetz's
#: time in the session's zone, max and min of a column of times (no row reads a time column),
#: in_range (refused before any row, as every engine but PostgreSQL's), make_time, and
#: extract(unit FROM time), which the translator reads as the same node as date_part's.
STILL_REFUSED = frozenset(
    {"timetz", "timestamptz", "time", "max", "min", "in_range", "make_time", "extract"}
)
#: The statements whose answer is 24:00:00, which PostgreSQL's time holds and pgwire reads no time
#: of back (cast_types.clock_of_text): the time row's stated 0A000 (REQ-2236, AN EDGE CASE RAISES).
EDGES = frozenset({"SELECT '24:00:00'::time AS v", "SELECT '23:59:59.9999995'::time AS v"})
_REFUSED_NAMING = re.compile(r"refused: 0A000: (\w+)\(\) has no form on clickhouse")


def _allowed(sql: str, why: str | None) -> bool:
    """Whether ``why`` is no difference: PostgreSQL's answer or error, a stated edge's 0A000, or a
    refusal by name of a function still refused."""
    if why is None:
        return True
    if sql in EDGES:
        return why == f"refused: 0A000: {clocks.LAST_SHOWN.message}"
    refused = _REFUSED_NAMING.fullmatch(why)
    return refused is not None and refused[1] in STILL_REFUSED


@pytest.mark.requires_clickhouse
def test_each_time_statement_answers_on_the_clickhouse_server_as_postgresql_does():
    """One test, every statement: requires_clickhouse starts and removes the service per test."""
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    version = client.query("SELECT version()").result_rows[0][0]
    found = {sql: _outcome(client, sql) for sql in RECORDED}
    for sql, why in found.items():
        print(f"{'answers' if why is None else why[:120]} | {sql}")
    assert {sql: why for sql, why in found.items() if not _allowed(sql, why)} == {}, version
