# Copyright (c) 2026 Kenneth Stott
# Canary: 6c1e0b7d-2a94-4f3e-8d51-b9a3c7e2f014
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Bit strings on ClickHouse (REQ-2236; REQ-2167, AN ANSWER WITHIN A STATED BOUND COMES BEFORE A
REFUSAL): each row of polyfill.bits answered on the embedded engine (chdb), against PostgreSQL's
answer. Each overload was refused by name on ClickHouse on the base: ClickHouse has no bit type.

The statements are the function support measure's own calls of each overload and the edges the rows
read (strings of other lengths, hexadecimal input, an index past the string, 32 and 64 bits, a cast
that pads or cuts), answered by the embedded PostgreSQL the unit tests boot (PostgreSQL 16.11)."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import pytest

from provisa.core.pg_error import from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import FunctionRefused, RowRefused
from provisa.transpiler.transpile import transpile

ANSWERED = [  # (statement, PostgreSQL's text of its one row)
    ('SELECT pg_catalog."bit_and"(NULL::bit) AS v', None),
    (
        "SELECT pg_catalog.\"bit_and\"(c1) AS v FROM (VALUES ('1'::bit), ('0'::bit), ('1'::bit)) AS t(c1)",
        "0",
    ),
    ('SELECT pg_catalog."bit_and"(c1) AS v FROM (VALUES (NULL::bit), (NULL::bit)) AS t(c1)', None),
    ('SELECT pg_catalog."bit_count"(NULL::bit) AS v', None),
    ("SELECT pg_catalog.\"bit_count\"('1'::bit) AS v", "1"),
    ("SELECT pg_catalog.\"bit_count\"('0'::bit) AS v", "0"),
    ('SELECT pg_catalog."bit_length"(NULL::bit) AS v', None),
    ("SELECT pg_catalog.\"bit_length\"('1'::bit) AS v", "1"),
    ("SELECT pg_catalog.\"bit_length\"('0'::bit) AS v", "1"),
    ('SELECT pg_catalog."bit_or"(NULL::bit) AS v', None),
    (
        "SELECT pg_catalog.\"bit_or\"(c1) AS v FROM (VALUES ('1'::bit), ('0'::bit), ('1'::bit)) AS t(c1)",
        "1",
    ),
    ('SELECT pg_catalog."bit_or"(c1) AS v FROM (VALUES (NULL::bit), (NULL::bit)) AS t(c1)', None),
    ('SELECT pg_catalog."bit_xor"(NULL::bit) AS v', None),
    (
        "SELECT pg_catalog.\"bit_xor\"(c1) AS v FROM (VALUES ('1'::bit), ('0'::bit), ('1'::bit)) AS t(c1)",
        "0",
    ),
    ('SELECT pg_catalog."bit_xor"(c1) AS v FROM (VALUES (NULL::bit), (NULL::bit)) AS t(c1)', None),
    ('SELECT pg_catalog."bit"(NULL::bit, NULL::integer, NULL::boolean) AS v', None),
    ("SELECT pg_catalog.\"bit\"('1'::bit, 3::integer, true::boolean) AS v", "100"),
    ("SELECT pg_catalog.\"bit\"('0'::bit, (-7)::integer, false::boolean) AS v", "0"),
    (
        "SELECT pg_catalog.\"bit\"('1'::bit, 42::integer, true::boolean) AS v",
        "100000000000000000000000000000000000000000",
    ),
    ('SELECT pg_catalog."bit"(NULL::bit, 3::integer, true::boolean) AS v', None),
    ("SELECT pg_catalog.\"bit\"('1'::bit, NULL::integer, true::boolean) AS v", None),
    ("SELECT pg_catalog.\"bit\"('1'::bit, 3::integer, NULL::boolean) AS v", None),
    ("SELECT pg_catalog.\"bit\"('1'::bit, '-2147483648'::integer, true::boolean) AS v", "1"),
    ("SELECT pg_catalog.\"bit\"('1'::bit, '2147483647'::integer, true::boolean) AS v", "1"),
    ("SELECT pg_catalog.\"bit\"('1'::bit, '0'::integer, true::boolean) AS v", "1"),
    ('SELECT pg_catalog."bitand"(NULL::bit, NULL::bit) AS v', None),
    ("SELECT pg_catalog.\"bitand\"('1'::bit, '1'::bit) AS v", "1"),
    ("SELECT pg_catalog.\"bitand\"('0'::bit, '0'::bit) AS v", "0"),
    ("SELECT pg_catalog.\"bitand\"(NULL::bit, '1'::bit) AS v", None),
    ("SELECT pg_catalog.\"bitand\"('1'::bit, NULL::bit) AS v", None),
    ('SELECT pg_catalog."bitcat"(NULL::bit varying, NULL::bit varying) AS v', None),
    ("SELECT pg_catalog.\"bitcat\"('101'::bit varying, '101'::bit varying) AS v", "101101"),
    ("SELECT pg_catalog.\"bitcat\"('1'::bit varying, '1'::bit varying) AS v", "11"),
    ("SELECT pg_catalog.\"bitcat\"('0011'::bit varying, '0011'::bit varying) AS v", "00110011"),
    ("SELECT pg_catalog.\"bitcat\"(NULL::bit varying, '101'::bit varying) AS v", None),
    ("SELECT pg_catalog.\"bitcat\"('101'::bit varying, NULL::bit varying) AS v", None),
    ("SELECT pg_catalog.\"bitcat\"(''::bit varying, '101'::bit varying) AS v", "101"),
    ("SELECT pg_catalog.\"bitcat\"('101'::bit varying, ''::bit varying) AS v", "101"),
    ('SELECT pg_catalog."bitcmp"(NULL::bit, NULL::bit) AS v', None),
    ("SELECT pg_catalog.\"bitcmp\"('1'::bit, '1'::bit) AS v", "0"),
    ("SELECT pg_catalog.\"bitcmp\"('0'::bit, '0'::bit) AS v", "0"),
    ("SELECT pg_catalog.\"bitcmp\"(NULL::bit, '1'::bit) AS v", None),
    ("SELECT pg_catalog.\"bitcmp\"('1'::bit, NULL::bit) AS v", None),
    ('SELECT pg_catalog."biteq"(NULL::bit, NULL::bit) AS v', None),
    ("SELECT pg_catalog.\"biteq\"('1'::bit, '1'::bit) AS v", "t"),
    ("SELECT pg_catalog.\"biteq\"('0'::bit, '0'::bit) AS v", "t"),
    ("SELECT pg_catalog.\"biteq\"(NULL::bit, '1'::bit) AS v", None),
    ("SELECT pg_catalog.\"biteq\"('1'::bit, NULL::bit) AS v", None),
    ('SELECT pg_catalog."bitge"(NULL::bit, NULL::bit) AS v', None),
    ("SELECT pg_catalog.\"bitge\"('1'::bit, '1'::bit) AS v", "t"),
    ("SELECT pg_catalog.\"bitge\"('0'::bit, '0'::bit) AS v", "t"),
    ("SELECT pg_catalog.\"bitge\"(NULL::bit, '1'::bit) AS v", None),
    ("SELECT pg_catalog.\"bitge\"('1'::bit, NULL::bit) AS v", None),
    ('SELECT pg_catalog."bitgt"(NULL::bit, NULL::bit) AS v', None),
    ("SELECT pg_catalog.\"bitgt\"('1'::bit, '1'::bit) AS v", "f"),
    ("SELECT pg_catalog.\"bitgt\"('0'::bit, '0'::bit) AS v", "f"),
    ("SELECT pg_catalog.\"bitgt\"(NULL::bit, '1'::bit) AS v", None),
    ("SELECT pg_catalog.\"bitgt\"('1'::bit, NULL::bit) AS v", None),
    ('SELECT pg_catalog."bitle"(NULL::bit, NULL::bit) AS v', None),
    ("SELECT pg_catalog.\"bitle\"('1'::bit, '1'::bit) AS v", "t"),
    ("SELECT pg_catalog.\"bitle\"('0'::bit, '0'::bit) AS v", "t"),
    ("SELECT pg_catalog.\"bitle\"(NULL::bit, '1'::bit) AS v", None),
    ("SELECT pg_catalog.\"bitle\"('1'::bit, NULL::bit) AS v", None),
    ('SELECT pg_catalog."bitlt"(NULL::bit, NULL::bit) AS v', None),
    ("SELECT pg_catalog.\"bitlt\"('1'::bit, '1'::bit) AS v", "f"),
    ("SELECT pg_catalog.\"bitlt\"('0'::bit, '0'::bit) AS v", "f"),
    ("SELECT pg_catalog.\"bitlt\"(NULL::bit, '1'::bit) AS v", None),
    ("SELECT pg_catalog.\"bitlt\"('1'::bit, NULL::bit) AS v", None),
    ('SELECT pg_catalog."bitne"(NULL::bit, NULL::bit) AS v', None),
    ("SELECT pg_catalog.\"bitne\"('1'::bit, '1'::bit) AS v", "f"),
    ("SELECT pg_catalog.\"bitne\"('0'::bit, '0'::bit) AS v", "f"),
    ("SELECT pg_catalog.\"bitne\"(NULL::bit, '1'::bit) AS v", None),
    ("SELECT pg_catalog.\"bitne\"('1'::bit, NULL::bit) AS v", None),
    ('SELECT pg_catalog."bitnot"(NULL::bit) AS v', None),
    ("SELECT pg_catalog.\"bitnot\"('1'::bit) AS v", "0"),
    ("SELECT pg_catalog.\"bitnot\"('0'::bit) AS v", "1"),
    ('SELECT pg_catalog."bitor"(NULL::bit, NULL::bit) AS v', None),
    ("SELECT pg_catalog.\"bitor\"('1'::bit, '1'::bit) AS v", "1"),
    ("SELECT pg_catalog.\"bitor\"('0'::bit, '0'::bit) AS v", "0"),
    ("SELECT pg_catalog.\"bitor\"(NULL::bit, '1'::bit) AS v", None),
    ("SELECT pg_catalog.\"bitor\"('1'::bit, NULL::bit) AS v", None),
    ('SELECT pg_catalog."bitshiftleft"(NULL::bit, NULL::integer) AS v', None),
    ("SELECT pg_catalog.\"bitshiftleft\"('1'::bit, 3::integer) AS v", "0"),
    ("SELECT pg_catalog.\"bitshiftleft\"('0'::bit, (-7)::integer) AS v", "0"),
    ("SELECT pg_catalog.\"bitshiftleft\"('1'::bit, 42::integer) AS v", "0"),
    ('SELECT pg_catalog."bitshiftleft"(NULL::bit, 3::integer) AS v', None),
    ("SELECT pg_catalog.\"bitshiftleft\"('1'::bit, NULL::integer) AS v", None),
    ("SELECT pg_catalog.\"bitshiftleft\"('1'::bit, '-2147483648'::integer) AS v", "0"),
    ("SELECT pg_catalog.\"bitshiftleft\"('1'::bit, '2147483647'::integer) AS v", "0"),
    ("SELECT pg_catalog.\"bitshiftleft\"('1'::bit, '0'::integer) AS v", "1"),
    ('SELECT pg_catalog."bitshiftright"(NULL::bit, NULL::integer) AS v', None),
    ("SELECT pg_catalog.\"bitshiftright\"('1'::bit, 3::integer) AS v", "0"),
    ("SELECT pg_catalog.\"bitshiftright\"('0'::bit, (-7)::integer) AS v", "0"),
    ("SELECT pg_catalog.\"bitshiftright\"('1'::bit, 42::integer) AS v", "0"),
    ('SELECT pg_catalog."bitshiftright"(NULL::bit, 3::integer) AS v', None),
    ("SELECT pg_catalog.\"bitshiftright\"('1'::bit, NULL::integer) AS v", None),
    ("SELECT pg_catalog.\"bitshiftright\"('1'::bit, '-2147483648'::integer) AS v", "0"),
    ("SELECT pg_catalog.\"bitshiftright\"('1'::bit, '2147483647'::integer) AS v", "0"),
    ("SELECT pg_catalog.\"bitshiftright\"('1'::bit, '0'::integer) AS v", "1"),
    ('SELECT pg_catalog."bitxor"(NULL::bit, NULL::bit) AS v', None),
    ("SELECT pg_catalog.\"bitxor\"('1'::bit, '1'::bit) AS v", "0"),
    ("SELECT pg_catalog.\"bitxor\"('0'::bit, '0'::bit) AS v", "0"),
    ("SELECT pg_catalog.\"bitxor\"(NULL::bit, '1'::bit) AS v", None),
    ("SELECT pg_catalog.\"bitxor\"('1'::bit, NULL::bit) AS v", None),
    ('SELECT pg_catalog."get_bit"(NULL::bit, NULL::integer) AS v', None),
    ("SELECT pg_catalog.\"get_bit\"('1'::bit, 0::integer) AS v", "1"),
    ('SELECT pg_catalog."get_bit"(NULL::bit, 0::integer) AS v', None),
    ('SELECT pg_catalog."int4"(NULL::bit) AS v', None),
    ("SELECT pg_catalog.\"int4\"('1'::bit) AS v", "1"),
    ("SELECT pg_catalog.\"int4\"('0'::bit) AS v", "0"),
    ('SELECT pg_catalog."int8"(NULL::bit) AS v', None),
    ("SELECT pg_catalog.\"int8\"('1'::bit) AS v", "1"),
    ("SELECT pg_catalog.\"int8\"('0'::bit) AS v", "0"),
    ('SELECT pg_catalog."length"(NULL::bit) AS v', None),
    ("SELECT pg_catalog.\"length\"('1'::bit) AS v", "1"),
    ("SELECT pg_catalog.\"length\"('0'::bit) AS v", "1"),
    ('SELECT pg_catalog."octet_length"(NULL::bit) AS v', None),
    ("SELECT pg_catalog.\"octet_length\"('1'::bit) AS v", "1"),
    ("SELECT pg_catalog.\"octet_length\"('0'::bit) AS v", "1"),
    ('SELECT pg_catalog."overlay"(NULL::bit, NULL::bit, NULL::integer, NULL::integer) AS v', None),
    ("SELECT pg_catalog.\"overlay\"('1'::bit, '1'::bit, 1::integer, 1::integer) AS v", "1"),
    ("SELECT pg_catalog.\"overlay\"('0'::bit, '0'::bit, 2::integer, 2::integer) AS v", "00"),
    ("SELECT pg_catalog.\"overlay\"(NULL::bit, '1'::bit, 1::integer, 1::integer) AS v", None),
    ("SELECT pg_catalog.\"overlay\"('1'::bit, NULL::bit, 1::integer, 1::integer) AS v", None),
    ('SELECT pg_catalog."overlay"(NULL::bit, NULL::bit, NULL::integer) AS v', None),
    ("SELECT pg_catalog.\"overlay\"('1'::bit, '1'::bit, 1::integer) AS v", "1"),
    ("SELECT pg_catalog.\"overlay\"('0'::bit, '0'::bit, 2::integer) AS v", "00"),
    ("SELECT pg_catalog.\"overlay\"(NULL::bit, '1'::bit, 1::integer) AS v", None),
    ("SELECT pg_catalog.\"overlay\"('1'::bit, NULL::bit, 1::integer) AS v", None),
    ('SELECT pg_catalog."position"(NULL::bit, NULL::bit) AS v', None),
    ("SELECT pg_catalog.\"position\"('1'::bit, '1'::bit) AS v", "1"),
    ("SELECT pg_catalog.\"position\"('0'::bit, '0'::bit) AS v", "1"),
    ("SELECT pg_catalog.\"position\"(NULL::bit, '1'::bit) AS v", None),
    ("SELECT pg_catalog.\"position\"('1'::bit, NULL::bit) AS v", None),
    ('SELECT pg_catalog."set_bit"(NULL::bit, NULL::integer, NULL::integer) AS v', None),
    ("SELECT pg_catalog.\"set_bit\"('1'::bit, 0::integer, 1::integer) AS v", "1"),
    ('SELECT pg_catalog."set_bit"(NULL::bit, 0::integer, 1::integer) AS v', None),
    ('SELECT pg_catalog."substring"(NULL::bit, NULL::integer, NULL::integer) AS v', None),
    ("SELECT pg_catalog.\"substring\"('1'::bit, 1::integer, 2::integer) AS v", "1"),
    ("SELECT pg_catalog.\"substring\"('0'::bit, 2::integer, 3::integer) AS v", ""),
    ("SELECT pg_catalog.\"substring\"('1'::bit, 1::integer, 1::integer) AS v", "1"),
    ('SELECT pg_catalog."substring"(NULL::bit, 1::integer, 2::integer) AS v', None),
    ('SELECT pg_catalog."substring"(NULL::bit, NULL::integer) AS v', None),
    ("SELECT pg_catalog.\"substring\"('1'::bit, 1::integer) AS v", "1"),
    ("SELECT pg_catalog.\"substring\"('0'::bit, 2::integer) AS v", ""),
    ('SELECT pg_catalog."substring"(NULL::bit, 1::integer) AS v', None),
    ('SELECT pg_catalog."varbit"(NULL::bit varying, NULL::integer, NULL::boolean) AS v', None),
    ("SELECT pg_catalog.\"varbit\"('101'::bit varying, 3::integer, true::boolean) AS v", "101"),
    ("SELECT pg_catalog.\"varbit\"('1'::bit varying, (-7)::integer, false::boolean) AS v", "1"),
    ("SELECT pg_catalog.\"varbit\"('0011'::bit varying, 42::integer, true::boolean) AS v", "0011"),
    ('SELECT pg_catalog."varbit"(NULL::bit varying, 3::integer, true::boolean) AS v', None),
    ("SELECT pg_catalog.\"varbit\"('101'::bit varying, NULL::integer, true::boolean) AS v", None),
    ("SELECT pg_catalog.\"varbit\"('101'::bit varying, 3::integer, NULL::boolean) AS v", None),
    ("SELECT pg_catalog.\"varbit\"(''::bit varying, 3::integer, true::boolean) AS v", ""),
    (
        "SELECT pg_catalog.\"varbit\"('101'::bit varying, '-2147483648'::integer, true::boolean) AS v",
        "101",
    ),
    (
        "SELECT pg_catalog.\"varbit\"('101'::bit varying, '2147483647'::integer, true::boolean) AS v",
        "101",
    ),
    ("SELECT pg_catalog.\"varbit\"('101'::bit varying, '0'::integer, true::boolean) AS v", "101"),
    ('SELECT pg_catalog."varbitcmp"(NULL::bit varying, NULL::bit varying) AS v', None),
    ("SELECT pg_catalog.\"varbitcmp\"('101'::bit varying, '101'::bit varying) AS v", "0"),
    ("SELECT pg_catalog.\"varbitcmp\"('1'::bit varying, '1'::bit varying) AS v", "0"),
    ("SELECT pg_catalog.\"varbitcmp\"('0011'::bit varying, '0011'::bit varying) AS v", "0"),
    ("SELECT pg_catalog.\"varbitcmp\"(NULL::bit varying, '101'::bit varying) AS v", None),
    ("SELECT pg_catalog.\"varbitcmp\"('101'::bit varying, NULL::bit varying) AS v", None),
    ("SELECT pg_catalog.\"varbitcmp\"(''::bit varying, '101'::bit varying) AS v", "-1"),
    ("SELECT pg_catalog.\"varbitcmp\"('101'::bit varying, ''::bit varying) AS v", "1"),
    ('SELECT pg_catalog."varbiteq"(NULL::bit varying, NULL::bit varying) AS v', None),
    ("SELECT pg_catalog.\"varbiteq\"('101'::bit varying, '101'::bit varying) AS v", "t"),
    ("SELECT pg_catalog.\"varbiteq\"('1'::bit varying, '1'::bit varying) AS v", "t"),
    ("SELECT pg_catalog.\"varbiteq\"('0011'::bit varying, '0011'::bit varying) AS v", "t"),
    ("SELECT pg_catalog.\"varbiteq\"(NULL::bit varying, '101'::bit varying) AS v", None),
    ("SELECT pg_catalog.\"varbiteq\"('101'::bit varying, NULL::bit varying) AS v", None),
    ("SELECT pg_catalog.\"varbiteq\"(''::bit varying, '101'::bit varying) AS v", "f"),
    ("SELECT pg_catalog.\"varbiteq\"('101'::bit varying, ''::bit varying) AS v", "f"),
    ('SELECT pg_catalog."varbitge"(NULL::bit varying, NULL::bit varying) AS v', None),
    ("SELECT pg_catalog.\"varbitge\"('101'::bit varying, '101'::bit varying) AS v", "t"),
    ("SELECT pg_catalog.\"varbitge\"('1'::bit varying, '1'::bit varying) AS v", "t"),
    ("SELECT pg_catalog.\"varbitge\"('0011'::bit varying, '0011'::bit varying) AS v", "t"),
    ("SELECT pg_catalog.\"varbitge\"(NULL::bit varying, '101'::bit varying) AS v", None),
    ("SELECT pg_catalog.\"varbitge\"('101'::bit varying, NULL::bit varying) AS v", None),
    ("SELECT pg_catalog.\"varbitge\"(''::bit varying, '101'::bit varying) AS v", "f"),
    ("SELECT pg_catalog.\"varbitge\"('101'::bit varying, ''::bit varying) AS v", "t"),
    ('SELECT pg_catalog."varbitgt"(NULL::bit varying, NULL::bit varying) AS v', None),
    ("SELECT pg_catalog.\"varbitgt\"('101'::bit varying, '101'::bit varying) AS v", "f"),
    ("SELECT pg_catalog.\"varbitgt\"('1'::bit varying, '1'::bit varying) AS v", "f"),
    ("SELECT pg_catalog.\"varbitgt\"('0011'::bit varying, '0011'::bit varying) AS v", "f"),
    ("SELECT pg_catalog.\"varbitgt\"(NULL::bit varying, '101'::bit varying) AS v", None),
    ("SELECT pg_catalog.\"varbitgt\"('101'::bit varying, NULL::bit varying) AS v", None),
    ("SELECT pg_catalog.\"varbitgt\"(''::bit varying, '101'::bit varying) AS v", "f"),
    ("SELECT pg_catalog.\"varbitgt\"('101'::bit varying, ''::bit varying) AS v", "t"),
    ('SELECT pg_catalog."varbitle"(NULL::bit varying, NULL::bit varying) AS v', None),
    ("SELECT pg_catalog.\"varbitle\"('101'::bit varying, '101'::bit varying) AS v", "t"),
    ("SELECT pg_catalog.\"varbitle\"('1'::bit varying, '1'::bit varying) AS v", "t"),
    ("SELECT pg_catalog.\"varbitle\"('0011'::bit varying, '0011'::bit varying) AS v", "t"),
    ("SELECT pg_catalog.\"varbitle\"(NULL::bit varying, '101'::bit varying) AS v", None),
    ("SELECT pg_catalog.\"varbitle\"('101'::bit varying, NULL::bit varying) AS v", None),
    ("SELECT pg_catalog.\"varbitle\"(''::bit varying, '101'::bit varying) AS v", "t"),
    ("SELECT pg_catalog.\"varbitle\"('101'::bit varying, ''::bit varying) AS v", "f"),
    ('SELECT pg_catalog."varbitlt"(NULL::bit varying, NULL::bit varying) AS v', None),
    ("SELECT pg_catalog.\"varbitlt\"('101'::bit varying, '101'::bit varying) AS v", "f"),
    ("SELECT pg_catalog.\"varbitlt\"('1'::bit varying, '1'::bit varying) AS v", "f"),
    ("SELECT pg_catalog.\"varbitlt\"('0011'::bit varying, '0011'::bit varying) AS v", "f"),
    ("SELECT pg_catalog.\"varbitlt\"(NULL::bit varying, '101'::bit varying) AS v", None),
    ("SELECT pg_catalog.\"varbitlt\"('101'::bit varying, NULL::bit varying) AS v", None),
    ("SELECT pg_catalog.\"varbitlt\"(''::bit varying, '101'::bit varying) AS v", "t"),
    ("SELECT pg_catalog.\"varbitlt\"('101'::bit varying, ''::bit varying) AS v", "f"),
    ('SELECT pg_catalog."varbitne"(NULL::bit varying, NULL::bit varying) AS v', None),
    ("SELECT pg_catalog.\"varbitne\"('101'::bit varying, '101'::bit varying) AS v", "f"),
    ("SELECT pg_catalog.\"varbitne\"('1'::bit varying, '1'::bit varying) AS v", "f"),
    ("SELECT pg_catalog.\"varbitne\"('0011'::bit varying, '0011'::bit varying) AS v", "f"),
    ("SELECT pg_catalog.\"varbitne\"(NULL::bit varying, '101'::bit varying) AS v", None),
    ("SELECT pg_catalog.\"varbitne\"('101'::bit varying, NULL::bit varying) AS v", None),
    ("SELECT pg_catalog.\"varbitne\"(''::bit varying, '101'::bit varying) AS v", "t"),
    ("SELECT pg_catalog.\"varbitne\"('101'::bit varying, ''::bit varying) AS v", "t"),
    ("SELECT pg_catalog.bitcmp('10'::bit(2), '1'::bit(1)) AS v", "1"),
    ("SELECT pg_catalog.bitlt('0'::bit(1), '00'::bit(2)) AS v", "t"),
    ("SELECT pg_catalog.varbitcmp('1'::varbit, '0111'::varbit) AS v", "16"),
    ("SELECT pg_catalog.biteq('10'::bit(2), '1'::bit(1)) AS v", "f"),
    ("SELECT pg_catalog.bitand('1100'::bit(4), '1010'::bit(4)) AS v", "1000"),
    ("SELECT pg_catalog.bitor('1100'::bit(4), '1010'::bit(4)) AS v", "1110"),
    ("SELECT pg_catalog.bitxor('1100'::bit(4), '1010'::bit(4)) AS v", "0110"),
    ("SELECT pg_catalog.bitnot('1100'::bit(4)) AS v", "0011"),
    ("SELECT pg_catalog.bitshiftleft('10110'::bit(5), 2) AS v", "11000"),
    ("SELECT pg_catalog.bitshiftleft('10110'::bit(5), -2) AS v", "00101"),
    ("SELECT pg_catalog.bitshiftright('10110'::bit(5), 2) AS v", "00101"),
    ("SELECT pg_catalog.bitshiftright('10110'::bit(5), 9) AS v", "00000"),
    ("SELECT pg_catalog.bitcat('101'::varbit, ''::varbit) AS v", "101"),
    ("SELECT pg_catalog.bit_count('10110'::bit(5)) AS v", "3"),
    ("SELECT pg_catalog.octet_length('101101011'::bit(9)) AS v", "2"),
    ("SELECT pg_catalog.length('101101011'::bit(9)) AS v", "9"),
    ("SELECT pg_catalog.get_bit('10110'::bit(5), 2) AS v", "1"),
    ("SELECT pg_catalog.set_bit('10110'::bit(5), 1, 1) AS v", "11110"),
    ("SELECT pg_catalog.int4('11111111111111111111111111111111'::bit(32)) AS v", "-1"),
    ("SELECT pg_catalog.int4('101'::bit(3)) AS v", "5"),
    (
        "SELECT pg_catalog.int8('1111111111111111111111111111111111111111111111111111111111111111'::bit(64)) AS v",
        "-1",
    ),
    (
        "SELECT pg_catalog.int8('1000000000000000000000000000000000000000000000000000000000000000'::bit(64)) AS v",
        "-9223372036854775808",
    ),
    ("SELECT pg_catalog.\"bit\"('101'::bit(3), 5, true) AS v", "10100"),
    ("SELECT pg_catalog.\"bit\"('101'::bit(3), 2, true) AS v", "10"),
    ("SELECT pg_catalog.varbit('1011'::varbit, 2, true) AS v", "10"),
    ("SELECT pg_catalog.\"substring\"('10110'::bit(5), 2, 3) AS v", "011"),
    ("SELECT pg_catalog.\"substring\"('10110'::bit(5), -1, 3) AS v", "1"),
    ("SELECT pg_catalog.\"substring\"('10110'::bit(5), 3) AS v", "110"),
    ("SELECT pg_catalog.\"substring\"('10110'::bit(5), 9) AS v", ""),
    ("SELECT pg_catalog.\"overlay\"('10110'::bit(5), '00'::bit(2), 2) AS v", "10010"),
    ("SELECT pg_catalog.\"overlay\"('10110'::bit(5), '00'::bit(2), 2, 3) AS v", "1000"),
    ("SELECT pg_catalog.\"position\"('10110'::bit(5), '11'::bit(2)) AS v", "3"),
    ("SELECT pg_catalog.\"position\"('10110'::bit(5), ''::varbit) AS v", "1"),
    ("SELECT pg_catalog.\"position\"('10110'::bit(5), '111'::bit(3)) AS v", "0"),
    (
        "SELECT pg_catalog.bit_and(c) AS v FROM (VALUES ('1101'::bit(4)), ('0111'::bit(4))) AS t(c)",
        "0101",
    ),
    (
        "SELECT pg_catalog.bit_or(c) AS v FROM (VALUES ('1101'::bit(4)), ('0011'::bit(4))) AS t(c)",
        "1111",
    ),
    (
        "SELECT pg_catalog.bit_xor(c) AS v FROM (VALUES ('1101'::bit(4)), ('0111'::bit(4)), ('0001'::bit(4))) AS t(c)",
        "1011",
    ),
    (
        "SELECT pg_catalog.bit_or(c) AS v FROM (VALUES ('1101'::bit(4)), (NULL::bit(4))) AS t(c)",
        "1101",
    ),
    ("SELECT pg_catalog.bitand('x1F'::bit(8), 'xF0'::bit(8)) AS v", "00010000"),
    ("SELECT pg_catalog.bitnot('101'::bit(2)) AS v", "01"),
    ("SELECT pg_catalog.bitcat('101'::varbit(2), '1'::varbit) AS v", "101"),
    ("SELECT pg_catalog.bitnot('101'::bit) AS v", "0"),
]
RAISED = [
    ("SELECT pg_catalog.\"bit\"('1'::bit, '2147483648'::integer, true::boolean) AS v", "22003"),
    ("SELECT pg_catalog.\"bitshiftleft\"('1'::bit, '2147483648'::integer) AS v", "22003"),
    ("SELECT pg_catalog.\"bitshiftright\"('1'::bit, '2147483648'::integer) AS v", "22003"),
    ("SELECT pg_catalog.\"get_bit\"('0'::bit, 1::integer) AS v", "2202E"),
    ("SELECT pg_catalog.\"set_bit\"('0'::bit, 1::integer, 0::integer) AS v", "2202E"),
    (
        "SELECT pg_catalog.\"varbit\"('101'::bit varying, '2147483648'::integer, true::boolean) AS v",
        "22003",
    ),
    ("SELECT pg_catalog.bitand('1100'::bit(4), '10'::bit(2)) AS v", "22026"),
    ("SELECT pg_catalog.get_bit('10110'::bit(5), 5) AS v", "2202E"),
    ("SELECT pg_catalog.get_bit('10110'::bit(5), -1) AS v", "2202E"),
    ("SELECT pg_catalog.set_bit('10110'::bit(5), 9, 1) AS v", "2202E"),
    ("SELECT pg_catalog.set_bit('10110'::bit(5), 1, 2) AS v", "22023"),
    ("SELECT pg_catalog.int4('111111111111111111111111111111111'::bit(33)) AS v", "22003"),
    ("SELECT pg_catalog.\"bit\"('101'::bit(3), 2, false) AS v", "22026"),
    ("SELECT pg_catalog.varbit('1011'::varbit, 2, false) AS v", "22001"),
    ("SELECT pg_catalog.\"substring\"('10110'::bit(5), 2, -1) AS v", "22011"),
    ("SELECT pg_catalog.\"overlay\"('10110'::bit(5), '00'::bit(2), 0) AS v", "22011"),
    (
        "SELECT pg_catalog.bit_and(c) AS v FROM (VALUES ('1101'::varbit), ('01'::varbit)) AS t(c)",
        "22026",
    ),
    ("SELECT pg_catalog.bitnot('102'::bit(3)) AS v", "22P02"),
]

#: Calls a row refuses by name, each within its stated bound.
REFUSED = [
    "SELECT pg_catalog.bitnot(CAST(c AS bit(3))) AS v FROM (VALUES ('101'::text)) AS t(c)",
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


def test_a_call_whose_argument_is_said_to_be_no_bit_string_is_not_the_row_s():
    sql = "SELECT pg_catalog.bitnot(c) AS v FROM (VALUES ('101'::text)) AS t(c)"
    assert "translate(" not in transpile(sql, "clickhouse", generated=False)


def test_a_cast_that_pads_past_the_bound_raises_0a000(runtime):
    with pytest.raises(Exception) as raised:
        _answer(runtime, "SELECT pg_catalog.\"bit\"('1'::bit, 2000000, true) AS v")
    error = from_engine(raised.value)
    assert error is not None and error.sqlstate == "0A000", str(raised.value)[:300]


@pytest.mark.parametrize("dialect", ["trino", "duckdb"])
def test_every_other_dialect_keeps_what_it_had(dialect):
    sql = "SELECT pg_catalog.\"bit\"('1'::bit, 3, true) AS v"
    if dialect == "trino":
        with pytest.raises(FunctionRefused):
            transpile(sql, dialect, generated=False)
    else:
        assert "BIT" in transpile(sql, dialect, generated=False).upper()
