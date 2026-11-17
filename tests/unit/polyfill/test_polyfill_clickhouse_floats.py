# Copyright (c) 2026 Kenneth Stott
# Canary: 2d7a9e14-6c3b-4f08-9b51-e8f0a3c6d725
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse's rows of float and integer math at its edges, and its readers of a real's and a
numeric's literal (REQ-2236; REQ-2171, the function support measure's edge values): each answered
on the embedded engine (chdb), against PostgreSQL's answer. Each edge answered a wrong value on the
base: NaN compared as IEEE compares it, a domain error or an overflow answered as NaN or infinity,
abs of an integer type's least value widened, '3.4028235e+38'::real read as 3.4028233e+38, and a
numeric NaN read as NULL.

The statements are the function support measure's own calls of each overload (its NULL call,
samples and edge values, function_support.calls and edge_calls) and the edges the rows read (a tie
between two reals, a power of zero to a negative, NaN beside NULL), answered by the embedded
PostgreSQL the unit tests boot (PostgreSQL 16.11, its text output)."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import decimal
import math
import struct

import pytest

from provisa.core.pg_error import from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import FunctionRefused, RowRefused
from provisa.transpiler.polyfill.clickhouse import nearest_real
from provisa.transpiler.transpile import transpile

ANSWERED = [
    ('SELECT pg_catalog."abs"(NULL::bigint) AS v', None),
    ('SELECT pg_catalog."abs"(3::bigint) AS v', "3"),
    ('SELECT pg_catalog."abs"((-7)::bigint) AS v', "7"),
    ('SELECT pg_catalog."abs"(5000000000::bigint) AS v', "5000000000"),
    ("SELECT pg_catalog.\"abs\"('9223372036854775807'::bigint) AS v", "9223372036854775807"),
    ("SELECT pg_catalog.\"abs\"('0'::bigint) AS v", "0"),
    ('SELECT pg_catalog."abs"(NULL::integer) AS v', None),
    ('SELECT pg_catalog."abs"(3::integer) AS v', "3"),
    ('SELECT pg_catalog."abs"((-7)::integer) AS v', "7"),
    ('SELECT pg_catalog."abs"(42::integer) AS v', "42"),
    ("SELECT pg_catalog.\"abs\"('2147483647'::integer) AS v", "2147483647"),
    ("SELECT pg_catalog.\"abs\"('0'::integer) AS v", "0"),
    ('SELECT pg_catalog."abs"(NULL::smallint) AS v', None),
    ('SELECT pg_catalog."abs"(3::smallint) AS v', "3"),
    ('SELECT pg_catalog."abs"((-7)::smallint) AS v', "7"),
    ('SELECT pg_catalog."abs"(42::smallint) AS v', "42"),
    ("SELECT pg_catalog.\"abs\"('32767'::smallint) AS v", "32767"),
    ("SELECT pg_catalog.\"abs\"('0'::smallint) AS v", "0"),
    ('SELECT pg_catalog."abs"(NULL::real) AS v', None),
    ('SELECT pg_catalog."abs"(0.5::real) AS v', "0.5"),
    ('SELECT pg_catalog."abs"((-2.25)::real) AS v', "2.25"),
    ('SELECT pg_catalog."abs"(100::real) AS v', "100"),
    ("SELECT pg_catalog.\"abs\"('NaN'::real) AS v", "NaN"),
    ("SELECT pg_catalog.\"abs\"('-Infinity'::real) AS v", "Infinity"),
    ("SELECT pg_catalog.\"abs\"('-0'::real) AS v", "0"),
    ("SELECT pg_catalog.\"abs\"('3.4028235e+38'::real) AS v", "3.4028235e+38"),
    ('SELECT pg_catalog."abs"(NULL::double precision) AS v', None),
    ('SELECT pg_catalog."abs"(0.5::double precision) AS v', "0.5"),
    ('SELECT pg_catalog."abs"((-2.25)::double precision) AS v', "2.25"),
    ('SELECT pg_catalog."abs"(100::double precision) AS v', "100"),
    ("SELECT pg_catalog.\"abs\"('NaN'::double precision) AS v", "NaN"),
    ("SELECT pg_catalog.\"abs\"('-Infinity'::double precision) AS v", "Infinity"),
    ("SELECT pg_catalog.\"abs\"('-0'::double precision) AS v", "0"),
    (
        "SELECT pg_catalog.\"abs\"('1.7976931348623157e+308'::double precision) AS v",
        "1.7976931348623157e+308",
    ),
    ('SELECT pg_catalog."acos"(NULL::double precision) AS v', None),
    ('SELECT pg_catalog."acos"(0.5::double precision) AS v', "1.0471975511965976"),
    ("SELECT pg_catalog.\"acos\"('NaN'::double precision) AS v", "NaN"),
    ("SELECT pg_catalog.\"acos\"('-0'::double precision) AS v", "1.5707963267948966"),
    ('SELECT pg_catalog."acosh"(NULL::double precision) AS v', None),
    ('SELECT pg_catalog."acosh"(100::double precision) AS v', "5.298292365610484"),
    ("SELECT pg_catalog.\"acosh\"('NaN'::double precision) AS v", "NaN"),
    (
        "SELECT pg_catalog.\"acosh\"('1.7976931348623157e+308'::double precision) AS v",
        "710.4758600739439",
    ),
    ('SELECT pg_catalog."asin"(NULL::double precision) AS v', None),
    ('SELECT pg_catalog."asin"(0.5::double precision) AS v', "0.5235987755982988"),
    ("SELECT pg_catalog.\"asin\"('NaN'::double precision) AS v", "NaN"),
    ("SELECT pg_catalog.\"asin\"('-0'::double precision) AS v", "-0"),
    ('SELECT pg_catalog."atanh"(NULL::double precision) AS v', None),
    ('SELECT pg_catalog."atanh"(0.5::double precision) AS v', "0.5493061443340549"),
    ("SELECT pg_catalog.\"atanh\"('NaN'::double precision) AS v", "NaN"),
    ("SELECT pg_catalog.\"atanh\"('-0'::double precision) AS v", "-0"),
    ('SELECT pg_catalog."sin"(NULL::double precision) AS v', None),
    ('SELECT pg_catalog."sin"(0.5::double precision) AS v', "0.479425538604203"),
    ('SELECT pg_catalog."sin"((-2.25)::double precision) AS v', "-0.7780731968879212"),
    ('SELECT pg_catalog."sin"(100::double precision) AS v', "-0.5063656411097588"),
    ("SELECT pg_catalog.\"sin\"('NaN'::double precision) AS v", "NaN"),
    ("SELECT pg_catalog.\"sin\"('-0'::double precision) AS v", "-0"),
    (
        "SELECT pg_catalog.\"sin\"('1.7976931348623157e+308'::double precision) AS v",
        "0.004961954789184062",
    ),
    ('SELECT pg_catalog."cos"(NULL::double precision) AS v', None),
    ('SELECT pg_catalog."cos"(0.5::double precision) AS v', "0.8775825618903728"),
    ('SELECT pg_catalog."cos"((-2.25)::double precision) AS v', "-0.6281736227227391"),
    ('SELECT pg_catalog."cos"(100::double precision) AS v', "0.8623188722876839"),
    ("SELECT pg_catalog.\"cos\"('NaN'::double precision) AS v", "NaN"),
    ("SELECT pg_catalog.\"cos\"('-0'::double precision) AS v", "1"),
    (
        "SELECT pg_catalog.\"cos\"('1.7976931348623157e+308'::double precision) AS v",
        "-0.9999876894265599",
    ),
    ('SELECT pg_catalog."tan"(NULL::double precision) AS v', None),
    ('SELECT pg_catalog."tan"(0.5::double precision) AS v', "0.5463024898437905"),
    ('SELECT pg_catalog."tan"((-2.25)::double precision) AS v', "1.2386276162240968"),
    ('SELECT pg_catalog."tan"(100::double precision) AS v', "-0.5872139151569291"),
    ("SELECT pg_catalog.\"tan\"('NaN'::double precision) AS v", "NaN"),
    ("SELECT pg_catalog.\"tan\"('-0'::double precision) AS v", "-0"),
    (
        "SELECT pg_catalog.\"tan\"('1.7976931348623157e+308'::double precision) AS v",
        "-0.004962015874444894",
    ),
    ('SELECT pg_catalog."cot"(NULL::double precision) AS v', None),
    ('SELECT pg_catalog."cot"(0.5::double precision) AS v', "1.830487721712452"),
    ('SELECT pg_catalog."cot"((-2.25)::double precision) AS v', "0.8073451511185074"),
    ('SELECT pg_catalog."cot"(100::double precision) AS v', "-1.702956919426469"),
    ("SELECT pg_catalog.\"cot\"('NaN'::double precision) AS v", "NaN"),
    ("SELECT pg_catalog.\"cot\"('-0'::double precision) AS v", "-Infinity"),
    (
        "SELECT pg_catalog.\"cot\"('1.7976931348623157e+308'::double precision) AS v",
        "-201.5309957290032",
    ),
    ('SELECT pg_catalog."sqrt"(NULL::double precision) AS v', None),
    ('SELECT pg_catalog."sqrt"(0.5::double precision) AS v', "0.7071067811865476"),
    ('SELECT pg_catalog."sqrt"(100::double precision) AS v', "10"),
    ("SELECT pg_catalog.\"sqrt\"('NaN'::double precision) AS v", "NaN"),
    ("SELECT pg_catalog.\"sqrt\"('-0'::double precision) AS v", "-0"),
    (
        "SELECT pg_catalog.\"sqrt\"('1.7976931348623157e+308'::double precision) AS v",
        "1.3407807929942596e+154",
    ),
    ('SELECT pg_catalog."log"(NULL::double precision) AS v', None),
    ('SELECT pg_catalog."log"(0.5::double precision) AS v', "-0.3010299956639812"),
    ('SELECT pg_catalog."log"(100::double precision) AS v', "2"),
    ("SELECT pg_catalog.\"log\"('NaN'::double precision) AS v", "NaN"),
    (
        "SELECT pg_catalog.\"log\"('1.7976931348623157e+308'::double precision) AS v",
        "308.25471555991675",
    ),
    ('SELECT pg_catalog."log10"(NULL::double precision) AS v', None),
    ('SELECT pg_catalog."log10"(0.5::double precision) AS v', "-0.3010299956639812"),
    ('SELECT pg_catalog."log10"(100::double precision) AS v', "2"),
    ("SELECT pg_catalog.\"log10\"('NaN'::double precision) AS v", "NaN"),
    (
        "SELECT pg_catalog.\"log10\"('1.7976931348623157e+308'::double precision) AS v",
        "308.25471555991675",
    ),
    ('SELECT pg_catalog."pow"(NULL::double precision, NULL::double precision) AS v', None),
    (
        'SELECT pg_catalog."pow"(0.5::double precision, 0.5::double precision) AS v',
        "0.7071067811865476",
    ),
    ('SELECT pg_catalog."pow"(100::double precision, 100::double precision) AS v', "1e+200"),
    ('SELECT pg_catalog."pow"(NULL::double precision, 0.5::double precision) AS v', None),
    ('SELECT pg_catalog."pow"(0.5::double precision, NULL::double precision) AS v', None),
    ("SELECT pg_catalog.\"pow\"('NaN'::double precision, 0.5::double precision) AS v", "NaN"),
    ("SELECT pg_catalog.\"pow\"(0.5::double precision, 'NaN'::double precision) AS v", "NaN"),
    (
        "SELECT pg_catalog.\"pow\"(0.5::double precision, '-Infinity'::double precision) AS v",
        "Infinity",
    ),
    ("SELECT pg_catalog.\"pow\"('-0'::double precision, 0.5::double precision) AS v", "0"),
    ("SELECT pg_catalog.\"pow\"(0.5::double precision, '-0'::double precision) AS v", "1"),
    ('SELECT pg_catalog."power"(NULL::double precision, NULL::double precision) AS v', None),
    (
        'SELECT pg_catalog."power"(0.5::double precision, 0.5::double precision) AS v',
        "0.7071067811865476",
    ),
    ('SELECT pg_catalog."power"(100::double precision, 100::double precision) AS v', "1e+200"),
    ('SELECT pg_catalog."power"(NULL::double precision, 0.5::double precision) AS v', None),
    ('SELECT pg_catalog."power"(0.5::double precision, NULL::double precision) AS v', None),
    ("SELECT pg_catalog.\"power\"('NaN'::double precision, 0.5::double precision) AS v", "NaN"),
    ("SELECT pg_catalog.\"power\"(0.5::double precision, 'NaN'::double precision) AS v", "NaN"),
    (
        "SELECT pg_catalog.\"power\"(0.5::double precision, '-Infinity'::double precision) AS v",
        "Infinity",
    ),
    ("SELECT pg_catalog.\"power\"('-0'::double precision, 0.5::double precision) AS v", "0"),
    ("SELECT pg_catalog.\"power\"(0.5::double precision, '-0'::double precision) AS v", "1"),
    ('SELECT pg_catalog."degrees"(NULL::double precision) AS v', None),
    ('SELECT pg_catalog."degrees"(0.5::double precision) AS v', "28.64788975654116"),
    ('SELECT pg_catalog."degrees"((-2.25)::double precision) AS v', "-128.9155039044352"),
    ('SELECT pg_catalog."degrees"(100::double precision) AS v', "5729.5779513082325"),
    ("SELECT pg_catalog.\"degrees\"('NaN'::double precision) AS v", "NaN"),
    ("SELECT pg_catalog.\"degrees\"('-Infinity'::double precision) AS v", "-Infinity"),
    ("SELECT pg_catalog.\"degrees\"('-0'::double precision) AS v", "-0"),
    ('SELECT pg_catalog."sign"(NULL::double precision) AS v', None),
    ('SELECT pg_catalog."sign"(0.5::double precision) AS v', "1"),
    ('SELECT pg_catalog."sign"((-2.25)::double precision) AS v', "-1"),
    ('SELECT pg_catalog."sign"(100::double precision) AS v', "1"),
    ("SELECT pg_catalog.\"sign\"('NaN'::double precision) AS v", "0"),
    ("SELECT pg_catalog.\"sign\"('-Infinity'::double precision) AS v", "-1"),
    ("SELECT pg_catalog.\"sign\"('-0'::double precision) AS v", "0"),
    ("SELECT pg_catalog.\"sign\"('1.7976931348623157e+308'::double precision) AS v", "1"),
    ('SELECT pg_catalog."float48ge"(NULL::real, NULL::double precision) AS v', None),
    ('SELECT pg_catalog."float48ge"(0.5::real, 0.5::double precision) AS v', "t"),
    ('SELECT pg_catalog."float48ge"((-2.25)::real, (-2.25)::double precision) AS v', "t"),
    ('SELECT pg_catalog."float48ge"(100::real, 100::double precision) AS v', "t"),
    ('SELECT pg_catalog."float48ge"(NULL::real, 0.5::double precision) AS v', None),
    ('SELECT pg_catalog."float48ge"(0.5::real, NULL::double precision) AS v', None),
    ("SELECT pg_catalog.\"float48ge\"('NaN'::real, 0.5::double precision) AS v", "t"),
    ("SELECT pg_catalog.\"float48ge\"(0.5::real, 'NaN'::double precision) AS v", "f"),
    ("SELECT pg_catalog.\"float48ge\"('-Infinity'::real, 0.5::double precision) AS v", "f"),
    ("SELECT pg_catalog.\"float48ge\"(0.5::real, '-Infinity'::double precision) AS v", "t"),
    ("SELECT pg_catalog.\"float48ge\"('-0'::real, 0.5::double precision) AS v", "f"),
    ("SELECT pg_catalog.\"float48ge\"(0.5::real, '-0'::double precision) AS v", "t"),
    ('SELECT pg_catalog."float48gt"(NULL::real, NULL::double precision) AS v', None),
    ('SELECT pg_catalog."float48gt"(0.5::real, 0.5::double precision) AS v', "f"),
    ('SELECT pg_catalog."float48gt"((-2.25)::real, (-2.25)::double precision) AS v', "f"),
    ('SELECT pg_catalog."float48gt"(100::real, 100::double precision) AS v', "f"),
    ('SELECT pg_catalog."float48gt"(NULL::real, 0.5::double precision) AS v', None),
    ('SELECT pg_catalog."float48gt"(0.5::real, NULL::double precision) AS v', None),
    ("SELECT pg_catalog.\"float48gt\"('NaN'::real, 0.5::double precision) AS v", "t"),
    ("SELECT pg_catalog.\"float48gt\"(0.5::real, 'NaN'::double precision) AS v", "f"),
    ("SELECT pg_catalog.\"float48gt\"('-Infinity'::real, 0.5::double precision) AS v", "f"),
    ("SELECT pg_catalog.\"float48gt\"(0.5::real, '-Infinity'::double precision) AS v", "t"),
    ("SELECT pg_catalog.\"float48gt\"('-0'::real, 0.5::double precision) AS v", "f"),
    ("SELECT pg_catalog.\"float48gt\"(0.5::real, '-0'::double precision) AS v", "t"),
    ('SELECT pg_catalog."float48le"(NULL::real, NULL::double precision) AS v', None),
    ('SELECT pg_catalog."float48le"(0.5::real, 0.5::double precision) AS v', "t"),
    ('SELECT pg_catalog."float48le"((-2.25)::real, (-2.25)::double precision) AS v', "t"),
    ('SELECT pg_catalog."float48le"(100::real, 100::double precision) AS v', "t"),
    ('SELECT pg_catalog."float48le"(NULL::real, 0.5::double precision) AS v', None),
    ('SELECT pg_catalog."float48le"(0.5::real, NULL::double precision) AS v', None),
    ("SELECT pg_catalog.\"float48le\"('NaN'::real, 0.5::double precision) AS v", "f"),
    ("SELECT pg_catalog.\"float48le\"(0.5::real, 'NaN'::double precision) AS v", "t"),
    ("SELECT pg_catalog.\"float48le\"('-Infinity'::real, 0.5::double precision) AS v", "t"),
    ("SELECT pg_catalog.\"float48le\"(0.5::real, '-Infinity'::double precision) AS v", "f"),
    ("SELECT pg_catalog.\"float48le\"('-0'::real, 0.5::double precision) AS v", "t"),
    ("SELECT pg_catalog.\"float48le\"(0.5::real, '-0'::double precision) AS v", "f"),
    ('SELECT pg_catalog."float48lt"(NULL::real, NULL::double precision) AS v', None),
    ('SELECT pg_catalog."float48lt"(0.5::real, 0.5::double precision) AS v', "f"),
    ('SELECT pg_catalog."float48lt"((-2.25)::real, (-2.25)::double precision) AS v', "f"),
    ('SELECT pg_catalog."float48lt"(100::real, 100::double precision) AS v', "f"),
    ('SELECT pg_catalog."float48lt"(NULL::real, 0.5::double precision) AS v', None),
    ('SELECT pg_catalog."float48lt"(0.5::real, NULL::double precision) AS v', None),
    ("SELECT pg_catalog.\"float48lt\"('NaN'::real, 0.5::double precision) AS v", "f"),
    ("SELECT pg_catalog.\"float48lt\"(0.5::real, 'NaN'::double precision) AS v", "t"),
    ("SELECT pg_catalog.\"float48lt\"('-Infinity'::real, 0.5::double precision) AS v", "t"),
    ("SELECT pg_catalog.\"float48lt\"(0.5::real, '-Infinity'::double precision) AS v", "f"),
    ("SELECT pg_catalog.\"float48lt\"('-0'::real, 0.5::double precision) AS v", "t"),
    ("SELECT pg_catalog.\"float48lt\"(0.5::real, '-0'::double precision) AS v", "f"),
    ('SELECT pg_catalog."float48eq"(NULL::real, NULL::double precision) AS v', None),
    ('SELECT pg_catalog."float48eq"(0.5::real, 0.5::double precision) AS v', "t"),
    ('SELECT pg_catalog."float48eq"((-2.25)::real, (-2.25)::double precision) AS v', "t"),
    ('SELECT pg_catalog."float48eq"(100::real, 100::double precision) AS v', "t"),
    ('SELECT pg_catalog."float48eq"(NULL::real, 0.5::double precision) AS v', None),
    ('SELECT pg_catalog."float48eq"(0.5::real, NULL::double precision) AS v', None),
    ("SELECT pg_catalog.\"float48eq\"('NaN'::real, 0.5::double precision) AS v", "f"),
    ("SELECT pg_catalog.\"float48eq\"(0.5::real, 'NaN'::double precision) AS v", "f"),
    ("SELECT pg_catalog.\"float48eq\"('-Infinity'::real, 0.5::double precision) AS v", "f"),
    ("SELECT pg_catalog.\"float48eq\"(0.5::real, '-Infinity'::double precision) AS v", "f"),
    ("SELECT pg_catalog.\"float48eq\"('-0'::real, 0.5::double precision) AS v", "f"),
    ("SELECT pg_catalog.\"float48eq\"(0.5::real, '-0'::double precision) AS v", "f"),
    ('SELECT pg_catalog."float48ne"(NULL::real, NULL::double precision) AS v', None),
    ('SELECT pg_catalog."float48ne"(0.5::real, 0.5::double precision) AS v', "f"),
    ('SELECT pg_catalog."float48ne"((-2.25)::real, (-2.25)::double precision) AS v', "f"),
    ('SELECT pg_catalog."float48ne"(100::real, 100::double precision) AS v', "f"),
    ('SELECT pg_catalog."float48ne"(NULL::real, 0.5::double precision) AS v', None),
    ('SELECT pg_catalog."float48ne"(0.5::real, NULL::double precision) AS v', None),
    ("SELECT pg_catalog.\"float48ne\"('NaN'::real, 0.5::double precision) AS v", "t"),
    ("SELECT pg_catalog.\"float48ne\"(0.5::real, 'NaN'::double precision) AS v", "t"),
    ("SELECT pg_catalog.\"float48ne\"('-Infinity'::real, 0.5::double precision) AS v", "t"),
    ("SELECT pg_catalog.\"float48ne\"(0.5::real, '-Infinity'::double precision) AS v", "t"),
    ("SELECT pg_catalog.\"float48ne\"('-0'::real, 0.5::double precision) AS v", "t"),
    ("SELECT pg_catalog.\"float48ne\"(0.5::real, '-0'::double precision) AS v", "t"),
    ('SELECT pg_catalog."float84ge"(NULL::double precision, NULL::real) AS v', None),
    ('SELECT pg_catalog."float84ge"(0.5::double precision, 0.5::real) AS v', "t"),
    ('SELECT pg_catalog."float84ge"((-2.25)::double precision, (-2.25)::real) AS v', "t"),
    ('SELECT pg_catalog."float84ge"(100::double precision, 100::real) AS v', "t"),
    ('SELECT pg_catalog."float84ge"(NULL::double precision, 0.5::real) AS v', None),
    ('SELECT pg_catalog."float84ge"(0.5::double precision, NULL::real) AS v', None),
    ("SELECT pg_catalog.\"float84ge\"('NaN'::double precision, 0.5::real) AS v", "t"),
    ("SELECT pg_catalog.\"float84ge\"(0.5::double precision, 'NaN'::real) AS v", "f"),
    ("SELECT pg_catalog.\"float84ge\"('-Infinity'::double precision, 0.5::real) AS v", "f"),
    ("SELECT pg_catalog.\"float84ge\"(0.5::double precision, '-Infinity'::real) AS v", "t"),
    ("SELECT pg_catalog.\"float84ge\"('-0'::double precision, 0.5::real) AS v", "f"),
    ("SELECT pg_catalog.\"float84ge\"(0.5::double precision, '-0'::real) AS v", "t"),
    ('SELECT pg_catalog."float84gt"(NULL::double precision, NULL::real) AS v', None),
    ('SELECT pg_catalog."float84gt"(0.5::double precision, 0.5::real) AS v', "f"),
    ('SELECT pg_catalog."float84gt"((-2.25)::double precision, (-2.25)::real) AS v', "f"),
    ('SELECT pg_catalog."float84gt"(100::double precision, 100::real) AS v', "f"),
    ('SELECT pg_catalog."float84gt"(NULL::double precision, 0.5::real) AS v', None),
    ('SELECT pg_catalog."float84gt"(0.5::double precision, NULL::real) AS v', None),
    ("SELECT pg_catalog.\"float84gt\"('NaN'::double precision, 0.5::real) AS v", "t"),
    ("SELECT pg_catalog.\"float84gt\"(0.5::double precision, 'NaN'::real) AS v", "f"),
    ("SELECT pg_catalog.\"float84gt\"('-Infinity'::double precision, 0.5::real) AS v", "f"),
    ("SELECT pg_catalog.\"float84gt\"(0.5::double precision, '-Infinity'::real) AS v", "t"),
    ("SELECT pg_catalog.\"float84gt\"('-0'::double precision, 0.5::real) AS v", "f"),
    ("SELECT pg_catalog.\"float84gt\"(0.5::double precision, '-0'::real) AS v", "t"),
    ('SELECT pg_catalog."float84le"(NULL::double precision, NULL::real) AS v', None),
    ('SELECT pg_catalog."float84le"(0.5::double precision, 0.5::real) AS v', "t"),
    ('SELECT pg_catalog."float84le"((-2.25)::double precision, (-2.25)::real) AS v', "t"),
    ('SELECT pg_catalog."float84le"(100::double precision, 100::real) AS v', "t"),
    ('SELECT pg_catalog."float84le"(NULL::double precision, 0.5::real) AS v', None),
    ('SELECT pg_catalog."float84le"(0.5::double precision, NULL::real) AS v', None),
    ("SELECT pg_catalog.\"float84le\"('NaN'::double precision, 0.5::real) AS v", "f"),
    ("SELECT pg_catalog.\"float84le\"(0.5::double precision, 'NaN'::real) AS v", "t"),
    ("SELECT pg_catalog.\"float84le\"('-Infinity'::double precision, 0.5::real) AS v", "t"),
    ("SELECT pg_catalog.\"float84le\"(0.5::double precision, '-Infinity'::real) AS v", "f"),
    ("SELECT pg_catalog.\"float84le\"('-0'::double precision, 0.5::real) AS v", "t"),
    ("SELECT pg_catalog.\"float84le\"(0.5::double precision, '-0'::real) AS v", "f"),
    ('SELECT pg_catalog."float84lt"(NULL::double precision, NULL::real) AS v', None),
    ('SELECT pg_catalog."float84lt"(0.5::double precision, 0.5::real) AS v', "f"),
    ('SELECT pg_catalog."float84lt"((-2.25)::double precision, (-2.25)::real) AS v', "f"),
    ('SELECT pg_catalog."float84lt"(100::double precision, 100::real) AS v', "f"),
    ('SELECT pg_catalog."float84lt"(NULL::double precision, 0.5::real) AS v', None),
    ('SELECT pg_catalog."float84lt"(0.5::double precision, NULL::real) AS v', None),
    ("SELECT pg_catalog.\"float84lt\"('NaN'::double precision, 0.5::real) AS v", "f"),
    ("SELECT pg_catalog.\"float84lt\"(0.5::double precision, 'NaN'::real) AS v", "t"),
    ("SELECT pg_catalog.\"float84lt\"('-Infinity'::double precision, 0.5::real) AS v", "t"),
    ("SELECT pg_catalog.\"float84lt\"(0.5::double precision, '-Infinity'::real) AS v", "f"),
    ("SELECT pg_catalog.\"float84lt\"('-0'::double precision, 0.5::real) AS v", "t"),
    ("SELECT pg_catalog.\"float84lt\"(0.5::double precision, '-0'::real) AS v", "f"),
    ('SELECT pg_catalog."float84eq"(NULL::double precision, NULL::real) AS v', None),
    ('SELECT pg_catalog."float84eq"(0.5::double precision, 0.5::real) AS v', "t"),
    ('SELECT pg_catalog."float84eq"((-2.25)::double precision, (-2.25)::real) AS v', "t"),
    ('SELECT pg_catalog."float84eq"(100::double precision, 100::real) AS v', "t"),
    ('SELECT pg_catalog."float84eq"(NULL::double precision, 0.5::real) AS v', None),
    ('SELECT pg_catalog."float84eq"(0.5::double precision, NULL::real) AS v', None),
    ("SELECT pg_catalog.\"float84eq\"('NaN'::double precision, 0.5::real) AS v", "f"),
    ("SELECT pg_catalog.\"float84eq\"(0.5::double precision, 'NaN'::real) AS v", "f"),
    ("SELECT pg_catalog.\"float84eq\"('-Infinity'::double precision, 0.5::real) AS v", "f"),
    ("SELECT pg_catalog.\"float84eq\"(0.5::double precision, '-Infinity'::real) AS v", "f"),
    ("SELECT pg_catalog.\"float84eq\"('-0'::double precision, 0.5::real) AS v", "f"),
    ("SELECT pg_catalog.\"float84eq\"(0.5::double precision, '-0'::real) AS v", "f"),
    ('SELECT pg_catalog."float84ne"(NULL::double precision, NULL::real) AS v', None),
    ('SELECT pg_catalog."float84ne"(0.5::double precision, 0.5::real) AS v', "f"),
    ('SELECT pg_catalog."float84ne"((-2.25)::double precision, (-2.25)::real) AS v', "f"),
    ('SELECT pg_catalog."float84ne"(100::double precision, 100::real) AS v', "f"),
    ('SELECT pg_catalog."float84ne"(NULL::double precision, 0.5::real) AS v', None),
    ('SELECT pg_catalog."float84ne"(0.5::double precision, NULL::real) AS v', None),
    ("SELECT pg_catalog.\"float84ne\"('NaN'::double precision, 0.5::real) AS v", "t"),
    ("SELECT pg_catalog.\"float84ne\"(0.5::double precision, 'NaN'::real) AS v", "t"),
    ("SELECT pg_catalog.\"float84ne\"('-Infinity'::double precision, 0.5::real) AS v", "t"),
    ("SELECT pg_catalog.\"float84ne\"(0.5::double precision, '-Infinity'::real) AS v", "t"),
    ("SELECT pg_catalog.\"float84ne\"('-0'::double precision, 0.5::real) AS v", "t"),
    ("SELECT pg_catalog.\"float84ne\"(0.5::double precision, '-0'::real) AS v", "t"),
    ('SELECT pg_catalog."float4ge"(NULL::real, NULL::real) AS v', None),
    ('SELECT pg_catalog."float4ge"(0.5::real, 0.5::real) AS v', "t"),
    ('SELECT pg_catalog."float4ge"((-2.25)::real, (-2.25)::real) AS v', "t"),
    ('SELECT pg_catalog."float4ge"(100::real, 100::real) AS v', "t"),
    ('SELECT pg_catalog."float4ge"(NULL::real, 0.5::real) AS v', None),
    ('SELECT pg_catalog."float4ge"(0.5::real, NULL::real) AS v', None),
    ("SELECT pg_catalog.\"float4ge\"('NaN'::real, 0.5::real) AS v", "t"),
    ("SELECT pg_catalog.\"float4ge\"(0.5::real, 'NaN'::real) AS v", "f"),
    ("SELECT pg_catalog.\"float4ge\"('-Infinity'::real, 0.5::real) AS v", "f"),
    ("SELECT pg_catalog.\"float4ge\"(0.5::real, '-Infinity'::real) AS v", "t"),
    ("SELECT pg_catalog.\"float4ge\"('-0'::real, 0.5::real) AS v", "f"),
    ("SELECT pg_catalog.\"float4ge\"(0.5::real, '-0'::real) AS v", "t"),
    ('SELECT pg_catalog."float4gt"(NULL::real, NULL::real) AS v', None),
    ('SELECT pg_catalog."float4gt"(0.5::real, 0.5::real) AS v', "f"),
    ('SELECT pg_catalog."float4gt"((-2.25)::real, (-2.25)::real) AS v', "f"),
    ('SELECT pg_catalog."float4gt"(100::real, 100::real) AS v', "f"),
    ('SELECT pg_catalog."float4gt"(NULL::real, 0.5::real) AS v', None),
    ('SELECT pg_catalog."float4gt"(0.5::real, NULL::real) AS v', None),
    ("SELECT pg_catalog.\"float4gt\"('NaN'::real, 0.5::real) AS v", "t"),
    ("SELECT pg_catalog.\"float4gt\"(0.5::real, 'NaN'::real) AS v", "f"),
    ("SELECT pg_catalog.\"float4gt\"('-Infinity'::real, 0.5::real) AS v", "f"),
    ("SELECT pg_catalog.\"float4gt\"(0.5::real, '-Infinity'::real) AS v", "t"),
    ("SELECT pg_catalog.\"float4gt\"('-0'::real, 0.5::real) AS v", "f"),
    ("SELECT pg_catalog.\"float4gt\"(0.5::real, '-0'::real) AS v", "t"),
    ('SELECT pg_catalog."float4le"(NULL::real, NULL::real) AS v', None),
    ('SELECT pg_catalog."float4le"(0.5::real, 0.5::real) AS v', "t"),
    ('SELECT pg_catalog."float4le"((-2.25)::real, (-2.25)::real) AS v', "t"),
    ('SELECT pg_catalog."float4le"(100::real, 100::real) AS v', "t"),
    ('SELECT pg_catalog."float4le"(NULL::real, 0.5::real) AS v', None),
    ('SELECT pg_catalog."float4le"(0.5::real, NULL::real) AS v', None),
    ("SELECT pg_catalog.\"float4le\"('NaN'::real, 0.5::real) AS v", "f"),
    ("SELECT pg_catalog.\"float4le\"(0.5::real, 'NaN'::real) AS v", "t"),
    ("SELECT pg_catalog.\"float4le\"('-Infinity'::real, 0.5::real) AS v", "t"),
    ("SELECT pg_catalog.\"float4le\"(0.5::real, '-Infinity'::real) AS v", "f"),
    ("SELECT pg_catalog.\"float4le\"('-0'::real, 0.5::real) AS v", "t"),
    ("SELECT pg_catalog.\"float4le\"(0.5::real, '-0'::real) AS v", "f"),
    ('SELECT pg_catalog."float4lt"(NULL::real, NULL::real) AS v', None),
    ('SELECT pg_catalog."float4lt"(0.5::real, 0.5::real) AS v', "f"),
    ('SELECT pg_catalog."float4lt"((-2.25)::real, (-2.25)::real) AS v', "f"),
    ('SELECT pg_catalog."float4lt"(100::real, 100::real) AS v', "f"),
    ('SELECT pg_catalog."float4lt"(NULL::real, 0.5::real) AS v', None),
    ('SELECT pg_catalog."float4lt"(0.5::real, NULL::real) AS v', None),
    ("SELECT pg_catalog.\"float4lt\"('NaN'::real, 0.5::real) AS v", "f"),
    ("SELECT pg_catalog.\"float4lt\"(0.5::real, 'NaN'::real) AS v", "t"),
    ("SELECT pg_catalog.\"float4lt\"('-Infinity'::real, 0.5::real) AS v", "t"),
    ("SELECT pg_catalog.\"float4lt\"(0.5::real, '-Infinity'::real) AS v", "f"),
    ("SELECT pg_catalog.\"float4lt\"('-0'::real, 0.5::real) AS v", "t"),
    ("SELECT pg_catalog.\"float4lt\"(0.5::real, '-0'::real) AS v", "f"),
    ('SELECT pg_catalog."float4eq"(NULL::real, NULL::real) AS v', None),
    ('SELECT pg_catalog."float4eq"(0.5::real, 0.5::real) AS v', "t"),
    ('SELECT pg_catalog."float4eq"((-2.25)::real, (-2.25)::real) AS v', "t"),
    ('SELECT pg_catalog."float4eq"(100::real, 100::real) AS v', "t"),
    ('SELECT pg_catalog."float4eq"(NULL::real, 0.5::real) AS v', None),
    ('SELECT pg_catalog."float4eq"(0.5::real, NULL::real) AS v', None),
    ("SELECT pg_catalog.\"float4eq\"('NaN'::real, 0.5::real) AS v", "f"),
    ("SELECT pg_catalog.\"float4eq\"(0.5::real, 'NaN'::real) AS v", "f"),
    ("SELECT pg_catalog.\"float4eq\"('-Infinity'::real, 0.5::real) AS v", "f"),
    ("SELECT pg_catalog.\"float4eq\"(0.5::real, '-Infinity'::real) AS v", "f"),
    ("SELECT pg_catalog.\"float4eq\"('-0'::real, 0.5::real) AS v", "f"),
    ("SELECT pg_catalog.\"float4eq\"(0.5::real, '-0'::real) AS v", "f"),
    ('SELECT pg_catalog."float4ne"(NULL::real, NULL::real) AS v', None),
    ('SELECT pg_catalog."float4ne"(0.5::real, 0.5::real) AS v', "f"),
    ('SELECT pg_catalog."float4ne"((-2.25)::real, (-2.25)::real) AS v', "f"),
    ('SELECT pg_catalog."float4ne"(100::real, 100::real) AS v', "f"),
    ('SELECT pg_catalog."float4ne"(NULL::real, 0.5::real) AS v', None),
    ('SELECT pg_catalog."float4ne"(0.5::real, NULL::real) AS v', None),
    ("SELECT pg_catalog.\"float4ne\"('NaN'::real, 0.5::real) AS v", "t"),
    ("SELECT pg_catalog.\"float4ne\"(0.5::real, 'NaN'::real) AS v", "t"),
    ("SELECT pg_catalog.\"float4ne\"('-Infinity'::real, 0.5::real) AS v", "t"),
    ("SELECT pg_catalog.\"float4ne\"(0.5::real, '-Infinity'::real) AS v", "t"),
    ("SELECT pg_catalog.\"float4ne\"('-0'::real, 0.5::real) AS v", "t"),
    ("SELECT pg_catalog.\"float4ne\"(0.5::real, '-0'::real) AS v", "t"),
    ('SELECT pg_catalog."float8ge"(NULL::double precision, NULL::double precision) AS v', None),
    ('SELECT pg_catalog."float8ge"(0.5::double precision, 0.5::double precision) AS v', "t"),
    (
        'SELECT pg_catalog."float8ge"((-2.25)::double precision, (-2.25)::double precision) AS v',
        "t",
    ),
    ('SELECT pg_catalog."float8ge"(100::double precision, 100::double precision) AS v', "t"),
    ('SELECT pg_catalog."float8ge"(NULL::double precision, 0.5::double precision) AS v', None),
    ('SELECT pg_catalog."float8ge"(0.5::double precision, NULL::double precision) AS v', None),
    ("SELECT pg_catalog.\"float8ge\"('NaN'::double precision, 0.5::double precision) AS v", "t"),
    ("SELECT pg_catalog.\"float8ge\"(0.5::double precision, 'NaN'::double precision) AS v", "f"),
    (
        "SELECT pg_catalog.\"float8ge\"('-Infinity'::double precision, 0.5::double precision) AS v",
        "f",
    ),
    (
        "SELECT pg_catalog.\"float8ge\"(0.5::double precision, '-Infinity'::double precision) AS v",
        "t",
    ),
    ("SELECT pg_catalog.\"float8ge\"('-0'::double precision, 0.5::double precision) AS v", "f"),
    ("SELECT pg_catalog.\"float8ge\"(0.5::double precision, '-0'::double precision) AS v", "t"),
    ('SELECT pg_catalog."float8gt"(NULL::double precision, NULL::double precision) AS v', None),
    ('SELECT pg_catalog."float8gt"(0.5::double precision, 0.5::double precision) AS v', "f"),
    (
        'SELECT pg_catalog."float8gt"((-2.25)::double precision, (-2.25)::double precision) AS v',
        "f",
    ),
    ('SELECT pg_catalog."float8gt"(100::double precision, 100::double precision) AS v', "f"),
    ('SELECT pg_catalog."float8gt"(NULL::double precision, 0.5::double precision) AS v', None),
    ('SELECT pg_catalog."float8gt"(0.5::double precision, NULL::double precision) AS v', None),
    ("SELECT pg_catalog.\"float8gt\"('NaN'::double precision, 0.5::double precision) AS v", "t"),
    ("SELECT pg_catalog.\"float8gt\"(0.5::double precision, 'NaN'::double precision) AS v", "f"),
    (
        "SELECT pg_catalog.\"float8gt\"('-Infinity'::double precision, 0.5::double precision) AS v",
        "f",
    ),
    (
        "SELECT pg_catalog.\"float8gt\"(0.5::double precision, '-Infinity'::double precision) AS v",
        "t",
    ),
    ("SELECT pg_catalog.\"float8gt\"('-0'::double precision, 0.5::double precision) AS v", "f"),
    ("SELECT pg_catalog.\"float8gt\"(0.5::double precision, '-0'::double precision) AS v", "t"),
    ('SELECT pg_catalog."float8le"(NULL::double precision, NULL::double precision) AS v', None),
    ('SELECT pg_catalog."float8le"(0.5::double precision, 0.5::double precision) AS v', "t"),
    (
        'SELECT pg_catalog."float8le"((-2.25)::double precision, (-2.25)::double precision) AS v',
        "t",
    ),
    ('SELECT pg_catalog."float8le"(100::double precision, 100::double precision) AS v', "t"),
    ('SELECT pg_catalog."float8le"(NULL::double precision, 0.5::double precision) AS v', None),
    ('SELECT pg_catalog."float8le"(0.5::double precision, NULL::double precision) AS v', None),
    ("SELECT pg_catalog.\"float8le\"('NaN'::double precision, 0.5::double precision) AS v", "f"),
    ("SELECT pg_catalog.\"float8le\"(0.5::double precision, 'NaN'::double precision) AS v", "t"),
    (
        "SELECT pg_catalog.\"float8le\"('-Infinity'::double precision, 0.5::double precision) AS v",
        "t",
    ),
    (
        "SELECT pg_catalog.\"float8le\"(0.5::double precision, '-Infinity'::double precision) AS v",
        "f",
    ),
    ("SELECT pg_catalog.\"float8le\"('-0'::double precision, 0.5::double precision) AS v", "t"),
    ("SELECT pg_catalog.\"float8le\"(0.5::double precision, '-0'::double precision) AS v", "f"),
    ('SELECT pg_catalog."float8lt"(NULL::double precision, NULL::double precision) AS v', None),
    ('SELECT pg_catalog."float8lt"(0.5::double precision, 0.5::double precision) AS v', "f"),
    (
        'SELECT pg_catalog."float8lt"((-2.25)::double precision, (-2.25)::double precision) AS v',
        "f",
    ),
    ('SELECT pg_catalog."float8lt"(100::double precision, 100::double precision) AS v', "f"),
    ('SELECT pg_catalog."float8lt"(NULL::double precision, 0.5::double precision) AS v', None),
    ('SELECT pg_catalog."float8lt"(0.5::double precision, NULL::double precision) AS v', None),
    ("SELECT pg_catalog.\"float8lt\"('NaN'::double precision, 0.5::double precision) AS v", "f"),
    ("SELECT pg_catalog.\"float8lt\"(0.5::double precision, 'NaN'::double precision) AS v", "t"),
    (
        "SELECT pg_catalog.\"float8lt\"('-Infinity'::double precision, 0.5::double precision) AS v",
        "t",
    ),
    (
        "SELECT pg_catalog.\"float8lt\"(0.5::double precision, '-Infinity'::double precision) AS v",
        "f",
    ),
    ("SELECT pg_catalog.\"float8lt\"('-0'::double precision, 0.5::double precision) AS v", "t"),
    ("SELECT pg_catalog.\"float8lt\"(0.5::double precision, '-0'::double precision) AS v", "f"),
    ('SELECT pg_catalog."float8eq"(NULL::double precision, NULL::double precision) AS v', None),
    ('SELECT pg_catalog."float8eq"(0.5::double precision, 0.5::double precision) AS v', "t"),
    (
        'SELECT pg_catalog."float8eq"((-2.25)::double precision, (-2.25)::double precision) AS v',
        "t",
    ),
    ('SELECT pg_catalog."float8eq"(100::double precision, 100::double precision) AS v', "t"),
    ('SELECT pg_catalog."float8eq"(NULL::double precision, 0.5::double precision) AS v', None),
    ('SELECT pg_catalog."float8eq"(0.5::double precision, NULL::double precision) AS v', None),
    ("SELECT pg_catalog.\"float8eq\"('NaN'::double precision, 0.5::double precision) AS v", "f"),
    ("SELECT pg_catalog.\"float8eq\"(0.5::double precision, 'NaN'::double precision) AS v", "f"),
    (
        "SELECT pg_catalog.\"float8eq\"('-Infinity'::double precision, 0.5::double precision) AS v",
        "f",
    ),
    (
        "SELECT pg_catalog.\"float8eq\"(0.5::double precision, '-Infinity'::double precision) AS v",
        "f",
    ),
    ("SELECT pg_catalog.\"float8eq\"('-0'::double precision, 0.5::double precision) AS v", "f"),
    ("SELECT pg_catalog.\"float8eq\"(0.5::double precision, '-0'::double precision) AS v", "f"),
    ('SELECT pg_catalog."float8ne"(NULL::double precision, NULL::double precision) AS v', None),
    ('SELECT pg_catalog."float8ne"(0.5::double precision, 0.5::double precision) AS v', "f"),
    (
        'SELECT pg_catalog."float8ne"((-2.25)::double precision, (-2.25)::double precision) AS v',
        "f",
    ),
    ('SELECT pg_catalog."float8ne"(100::double precision, 100::double precision) AS v', "f"),
    ('SELECT pg_catalog."float8ne"(NULL::double precision, 0.5::double precision) AS v', None),
    ('SELECT pg_catalog."float8ne"(0.5::double precision, NULL::double precision) AS v', None),
    ("SELECT pg_catalog.\"float8ne\"('NaN'::double precision, 0.5::double precision) AS v", "t"),
    ("SELECT pg_catalog.\"float8ne\"(0.5::double precision, 'NaN'::double precision) AS v", "t"),
    (
        "SELECT pg_catalog.\"float8ne\"('-Infinity'::double precision, 0.5::double precision) AS v",
        "t",
    ),
    (
        "SELECT pg_catalog.\"float8ne\"(0.5::double precision, '-Infinity'::double precision) AS v",
        "t",
    ),
    ("SELECT pg_catalog.\"float8ne\"('-0'::double precision, 0.5::double precision) AS v", "t"),
    ("SELECT pg_catalog.\"float8ne\"(0.5::double precision, '-0'::double precision) AS v", "t"),
    ('SELECT pg_catalog."float4larger"(NULL::real, NULL::real) AS v', None),
    ('SELECT pg_catalog."float4larger"(0.5::real, 0.5::real) AS v', "0.5"),
    ('SELECT pg_catalog."float4larger"((-2.25)::real, (-2.25)::real) AS v', "-2.25"),
    ('SELECT pg_catalog."float4larger"(100::real, 100::real) AS v', "100"),
    ('SELECT pg_catalog."float4larger"(NULL::real, 0.5::real) AS v', None),
    ('SELECT pg_catalog."float4larger"(0.5::real, NULL::real) AS v', None),
    ("SELECT pg_catalog.\"float4larger\"('NaN'::real, 0.5::real) AS v", "NaN"),
    ("SELECT pg_catalog.\"float4larger\"(0.5::real, 'NaN'::real) AS v", "NaN"),
    ("SELECT pg_catalog.\"float4larger\"('-Infinity'::real, 0.5::real) AS v", "0.5"),
    ("SELECT pg_catalog.\"float4larger\"(0.5::real, '-Infinity'::real) AS v", "0.5"),
    ("SELECT pg_catalog.\"float4larger\"('-0'::real, 0.5::real) AS v", "0.5"),
    ("SELECT pg_catalog.\"float4larger\"(0.5::real, '-0'::real) AS v", "0.5"),
    ('SELECT pg_catalog."float4smaller"(NULL::real, NULL::real) AS v', None),
    ('SELECT pg_catalog."float4smaller"(0.5::real, 0.5::real) AS v', "0.5"),
    ('SELECT pg_catalog."float4smaller"((-2.25)::real, (-2.25)::real) AS v', "-2.25"),
    ('SELECT pg_catalog."float4smaller"(100::real, 100::real) AS v', "100"),
    ('SELECT pg_catalog."float4smaller"(NULL::real, 0.5::real) AS v', None),
    ('SELECT pg_catalog."float4smaller"(0.5::real, NULL::real) AS v', None),
    ("SELECT pg_catalog.\"float4smaller\"('NaN'::real, 0.5::real) AS v", "0.5"),
    ("SELECT pg_catalog.\"float4smaller\"(0.5::real, 'NaN'::real) AS v", "0.5"),
    ("SELECT pg_catalog.\"float4smaller\"('-Infinity'::real, 0.5::real) AS v", "-Infinity"),
    ("SELECT pg_catalog.\"float4smaller\"(0.5::real, '-Infinity'::real) AS v", "-Infinity"),
    ("SELECT pg_catalog.\"float4smaller\"('-0'::real, 0.5::real) AS v", "-0"),
    ("SELECT pg_catalog.\"float4smaller\"(0.5::real, '-0'::real) AS v", "-0"),
    ('SELECT pg_catalog."float8larger"(NULL::double precision, NULL::double precision) AS v', None),
    ('SELECT pg_catalog."float8larger"(0.5::double precision, 0.5::double precision) AS v', "0.5"),
    (
        'SELECT pg_catalog."float8larger"((-2.25)::double precision, (-2.25)::double precision) AS v',
        "-2.25",
    ),
    ('SELECT pg_catalog."float8larger"(100::double precision, 100::double precision) AS v', "100"),
    ('SELECT pg_catalog."float8larger"(NULL::double precision, 0.5::double precision) AS v', None),
    ('SELECT pg_catalog."float8larger"(0.5::double precision, NULL::double precision) AS v', None),
    (
        "SELECT pg_catalog.\"float8larger\"('NaN'::double precision, 0.5::double precision) AS v",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"float8larger\"(0.5::double precision, 'NaN'::double precision) AS v",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"float8larger\"('-Infinity'::double precision, 0.5::double precision) AS v",
        "0.5",
    ),
    (
        "SELECT pg_catalog.\"float8larger\"(0.5::double precision, '-Infinity'::double precision) AS v",
        "0.5",
    ),
    (
        "SELECT pg_catalog.\"float8larger\"('-0'::double precision, 0.5::double precision) AS v",
        "0.5",
    ),
    (
        "SELECT pg_catalog.\"float8larger\"(0.5::double precision, '-0'::double precision) AS v",
        "0.5",
    ),
    (
        'SELECT pg_catalog."float8smaller"(NULL::double precision, NULL::double precision) AS v',
        None,
    ),
    ('SELECT pg_catalog."float8smaller"(0.5::double precision, 0.5::double precision) AS v', "0.5"),
    (
        'SELECT pg_catalog."float8smaller"((-2.25)::double precision, (-2.25)::double precision) AS v',
        "-2.25",
    ),
    ('SELECT pg_catalog."float8smaller"(100::double precision, 100::double precision) AS v', "100"),
    ('SELECT pg_catalog."float8smaller"(NULL::double precision, 0.5::double precision) AS v', None),
    ('SELECT pg_catalog."float8smaller"(0.5::double precision, NULL::double precision) AS v', None),
    (
        "SELECT pg_catalog.\"float8smaller\"('NaN'::double precision, 0.5::double precision) AS v",
        "0.5",
    ),
    (
        "SELECT pg_catalog.\"float8smaller\"(0.5::double precision, 'NaN'::double precision) AS v",
        "0.5",
    ),
    (
        "SELECT pg_catalog.\"float8smaller\"('-Infinity'::double precision, 0.5::double precision) AS v",
        "-Infinity",
    ),
    (
        "SELECT pg_catalog.\"float8smaller\"(0.5::double precision, '-Infinity'::double precision) AS v",
        "-Infinity",
    ),
    (
        "SELECT pg_catalog.\"float8smaller\"('-0'::double precision, 0.5::double precision) AS v",
        "-0",
    ),
    (
        "SELECT pg_catalog.\"float8smaller\"(0.5::double precision, '-0'::double precision) AS v",
        "-0",
    ),
    ('SELECT pg_catalog."float4abs"(NULL::real) AS v', None),
    ('SELECT pg_catalog."float4abs"(0.5::real) AS v', "0.5"),
    ('SELECT pg_catalog."float4abs"((-2.25)::real) AS v', "2.25"),
    ('SELECT pg_catalog."float4abs"(100::real) AS v', "100"),
    ("SELECT pg_catalog.\"float4abs\"('NaN'::real) AS v", "NaN"),
    ("SELECT pg_catalog.\"float4abs\"('-Infinity'::real) AS v", "Infinity"),
    ("SELECT pg_catalog.\"float4abs\"('-0'::real) AS v", "0"),
    ("SELECT pg_catalog.\"float4abs\"('3.4028235e+38'::real) AS v", "3.4028235e+38"),
    ('SELECT pg_catalog."float4um"(NULL::real) AS v', None),
    ('SELECT pg_catalog."float4um"(0.5::real) AS v', "-0.5"),
    ('SELECT pg_catalog."float4um"((-2.25)::real) AS v', "2.25"),
    ('SELECT pg_catalog."float4um"(100::real) AS v', "-100"),
    ("SELECT pg_catalog.\"float4um\"('NaN'::real) AS v", "NaN"),
    ("SELECT pg_catalog.\"float4um\"('-Infinity'::real) AS v", "Infinity"),
    ("SELECT pg_catalog.\"float4um\"('-0'::real) AS v", "0"),
    ("SELECT pg_catalog.\"float4um\"('3.4028235e+38'::real) AS v", "-3.4028235e+38"),
    ('SELECT pg_catalog."float4up"(NULL::real) AS v', None),
    ('SELECT pg_catalog."float4up"(0.5::real) AS v', "0.5"),
    ('SELECT pg_catalog."float4up"((-2.25)::real) AS v', "-2.25"),
    ('SELECT pg_catalog."float4up"(100::real) AS v', "100"),
    ("SELECT pg_catalog.\"float4up\"('NaN'::real) AS v", "NaN"),
    ("SELECT pg_catalog.\"float4up\"('-Infinity'::real) AS v", "-Infinity"),
    ("SELECT pg_catalog.\"float4up\"('-0'::real) AS v", "-0"),
    ("SELECT pg_catalog.\"float4up\"('3.4028235e+38'::real) AS v", "3.4028235e+38"),
    ('SELECT pg_catalog."float8"(NULL::real) AS v', None),
    ('SELECT pg_catalog."float8"(0.5::real) AS v', "0.5"),
    ('SELECT pg_catalog."float8"((-2.25)::real) AS v', "-2.25"),
    ('SELECT pg_catalog."float8"(100::real) AS v', "100"),
    ("SELECT pg_catalog.\"float8\"('NaN'::real) AS v", "NaN"),
    ("SELECT pg_catalog.\"float8\"('-Infinity'::real) AS v", "-Infinity"),
    ("SELECT pg_catalog.\"float8\"('-0'::real) AS v", "-0"),
    ("SELECT pg_catalog.\"float8\"('3.4028235e+38'::real) AS v", "3.4028234663852886e+38"),
    ('SELECT pg_catalog."hashfloat4"(NULL::real) AS v', None),
    ('SELECT pg_catalog."hashfloat4"(0.5::real) AS v', "109745130"),
    ('SELECT pg_catalog."hashfloat4"((-2.25)::real) AS v', "1232036769"),
    ('SELECT pg_catalog."hashfloat4"(100::real) AS v', "2037975035"),
    ("SELECT pg_catalog.\"hashfloat4\"('NaN'::real) AS v", "-1275764840"),
    ("SELECT pg_catalog.\"hashfloat4\"('-Infinity'::real) AS v", "-1377680656"),
    ("SELECT pg_catalog.\"hashfloat4\"('-0'::real) AS v", "0"),
    ("SELECT pg_catalog.\"hashfloat4\"('3.4028235e+38'::real) AS v", "-1201430654"),
    ('SELECT pg_catalog."int2"(NULL::numeric) AS v', None),
    ('SELECT pg_catalog."int2"(2.5::numeric) AS v', "3"),
    ('SELECT pg_catalog."int2"((-7.125)::numeric) AS v', "-7"),
    ('SELECT pg_catalog."int2"(1000::numeric) AS v', "1000"),
    ("SELECT pg_catalog.\"int2\"('0'::numeric) AS v", "0"),
    ("SELECT pg_catalog.\"int2\"('-0.000000000000000000001'::numeric) AS v", "0"),
    ('SELECT pg_catalog."int4"(NULL::numeric) AS v', None),
    ('SELECT pg_catalog."int4"(2.5::numeric) AS v', "3"),
    ('SELECT pg_catalog."int4"((-7.125)::numeric) AS v', "-7"),
    ('SELECT pg_catalog."int4"(1000::numeric) AS v', "1000"),
    ("SELECT pg_catalog.\"int4\"('0'::numeric) AS v", "0"),
    ("SELECT pg_catalog.\"int4\"('-0.000000000000000000001'::numeric) AS v", "0"),
    ('SELECT pg_catalog."int8"(NULL::numeric) AS v', None),
    ('SELECT pg_catalog."int8"(2.5::numeric) AS v', "3"),
    ('SELECT pg_catalog."int8"((-7.125)::numeric) AS v', "-7"),
    ('SELECT pg_catalog."int8"(1000::numeric) AS v', "1000"),
    ("SELECT pg_catalog.\"int8\"('0'::numeric) AS v", "0"),
    ("SELECT pg_catalog.\"int8\"('-0.000000000000000000001'::numeric) AS v", "0"),
    ('SELECT pg_catalog."lcm"(NULL::integer, NULL::integer) AS v', None),
    ('SELECT pg_catalog."lcm"(3::integer, 3::integer) AS v', "3"),
    ('SELECT pg_catalog."lcm"((-7)::integer, (-7)::integer) AS v', "7"),
    ('SELECT pg_catalog."lcm"(42::integer, 42::integer) AS v', "42"),
    ('SELECT pg_catalog."lcm"(NULL::integer, 3::integer) AS v', None),
    ('SELECT pg_catalog."lcm"(3::integer, NULL::integer) AS v', None),
    ("SELECT pg_catalog.\"lcm\"('0'::integer, 3::integer) AS v", "0"),
    ("SELECT pg_catalog.\"lcm\"(3::integer, '0'::integer) AS v", "0"),
    ('SELECT pg_catalog."lcm"(NULL::bigint, NULL::bigint) AS v', None),
    ('SELECT pg_catalog."lcm"(3::bigint, 3::bigint) AS v', "3"),
    ('SELECT pg_catalog."lcm"((-7)::bigint, (-7)::bigint) AS v', "7"),
    ('SELECT pg_catalog."lcm"(5000000000::bigint, 5000000000::bigint) AS v', "5000000000"),
    ('SELECT pg_catalog."lcm"(NULL::bigint, 3::bigint) AS v', None),
    ('SELECT pg_catalog."lcm"(3::bigint, NULL::bigint) AS v', None),
    ("SELECT pg_catalog.\"lcm\"('0'::bigint, 3::bigint) AS v", "0"),
    ("SELECT pg_catalog.\"lcm\"(3::bigint, '0'::bigint) AS v", "0"),
    ('SELECT pg_catalog."factorial"(NULL::bigint) AS v', None),
    ('SELECT pg_catalog."factorial"(3::bigint) AS v', "6"),
    ("SELECT pg_catalog.\"factorial\"('0'::bigint) AS v", "1"),
    ('SELECT pg_catalog."chr"(NULL::integer) AS v', None),
    ('SELECT pg_catalog."chr"(3::integer) AS v', "\x03"),
    ('SELECT pg_catalog."chr"(42::integer) AS v', "*"),
    ('SELECT pg_catalog."avg"(NULL::double precision) AS v', None),
    (
        'SELECT pg_catalog."avg"(c1) AS v FROM (VALUES (0.5::double precision), ((-2.25)::double precision), (100::double precision)) AS t(c1)',
        "32.75",
    ),
    (
        'SELECT pg_catalog."avg"(c1) AS v FROM (VALUES (NULL::double precision), (NULL::double precision)) AS t(c1)',
        None,
    ),
    (
        "SELECT pg_catalog.\"avg\"(c1) AS v FROM (VALUES ('NaN'::double precision), ('NaN'::double precision)) AS t(c1)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"avg\"(c1) AS v FROM (VALUES ('-Infinity'::double precision), ('-Infinity'::double precision)) AS t(c1)",
        "-Infinity",
    ),
    (
        "SELECT pg_catalog.\"avg\"(c1) AS v FROM (VALUES ('-0'::double precision), ('-0'::double precision)) AS t(c1)",
        "0",
    ),
    ('SELECT pg_catalog."avg"(NULL::real) AS v', None),
    (
        'SELECT pg_catalog."avg"(c1) AS v FROM (VALUES (0.5::real), ((-2.25)::real), (100::real)) AS t(c1)',
        "32.75",
    ),
    ('SELECT pg_catalog."avg"(c1) AS v FROM (VALUES (NULL::real), (NULL::real)) AS t(c1)', None),
    (
        "SELECT pg_catalog.\"avg\"(c1) AS v FROM (VALUES ('NaN'::real), ('NaN'::real)) AS t(c1)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"avg\"(c1) AS v FROM (VALUES ('-Infinity'::real), ('-Infinity'::real)) AS t(c1)",
        "-Infinity",
    ),
    ("SELECT pg_catalog.\"avg\"(c1) AS v FROM (VALUES ('-0'::real), ('-0'::real)) AS t(c1)", "0"),
    (
        "SELECT pg_catalog.\"avg\"(c1) AS v FROM (VALUES ('3.4028235e+38'::real), ('3.4028235e+38'::real)) AS t(c1)",
        "3.4028234663852886e+38",
    ),
    ('SELECT pg_catalog."var_pop"(NULL::double precision) AS v', None),
    (
        'SELECT pg_catalog."var_pop"(c1) AS v FROM (VALUES (0.5::double precision), ((-2.25)::double precision), (100::double precision)) AS t(c1)',
        "2262.5416666666665",
    ),
    (
        'SELECT pg_catalog."var_pop"(c1) AS v FROM (VALUES (NULL::double precision), (NULL::double precision)) AS t(c1)',
        None,
    ),
    (
        "SELECT pg_catalog.\"var_pop\"(c1) AS v FROM (VALUES ('NaN'::double precision), ('NaN'::double precision)) AS t(c1)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"var_pop\"(c1) AS v FROM (VALUES ('-Infinity'::double precision), ('-Infinity'::double precision)) AS t(c1)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"var_pop\"(c1) AS v FROM (VALUES ('-0'::double precision), ('-0'::double precision)) AS t(c1)",
        "0",
    ),
    ('SELECT pg_catalog."var_samp"(NULL::double precision) AS v', None),
    (
        'SELECT pg_catalog."var_samp"(c1) AS v FROM (VALUES (0.5::double precision), ((-2.25)::double precision), (100::double precision)) AS t(c1)',
        "3393.8125",
    ),
    (
        'SELECT pg_catalog."var_samp"(c1) AS v FROM (VALUES (NULL::double precision), (NULL::double precision)) AS t(c1)',
        None,
    ),
    (
        "SELECT pg_catalog.\"var_samp\"(c1) AS v FROM (VALUES ('NaN'::double precision), ('NaN'::double precision)) AS t(c1)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"var_samp\"(c1) AS v FROM (VALUES ('-Infinity'::double precision), ('-Infinity'::double precision)) AS t(c1)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"var_samp\"(c1) AS v FROM (VALUES ('-0'::double precision), ('-0'::double precision)) AS t(c1)",
        "0",
    ),
    ('SELECT pg_catalog."variance"(NULL::double precision) AS v', None),
    (
        'SELECT pg_catalog."variance"(c1) AS v FROM (VALUES (0.5::double precision), ((-2.25)::double precision), (100::double precision)) AS t(c1)',
        "3393.8125",
    ),
    (
        'SELECT pg_catalog."variance"(c1) AS v FROM (VALUES (NULL::double precision), (NULL::double precision)) AS t(c1)',
        None,
    ),
    (
        "SELECT pg_catalog.\"variance\"(c1) AS v FROM (VALUES ('NaN'::double precision), ('NaN'::double precision)) AS t(c1)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"variance\"(c1) AS v FROM (VALUES ('-Infinity'::double precision), ('-Infinity'::double precision)) AS t(c1)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"variance\"(c1) AS v FROM (VALUES ('-0'::double precision), ('-0'::double precision)) AS t(c1)",
        "0",
    ),
    ('SELECT pg_catalog."stddev"(NULL::double precision) AS v', None),
    (
        'SELECT pg_catalog."stddev"(c1) AS v FROM (VALUES (0.5::double precision), ((-2.25)::double precision), (100::double precision)) AS t(c1)',
        "58.256437412529785",
    ),
    (
        'SELECT pg_catalog."stddev"(c1) AS v FROM (VALUES (NULL::double precision), (NULL::double precision)) AS t(c1)',
        None,
    ),
    (
        "SELECT pg_catalog.\"stddev\"(c1) AS v FROM (VALUES ('NaN'::double precision), ('NaN'::double precision)) AS t(c1)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"stddev\"(c1) AS v FROM (VALUES ('-Infinity'::double precision), ('-Infinity'::double precision)) AS t(c1)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"stddev\"(c1) AS v FROM (VALUES ('-0'::double precision), ('-0'::double precision)) AS t(c1)",
        "0",
    ),
    ('SELECT pg_catalog."stddev_pop"(NULL::double precision) AS v', None),
    (
        'SELECT pg_catalog."stddev_pop"(c1) AS v FROM (VALUES (0.5::double precision), ((-2.25)::double precision), (100::double precision)) AS t(c1)',
        "47.56618196436063",
    ),
    (
        'SELECT pg_catalog."stddev_pop"(c1) AS v FROM (VALUES (NULL::double precision), (NULL::double precision)) AS t(c1)',
        None,
    ),
    (
        "SELECT pg_catalog.\"stddev_pop\"(c1) AS v FROM (VALUES ('NaN'::double precision), ('NaN'::double precision)) AS t(c1)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"stddev_pop\"(c1) AS v FROM (VALUES ('-Infinity'::double precision), ('-Infinity'::double precision)) AS t(c1)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"stddev_pop\"(c1) AS v FROM (VALUES ('-0'::double precision), ('-0'::double precision)) AS t(c1)",
        "0",
    ),
    ('SELECT pg_catalog."stddev_samp"(NULL::double precision) AS v', None),
    (
        'SELECT pg_catalog."stddev_samp"(c1) AS v FROM (VALUES (0.5::double precision), ((-2.25)::double precision), (100::double precision)) AS t(c1)',
        "58.256437412529785",
    ),
    (
        'SELECT pg_catalog."stddev_samp"(c1) AS v FROM (VALUES (NULL::double precision), (NULL::double precision)) AS t(c1)',
        None,
    ),
    (
        "SELECT pg_catalog.\"stddev_samp\"(c1) AS v FROM (VALUES ('NaN'::double precision), ('NaN'::double precision)) AS t(c1)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"stddev_samp\"(c1) AS v FROM (VALUES ('-Infinity'::double precision), ('-Infinity'::double precision)) AS t(c1)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"stddev_samp\"(c1) AS v FROM (VALUES ('-0'::double precision), ('-0'::double precision)) AS t(c1)",
        "0",
    ),
    ('SELECT pg_catalog."var_pop"(NULL::real) AS v', None),
    (
        'SELECT pg_catalog."var_pop"(c1) AS v FROM (VALUES (0.5::real), ((-2.25)::real), (100::real)) AS t(c1)',
        "2262.5416666666665",
    ),
    (
        'SELECT pg_catalog."var_pop"(c1) AS v FROM (VALUES (NULL::real), (NULL::real)) AS t(c1)',
        None,
    ),
    (
        "SELECT pg_catalog.\"var_pop\"(c1) AS v FROM (VALUES ('NaN'::real), ('NaN'::real)) AS t(c1)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"var_pop\"(c1) AS v FROM (VALUES ('-Infinity'::real), ('-Infinity'::real)) AS t(c1)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"var_pop\"(c1) AS v FROM (VALUES ('-0'::real), ('-0'::real)) AS t(c1)",
        "0",
    ),
    (
        "SELECT pg_catalog.\"var_pop\"(c1) AS v FROM (VALUES ('3.4028235e+38'::real), ('3.4028235e+38'::real)) AS t(c1)",
        "0",
    ),
    ('SELECT pg_catalog."stddev"(NULL::real) AS v', None),
    (
        'SELECT pg_catalog."stddev"(c1) AS v FROM (VALUES (0.5::real), ((-2.25)::real), (100::real)) AS t(c1)',
        "58.256437412529785",
    ),
    ('SELECT pg_catalog."stddev"(c1) AS v FROM (VALUES (NULL::real), (NULL::real)) AS t(c1)', None),
    (
        "SELECT pg_catalog.\"stddev\"(c1) AS v FROM (VALUES ('NaN'::real), ('NaN'::real)) AS t(c1)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"stddev\"(c1) AS v FROM (VALUES ('-Infinity'::real), ('-Infinity'::real)) AS t(c1)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"stddev\"(c1) AS v FROM (VALUES ('-0'::real), ('-0'::real)) AS t(c1)",
        "0",
    ),
    (
        "SELECT pg_catalog.\"stddev\"(c1) AS v FROM (VALUES ('3.4028235e+38'::real), ('3.4028235e+38'::real)) AS t(c1)",
        "0",
    ),
    ('SELECT pg_catalog."regr_sxx"(NULL::double precision, NULL::double precision) AS v', None),
    (
        'SELECT pg_catalog."regr_sxx"(c1, c2) AS v FROM (VALUES (0.5::double precision, 0.5::double precision), ((-2.25)::double precision, (-2.25)::double precision), (100::double precision, 100::double precision)) AS t(c1, c2)',
        "6787.625",
    ),
    (
        'SELECT pg_catalog."regr_sxx"(c1, c2) AS v FROM (VALUES (NULL::double precision, 0.5::double precision), (NULL::double precision, 0.5::double precision)) AS t(c1, c2)',
        None,
    ),
    (
        'SELECT pg_catalog."regr_sxx"(c1, c2) AS v FROM (VALUES (0.5::double precision, NULL::double precision), (0.5::double precision, NULL::double precision)) AS t(c1, c2)',
        None,
    ),
    (
        "SELECT pg_catalog.\"regr_sxx\"(c1, c2) AS v FROM (VALUES ('NaN'::double precision, 0.5::double precision), ('NaN'::double precision, 0.5::double precision)) AS t(c1, c2)",
        "0",
    ),
    (
        "SELECT pg_catalog.\"regr_sxx\"(c1, c2) AS v FROM (VALUES (0.5::double precision, 'NaN'::double precision), (0.5::double precision, 'NaN'::double precision)) AS t(c1, c2)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"regr_sxx\"(c1, c2) AS v FROM (VALUES ('-Infinity'::double precision, 0.5::double precision), ('-Infinity'::double precision, 0.5::double precision)) AS t(c1, c2)",
        "0",
    ),
    (
        "SELECT pg_catalog.\"regr_sxx\"(c1, c2) AS v FROM (VALUES (0.5::double precision, '-Infinity'::double precision), (0.5::double precision, '-Infinity'::double precision)) AS t(c1, c2)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"regr_sxx\"(c1, c2) AS v FROM (VALUES ('-0'::double precision, 0.5::double precision), ('-0'::double precision, 0.5::double precision)) AS t(c1, c2)",
        "0",
    ),
    (
        "SELECT pg_catalog.\"regr_sxx\"(c1, c2) AS v FROM (VALUES (0.5::double precision, '-0'::double precision), (0.5::double precision, '-0'::double precision)) AS t(c1, c2)",
        "0",
    ),
    ('SELECT pg_catalog."regr_syy"(NULL::double precision, NULL::double precision) AS v', None),
    (
        'SELECT pg_catalog."regr_syy"(c1, c2) AS v FROM (VALUES (0.5::double precision, 0.5::double precision), ((-2.25)::double precision, (-2.25)::double precision), (100::double precision, 100::double precision)) AS t(c1, c2)',
        "6787.625",
    ),
    (
        'SELECT pg_catalog."regr_syy"(c1, c2) AS v FROM (VALUES (NULL::double precision, 0.5::double precision), (NULL::double precision, 0.5::double precision)) AS t(c1, c2)',
        None,
    ),
    (
        'SELECT pg_catalog."regr_syy"(c1, c2) AS v FROM (VALUES (0.5::double precision, NULL::double precision), (0.5::double precision, NULL::double precision)) AS t(c1, c2)',
        None,
    ),
    (
        "SELECT pg_catalog.\"regr_syy\"(c1, c2) AS v FROM (VALUES ('NaN'::double precision, 0.5::double precision), ('NaN'::double precision, 0.5::double precision)) AS t(c1, c2)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"regr_syy\"(c1, c2) AS v FROM (VALUES (0.5::double precision, 'NaN'::double precision), (0.5::double precision, 'NaN'::double precision)) AS t(c1, c2)",
        "0",
    ),
    (
        "SELECT pg_catalog.\"regr_syy\"(c1, c2) AS v FROM (VALUES ('-Infinity'::double precision, 0.5::double precision), ('-Infinity'::double precision, 0.5::double precision)) AS t(c1, c2)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"regr_syy\"(c1, c2) AS v FROM (VALUES (0.5::double precision, '-Infinity'::double precision), (0.5::double precision, '-Infinity'::double precision)) AS t(c1, c2)",
        "0",
    ),
    (
        "SELECT pg_catalog.\"regr_syy\"(c1, c2) AS v FROM (VALUES ('-0'::double precision, 0.5::double precision), ('-0'::double precision, 0.5::double precision)) AS t(c1, c2)",
        "0",
    ),
    (
        "SELECT pg_catalog.\"regr_syy\"(c1, c2) AS v FROM (VALUES (0.5::double precision, '-0'::double precision), (0.5::double precision, '-0'::double precision)) AS t(c1, c2)",
        "0",
    ),
    ('SELECT pg_catalog."regr_slope"(NULL::double precision, NULL::double precision) AS v', None),
    (
        'SELECT pg_catalog."regr_slope"(c1, c2) AS v FROM (VALUES (0.5::double precision, 0.5::double precision), ((-2.25)::double precision, (-2.25)::double precision), (100::double precision, 100::double precision)) AS t(c1, c2)',
        "1",
    ),
    (
        'SELECT pg_catalog."regr_slope"(c1, c2) AS v FROM (VALUES (NULL::double precision, 0.5::double precision), (NULL::double precision, 0.5::double precision)) AS t(c1, c2)',
        None,
    ),
    (
        'SELECT pg_catalog."regr_slope"(c1, c2) AS v FROM (VALUES (0.5::double precision, NULL::double precision), (0.5::double precision, NULL::double precision)) AS t(c1, c2)',
        None,
    ),
    (
        "SELECT pg_catalog.\"regr_slope\"(c1, c2) AS v FROM (VALUES ('NaN'::double precision, 0.5::double precision), ('NaN'::double precision, 0.5::double precision)) AS t(c1, c2)",
        None,
    ),
    (
        "SELECT pg_catalog.\"regr_slope\"(c1, c2) AS v FROM (VALUES (0.5::double precision, 'NaN'::double precision), (0.5::double precision, 'NaN'::double precision)) AS t(c1, c2)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"regr_slope\"(c1, c2) AS v FROM (VALUES ('-Infinity'::double precision, 0.5::double precision), ('-Infinity'::double precision, 0.5::double precision)) AS t(c1, c2)",
        None,
    ),
    (
        "SELECT pg_catalog.\"regr_slope\"(c1, c2) AS v FROM (VALUES (0.5::double precision, '-Infinity'::double precision), (0.5::double precision, '-Infinity'::double precision)) AS t(c1, c2)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"regr_slope\"(c1, c2) AS v FROM (VALUES ('-0'::double precision, 0.5::double precision), ('-0'::double precision, 0.5::double precision)) AS t(c1, c2)",
        None,
    ),
    (
        "SELECT pg_catalog.\"regr_slope\"(c1, c2) AS v FROM (VALUES (0.5::double precision, '-0'::double precision), (0.5::double precision, '-0'::double precision)) AS t(c1, c2)",
        None,
    ),
    (
        'SELECT pg_catalog."regr_intercept"(NULL::double precision, NULL::double precision) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."regr_intercept"(c1, c2) AS v FROM (VALUES (0.5::double precision, 0.5::double precision), ((-2.25)::double precision, (-2.25)::double precision), (100::double precision, 100::double precision)) AS t(c1, c2)',
        "0",
    ),
    (
        'SELECT pg_catalog."regr_intercept"(c1, c2) AS v FROM (VALUES (NULL::double precision, 0.5::double precision), (NULL::double precision, 0.5::double precision)) AS t(c1, c2)',
        None,
    ),
    (
        'SELECT pg_catalog."regr_intercept"(c1, c2) AS v FROM (VALUES (0.5::double precision, NULL::double precision), (0.5::double precision, NULL::double precision)) AS t(c1, c2)',
        None,
    ),
    (
        "SELECT pg_catalog.\"regr_intercept\"(c1, c2) AS v FROM (VALUES ('NaN'::double precision, 0.5::double precision), ('NaN'::double precision, 0.5::double precision)) AS t(c1, c2)",
        None,
    ),
    (
        "SELECT pg_catalog.\"regr_intercept\"(c1, c2) AS v FROM (VALUES (0.5::double precision, 'NaN'::double precision), (0.5::double precision, 'NaN'::double precision)) AS t(c1, c2)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"regr_intercept\"(c1, c2) AS v FROM (VALUES ('-Infinity'::double precision, 0.5::double precision), ('-Infinity'::double precision, 0.5::double precision)) AS t(c1, c2)",
        None,
    ),
    (
        "SELECT pg_catalog.\"regr_intercept\"(c1, c2) AS v FROM (VALUES (0.5::double precision, '-Infinity'::double precision), (0.5::double precision, '-Infinity'::double precision)) AS t(c1, c2)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"regr_intercept\"(c1, c2) AS v FROM (VALUES ('-0'::double precision, 0.5::double precision), ('-0'::double precision, 0.5::double precision)) AS t(c1, c2)",
        None,
    ),
    (
        "SELECT pg_catalog.\"regr_intercept\"(c1, c2) AS v FROM (VALUES (0.5::double precision, '-0'::double precision), (0.5::double precision, '-0'::double precision)) AS t(c1, c2)",
        None,
    ),
    ('SELECT pg_catalog."regr_r2"(NULL::double precision, NULL::double precision) AS v', None),
    (
        'SELECT pg_catalog."regr_r2"(c1, c2) AS v FROM (VALUES (0.5::double precision, 0.5::double precision), ((-2.25)::double precision, (-2.25)::double precision), (100::double precision, 100::double precision)) AS t(c1, c2)',
        "1",
    ),
    (
        'SELECT pg_catalog."regr_r2"(c1, c2) AS v FROM (VALUES (NULL::double precision, 0.5::double precision), (NULL::double precision, 0.5::double precision)) AS t(c1, c2)',
        None,
    ),
    (
        'SELECT pg_catalog."regr_r2"(c1, c2) AS v FROM (VALUES (0.5::double precision, NULL::double precision), (0.5::double precision, NULL::double precision)) AS t(c1, c2)',
        None,
    ),
    (
        "SELECT pg_catalog.\"regr_r2\"(c1, c2) AS v FROM (VALUES ('NaN'::double precision, 0.5::double precision), ('NaN'::double precision, 0.5::double precision)) AS t(c1, c2)",
        None,
    ),
    (
        "SELECT pg_catalog.\"regr_r2\"(c1, c2) AS v FROM (VALUES (0.5::double precision, 'NaN'::double precision), (0.5::double precision, 'NaN'::double precision)) AS t(c1, c2)",
        "1",
    ),
    (
        "SELECT pg_catalog.\"regr_r2\"(c1, c2) AS v FROM (VALUES ('-Infinity'::double precision, 0.5::double precision), ('-Infinity'::double precision, 0.5::double precision)) AS t(c1, c2)",
        None,
    ),
    (
        "SELECT pg_catalog.\"regr_r2\"(c1, c2) AS v FROM (VALUES (0.5::double precision, '-Infinity'::double precision), (0.5::double precision, '-Infinity'::double precision)) AS t(c1, c2)",
        "1",
    ),
    (
        "SELECT pg_catalog.\"regr_r2\"(c1, c2) AS v FROM (VALUES ('-0'::double precision, 0.5::double precision), ('-0'::double precision, 0.5::double precision)) AS t(c1, c2)",
        None,
    ),
    (
        "SELECT pg_catalog.\"regr_r2\"(c1, c2) AS v FROM (VALUES (0.5::double precision, '-0'::double precision), (0.5::double precision, '-0'::double precision)) AS t(c1, c2)",
        None,
    ),
    ('SELECT pg_catalog."corr"(NULL::double precision, NULL::double precision) AS v', None),
    (
        'SELECT pg_catalog."corr"(c1, c2) AS v FROM (VALUES (0.5::double precision, 0.5::double precision), ((-2.25)::double precision, (-2.25)::double precision), (100::double precision, 100::double precision)) AS t(c1, c2)',
        "1",
    ),
    (
        'SELECT pg_catalog."corr"(c1, c2) AS v FROM (VALUES (NULL::double precision, 0.5::double precision), (NULL::double precision, 0.5::double precision)) AS t(c1, c2)',
        None,
    ),
    (
        'SELECT pg_catalog."corr"(c1, c2) AS v FROM (VALUES (0.5::double precision, NULL::double precision), (0.5::double precision, NULL::double precision)) AS t(c1, c2)',
        None,
    ),
    (
        "SELECT pg_catalog.\"corr\"(c1, c2) AS v FROM (VALUES ('NaN'::double precision, 0.5::double precision), ('NaN'::double precision, 0.5::double precision)) AS t(c1, c2)",
        None,
    ),
    (
        "SELECT pg_catalog.\"corr\"(c1, c2) AS v FROM (VALUES (0.5::double precision, 'NaN'::double precision), (0.5::double precision, 'NaN'::double precision)) AS t(c1, c2)",
        None,
    ),
    (
        "SELECT pg_catalog.\"corr\"(c1, c2) AS v FROM (VALUES ('-Infinity'::double precision, 0.5::double precision), ('-Infinity'::double precision, 0.5::double precision)) AS t(c1, c2)",
        None,
    ),
    (
        "SELECT pg_catalog.\"corr\"(c1, c2) AS v FROM (VALUES (0.5::double precision, '-Infinity'::double precision), (0.5::double precision, '-Infinity'::double precision)) AS t(c1, c2)",
        None,
    ),
    (
        "SELECT pg_catalog.\"corr\"(c1, c2) AS v FROM (VALUES ('-0'::double precision, 0.5::double precision), ('-0'::double precision, 0.5::double precision)) AS t(c1, c2)",
        None,
    ),
    (
        "SELECT pg_catalog.\"corr\"(c1, c2) AS v FROM (VALUES (0.5::double precision, '-0'::double precision), (0.5::double precision, '-0'::double precision)) AS t(c1, c2)",
        None,
    ),
    ('SELECT pg_catalog."covar_pop"(NULL::double precision, NULL::double precision) AS v', None),
    (
        'SELECT pg_catalog."covar_pop"(c1, c2) AS v FROM (VALUES (0.5::double precision, 0.5::double precision), ((-2.25)::double precision, (-2.25)::double precision), (100::double precision, 100::double precision)) AS t(c1, c2)',
        "2262.5416666666665",
    ),
    (
        'SELECT pg_catalog."covar_pop"(c1, c2) AS v FROM (VALUES (NULL::double precision, 0.5::double precision), (NULL::double precision, 0.5::double precision)) AS t(c1, c2)',
        None,
    ),
    (
        'SELECT pg_catalog."covar_pop"(c1, c2) AS v FROM (VALUES (0.5::double precision, NULL::double precision), (0.5::double precision, NULL::double precision)) AS t(c1, c2)',
        None,
    ),
    (
        "SELECT pg_catalog.\"covar_pop\"(c1, c2) AS v FROM (VALUES ('NaN'::double precision, 0.5::double precision), ('NaN'::double precision, 0.5::double precision)) AS t(c1, c2)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"covar_pop\"(c1, c2) AS v FROM (VALUES (0.5::double precision, 'NaN'::double precision), (0.5::double precision, 'NaN'::double precision)) AS t(c1, c2)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"covar_pop\"(c1, c2) AS v FROM (VALUES ('-Infinity'::double precision, 0.5::double precision), ('-Infinity'::double precision, 0.5::double precision)) AS t(c1, c2)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"covar_pop\"(c1, c2) AS v FROM (VALUES (0.5::double precision, '-Infinity'::double precision), (0.5::double precision, '-Infinity'::double precision)) AS t(c1, c2)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"covar_pop\"(c1, c2) AS v FROM (VALUES ('-0'::double precision, 0.5::double precision), ('-0'::double precision, 0.5::double precision)) AS t(c1, c2)",
        "0",
    ),
    (
        "SELECT pg_catalog.\"covar_pop\"(c1, c2) AS v FROM (VALUES (0.5::double precision, '-0'::double precision), (0.5::double precision, '-0'::double precision)) AS t(c1, c2)",
        "0",
    ),
    ('SELECT pg_catalog."regr_avgx"(NULL::double precision, NULL::double precision) AS v', None),
    (
        'SELECT pg_catalog."regr_avgx"(c1, c2) AS v FROM (VALUES (0.5::double precision, 0.5::double precision), ((-2.25)::double precision, (-2.25)::double precision), (100::double precision, 100::double precision)) AS t(c1, c2)',
        "32.75",
    ),
    (
        'SELECT pg_catalog."regr_avgx"(c1, c2) AS v FROM (VALUES (NULL::double precision, 0.5::double precision), (NULL::double precision, 0.5::double precision)) AS t(c1, c2)',
        None,
    ),
    (
        'SELECT pg_catalog."regr_avgx"(c1, c2) AS v FROM (VALUES (0.5::double precision, NULL::double precision), (0.5::double precision, NULL::double precision)) AS t(c1, c2)',
        None,
    ),
    (
        "SELECT pg_catalog.\"regr_avgx\"(c1, c2) AS v FROM (VALUES ('NaN'::double precision, 0.5::double precision), ('NaN'::double precision, 0.5::double precision)) AS t(c1, c2)",
        "0.5",
    ),
    (
        "SELECT pg_catalog.\"regr_avgx\"(c1, c2) AS v FROM (VALUES (0.5::double precision, 'NaN'::double precision), (0.5::double precision, 'NaN'::double precision)) AS t(c1, c2)",
        "NaN",
    ),
    (
        "SELECT pg_catalog.\"regr_avgx\"(c1, c2) AS v FROM (VALUES ('-Infinity'::double precision, 0.5::double precision), ('-Infinity'::double precision, 0.5::double precision)) AS t(c1, c2)",
        "0.5",
    ),
    (
        "SELECT pg_catalog.\"regr_avgx\"(c1, c2) AS v FROM (VALUES (0.5::double precision, '-Infinity'::double precision), (0.5::double precision, '-Infinity'::double precision)) AS t(c1, c2)",
        "-Infinity",
    ),
    (
        "SELECT pg_catalog.\"regr_avgx\"(c1, c2) AS v FROM (VALUES ('-0'::double precision, 0.5::double precision), ('-0'::double precision, 0.5::double precision)) AS t(c1, c2)",
        "0.5",
    ),
    (
        "SELECT pg_catalog.\"regr_avgx\"(c1, c2) AS v FROM (VALUES (0.5::double precision, '-0'::double precision), (0.5::double precision, '-0'::double precision)) AS t(c1, c2)",
        "0",
    ),
    ('SELECT pg_catalog."var_pop"(NULL::integer) AS v', None),
    (
        'SELECT pg_catalog."var_pop"(c1) AS v FROM (VALUES (3::integer), ((-7)::integer), (42::integer)) AS t(c1)',
        "446.8888888888888889",
    ),
    (
        'SELECT pg_catalog."var_pop"(c1) AS v FROM (VALUES (NULL::integer), (NULL::integer)) AS t(c1)',
        None,
    ),
    (
        "SELECT pg_catalog.\"var_pop\"(c1) AS v FROM (VALUES ('-2147483648'::integer), ('-2147483648'::integer)) AS t(c1)",
        "0",
    ),
    (
        "SELECT pg_catalog.\"var_pop\"(c1) AS v FROM (VALUES ('2147483647'::integer), ('2147483647'::integer)) AS t(c1)",
        "0",
    ),
    (
        "SELECT pg_catalog.\"var_pop\"(c1) AS v FROM (VALUES ('0'::integer), ('0'::integer)) AS t(c1)",
        "0",
    ),
    ('SELECT pg_catalog."avg"(NULL::bigint) AS v', None),
    (
        'SELECT pg_catalog."avg"(c1) AS v FROM (VALUES (3::bigint), ((-7)::bigint), (5000000000::bigint)) AS t(c1)',
        "1666666665.33333333",
    ),
    (
        'SELECT pg_catalog."avg"(c1) AS v FROM (VALUES (NULL::bigint), (NULL::bigint)) AS t(c1)',
        None,
    ),
    (
        "SELECT pg_catalog.\"avg\"(c1) AS v FROM (VALUES ('-9223372036854775808'::bigint), ('-9223372036854775808'::bigint)) AS t(c1)",
        "-9223372036854775808",
    ),
    (
        "SELECT pg_catalog.\"avg\"(c1) AS v FROM (VALUES ('9223372036854775807'::bigint), ('9223372036854775807'::bigint)) AS t(c1)",
        "9223372036854775807",
    ),
    (
        "SELECT pg_catalog.\"avg\"(c1) AS v FROM (VALUES ('0'::bigint), ('0'::bigint)) AS t(c1)",
        "0.00000000000000000000",
    ),
    (
        'SELECT pg_catalog."in_range"(NULL::bigint, NULL::bigint, NULL::bigint, NULL::boolean, NULL::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::bigint, 3::bigint, 3::bigint, true::boolean, true::boolean) AS v',
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(5000000000::bigint, 5000000000::bigint, 5000000000::bigint, true::boolean, true::boolean) AS v',
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(NULL::bigint, 3::bigint, 3::bigint, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::bigint, NULL::bigint, 3::bigint, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::bigint, 3::bigint, NULL::bigint, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::bigint, 3::bigint, 3::bigint, NULL::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::bigint, 3::bigint, 3::bigint, true::boolean, NULL::boolean) AS v',
        None,
    ),
    (
        "SELECT pg_catalog.\"in_range\"('-9223372036854775808'::bigint, 3::bigint, 3::bigint, true::boolean, true::boolean) AS v",
        "t",
    ),
    (
        "SELECT pg_catalog.\"in_range\"(3::bigint, '-9223372036854775808'::bigint, 3::bigint, true::boolean, true::boolean) AS v",
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(NULL::integer, NULL::integer, NULL::bigint, NULL::boolean, NULL::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::integer, 3::integer, 3::bigint, true::boolean, true::boolean) AS v',
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(42::integer, 42::integer, 5000000000::bigint, true::boolean, true::boolean) AS v',
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(NULL::integer, 3::integer, 3::bigint, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::integer, NULL::integer, 3::bigint, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::integer, 3::integer, NULL::bigint, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::integer, 3::integer, 3::bigint, NULL::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::integer, 3::integer, 3::bigint, true::boolean, NULL::boolean) AS v',
        None,
    ),
    (
        "SELECT pg_catalog.\"in_range\"('-2147483648'::integer, 3::integer, 3::bigint, true::boolean, true::boolean) AS v",
        "t",
    ),
    (
        "SELECT pg_catalog.\"in_range\"(3::integer, '-2147483648'::integer, 3::bigint, true::boolean, true::boolean) AS v",
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(NULL::integer, NULL::integer, NULL::integer, NULL::boolean, NULL::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::integer, 3::integer, 3::integer, true::boolean, true::boolean) AS v',
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(42::integer, 42::integer, 42::integer, true::boolean, true::boolean) AS v',
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(NULL::integer, 3::integer, 3::integer, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::integer, NULL::integer, 3::integer, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::integer, 3::integer, NULL::integer, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::integer, 3::integer, 3::integer, NULL::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::integer, 3::integer, 3::integer, true::boolean, NULL::boolean) AS v',
        None,
    ),
    (
        "SELECT pg_catalog.\"in_range\"('-2147483648'::integer, 3::integer, 3::integer, true::boolean, true::boolean) AS v",
        "t",
    ),
    (
        "SELECT pg_catalog.\"in_range\"(3::integer, '-2147483648'::integer, 3::integer, true::boolean, true::boolean) AS v",
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(NULL::integer, NULL::integer, NULL::smallint, NULL::boolean, NULL::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::integer, 3::integer, 3::smallint, true::boolean, true::boolean) AS v',
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(42::integer, 42::integer, 42::smallint, true::boolean, true::boolean) AS v',
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(NULL::integer, 3::integer, 3::smallint, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::integer, NULL::integer, 3::smallint, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::integer, 3::integer, NULL::smallint, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::integer, 3::integer, 3::smallint, NULL::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::integer, 3::integer, 3::smallint, true::boolean, NULL::boolean) AS v',
        None,
    ),
    (
        "SELECT pg_catalog.\"in_range\"('-2147483648'::integer, 3::integer, 3::smallint, true::boolean, true::boolean) AS v",
        "t",
    ),
    (
        "SELECT pg_catalog.\"in_range\"(3::integer, '-2147483648'::integer, 3::smallint, true::boolean, true::boolean) AS v",
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(NULL::smallint, NULL::smallint, NULL::bigint, NULL::boolean, NULL::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::smallint, 3::smallint, 3::bigint, true::boolean, true::boolean) AS v',
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(42::smallint, 42::smallint, 5000000000::bigint, true::boolean, true::boolean) AS v',
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(NULL::smallint, 3::smallint, 3::bigint, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::smallint, NULL::smallint, 3::bigint, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::smallint, 3::smallint, NULL::bigint, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::smallint, 3::smallint, 3::bigint, NULL::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::smallint, 3::smallint, 3::bigint, true::boolean, NULL::boolean) AS v',
        None,
    ),
    (
        "SELECT pg_catalog.\"in_range\"('-32768'::smallint, 3::smallint, 3::bigint, true::boolean, true::boolean) AS v",
        "t",
    ),
    (
        "SELECT pg_catalog.\"in_range\"(3::smallint, '-32768'::smallint, 3::bigint, true::boolean, true::boolean) AS v",
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(NULL::smallint, NULL::smallint, NULL::integer, NULL::boolean, NULL::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::smallint, 3::smallint, 3::integer, true::boolean, true::boolean) AS v',
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(42::smallint, 42::smallint, 42::integer, true::boolean, true::boolean) AS v',
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(NULL::smallint, 3::smallint, 3::integer, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::smallint, NULL::smallint, 3::integer, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::smallint, 3::smallint, NULL::integer, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::smallint, 3::smallint, 3::integer, NULL::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::smallint, 3::smallint, 3::integer, true::boolean, NULL::boolean) AS v',
        None,
    ),
    (
        "SELECT pg_catalog.\"in_range\"('-32768'::smallint, 3::smallint, 3::integer, true::boolean, true::boolean) AS v",
        "t",
    ),
    (
        "SELECT pg_catalog.\"in_range\"(3::smallint, '-32768'::smallint, 3::integer, true::boolean, true::boolean) AS v",
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(NULL::smallint, NULL::smallint, NULL::smallint, NULL::boolean, NULL::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::smallint, 3::smallint, 3::smallint, true::boolean, true::boolean) AS v',
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(42::smallint, 42::smallint, 42::smallint, true::boolean, true::boolean) AS v',
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(NULL::smallint, 3::smallint, 3::smallint, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::smallint, NULL::smallint, 3::smallint, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::smallint, 3::smallint, NULL::smallint, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::smallint, 3::smallint, 3::smallint, NULL::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(3::smallint, 3::smallint, 3::smallint, true::boolean, NULL::boolean) AS v',
        None,
    ),
    (
        "SELECT pg_catalog.\"in_range\"('-32768'::smallint, 3::smallint, 3::smallint, true::boolean, true::boolean) AS v",
        "t",
    ),
    (
        "SELECT pg_catalog.\"in_range\"(3::smallint, '-32768'::smallint, 3::smallint, true::boolean, true::boolean) AS v",
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(NULL::double precision, NULL::double precision, NULL::double precision, NULL::boolean, NULL::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(0.5::double precision, 0.5::double precision, 0.5::double precision, true::boolean, true::boolean) AS v',
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(100::double precision, 100::double precision, 100::double precision, true::boolean, true::boolean) AS v',
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(NULL::double precision, 0.5::double precision, 0.5::double precision, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(0.5::double precision, NULL::double precision, 0.5::double precision, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(0.5::double precision, 0.5::double precision, NULL::double precision, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(0.5::double precision, 0.5::double precision, 0.5::double precision, NULL::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(0.5::double precision, 0.5::double precision, 0.5::double precision, true::boolean, NULL::boolean) AS v',
        None,
    ),
    (
        "SELECT pg_catalog.\"in_range\"('NaN'::double precision, 0.5::double precision, 0.5::double precision, true::boolean, true::boolean) AS v",
        "f",
    ),
    (
        "SELECT pg_catalog.\"in_range\"(0.5::double precision, 'NaN'::double precision, 0.5::double precision, true::boolean, true::boolean) AS v",
        "t",
    ),
    (
        'SELECT pg_catalog."in_range"(NULL::real, NULL::real, NULL::double precision, NULL::boolean, NULL::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(0.5::real, 0.5::real, 0.5::double precision, true::boolean, true::boolean) AS v',
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(100::real, 100::real, 100::double precision, true::boolean, true::boolean) AS v',
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(NULL::real, 0.5::real, 0.5::double precision, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(0.5::real, NULL::real, 0.5::double precision, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(0.5::real, 0.5::real, NULL::double precision, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(0.5::real, 0.5::real, 0.5::double precision, NULL::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(0.5::real, 0.5::real, 0.5::double precision, true::boolean, NULL::boolean) AS v',
        None,
    ),
    (
        "SELECT pg_catalog.\"in_range\"('NaN'::real, 0.5::real, 0.5::double precision, true::boolean, true::boolean) AS v",
        "f",
    ),
    (
        "SELECT pg_catalog.\"in_range\"(0.5::real, 'NaN'::real, 0.5::double precision, true::boolean, true::boolean) AS v",
        "t",
    ),
    (
        'SELECT pg_catalog."in_range"(NULL::numeric, NULL::numeric, NULL::numeric, NULL::boolean, NULL::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(2.5::numeric, 2.5::numeric, 2.5::numeric, true::boolean, true::boolean) AS v',
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(1000::numeric, 1000::numeric, 1000::numeric, true::boolean, true::boolean) AS v',
        "f",
    ),
    (
        'SELECT pg_catalog."in_range"(NULL::numeric, 2.5::numeric, 2.5::numeric, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(2.5::numeric, NULL::numeric, 2.5::numeric, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(2.5::numeric, 2.5::numeric, NULL::numeric, true::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(2.5::numeric, 2.5::numeric, 2.5::numeric, NULL::boolean, true::boolean) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."in_range"(2.5::numeric, 2.5::numeric, 2.5::numeric, true::boolean, NULL::boolean) AS v',
        None,
    ),
    (
        "SELECT pg_catalog.\"in_range\"('0'::numeric, 2.5::numeric, 2.5::numeric, true::boolean, true::boolean) AS v",
        "t",
    ),
    (
        "SELECT pg_catalog.\"in_range\"(2.5::numeric, '0'::numeric, 2.5::numeric, true::boolean, true::boolean) AS v",
        "f",
    ),
    (
        "SELECT pg_catalog.\"in_range\"(2.5::numeric, 2.5::numeric, '0'::numeric, true::boolean, true::boolean) AS v",
        "t",
    ),
    ('SELECT pg_catalog."to_hex"(NULL::integer) AS v', None),
    ('SELECT pg_catalog."to_hex"(3::integer) AS v', "3"),
    ('SELECT pg_catalog."to_hex"((-7)::integer) AS v', "fffffff9"),
    ('SELECT pg_catalog."to_hex"(42::integer) AS v', "2a"),
    ("SELECT pg_catalog.\"to_hex\"('-2147483648'::integer) AS v", "80000000"),
    ("SELECT pg_catalog.\"to_hex\"('2147483647'::integer) AS v", "7fffffff"),
    ("SELECT pg_catalog.\"to_hex\"('0'::integer) AS v", "0"),
    ('SELECT pg_catalog."to_hex"(NULL::bigint) AS v', None),
    ('SELECT pg_catalog."to_hex"(3::bigint) AS v', "3"),
    ('SELECT pg_catalog."to_hex"((-7)::bigint) AS v', "fffffffffffffff9"),
    ('SELECT pg_catalog."to_hex"(5000000000::bigint) AS v', "12a05f200"),
    ("SELECT pg_catalog.\"to_hex\"('-9223372036854775808'::bigint) AS v", "8000000000000000"),
    ("SELECT pg_catalog.\"to_hex\"('9223372036854775807'::bigint) AS v", "7fffffffffffffff"),
    ("SELECT pg_catalog.\"to_hex\"('0'::bigint) AS v", "0"),
    ("SELECT '1.4e-45'::real", "1e-45"),
    ("SELECT 0.1::real", "0.1"),
    ("SELECT '-7.125'::real", "-7.125"),
    ("SELECT pg_catalog.float8('16777217'::real)", "16777216"),
    ("SELECT pg_catalog.float8('0.30000001'::real)", "0.30000001192092896"),
    ("SELECT pg_catalog.power((-8)::double precision, 'NaN'::double precision)", "NaN"),
    ("SELECT pg_catalog.power('NaN'::double precision, 0::double precision)", "1"),
    ("SELECT pg_catalog.atanh(1::double precision)", "Infinity"),
    ("SELECT pg_catalog.acosh('Infinity'::double precision)", "Infinity"),
    ("SELECT pg_catalog.float8eq('NaN'::double precision, 'NaN'::double precision)", "t"),
    ("SELECT pg_catalog.float8lt('NaN'::double precision, 'NaN'::double precision)", "f"),
    ("SELECT pg_catalog.float8gt('Infinity'::double precision, 'NaN'::double precision)", "f"),
    ("SELECT pg_catalog.float8larger(NULL::double precision, 'NaN'::double precision)", None),
    ("SELECT pg_catalog.float8smaller('NaN'::double precision, NULL::double precision)", None),
    ("SELECT pg_catalog.abs((-5)::numeric)", "5"),
    ("SELECT pg_catalog.sign((-0.5)::numeric)", "-1"),
    ("SELECT pg_catalog.chr(26085)", "日"),
    ("SELECT pg_catalog.chr(128512)", "😀"),
    ("SELECT pg_catalog.chr(233)", "é"),
    (
        "SELECT pg_catalog.factorial(56::bigint)",
        "710998587804863451854045647463724949736497978881168458687447040000000000000",
    ),
    ("SELECT pg_catalog.factorial(0::bigint)", "1"),
    ("SELECT pg_catalog.lcm((-4)::integer, 6::integer)", "12"),
    ("SELECT pg_catalog.lcm(0::bigint, 0::bigint)", "0"),
    ("SELECT pg_catalog.var_samp(c1) AS v FROM (VALUES (1::double precision)) AS t(c1)", None),
    (
        "SELECT pg_catalog.stddev(c1) AS v FROM (VALUES (1::double precision), (2::double precision), (4::double precision)) AS t(c1)",
        "1.5275252316519468",
    ),
    (
        "SELECT pg_catalog.corr(c1, c2) AS v FROM (VALUES (1::double precision, 2::double precision), (2::double precision, 4.5::double precision), (3::double precision, 5::double precision)) AS t(c1, c2)",
        "0.9332565252573827",
    ),
    (
        "SELECT pg_catalog.regr_r2(c1, c2) AS v FROM (VALUES (1::double precision, NULL::double precision), (2::double precision, 4.5::double precision), (3::double precision, 5::double precision)) AS t(c1, c2)",
        "1",
    ),
    (
        "SELECT pg_catalog.in_range('Infinity'::double precision, '-Infinity'::double precision, 'Infinity'::double precision, false, true)",
        "t",
    ),
    (
        "SELECT pg_catalog.in_range('NaN'::double precision, 'NaN'::double precision, 1::double precision, true, false)",
        "t",
    ),
    ("SELECT pg_catalog.in_range(5::integer, 2147483647::integer, 3::integer, false, false)", "f"),
]
RAISED = [
    ("SELECT pg_catalog.\"abs\"('-9223372036854775808'::bigint) AS v", "22003"),
    ("SELECT pg_catalog.\"abs\"('9223372036854775808'::bigint) AS v", "22003"),
    ("SELECT pg_catalog.\"abs\"('-2147483648'::integer) AS v", "22003"),
    ("SELECT pg_catalog.\"abs\"('2147483648'::integer) AS v", "22003"),
    ("SELECT pg_catalog.\"abs\"('-32768'::smallint) AS v", "22003"),
    ("SELECT pg_catalog.\"abs\"('32768'::smallint) AS v", "22003"),
    ('SELECT pg_catalog."acos"((-2.25)::double precision) AS v', "22003"),
    ('SELECT pg_catalog."acos"(100::double precision) AS v', "22003"),
    ("SELECT pg_catalog.\"acos\"('-Infinity'::double precision) AS v", "22003"),
    ("SELECT pg_catalog.\"acos\"('1.7976931348623157e+308'::double precision) AS v", "22003"),
    ('SELECT pg_catalog."acosh"(0.5::double precision) AS v', "22003"),
    ('SELECT pg_catalog."acosh"((-2.25)::double precision) AS v', "22003"),
    ("SELECT pg_catalog.\"acosh\"('-Infinity'::double precision) AS v", "22003"),
    ("SELECT pg_catalog.\"acosh\"('-0'::double precision) AS v", "22003"),
    ('SELECT pg_catalog."asin"((-2.25)::double precision) AS v', "22003"),
    ('SELECT pg_catalog."asin"(100::double precision) AS v', "22003"),
    ("SELECT pg_catalog.\"asin\"('-Infinity'::double precision) AS v", "22003"),
    ("SELECT pg_catalog.\"asin\"('1.7976931348623157e+308'::double precision) AS v", "22003"),
    ('SELECT pg_catalog."atanh"((-2.25)::double precision) AS v', "22003"),
    ('SELECT pg_catalog."atanh"(100::double precision) AS v', "22003"),
    ("SELECT pg_catalog.\"atanh\"('-Infinity'::double precision) AS v", "22003"),
    ("SELECT pg_catalog.\"atanh\"('1.7976931348623157e+308'::double precision) AS v", "22003"),
    ("SELECT pg_catalog.\"sin\"('-Infinity'::double precision) AS v", "22003"),
    ("SELECT pg_catalog.\"cos\"('-Infinity'::double precision) AS v", "22003"),
    ("SELECT pg_catalog.\"tan\"('-Infinity'::double precision) AS v", "22003"),
    ("SELECT pg_catalog.\"cot\"('-Infinity'::double precision) AS v", "22003"),
    ('SELECT pg_catalog."sqrt"((-2.25)::double precision) AS v', "2201F"),
    ("SELECT pg_catalog.\"sqrt\"('-Infinity'::double precision) AS v", "2201F"),
    ('SELECT pg_catalog."log"((-2.25)::double precision) AS v', "2201E"),
    ("SELECT pg_catalog.\"log\"('-Infinity'::double precision) AS v", "2201E"),
    ("SELECT pg_catalog.\"log\"('-0'::double precision) AS v", "2201E"),
    ('SELECT pg_catalog."log10"((-2.25)::double precision) AS v', "2201E"),
    ("SELECT pg_catalog.\"log10\"('-Infinity'::double precision) AS v", "2201E"),
    ("SELECT pg_catalog.\"log10\"('-0'::double precision) AS v", "2201E"),
    ('SELECT pg_catalog."pow"((-2.25)::double precision, (-2.25)::double precision) AS v', "2201F"),
    (
        "SELECT pg_catalog.\"pow\"('-Infinity'::double precision, 0.5::double precision) AS v",
        "2201F",
    ),
    (
        'SELECT pg_catalog."power"((-2.25)::double precision, (-2.25)::double precision) AS v',
        "2201F",
    ),
    (
        "SELECT pg_catalog.\"power\"('-Infinity'::double precision, 0.5::double precision) AS v",
        "2201F",
    ),
    ("SELECT pg_catalog.\"degrees\"('1.7976931348623157e+308'::double precision) AS v", "22003"),
    ("SELECT pg_catalog.\"int2\"('NaN'::numeric) AS v", "0A000"),
    ("SELECT pg_catalog.\"int4\"('NaN'::numeric) AS v", "0A000"),
    ("SELECT pg_catalog.\"int8\"('NaN'::numeric) AS v", "0A000"),
    ("SELECT pg_catalog.\"lcm\"('-2147483648'::integer, 3::integer) AS v", "22003"),
    ("SELECT pg_catalog.\"lcm\"(3::integer, '-2147483648'::integer) AS v", "22003"),
    ("SELECT pg_catalog.\"lcm\"('2147483647'::integer, 3::integer) AS v", "22003"),
    ("SELECT pg_catalog.\"lcm\"(3::integer, '2147483647'::integer) AS v", "22003"),
    ("SELECT pg_catalog.\"lcm\"('-9223372036854775808'::bigint, 3::bigint) AS v", "22003"),
    ("SELECT pg_catalog.\"lcm\"(3::bigint, '-9223372036854775808'::bigint) AS v", "22003"),
    ("SELECT pg_catalog.\"lcm\"('9223372036854775807'::bigint, 3::bigint) AS v", "22003"),
    ("SELECT pg_catalog.\"lcm\"(3::bigint, '9223372036854775807'::bigint) AS v", "22003"),
    ('SELECT pg_catalog."factorial"((-7)::bigint) AS v', "22003"),
    ('SELECT pg_catalog."factorial"(5000000000::bigint) AS v', "22003"),
    ("SELECT pg_catalog.\"factorial\"('-9223372036854775808'::bigint) AS v", "22003"),
    ("SELECT pg_catalog.\"factorial\"('9223372036854775807'::bigint) AS v", "22003"),
    ("SELECT pg_catalog.\"factorial\"('9223372036854775808'::bigint) AS v", "22003"),
    ('SELECT pg_catalog."chr"((-7)::integer) AS v', "22023"),
    ("SELECT pg_catalog.\"chr\"('-2147483648'::integer) AS v", "22023"),
    ("SELECT pg_catalog.\"chr\"('2147483647'::integer) AS v", "54000"),
    ("SELECT pg_catalog.\"chr\"('0'::integer) AS v", "54000"),
    ("SELECT pg_catalog.\"chr\"('2147483648'::integer) AS v", "22003"),
    (
        "SELECT pg_catalog.\"avg\"(c1) AS v FROM (VALUES ('1.7976931348623157e+308'::double precision), ('1.7976931348623157e+308'::double precision)) AS t(c1)",
        "22003",
    ),
    (
        "SELECT pg_catalog.\"var_pop\"(c1) AS v FROM (VALUES ('1.7976931348623157e+308'::double precision), ('1.7976931348623157e+308'::double precision)) AS t(c1)",
        "22003",
    ),
    (
        "SELECT pg_catalog.\"var_samp\"(c1) AS v FROM (VALUES ('1.7976931348623157e+308'::double precision), ('1.7976931348623157e+308'::double precision)) AS t(c1)",
        "22003",
    ),
    (
        "SELECT pg_catalog.\"variance\"(c1) AS v FROM (VALUES ('1.7976931348623157e+308'::double precision), ('1.7976931348623157e+308'::double precision)) AS t(c1)",
        "22003",
    ),
    (
        "SELECT pg_catalog.\"stddev\"(c1) AS v FROM (VALUES ('1.7976931348623157e+308'::double precision), ('1.7976931348623157e+308'::double precision)) AS t(c1)",
        "22003",
    ),
    (
        "SELECT pg_catalog.\"stddev_pop\"(c1) AS v FROM (VALUES ('1.7976931348623157e+308'::double precision), ('1.7976931348623157e+308'::double precision)) AS t(c1)",
        "22003",
    ),
    (
        "SELECT pg_catalog.\"stddev_samp\"(c1) AS v FROM (VALUES ('1.7976931348623157e+308'::double precision), ('1.7976931348623157e+308'::double precision)) AS t(c1)",
        "22003",
    ),
    (
        "SELECT pg_catalog.\"var_pop\"(c1) AS v FROM (VALUES ('2147483648'::integer), ('2147483648'::integer)) AS t(c1)",
        "22003",
    ),
    (
        "SELECT pg_catalog.\"avg\"(c1) AS v FROM (VALUES ('9223372036854775808'::bigint), ('9223372036854775808'::bigint)) AS t(c1)",
        "22003",
    ),
    (
        'SELECT pg_catalog."in_range"((-7)::bigint, (-7)::bigint, (-7)::bigint, false::boolean, false::boolean) AS v',
        "22013",
    ),
    (
        "SELECT pg_catalog.\"in_range\"(3::bigint, 3::bigint, '-9223372036854775808'::bigint, true::boolean, true::boolean) AS v",
        "22013",
    ),
    (
        'SELECT pg_catalog."in_range"((-7)::integer, (-7)::integer, (-7)::bigint, false::boolean, false::boolean) AS v',
        "22013",
    ),
    (
        "SELECT pg_catalog.\"in_range\"(3::integer, 3::integer, '-9223372036854775808'::bigint, true::boolean, true::boolean) AS v",
        "22013",
    ),
    (
        'SELECT pg_catalog."in_range"((-7)::integer, (-7)::integer, (-7)::integer, false::boolean, false::boolean) AS v',
        "22013",
    ),
    (
        "SELECT pg_catalog.\"in_range\"(3::integer, 3::integer, '-2147483648'::integer, true::boolean, true::boolean) AS v",
        "22013",
    ),
    (
        'SELECT pg_catalog."in_range"((-7)::integer, (-7)::integer, (-7)::smallint, false::boolean, false::boolean) AS v',
        "22013",
    ),
    (
        "SELECT pg_catalog.\"in_range\"(3::integer, 3::integer, '-32768'::smallint, true::boolean, true::boolean) AS v",
        "22013",
    ),
    (
        'SELECT pg_catalog."in_range"((-7)::smallint, (-7)::smallint, (-7)::bigint, false::boolean, false::boolean) AS v',
        "22013",
    ),
    (
        "SELECT pg_catalog.\"in_range\"(3::smallint, 3::smallint, '-9223372036854775808'::bigint, true::boolean, true::boolean) AS v",
        "22013",
    ),
    (
        'SELECT pg_catalog."in_range"((-7)::smallint, (-7)::smallint, (-7)::integer, false::boolean, false::boolean) AS v',
        "22013",
    ),
    (
        "SELECT pg_catalog.\"in_range\"(3::smallint, 3::smallint, '-2147483648'::integer, true::boolean, true::boolean) AS v",
        "22013",
    ),
    (
        'SELECT pg_catalog."in_range"((-7)::smallint, (-7)::smallint, (-7)::smallint, false::boolean, false::boolean) AS v',
        "22013",
    ),
    (
        "SELECT pg_catalog.\"in_range\"(3::smallint, 3::smallint, '-32768'::smallint, true::boolean, true::boolean) AS v",
        "22013",
    ),
    (
        'SELECT pg_catalog."in_range"((-2.25)::double precision, (-2.25)::double precision, (-2.25)::double precision, false::boolean, false::boolean) AS v',
        "22013",
    ),
    (
        "SELECT pg_catalog.\"in_range\"(0.5::double precision, 0.5::double precision, 'NaN'::double precision, true::boolean, true::boolean) AS v",
        "22013",
    ),
    (
        'SELECT pg_catalog."in_range"((-2.25)::real, (-2.25)::real, (-2.25)::double precision, false::boolean, false::boolean) AS v',
        "22013",
    ),
    (
        "SELECT pg_catalog.\"in_range\"(0.5::real, 0.5::real, 'NaN'::double precision, true::boolean, true::boolean) AS v",
        "22013",
    ),
    (
        'SELECT pg_catalog."in_range"((-7.125)::numeric, (-7.125)::numeric, (-7.125)::numeric, false::boolean, false::boolean) AS v',
        "22013",
    ),
    ("SELECT pg_catalog.\"to_hex\"('2147483648'::integer) AS v", "22003"),
    ("SELECT pg_catalog.\"to_hex\"('9223372036854775808'::bigint) AS v", "22003"),
    ("SELECT '3.4028236e+38'::real", "22003"),
    ("SELECT '1e-46'::real", "22003"),
    ("SELECT pg_catalog.power(0::double precision, (-1)::double precision)", "2201F"),
    ("SELECT pg_catalog.power((-8)::double precision, (1)::double precision / 3)", "2201F"),
    ("SELECT pg_catalog.power(10::double precision, 400::double precision)", "22003"),
    ("SELECT pg_catalog.log10(0::double precision)", "2201E"),
    ("SELECT pg_catalog.log(0::double precision)", "2201E"),
    ("SELECT pg_catalog.chr(55296)", "54000"),
    ("SELECT pg_catalog.chr(1114112)", "54000"),
    ("SELECT pg_catalog.factorial(32178::bigint)", "22003"),
    ("SELECT pg_catalog.lcm(65536::integer, 32769::integer)", "22003"),
    ("SELECT pg_catalog.lcm(3037000499::bigint, 3037000507::bigint)", "22003"),
    (
        "SELECT pg_catalog.var_pop(c1) AS v FROM (VALUES ('1e308'::double precision), ('1e308'::double precision)) AS t(c1)",
        "22003",
    ),
]

#: A numeric NaN or infinity: ClickHouse's Decimal holds neither, and its cast answered NULL.
NOT_FINITE_NUMERICS = [
    "SELECT pg_catalog.\"abs\"('NaN'::numeric) AS v",
    "SELECT pg_catalog.\"round\"('NaN'::numeric) AS v",
    "SELECT 'Infinity'::numeric AS v",
    "SELECT '-Infinity'::numeric AS v",
    "SELECT pg_catalog.\"sum\"(c1) AS v FROM (VALUES ('NaN'::numeric), ('NaN'::numeric)) AS t(c1)",
]


def _real(value: float) -> float:
    return struct.unpack("<f", struct.pack("<f", value))[0]


def _same(want, got) -> bool:
    """PostgreSQL's text of a value against the engine's: a boolean t or f (ClickHouse's 1 or 0),
    a float by its value (NaN equal to NaN), a real by its value as a real, a double within 2
    units in the last place (libm's trigonometry and pow differ by one)."""
    if want is None or got is None:
        return want is got
    if want in ("t", "f") and isinstance(got, (bool, int)):
        return (want == "t") == bool(got)
    if isinstance(got, float):
        expected = float(want)
        if math.isnan(expected) or math.isnan(got):
            return math.isnan(expected) and math.isnan(got)
        if expected == got or _real(expected) == got:
            return True
        return math.isfinite(expected) and abs(expected - got) <= 2 * math.ulp(expected)
    if isinstance(got, int):
        return int(want) == got
    return want == str(got)


@pytest.fixture(scope="module")
def runtime() -> ClickHouseFederationRuntime:
    return ClickHouseFederationRuntime.embedded()


def _answer(runtime: ClickHouseFederationRuntime, sql: str) -> object:
    ((value,),) = list(runtime.run_sync(transpile(sql, "clickhouse")).iter_rows())
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


@pytest.mark.parametrize("sql", NOT_FINITE_NUMERICS)
def test_a_numeric_nan_or_infinity_is_refused_not_read_as_null(sql):
    with pytest.raises(RowRefused) as raised:
        transpile(sql, "clickhouse")
    assert raised.value.sqlstate == "0A000"
    assert "has no form on clickhouse" in str(raised.value)


def _bits(value: float) -> int:
    return struct.unpack("<I", struct.pack("<f", value))[0]


def _of_bits(bits: int) -> float:
    return struct.unpack("<f", struct.pack("<I", bits))[0]


@pytest.mark.parametrize("bits", [1, 0x00800000, 0x3DCCCCCD, 0x4B800001, 0x7F7FFFFE])
def test_a_real_literal_is_the_nearest_real_a_tie_to_the_even_one(bits):
    with decimal.localcontext(decimal.Context(prec=1000)):
        low, high = _of_bits(bits), _of_bits(bits + 1)
        tie = (decimal.Decimal(low) + decimal.Decimal(high)) / 2
        nudge = decimal.Decimal(10) ** (tie.adjusted() - 60)
        even = low if bits % 2 == 0 else high
        assert nearest_real(tie) == even
        assert nearest_real(tie + nudge) == high
        assert nearest_real(tie - nudge) == low
        assert nearest_real(-tie) == -even


def test_a_real_past_the_type_is_none():
    assert nearest_real(decimal.Decimal("3.4028236e+38")) is None
    assert math.isfinite(nearest_real(decimal.Decimal("3.4028235e+38")) or math.inf)


@pytest.mark.parametrize("name", ["to_hex", "to_oct", "to_bin"])
@pytest.mark.parametrize(
    ("literal", "type_name"),
    [("'2147483648'", "integer"), ("'9223372036854775808'", "bigint")],
)
def test_a_literal_past_its_type_raises_in_a_rule_of_no_pattern(name, literal, type_name):
    """to_hex and its kin are rules of no pattern, which bind the argument as written: the
    literal is read there as PostgreSQL reads it, where ClickHouse's cast answered NULL."""
    with pytest.raises(RowRefused) as raised:
        transpile(f'SELECT pg_catalog."{name}"({literal}::{type_name}) AS v', "clickhouse")
    assert raised.value.sqlstate == "22003"


#: A numeric's call PostgreSQL answers in numeric's exact digits: refused by name, as it was, where
#: the row answers a double's.
NUMERIC_CALLS = [
    "SELECT sqrt(2.5::numeric) AS v",
    "SELECT log10(2.5::numeric) AS v",
    "SELECT log(2.5::numeric) AS v",
    "SELECT power(2.5::numeric, 3::numeric) AS v",
    "SELECT lcm(2.5::numeric, 3::numeric) AS v",
]


@pytest.mark.parametrize("sql", NUMERIC_CALLS)
def test_a_numeric_call_stays_refused_by_name(sql):
    with pytest.raises(FunctionRefused):
        transpile(sql, "clickhouse")


def test_a_row_reads_its_argument_once():
    """abs inside a range's form, itself inside another: each row's argument is bound once, so
    the statement grows with the nesting, not as its product (ClickHouse's AST bound)."""
    written = transpile("SELECT abs(abs(abs(abs(x)))) AS v FROM t", "clickhouse")
    assert written.count("[x]") == 1


@pytest.mark.parametrize(
    ("sql", "dialect"),
    [
        ("SELECT float8ge(a, b) FROM t", "duckdb"),
        ("SELECT float8ge(a, b) FROM t", "trino"),
        ("SELECT acos(a) FROM t", "duckdb"),
        ("SELECT abs(a) FROM t", "trino"),
        ('SELECT pg_catalog."log10"(a) FROM t', "duckdb"),
    ],
)
def test_every_other_dialect_keeps_its_form(sql, dialect):
    assert "isNaN" not in transpile(sql, dialect)
    assert "throwIf" not in transpile(sql, dialect)
