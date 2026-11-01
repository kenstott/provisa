# Copyright (c) 2026 Kenneth Stott
# Canary: 8c2e5a14-6f37-4d09-b1a8-2e97c3d4f6b0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The wrong answers the function support measure found on Trino (REQ-2171, AN ANSWER THAT DIFFERS
IS REFUSED): each statement as Provisa writes it for Trino, run there, against postgres:17.11's
answer, or refused by name."""

# Requirements: REQ-2171

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.transpile import transpile

pytestmark = [pytest.mark.integration, pytest.mark.requires_trino]

#: statement -> postgres:17.11's answer. A decimal compares with its scale.
ANSWERS: dict[str, Any] = {
    "SELECT numeric_abs(2.5::numeric) AS v": Decimal("2.5"),
    "SELECT numeric_add(2.5::numeric, 2.5::numeric) AS v": Decimal("5.0"),
    "SELECT numeric_inc(2.5::numeric) AS v": Decimal("3.5"),
    "SELECT numeric_larger(2.5::numeric, 2.5::numeric) AS v": Decimal("2.5"),
    "SELECT numeric_mod(2.5::numeric, 2.5::numeric) AS v": Decimal("0.0"),
    "SELECT numeric_mul(2.5::numeric, 2.5::numeric) AS v": Decimal("6.25"),
    "SELECT numeric_smaller(2.5::numeric, 2.5::numeric) AS v": Decimal("2.5"),
    "SELECT numeric_sub(2.5::numeric, 2.5::numeric) AS v": Decimal("0.0"),
    "SELECT numeric_uminus(2.5::numeric) AS v": Decimal("-2.5"),
    "SELECT numeric_uplus(2.5::numeric) AS v": Decimal("2.5"),
    "SELECT scale(2.5::numeric) AS v": 1,
    "SELECT numrange((-7.125)::numeric, (-7.125)::numeric, '[]'::text) AS v": "[-7.125,-7.125]",
    "SELECT center(NULL::box) AS v": None,
    "SELECT center(NULL::circle) AS v": None,
    "SELECT box_center(NULL::box) AS v": None,
    "SELECT circle_center(NULL::circle) AS v": None,
    "SELECT diagonal(NULL::box) AS v": None,
    "SELECT icu_unicode_version() AS v": "16.0",
}
#: money is sent from its numeric base: the base's value, of any scale.
MONEY: dict[str, Decimal] = {
    "SELECT cash_div_int2('1000.00'::money, 42::smallint) AS v": Decimal("23.80"),
    "SELECT cash_div_int4('1000.00'::money, 42::integer) AS v": Decimal("23.80"),
    "SELECT money(2.5::numeric) AS v": Decimal("2.50"),
}


def _answer(trino_conn: Any, sql: str) -> Any:
    cursor = trino_conn.cursor()
    cursor.execute(transpile(sql, "trino"))
    return cursor.fetchall()[0][0]


@pytest.mark.parametrize("sql", ANSWERS)
def test_it_answers_as_postgresql_does(trino_conn, sql):
    got = _answer(trino_conn, sql)
    want = ANSWERS[sql]
    assert got == want and (not isinstance(want, Decimal) or str(got) == str(want))


@pytest.mark.parametrize("sql", MONEY)
def test_money_answers_as_postgresql_does(trino_conn, sql):
    assert Decimal(_answer(trino_conn, sql)) == MONEY[sql]


def test_two_timestamps_differ_by_an_interval_it_cannot_write():
    with pytest.raises(FunctionRefused):
        transpile("SELECT timestamp_mi(NULL::timestamp, NULL::timestamp) AS v", "trino")
