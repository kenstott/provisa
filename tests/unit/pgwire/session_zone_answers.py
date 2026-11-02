# Copyright (c) 2026 Kenneth Stott
# Canary: b2c6d1f8-4e07-49a3-9c15-e08a73f2d641
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a statement over a timestamptz answers in a session on a zone. The text of each instant is
what test_timestamptz_session_zone.py recorded of postgres:17.11; tests/integration/
test_engine_session_zone.py holds this whole table against postgres:17.11 itself."""

# Requirements: REQ-2171

from __future__ import annotations

#: The instants, in the order of ``i``: summer, winter, and one a zone west of UTC sees as yesterday.
INSTANTS = ("2026-07-01 12:00:00.5+00", "2026-01-01 12:00:00+00", "2026-07-01 02:30:00+00")

EXPRESSIONS = {
    "text": "c::text",
    "date": "c::date",
    "hour": "extract(hour from c)",
    "trunc": "date_trunc('day', c)",
    "second": "date_trunc('second', c)",
    "millisecond": "date_trunc('millisecond', c)",
    "microsecond": "date_trunc('microsecond', c)",
    "json": "to_json(c)",
}

#: The statement that puts a session on the zone -> what each expression answers of each instant.
ANSWERS: dict[str, dict[str, tuple[str, str, str]]] = {
    "SET TimeZone = 'America/New_York'": {
        "text": ("2026-07-01 08:00:00.5-04", "2026-01-01 07:00:00-05", "2026-06-30 22:30:00-04"),
        "date": ("2026-07-01", "2026-01-01", "2026-06-30"),
        "hour": ("8", "7", "22"),
        "trunc": ("2026-07-01 00:00:00-04", "2026-01-01 00:00:00-05", "2026-06-30 00:00:00-04"),
        "second": ("2026-07-01 08:00:00-04", "2026-01-01 07:00:00-05", "2026-06-30 22:30:00-04"),
        "millisecond": (
            "2026-07-01 08:00:00.5-04",
            "2026-01-01 07:00:00-05",
            "2026-06-30 22:30:00-04",
        ),
        "microsecond": (
            "2026-07-01 08:00:00.5-04",
            "2026-01-01 07:00:00-05",
            "2026-06-30 22:30:00-04",
        ),
        "json": (
            '"2026-07-01T08:00:00.5-04:00"',
            '"2026-01-01T07:00:00-05:00"',
            '"2026-06-30T22:30:00-04:00"',
        ),
    },
    "SET TimeZone = 'Asia/Kolkata'": {
        "text": (
            "2026-07-01 17:30:00.5+05:30",
            "2026-01-01 17:30:00+05:30",
            "2026-07-01 08:00:00+05:30",
        ),
        "date": ("2026-07-01", "2026-01-01", "2026-07-01"),
        "hour": ("17", "17", "8"),
        "trunc": (
            "2026-07-01 00:00:00+05:30",
            "2026-01-01 00:00:00+05:30",
            "2026-07-01 00:00:00+05:30",
        ),
        "second": (
            "2026-07-01 17:30:00+05:30",
            "2026-01-01 17:30:00+05:30",
            "2026-07-01 08:00:00+05:30",
        ),
        "millisecond": (
            "2026-07-01 17:30:00.5+05:30",
            "2026-01-01 17:30:00+05:30",
            "2026-07-01 08:00:00+05:30",
        ),
        "microsecond": (
            "2026-07-01 17:30:00.5+05:30",
            "2026-01-01 17:30:00+05:30",
            "2026-07-01 08:00:00+05:30",
        ),
        "json": (
            '"2026-07-01T17:30:00.5+05:30"',
            '"2026-01-01T17:30:00+05:30"',
            '"2026-07-01T08:00:00+05:30"',
        ),
    },
    # POSIX's sign: five hours WEST. A name read as ICU reads it would be five hours east.
    "SET TimeZone = 'UTC+5'": {
        "text": ("2026-07-01 07:00:00.5-05", "2026-01-01 07:00:00-05", "2026-06-30 21:30:00-05"),
        "date": ("2026-07-01", "2026-01-01", "2026-06-30"),
        "hour": ("7", "7", "21"),
        "trunc": ("2026-07-01 00:00:00-05", "2026-01-01 00:00:00-05", "2026-06-30 00:00:00-05"),
        "second": ("2026-07-01 07:00:00-05", "2026-01-01 07:00:00-05", "2026-06-30 21:30:00-05"),
        "millisecond": (
            "2026-07-01 07:00:00.5-05",
            "2026-01-01 07:00:00-05",
            "2026-06-30 21:30:00-05",
        ),
        "microsecond": (
            "2026-07-01 07:00:00.5-05",
            "2026-01-01 07:00:00-05",
            "2026-06-30 21:30:00-05",
        ),
        "json": (
            '"2026-07-01T07:00:00.5-05:00"',
            '"2026-01-01T07:00:00-05:00"',
            '"2026-06-30T21:30:00-05:00"',
        ),
    },
    "SET TIME ZONE -5": {
        "text": ("2026-07-01 07:00:00.5-05", "2026-01-01 07:00:00-05", "2026-06-30 21:30:00-05"),
        "date": ("2026-07-01", "2026-01-01", "2026-06-30"),
        "hour": ("7", "7", "21"),
        "trunc": ("2026-07-01 00:00:00-05", "2026-01-01 00:00:00-05", "2026-06-30 00:00:00-05"),
        "second": ("2026-07-01 07:00:00-05", "2026-01-01 07:00:00-05", "2026-06-30 21:30:00-05"),
        "millisecond": (
            "2026-07-01 07:00:00.5-05",
            "2026-01-01 07:00:00-05",
            "2026-06-30 21:30:00-05",
        ),
        "microsecond": (
            "2026-07-01 07:00:00.5-05",
            "2026-01-01 07:00:00-05",
            "2026-06-30 21:30:00-05",
        ),
        "json": (
            '"2026-07-01T07:00:00.5-05:00"',
            '"2026-01-01T07:00:00-05:00"',
            '"2026-06-30T21:30:00-05:00"',
        ),
    },
    "SET TimeZone = 'UTC'": {
        "text": ("2026-07-01 12:00:00.5+00", "2026-01-01 12:00:00+00", "2026-07-01 02:30:00+00"),
        "date": ("2026-07-01", "2026-01-01", "2026-07-01"),
        "hour": ("12", "12", "2"),
        "trunc": ("2026-07-01 00:00:00+00", "2026-01-01 00:00:00+00", "2026-07-01 00:00:00+00"),
        "second": ("2026-07-01 12:00:00+00", "2026-01-01 12:00:00+00", "2026-07-01 02:30:00+00"),
        "millisecond": (
            "2026-07-01 12:00:00.5+00",
            "2026-01-01 12:00:00+00",
            "2026-07-01 02:30:00+00",
        ),
        "microsecond": (
            "2026-07-01 12:00:00.5+00",
            "2026-01-01 12:00:00+00",
            "2026-07-01 02:30:00+00",
        ),
        "json": (
            '"2026-07-01T12:00:00.5+00:00"',
            '"2026-01-01T12:00:00+00:00"',
            '"2026-07-01T02:30:00+00:00"',
        ),
    },
}

#: Each case of the table: (the zone's statement, the expression's name, the instants' answers).
CASES = [(zone, name, want) for zone, column in ANSWERS.items() for name, want in column.items()]


def statement(name: str) -> str:
    return f"SELECT {EXPRESSIONS[name]} AS v FROM t ORDER BY i"
