# Copyright (c) 2026 Kenneth Stott
# Canary: 69f3a07e-15e0-40c8-a426-837a9192ca1d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Detect: what a Generic SQL source has been shown to do (REQ-2191).

Run only when the admin presses Detect on the source form, never on connect. Each check is a
read-only statement against the real connection, written the way the source's real queries are --
in PostgreSQL, transpiled to the source's SQL dialect -- over rows the statement makes itself, so
no table of the source is needed and nothing is written. A check passes when the source answers
with the rows the statement means. Its result names why it failed: the source's own error, or the
rows it answered instead.

Nothing is saved here: the form ticks the settings that passed, and the admin saves the form.
Changing the dialect means pressing Detect again.
"""

# Requirements: REQ-2191

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from provisa.generic_sql.settings import (
    AGGREGATE,
    ATTACH,
    JOIN,
    KEY_RANGE,
    PREDICATE,
    attach_kind,
    backend,
)

#: Each pushdown check: the statement, in PostgreSQL, and the rows it means.
_CHECKS: dict[str, tuple[str, list[tuple]]] = {
    PREDICATE: (
        "SELECT t.x FROM (SELECT 1 AS x, 'a' AS y UNION ALL SELECT 2 AS x, 'c' AS y) AS t "
        "WHERE t.x = 1 AND t.y IN ('a', 'b')",
        [(1,)],
    ),
    JOIN: (
        "SELECT a.x, b.y FROM (SELECT 1 AS x) AS a "
        "JOIN (SELECT 1 AS x, 2 AS y UNION ALL SELECT 3 AS x, 4 AS y) AS b ON a.x = b.x",
        [(1, 2)],
    ),
    AGGREGATE: (
        "SELECT t.g, COUNT(*) AS n, SUM(t.v) AS s FROM "
        "(SELECT 1 AS g, 2 AS v UNION ALL SELECT 1 AS g, 3 AS v) AS t GROUP BY t.g",
        [(1, 2, 5)],
    ),
    KEY_RANGE: (
        "SELECT t.k FROM (SELECT 1 AS k UNION ALL SELECT 2 AS k UNION ALL SELECT 5 AS k) AS t "
        "WHERE t.k >= 1 AND t.k < 3 ORDER BY t.k",
        [(1,), (2,)],
    ),
}

#: Dialects in which every SELECT names a table: the rows a check makes are selected FROM DUAL.
_FROM_REQUIRED: dict[str, str] = {"oracle": "dual"}


@dataclass(frozen=True)
class Check:
    capability: str
    passed: bool
    #: Why it failed; empty when it passed.
    reason: str


def statement(capability: str, sql_dialect: str) -> str:
    """The check for capability, as the source's dialect writes it."""
    import sqlglot
    from sqlglot import exp

    import provisa.generic_sql.dialect  # noqa: F401 -- registers "ansi"

    tree = sqlglot.parse_one(_CHECKS[capability][0], read="postgres")
    table = _FROM_REQUIRED.get(sql_dialect)
    if table is not None:
        for select in tree.find_all(exp.Select):
            if select.args.get("from_") is None:
                select.from_(table, copy=False)
    return tree.sql(dialect=sql_dialect)


def _plain(value: Any) -> Any:
    if isinstance(value, Decimal) and value == value.to_integral_value():
        return int(value)
    return value


def _check_rows(capability: str, rows: list[tuple] | None, failure: str) -> Check:
    if rows is None:
        return Check(capability, False, failure)
    expected = _CHECKS[capability][1]
    plain = [tuple(_plain(v) for v in row) for row in rows]
    if plain != expected:
        return Check(capability, False, f"answered {plain!r}, expected {expected!r}")
    return Check(capability, True, "")


def _attach(url: str) -> Check:
    if attach_kind(url) is None:
        return Check(ATTACH, False, f"a {backend(url)} URL has no engine attach")
    return Check(ATTACH, False, "live attach of a Generic SQL source is not built yet")


async def detect(hints: dict[str, str], user: str, password: str) -> list[Check]:
    """Every capability's check against the source the form's settings (``hints``) describe, in
    the order the form shows them, through the driver the source would be read through
    (``GenericSQLDriver``): its URL held to the org's folder and its egress rules, a jdbc: URL to
    the customer's driver where this server runs one. A check the source answers with an error
    fails naming it; a source that cannot be connected to fails every check with why."""
    from provisa.executor.drivers.generic_sql import GenericSQLDriver

    driver = GenericSQLDriver()
    driver.configure(hints)
    settings = driver.settings
    checks = [_attach(settings.url)]
    failure = ""
    try:
        await driver.connect("", 0, "", user, password)
    except Exception as exc:  # noqa: BLE001 -- why it cannot be connected is every check's reason
        failure = _reason(exc)
    try:
        for capability in (PREDICATE, JOIN, AGGREGATE, KEY_RANGE):
            rows: list[tuple] | None = None
            reason = failure
            if not failure:
                try:
                    sql = statement(capability, settings.sql_dialect)
                    rows = (await driver.execute(sql)).rows
                except Exception as exc:  # noqa: BLE001 -- the source's own error is the reason
                    reason = _reason(exc)
            checks.append(_check_rows(capability, rows, reason))
        return checks
    finally:
        if driver.is_connected:
            await driver.close()


def _reason(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}".splitlines()[0]
