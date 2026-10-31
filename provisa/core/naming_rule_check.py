# Copyright (c) 2026 Kenneth Stott
# Canary: 96d1b4f7-3e08-4a2c-8c51-f2a7036e9b14
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A naming rule runs in two engines (REQ-2190): Python's ``re`` names a table for the compiler, and
PostgreSQL's ``regexp_replace`` names it for the catalog's views. A rule they would read differently
gives a table two names, so it is refused where it is written."""

# Requirements: REQ-2190

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: Python's alone: PostgreSQL writes no named group, and reads ``\g<1>`` as the letter g.
_PYTHON_ONLY = re.compile(r"\(\?P[<=]")
_PYTHON_REPLACEMENT = re.compile(r"\\g<")
#: The names a rule is tried on, beside the tables it would name.
_NAMES = (
    "orders",
    "customer_orders",
    "CustomerOrders",
    "tbl_orders",
    "orders_v1",
    "v_orders_2024",
    "stg__orders",
    "ORDERS",
    "a",
    "_x_",
    "orders.archive",
    "price$usd",
    "日本語",
)
_APPLY = "SELECT regexp_replace($1, $2, $3, 'g')"


class NamingRuleRefused(ValueError):
    """A naming rule Python and PostgreSQL would read differently."""

    code = "config.naming_rule_refused"

    def __init__(self, pattern: str, why: str) -> None:
        self.params = {"pattern": pattern}
        super().__init__(f"naming rule {pattern!r}: {why}")


async def refuse_divergent_rule(
    conn: "Connection", pattern: str, replacement: str, names: Iterable[str] = ()
) -> None:
    """Raise :class:`NamingRuleRefused` where the rule's pattern is one only one engine reads, or
    the two answer differently on ``names`` or a set of names of the usual shapes."""
    if _PYTHON_ONLY.search(pattern) or _PYTHON_REPLACEMENT.search(replacement):
        raise NamingRuleRefused(pattern, "it uses a form only Python reads")
    try:
        compiled = re.compile(pattern)
    except re.error as exc:
        raise NamingRuleRefused(pattern, f"Python cannot read it: {exc}") from exc
    for name in (*_NAMES, *names):
        python = compiled.sub(replacement, name)
        try:
            async with conn.transaction():
                postgres = await conn.fetchval(_APPLY, name, pattern, replacement)
        except Exception as exc:  # complexity-gate: allow-ble=1 reason=any refusal of the regex by the server is the verdict this function exists to report
            raise NamingRuleRefused(pattern, f"PostgreSQL cannot read it: {exc}") from exc
        if python != postgres:
            raise NamingRuleRefused(
                pattern, f"on {name!r} Python answers {python!r}, PostgreSQL {postgres!r}"
            )
