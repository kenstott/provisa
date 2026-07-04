# Copyright (c) 2026 Kenneth Stott
# Canary: 7a9c1e3d-5b2f-4640-8a1c-3e5d7f9b0a2c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""MV definition determinism check (REQ-879).

A ``distributed`` (per-instance / eventually-consistent) MV is only *eventually*
consistent if its ``view_sql`` is DETERMINISTIC — otherwise two instances compute
different content from the same source and never converge. This module rejects the
non-deterministic constructs a distributed MV must not use: volatile functions
(now/current_timestamp/random/uuid/…) and a ``LIMIT`` with no total ``ORDER BY``
(arbitrary row selection). ``shared`` MVs have a single coordinated copy, so
determinism is irrelevant to their consistency and this check does not apply.
"""

from __future__ import annotations

import sqlglot
from sqlglot import expressions as exp
from sqlglot.errors import SqlglotError

# Functions whose value varies per evaluation, independent of the source data.
_VOLATILE_FUNCS = frozenset(
    {
        "now",
        "current_timestamp",
        "current_date",
        "current_time",
        "localtimestamp",
        "localtime",
        "random",
        "rand",
        "uuid",
        "gen_random_uuid",
        "uuid_generate_v4",
        "newid",
        "sysdate",
        "getdate",
    }
)
_VOLATILE_NODES = (exp.CurrentTimestamp, exp.CurrentDate, exp.CurrentTime)


def _func_name(node) -> str:
    # Anonymous (unrecognised) funcs carry the real name on .name; sql_name() is
    # the literal "ANONYMOUS". Built-in funcs expose it via sql_name().
    if isinstance(node, exp.Anonymous):
        return (node.name or "").lower()
    return (node.sql_name() or node.name or "").lower()


def check_view_determinism(sql: str, dialect: str | None = None) -> tuple[bool, str]:
    """Return (deterministic, reason). ``reason`` is empty when deterministic.

    Fail-closed on parse errors: a view whose SQL cannot be parsed cannot be
    certified deterministic, so it is rejected for the distributed tier.
    """
    try:
        expr = sqlglot.parse_one(sql, dialect=dialect)
    except SqlglotError as exc:
        return False, f"view_sql cannot be parsed: {exc}"

    for node in expr.find_all(exp.Func, exp.Anonymous, *_VOLATILE_NODES):
        if isinstance(node, _VOLATILE_NODES) or _func_name(node) in _VOLATILE_FUNCS:
            return False, f"non-deterministic function: {node.sql(dialect=dialect)}"

    # A LIMIT with no total ORDER BY selects arbitrary rows — non-deterministic.
    for select in expr.find_all(exp.Select):
        if select.args.get("limit") is not None and select.args.get("order") is None:
            return False, "LIMIT without a total ORDER BY selects arbitrary rows"

    return True, ""


def validate_mv_consistency(
    consistency: str, view_sql: str | None, dialect: str | None = None
) -> None:
    """Enforce the MV consistency tier at registration (REQ-879).

    A ``distributed`` MV must have a deterministic view, else per-instance copies never
    converge; raise so it is rejected. ``shared`` MVs are single-copy and need no check.
    A join-pattern MV (no explicit ``view_sql``) is deterministic by construction.
    """
    if consistency != "distributed" or not view_sql:
        return
    ok, reason = check_view_determinism(view_sql, dialect)
    if not ok:
        raise ValueError(
            f"distributed MV requires a deterministic view (per-instance copies would "
            f"never converge): {reason}. Use consistency='shared' or fix the view."
        )
