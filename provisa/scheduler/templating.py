# Copyright (c) 2026 Kenneth Stott
# Canary: 9772d88f-e0b2-4f80-835a-16ab1d388665
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Date/timestamp token substitution for scheduled SQL (REQ-1004).

A scheduled SQL statement may embed ``{{token}}`` placeholders that are
replaced with the run's execution date/time immediately before execution.
Substitution is pure and deterministic given ``run_at``.

Supported tokens:

    {{yyyymmdd}}    -> run_at as YYYYMMDD           (e.g. 20260713)
    {{YYYY-MM-DD}}  -> run_at as YYYY-MM-DD          (e.g. 2026-07-13)
    {{iso8601}}     -> run_at.isoformat()            (e.g. 2026-07-13T14:30:00+00:00)
    {{timestamp}}   -> integer Unix epoch seconds    (e.g. 1784056200)

A token supplies a VALUE, never part of a name: inside a single-quoted string literal it is
written into the literal; standing alone it becomes a literal of its own (a number for
``yyyymmdd``/``timestamp``, a quoted string otherwise). A token inside a quoted identifier, or
joined to the characters of a name (``orders_{{yyyymmdd}}``), is refused — a date that chose
which table a statement touches would let the schedule reach tables nobody reviewed.

An unrecognized ``{{...}}`` token raises ValueError (fail loud — no silent
pass-through of a possibly-mistyped token).
"""
# Requirements: REQ-1004

from __future__ import annotations

import re
from datetime import datetime

_TOKEN_RE = re.compile(r"\{\{\s*([^}]*?)\s*\}\}")
_NUMERIC_TOKENS = frozenset({"yyyymmdd", "timestamp"})
_NAME_CHAR = re.compile(r"[A-Za-z0-9_$.\"]")


class DateTokenNotAValue(ValueError):
    """A date token placed where it would form part of a name rather than a value."""


def _render_token(name: str, run_at: datetime) -> str:
    if name == "yyyymmdd":
        return run_at.strftime("%Y%m%d")
    if name == "YYYY-MM-DD":
        return run_at.strftime("%Y-%m-%d")
    if name == "iso8601":
        return run_at.isoformat()
    if name == "timestamp":
        return str(int(run_at.timestamp()))
    raise ValueError(f"Unrecognized scheduled-SQL date token: {{{{{name}}}}}")


def _quote_state(sql: str, end: int) -> str | None:
    """The quote ``sql[:end]`` leaves open: ``"'"`` (a string literal), ``'"'`` (a quoted
    identifier), or None."""
    quote: str | None = None
    i = 0
    while i < end:
        ch = sql[i]
        if quote:
            if ch == quote:
                if i + 1 < end and sql[i + 1] == quote:  # a doubled quote stays inside
                    i += 2
                    continue
                quote = None
        elif ch in "'\"":
            quote = ch
        i += 1
    return quote


def substitute_date_tokens(sql: str, run_at: datetime) -> str:
    """Replace each ``{{token}}`` in ``sql`` with its VALUE for ``run_at`` (see the module note).
    Pure and deterministic. Raises ValueError on an unknown token, and
    :class:`DateTokenNotAValue` on one placed in or against a name (REQ-1004)."""
    out: list[str] = []
    last = 0
    for m in _TOKEN_RE.finditer(sql):
        name = m.group(1)
        value = _render_token(name, run_at)
        quote = _quote_state(sql, m.start())
        if quote == '"':
            raise DateTokenNotAValue(
                f"date token {{{{{name}}}}} is inside a quoted name; a token supplies a value"
            )
        if quote is None:
            before = sql[m.start() - 1] if m.start() > 0 else " "
            after = sql[m.end()] if m.end() < len(sql) else " "
            if _NAME_CHAR.match(before) or _NAME_CHAR.match(after):
                raise DateTokenNotAValue(
                    f"date token {{{{{name}}}}} is joined to a name; a token supplies a value"
                )
            value = value if name in _NUMERIC_TOKENS else f"'{value}'"
        out.append(sql[last : m.start()])
        out.append(value)
        last = m.end()
    out.append(sql[last:])
    return "".join(out)
