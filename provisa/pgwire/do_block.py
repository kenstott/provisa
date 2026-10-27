# Copyright (c) 2026 Kenneth Stott
# Canary: 0b300364-a2e3-4d3f-b12e-a80e159d5e76
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``DO`` blocks of ``RAISE`` statements over pgwire.

Provisa runs no PL/pgSQL. A ``DO`` block whose body is ``RAISE`` statements alone -- a client
writing a message to its session log -- is answered as PostgreSQL answers it: each ``RAISE``
below ``EXCEPTION`` a NoticeResponse at its level, an ``EXCEPTION`` the ErrorResponse that ends
the block (P0001). The message is the format with each ``%`` replaced by the next argument; an
argument is a literal. Any other statement in a block is refused by name.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from provisa.compiler.definitions import NotAvailableHere

_DO_RE = re.compile(
    r"^\s*DO\s+(?:LANGUAGE\s+plpgsql\s+)?(?P<q>\$[A-Za-z_]*\$)(?P<body>.*)(?P=q)"
    r"(?:\s+LANGUAGE\s+plpgsql)?\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_BLOCK_RE = re.compile(r"^\s*BEGIN\b(?P<stmts>.*)\bEND\s*;?\s*$", re.IGNORECASE | re.DOTALL)
_RAISE_RE = re.compile(
    r"^RAISE(?:\s+(?P<level>DEBUG|LOG|INFO|NOTICE|WARNING|EXCEPTION))?\s+"
    r"'(?P<fmt>(?:[^']|'')*)'(?P<args>(?:\s*,\s*[^,]+)*)$",
    re.IGNORECASE | re.DOTALL,
)
_LITERAL_RE = re.compile(r"^(?:'(?P<s>(?:[^']|'')*)'|(?P<n>-?\d+(?:\.\d+)?)|(?P<null>NULL))$", re.I)


@dataclass(frozen=True)
class Raised:
    """One RAISE: its level (NOTICE, WARNING, ..., EXCEPTION) and its message."""

    level: str
    message: str


def is_do_block(sql: str) -> bool:
    return re.match(r"^\s*DO\b", sql, re.IGNORECASE) is not None


def _statements(body: str) -> list[str]:
    """``body``'s statements, split at semicolons outside string literals."""
    out, current, quoted = [], [], False
    for ch in body:
        if ch == "'":
            quoted = not quoted
        if ch == ";" and not quoted:
            out.append("".join(current).strip())
            current = []
            continue
        current.append(ch)
    tail = "".join(current).strip()
    if tail:
        out.append(tail)
    return [s for s in out if s]


def _argument(text: str) -> str:
    m = _LITERAL_RE.match(text.strip())
    if m is None:
        raise NotAvailableHere(
            f"a RAISE argument is a literal; {text.strip()!r} is an expression, which a DO block "
            "here does not evaluate"
        )
    if m["s"] is not None:
        return m["s"].replace("''", "'")
    return m["n"] if m["n"] is not None else "<NULL>"


def _message(fmt: str, args: list[str]) -> str:
    out, rest = [], list(args)
    i = 0
    while i < len(fmt):
        if fmt.startswith("%%", i):
            out.append("%")
            i += 2
        elif fmt[i] == "%":
            if not rest:
                raise ValueError("too few parameters specified for RAISE")
            out.append(rest.pop(0))
            i += 1
        else:
            out.append(fmt[i])
            i += 1
    if rest:
        raise ValueError("too many parameters specified for RAISE")
    return "".join(out)


def raised(sql: str) -> list[Raised]:
    """What the DO block ``sql`` raises, in order. Refused when its body is anything but RAISE
    statements."""
    do = _DO_RE.match(sql)
    block = _BLOCK_RE.match(do["body"]) if do else None
    if block is None:
        raise NotAvailableHere(
            "DO runs a block of RAISE statements here (DO $$ BEGIN RAISE NOTICE '...'; END $$); "
            "Provisa runs no other PL/pgSQL"
        )
    out: list[Raised] = []
    for statement in _statements(block["stmts"]):
        m = _RAISE_RE.match(statement)
        if m is None:
            raise NotAvailableHere(
                f"a DO block here runs RAISE statements alone; {statement.split()[0]!r} is not one"
            )
        args = [a for a in re.split(r"\s*,\s*", m["args"].strip().lstrip(",").strip()) if a]
        message = _message(m["fmt"].replace("''", "'"), [_argument(a) for a in args])
        out.append(Raised((m["level"] or "EXCEPTION").upper(), message))
    return out
