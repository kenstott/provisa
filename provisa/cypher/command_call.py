# Copyright (c) 2026 Kenneth Stott
# Canary: 3e7b1c95-2d48-4f6a-8b03-9c5e1a7d2f64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``CALL <command>(args) [YIELD …] [RETURN …]`` — the one reading of a command call in Cypher,
for Bolt and Cypher over HTTP alike (REQ-872, REQ-1156).

A bare (undotted) procedure name is always a command: Neo4j's own procedures are namespaced
(``db.labels``, ``dbms.procedures``) and Provisa's are under ``provisa.``. So a call to a name that
is no command the role may use answers as an unknown command, whether it was never registered or
is not the caller's to use.

What is read here and refused by name when it cannot be honoured:

* arguments — split on top-level commas (quotes and brackets respected); each is a literal
  (string, number, boolean, null, list of these) or a ``$parameter``, bound from the request's
  parameters; a parameter the request does not carry is refused, as is anything else (an
  expression, a name);
* ``YIELD col [AS alias], …`` or ``YIELD *`` — the columns kept, renamed as written;
* ``RETURN name [AS alias], …`` or ``RETURN *`` — a projection of what the call yields; any other
  RETURN, and any other clause after the call, is refused rather than ignored.

The argument COUNT is checked against the command's signature only after the command is admitted
for the role (``action_exec.bind_command_args``), so a refusal never tells a caller about a
command it may not use."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

_CALL_HEAD_RE = re.compile(r"^\s*CALL\s+([A-Za-z_]\w*)\s*\(", re.IGNORECASE)
_ITEM_RE = re.compile(r"^\s*([A-Za-z_]\w*)\s*(?:\s+AS\s+([A-Za-z_]\w*))?\s*$", re.IGNORECASE)
_YIELD_RE = re.compile(r"^\s*YIELD\s+(.+?)(?=\s+RETURN\b|$)", re.IGNORECASE | re.DOTALL)
_RETURN_RE = re.compile(r"^\s*RETURN\s+(.+)$", re.IGNORECASE | re.DOTALL)
_NUMBER_RE = re.compile(r"^-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?$")


class CommandCallRefused(ValueError):
    """A command call that cannot be honoured as written — said by name, never run otherwise."""


@dataclass(frozen=True)
class CommandCall:
    name: str
    values: list[Any]  # positional, in the order written
    yields: list[tuple[str, str]] | None  # (column, alias); None = no YIELD (every column)
    returns: list[tuple[str, str]] | None  # (name, alias); None = no RETURN


def _scan_parens(text: str, start: int) -> int:
    """Index of the ``)`` closing the ``(`` just before ``start``; quotes and nesting respected."""
    depth = 1
    quote: str | None = None
    i = start
    while i < len(text):
        ch = text[i]
        if quote:
            if ch == "\\":
                i += 2
                continue
            if ch == quote:
                quote = None
        elif ch in "'\"":
            quote = ch
        elif ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    raise CommandCallRefused("CALL: the argument list is not closed")


def _split_top_level(raw: str) -> list[str]:
    parts: list[str] = []
    buf: list[str] = []
    depth = 0
    quote: str | None = None
    i = 0
    while i < len(raw):
        ch = raw[i]
        if quote:
            buf.append(ch)
            if ch == "\\" and i + 1 < len(raw):
                buf.append(raw[i + 1])
                i += 2
                continue
            if ch == quote:
                quote = None
        elif ch in "'\"":
            quote = ch
            buf.append(ch)
        elif ch in "([{":
            depth += 1
            buf.append(ch)
        elif ch in ")]}":
            depth -= 1
            buf.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
        i += 1
    tail = "".join(buf)
    if tail.strip() or parts:
        parts.append(tail)
    return parts


def _unquote(tok: str) -> str:
    body = tok[1:-1]
    out: list[str] = []
    i = 0
    while i < len(body):
        ch = body[i]
        if ch == "\\" and i + 1 < len(body):
            nxt = body[i + 1]
            out.append({"n": "\n", "t": "\t", "r": "\r"}.get(nxt, nxt))
            i += 2
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _value(tok: str, params: dict, name: str, position: int) -> Any:
    tok = tok.strip()
    if not tok:
        raise CommandCallRefused(f"CALL {name}: argument {position} is empty")
    if tok.startswith("$"):
        key = tok[1:]
        if key not in params:
            raise CommandCallRefused(
                f"CALL {name}: parameter ${key} (argument {position}) was not supplied"
            )
        return params[key]
    if len(tok) >= 2 and tok[0] in "'\"" and tok[-1] == tok[0]:  # noqa: PLR2004
        return _unquote(tok)
    if tok.startswith("[") and tok.endswith("]"):
        return [_value(t, params, name, position) for t in _split_top_level(tok[1:-1])]
    low = tok.lower()
    if low in ("true", "false"):
        return low == "true"
    if low == "null":
        return None
    if _NUMBER_RE.match(tok):
        return int(tok) if re.fullmatch(r"-?\d+", tok) else float(tok)
    raise CommandCallRefused(
        f"CALL {name}: argument {position} is not a value or a $parameter: {tok!r}"
    )


def _items(raw: str, clause: str, name: str) -> list[tuple[str, str]] | None:
    """``*`` (None: everything) or ``col [AS alias], …``."""
    if raw.strip() == "*":
        return None
    items: list[tuple[str, str]] = []
    for part in _split_top_level(raw):
        m = _ITEM_RE.match(part)
        if m is None:
            raise CommandCallRefused(
                f"{clause} after CALL {name} takes names the call returns, optionally "
                f"renamed with AS; not {part.strip()!r}"
            )
        items.append((m.group(1), m.group(2) or m.group(1)))
    return items


def parse_command_call(query: str, params: dict) -> CommandCall | None:
    """The command call ``query`` is, or None when it is no ``CALL <bare name>(…)``."""
    text = query.strip().rstrip(";").rstrip()
    head = _CALL_HEAD_RE.match(text)
    if head is None:
        return None
    name = head.group(1)
    close = _scan_parens(text, head.end())
    values = [
        _value(tok, params, name, i + 1)
        for i, tok in enumerate(_split_top_level(text[head.end() : close]))
    ]
    rest = text[close + 1 :]
    yields: list[tuple[str, str]] | None = None
    returns: list[tuple[str, str]] | None = None
    m = _YIELD_RE.match(rest)
    if m is not None:
        yields = _items(m.group(1), "YIELD", name)
        rest = rest[m.end() :]
    m = _RETURN_RE.match(rest)
    if m is not None:
        returns = _items(m.group(1), "RETURN", name) or []
        rest = ""
    if rest.strip():
        raise CommandCallRefused(
            f"CALL {name}: only YIELD and RETURN may follow a command call; not "
            f"{rest.strip().split()[0]!r}"
        )
    return CommandCall(name=name, values=values, yields=yields, returns=returns)


def project(call: CommandCall, rows: list[dict]) -> tuple[list[str], list[dict]]:
    """The call's rows as its YIELD and RETURN shape them. A column named that the call did not
    return is refused by name."""
    cols = list(rows[0].keys()) if rows else []
    if call.yields is not None:
        if rows:
            missing = [src for src, _ in call.yields if src not in rows[0]]
            if missing:
                raise CommandCallRefused(
                    f"YIELD after CALL {call.name}: {missing[0]!r} is not a column it returns "
                    f"({', '.join(cols)})"
                )
        rows = [{alias: r.get(src) for src, alias in call.yields} for r in rows]
        cols = [alias for _, alias in call.yields]
    if call.returns:
        missing = [src for src, _ in call.returns if src not in cols]
        if missing and (rows or call.yields is not None):
            raise CommandCallRefused(
                f"RETURN after CALL {call.name}: {missing[0]!r} is not a name the call yields "
                f"({', '.join(cols)})"
            )
        rows = [{alias: r.get(src) for src, alias in call.returns} for r in rows]
        cols = [alias for _, alias in call.returns]
    return cols, rows
