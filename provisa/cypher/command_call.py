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

The name is a command's Cypher command binding (REQ-2168), bare or dotted (``apoc.meta.data``).
Neo4j's own procedures (``db.labels``, ``dbms.procedures``) and Provisa's (``provisa.metric``) are
answered elsewhere, so a name under those namespaces is no command call. A call to any other name
that is no command the role may use answers as an unknown command, whether it was never
registered or is not the caller's to use.

What is read here and refused by name when it cannot be honoured:

* ``CALL name`` without parentheses (implicit arguments, REQ-2001 amended 2026-10-28) — each of
  the command's arguments is bound from the request parameter of the same name, once the command
  is known (``action_exec.bind_command_args``); a parameter the request does not carry is refused
  by name;
* arguments — split on top-level commas (quotes and brackets respected); each is a literal
  (string, number, boolean, null, list of these) or a ``$parameter``, bound from the request's
  parameters; a parameter the request does not carry is refused, as is anything else (an
  expression, a name);
* ``YIELD col [AS alias], …`` or ``YIELD *`` — the columns kept, renamed as written;
* ``RETURN name [AS alias], …`` or ``RETURN *`` — a projection of what the call yields. A call
  followed by any other clause (WHERE, WITH, UNWIND, …) is a call composed in a statement, which
  the Cypher translator reads (REQ-2168), so it is no call read here.

The argument COUNT is checked against the command's signature only after the command is admitted
for the role (``action_exec.bind_command_args``), so a refusal never tells a caller about a
command it may not use."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

_CALL_HEAD_RE = re.compile(r"^\s*CALL\s+([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)\s*\(", re.IGNORECASE)
# A call without parentheses: the name, then the end, YIELD or RETURN.
_IMPLICIT_HEAD_RE = re.compile(
    r"^\s*CALL\s+([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)(?=\s*$|\s+(?:YIELD|RETURN)\b)",
    re.IGNORECASE,
)
# Neo4j's own procedure namespaces and Provisa's, answered before a command call is read.
_ANSWERED_ELSEWHERE = ("db.", "dbms.", "provisa.")
_ITEM_RE = re.compile(r"^\s*([A-Za-z_]\w*)\s*(?:\s+AS\s+([A-Za-z_]\w*))?\s*$", re.IGNORECASE)
_YIELD_RE = re.compile(r"^\s*YIELD\s+(.+?)(?=\s+RETURN\b|$)", re.IGNORECASE | re.DOTALL)
_RETURN_RE = re.compile(r"^\s*RETURN\s+(.+)$", re.IGNORECASE | re.DOTALL)
# What follows a call composed in a statement: a YIELD then another clause than RETURN.
_COMPOSED_RE = re.compile(
    r"^\s*YIELD\s+.+?\s(?:WHERE|WITH|UNWIND|MATCH|OPTIONAL|CALL|ORDER|SKIP|LIMIT)\b",
    re.IGNORECASE | re.DOTALL,
)
_NUMBER_RE = re.compile(r"^-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?$")


class CommandCallRefused(ValueError):
    """A command call that cannot be honoured as written — said by name, never run otherwise."""


class ImplicitArguments(dict):
    """The request's parameters of a ``CALL name`` written without parentheses: each of the
    command's arguments is bound from the one of the same name (REQ-2001)."""


@dataclass(frozen=True)
class CommandCall:
    name: str
    # positional, in the order written; or, for a call without parentheses, the parameters its
    # arguments are bound from by name
    values: list[Any] | ImplicitArguments
    yields: list[tuple[str, str]] | None  # (column, alias); None = no YIELD (every column)
    returns: list[tuple[str, str]] | None  # (name, alias); None = no RETURN
    yields_written: bool  # a YIELD follows the call (``YIELD *`` included)


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
    if tok.startswith("{") and tok.endswith("}"):
        # A map of values or $parameters ({sample: $SAMPLE}), as LangChain passes apoc.meta.data.
        out: dict[str, Any] = {}
        for entry in _split_top_level(tok[1:-1]):
            key, sep, value = entry.partition(":")
            key = key.strip().strip("`")
            if not sep or not re.fullmatch(r"[A-Za-z_]\w*", key):
                raise CommandCallRefused(
                    f"CALL {name}: argument {position} is a map whose entry {entry.strip()!r} "
                    "is not key: value"
                )
            out[key] = _value(value, params, name, position)
        return out
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
    implicit = _IMPLICIT_HEAD_RE.match(text) if head is None else None
    if head is None and implicit is None:
        return None
    name = (head or implicit).group(1)  # type: ignore[union-attr]
    if name.lower().startswith(_ANSWERED_ELSEWHERE):
        return None
    if implicit is not None:
        rest = text[implicit.end() :]
        if _COMPOSED_RE.match(rest):
            return None  # composed in a statement: the translator refuses it there
        values: list[Any] | ImplicitArguments = ImplicitArguments(params)
    else:
        assert head is not None
        close = _scan_parens(text, head.end())
        rest = text[close + 1 :]
        if _COMPOSED_RE.match(rest):
            return None  # REQ-2168: composed in a statement; the translator reads its arguments
        values = [
            _value(tok, params, name, i + 1)
            for i, tok in enumerate(_split_top_level(text[head.end() : close]))
        ]
    yields: list[tuple[str, str]] | None = None
    returns: list[tuple[str, str]] | None = None
    m = _YIELD_RE.match(rest)
    yields_written = m is not None
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
    return CommandCall(
        name=name, values=values, yields=yields, returns=returns, yields_written=yields_written
    )


def command_signature(cmd: dict) -> str:
    """A listed command's Neo4j-style procedure signature, the same on Bolt and Cypher over HTTP
    (REQ-1156): ``name(arg [= default] :: TYPE) :: (MAP)``, ``(LIST OF MAP)`` for one answering a
    table, ``()`` for a VOID one (REQ-2001). ``cmd`` is an entry of
    ``action_exec.list_visible_commands``."""
    import json

    args = ", ".join(
        f"{a['name']}"
        # REQ-2168: an argument that declares a default, written as Neo4j writes one.
        + (f" = {json.dumps(a['default'], default=str)}" if "default" in a else "")
        + f" :: {str(a.get('type', 'String')).upper()}"
        for a in cmd["arguments"]
    )
    if cmd["void"]:  # REQ-2001: a VOID command answers nothing
        return f"{cmd['name']}({args}) :: ()"
    ret = "LIST OF MAP" if cmd["set_returning"] else "MAP"
    return f"{cmd['name']}({args}) :: ({ret})"


def check_yields(call: CommandCall, void: bool) -> None:
    """A standalone call yields what its command answers: a VOID command nothing, so a YIELD
    after its call, ``YIELD *`` included, is refused naming VOID (REQ-2001), as it is in a
    statement (api/data/cypher_commands.py)."""
    if void and call.yields_written:
        raise CommandCallRefused(
            f"YIELD after CALL {call.name}: it is VOID (it declares no outputs), so it yields "
            "nothing"
        )


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
