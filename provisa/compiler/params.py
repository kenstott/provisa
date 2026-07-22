# Copyright (c) 2026 Kenneth Stott
# Canary: bf3c93ad-012b-42f5-ab8f-4dd7d96cb416
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GraphQL variables to parameterized SQL. Never interpolates values."""

# Requirements: REQ-301, REQ-211

import re as _re


def _sql_literal(
    val: object,  # object-ok: accepts any SQL-serializable scalar (None, bool, int, float, str)
) -> str:
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    if isinstance(val, (int, float)):
        return str(val)
    return "'" + str(val).replace("'", "''") + "'"


def _parse_sql_literal(s: str) -> object:
    s = s.strip()
    if s == "NULL":
        return None
    if s == "TRUE":
        return True
    if s == "FALSE":
        return False
    try:
        return int(s)
    except ValueError:
        pass
    try:
        return float(s)
    except ValueError:
        pass
    if s.startswith("'") and s.endswith("'"):
        return s[1:-1].replace("''", "'")
    return s


_COMMENT_PREFIX = "-- provisa-params:"
# Matches $N=<value> where value is NULL/TRUE/FALSE, a number, or a single-quoted string
_PARAM_RE = _re.compile(r"\$(\d+)=(NULL|TRUE|FALSE|-?\d+(?:\.\d+)?|'(?:[^']|'')*')")


def _token_char_spans(sql: str) -> list[tuple[int, int]] | None:
    """Char spans covered by sqlglot tokens (string literals, identifiers, keywords, numbers, ...).

    A SQL comment is NOT a token, so a genuine ``--`` / ``/* */`` comment start falls OUTSIDE every
    span, while the SAME directive text sitting INSIDE a string literal falls INSIDE that STRING token's
    span. This is what lets directive extraction be parse-aware and refuse a directive smuggled in a
    literal (the parser-differential where governance/params were toggled by text the engine treats as
    a string). Returns None if the SQL cannot be tokenized — the caller then FAILS SAFE (an
    unconfirmable directive is not honored)."""
    import sqlglot

    try:
        toks = sqlglot.tokenize(sql, read="postgres")
    except Exception:
        return None
    return [(t.start, t.end) for t in toks]


def _inside_a_token(pos: int, spans: list[tuple[int, int]] | None) -> bool:
    """True if char offset ``pos`` lies within any token span (e.g. inside a string literal) — i.e. it
    is NOT a genuine top-level comment. None spans (untokenizable SQL) → treat as inside, so a
    governance/params directive we cannot confirm is a real comment is NOT honored (fail safe)."""
    if spans is None:
        return True
    return any(s <= pos <= e for s, e in spans)


def embed_params_comment(sql: str, params: list) -> str:
    """Prepend a provisa-params comment so the SQL is self-contained and executable."""
    if not params:
        return sql
    parts = ", ".join(f"${i + 1}={_sql_literal(v)}" for i, v in enumerate(params))
    return f"{_COMMENT_PREFIX} {parts}\n{sql}"


def extract_params_comment(sql: str) -> tuple[str, list]:  # REQ-603
    """Strip the provisa-params comment and return (sql, params_list).

    Searches all lines — the comment may be embedded inside a subquery wrapper
    added by the UI (e.g. SELECT * FROM (<comment>\n...) _sample LIMIT N).
    """
    spans = _token_char_spans(sql)
    lines = sql.split("\n")
    offset = 0
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith(_COMMENT_PREFIX):
            # Parse-aware: honor the directive ONLY when this ``--`` is a genuine comment, never when
            # the line lives inside a (multi-line) string literal — else a literal could inject params.
            prefix_pos = offset + (len(line) - len(line.lstrip()))
            if not _inside_a_token(prefix_pos, spans):
                matches = _PARAM_RE.findall(stripped)
                remaining = "\n".join(lines[:i] + lines[i + 1 :])
                if not matches:
                    return remaining, []
                indexed = sorted((int(idx), _parse_sql_literal(val)) for idx, val in matches)
                return remaining, [v for _, v in indexed]
        offset += len(line) + 1  # +1 for the '\n' consumed by split
    return sql, []


_RELATIONSHIP_GUARD_RE = _re.compile(r"--\s*relationship-guard\s*=\s*false", _re.IGNORECASE)


def extract_relationship_guard_comment(sql: str) -> tuple[str, bool]:  # REQ-603
    """Strip --relationship-guard=false comment and return (sql, opted_out).

    opted_out is True only when the comment is present. Both the role flag
    AND this comment must be present to bypass V002.
    """
    spans = _token_char_spans(sql)
    opted_out = False
    out_parts: list[str] = []
    last = 0
    for m in _RELATIONSHIP_GUARD_RE.finditer(sql):
        # Parse-aware: the same text inside a string literal is inert — only a real comment opts out
        # (and only then does the role flag decide whether V002 is actually bypassed).
        if _inside_a_token(m.start(), spans):
            continue
        opted_out = True
        # A ``--`` comment runs to end of line; strip from the directive start to EOL, preserving any
        # code BEFORE it on the same line (the old line-drop deleted that code too).
        eol = sql.find("\n", m.start())
        eol = len(sql) if eol == -1 else eol
        out_parts.append(sql[last : m.start()])
        last = eol
    out_parts.append(sql[last:])
    return "".join(out_parts), opted_out


class ParamCollector:
    """Collects parameter values and returns positional placeholders ($1, $2, ...)."""

    def __init__(self) -> None:
        self._params: list = []

    def add(
        self, value: object
    ) -> str:  # object-ok: accepts any SQL-serializable scalar (None, bool, int, float, str)
        """Add a parameter value and return its placeholder string."""
        self._params.append(value)
        return f"${len(self._params)}"

    @property
    def params(self) -> list:
        return list(self._params)
