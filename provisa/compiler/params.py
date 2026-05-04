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

import re as _re


def _sql_literal(val: object) -> str:
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
_PARAM_RE = _re.compile(
    r"\$(\d+)=(NULL|TRUE|FALSE|-?\d+(?:\.\d+)?|'(?:[^']|'')*')"
)


def embed_params_comment(sql: str, params: list) -> str:
    """Prepend a provisa-params comment so the SQL is self-contained and executable."""
    if not params:
        return sql
    parts = ", ".join(
        f"${i + 1}={_sql_literal(v)}" for i, v in enumerate(params)
    )
    return f"{_COMMENT_PREFIX} {parts}\n{sql}"


def extract_params_comment(sql: str) -> tuple[str, list]:
    """Strip the provisa-params comment and return (sql, params_list).

    Searches all lines — the comment may be embedded inside a subquery wrapper
    added by the UI (e.g. SELECT * FROM (<comment>\n...) _sample LIMIT N).
    """
    lines = sql.split("\n")
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped.startswith(_COMMENT_PREFIX):
            continue
        matches = _PARAM_RE.findall(stripped)
        remaining = "\n".join(lines[:i] + lines[i + 1:])
        if not matches:
            return remaining, []
        indexed = sorted((int(idx), _parse_sql_literal(val)) for idx, val in matches)
        return remaining, [v for _, v in indexed]
    return sql, []


class ParamCollector:
    """Collects parameter values and returns positional placeholders ($1, $2, ...)."""

    def __init__(self) -> None:
        self._params: list = []

    def add(self, value: object) -> str:
        """Add a parameter value and return its placeholder string."""
        self._params.append(value)
        return f"${len(self._params)}"

    @property
    def params(self) -> list:
        return list(self._params)
