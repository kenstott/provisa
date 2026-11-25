# Copyright (c) 2026 Kenneth Stott
# Canary: 3b8f6d21-94c7-4e0a-b5d2-6c1e7a9f4d83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's format() on DuckDB (varlena.c's text_format), whose own format of the name reads
fmt's {} and answers NULL of any NULL value. format is not strict in its values: %s of a NULL is
the empty text, %L of one is NULL unquoted, and %I of one raises 22004 (REQ-2236)."""

# Requirements: REQ-2236

from __future__ import annotations

import re

from provisa.core.pg_error import END, MARK

#: An identifier quote_ident leaves bare.
_BARE = re.compile(r"[a-z_][a-z0-9_$]*")


def _error(sqlstate: str, message: str) -> Exception:
    return ValueError(f"{MARK} {sqlstate}: {message}{END}")


def _quote_ident(text: str) -> str:
    return text if _BARE.fullmatch(text) else '"' + text.replace('"', '""') + '"'


def _quote_literal(text: str) -> str:
    quoted = "'" + text.replace("'", "''").replace("\\", "\\\\") + "'"
    return "E" + quoted if "\\" in text else quoted


class _Template:
    """One template read left to right, its values taken in order or by position."""

    def __init__(self, template: str, values: list[str | None]) -> None:
        self.template = template
        self.values = values
        self.at = 0
        self.next_value = 0

    def _unterminated(self) -> Exception:
        return _error("22023", "unterminated format() type specifier")

    def _peek(self) -> str:
        if self.at >= len(self.template):
            raise self._unterminated()
        return self.template[self.at]

    def _digits(self) -> int | None:
        start = self.at
        while self.at < len(self.template) and self.template[self.at].isdigit():
            self.at += 1
        return int(self.template[start : self.at]) if self.at > start else None

    def _value(self, position: int | None) -> str | None:
        index = self.next_value if position is None else position - 1
        if index >= len(self.values):
            raise _error("22023", "too few arguments for format()")
        self.next_value = index + 1
        return self.values[index]

    def _position(self) -> int | None:
        """An n$ at the cursor, else None with the cursor where it was."""
        start = self.at
        number = self._digits()
        if number is not None and self.at < len(self.template) and self.template[self.at] == "$":
            if number == 0:
                raise _error(
                    "22023", "format specifies argument 0, but arguments are numbered from 1"
                )
            self.at += 1
            return number
        self.at = start
        return None

    def _width(self) -> tuple[int, bool]:
        """The width and whether it is left-justified: digits, or * of a value (n$ or the next)."""
        left = False
        while self._peek() == "-":
            left = True
            self.at += 1
        if self._peek() != "*":
            return self._digits() or 0, left
        self.at += 1
        given = self._value(self._position())
        width = 0 if given is None else int(given)
        return abs(width), left or width < 0

    def specifier(self) -> str:
        self.at += 1  # the %
        if self._peek() == "%":
            self.at += 1
            return "%"
        position = self._position()
        width, left = self._width()
        kind = self._peek()
        self.at += 1
        if kind not in "sIL":
            raise _error("22023", f'unrecognized format() type specifier "{kind}"')
        value = self._value(position)
        if value is None and kind == "I":
            raise _error("22004", "null values cannot be formatted as an SQL identifier")
        written = {
            "s": "" if value is None else value,
            "I": "" if value is None else _quote_ident(value),
            "L": "NULL" if value is None else _quote_literal(value),
        }[kind]
        return written.ljust(width) if left else written.rjust(width)

    def written(self) -> str:
        pieces = []
        while self.at < len(self.template):
            if self.template[self.at] == "%":
                pieces.append(self.specifier())
            else:
                pieces.append(self.template[self.at])
                self.at += 1
        return "".join(pieces)


def pg_format(template: str | None, values: list[str | None] | None) -> str | None:
    """format(template, VARIADIC values): NULL of a NULL template, each value as its text."""
    if template is None:
        return None
    return _Template(template, values or []).written()
