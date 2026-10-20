# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which predicates a code table's function takes as arguments (REQ-2100).

A ``@table`` or ``@stream`` function receives a predicate on one of its return columns through a
parameter, declared one of three ways, all with one vocabulary and resolved here alike:

- by NAME: a parameter named exactly as a return column (its raw declared name) takes an equality
  on that column; one named ``{op}_{column}`` takes that operator on the column;
- by TAG: a tag line ``@{op}_{column} parameter`` (or ``@{column} parameter``) in the docstring or
  JSDoc, or an inline ``/* @{op}_{column} */`` before a TypeScript parameter;
- by ``typing.Annotated``: a string ``"{op}_{column}"`` (or ``"{column}"``) on the parameter's hint.

``op`` is one of ne, gt, gte, lt, lte, in (the parameter a list of the column's type) and like (both
text). Every column name is the raw declared one: a name that matches only after Provisa's naming
conventions is not a match. A declaration the function's types cannot take is refused by name.
The predicate is still applied to the output after the call (the residual), so a function that
ignores or loosens a value it is given cannot return wrong rows.
"""

# Requirements: REQ-2100
from __future__ import annotations

import re
from dataclasses import dataclass

EQ = "eq"
#: The operators a ``{op}_{column}`` name or declaration may carry.
OPS = ("ne", "gt", "gte", "lt", "lte", "in", "like")
_PREFIXED = re.compile(rf"^({'|'.join(OPS)})_(.+)$")


@dataclass(frozen=True)
class Pushdown:
    """A predicate a parameter is given: ``column`` (raw) compared by ``op``."""

    column: str
    op: str


def parse(spelling: str, columns: frozenset[str]) -> Pushdown | None:
    """What ``spelling`` names among the raw ``columns``: a column itself is its equality, else
    ``{op}_{column}``; None when it names neither."""
    if spelling in columns:
        return Pushdown(spelling, EQ)
    match = _PREFIXED.match(spelling)
    if match and match.group(2) in columns:
        return Pushdown(match.group(2), match.group(1))
    return None


def type_problem(op: str, column_type: str, parameter_type: str, element_type: str | None) -> str:
    """Why a parameter of ``parameter_type`` (a list's ``element_type``) cannot take ``op`` on a
    column of ``column_type``; empty when it can. Types are IR names."""
    if op == "in":
        if parameter_type != "list" or element_type != column_type:
            return f"in on a {column_type} column takes a list of {column_type}"
        return ""
    if op == "like":
        if column_type != "text" or parameter_type != "text":
            return "like takes a text parameter on a text column"
        return ""
    if parameter_type != column_type:
        return f"{op} on a {column_type} column takes a {column_type}"
    return ""
