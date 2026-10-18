# Copyright (c) 2026 Kenneth Stott
# Canary: 17dfec27-410e-448f-bd3f-f9e256dc816c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table's landing filter (REQ-2062): a SQL predicate over its own columns, applied when its
replica lands, so the replica holds only the rows that pass it.

It is written in Provisa's SQL (the PostgreSQL dialect) and read in the dialect of whatever reads
the source for the build -- the engine, or the source's own driver. It names only the table's
own columns: no subquery and no other table, since it is evaluated against the source alone."""

# Requirements: REQ-2062

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import ParseError


def _parsed(table_name: str, predicate: str) -> exp.Expr:
    try:
        return sqlglot.parse_one(predicate, read="postgres")
    except ParseError as exc:
        raise ValueError(
            f"table {table_name!r}: landing_filter is not a SQL predicate: {exc}"
        ) from exc


def check(table_name: str, predicate: str, columns: list[str]) -> None:
    """Refuse, by name, a landing filter that is not a predicate over columns alone."""
    tree = _parsed(table_name, predicate)
    if isinstance(tree, (exp.Select, exp.Union)) or tree.find(exp.Select, exp.Table):
        raise ValueError(
            f"table {table_name!r}: landing_filter is a predicate over the table's own columns; "
            "it may hold no subquery and name no table"
        )
    named = {c.name for c in tree.find_all(exp.Column)}
    unknown = sorted(named - set(columns))
    if unknown:
        raise ValueError(
            f"table {table_name!r}: landing_filter names {', '.join(unknown)}, not columns of the "
            "table"
        )
    if not named:
        raise ValueError(f"table {table_name!r}: landing_filter names none of the table's columns")


def where(table: Any, dialect_of: Callable[[], str | None]) -> str:
    """The ``" WHERE ..."`` that applies ``table``'s landing filter in the dialect ``dialect_of``
    gives, or ``""`` when it has none (the dialect is then not asked for)."""
    predicate = table.landing_filter
    if predicate is None:
        return ""
    dialect = dialect_of()
    if not dialect:
        raise ValueError(
            f"table {table.table_name!r} has a landing_filter and the read that lands it has no "
            "SQL dialect to apply it in"
        )
    # Every identifier is quoted: a column named as a keyword of the reader's SQL (year, month,
    # date in Calcite's) is read as that keyword unquoted. ``check`` holds each one to a column's
    # own name, so the quoted name is the column's.
    return " WHERE " + _parsed(table.table_name, predicate).sql(dialect=dialect, identify=True)
