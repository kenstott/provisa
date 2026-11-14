# Copyright (c) 2026 Kenneth Stott
# Canary: 4c7e2a91-d3b8-4f05-a6e1-9b2d58c0f7a3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A regulation's check on a column's values, kept as a data quality rule that cites the library
section it came from (REQ-2245).

The rule is one Soda ``failed_rows`` check of a checker table that scans the column's table: its
``expression`` is the condition, and its ``qualifier`` -- not its position in the contract, which
changes when a check is added before it -- is its identity. The citation is a
``reference_citations`` row of kind ``dq_check`` (tag id NULL) whose object key names the checker
table, the qualifier, the target table and the column (:func:`check_object_key`). A check that
leaves its contract takes its citation with it (:func:`uncite_removed`), in the transaction that
removed it.
"""

# Requirements: REQ-2245

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

import sqlglot
import sqlglot.expressions as exp
import yaml
from sqlalchemy import select
from sqlglot.errors import SqlglotError

from provisa.core.landing_filter import NotOverColumns, columns_read
from provisa.dq.catalog import build_check_definition
from provisa.dq.contract import contract_checks
from provisa.reference_library import citations

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: The one checker dialect a regulation check is kept in: Soda has ``failed_rows`` and qualifiers.
SODA = "soda"
_SEPARATOR = "|"
_QUALIFIER_PREFIX = "regulation_"


@dataclass(frozen=True)
class CheckKey:
    """What a ``dq_check`` citation's object key names."""

    checker_table_id: int
    qualifier: str
    table_id: int
    column: str


def check_object_key(key: CheckKey) -> str:
    """``<checker_table_id>|<qualifier>|<target table_id>|<column>``: the citation's object key,
    read back by :func:`parse_check_object_key`. The column is last, so it may hold the separator;
    the qualifier may not."""
    if _SEPARATOR in key.qualifier or not key.qualifier:
        raise ValueError(f"a check's qualifier must be non-empty without {_SEPARATOR!r}")
    if not key.column:
        raise ValueError("a check's column must be named")
    return _SEPARATOR.join(
        (str(key.checker_table_id), key.qualifier, str(key.table_id), key.column)
    )


def parse_check_object_key(object_key: str) -> CheckKey:
    """The :class:`CheckKey` a ``dq_check`` citation's object key names; :class:`ValueError` for a
    key :func:`check_object_key` did not make."""
    parts = object_key.split(_SEPARATOR, 3)
    if len(parts) != 4 or not parts[1] or not parts[3]:
        raise ValueError(f"not a data quality check's object key: {object_key!r}")
    checker, qualifier, table, column = parts
    return CheckKey(int(checker), qualifier, int(table), column)


def regulation_qualifier(table_id: int, column: str, condition: str, cite_key: str) -> str:
    """The qualifier of the check keeping ``condition`` on ``column`` of table ``table_id``, citing
    the place ``cite_key`` names: the same for the same rule, distinct for another."""
    digest = hashlib.sha256(
        "\x1f".join((str(table_id), column, condition, cite_key)).encode()
    ).hexdigest()
    return f"{_QUALIFIER_PREFIX}{digest[:16]}"


def regulation_check(qualifier: str, condition: str) -> dict:
    """The Soda ``failed_rows`` check row (dataset scope) failing on the rows meeting
    ``condition``."""
    definition = build_check_definition(
        SODA, "failed_rows", params={"qualifier": qualifier, "expression": condition}
    )
    return {"column_name": "", "check_type": "failed_rows", "definition": definition}


def held_qualifiers(contract: str) -> set[str]:
    """The qualifiers of the ``failed_rows`` checks a Soda contract holds."""
    held = set()
    for check in contract_checks(contract, SODA):
        if check["check_type"] != "failed_rows" or not check["definition"]:
            continue
        body = yaml.safe_load(check["definition"])
        # A qualifier is optional in Soda: a failed_rows check without one is no kept rule.
        if isinstance(body, dict) and isinstance(body.get("qualifier"), str):
            held.add(body["qualifier"])
    return held


async def uncite_removed(conn: Connection, checker_table_id: int) -> list[str]:
    """Take off every ``dq_check`` citation of checker table ``checker_table_id`` whose check its
    contract no longer holds -- all of them when the table is gone, holds no contract or is no
    Soda checker. Returns the object keys uncited."""
    from provisa.core.schema_org import reference_citations as citations_t
    from provisa.core.schema_org import registered_tables as rt
    from provisa.core.schema_org import sources

    row = (
        await conn.execute_core(
            select(rt.c.dq_contract, sources.c.type)
            .join(sources, sources.c.id == rt.c.source_id)
            .where(rt.c.id == checker_table_id)
        )
    ).fetchone()
    held = (
        held_qualifiers(row.dq_contract)
        if row is not None and row.dq_contract and row.type == SODA
        else set()
    )
    keys = (
        await conn.execute_core(
            select(citations_t.c.object_key)
            .where(
                citations_t.c.citer_kind == citations.DQ_CHECK,
                citations_t.c.object_key.startswith(f"{checker_table_id}{_SEPARATOR}"),
            )
            .distinct()
        )
    ).fetchall()
    gone = sorted(
        r.object_key for r in keys if parse_check_object_key(r.object_key).qualifier not in held
    )
    for key in gone:
        await citations.uncite_all(conn, citations.DQ_CHECK, None, key)
    return gone


def not_plain_text(column: str, readers: list[tuple[str, str]]) -> str:
    """A refusal naming, for each reader that does not read ``column`` in plain, how it reads it:
    ``readers`` is ``(who, state)`` pairs. Names states, never a value."""
    named = "; ".join(f"{state} for {who}" for who, state in readers)
    return (
        f"column {column!r} must be read in plain by you, by the checker's user and by every "
        f"role that sees the checker's results, but it is {named}"
    )


class ConditionUnreadable(ValueError):
    """A condition that cannot be parsed as a SQL condition."""


def condition_refusal(
    condition: str, column: str, is_the_column: Callable[[exp.Column], bool]
) -> str | None:
    """Why ``condition`` may not be kept as a check on ``column``, or None when it reads that
    column alone: it holds a subquery, names a table, or reads a column ``is_the_column`` does not
    take for it. Parsed as the SQL path parses (sqlglot, PostgreSQL); :class:`ConditionUnreadable`
    when it cannot be. Names columns, never a value."""
    try:
        tree = sqlglot.parse_one(condition, read="postgres")
    except SqlglotError:
        raise ConditionUnreadable(
            f"the condition on column {column!r} cannot be parsed as a SQL condition"
        ) from None
    try:
        read = columns_read(tree)
    except NotOverColumns:
        return (
            f"the condition may read only column {column!r}, but it holds a subquery or names "
            "a table"
        )
    others = sorted({c.sql(dialect="postgres") for c in read if not is_the_column(c)})
    if not others:
        return None
    return f"the condition may read only column {column!r}, but it also reads {', '.join(others)}"
