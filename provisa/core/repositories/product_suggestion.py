# Copyright (c) 2026 Kenneth Stott
# Canary: 6c1e9a42-8d37-4b05-a2f9-3e7d0b5c9a18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Suggestions of a data product from listed tables (REQ-2180, amended 2026-10-28, SUGGEST A
PRODUCT FROM A LISTED TABLE).

A listed table is not requested directly. A person who finds one they need (REQ-2153) suggests a
product: the suggestion names the table, or several tables of the same domain, states its reason,
and goes to the domain's data product owners and data stewards. Suggestions are counted per table,
so a domain sees which of its listed tables are most wanted; the counts and each table's
suggestions are read by the domain's stewards and data product owners, the people they go to
(amended 2026-11-24).

A table is named as the catalog lists it to its reader: its domain and its table name. A reader
suggests only from a table they see -- one with a listed (or public) column, or one in a domain
they reach -- and any other table is refused by name, whether it exists or not, so a refusal tells
nothing the catalog does not.
"""

# Requirements: REQ-2180, REQ-2153

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import func, insert, select

from provisa.core.column_scope import SCOPE_LISTED, SCOPE_PUBLIC
from provisa.core.schema_org import (
    product_suggestion_tables,
    product_suggestions,
    registered_tables,
    table_columns,
)

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: The column scopes that put a table's name in every reader's catalog (provisa.core.column_scope).
LISTING_SCOPES = (SCOPE_LISTED, SCOPE_PUBLIC)


class SuggestionRefused(ValueError):
    """A suggestion that cannot be made, by a stable ``code``, a message naming why, and the
    ``params`` the UI words it with (serverErrors.product_suggestion)."""

    def __init__(self, code: str, message: str, *, status: int = 400, **params: str) -> None:
        super().__init__(message)
        self.code = code
        self.status = status
        self.params = params


@dataclass(frozen=True)
class SeenTable:
    id: int
    domain_id: str
    table_name: str


def _live():
    return (registered_tables.c.withdrawn.is_(False), registered_tables.c.retired.is_(False))


async def seen_tables(
    conn: "Connection",
    domain_id: str,
    table_names: Collection[str],
    reach: frozenset[str] | None,
) -> list[SeenTable]:
    """The tables named ``table_names`` in ``domain_id`` that the reader sees: every one, when
    the domain is within ``reach`` (None: every domain); else those holding a listed or public
    column. A name the reader does not see is refused by name."""
    wanted = sorted({n.strip() for n in table_names})
    if not wanted or "" in wanted:
        raise SuggestionRefused(
            "product_suggestion.tables_required", "a suggestion names at least one table"
        )
    query = select(
        registered_tables.c.id, registered_tables.c.domain_id, registered_tables.c.table_name
    ).where(
        registered_tables.c.domain_id == domain_id,
        registered_tables.c.table_name.in_(wanted),
        *_live(),
    )
    if reach is not None and domain_id not in reach:
        listing = select(table_columns.c.table_id).where(
            table_columns.c.scope.in_(LISTING_SCOPES), table_columns.c.retired.is_(False)
        )
        query = query.where(registered_tables.c.id.in_(listing))
    rows = (await conn.execute_core(query.order_by(registered_tables.c.id))).fetchall()
    found = {r.table_name for r in rows}
    unseen = [n for n in wanted if n not in found]
    if unseen:
        names = ", ".join(f"{domain_id}.{n}" for n in unseen)
        raise SuggestionRefused(
            "product_suggestion.table_not_listed",
            f"table {names} is not listed to you; a product is suggested from a table the "
            "catalog lists",
            status=404,
            tables=names,
        )
    return [SeenTable(r.id, r.domain_id, r.table_name) for r in rows]


async def record(
    conn: "Connection",
    *,
    domain_id: str,
    tables: list[SeenTable],
    suggested_by: str,
    reason: str,
    recipients: list[str],
) -> dict:
    """Store the suggestion, its tables, and the people it went to; the stored row, with its
    table names."""
    if not reason.strip():
        raise SuggestionRefused(
            "product_suggestion.reason_required", "a suggestion states its reason"
        )
    row = (
        await conn.execute_core(
            insert(product_suggestions)
            .values(
                domain_id=domain_id,
                suggested_by=suggested_by,
                reason=reason.strip(),
                recipients=recipients,
            )
            .returning(*product_suggestions.c)
        )
    ).one()
    for table in tables:
        await conn.execute_core(
            insert(product_suggestion_tables).values(suggestion_id=row.id, table_id=table.id)
        )
    return {**row._asdict(), "tables": sorted({t.table_name for t in tables})}


async def counts(conn: "Connection", domain_ids: Collection[str]) -> list[dict]:
    """How many suggestions name each table, for the tables of ``domain_ids``, most wanted
    first."""
    n = func.count(product_suggestion_tables.c.suggestion_id).label("suggestions")
    query = (
        select(
            registered_tables.c.id.label("table_id"),
            registered_tables.c.domain_id,
            registered_tables.c.table_name,
            n,
        )
        .select_from(
            product_suggestion_tables.join(
                registered_tables, registered_tables.c.id == product_suggestion_tables.c.table_id
            )
        )
        .group_by(
            registered_tables.c.id, registered_tables.c.domain_id, registered_tables.c.table_name
        )
        .order_by(n.desc(), registered_tables.c.domain_id, registered_tables.c.table_name)
        .where(registered_tables.c.domain_id.in_(sorted(domain_ids)))
    )
    return [r._asdict() for r in (await conn.execute_core(query)).fetchall()]


async def of_table(conn: "Connection", domain_id: str, table_name: str) -> list[dict]:
    """The suggestions naming the table ``table_name`` of ``domain_id``, newest first, each with
    every table it names."""
    named = (
        select(product_suggestion_tables.c.suggestion_id)
        .join(registered_tables, registered_tables.c.id == product_suggestion_tables.c.table_id)
        .where(
            registered_tables.c.domain_id == domain_id,
            registered_tables.c.table_name == table_name,
        )
    )
    rows = (
        await conn.execute_core(
            select(*product_suggestions.c)
            .where(product_suggestions.c.id.in_(named))
            .order_by(product_suggestions.c.created_at.desc(), product_suggestions.c.id.desc())
        )
    ).fetchall()
    if not rows:
        return []
    tables = (
        await conn.execute_core(
            select(product_suggestion_tables.c.suggestion_id, registered_tables.c.table_name)
            .join(registered_tables, registered_tables.c.id == product_suggestion_tables.c.table_id)
            .where(product_suggestion_tables.c.suggestion_id.in_([r.id for r in rows]))
        )
    ).fetchall()
    names: dict[int, set[str]] = {r.id: set() for r in rows}
    for t in tables:
        names[t.suggestion_id].add(t.table_name)
    return [{**r._asdict(), "tables": sorted(names[r.id])} for r in rows]
