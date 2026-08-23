# Copyright (c) 2026 Kenneth Stott
# Canary: 7bf6cae6-0499-4de9-85eb-f6eb4e80099d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a mutation may be done TO: the domain gate (REQ-1530, REQ-1531).

``require_capability`` answers what KIND of act a member may perform. This module answers which
objects those acts may touch, for the mutations whose object carries a domain but does not hand one
to the gate directly:

- an object identified only by its id (a table id, a metric name) — its domain is looked up;
- a VIEW, which has no domain until it is registered into one, and whose SQL names the tables it
  reads — so both the domain registered into AND every table read must be within the caller's
  domains, or a member of one domain registers a view over another domain's data and the data
  arrives somewhere its owner does not govern.

Parsing FAILS CLOSED here, unlike the relationship gate: an authorization question about SQL nobody
can parse has no safe affirmative answer.
"""

# Requirements: REQ-1530, REQ-1531

from __future__ import annotations

from typing import TYPE_CHECKING, Iterable

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from provisa.api.admin.capabilities import require_domain

if TYPE_CHECKING:
    from strawberry.types.info import Info as StrawberryInfo

    from provisa.core.database import Connection


class DomainLookupError(LookupError):
    """The object a mutation names does not exist, so its domain cannot be established."""


async def table_domain(conn: "Connection", table_id: int) -> str:
    """The domain of a registered table. ``registered_tables.domain_id`` is NOT NULL, so a row that
    exists has one; a row that does not exist is a caller naming an object that is not there."""
    from provisa.core.repositories import table as table_repo

    row = await table_repo.get(conn, table_id)
    if row is None:
        raise DomainLookupError(f"no registered table with id {table_id}")
    return row["domain_id"]


async def table_domain_by_name(conn: "Connection", table_name: str) -> str | None:
    """The domain of a registered table named by its VIRTUAL name, or ``None`` if no such table.

    ``None`` rather than a raise because the callers that name a table this way (an RLS rule) have
    their own answer for an unregistered name, and it is not an authorization answer.
    """
    from provisa.core.repositories import table as table_repo

    row = await table_repo.find_by_table_name(conn, table_name)
    return None if row is None else row["domain_id"]


async def require_table_domain(info: "StrawberryInfo", conn: "Connection", table_id: int) -> None:
    """Gate an act on a table the caller named by id (REQ-1531)."""
    require_domain(info, await table_domain(conn, table_id))


def require_domains(info: "StrawberryInfo", domain_ids: Iterable[str]) -> None:
    """Gate an act that touches several domains: every one of them must be the caller's.

    An act reaching two domains is not half-permitted — a view reading a table it may not read is
    refused whichever other tables it also reads.
    """
    for domain_id in sorted(set(domain_ids)):
        require_domain(info, domain_id)


def referenced_table_names(view_sql: str, dialect: str | None = None) -> set[str]:
    """The names a view's SQL READS, with CTE names excluded — a CTE is defined in the statement
    rather than read from the model, so it names no governed object.

    Raises ``ValueError`` on unparseable SQL: the caller is asking whether this view may be
    registered, and "the tables are unknown" is a refusal, not an empty answer.
    """
    try:
        statement = sqlglot.parse_one(view_sql, dialect=dialect)
    except SqlglotError as e:
        raise ValueError(f"view SQL could not be parsed, so the tables it reads are unknown: {e}")
    if statement is None:
        raise ValueError("view SQL could not be parsed, so the tables it reads are unknown")
    defined = {cte.alias_or_name for cte in statement.find_all(exp.CTE)}
    return {t.name for t in statement.find_all(exp.Table) if t.name and t.name not in defined}


async def view_read_domains(
    conn: "Connection", view_sql: str, dialect: str | None = None
) -> set[str]:
    """The domains a view's SQL reads from.

    A name the model does not register is not governed by a domain and contributes none — it is
    rejected by registration itself, which is where an unknown table is that mutation's problem.
    """
    from provisa.core.repositories import table as table_repo

    domains: set[str] = set()
    for name in referenced_table_names(view_sql, dialect):
        row = await table_repo.find_by_table_name(conn, name)
        if row is not None:
            domains.add(row["domain_id"])
    return domains


async def require_view_within_domains(
    info: "StrawberryInfo",
    conn: "Connection",
    view_sql: str,
    dialect: str | None = None,
) -> None:
    """Both halves of the view gate (REQ-1531): every table the SQL reads is the caller's.

    The domain the view is REGISTERED INTO is gated by the registration itself; this is the half
    that would otherwise be missing, because free-hand SQL names its tables directly.
    """
    require_domains(info, await view_read_domains(conn, view_sql, dialect))
