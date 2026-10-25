# Copyright (c) 2026 Kenneth Stott
# Canary: 6e8edd5f-720d-4b2d-82ee-948eb9b1c6e7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What of the model an admin-API caller may be told (REQ-2152).

A capability opens a feature; what the feature names stays within the caller's grants. The
catalog itself is narrowed by :mod:`provisa.api.admin.catalog_scope`; this module answers the
same question for the things a feature NAMES while doing something else -- the dependents a
column rename would break (REQ-1484), the objects a refused delete is still referred to by
(REQ-1918):

- a table, view or materialized view: the acting role is served a column of it;
- a column, a relationship end, a tag on a column: the acting role is served that column;
- a metric or a command: it is granted to the acting role;
- a glossary term: the caller's domains read it.

Anything else a refusal names (a domain, a source, a role, a region...) is not a field, table,
column or check, and is named as before.
"""

# Requirements: REQ-2152, REQ-1484, REQ-1918

from __future__ import annotations

import types
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

if TYPE_CHECKING:
    from provisa.core.database import Connection
    from provisa.core.repositories.integrity import Dependent


@dataclass(frozen=True)
class DependentsReader:  # REQ-2152
    """What the caller may read: a dependent it cannot read is left out of the answer, whatever
    right opened the feature.

    ``column(table_id, column)``: the caller is served that column (``None``: any column of the
    table); ``metric(row)``: the metric (its stored row) is granted to it; ``term(domains)``: a term
    scoped to ``domains`` is readable to it; ``command(name)``: the command is assigned to it."""

    column: Callable[[int, str | None], bool]
    metric: Callable[[dict], bool]
    term: Callable[[set[str]], bool]
    command: Callable[[str], bool]


#: Provisa's own identity (REQ-2152's second exception: builds, fakes, checkers), which reads
#: everything and shows nothing to a person.
SERVICE_READER = DependentsReader(
    column=lambda table_id, column: True,
    metric=lambda row: True,
    term=lambda domains: True,
    command=lambda name: True,
)


def reader_for(info: Any) -> DependentsReader:
    """The reader of the caller behind a GraphQL ``info``: the columns its acting role is served,
    the metrics and commands granted to it, the terms its domains read."""
    from provisa.api.admin.capabilities import allowed_domains_request
    from provisa.api.admin.catalog_scope import catalog_scope
    from provisa.api.admin.schema_query import _sees_metric
    from provisa.api.app import state
    from provisa.core.glossary import readable_term
    from provisa.security.meta_role import acting_roles
    from provisa.security.mutation_authz import command_reachable

    scope = catalog_scope(info)
    context = info.context
    request = context.get("request") if isinstance(context, dict) else context.request
    allowed = None if scope.whole else allowed_domains_request(request)

    def _command(name: str) -> bool:
        if scope.whole:
            return True
        command = (getattr(state, "tracked_functions", None) or {}).get(name)
        if command is None or scope.role is None:
            return False
        return any(
            command_reachable(command, {"id": r, **state.roles[r]})
            for r in acting_roles(state, scope.role)
            if r in state.roles
        )

    return DependentsReader(
        column=scope.lists_column,
        metric=lambda row: _sees_metric(info, row),
        term=lambda domains: scope.whole or readable_term(allowed, domains),
        command=_command,
    )


def reader_for_request(request: Any) -> DependentsReader:
    """:func:`reader_for` for a REST handler's ``request``."""
    return reader_for(types.SimpleNamespace(context={"request": request}))


async def _tables_named(conn: "Connection", names: list[str]) -> list[int]:
    from provisa.core.schema_org import registered_tables

    if not names:
        return []
    res = await conn.execute_core(
        select(registered_tables.c.id).where(registered_tables.c.table_name.in_(names))
    )
    return [r.id for r in res.fetchall()]


async def _readable(conn: "Connection", reader: DependentsReader, dependent: "Dependent") -> bool:
    from provisa.core.repositories.glossary import term_domains
    from provisa.core.schema_org import (
        materialized_views,
        metrics,
        rls_rules,
        table_columns,
        tag_assignments,
    )

    kind, key = dependent.ref.kind, dependent.ref.id
    if kind == "table":
        return reader.column(int(key), None)
    if kind == "column":
        row = (
            await conn.execute_core(
                select(table_columns.c.table_id, table_columns.c.column_name).where(
                    table_columns.c.id == key
                )
            )
        ).fetchone()
        return row is not None and reader.column(row.table_id, row.column_name)
    if kind == "relationship":
        return await relationship_readable(conn, reader, str(key))
    if kind == "metric":
        row = (await conn.execute_core(select(metrics).where(metrics.c.name == key))).fetchone()
        return row is not None and reader.metric(dict(row._mapping))
    if kind == "command":
        return reader.command(str(key))
    if kind == "glossary_term":
        return reader.term((await term_domains(conn)).get(int(key), set()))
    if kind == "row_filter":
        row = (
            await conn.execute_core(select(rls_rules.c.table_id).where(rls_rules.c.id == key))
        ).fetchone()
        return row is not None and (row.table_id is None or reader.column(row.table_id, None))
    if kind == "tag_assignment":
        row = (
            await conn.execute_core(
                select(tag_assignments.c.table_id, tag_assignments.c.column_name).where(
                    tag_assignments.c.id == key
                )
            )
        ).fetchone()
        return row is not None and (
            row.table_id is None or reader.column(row.table_id, row.column_name)
        )
    if kind == "materialized_view":
        # A view's storage row is kept under ``view-<table name>`` (integrity._attributes); any
        # other materialized view is named by the tables it reads.
        if str(key).startswith("view-"):
            names = [str(key)[len("view-") :]]
        else:
            row = (
                await conn.execute_core(
                    select(materialized_views.c.source_tables).where(materialized_views.c.id == key)
                )
            ).fetchone()
            names = list(row.source_tables or []) if row is not None else []
        tables = await _tables_named(conn, names)
        return bool(tables) and all(reader.column(t, None) for t in tables)
    return True


async def relationship_readable(conn: "Connection", reader: DependentsReader, rel_id: str) -> bool:
    """Whether ``reader`` is served every column relationship ``rel_id`` joins on, and the table
    it goes through; a relationship that does not exist is not readable."""
    from provisa.core.schema_org import relationships

    row = (
        await conn.execute_core(select(relationships).where(relationships.c.id == rel_id))
    ).fetchone()
    if row is None:
        return False
    ends = [
        (row.source_table_id, row.source_column),
        (row.target_table_id, row.target_column),
        (row.via_table_id, None),
    ]
    return all(
        reader.column(table_id, c)
        for table_id, column in ends
        if table_id is not None
        # A composite key lists its columns comma-separated (REQ-1586).
        for c in ([part.strip() for part in column.split(",")] if column else [None])
    )


async def readable_dependents(
    conn: "Connection", reader: DependentsReader, dependents: "list[Dependent]"
) -> "list[Dependent]":
    """The ``dependents`` ``reader`` may be told of (REQ-2152)."""
    return [d for d in dependents if await _readable(conn, reader, d)]


def referred_text(lead: str, readable: "list[Dependent]") -> str:
    """A refusal's message (REQ-1918, REQ-2152): ``lead`` and the dependents its caller may read,
    or, when it may read none of them, that what still refers to the object is outside its grants
    -- the refusal stands either way, naming and counting nothing else."""
    if not readable:
        return f"{lead} objects outside your grants"
    return f"{lead}: " + ", ".join(f"{d.ref.kind} {d.name or d.ref.id}" for d in readable)


async def refusal(
    conn: "Connection", reader: DependentsReader, refused: Any
) -> tuple[str, list[dict]]:
    """A delete refusal (REQ-1918) as ``reader`` is answered it (REQ-2152): its message and the
    dependents it lists, only those ``reader`` may read and never how many there are. A refusal
    with no dependents (a system object) is answered as raised. ``conn`` is the connection the
    refused delete ran on."""
    if not refused.dependents:
        return str(refused), []
    readable = await readable_dependents(conn, reader, refused.dependents)
    return referred_text(refused.lead, readable), [d.as_dict() for d in readable]


async def readable_columns_report(
    conn: "Connection", reader: DependentsReader, columns: "dict[str, list[Dependent]]"
) -> dict[str, list[dict]]:
    """A refused column drop's report (REQ-1918) as ``reader`` is answered it (REQ-2152): each
    column with only the dependents ``reader`` may read."""
    return {
        column: [d.as_dict() for d in await readable_dependents(conn, reader, dependents)]
        for column, dependents in columns.items()
    }


def column_drop_text(table_name: str, report: dict[str, list[dict]]) -> str:
    """A refused column drop's message, naming per column only the readable dependents in
    ``report`` (:func:`readable_columns_report`), or that they are outside the caller's grants."""
    named = "; ".join(
        f"{column} (referred to by"
        + (
            ": " + ", ".join(f"{d['kind']} {d['name'] or d['id']}" for d in dependents)
            if dependents
            else " objects outside your grants"
        )
        + ")"
        for column, dependents in report.items()
    )
    return f"Table {table_name!r} cannot drop column(s) that are still referred to: {named}"


async def served_columns_of(conn: "Connection", scope: Any, table_id: int) -> set[str] | None:
    """The physical columns of table ``table_id`` the caller behind ``scope`` (its
    :class:`~provisa.api.admin.catalog_scope.CatalogScope`) is served; None when nothing narrows
    it. A table no schema build compiled yet is judged by the rule over its stored grants
    (``CatalogScope.answered_columns``), read here (REQ-2152)."""
    from provisa.core.schema_org import registered_tables, table_columns

    if scope.whole:
        return None
    if table_id in scope.compiled or table_id in scope.served:
        return scope.served.get(table_id, set())
    domain = (
        await conn.execute_core(
            select(registered_tables.c.domain_id).where(registered_tables.c.id == table_id)
        )
    ).fetchone()
    if domain is None:
        return set()
    columns = await conn.execute_core(
        select(
            table_columns.c.column_name, table_columns.c.visible_to, table_columns.c.scope
        ).where(table_columns.c.table_id == table_id)
    )
    return scope.stored_columns(
        {
            "id": table_id,
            "domain_id": domain.domain_id,
            "columns": [dict(r._mapping) for r in columns.fetchall()],
        }
    )


async def served_columns_request(
    conn: "Connection", request: Any, table_id: int
) -> set[str] | None:
    """:func:`served_columns_of` for a REST handler's ``request``."""
    from provisa.api.admin.catalog_scope import catalog_scope

    return await served_columns_of(
        conn, catalog_scope(types.SimpleNamespace(context={"request": request})), table_id
    )


async def serves_every_column(conn: "Connection", scope: Any, table_id: int) -> bool:
    """Whether the caller behind ``scope`` is served every stored column of ``table_id``."""
    from provisa.core.schema_org import table_columns

    served = await served_columns_of(conn, scope, table_id)
    if served is None:
        return True
    stored = await conn.execute_core(
        select(table_columns.c.column_name).where(table_columns.c.table_id == table_id)
    )
    return all(r.column_name in served for r in stored.fetchall())


def role_within(state: Any, scope: Any, role: str) -> bool:
    """Whether every column ``role`` is served is served to the caller behind ``scope`` too: the
    roles a caller may hand its reads to (REQ-2152, ruling Q8 -- what you cannot read, you cannot
    give). A caller nothing narrows holds every role's reads."""
    from provisa.api.flight.catalog import role_visibility

    if scope.whole:
        return True
    return all(
        columns <= scope.served.get(table_id, set())
        for table_id, columns in role_visibility(state, role).items()
    )
