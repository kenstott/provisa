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

from typing import TYPE_CHECKING, Any, Iterable

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


async def relationship_domains(conn: "Connection", rel_id: str) -> set[str] | None:
    """The domains of the tables a relationship takes part in — its two ends, and the table it
    goes through when it has one — or ``None`` when there is no such relationship. A
    relationship belongs to each of them: removing it changes what each table's readers can
    traverse, so an act on it is an act in every one of those domains (REQ-1531)."""
    from provisa.core.repositories import relationship as rel_repo

    row = await rel_repo.get(conn, rel_id)
    if row is None:
        return None
    table_ids = [row["source_table_id"], row["target_table_id"], row["via_table_id"]]
    return {await table_domain(conn, table_id) for table_id in table_ids if table_id is not None}


async def metric_domains(conn: "Connection", name: str) -> set[str] | None:
    """The domains of the tables a metric's expression reads, or ``None`` when there is no such
    metric. A metric has no domain of its own — its expression names semantic tables
    (``SUM(orders.amount)``) and it is an object of each domain those tables sit in (REQ-1531).
    A name the model does not register contributes none, as in :func:`view_read_domains`."""
    from provisa.core.repositories import metric as metric_repo

    row = await metric_repo.get(conn, name)
    if row is None:
        return None
    return await expression_domains(conn, name, row["expression"])


async def expression_domains(conn: "Connection", name: str, expression: str) -> set[str]:
    """The domains of the tables a metric EXPRESSION reads -- the question :func:`metric_domains`
    asks of a stored metric, asked of an expression that is about to be saved (REQ-1531)."""
    from provisa.compiler.metric_expand import metric_reference_tables
    from provisa.core.repositories import table as table_repo

    domains: set[str] = set()
    for table_name in metric_reference_tables(name, expression):
        table = await table_repo.find_by_table_name(conn, table_name)
        if table is not None:
            domains.add(table["domain_id"])
    return domains


#: The domain_access value meaning "every domain". A source that names no domains is open to all
#: of them, so an act on it is an act in all of them.
_ALL_DOMAINS = "*"


async def tag_target_domains(conn: "Connection", assignment: Any) -> set[str]:
    """The domains a tag assignment's TARGET sits in: the assignment's own domain (REQ-1531).

    A table's, and the same for one of its columns. A relationship's is the domain of the table
    the edge STARTS at -- relationships are owned by direction. A command's is the domain it sits
    in (none, for a stored row that names none). A source belongs to every domain it is open to,
    and one that names none is open to all of them. A glossary term's are its columns' (REQ-2145).
    A row rule's are the domains it decides rows in, a metric's those its expression reads, and a
    role is in none (REQ-2208). Raises :class:`DomainLookupError` when the
    target does not exist.
    """
    from sqlalchemy import select

    from provisa.core.repositories import relationship as rel_repo
    from provisa.core.repositories import source as source_repo
    from provisa.core.schema_org import tracked_functions, tracked_webhooks

    kind = assignment.object_type
    if kind in ("table", "column"):
        return {await table_domain(conn, assignment.table_id)}
    if kind == "relationship":
        row = await rel_repo.get(conn, assignment.relationship_id)
        if row is None:
            raise DomainLookupError(f"no relationship {assignment.relationship_id!r}")
        return {await table_domain(conn, row["source_table_id"])}
    if kind == "command":
        found = False
        domains: set[str] = set()
        for table in (tracked_functions, tracked_webhooks):
            result = await conn.execute_core(
                select(table.c.domain_id).where(table.c.name == assignment.command_name)
            )
            for (domain_id,) in result.fetchall():
                found = True
                if domain_id:
                    domains.add(domain_id)
        if not found:
            raise DomainLookupError(f"no command {assignment.command_name!r}")
        return domains
    if kind == "source":
        row = await source_repo.get(conn, assignment.source_id)
        if row is None:
            raise DomainLookupError(f"no source {assignment.source_id!r}")
        return set(row["allowed_domains"] or [_ALL_DOMAINS])
    if kind == "term":  # REQ-2145: the domains of the columns the term holds
        from provisa.core.repositories import glossary as glossary_repo

        if not await glossary_repo.term_exists(conn, assignment.term_id):
            raise DomainLookupError(f"no term {assignment.term_id!r}")
        return (await glossary_repo.term_domains(conn)).get(assignment.term_id, set())
    if kind == "product":  # REQ-1634: the product's own domain
        from provisa.core.repositories import data_product as data_product_repo

        product = await data_product_repo.get(conn, assignment.product_id)
        if product is None:
            raise DomainLookupError(f"no data product {assignment.product_id!r}")
        return {product["domain_id"]}
    if kind in ("document", "section"):  # REQ-2208: the library document's domains
        return await library_target_domains(conn, assignment)
    if kind == "rls_rule":  # REQ-2208
        return await rule_domains(conn, assignment.rls_rule_id)
    if kind == "role":  # REQ-2208: a role is the organization's, in no domain
        from provisa.core.schema_org import roles

        found = await conn.execute_core(select(roles.c.id).where(roles.c.id == assignment.role_id))
        if found.fetchone() is None:
            raise DomainLookupError(f"no role {assignment.role_id!r}")
        return set()
    if kind == "metric":  # REQ-2208: the domains its expression reads (REQ-1531)
        metric_reads = await metric_domains(conn, assignment.metric_name)
        if metric_reads is None:
            raise DomainLookupError(f"no metric {assignment.metric_name!r}")
        return metric_reads
    raise DomainLookupError(f"a {kind!r} tag assignment has no target this gate can place")


def tag_offered(
    tag_domains: Iterable[str], object_domains: Iterable[str], acting: Iterable[str] | None
) -> bool:  # REQ-2208
    """Whether a tag whose allowed domains are ``tag_domains`` is offered on, and may be put on, an
    object in ``object_domains`` (:func:`tag_target_domains`) by a caller acting in ``acting``
    (``None``: every domain, as ``allowed_domains_request`` answers it).

    A tag that names no domain is every domain's, as a source that names none is. An object is
    of the tag's domains when one of its domains is among them; a source open to every domain
    (``*``) is of all of them. An object in no domain (a role) is placed by its assigner instead:
    the tag is offered to someone acting in one of its domains, for that domain's own purposes."""
    allowed = {d for d in tag_domains if d}
    if not allowed or _ALL_DOMAINS in allowed:
        return True
    placed = set(object_domains)
    if placed:
        return _ALL_DOMAINS in placed or bool(placed & allowed)
    return acting is None or bool(set(acting) & allowed)


async def tag_assignments_outside(
    conn: "Connection", tag_id: str, tag_domains: Iterable[str]
) -> list[tuple[dict, set[str]]]:  # REQ-2208
    """The assignments of tag ``tag_id`` on objects outside ``tag_domains``, each with its object's
    domains: what narrowing the tag's domains leaves outside them. An assignment on an object in no
    domain is not among them: whether it is outside is its assigner's domain, which the assignment
    does not record."""
    import types

    from provisa.core.models import base_tag_id
    from provisa.core.repositories import tag as tag_repo

    allowed = list(tag_domains)
    base = base_tag_id(tag_id)
    rows = [
        *await tag_repo.list_assignments(conn),
        *await tag_repo.list_library_assignments(conn),
    ]
    outside: list[tuple[dict, set[str]]] = []
    for row in rows:
        if row["base_tag_id"] != base:
            continue
        target = types.SimpleNamespace(
            **{"document_id": None, "section_heading": None, "section_page": None, **row}
        )
        domains = await tag_target_domains(conn, target)
        if domains and not tag_offered(allowed, domains, None):
            outside.append((row, domains))
    return outside


async def library_target_domains(conn: "Connection", assignment: Any) -> set[str]:
    """The domains of the library document a document or section tag is on (REQ-2208): those it
    names, ``*`` for the whole org's. A section must be one the library read from the document:
    its heading, or for a section with none its page. Raises :class:`DomainLookupError` when the
    document or the section is not there."""
    from sqlalchemy import select

    from provisa.reference_library import store
    from provisa.core.schema_org import reference_passages as rp

    document = await store.document(conn, assignment.document_id)
    if document is None:
        raise DomainLookupError(f"no library document {assignment.document_id!r}")
    if assignment.object_type == "section":
        where = (
            rp.c.section == assignment.section_heading
            if assignment.section_heading is not None
            else rp.c.section.is_(None) & (rp.c.page == assignment.section_page)
        )
        found = (
            await conn.execute_core(
                select(rp.c.id).where((rp.c.document_id == document.id) & where).limit(1)
            )
        ).fetchone()
        if found is None:
            raise DomainLookupError(
                f"no section {assignment.section_heading or assignment.section_page!r} in "
                f"library document {document.id}"
            )
    return set(document.domains)


async def rule_domains(conn: "Connection", rule_id: int) -> set[str]:  # REQ-2208
    """The domains a row rule decides rows in, as saving it is gated: a table rule's table's, a
    domain rule's domain, an action rule's command's, and a tag rule's every table carrying the
    tag. Raises :class:`DomainLookupError` when there is no such rule."""
    from sqlalchemy import select

    from provisa.core.schema_org import (
        registered_tables,
        rls_rules,
        tag_assignments,
        tracked_functions,
        tracked_webhooks,
    )

    rule = (await conn.execute_core(select(rls_rules).where(rls_rules.c.id == rule_id))).fetchone()
    if rule is None:
        raise DomainLookupError(f"no row rule {rule_id!r}")
    if rule.table_id is not None:
        return {await table_domain(conn, rule.table_id)}
    if rule.domain_id is not None:
        return {rule.domain_id}
    if rule.action_name is not None:
        domains: set[str] = set()
        for table in (tracked_functions, tracked_webhooks):
            result = await conn.execute_core(
                select(table.c.domain_id).where(table.c.name == rule.action_name)
            )
            domains |= {d for (d,) in result.fetchall() if d}
        return domains
    result = await conn.execute_core(
        select(registered_tables.c.domain_id)
        .join(tag_assignments, tag_assignments.c.table_id == registered_tables.c.id)
        .where(
            tag_assignments.c.object_type == "table",
            tag_assignments.c.base_tag_id == rule.tag_id,
        )
    )
    return {d for (d,) in result.fetchall()}


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


async def require_current_domain(info: "StrawberryInfo", conn: "Connection", model: Any) -> None:
    """Gate a save to an address that already holds an object in another domain (REQ-1531).

    Saving there moves the object out of its domain, which is an act in that domain as much as in
    the one it is saved into: the caller's ``table_registration`` right is checked in the stored
    domain too. A new address, or one whose object is already in the domain sent, has nothing
    more to check here.
    """
    from provisa.api.admin._hiding_guard import stored_table
    from provisa.api.admin.capabilities import require_capability

    stored = await stored_table(conn, model)
    if stored is not None and stored["domain_id"] != model.domain_id:
        require_capability(info, "table_registration", domain_id=stored["domain_id"])
