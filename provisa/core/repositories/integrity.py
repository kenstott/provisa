# Copyright (c) 2026 Kenneth Stott
# Canary: 1182e13d-115e-4372-ad11-cae551f5fa65
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The model store's dependency guard: what refers to an object, and whether it may go (REQ-1918).

An object has PARTS and DEPENDENTS. A part exists only as part of it and goes with it. A
dependent is another object that refers to it, and blocks its deletion. A reference from an
object to itself never blocks. Dependents are chosen so that something can always be deleted
first — a relationship stands on its own and goes before the tables it joins — so no legitimate
circle of objects blocking each other exists; views that read each other, the one way to make
one, are refused when a view is saved.

``REFERENCES`` is the inventory: every column that refers to an object, whether or not the
schema declares a foreign key for it, with its standing. It is data. A test holds that every
declared foreign key is listed. The answers are computed in code from rows read through the
control-plane connection, so they are the same on PostgreSQL and SQLite and rely on no database
cascade.

A kind's delete in the model store calls :func:`guard` first and refuses while it returns
anything; then :func:`remove_parts` and the row itself, in one transaction. A change of an
object's domain will ask the same inventory.

Not covered here: objects kept in the platform plane (org, environment, user, secret, personal
access token, invite) and their references — among them the sources and configuration that
name an org secret; ``kafka_sinks.query_stable_id`` and the config file's scheduled triggers,
which name a table in forms not yet established; a materialized view's reliance on the relationships its SQL joins
over, which is by joined columns and not by a relationship's id; data held outside the control
plane (replicas, view storage, caches).
"""

# Requirements: REQ-1917, REQ-1918

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

import sqlglot
from sqlalchemy import and_, func, or_, select
from sqlglot import exp
from sqlglot.errors import SqlglotError

from provisa.core.schema_org import metadata

if TYPE_CHECKING:
    from provisa.core.database import Connection


class Standing(Enum):
    PART = "part"
    DEPENDENT = "dependent"


class Match(Enum):
    EQUALS = "equals"  # the column holds the object's key
    MEMBER = "member"  # the column holds a JSON list (or, with ``path``, an object holding one)
    MENTIONS = "mentions"  # the column holds SQL text that names the object


@dataclass(frozen=True)
class Kind:
    """A kind of object: the table its rows live in, the column that identifies one, and — when
    other objects refer to it by a name rather than by that key — the column holding the name."""

    table: str
    key: str
    name: str | None = None


@dataclass(frozen=True)
class ObjectRef:
    kind: str
    id: Any


@dataclass(frozen=True)
class Reference:
    """One column that refers to objects of kind ``to``.

    ``by`` is which attribute of the referred object the column holds (``"key"``, ``"name"``, or
    for a table ``"view_mv_id"`` or ``"schema_table"``). A DEPENDENT reference names the object the referring row is,
    or belongs to: its kind ``of`` and the column ``owner`` holding that object's key. A PART
    reference names them only when the part is itself an object with parts of its own, which
    then go too. ``ends`` groups the columns of one row that are the ends of a link; a row all
    of whose ends are the same object is that object's reference to itself, and goes with it.
    """

    table: str
    column: str
    to: str
    standing: Standing
    match: Match = Match.EQUALS
    by: str = "key"
    of: str | None = None
    owner: str | None = None
    path: str | None = None
    ends: tuple[str, ...] = ()


@dataclass(frozen=True)
class Dependent:
    ref: ObjectRef
    via: tuple[str, ...]  # the referring columns, as "table.column"


KINDS: dict[str, Kind] = {
    "domain": Kind("domains", "id"),
    "source": Kind("sources", "id"),
    "table": Kind("registered_tables", "id", name="table_name"),
    "column": Kind("table_columns", "id"),
    "relationship": Kind("relationships", "id"),
    "role": Kind("roles", "id"),
    "role_assignment": Kind("user_role_assignments", "id"),
    "row_filter": Kind("rls_rules", "id"),
    "metric": Kind("metrics", "name"),
    "materialized_view": Kind("materialized_views", "id"),
    "command": Kind("tracked_functions", "name"),
    "webhook": Kind("tracked_webhooks", "name"),
    "data_product": Kind("data_products", "id"),
    "glossary_term": Kind("glossary_terms", "id"),
    "tag": Kind("tags", "id"),
    "tag_assignment": Kind("tag_assignments", "id"),
    "calendar": Kind("calendars", "name"),
    "api_source": Kind("api_sources", "id"),
    "kafka_source": Kind("kafka_sources", "id"),
    "remote_registration": Kind("provisa_sources", "id"),
    "event": Kind("events", "id"),
}

_P, _D = Standing.PART, Standing.DEPENDENT
_ENDS = ("source_table_id", "target_table_id", "via_table_id")


def _part(table: str, column: str, to: str, **kw: Any) -> Reference:
    return Reference(table, column, to, _P, **kw)


def _dep(table: str, column: str, to: str, of: str, owner: str, **kw: Any) -> Reference:
    return Reference(table, column, to, _D, of=of, owner=owner, **kw)


REFERENCES: tuple[Reference, ...] = (
    # --- to a domain: it removes only itself (REQ-1917), so everything in it blocks -------------
    _dep("registered_tables", "domain_id", "domain", "table", "id"),
    _dep("table_columns", "domain_id", "domain", "table", "table_id"),
    _dep("data_products", "domain_id", "domain", "data_product", "id"),
    _dep("rls_rules", "domain_id", "domain", "row_filter", "id"),
    _dep("roles", "domain_access", "domain", "role", "id", match=Match.MEMBER),
    _dep("sources", "allowed_domains", "domain", "source", "id", match=Match.MEMBER),
    _dep("glossary_term_domains", "domain_id", "domain", "glossary_term", "term_id"),
    _dep("tracked_functions", "domain_id", "domain", "command", "name"),
    _dep("tracked_webhooks", "domain_id", "domain", "webhook", "name"),
    _dep("provisa_sources", "domain_id", "domain", "remote_registration", "id"),
    _dep("user_role_assignments", "domain_id", "domain", "role_assignment", "id"),
    # --- to a source ---------------------------------------------------------------------------
    _dep("registered_tables", "source_id", "source", "table", "id"),
    _dep("tracked_functions", "source_id", "source", "command", "name"),
    _part("tag_assignments", "source_id", "source"),
    _part("provisa_sources", "source_id", "source"),
    _part("api_sources", "id", "source", of="api_source", owner="id"),
    _part("kafka_sources", "id", "source", of="kafka_source", owner="id"),
    # --- to a registered table (a view is one) -------------------------------------------------
    _part("table_columns", "table_id", "table"),
    _part("file_source_mtimes", "table_id", "table"),
    _part("glossary_term_refs", "table_id", "table"),
    _part("table_meta_links", "source_table_id", "table"),
    _part("table_meta_links", "target_table_id", "table"),
    _part("tag_assignments", "table_id", "table"),
    _part("relationship_candidates", "source_table_id", "table"),
    _part("relationship_candidates", "target_table_id", "table"),
    _part("materialized_views", "id", "table", by="view_mv_id", of="materialized_view", owner="id"),
    _part("rls_rules", "table_id", "table"),
    # A relationship stands on its own: it blocks every table it takes part in, at either end
    # or as the table it goes through, and nothing refers to it, so it can always go first.
    _dep("relationships", "source_table_id", "table", "relationship", "id", ends=_ENDS),
    _dep("relationships", "target_table_id", "table", "relationship", "id", ends=_ENDS),
    _dep("relationships", "via_table_id", "table", "relationship", "id", ends=_ENDS),
    _dep("registered_tables", "view_sql", "table", "table", "id", match=Match.MENTIONS, by="name"),
    _dep(
        "materialized_views",
        "source_tables",
        "table",
        "materialized_view",
        "id",
        match=Match.MEMBER,
        by="name",
    ),
    _dep(
        "materialized_views",
        "custom_sql",
        "table",
        "materialized_view",
        "id",
        match=Match.MENTIONS,
        by="name",
    ),
    _dep("metrics", "expression", "table", "metric", "name", match=Match.MENTIONS, by="name"),
    _dep("metrics", "from_fact", "table", "metric", "name", by="name"),
    # A command or webhook that returns the table's rows names it as "schema.table".
    _dep("tracked_functions", "returns", "table", "command", "name", by="schema_table"),
    _dep("tracked_webhooks", "returns", "table", "webhook", "name", by="schema_table"),
    # --- to a relationship ---------------------------------------------------------------------
    _part("tag_assignments", "relationship_id", "relationship"),
    # --- to a role -----------------------------------------------------------------------------
    _dep("roles", "parent_role_id", "role", "role", "id"),
    _dep("roles", "defined_from", "role", "role", "id"),
    # Anyone who holds the role, and every grant or ownership that names it, blocks it: nothing
    # may be left naming a deleted role.
    _dep("user_role_assignments", "role_id", "role", "role_assignment", "id"),
    _part("rls_rules", "role_id", "role"),
    _dep("table_columns", "visible_to", "role", "column", "id", match=Match.MEMBER),
    _dep("table_columns", "writable_by", "role", "column", "id", match=Match.MEMBER),
    _dep("table_columns", "unmasked_to", "role", "column", "id", match=Match.MEMBER),
    _dep("metrics", "visible_to", "role", "metric", "name", match=Match.MEMBER),
    _dep("tracked_functions", "visible_to", "role", "command", "name", match=Match.MEMBER),
    _dep("tracked_functions", "writable_by", "role", "command", "name", match=Match.MEMBER),
    _dep("tracked_webhooks", "visible_to", "role", "webhook", "name", match=Match.MEMBER),
    _dep("data_products", "owner_role", "role", "data_product", "id"),
    _dep("data_products", "team_role", "role", "data_product", "id"),
    _dep("domains", "steward", "role", "domain", "id"),
    # --- to a metric ---------------------------------------------------------------------------
    _dep(
        "registered_tables",
        "view_metrics",
        "metric",
        "table",
        "id",
        match=Match.MEMBER,
        path="metrics",
    ),
    _dep("registered_tables", "view_sql", "metric", "table", "id", match=Match.MENTIONS),
    # --- to a materialized view ----------------------------------------------------------------
    _part("mv_refresh_log", "mv_id", "materialized_view"),
    _part("mv_delta_ledger", "mv_id", "materialized_view"),
    # --- to a calendar -------------------------------------------------------------------------
    _dep("materialized_views", "calendar", "calendar", "materialized_view", "id"),
    _dep("registered_tables", "mv_calendar", "calendar", "table", "id"),
    # --- to a command or a webhook -------------------------------------------------------------
    _part("rls_rules", "action_name", "command"),
    _part("tag_assignments", "command_name", "command"),
    _part("rls_rules", "action_name", "webhook"),
    _part("tag_assignments", "command_name", "webhook"),
    # --- to a data product ---------------------------------------------------------------------
    _dep("registered_tables", "product_id", "data_product", "table", "id"),
    _dep("tracked_functions", "product_id", "data_product", "command", "name"),
    _part("tag_assignments", "product_id", "data_product"),
    # --- to a glossary term --------------------------------------------------------------------
    _part("glossary_term_refs", "term_id", "glossary_term"),
    _part("glossary_term_domains", "term_id", "glossary_term"),
    _part("glossary_term_experts", "term_id", "glossary_term"),
    _part("glossary_term_edges", "from_term_id", "glossary_term"),
    _part("glossary_term_edges", "to_term_id", "glossary_term"),
    # --- to a tag ------------------------------------------------------------------------------
    _part("tag_param_values", "tag_id", "tag"),
    # A tag takes its assignments with it.
    _part("tag_assignments", "base_tag_id", "tag"),
    # --- to a remote source registration, and runtime rows -------------------------------------
    _part("api_endpoints", "source_id", "api_source"),
    _part("api_endpoint_candidates", "source_id", "api_source"),
    _part("kafka_topics", "source_id", "kafka_source"),
    _part("event_status", "event_id", "event"),
)


def names_in_sql(sql: str) -> tuple[set[str], set[str]]:
    """The relation names a SQL text reads, and the metric names it reads as ``metrics.<name>``.

    Relation names are the tables in FROM/JOIN position (names the statement defines itself, as
    CTEs, excluded) and the qualifiers of its columns — a metric expression names its tables
    only that way (``SUM(orders.amount)``). Text that does not parse raises ``ValueError``: what
    it reads is unknown, and unknown is not "nothing".
    """
    try:
        statement = sqlglot.parse_one(sql)
    except SqlglotError as e:
        raise ValueError(f"SQL could not be parsed, so what it reads is unknown: {e}") from e
    defined = {cte.alias_or_name for cte in statement.find_all(exp.CTE)}
    relations: set[str] = set()
    metric_names: set[str] = set()
    for table in statement.find_all(exp.Table):
        if not table.name or table.name in defined:
            continue
        if table.db == "metrics":
            metric_names.add(table.name)
        else:
            relations.add(table.name)
    aliases = {t.alias for t in statement.find_all(exp.Table) if t.alias}
    for column in statement.find_all(exp.Column):
        if column.table and column.table not in aliases and column.table not in defined:
            relations.add(column.table)
    return relations, metric_names


def _matches(reference: Reference, value: Any, wanted: Any) -> bool:
    if value is None:
        return False
    if reference.match is Match.EQUALS:
        return value == wanted
    if reference.match is Match.MEMBER:
        members = value[reference.path] if reference.path is not None else value
        return wanted in members
    relations, metric_names = names_in_sql(value)
    return wanted in (metric_names if reference.to == "metric" else relations)


async def _attributes(conn: "Connection", ref: ObjectRef) -> dict[str, Any]:
    """What other rows may hold to refer to this object: its key, its name when its kind has
    one, and for a table the id its view storage row is kept under. ``LookupError`` when there
    is no such object."""
    kind = KINDS[ref.kind]
    table = metadata.tables[kind.table]
    columns = [table.c[kind.key]] + ([table.c[kind.name]] if kind.name else [])
    if ref.kind == "table":
        columns.append(table.c.schema_name)
    row = (await conn.execute_core(select(*columns).where(table.c[kind.key] == ref.id))).fetchone()
    if row is None:
        raise LookupError(f"no {ref.kind} {ref.id!r}")
    attributes: dict[str, Any] = {"key": row[0]}
    if kind.name:
        attributes["name"] = row[1]
    if ref.kind == "table":
        attributes["view_mv_id"] = f"view-{row[1]}"
        attributes["schema_table"] = f"{row[2]}.{row[1]}"
    return attributes


async def _referring_rows(
    conn: "Connection", reference: Reference, wanted: Any
) -> list[Mapping[str, Any]]:
    """The rows of ``reference.table`` whose column refers to ``wanted``."""
    table = metadata.tables[reference.table]
    names = {reference.column, *reference.ends}
    if reference.owner is not None:
        names.add(reference.owner)
    statement = select(*(table.c[name] for name in sorted(names)))
    if reference.match is Match.EQUALS:
        statement = statement.where(table.c[reference.column] == wanted)
    rows = (await conn.execute_core(statement)).fetchall()
    return [r._mapping for r in rows if _matches(reference, r._mapping[reference.column], wanted)]


def _is_its_own(reference: Reference, row: Mapping[str, Any], ref: ObjectRef) -> bool:
    """True when the referring row is the object's reference to ITSELF, which never blocks it:
    the row is the object, or it is a link row every end of which is the object."""
    if reference.of == ref.kind and reference.owner is not None and row[reference.owner] == ref.id:
        return True
    return bool(reference.ends) and all(row[end] in (None, ref.id) for end in reference.ends)


async def guard(conn: "Connection", ref: ObjectRef) -> list[Dependent]:
    """The objects that block ``ref``'s deletion — empty when it may go.

    Direct dependents only: its parts and its references to itself are not among them.
    ``LookupError`` when there is no such object.
    """
    attributes = await _attributes(conn, ref)
    blocking: dict[ObjectRef, set[str]] = {}
    for reference in REFERENCES:
        if reference.to != ref.kind or reference.standing is not Standing.DEPENDENT:
            continue
        assert reference.of is not None and reference.owner is not None
        for row in await _referring_rows(conn, reference, attributes[reference.by]):
            if _is_its_own(reference, row, ref):
                continue
            referrer = ObjectRef(reference.of, row[reference.owner])
            blocking.setdefault(referrer, set()).add(f"{reference.table}.{reference.column}")
    return sorted(
        (Dependent(referrer, tuple(sorted(via))) for referrer, via in blocking.items()),
        key=lambda d: (d.ref.kind, str(d.ref.id)),
    )


def _part_statements(ref: ObjectRef, attributes: Mapping[str, Any]):
    """One (reference, WHERE clause) per kind of row that goes with ``ref``: its PART rows, and
    the link rows that are its reference to itself."""
    for reference in REFERENCES:
        if reference.to != ref.kind:
            continue
        table = metadata.tables[reference.table]
        refers = table.c[reference.column] == attributes[reference.by]
        if reference.standing is Standing.PART:
            # Every PART reference holds the object's key or name outright (a test holds this),
            # so the rows are found by equality.
            yield reference, refers
        elif reference.ends:
            own = [or_(table.c[end].is_(None), table.c[end] == ref.id) for end in reference.ends]
            yield reference, and_(refers, *own)


async def parts(conn: "Connection", ref: ObjectRef) -> dict[str, int]:
    """How many rows go with ``ref``, by ``table.column`` — only the columns that have any."""
    attributes = await _attributes(conn, ref)
    counts: dict[str, int] = {}
    for reference, where in _part_statements(ref, attributes):
        table = metadata.tables[reference.table]
        found = (
            await conn.execute_core(select(func.count()).select_from(table).where(where))
        ).scalar_one()
        if found:
            counts[f"{reference.table}.{reference.column}"] = found
    return counts


async def remove_parts(conn: "Connection", ref: ObjectRef) -> None:
    """Delete the rows that go with ``ref``. The caller holds the transaction, has asked
    :func:`guard`, and deletes the object's own row after this."""
    attributes = await _attributes(conn, ref)
    for reference, where in _part_statements(ref, attributes):
        table = metadata.tables[reference.table]
        if reference.standing is Standing.PART and reference.of is not None:
            # A part that is itself an object: its own parts go first.
            assert reference.owner is not None
            keys = (
                await conn.execute_core(select(table.c[reference.owner]).where(where))
            ).fetchall()
            for (key,) in keys:
                await remove_parts(conn, ObjectRef(reference.of, key))
        await conn.execute_core(table.delete().where(where))


async def circle_of(conn: "Connection", ref: ObjectRef) -> list[ObjectRef]:
    """The OTHER objects that block ``ref`` and are in turn blocked by it — empty when there
    are none, which is the case on any control plane written through the model store.

    Only a damaged control plane holds such a circle (views saved reading each other before that
    was refused, a role parent loop written around the save check). A deletion is refused there
    like any other; this names the members so that the operator can edit one of them.
    """
    blocked_by: dict[ObjectRef, list[ObjectRef]] = {}
    frontier = [ref]
    while frontier:
        current = frontier.pop()
        if current in blocked_by:
            continue
        blocked_by[current] = [d.ref for d in await guard(conn, current)]
        frontier.extend(blocked_by[current])
    # ``blocked_by`` holds everything that blocks ref, at any distance. A member of ref's circle
    # is one of those that ref blocks in turn: one from which ref is reachable.
    reaches_ref = {ref}
    grew = True
    while grew:
        grew = False
        for node, blockers in blocked_by.items():
            if node not in reaches_ref and any(b in reaches_ref for b in blockers):
                reaches_ref.add(node)
                grew = True
    return sorted(reaches_ref - {ref}, key=lambda o: (o.kind, str(o.id)))
