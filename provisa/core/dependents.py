# Copyright (c) 2026 Kenneth Stott
# Canary: 1182e13d-115e-4372-ad11-cae551f5fa65
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What refers to an object in an org's control plane, and what that means for deleting it (REQ-1918).

An object has PARTS and DEPENDENTS. A part exists only as part of it and goes with it. A
dependent is another object that refers to it, and blocks its deletion. A reference from an
object to itself never blocks. Objects that refer to each other in a circle form a cycle, which
no single deletion can remove.

This module holds the ONE inventory of those references and answers three questions from it:

* :func:`dependents` — which objects block this one;
* :func:`parts` — which rows go with it;
* :func:`cycle_of` — which other objects it forms a cycle with.

Nothing here deletes anything, and no delete path calls it yet.

**The inventory is data.** ``REFERENCES`` lists every column that refers to an object, whether or
not the schema declares a foreign key for it; a test holds that every declared foreign key is
listed. The answers are computed in code from rows read through the control-plane connection, so
they are the same on PostgreSQL and SQLite and rely on no database cascade.

**A reference whose standing is not yet ruled is UNDECIDED.** ``PENDING_CHOICES`` names each open
choice and its alternatives. The functions take the rulings as an argument; a reference whose
choice is not among them raises :class:`PendingChoice` when a row actually matches it. There is
no default standing.

**Not covered here.** Objects kept in the platform plane (org, environment, user, secret,
personal access token, invite) and their references; ``tracked_functions.returns``,
``kafka_sinks.query_stable_id`` and the config file's scheduled triggers, which name a table in
forms not yet established; data held outside the control plane (replicas, view storage, caches).
"""

# Requirements: REQ-1917, REQ-1918

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

import sqlglot
from sqlalchemy import select
from sqlglot import exp
from sqlglot.errors import SqlglotError

from provisa.core.schema_org import metadata

if TYPE_CHECKING:
    from provisa.core.database import Connection


class Standing(Enum):
    PART = "part"
    DEPENDENT = "dependent"
    UNDECIDED = "undecided"


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
    for a table ``"view_mv_id"``). A DEPENDENT or UNDECIDED reference names the object the
    referring row is, or belongs to: its kind ``of`` and the column ``owner`` holding that
    object's key. ``ends`` groups the columns of one row that are the ends of a link; a row all
    of whose ends are the same object is that object's reference to itself, and is its part.
    """

    table: str
    column: str
    to: str
    standing: Standing
    match: Match = Match.EQUALS
    by: str = "key"
    of: str | None = None
    owner: str | None = None
    choice: str | None = None
    path: str | None = None
    ends: tuple[str, ...] = ()


@dataclass(frozen=True)
class Dependent:
    ref: ObjectRef
    via: tuple[str, ...]  # the referring columns, as "table.column"


@dataclass(frozen=True)
class PartRows:
    table: str
    column: str
    count: int


class PendingChoice(Exception):
    """A row matches a reference whose standing has not been ruled."""

    def __init__(self, choice: str, reference: Reference) -> None:
        self.choice = choice
        self.reference = reference
        super().__init__(
            f"{reference.table}.{reference.column} refers to a {reference.to}, and whether that "
            f"is a part or a dependent is not ruled: {choice} — {PENDING_CHOICES[choice]}"
        )


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

# Each open choice and its alternatives. A choice leaves this table when it is ruled: its
# references take the ruled standing and lose their ``choice``.
PENDING_CHOICES: dict[str, str] = {
    "source.tables": "a source's registered tables block its deletion, or go with it",
    "table.row_filters": "a row filter on the table goes with it, or blocks its deletion",
    "table.outgoing_relationships": (
        "a relationship FROM the table blocks its deletion, or goes with it"
    ),
    "role.assignments": "a role's assignments block its deletion, or go with it",
    "role.grant_lists": (
        "an object naming the role in a grant or ownership list blocks its deletion, or the "
        "role is removed from the list"
    ),
    "data_product.members": (
        "a data product's member tables and commands block its deletion, or are detached"
    ),
    "tag.assignments": "a tag's assignments go with it, or block its deletion",
    "glossary_term.edges": "an edge from another term goes with the term, or blocks its deletion",
}

# A kind whose objects may be PARTS of another object: the owner's kind, the column of the kind's
# own table holding the owner's key, and the open choice that decides it (None: decided, a part).
# A dependent of such a kind is reported as its owner — what refers to the object is then the
# owner, through its part. This is what makes two tables each holding a relationship to the other
# a cycle of the two tables: each table's relationship is its part and refers to the other table.
# A row whose owner column is empty has no owner and stands for itself.
OWNED: dict[str, tuple[str, str, str | None]] = {
    "relationship": ("table", "source_table_id", "table.outgoing_relationships"),
    "row_filter": ("table", "table_id", "table.row_filters"),
    "role_assignment": ("role", "role_id", "role.assignments"),
}

_P, _D, _U = Standing.PART, Standing.DEPENDENT, Standing.UNDECIDED
_ENDS = ("source_table_id", "target_table_id", "via_table_id")


def _part(table: str, column: str, to: str, **kw: Any) -> Reference:
    return Reference(table, column, to, _P, **kw)


def _dep(table: str, column: str, to: str, of: str, owner: str, **kw: Any) -> Reference:
    return Reference(table, column, to, _D, of=of, owner=owner, **kw)


def _open(
    table: str, column: str, to: str, of: str, owner: str, choice: str, **kw: Any
) -> Reference:
    return Reference(table, column, to, _U, of=of, owner=owner, choice=choice, **kw)


_GRANTS = "role.grant_lists"

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
    _open("registered_tables", "source_id", "source", "table", "id", "source.tables"),
    _dep("tracked_functions", "source_id", "source", "command", "name"),
    _part("tag_assignments", "source_id", "source"),
    _part("provisa_sources", "source_id", "source"),
    _part("api_sources", "id", "source"),
    _part("kafka_sources", "id", "source"),
    # --- to a registered table (a view is one) -------------------------------------------------
    _part("table_columns", "table_id", "table"),
    _part("file_source_mtimes", "table_id", "table"),
    _part("glossary_term_refs", "table_id", "table"),
    _part("table_meta_links", "source_table_id", "table"),
    _part("table_meta_links", "target_table_id", "table"),
    _part("tag_assignments", "table_id", "table"),
    _part("relationship_candidates", "source_table_id", "table"),
    _part("relationship_candidates", "target_table_id", "table"),
    _part("materialized_views", "id", "table", by="view_mv_id"),
    _open("rls_rules", "table_id", "table", "row_filter", "id", "table.row_filters"),
    _open(
        "relationships",
        "source_table_id",
        "table",
        "relationship",
        "id",
        "table.outgoing_relationships",
        ends=_ENDS,
    ),
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
    # --- to a relationship ---------------------------------------------------------------------
    _part("tag_assignments", "relationship_id", "relationship"),
    # --- to a role -----------------------------------------------------------------------------
    _dep("roles", "parent_role_id", "role", "role", "id"),
    _dep("roles", "defined_from", "role", "role", "id"),
    _open("user_role_assignments", "role_id", "role", "role_assignment", "id", "role.assignments"),
    _part("rls_rules", "role_id", "role"),
    _open("table_columns", "visible_to", "role", "column", "id", _GRANTS, match=Match.MEMBER),
    _open("table_columns", "writable_by", "role", "column", "id", _GRANTS, match=Match.MEMBER),
    _open("table_columns", "unmasked_to", "role", "column", "id", _GRANTS, match=Match.MEMBER),
    _open("metrics", "visible_to", "role", "metric", "name", _GRANTS, match=Match.MEMBER),
    _open(
        "tracked_functions", "visible_to", "role", "command", "name", _GRANTS, match=Match.MEMBER
    ),
    _open(
        "tracked_functions", "writable_by", "role", "command", "name", _GRANTS, match=Match.MEMBER
    ),
    _open("tracked_webhooks", "visible_to", "role", "webhook", "name", _GRANTS, match=Match.MEMBER),
    _open("data_products", "owner_role", "role", "data_product", "id", _GRANTS),
    _open("data_products", "team_role", "role", "data_product", "id", _GRANTS),
    _open("domains", "steward", "role", "domain", "id", _GRANTS),
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
    _open("registered_tables", "product_id", "data_product", "table", "id", "data_product.members"),
    _open(
        "tracked_functions", "product_id", "data_product", "command", "name", "data_product.members"
    ),
    _part("tag_assignments", "product_id", "data_product"),
    # --- to a glossary term --------------------------------------------------------------------
    _part("glossary_term_refs", "term_id", "glossary_term"),
    _part("glossary_term_domains", "term_id", "glossary_term"),
    _part("glossary_term_experts", "term_id", "glossary_term"),
    _part("glossary_term_edges", "from_term_id", "glossary_term"),
    _open(
        "glossary_term_edges",
        "to_term_id",
        "glossary_term",
        "glossary_term",
        "from_term_id",
        "glossary_term.edges",
    ),
    # --- to a tag ------------------------------------------------------------------------------
    _part("tag_param_values", "tag_id", "tag"),
    _open("tag_assignments", "base_tag_id", "tag", "tag_assignment", "id", "tag.assignments"),
    # --- to a remote source registration, and runtime rows -------------------------------------
    _part("api_endpoints", "source_id", "api_source"),
    _part("api_endpoint_candidates", "source_id", "api_source"),
    _part("kafka_topics", "source_id", "kafka_source"),
    _part("event_status", "event_id", "event"),
)

Rulings = Mapping[str, Standing]
NO_RULINGS: Rulings = {}


def _standing(reference: Reference, rulings: Rulings) -> Standing:
    """The reference's standing: its own, or — for an open choice — the ruling given for it.
    Raises :class:`PendingChoice` for an open choice with no ruling."""
    if reference.standing is not Standing.UNDECIDED:
        return reference.standing
    assert reference.choice is not None
    ruled = rulings.get(reference.choice)
    if ruled is None or ruled is Standing.UNDECIDED:
        raise PendingChoice(reference.choice, reference)
    return ruled


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


async def _attributes(conn: "Connection", ref: ObjectRef) -> dict[str, Any] | None:
    """What other rows may hold to refer to this object, or ``None`` when there is no such
    object: its key, its name when its kind has one, and for a table the id its view storage
    row is kept under."""
    kind = KINDS[ref.kind]
    table = metadata.tables[kind.table]
    columns = [table.c[kind.key]] + ([table.c[kind.name]] if kind.name else [])
    row = (await conn.execute_core(select(*columns).where(table.c[kind.key] == ref.id))).fetchone()
    if row is None:
        return None
    attributes: dict[str, Any] = {"key": row[0]}
    if kind.name:
        attributes["name"] = row[1]
    if ref.kind == "table":
        attributes["view_mv_id"] = f"view-{row[1]}"
    return attributes


async def _referring_rows(
    conn: "Connection", reference: Reference, wanted: Any
) -> list[Mapping[str, Any]]:
    """The rows of ``reference.table`` whose column refers to ``wanted``."""
    table = metadata.tables[reference.table]
    names = {reference.column, *reference.ends}
    if reference.owner is not None:
        names.add(reference.owner)
    if reference.of in OWNED:
        names.add(OWNED[reference.of][1])
    statement = select(*(table.c[name] for name in sorted(names)))
    if reference.match is Match.EQUALS:
        statement = statement.where(table.c[reference.column] == wanted)
    rows = (await conn.execute_core(statement)).fetchall()
    return [r._mapping for r in rows if _matches(reference, r._mapping[reference.column], wanted)]


def _referrer(reference: Reference, row: Mapping[str, Any], rulings: Rulings) -> ObjectRef:
    """The object a referring row stands for: the object the row is, or — when objects of that
    kind are parts of another — the object it is part of."""
    assert reference.of is not None and reference.owner is not None
    itself = ObjectRef(reference.of, row[reference.owner])
    if reference.of not in OWNED:
        return itself
    owner_kind, owner_column, choice = OWNED[reference.of]
    if row[owner_column] is None:
        return itself
    if choice is not None:
        ruled = rulings.get(choice)
        if ruled is None or ruled is Standing.UNDECIDED:
            raise PendingChoice(choice, reference)
        if ruled is Standing.DEPENDENT:
            return itself
    return ObjectRef(owner_kind, row[owner_column])


def _refers_only_to_itself(reference: Reference, row: Mapping[str, Any], key: Any) -> bool:
    """True when every end of a link row is the one object: the object's reference to itself."""
    return bool(reference.ends) and all(row[end] in (None, key) for end in reference.ends)


async def _scan(
    conn: "Connection", ref: ObjectRef, rulings: Rulings
) -> tuple[dict[ObjectRef, list[str]], list[PartRows]] | None:
    """Every row that refers to ``ref``, sorted into the objects that block it and the rows that
    go with it; ``None`` when there is no such object."""
    attributes = await _attributes(conn, ref)
    if attributes is None:
        return None
    blocking: dict[ObjectRef, list[str]] = {}
    going: list[PartRows] = []
    for reference in REFERENCES:
        if reference.to != ref.kind:
            continue
        rows = await _referring_rows(conn, reference, attributes[reference.by])
        if not rows:
            continue
        own = [r for r in rows if _refers_only_to_itself(reference, r, attributes["key"])]
        others = [r for r in rows if r not in own]
        standing = _standing(reference, rulings) if others else Standing.PART
        if standing is Standing.PART:
            going.append(PartRows(reference.table, reference.column, len(rows)))
            continue
        if own:
            going.append(PartRows(reference.table, reference.column, len(own)))
        for row in others:
            referrer = _referrer(reference, row, rulings)
            if referrer == ref:
                continue  # a reference from an object to itself never blocks it
            blocking.setdefault(referrer, []).append(f"{reference.table}.{reference.column}")
    return blocking, going


async def dependents(
    conn: "Connection", ref: ObjectRef, rulings: Rulings = NO_RULINGS
) -> list[Dependent]:
    """The objects that refer to ``ref`` and block its deletion: direct only, its parts and its
    references to itself excluded. Raises ``LookupError`` when there is no such object and
    :class:`PendingChoice` when a row matches a reference whose standing is not ruled."""
    scanned = await _scan(conn, ref, rulings)
    if scanned is None:
        raise LookupError(f"no {ref.kind} {ref.id!r}")
    blocking, _ = scanned
    return sorted(
        (Dependent(referrer, tuple(sorted(set(via)))) for referrer, via in blocking.items()),
        key=lambda d: (d.ref.kind, str(d.ref.id)),
    )


async def parts(
    conn: "Connection", ref: ObjectRef, rulings: Rulings = NO_RULINGS
) -> list[PartRows]:
    """The rows that exist only as part of ``ref`` and go with it, by table and column."""
    scanned = await _scan(conn, ref, rulings)
    if scanned is None:
        raise LookupError(f"no {ref.kind} {ref.id!r}")
    return sorted(scanned[1], key=lambda p: (p.table, p.column))


async def cycle_of(
    conn: "Connection", ref: ObjectRef, rulings: Rulings = NO_RULINGS
) -> list[ObjectRef]:
    """The OTHER members of the cycle ``ref`` is in — empty when it is in none.

    The cycle is the strongly connected component of the blocking graph that contains ``ref``:
    every object that blocks ``ref`` (directly or through others) and is in turn blocked by it.
    """
    blocked_by: dict[ObjectRef, list[ObjectRef]] = {}
    frontier = [ref]
    while frontier:
        current = frontier.pop()
        if current in blocked_by:
            continue
        found = await dependents(conn, current, rulings)
        blocked_by[current] = [d.ref for d in found]
        frontier.extend(blocked_by[current])
    # ``blocked_by`` now holds everything that blocks ref, at any distance. A member of ref's
    # cycle is one of those that ref blocks in turn: one from which ref is reachable.
    reaches_ref = {ref}
    grew = True
    while grew:
        grew = False
        for node, blockers in blocked_by.items():
            if node not in reaches_ref and any(b in reaches_ref for b in blockers):
                reaches_ref.add(node)
                grew = True
    return sorted(reaches_ref - {ref}, key=lambda o: (o.kind, str(o.id)))
