# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a1c57-2e9d-4b06-9a4c-7d5e6f21b8c3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The keys a landed model carries (REQ-1652): which landed table holds which PRIMARY KEY, and which
FOREIGN KEYs the approved relationships place between landed tables.

Derived from the registration tables and the relationships -- model state, not catalog state -- so
they converge on the same schedule as the replicas themselves, in the landing reconcile
(``reconcile_landed_tables``), for every landed table whether or not a Data Product names it. Pure:
the plan is computed here from rows; a store runtime that can hold informational constraints
(Snowflake, Databricks, BigQuery) applies it. An enforcing store (PostgreSQL, DuckDB, MySQL) gets
no FOREIGN KEYs by design: a REPLACE land deletes and re-inserts a parent's rows on every refresh,
which a real constraint would refuse.

A key the store cannot hold -- a relationship whose referenced columns are not the referenced
table's PRIMARY KEY (the demo's ``assignments.breed_name``), or whose end is not landed -- is
withheld with a reason, never emitted.
"""

# Requirements: REQ-1652, REQ-1654, REQ-1655, REQ-1586

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from provisa.compiler.sql_types import key_list

#: A landed table's identity across the plan: ``source_id.schema_name.table_name``.
Identity = str

FK_PREFIX = "provisa_fk_"


def identity(source_id: str, schema_name: str, table_name: str) -> Identity:
    return f"{source_id}.{schema_name}.{table_name}"


@dataclass(frozen=True)
class LandedTable:
    """One entry of the landing worklist, as the key plan needs it."""

    source_id: str
    schema_name: str
    table_name: str
    primary_key: tuple[str, ...]
    # The model's descriptions (REQ-1654): the replica and its backing view carry them as COMMENTs.
    description: str = ""
    column_descriptions: dict[str, str] = field(default_factory=dict)
    # The stewards' classifications (REQ-1655), ``(tag id, value)`` on the table and per column;
    # the value is the assignment's reason, else the tag id itself.
    tags: tuple[tuple[str, str], ...] = ()
    column_tags: dict[str, tuple[tuple[str, str], ...]] = field(default_factory=dict)

    @property
    def identity(self) -> Identity:
        return identity(self.source_id, self.schema_name, self.table_name)


@dataclass(frozen=True)
class ForeignKeyEdge:
    """One FOREIGN KEY: ``holder(holder_columns)`` REFERENCES ``referenced(referenced_columns)``.
    Direction follows the cardinality (the "many" side holds the key); a junction relationship
    (REQ-1586) yields one edge per hop."""

    name: str
    holder: Identity
    holder_columns: tuple[str, ...]
    referenced: Identity
    referenced_columns: tuple[str, ...]


@dataclass
class KeyPlan:
    tables: dict[Identity, LandedTable] = field(default_factory=dict)
    edges: list[ForeignKeyEdge] = field(default_factory=list)
    withheld: list[tuple[str, str]] = field(default_factory=list)
    # Every tag id the model defines: a tag of that name found on an object but no longer assigned
    # to it is withdrawn; a tag of any other origin is never touched.
    known_tags: frozenset[str] = frozenset()
    # An entry whose store address is not derivable from its identity (an MV: registered by id,
    # stored as mv_<id> in the cache schema) carries it here.
    store_parts: dict[Identity, tuple[str, str, str]] = field(default_factory=dict)


def _fk_name(rel_id: str, suffix: str = "") -> str:
    from provisa.federation.snowflake_store import identifier

    return identifier(f"{FK_PREFIX}{rel_id}{suffix}")


def relationship_edges(
    relationships: list[dict[str, Any]], tables_by_id: dict[int, Identity]
) -> tuple[list[ForeignKeyEdge], list[tuple[str, str]]]:
    """The FOREIGN KEY edges the relationship rows declare, plus ``(relationship id, reason)`` for
    each that declares none. ``tables_by_id`` maps a registered table's integer id to its identity."""
    edges: list[ForeignKeyEdge] = []
    skipped: list[tuple[str, str]] = []

    def lookup(table_id: Any) -> Identity | None:
        return None if table_id is None else tables_by_id.get(int(table_id))

    for rel in relationships:
        rel_id = str(rel["id"])
        source = lookup(rel.get("source_table_id"))
        target = lookup(rel.get("target_table_id"))
        if source is None:
            skipped.append((rel_id, "source table is not registered"))
            continue
        if target is None:
            skipped.append((rel_id, "computed relationship has no target table"))
            continue
        via_id = rel.get("via_table_id")
        try:
            if via_id is not None:
                junction = lookup(via_id)
                if junction is None:
                    skipped.append((rel_id, "junction table is not registered"))
                    continue
                edges.append(
                    ForeignKeyEdge(
                        _fk_name(rel_id, "_source"),
                        junction,
                        key_list(rel.get("via_source_column") or ""),
                        source,
                        key_list(rel.get("source_column") or ""),
                    )
                )
                edges.append(
                    ForeignKeyEdge(
                        _fk_name(rel_id, "_target"),
                        junction,
                        key_list(rel.get("via_target_column") or ""),
                        target,
                        key_list(rel.get("target_column") or ""),
                    )
                )
                continue
            if not rel.get("target_column"):
                skipped.append((rel_id, "relationship names no target column"))
                continue
            if rel.get("cardinality") == "many-to-one":
                edges.append(
                    ForeignKeyEdge(
                        _fk_name(rel_id),
                        source,
                        key_list(rel["source_column"]),
                        target,
                        key_list(rel["target_column"]),
                    )
                )
            else:  # one-to-many: the "one" side is the source, the target holds the key
                edges.append(
                    ForeignKeyEdge(
                        _fk_name(rel_id),
                        target,
                        key_list(rel["target_column"]),
                        source,
                        key_list(rel["source_column"]),
                    )
                )
        except ValueError as exc:
            skipped.append((rel_id, str(exc)))
    return edges, skipped


def key_plan(
    landed: list[LandedTable],
    relationships: list[dict[str, Any]],
    tables_by_id: dict[int, Identity],
) -> KeyPlan:
    """The keys the store can hold for ``landed``: every landed table's PRIMARY KEY, and each
    relationship edge whose two ends are landed and whose referenced columns ARE the referenced
    table's PRIMARY KEY. Everything else is withheld with its reason."""
    plan = KeyPlan(tables={t.identity: t for t in landed})
    edges, plan.withheld = relationship_edges(relationships, tables_by_id)
    for edge in edges:
        label = f"{edge.holder} -> {edge.referenced}"
        if edge.holder not in plan.tables or edge.referenced not in plan.tables:
            plan.withheld.append((edge.name, f"{label}: both ends must be landed tables"))
            continue
        if not edge.holder_columns or len(edge.holder_columns) != len(edge.referenced_columns):
            plan.withheld.append((edge.name, f"{label}: key columns do not pair up"))
            continue
        if plan.tables[edge.referenced].primary_key != edge.referenced_columns:
            plan.withheld.append(
                (
                    edge.name,
                    f"{label}: referenced columns {list(edge.referenced_columns)} are not "
                    f"{edge.referenced}'s primary key",
                )
            )
            continue
        plan.edges.append(edge)
    return plan


def with_descriptions(
    landed: list[LandedTable], registered: list[dict[str, Any]]
) -> list[LandedTable]:
    """``landed`` with each table's description and column descriptions from its registration row."""
    by_identity = {
        identity(r["source_id"], r["schema_name"], r["table_name"]): r for r in registered
    }
    out: list[LandedTable] = []
    for table in landed:
        row = by_identity.get(table.identity)
        if row is None:
            out.append(table)
            continue
        out.append(
            LandedTable(
                table.source_id,
                table.schema_name,
                table.table_name,
                table.primary_key,
                description=(row.get("description") or "").strip(),
                column_descriptions={
                    c["column_name"]: (c.get("description") or "").strip()
                    for c in row.get("columns") or []
                    if (c.get("description") or "").strip()
                },
            )
        )
    return out


def with_tags(
    landed: list[LandedTable], assignments: list[Any], tables_by_id: dict[int, Identity]
) -> list[LandedTable]:
    """``landed`` with each table's and column's tag assignments (object types table/column; a
    relationship-, command- or product-scoped assignment has no landed object)."""
    table_tags: dict[Identity, list[tuple[str, str]]] = {}
    column_tags: dict[Identity, dict[str, list[tuple[str, str]]]] = {}
    for a in assignments:
        object_type = getattr(a, "object_type", None)
        if object_type not in ("table", "column"):
            continue
        ident: Identity | None = None
        table_id = getattr(a, "table_id", None)
        if table_id is not None:
            ident = tables_by_id.get(int(table_id))
        if ident is None:
            ident = getattr(a, "table_ref", None) or None
        if ident is None:
            continue
        value = (getattr(a, "reason", None) or "").strip() or a.tag_id
        if object_type == "table":
            table_tags.setdefault(ident, []).append((a.tag_id, value))
        elif getattr(a, "column_name", None):
            column_tags.setdefault(ident, {}).setdefault(a.column_name, []).append(
                (a.tag_id, value)
            )
    return [
        LandedTable(
            t.source_id,
            t.schema_name,
            t.table_name,
            t.primary_key,
            description=t.description,
            column_descriptions=t.column_descriptions,
            tags=tuple(table_tags.get(t.identity, ())),
            column_tags={col: tuple(tags) for col, tags in column_tags.get(t.identity, {}).items()},
        )
        for t in landed
    ]


def derived_registration(
    registered: list[dict[str, Any]], store_table: str
) -> dict[str, Any] | None:
    """The ``__derived__`` registration row behind an MV's store table ``mv_<id>`` (REQ-1654): the
    row is keyed by the MV id, the store table by the MV's target name."""
    for row in registered:
        if row.get("source_id") != "__derived__":
            continue
        name = str(row["table_name"])
        if store_table in (name, f"mv_{name.replace('-', '_')}"):
            return row
    return None


async def key_plan_for(state: Any, landed: list[LandedTable]) -> KeyPlan:
    """The plan for this deployment's landed tables -- keys AND descriptions -- read off the tenant
    registration tables and relationships."""
    from provisa.api.admin.db_queries import fetch_tables
    from provisa.core.repositories import relationship as relationship_repo

    tdb = getattr(state, "tenant_db", None)
    if tdb is None:
        return KeyPlan(tables={t.identity: t for t in landed})
    async with tdb.acquire() as conn:
        registered = await fetch_tables(conn)
        relationships = await relationship_repo.list_all(conn)
    tables_by_id = {
        int(r["id"]): identity(r["source_id"], r["schema_name"], r["table_name"])
        for r in registered
    }
    config = getattr(state, "config", None)
    assignments = list(getattr(config, "tag_assignments", None) or [])
    known = frozenset(t.id for t in (getattr(config, "tags", None) or []))
    enriched = with_tags(with_descriptions(landed, registered), assignments, tables_by_id)
    plan = key_plan(enriched, relationships, tables_by_id)
    plan.known_tags = known
    return plan
