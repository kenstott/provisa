# Copyright (c) 2026 Kenneth Stott
# Canary: 7a5e3b0c-1d86-4b03-b1f4-382eb0c1c2f2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The objects a role's lists can select, and how an identifier selects them (REQ-2219, REQ-194).

The index is the names table seen through one role's schema: the role is the ceiling, so what its
schema does not hold has no entry and no list can reach it. An identifier is the client-facing
SQL name, segment by segment, ignoring case (REQ-2214). It selects the object it names and,
naming a node of the hierarchy, everything beneath it: domain contains tables, relationships,
commands and data products; a table contains columns and metrics.
"""

# Requirements: REQ-2219, REQ-194, REQ-2214

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from provisa.compiler import naming

if TYPE_CHECKING:
    from provisa.compiler.names_table import NamesTable
    from provisa.entitlements.lists import EffectiveList

WILDCARD = "*"

TABLE = "table"
COLUMN = "column"
METRIC = "metric"
RELATIONSHIP = "relationship"
COMMAND = "command"
DATA_PRODUCT = "data_product"

#: Each object kind, and the list that selects it.
LIST_OF: dict[str, str] = {
    TABLE: "tables",
    COLUMN: "columns",
    METRIC: "metrics",
    RELATIONSHIP: "relationships",
    COMMAND: "commands",
    DATA_PRODUCT: "data_products",
}

#: The lists of the semantic hierarchy, the domain's own among them. Their identifiers name
#: objects of the names table, so one that names nothing can be flagged.
SEMANTIC_LISTS = frozenset({"domains", *LIST_OF.values()})

#: The kinds the data plane reads: a role that reads data uses them.
_DATA_PLANE = SEMANTIC_LISTS

#: The other kinds, each with the capabilities that read it (REQ-2219: a list for a kind no
#: scoped capability of the role uses is flagged).
_READ_BY: dict[str, frozenset[str]] = {
    "sources": frozenset({"source_registration", "table_registration"}),
    "environments": frozenset(
        {
            "environment_management",
            "environment_switch",
            "environment_owned_management",
            "environment_data",
        }
    ),
    "roles": frozenset({"user_management"}),
    "regions": frozenset({"platform_settings", "cross_org"}),
}


@dataclass(frozen=True)
class IndexedObject:
    """One object a list can select: ``bit`` is its position in the mask."""

    bit: int
    kind: str
    oid: str
    # Each spelling an identifier can use, as segments, lower-cased.
    paths: tuple[tuple[str, ...], ...]
    domain: str
    table_id: int | None = None
    target_table_id: int | None = None


@dataclass(frozen=True)
class Finding:
    """A flag on a list (REQ-2219: flagged, never silently ignored)."""

    code: str
    role_id: str
    kind: str
    identifier: str
    message: str


def _segments(identifier: str) -> tuple[str, ...]:
    return tuple(s.lower() for s in identifier.split("."))


def matches(identifier: str, paths: Iterable[tuple[str, ...]]) -> bool:
    """Whether ``identifier`` selects an object spelled ``paths``: segment by segment, ``*`` any
    segment, and a shorter identifier selects what is beneath it."""
    wanted = _segments(identifier)
    return any(
        len(wanted) <= len(path) and all(w in (WILDCARD, s) for w, s in zip(wanted, path))
        for path in paths
    )


class ObjectIndex:
    """The objects of one role's schema, in bit order."""

    def __init__(
        self,
        objects: list[IndexedObject],
        joins: Mapping[tuple[int, int, str, str], int],
    ) -> None:
        self.objects = tuple(objects)
        self.size = len(objects)
        self._bit = {(o.kind, o.oid): o.bit for o in objects}
        self._joins = dict(joins)
        self.table_ids = frozenset(o.table_id for o in objects if o.kind == TABLE)

    def bit_of(self, kind: str, oid: str) -> int | None:
        return self._bit.get((kind, oid))

    def relationship_bit(self, key: tuple[int, int, str, str]) -> int | None:
        return self._joins.get(key)


def build_index(
    names: NamesTable, ctx: Any, products: Iterable[tuple[str, str]] = ()
) -> ObjectIndex:
    """``names`` through the schema of the role whose compilation context is ``ctx`` (None: the
    whole model, which only the checks read). ``products`` are (product id, domain id)."""
    held = None if ctx is None else {tm.table_id for tm in ctx.tables.values()}
    tables: dict[int, tuple[str, str]] = {}
    for row in names.rows:
        if row.surface == "sql" and row.object_kind in ("table", "view") and len(row.scope) == 2:
            assert row.table_id is not None
            tables[row.table_id] = (row.scope[1].lower(), row.name.lower())
    out: list[IndexedObject] = []

    def add(kind: str, oid: str, paths: tuple, domain: str, **more: Any) -> None:
        out.append(IndexedObject(len(out), kind, oid, paths, domain, **more))

    for tid, (domain, name) in sorted(tables.items()):
        if held is None or tid in held:
            add(TABLE, str(tid), ((domain, name),), domain, table_id=tid)
    for row in names.rows:
        if row.surface != "sql" or row.object_kind not in (COLUMN, METRIC):
            continue
        tid = row.table_id
        if tid is None or tid not in tables or (held is not None and tid not in held):
            continue
        if ctx is not None and (tid, row.physical_name) not in ctx.physical_to_sql:
            continue
        domain, table = tables[tid]
        add(
            row.object_kind,
            row.object_id,
            ((domain, table, row.name.lower()),),
            domain,
            table_id=tid,
        )
    joins = _add_relationships(names, ctx, tables, held, out)
    _add_flat_objects(names, products, out)
    return ObjectIndex(out, joins)


def _add_relationships(
    names: NamesTable,
    ctx: Any,
    tables: Mapping[int, tuple[str, str]],
    held: set[int] | None,
    out: list[IndexedObject],
) -> dict[tuple[int, int, str, str], int]:
    """The relationships of the role's schema. A relationship's identifier is its source table
    and field: ``<table>_<field>``, under its source table's domain (REQ-1531), or bare."""
    type_ids = {tm.type_name: tm.table_id for tm in ctx.tables.values()} if ctx is not None else {}
    joins: dict[tuple[int, int, str, str], int] = {}
    seen: set[str] = set()
    for row in names.rows:
        if row.object_kind != RELATIONSHIP or row.surface != "graphql_field":
            continue
        tid = row.table_id
        if row.object_id in seen or tid is None or tid not in tables:
            continue
        if held is not None and tid not in held:
            continue
        target: int | None = None
        join = None
        if ctx is not None:
            type_name, _, field = row.object_id.partition(".")
            join = ctx.joins.get((type_name, field))
            if join is None or type_ids.get(type_name) != tid:
                continue
            target = join.target.table_id
        seen.add(row.object_id)
        domain, table = tables[tid]
        bare = f"{table}_{row.name.lower()}"
        bit = len(out)
        out.append(
            IndexedObject(
                bit,
                RELATIONSHIP,
                row.object_id,
                ((domain, bare), (bare,)),
                domain,
                table_id=tid,
                target_table_id=target,
            )
        )
        if join is not None and target is not None:
            joins[(tid, target, join.source_column, join.target_column)] = bit
            joins[(target, tid, join.target_column, join.source_column)] = bit
    return joins


def _add_flat_objects(
    names: NamesTable, products: Iterable[tuple[str, str]], out: list[IndexedObject]
) -> None:
    """Commands and data products: each under its domain."""
    seen: set[str] = set()
    for row in names.rows:
        if row.object_kind != COMMAND or row.object_id in seen:
            continue
        seen.add(row.object_id)
        domain = naming.domain_to_sql_name(row.domain_id).lower() if row.domain_id else ""
        paths = ((domain, row.object_id.lower()),) if domain else ((row.object_id.lower(),),)
        out.append(IndexedObject(len(out), COMMAND, row.object_id, paths, domain))
    for product_id, domain_id in sorted(products):
        domain = naming.domain_to_sql_name(domain_id).lower()
        out.append(
            IndexedObject(
                len(out), DATA_PRODUCT, product_id, ((domain, product_id.lower()),), domain
            )
        )


def check_lists(
    role_id: str,
    role: Mapping[str, Any],
    lists: Mapping[str, EffectiveList],
    model_index: ObjectIndex,
) -> list[Finding]:
    """The flags on a person's lists for ``role_id``: an identifier naming nothing in the model,
    and a list for a kind the role uses nothing of. ``model_index`` is the whole model's: an
    object outside the role still exists, and is simply out of its reach."""
    findings: list[Finding] = []
    capabilities = set(role.get("capabilities") or ())
    has_data_plane = bool(role.get("domain_access"))
    domains = {o.domain for o in model_index.objects}
    for kind in sorted(lists):
        listed = lists[kind]
        used = (
            has_data_plane
            if kind in _DATA_PLANE
            else bool(_READ_BY.get(kind, frozenset()) & capabilities)
        )
        if not used:
            findings.append(
                Finding(
                    "unused_kind",
                    role_id,
                    kind,
                    "",
                    f"role {role_id!r} uses no {kind!r}, so this list selects nothing",
                )
            )
            continue
        if kind not in SEMANTIC_LISTS:
            continue
        kinds = None if kind == "domains" else {k for k, v in LIST_OF.items() if v == kind}
        for identifier in listed.identifiers:
            if identifier == WILDCARD:
                continue
            if kinds is None:
                known = _segments(identifier)[0] in domains
            else:
                known = any(
                    o.kind in kinds and matches(identifier, o.paths) for o in model_index.objects
                )
            if not known:
                findings.append(
                    Finding(
                        "unknown_identifier",
                        role_id,
                        kind,
                        identifier,
                        f"{identifier!r} in {kind!r} for role {role_id!r} names nothing "
                        "in the names table",
                    )
                )
    return findings
