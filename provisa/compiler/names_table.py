# Copyright (c) 2026 Kenneth Stott
# Canary: 2f8e6a1c-5b47-4d93-a0c2-9e7b31d4f856
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The names table: every model object's name on every surface, built once per model version.

REQ-194 (amended 2026-11-07, ONE NAMES TABLE PER MODEL VERSION): the naming authority runs where
the schema is built and produces one table -- for every model object, each surface's name -- with
an index of SQL names ignoring case (REQ-2214). Every clash (case-only, cross-surface, a taken
address) is settled while the table is built under the org's clash policy: refused naming both
objects, or disambiguated from an attribute that tells them apart and frozen as the object's
alias. Every name is checked as it is entered, and each problem is a warning on the object
(THE NAMES TABLE FINDS NAMING PROBLEMS).

This module computes no name itself: every name comes from :mod:`provisa.compiler.naming`.
"""

# Requirements: REQ-194, REQ-2214

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from functools import partial
from typing import TYPE_CHECKING

from provisa.compiler import naming

if TYPE_CHECKING:
    from provisa.compiler.sql_types import CompilationContext, TableMeta

#: REQ-194 (CLASH POLICY): how a name clash is settled. Refuse is the default.
CLASH_POLICIES: tuple[str, ...] = ("refuse", "disambiguate")
DEFAULT_CLASH_POLICY = "refuse"

#: The object kinds the table names (REQ-194, ONE NAMES TABLE PER MODEL VERSION).
OBJECT_KINDS: tuple[str, ...] = (
    "source",
    "table",
    "view",
    "column",
    "relationship",
    "command",
    "enum",
    "enum_value",
    "metric",
)

#: Each surface name a row carries, with the surface its name checks run against
#: (:data:`naming.SURFACES`).
SURFACE_NAMES: dict[str, str] = {
    "sql": "sql",
    "catalog": "sql",
    "graphql_field": "graphql",
    "graphql_type": "graphql",
    "graphql_aggregate": "graphql",
    "graphql_group_by": "graphql",
    "graphql_connection": "graphql",
    "graphql_insert": "graphql",
    "graphql_update": "graphql",
    "graphql_delete": "graphql",
    "graphql_enum_value": "graphql",
    "cypher_label": "cypher",
    "cypher_rel_type": "cypher",
    "cypher_property": "cypher",
    "proto_message": "proto",
    "proto_field": "proto",
    "proto_rpc": "proto",
    "pg_constraint": "pg_constraint",
}

#: Surfaces whose names are matched ignoring case: SQL's (REQ-2214), so two names differing only
#: by case are one name there.
_CASE_INSENSITIVE = frozenset({"sql", "catalog", "pg_constraint"})


@dataclass(frozen=True)
class NameRow:
    """One model object's name on one surface."""

    object_kind: str
    object_id: str
    surface: str
    name: str
    physical_name: str
    label: str  # how a message names the object (``pg.public.orders``, ``orders.customer_id``)
    domain_id: str = ""
    table_id: int | None = None  # the table whose readers may see the row (REQ-1132)
    # The namespace the name must be unique in (empty: no uniqueness asked, e.g. a Cypher
    # relationship type many edges share).
    scope: tuple[str, ...] = ()
    alias: bool = False  # the name is an operator-set alias, not derived
    # REQ-194 (CLASH POLICY): what tells this object apart from one of its name, for the
    # disambiguate policy (a relationship's FK column, a table's source, a junction's target).
    attribute: str | None = None
    warnings: tuple[NameWarning, ...] = ()


@dataclass(frozen=True)
class NameWarning:
    """A naming problem recorded on an object (REQ-194, THE NAMES TABLE FINDS NAMING PROBLEMS)."""

    object_kind: str
    object_id: str
    surface: str
    code: str
    message: str


@dataclass(frozen=True)
class NameClash:
    """Two objects that would have one name in one namespace."""

    surface: str
    name: str
    holder: str
    newcomer: str

    def message(self) -> str:
        return (
            f"{self.newcomer} and {self.holder} would both be named {self.name!r} on "
            f"{self.surface}: set an alias on one of them"
        )


class NamesClashRefused(ValueError):  # REQ-194
    """The model holds names that clash; under the refuse policy the model version is not
    built. Every clash is named, with both its objects."""

    def __init__(self, clashes: list[NameClash]) -> None:
        self.clashes = clashes
        super().__init__("; ".join(c.message() for c in clashes))


@dataclass(frozen=True)
class FrozenAlias:
    """REQ-194 (CLASH POLICY): a name the disambiguate policy derived for an object, to be kept as
    its alias so later builds never rename it."""

    object_kind: str
    object_id: str
    surface: str
    alias: str


@dataclass
class NamesTable:
    """The names of one model version."""

    rows: tuple[NameRow, ...]
    frozen_aliases: tuple[FrozenAlias, ...] = ()
    _sql_index: dict[tuple[tuple[str, ...], str], NameRow] = field(default_factory=dict)

    @property
    def warnings(self) -> list[NameWarning]:
        return [w for r in self.rows for w in r.warnings]

    def resolve_sql(self, scope: tuple[str, ...], name: str) -> NameRow | None:  # REQ-2214
        """The object an unquoted SQL ``name`` in ``scope`` names, ignoring case; None if none."""
        return self._sql_index.get((scope, name.lower()))

    def names_of(self, object_kind: str, object_id: str) -> dict[str, str]:
        """The object's name on each surface."""
        return {
            r.surface: r.name
            for r in self.rows
            if r.object_kind == object_kind and r.object_id == object_id
        }


def _key(row: NameRow) -> str:
    return row.name.lower() if row.surface in _CASE_INSENSITIVE else row.name


def _checked(row: NameRow) -> NameRow:
    """``row`` with every problem its name has on its surface recorded as a warning."""
    surface = SURFACE_NAMES[row.surface]
    found = [
        NameWarning(row.object_kind, row.object_id, row.surface, code, f"{row.label}: {message}")
        for code, message in naming.name_problems(row.name, surface)
    ]
    return replace(row, warnings=(*row.warnings, *found)) if found else row


def _conversion_warnings(rows: Iterable[NameRow]) -> dict[tuple[str, str], list[NameWarning]]:
    """Each object's lossy or badly reading conversion, once per object (on its SQL row)."""
    out: dict[tuple[str, str], list[NameWarning]] = {}
    for r in rows:
        if r.surface != "sql" or r.alias or r.object_kind not in ("table", "view", "column"):
            continue
        for code, message in naming.conversion_problems(r.physical_name):
            out.setdefault((r.object_kind, r.object_id), []).append(
                NameWarning(r.object_kind, r.object_id, r.surface, code, f"{r.label}: {message}")
            )
    return out


def build_names_table(
    rows: Iterable[NameRow], policy: str = DEFAULT_CLASH_POLICY
) -> NamesTable:  # REQ-194, REQ-2214
    """The names table of ``rows``: each name checked, each clash settled under ``policy``.

    Rows are taken in the order given (the earlier holds a name), so a caller lists objects in a
    stable order. Under ``refuse`` every clash is collected and :class:`NamesClashRefused` raised
    naming both objects of each. Under ``disambiguate`` the later object is renamed by the
    authority (:func:`naming.disambiguate`) and the name returned as a :class:`FrozenAlias`, with
    a warning on the object."""
    if policy not in CLASH_POLICIES:
        raise ValueError(f"unknown clash policy {policy!r}; expected one of {CLASH_POLICIES}")
    rows = list(rows)
    conversion = _conversion_warnings(rows)
    held: dict[tuple[tuple[str, ...], str], NameRow] = {}
    clashes: list[NameClash] = []
    frozen: list[FrozenAlias] = []
    out: list[NameRow] = []
    for row in rows:
        if row.surface not in SURFACE_NAMES:
            raise ValueError(f"{row.surface!r} is not a names-table surface")
        if row.scope:
            holder = held.get((row.scope, _key(row)))
            if holder is not None and (holder.object_kind, holder.object_id) != (
                row.object_kind,
                row.object_id,
            ):
                clash = NameClash(row.surface, row.name, holder.label, row.label)
                if policy == "refuse":
                    clashes.append(clash)
                    continue
                scope = row.scope
                new_name = naming.disambiguate(
                    row.object_kind,
                    row.name,
                    row.attribute,
                    lambda n, s=scope, sf=row.surface: (
                        (
                            s,
                            n.lower() if sf in _CASE_INSENSITIVE else n,
                        )
                        in held
                    ),
                )
                # One alias per object: its first renamed surface's name (its SQL name, for a
                # column); its other surfaces derive from that alias at the next build.
                if not any(
                    (f.object_kind, f.object_id) == (row.object_kind, row.object_id) for f in frozen
                ):
                    frozen.append(
                        FrozenAlias(row.object_kind, row.object_id, row.surface, new_name)
                    )
                row = replace(
                    row,
                    name=new_name,
                    alias=True,
                    warnings=(
                        *row.warnings,
                        NameWarning(
                            row.object_kind,
                            row.object_id,
                            row.surface,
                            "disambiguated",
                            f"{clash.message()}; named {new_name!r} instead and kept as its alias",
                        ),
                    ),
                )
            held.setdefault((row.scope, _key(row)), row)
        row = _checked(row)
        extra = conversion.pop((row.object_kind, row.object_id), None)
        if extra:
            row = replace(row, warnings=(*row.warnings, *extra))
        out.append(row)
    if clashes:
        raise NamesClashRefused(clashes)
    out = _shadow_warnings(out)
    index = {
        (r.scope, r.name.lower()): r for r in out if r.surface in ("sql", "catalog") and r.scope
    }
    return NamesTable(rows=tuple(out), frozen_aliases=tuple(frozen), _sql_index=index)


def _shadow_warnings(rows: list[NameRow]) -> list[NameRow]:
    """REQ-194: an alias naming what another object's physical name is, in the alias's scope,
    shadows that object for a reader who knows it by its physical name."""
    physical: dict[tuple[tuple[str, ...], str], NameRow] = {}
    for r in rows:
        if r.scope and r.surface == "sql":
            physical.setdefault((r.scope, r.physical_name.lower()), r)
    out = []
    for r in rows:
        if r.alias and r.scope and r.surface == "sql":
            other = physical.get((r.scope, r.name.lower()))
            if other is not None and (other.object_kind, other.object_id) != (
                r.object_kind,
                r.object_id,
            ):
                r = replace(
                    r,
                    warnings=(
                        *r.warnings,
                        NameWarning(
                            r.object_kind,
                            r.object_id,
                            r.surface,
                            "alias_shadows",
                            f"{r.label}: its alias {r.name!r} is {other.label}'s physical name; "
                            "set a different alias",
                        ),
                    ),
                )
        out.append(r)
    return out


# ── the rows of a built model ──────────────────────────────────────────────────────────────────


def _table_label(tm: TableMeta) -> str:
    return f"{tm.source_id}.{tm.schema_name}.{tm.original_table_name or tm.table_name}"


def model_name_rows(  # REQ-194
    ctx: CompilationContext,
    tables: Iterable[Mapping],
    commands: Iterable[Mapping] = (),
    enums: Mapping[str, Iterable[str]] | None = None,
    sql_name_of: Mapping[int, str] | None = None,
) -> list[NameRow]:
    """Every name of the model compiled into ``ctx`` (the whole model's context, which belongs to
    no role), with ``tables`` (the stored table rows: ``id``, ``source_id``, ``alias``,
    ``view_sql``, ``gql_naming_convention``, ``columns`` with ``column_name`` and ``alias``),
    ``commands`` (each with ``name``, ``kind`` and its GraphQL ``field``) and ``enums`` (each
    source enum's labels, by name). ``sql_name_of`` gives each table's bare SQL name as the
    compiler spells it (``sql_rewrite.semantic_table_name``)."""
    stored = {t["id"]: t for t in tables}
    sql_names = sql_name_of or {}
    rows: list[NameRow] = []
    seen_sources: set[str] = set()
    by_type: dict[str, TableMeta] = {}
    for tm in sorted(
        {tm.table_id: tm for tm in ctx.tables.values()}.values(), key=lambda m: m.table_id
    ):
        by_type[tm.type_name] = tm
        t = stored.get(tm.table_id, {})
        if tm.source_id not in seen_sources:
            seen_sources.add(tm.source_id)
            rows.append(
                NameRow(
                    "source",
                    tm.source_id,
                    "catalog",
                    tm.catalog_name,
                    tm.source_id,
                    f"source {tm.source_id}",
                    scope=("catalog",),
                )
            )
        rows.extend(_table_rows(ctx, tm, t, sql_names.get(tm.table_id)))
        rows.extend(_column_rows(ctx, tm, t))
    rows.extend(_relationship_rows(ctx, by_type))
    rows.extend(_command_rows(commands))
    rows.extend(_enum_rows(enums or {}))
    return rows


def _table_rows(
    ctx: CompilationContext, tm: TableMeta, t: Mapping, sql_name: str | None
) -> list[NameRow]:
    kind = "view" if t.get("view_sql") else "table"
    oid = str(tm.table_id)
    label = _table_label(tm)
    physical = tm.original_table_name or tm.table_name
    convention = t.get("gql_naming_convention") or naming.active_gql_convention()
    domain_sql = naming.domain_to_sql_name(tm.domain_id)
    bare = naming.bare_field_name(tm.field_name, tm.domain_id)
    proto_type = naming.proto_type_name(tm.type_name)

    def row(surface: str, name: str, scope: tuple[str, ...] = ()) -> NameRow:
        return NameRow(
            kind,
            oid,
            surface,
            name,
            physical,
            label,
            domain_id=tm.domain_id,
            table_id=tm.table_id,
            scope=scope,
            alias=bool(t.get("alias")) and surface == "sql",
            attribute=tm.source_id,
        )

    out = [
        row("sql", sql_name or naming.apply_sql_name(tm.display_name or bare), ("sql", domain_sql)),
        row("graphql_field", tm.field_name, ("graphql_query",)),
        row("graphql_type", tm.type_name, ("graphql_type",)),
        row("cypher_label", naming.apply_cql_label(bare)),
        row("proto_message", proto_type, ("proto_message",)),
        row("proto_field", naming.proto_field_name(tm.field_name), ("proto_query_field",)),
        row("proto_rpc", naming.proto_table_rpcs(proto_type)["query"], ("proto_rpc",)),
    ]
    for variant in ("aggregate", "group_by", "connection"):
        out.append(
            row(
                f"graphql_{variant}",
                naming.root_field_variant(tm.field_name, variant, convention),
                ("graphql_query",),
            )
        )
    for op in sorted(ctx.write_ops.get(tm.table_id, frozenset())):
        if f"graphql_{op}" in SURFACE_NAMES:
            out.append(
                row(
                    f"graphql_{op}",
                    naming.mutation_field_name(op, tm.field_name, convention),
                    ("graphql_mutation",),
                )
            )
    if ctx.pk_columns.get(tm.table_id):
        out.append(
            row(
                "pg_constraint",
                naming.pg_pk_constraint_name(sql_name or naming.apply_sql_name(bare)),
                ("pg_constraint", domain_sql),
            )
        )
    return out


def _column_rows(ctx: CompilationContext, tm: TableMeta, t: Mapping) -> list[NameRow]:
    aliased = {c["column_name"] for c in t.get("columns", []) if c.get("alias")}
    out: list[NameRow] = []
    columns = sorted(
        {c for (tid, c) in (*ctx.physical_to_sql, *ctx.physical_to_gql) if tid == tm.table_id}
    )
    metrics = {name for (tid, name) in ctx.metric_exprs if tid == tm.table_id}
    for physical in columns:
        kind = "metric" if physical in metrics else "column"
        oid = f"{tm.table_id}:{physical}"
        label = f"{_table_label(tm)}.{physical}"

        is_alias = physical in aliased
        make = partial(
            NameRow,
            object_kind=kind,
            object_id=oid,
            physical_name=physical,
            label=label,
            domain_id=tm.domain_id,
            table_id=tm.table_id,
        )

        def row(
            surface: str, name: str, scope: tuple[str, ...] = (), make=make, is_alias=is_alias
        ) -> NameRow:
            return make(
                surface=surface, name=name, scope=scope, alias=is_alias and surface == "sql"
            )

        sql = ctx.physical_to_sql.get((tm.table_id, physical))
        gql = ctx.physical_to_gql.get((tm.table_id, physical))
        if sql is not None:
            out.append(row("sql", sql, ("sql_column", str(tm.table_id))))
        if gql is not None:
            out.append(row("graphql_field", gql, ("graphql_type_field", tm.type_name)))
            out.append(row("cypher_property", naming.apply_cql_property(gql)))
            out.append(
                row(
                    "proto_field",
                    naming.proto_field_name(gql),
                    ("proto_field", naming.proto_type_name(tm.type_name)),
                )
            )
    return out


def _relationship_rows(ctx: CompilationContext, by_type: Mapping[str, TableMeta]) -> list[NameRow]:
    out: list[NameRow] = []
    for (type_name, field_name), join in sorted(ctx.joins.items()):
        src = by_type.get(type_name)
        oid = f"{type_name}.{field_name}"
        label = f"relationship {oid}"
        common = {
            "domain_id": src.domain_id if src else "",
            "table_id": src.table_id if src else None,
            "attribute": join.source_column,
        }
        out.append(
            NameRow(
                "relationship",
                oid,
                "graphql_field",
                field_name,
                field_name,
                label,
                scope=("graphql_type_field", type_name),
                **common,
            )
        )
        if join.disable_cypher:
            continue
        if join.via is not None:
            via = join.via
            nominated = {
                "column": via.type_value,
                "table": via.table.table_name,
                "fixed": via.label_fixed,
            }.get(via.label_source)
            if not nominated:
                raise ValueError(f"{label}: junction has no label source nomination")
            rel_type = naming.cql_junction_rel_type(nominated)
        else:
            rel_type = join.cypher_alias or naming.cql_rel_type(field_name, join.cardinality)
        out.append(
            NameRow("relationship", oid, "cypher_rel_type", rel_type, field_name, label, **common)
        )
    return out


def _command_rows(commands: Iterable[Mapping]) -> list[NameRow]:
    out: list[NameRow] = []
    for c in sorted(commands, key=lambda c: c["name"]):
        root = "graphql_mutation" if c.get("kind") == "mutation" else "graphql_query"
        label = f"command {c['name']}"
        out.append(
            NameRow(
                "command", c["name"], "graphql_field", c["field"], c["name"], label, scope=(root,)
            )
        )
        out.append(
            NameRow(
                "command",
                c["name"],
                "proto_rpc",
                naming.proto_command_rpc(c["field"]),
                c["name"],
                label,
                scope=("proto_rpc",),
            )
        )
    return out


def _enum_rows(enums: Mapping[str, Iterable[str]]) -> list[NameRow]:
    out: list[NameRow] = []
    for enum_name in sorted(enums):
        type_name = naming.gql_enum_type_name(enum_name)
        out.append(
            NameRow(
                "enum",
                enum_name,
                "graphql_type",
                type_name,
                enum_name,
                f"enum {enum_name}",
                scope=("graphql_type",),
            )
        )
        for label in enums[enum_name]:
            out.append(
                NameRow(
                    "enum_value",
                    f"{enum_name}:{label}",
                    "graphql_enum_value",
                    naming.gql_enum_value_name(label),
                    label,
                    f"enum {enum_name} value {label!r}",
                    scope=("graphql_enum_value", type_name),
                )
            )
    return out
