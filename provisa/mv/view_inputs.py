# Copyright (c) 2026 Kenneth Stott
# Canary: 2f28d0ec-f014-474d-9027-3d87a39f4d71
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a materialized view reads, resolved to the registered model — never matched by text.

A view's SQL names its inputs in whatever spelling it was written or lowered in: a semantic
``domain.table`` or bare name as an author writes it, or the catalog-physical
``catalog.schema.table`` the schema build lowers it to. Each reference is resolved here, against
the whole model (every registered table and every materialized view — not the tables one role's
context exposes), to exactly one of:

* a registered table, by its id;
* a materialized view, by its id.

A reference that names nothing, or that two different tables or views answer to, is not resolved:
a view that reads it is refused when it is declared (save, config load —
``readable_inputs.require_readable_inputs``), naming the view and the reference, so it never gets
as far as being wired. Anything that later meets an unresolved reference (the event graph's
wiring) has met a defect, and raises.

The one resolution for every consumer: the view-input check at declaration, and the event graph's
edges (``provisa.events.nodes``).
"""

# Requirements: REQ-939, REQ-1674, REQ-1912, REQ-1918

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import sqlglot
import sqlglot.expressions as exp

from provisa.mv.models import TableIdentity


Ref = tuple[str, ...] | TableIdentity  # a name as SQL spells it, or a bound table


@dataclass(frozen=True)
class Resolved:
    """One input of a view: a registered table (``table_id``) or a materialized view (``view``,
    its id). Exactly one is set."""

    ref: str
    table_id: int | None = None
    view: str | None = None


class InputUnresolved(ValueError):
    """A view's SQL names a table the model does not hold, or one several tables answer to."""

    def __init__(self, view: str, ref: str, candidates: list[str]) -> None:
        self.view = view
        self.ref = ref
        self.candidates = candidates
        if candidates:
            why = f"it names more than one table or view ({', '.join(candidates)})"
        else:
            why = "no registered table or materialized view has that name"
        super().__init__(f"materialized view {view!r} reads {ref!r}: {why}")


def table_refs(sql: str, dialect: str = "postgres") -> list[tuple[str, ...]]:
    """The table references ``sql`` makes, each as its name parts (``catalog``, ``schema``,
    ``table`` as written, omitting the parts it leaves out), in first-reference order. A CTE
    defined in the same statement is not a reference."""
    tree = sqlglot.parse_one(sql, read=dialect)
    ctes = {c.alias_or_name for c in tree.find_all(exp.CTE)}
    out: list[tuple[str, ...]] = []
    for t in tree.find_all(exp.Table):
        parts = tuple(
            p.name
            for p in (t.args.get("catalog"), t.args.get("db"), t.this)
            if p is not None and p.name
        )
        if parts and t.this.name not in ctes and parts not in out:
            out.append(parts)
    return out


def _keys(parts: Iterable[str]) -> str:
    return ".".join(p.lower() for p in parts)


class ModelIndex:
    """Every spelling a reference may use, mapped to what it names. Built from the model the
    process holds after its schema build: the registered tables (``state.tables``), each table's
    semantic names in the compiled contexts of every role (the model's names are the same in all
    of them; a table only one role sees is still found), each source's engine catalog, and the
    materialized views in the registry."""

    def __init__(self, state: Any) -> None:
        from provisa.compiler.naming import apply_sql_name, domain_to_sql_name
        from provisa.compiler.sql_rewrite import semantic_table_name

        self._names: dict[str, set[tuple[str, Any]]] = {}
        self._rows: dict[int, dict] = {}
        self._ids: dict[TableIdentity, int] = {}
        catalogs = state.source_catalogs
        for row in state.tables:
            table_id = int(row["id"])
            self._rows[table_id] = row
            self._ids[TableIdentity(row["source_id"], row["schema_name"], row["table_name"])] = (
                table_id
            )
            target = ("table", table_id)
            original = row["table_name"]
            for name in {original, apply_sql_name(original)}:
                self._add((name,), target)
                if row.get("domain_id"):
                    self._add((domain_to_sql_name(row["domain_id"]), name), target)
                self._add((row["schema_name"], name), target)
                catalog = catalogs.get(row["source_id"])
                if catalog is not None:
                    self._add((catalog, row["schema_name"], name), target)
            if row.get("alias"):
                self._add((apply_sql_name(row["alias"]),), target)
        for ctx in state.contexts.values():
            for meta in ctx.tables.values():
                target = ("table", meta.table_id)
                semantic = semantic_table_name(meta)
                self._add((semantic,), target)
                self._add((domain_to_sql_name(meta.domain_id), semantic), target)
                self._add((meta.catalog_name, meta.schema_name, meta.table_name), target)
        for mv in state.mv_registry.get_enabled():
            target = ("view", mv.id)
            self._add((mv.target_catalog, mv.target_schema, mv.target_table), target)
            self._add((mv.target_schema, mv.target_table), target)

    def _add(self, parts: tuple[str, ...], target: tuple[str, Any]) -> None:
        if all(parts):
            self._names.setdefault(_keys(parts), set()).add(target)

    def row(self, table_id: int) -> dict:
        """The registered table ``table_id`` — every resolved id is one of the model's."""
        return self._rows[table_id]

    def resolve(self, view: str, parts: Ref) -> Resolved:
        """What ``parts`` names — a name as SQL spells it, or a table bound by its identity — or
        :class:`InputUnresolved` naming ``view`` and the reference."""
        if isinstance(parts, TableIdentity):
            table_id = self._ids.get(parts)
            if table_id is None:
                raise InputUnresolved(view, parts.label, [])
            return Resolved(parts.label, table_id=table_id)
        ref = ".".join(parts)
        found = self._names.get(_keys(parts), set())
        if len(found) != 1:
            raise InputUnresolved(view, ref, sorted(f"{kind} {value}" for kind, value in found))
        kind, value = next(iter(found))
        return Resolved(ref, table_id=value) if kind == "table" else Resolved(ref, view=value)


def view_refs(mv: Any) -> list[Ref]:
    """The references a view makes: its SQL's, or — for a join-pattern view, which has no SQL of
    its own — the tables it joins, as they were bound when it was declared."""
    if mv.sql:
        return list(table_refs(mv.sql))
    if len(mv.inputs) != len(mv.source_tables):
        raise RuntimeError(
            f"materialized view {mv.id!r} joins {mv.source_tables} but has "
            f"{len(mv.inputs)} bound input(s): a join-pattern view is bound when it is declared"
        )
    return list(mv.inputs)


def resolve_view_inputs(mv: Any, state: Any, index: ModelIndex | None = None) -> list[Resolved]:
    """Every input of ``mv`` resolved against the model (:func:`view_refs`). Raises
    :class:`InputUnresolved` for the first reference that names nothing or several things."""
    index = index if index is not None else ModelIndex(state)
    return [index.resolve(mv.id, ref) for ref in view_refs(mv)]
