# Copyright (c) 2026 Kenneth Stott
# Canary: ba2eaf2a-dc17-4b16-aef9-b4a7ffcba02c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The model's fakes checked together, when a table or relationship is saved and when the model
is loaded (REQ-1494): each table's fakes by :func:`provisa.fakes.checks.check_table`, and every
relationship's two columns by :func:`provisa.fakes.checks.check_joins`."""

# Requirements: REQ-1494

from __future__ import annotations

from typing import Any

from provisa.api.admin.types import MutationResult
from provisa.fakes.checks import (
    Child,
    DeclaredColumn,
    Join,
    check_joins,
    check_table,
    model_reads,
    model_cycle,
)
from provisa.fakes.kinds import FakeKind, FakeRefused


def _children(
    table_id: int, tables: dict[int, dict], relationships: list[dict]
) -> dict[str, Child]:
    """The tables joined to ``table_id`` as its children, by the name a ``sql_group`` fake reads
    them through: a one-to-many relationship's field name on the parent, or, for a many-to-one
    relationship declared on the child, the child table's name."""
    out: dict[str, Child] = {}
    for r in relationships:
        if r.get("target_table_id") is None or r.get("via_table_id") is not None:
            continue
        if r["cardinality"] == "one-to-many" and r["source_table_id"] == table_id:
            child, name = tables.get(r["target_table_id"]), r["graphql_alias"]
        elif r["cardinality"] == "many-to-one" and r["target_table_id"] == table_id:
            child = tables.get(r["source_table_id"])
            name = child["table_name"] if child is not None else None
        else:
            continue
        if child is None or name is None:
            continue
        out[name] = Child(
            child["table_name"], {c["column_name"]: c["data_type"] for c in child["columns"]}
        )
    return out


def _declared(table: dict) -> list[DeclaredColumn]:
    cols = []
    for c in table["columns"]:
        if c.get("data_type") is None:
            raise FakeRefused(f"{table['table_name']}.{c['column_name']} has no data type")
        cols.append(
            DeclaredColumn(
                c["column_name"],
                c["data_type"],
                c.get("fake"),
                bool(c.get("fake_stable")),
                c.get("synthetic_rule"),
            )
        )
    return cols


def _declares(c: dict) -> bool:
    return bool(c.get("fake") or c.get("fake_stable") or c.get("synthetic_rule"))


def check_model(tables: list[dict], relationships: list[dict]) -> None:
    """Refuse, by name, the first fake the model's tables cannot hold. ``tables`` as
    :func:`provisa.api.admin.db_queries.fetch_tables` returns them."""
    by_id = {t["id"]: t for t in tables}
    fakes: dict[tuple[str, str], tuple[FakeKind, bool]] = {}
    reads: dict[tuple[str, str], frozenset[tuple[str, str]]] = {}
    for t in tables:
        if not any(_declares(c) for c in t["columns"]):
            continue
        cols = _declared(t)
        children = _children(t["id"], by_id, relationships)
        checked = check_table(t["table_name"], cols, children)
        stable = {c.name: c.stable for c in cols}
        for name, kind in checked.fakes.items():
            fakes[(t["table_name"], name)] = (kind, stable[name])
        for name in {*checked.fakes, *checked.rules}:
            generated = checked.generated(name)
            assert generated is not None  # the column declares a fake or a rule
            reads[(t["table_name"], name)] = model_reads(t["table_name"], generated, children)
    # A cycle through a parent's children, across tables, among what generation computes
    # (REQ-1939, GENERATION IN PASSES).
    cycle = model_cycle(reads)
    if cycle:
        names = [".".join(n) for n in cycle]
        raise FakeRefused(
            f"the synthetic rules and fakes of {', '.join(names)} name one another in a cycle "
            f"({' -> '.join(names + [names[0]])})"
        )
    joins = []
    for r in relationships:
        if r.get("target_table_id") is None or r.get("target_column") is None:
            continue
        one, other = by_id.get(r["source_table_id"]), by_id.get(r["target_table_id"])
        if one is None or other is None:
            continue
        joins.append(
            Join(
                r["id"],
                (one["table_name"], r["source_column"]),
                (other["table_name"], r["target_column"]),
            )
        )
    check_joins(fakes, joins)


def _with_model(tables: list[dict], model: Any) -> list[dict]:
    """``tables`` with ``model``'s columns in place of its stored ones, or added when new."""
    columns = [
        {
            "column_name": c.name,
            "data_type": c.data_type,
            "fake": c.fake,
            "fake_stable": c.fake_stable,
            "synthetic_rule": c.synthetic_rule,
        }
        for c in model.columns
    ]
    out = []
    replaced = False
    for t in tables:
        same = (t["source_id"], t["schema_name"], t["table_name"]) == (
            model.source_id,
            model.schema_name,
            model.table_name,
        )
        if same:
            replaced = True
            out.append({**t, "columns": columns})
        else:
            out.append(t)
    if not replaced:
        out.append({"id": None, "table_name": model.table_name, "columns": columns})
    return out


async def table_fake_refusal(conn: Any, model: Any) -> MutationResult | None:
    """A failing MutationResult naming the first fake ``model`` -- a table about to be saved --
    cannot hold, in the model as it will stand; None otherwise."""
    if not any(c.fake or c.fake_stable or c.synthetic_rule for c in model.columns):
        return None
    from provisa.api.admin.db_queries import fetch_relationships, fetch_tables

    try:
        check_model(_with_model(await fetch_tables(conn), model), await fetch_relationships(conn))
    except FakeRefused as exc:
        return MutationResult(
            success=False,
            message=str(exc),
            code="schema.fake_refused",
            params={"table": model.table_name},
        )
    return None


class FakeRefusedSave(Exception):
    """A save undone because the model as saved holds a fake it cannot; ``result`` says why."""

    def __init__(self, result: MutationResult) -> None:
        super().__init__(result.message)
        self.result = result


async def relationship_fake_refusal(conn: Any, relationship_id: str) -> None:
    """Raise FakeRefusedSave when the model, with relationship ``relationship_id`` just saved,
    holds a fake it cannot -- its two columns faked differently, or a ``sql_group`` fake reading
    children it no longer reaches."""
    from provisa.api.admin.db_queries import fetch_relationships, fetch_tables

    try:
        check_model(await fetch_tables(conn), await fetch_relationships(conn))
    except FakeRefused as exc:
        raise FakeRefusedSave(
            MutationResult(
                success=False,
                message=str(exc),
                code="schema.fake_refused",
                params={"relationship": relationship_id},
            )
        ) from exc
