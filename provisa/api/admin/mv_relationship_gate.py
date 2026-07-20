# Copyright (c) 2026 Kenneth Stott
# Canary: 8c2f4a91-3d67-4e18-9b05-1a6e3c7f24d8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Relationship-approval gate for materialized-view publication (REQ-1140).

A materialized view may only be published if every relationship its ``view_sql`` joins over is
approved (present in the ``relationships`` table). This module is the PURE decision half:

- ``extract_join_deps`` parses ``view_sql`` (sqlglot) into the equi-join dependencies it relies on.
- ``relationship_present`` checks whether one join dependency is already an approved relationship.
- ``evaluate_gate`` resolves each dependency's tables and returns the missing ones.

The caller (register_table) applies the effect: with relationship-creation rights the missing
relationships are auto-created + approved and publication proceeds; without them, the missing
relationships and the view are queued for approval and publication is blocked until they land.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Awaitable, Callable

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError


@dataclass(frozen=True)
class JoinDep:  # REQ-1140
    """One equi-join dependency of a view: ``left_table.left_column = right_table.right_column``.
    Table names are the virtual (alias-or-name) identifiers as written in the ``view_sql``."""

    left_table: str
    left_column: str
    right_table: str
    right_column: str


@dataclass(frozen=True)
class MissingRelationship:  # REQ-1140
    """A join dependency with no approved relationship, its tables already resolved to ids."""

    dep: JoinDep
    left_table_id: int
    right_table_id: int


@dataclass(frozen=True)
class GateDecision:  # REQ-1140
    """Result of evaluating the gate: every parsed dependency and the subset still unapproved."""

    deps: list[JoinDep]
    missing: list[MissingRelationship]

    @property
    def satisfied(self) -> bool:
        return not self.missing


def _alias_map(statement) -> dict[str, str]:
    """qualifier (alias, else table name) -> real table name, for every table in the statement.

    A column reference ``o.cid`` qualifies by the alias ``o``; this resolves it back to the real
    table name so a dependency is expressed in the same names the relationship rows use."""
    out: dict[str, str] = {}
    for tbl in statement.find_all(exp.Table):
        name = tbl.name
        if not name:
            continue
        alias = tbl.alias or name
        out[alias] = name
        out.setdefault(name, name)
    return out


def extract_join_deps(view_sql: str, dialect: str | None = None) -> list[JoinDep]:
    """Parse ``view_sql`` into its equi-join dependencies (REQ-1140).

    Only column-to-column equalities in a JOIN's ON clause are dependencies — a JOIN on a literal
    predicate, or a comma/implicit cross join, contributes none. Fail-closed on a parse error: an
    unparseable view has undeterminable dependencies, so it is treated as depending on nothing here
    and the determinism/registration checks elsewhere reject it. Deduplicated, order-stable.
    """
    try:
        statement = sqlglot.parse_one(view_sql, dialect=dialect)
    except SqlglotError:
        return []
    if statement is None:
        return []

    aliases = _alias_map(statement)
    seen: set[tuple[str, str, str, str]] = set()
    deps: list[JoinDep] = []
    for join in statement.find_all(exp.Join):
        on = join.args.get("on")
        if on is None:
            continue
        for eq in on.find_all(exp.EQ):
            left, right = eq.this, eq.expression
            if not (isinstance(left, exp.Column) and isinstance(right, exp.Column)):
                continue
            lt = aliases.get(left.table)
            rt = aliases.get(right.table)
            if not lt or not rt or lt == rt:
                continue
            key = (lt, left.name, rt, right.name)
            if key in seen:
                continue
            seen.add(key)
            deps.append(JoinDep(lt, left.name, rt, right.name))
    return deps


def relationship_present(
    relationships: list[dict],
    left_table_id: int,
    left_column: str,
    right_table_id: int,
    right_column: str,
) -> bool:
    """Whether an approved relationship already connects this table/column pair (either direction).

    A relationship row is orientation-agnostic for gate purposes: ``a.x -> b.y`` satisfies a join
    written either way. ``target_column`` may be None on a function-target relationship; such a row
    never matches a column equi-join dependency."""
    for rel in relationships:
        s_id, t_id = rel.get("source_table_id"), rel.get("target_table_id")
        s_col, t_col = rel.get("source_column"), rel.get("target_column")
        if t_id is None or t_col is None:
            continue
        forward = (
            s_id == left_table_id
            and s_col == left_column
            and t_id == right_table_id
            and t_col == right_column
        )
        reverse = (
            s_id == right_table_id
            and s_col == right_column
            and t_id == left_table_id
            and t_col == left_column
        )
        if forward or reverse:
            return True
    return False


async def evaluate_gate(
    *,
    view_sql: str,
    dialect: str | None,
    relationships: list[dict],
    resolve_table_id: Callable[[str], Awaitable[int | None]],
) -> GateDecision:
    """Resolve every join dependency of ``view_sql`` and report which are unapproved (REQ-1140).

    ``resolve_table_id`` maps a virtual table name to its registered-table id (None if unknown). A
    dependency on a table that does not resolve is skipped — it is not a Provisa-tracked relationship
    (e.g. a subquery-derived alias), so it does not gate publication."""
    deps = extract_join_deps(view_sql, dialect)
    missing: list[MissingRelationship] = []
    for dep in deps:
        left_id = await resolve_table_id(dep.left_table)
        right_id = await resolve_table_id(dep.right_table)
        if left_id is None or right_id is None:
            continue
        if not relationship_present(
            relationships, left_id, dep.left_column, right_id, dep.right_column
        ):
            missing.append(MissingRelationship(dep, left_id, right_id))
    return GateDecision(deps=deps, missing=missing)
