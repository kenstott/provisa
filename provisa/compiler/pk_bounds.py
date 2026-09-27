# Copyright (c) 2026 Kenneth Stott
# Canary: 9b308840-389c-4b6f-b2bd-a73e08142493
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Resolve concrete primary-key bounds for row-materialized tables (REQ-1865).

Walks WHERE/JOIN-ON equality, IN-list, and OR-of-equalities predicates -- the same
``exp.Where``-walking pattern ``provisa.compiler.nf_extractor`` and ``provisa.compiler.rls`` already
use -- looking for a bounded PK-equality shape against any table named in ``row_materialized_tables``.
A table with no resolvable bound is simply absent from the result (most statements don't touch a
row-materialized table at all, and most that do aren't point lookups): never an error. See
docs/arch/row_level_materializer_design.md section 3.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import sqlglot.expressions as exp

if TYPE_CHECKING:
    from provisa.core.models import Table


@dataclass(frozen=True)
class JoinEdge:
    """A ``JOIN ... ON left.col = right.col`` equality (REQ-1865 key pushdown). Column-to-column
    only -- an edge with a literal on either side is a bound, not an edge (``extract_pk_bounds``
    already resolves those); this is for a row-materialize table reached ONLY through a chain of
    FK joins, with no literal predicate naming it directly at all (e.g. bench_contains_edge joined
    on order_id, with the literal bound sitting on customer_id three hops upstream)."""

    left_table: str
    left_column: str
    right_table: str
    right_column: str


@dataclass(frozen=True)
class PkBound:
    """A row-materialized table this statement can serve from the row cache, and the concrete PK
    value(s) its predicate resolves to."""

    source_id: str
    schema_name: str
    table_name: str
    pk_columns: tuple[str, ...]
    # One tuple of literal values per matched row, in pk_columns order.
    values: tuple[tuple[Any, ...], ...]


def _flatten_and(
    expr: exp.Expr,
) -> list[exp.Expr]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    if isinstance(expr, exp.And):
        return _flatten_and(expr.left) + _flatten_and(expr.right)
    return [expr]


def _flatten_or(
    expr: exp.Expr,
) -> list[exp.Expr]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    if isinstance(expr, exp.Or):
        return _flatten_or(expr.left) + _flatten_or(expr.right)
    return [expr]


def _literal_value(e: exp.Expr, params: list[Any] | None = None) -> tuple[Any, bool]:
    """(value, resolved) -- resolved=False for anything not a concrete value (a subquery, a column
    reference, a function call): the caller must treat that predicate as unbounded, never guess a
    value.

    A bound PARAMETER (``$1``/``?``) resolves too, from ``params`` (1-indexed, matching the bind
    order SQLGlot's postgres dialect renders a placeholder in) -- e.g. a Bolt/Cypher-transport
    query never inlines its predicate as a literal (``customer_id = $1``, the actual value bound
    separately), unlike the SQL-transport benchmark's own literal-inlined text. Without this, EVERY
    Bolt-transport query against a row_materialize table resolved zero bounds and silently fell
    back to a full-table land on every call -- confirmed live: a trivial single-table `LIMIT 1`
    query with no WHERE at all was the ONLY case that ever should fall back; a bound `WHERE pk =
    $1` should not have, and previously always did. ``params=None`` (the default, when the caller
    has none to offer) preserves the exact old behavior: a parameter placeholder stays unresolved.
    Two placeholder shapes exist depending on how sqlglot parsed the surrounding query (see
    provisa.cypher.translator._reconcile_params_with_placeholders's own docstring for why): a bare
    ``exp.Column`` whose identifier IS the literal text ``"$1"`` (the text/UNWIND parse path), or
    ``exp.Parameter(this=exp.Literal(this=1))`` (the AST/predicate parse path this module's own
    ``sqlglot.parse_one(..., read="postgres")`` call uses) -- both handled."""
    if isinstance(e, exp.Literal):
        if e.is_string:
            return e.this, True
        raw = e.this
        try:
            return (float(raw) if "." in str(raw) else int(raw)), True
        except (TypeError, ValueError):
            return None, False
    if isinstance(e, exp.Boolean):
        return bool(e.this), True
    if isinstance(e, exp.Null):
        return None, True
    if params:
        idx: int | None = None
        if isinstance(e, exp.Parameter) and isinstance(e.this, exp.Literal):
            try:
                idx = int(e.this.this)
            except (TypeError, ValueError):
                idx = None
        elif isinstance(e, exp.Column) and not e.table:
            name = e.this.this if isinstance(e.this, exp.Identifier) else e.this
            if isinstance(name, str) and name.startswith("$") and name[1:].isdigit():
                idx = int(name[1:])
        if idx is not None and 1 <= idx <= len(params):
            return params[idx - 1], True
    return None, False


def _alias_map(ast: exp.Expr) -> dict[str, exp.Table]:
    """alias (or bare table name when unaliased) -> the ``exp.Table`` node it names."""
    out: dict[str, exp.Table] = {}
    for t in ast.find_all(exp.Table):
        out[t.alias_or_name] = t
        out[t.name] = t
    return out


def _matches_pk_column(col: exp.Expr, aliases: set[str], pk_columns: tuple[str, ...]) -> str | None:
    if not isinstance(col, exp.Column):
        return None
    if col.name not in pk_columns:
        return None
    table_ref = col.table
    # An unqualified column reference is accepted only when exactly one candidate alias exists
    # for this table (the common single-table-reference case); a qualified reference must match
    # one of this table's known aliases.
    if table_ref:
        return col.name if table_ref in aliases else None
    return col.name


def _match_pk_eq(
    cond: exp.Expr, aliases: set[str], pk_columns: tuple[str, ...]
) -> tuple[str | None, exp.Expr | None]:
    if not isinstance(cond, exp.EQ):
        return None, None
    left, right = cond.left, cond.right
    col = _matches_pk_column(left, aliases, pk_columns)
    if col is not None:
        return col, right
    col = _matches_pk_column(right, aliases, pk_columns)
    if col is not None:
        return col, left
    return None, None


def _match_pk_in(
    cond: exp.Expr, aliases: set[str], pk_columns: tuple[str, ...]
) -> tuple[str | None, list[exp.Expr]]:
    if not isinstance(cond, exp.In):
        return None, []
    col = _matches_pk_column(cond.this, aliases, pk_columns)
    if col is None:
        return None, []
    expressions = cond.args.get("expressions") or []
    return col, list(expressions)


def _collect_predicates(ast: exp.Expr) -> list[exp.Expr]:
    predicates: list[exp.Expr] = []
    where = ast.find(exp.Where)
    if where is not None:
        predicates.append(where.this)
    for join in ast.find_all(exp.Join):
        on = join.args.get("on")
        if on is not None:
            predicates.append(on)
    return predicates


def extract_join_edges(ast: exp.Expr) -> list[JoinEdge]:
    """Every ``JOIN ... ON left.col = right.col`` column-to-column equality in the statement
    (REQ-1865 key pushdown). Symmetric candidates aren't de-duplicated or oriented -- the caller
    walks edges in both directions during its fixpoint anyway."""
    alias_map = _alias_map(ast)
    edges: list[JoinEdge] = []
    for join in ast.find_all(exp.Join):
        on = join.args.get("on")
        if on is None:
            continue
        for cond in _flatten_and(on):
            if not isinstance(cond, exp.EQ):
                continue
            left, right = cond.left, cond.right
            if not (isinstance(left, exp.Column) and isinstance(right, exp.Column)):
                continue
            left_table = alias_map.get(left.table) if left.table else None
            right_table = alias_map.get(right.table) if right.table else None
            if left_table is None or right_table is None:
                continue
            edges.append(
                JoinEdge(
                    left_table=left_table.name,
                    left_column=left.name,
                    right_table=right_table.name,
                    right_column=right.name,
                )
            )
    return edges


def extract_pk_bounds(
    ast: exp.Expr, row_materialized_tables: dict[str, "Table"], params: list[Any] | None = None
) -> list[PkBound]:
    """Walk WHERE/JOIN-ON equality and IN predicates; for every row-materialized table referenced,
    resolve its PK columns' literal (or bound-PARAMETER, see ``params``) values. A table with no
    resolvable bound is simply absent from the result -- never an error (most statements don't
    touch a row-materialized table at all).

    ``row_materialized_tables`` maps bare table_name -> the registered ``Table`` (row_materialize
    already validated True with >=1 is_primary_key column, per ``Table._validate_row_materialize``).

    ``params`` (REQ-1865 amendment) is the statement's own bind-parameter values, in bind order --
    a Bolt/Cypher-transport statement's predicate is NEVER inlined as a literal (``customer_id =
    $1``, the value bound out-of-band), unlike the SQL-transport's own literal-inlined text.
    Without it, ``_literal_value`` cannot resolve a ``$N`` placeholder and every such statement
    silently fell back to a full-table land, defeating row_materialize for that whole transport."""
    if not row_materialized_tables:
        return []

    alias_map = _alias_map(ast)
    predicates = _collect_predicates(ast)
    bounds: list[PkBound] = []

    for table_name, table in row_materialized_tables.items():
        pk_columns = tuple(c.name for c in table.columns if c.is_primary_key)
        if not pk_columns:
            continue
        aliases = {alias for alias, node in alias_map.items() if node.name == table_name}
        if not aliases:
            continue

        col_values: dict[str, set[Any]] = {pk: set() for pk in pk_columns}
        resolved_any = False
        for pred in predicates:
            for cond in _flatten_and(pred):
                col, val_expr = _match_pk_eq(cond, aliases, pk_columns)
                if col is not None and val_expr is not None:
                    value, ok = _literal_value(val_expr, params)
                    if ok:
                        col_values[col].add(value)
                        resolved_any = True
                    continue
                col, val_exprs = _match_pk_in(cond, aliases, pk_columns)
                if col is not None:
                    for val_expr in val_exprs:
                        value, ok = _literal_value(val_expr, params)
                        if ok:
                            col_values[col].add(value)
                            resolved_any = True
                    continue
                if isinstance(cond, exp.Or):
                    # An OR-chain of PK equalities (`pk = 1 OR pk = 2`) is the same bounded shape
                    # as an IN-list -- BUT ONLY when every disjunct is itself a PK-equality. A
                    # mixed OR (`pk = 1 OR status = 'x'`) is NOT bounded to pk=1: rows matching
                    # `status = 'x'` regardless of pk also satisfy the real predicate, and the row
                    # cache cannot represent that at all. Treating this as bounded would silently
                    # under-populate the cache and the physical query would then silently return
                    # too few rows -- a correctness bug, not a missed optimization. So: collect
                    # candidate values into a LOCAL set first, and only merge them into
                    # col_values (and set resolved_any) if EVERY disjunct resolved as a PK
                    # equality on the SAME pk column; a single non-PK disjunct, OR a disjunct
                    # naming a DIFFERENT pk column (e.g. `pk1 = 1 OR pk2 = 2` -- a disjunction,
                    # not a composite point lookup), discards the whole OR for this table.
                    or_col: str | None = None
                    or_values: set[Any] = set()
                    or_fully_resolved = True
                    for sub in _flatten_or(cond):
                        col, val_expr = _match_pk_eq(sub, aliases, pk_columns)
                        if (
                            col is None
                            or val_expr is None
                            or (or_col is not None and col != or_col)
                        ):
                            or_fully_resolved = False
                            break
                        value, ok = _literal_value(val_expr, params)
                        if not ok:
                            or_fully_resolved = False
                            break
                        or_col = col
                        or_values.add(value)
                    if or_fully_resolved and or_col is not None:
                        col_values[or_col] |= or_values
                        resolved_any = True

        if not resolved_any:
            continue

        if len(pk_columns) == 1:
            pk = pk_columns[0]
            if not col_values[pk]:
                continue
            values = tuple((v,) for v in col_values[pk])
        else:
            # A composite PK only resolves to a safe bound when every column has exactly ONE
            # resolved value each (a single point lookup) -- an independent IN-list per column
            # does not decompose to a safe cross product without knowing which values pair
            # together, so anything looser than singleton-per-column falls back (absent).
            if any(len(col_values[pk]) != 1 for pk in pk_columns):
                continue
            values = (tuple(next(iter(col_values[pk])) for pk in pk_columns),)

        bounds.append(
            PkBound(
                source_id=table.source_id,
                schema_name=table.schema_name,
                table_name=table.table_name,
                pk_columns=pk_columns,
                values=values,
            )
        )

    return bounds
