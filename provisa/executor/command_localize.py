# Copyright (c) 2026 Kenneth Stott
# Canary: b2e8c1a4-5f37-49d6-9c02-7a1e3d8f4b60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Inline command localization for composed SQL (REQ-1159).

A registered command (tracked function) may appear INLINE in a larger SQL statement — joined,
sub-queried, or projected — not only as a standalone ``SELECT * FROM fn(args)``. A command is an
external/hosted relation the fed engine cannot plan, so it is LOCALIZED before routing: every command
call in the parsed tree is detected, executed once (via the one shared ``invoke_tracked_function``
executor, so governance + the REQ-1159 I/O contract are enforced identically to a direct call), and
its call site is rewritten to a local relation carrying the returned rows. The rest of the statement
then runs through the normal governed pipeline over that local relation.

Substitution is SIZE-ADAPTIVE: at or below ``inline_values_max_rows`` the rows inline as a typed
``VALUES`` list (portable, no engine state); above it the rows are handed to the engine as a
registered relation (scales, zero-copy) — the registration seam is injected so this module stays
pure/testable. Both forms yield the SAME typed, aliased relation, so everything downstream (routing,
governance, lineage) is substitution-agnostic.

Because a localized statement carries an inline local relation, it CANNOT be pushed whole to a remote
source; the caller forces local (engine) execution when :func:`localize_commands` reports a hit.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import cast

import sqlglot
from sqlglot import expressions as exp

from provisa.core.ir_types import to_physical

# Execute one command by name with positional-or-named args, returning its rows. Injected so this
# module never imports the dispatch stack directly (keeps it unit-testable with a fake executor).
CommandRunner = Callable[[str, dict], Awaitable[list[dict]]]

# Register a batch of rows as a named local relation for the active engine, returning the relation
# name to reference. Injected; only used on the large-set path.
RelationRegistrar = Callable[[str, list[dict], list[str]], Awaitable[str]]

_DEFAULT_VALUES_MAX_ROWS = 1000


def find_command_calls(tree: exp.Expression, command_names: frozenset[str]) -> list[exp.Table]:
    """Every table-position command call in ``tree`` — an ``exp.Table`` whose ``this`` is an
    ``exp.Anonymous`` naming a registered command (e.g. ``FROM enrich('x') e``).

    Detect-ALL (not first-match): a statement may compose several commands. Scalar-position calls
    (``SELECT fn(x)``) are NOT localized here — those remain the standalone direct-invocation path."""
    hits: list[exp.Table] = []
    for tbl in tree.find_all(exp.Table):
        inner = tbl.this
        if isinstance(inner, exp.Anonymous) and inner.name in command_names:
            hits.append(tbl)
    return hits


def _table_alias(tbl: exp.Table, index: int) -> str:
    """The alias the localized relation takes. A composed call is aliased (``fn(...) AS e``) so the
    query references its columns by that alias; a bare standalone call (``FROM fn(...)``) carries
    none, so a stable synthetic alias is minted — nothing references it in that case."""
    alias = tbl.args.get("alias")
    if alias is not None and alias.this:
        return alias.this.name
    return f"_cmd{index}"


def _arg_values(tbl: exp.Table) -> list:
    """Positional literal argument values of a command call (strings/numbers/bools/null)."""
    anon = tbl.this
    return [_literal(a) for a in anon.expressions]


def _literal(node: exp.Expression):
    if isinstance(node, exp.Literal):
        if node.is_string:
            return node.this
        text = node.this
        try:
            return int(text)
        except ValueError:
            return float(text)
    if isinstance(node, exp.Boolean):
        return bool(node.this)
    if isinstance(node, exp.Null):
        return None
    return node.sql()


# sqlglot dialect name -> SQLAlchemy dialect name for to_physical (they diverge: sqlglot 'postgres'
# is SQLAlchemy 'postgresql'). Same-named dialects (duckdb, mysql, sqlite) fall through unchanged.
_SA_DIALECT: dict[str, str] = {"postgres": "postgresql"}


def _cast_sql(value, ir_type: str | None, dialect: str) -> str:
    """A single VALUES cell rendered as SQL, cast to its IR type's physical form when known so the
    inline relation's column types are pinned (not left to the engine's literal inference)."""
    lit = _render_literal(value)
    if ir_type is None:
        return lit
    return f"CAST({lit} AS {to_physical(ir_type, _SA_DIALECT.get(dialect, dialect))})"


def _render_literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _values_source(
    rows: list[dict], alias: str, columns: list[str], types: dict[str, str], dialect: str
) -> exp.Expression:
    """Build a typed ``(VALUES …) AS alias(cols)`` relation node from executed rows.

    The FIRST row's cells are CAST to the declared IR types (physical form) to fix the relation's
    column types; later rows inherit them. Column order is the declared/first-row order."""
    row_sqls = []
    for i, row in enumerate(rows):
        cells = [
            _cast_sql(row.get(c), types.get(c) if i == 0 else None, dialect) for c in columns
        ]
        row_sqls.append("(" + ", ".join(cells) + ")")
    body = ", ".join(row_sqls)
    col_list = ", ".join(columns)
    wrapper = sqlglot.parse_one(
        f"SELECT 1 FROM (VALUES {body}) AS {alias}({col_list})", read=dialect
    )
    return _from_source(cast(exp.Expression, wrapper))


def _empty_source(alias: str, columns: list[str], types: dict[str, str], dialect: str) -> exp.Expression:
    """An empty typed relation (zero returned rows): ``SELECT … WHERE FALSE`` projecting the declared
    columns, so the composed query still type-checks and yields no rows for this command."""
    projs = ", ".join(f"{_cast_sql(None, types.get(c), dialect)} AS {c}" for c in columns)
    wrapper = sqlglot.parse_one(
        f"SELECT 1 FROM (SELECT {projs} WHERE FALSE) AS {alias}", read=dialect
    )
    return _from_source(cast(exp.Expression, wrapper))


def _from_source(wrapper: exp.Expression) -> exp.Expression:
    """The FROM-clause source node of a single-source SELECT wrapper (fail loud if malformed)."""
    frm = wrapper.find(exp.From)
    if frm is None or frm.this is None:
        raise ValueError(f"expected a FROM source in constructed wrapper: {wrapper.sql()!r}")
    return frm.this


def _output_spec(command: dict, rows: list[dict]) -> tuple[list[str], dict[str, str]]:
    """(ordered column names, {col: ir_type}) for a command's output — from its declared
    ``output_columns`` contract (REQ-1159) when present, else inferred from the first returned row
    (names only, untyped)."""
    declared = command.get("output_columns")
    if declared:
        cols = [c["name"] for c in declared]
        types = {c["name"]: c.get("type") for c in declared}
        return cols, types
    if rows:
        return list(rows[0].keys()), {}
    raise ValueError(
        f"command {command.get('name')!r} returned no rows and declares no output_columns — "
        "cannot determine the inline relation's columns (declare output_columns, REQ-1159)"
    )


async def localize_commands(
    tree: exp.Expression,
    commands: dict[str, dict],
    run: CommandRunner,
    *,
    dialect: str = "duckdb",
    values_max_rows: int = _DEFAULT_VALUES_MAX_ROWS,
    register_relation: RelationRegistrar | None = None,
) -> bool:
    """Rewrite every inline command call in ``tree`` to a local relation, in place.

    Returns True when at least one command was localized (the caller must then force local execution:
    an inline local relation cannot be pushed to a remote source). Each command executes at most once
    per (name, args) within the statement — a repeated reference reuses the cached result. ``commands``
    maps command name → its registry dict (for the output contract). ``run`` executes a command via
    the shared governed executor. Large results (> ``values_max_rows``) go through ``register_relation``
    when supplied; without it, a large result still inlines as VALUES (correct, just larger SQL)."""
    hits = find_command_calls(tree, frozenset(commands))
    if not hits:
        return False
    # Cache the EXECUTED result per (name, args) — a command runs at most once per statement; each
    # call site then builds its own source node with that site's alias.
    cache: dict[tuple, tuple[list[dict], list[str], dict[str, str], str | None]] = {}
    for index, tbl in enumerate(hits):
        name = tbl.this.name
        alias = _table_alias(tbl, index)
        arg_values = _arg_values(tbl)
        key = (name, tuple(arg_values))
        if key not in cache:
            args = {f"a{i}": v for i, v in enumerate(arg_values)}
            rows = await run(name, args)
            cols, types = _output_spec(commands[name], rows)
            rel: str | None = None
            if rows and register_relation is not None and len(rows) > values_max_rows:
                rel = await register_relation(name, rows, cols)
            cache[key] = (rows, cols, types, rel)
        rows, cols, types, rel = cache[key]
        if rel is not None:  # large: reference the registered relation, aliased
            source: exp.Expression = cast(exp.Expression, sqlglot.to_table(rel).as_(alias))
        elif not rows:
            source = _empty_source(alias, cols, types, dialect)
        else:
            source = _values_source(rows, alias, cols, types, dialect)
        tbl.replace(source)
    return True
