# Copyright (c) 2026 Kenneth Stott
# Canary: 6f1a8c35-2d94-4b70-a5e3-9c0b7d4e1f26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One admission for every data write.

An ``INSERT``, ``UPDATE``, ``DELETE`` or ``MERGE`` is admitted in the governance stage of the one
pipeline — the stage a read's row filter and masks are applied in — so every surface that lowers
to SQL gets the same answer. Four rules, in this order:

1. **The right.** The role holds ``write`` (REQ-868).
2. **The columns.** Every column the statement writes names the role in its ``writable_by``
   (REQ-663). A column that declares no ``writable_by`` is writable by nobody. ``writable_by`` is
   the whole rule for a column: whether the role can read it (visibility, masks) is not consulted,
   so a column a role cannot read is writable by it only when ``writable_by`` says so. INSERT
   writes the columns it lists (all of the table's when it lists none); UPDATE the columns it
   sets; DELETE and MERGE act on whole rows and need every column.
3. **The rows it may touch.** The role's row filter — the same predicate, built by the same
   function, that its reads carry — is ANDed into the WHERE of an UPDATE or DELETE.
4. **The rows it leaves behind.** A filtered role may only write rows it could then see. An
   INSERT's rows, and the new values of an UPDATE that sets a column the filter reads, are
   checked against the filter before anything is sent to a source: the supplied values are put
   in the filter's place for those columns and the resulting constant predicate is evaluated in
   this process, with no round trip and no write. The answer given here decides admission; the
   source is not consulted, so where the source would compare differently (a case-insensitive
   collation, an implicit cast) the write is admitted or refused by this answer.

   Never admitted on doubt. Whatever cannot be decided here is refused by name: an
   ``INSERT … SELECT``, a MERGE, a value that is neither a literal nor a bound parameter, a
   filter that reads a column the statement does not supply or another table, a filter using a
   function or operator the evaluator does not have, a comparison it cannot make between the
   supplied types, and any error while evaluating. A statement is never rewritten to change
   only some of the rows it matched.

Nothing is written first and checked afterwards."""

from __future__ import annotations

from provisa.compiler.sql_literals import sql_literal

import re
from typing import TYPE_CHECKING

import sqlglot
import sqlglot.expressions as exp

from provisa.compiler.definitions import NotAvailableHere

from provisa.compiler.rls import _qualified_predicate, cast_session_terms
from provisa.security.mutation_authz import ColumnNotWritable

if TYPE_CHECKING:
    from provisa.compiler.stage2 import GovernanceContext

WRITES = (exp.Insert, exp.Update, exp.Delete, exp.Merge)

_SESSION_TERM = re.compile(r"current_setting\(\s*'provisa\.([A-Za-z0-9_]+)'\s*\)", re.IGNORECASE)


class WriteNotAdmitted(PermissionError):
    """A data write the role's rights, or its row filter, do not admit."""


class WriteNotSupported(NotAvailableHere):
    """A data write the table's source cannot take (executor/write_capability.py) — refused
    whatever the role holds, naming the table and the operation."""

    def __init__(self, table: str, operation: str) -> None:
        self.table = table
        self.operation = operation
        super().__init__(f"{table!r} does not take {operation.upper()}: its source cannot carry it")


WRITE_OPS: tuple[str, ...] = ("insert", "update", "delete")
_OPERATIONS = {"INSERT": ("insert",), "UPDATE": ("update",), "DELETE": ("delete",)}


def require_write_op(gov: "GovernanceContext", table_id: int, name: str, kind: str) -> None:
    """Refuse ``kind`` (INSERT, UPDATE, DELETE, MERGE) on a table whose source cannot take it. A
    MERGE needs every operation it may perform; a table whose capability is unknown takes none."""
    offered = gov.write_ops.get(table_id, frozenset())
    needed = _OPERATIONS.get(kind, WRITE_OPS)
    for operation in needed:
        if operation not in offered:
            raise WriteNotSupported(name, operation)


def is_write(tree: exp.Expression) -> bool:
    return isinstance(tree, WRITES)


def _kind(tree: exp.Expression) -> str:
    return type(tree).__name__.upper()


def _target(tree: exp.Expression) -> tuple[exp.Table, list[str] | None]:
    """The table a write targets, and for an INSERT the columns it lists (None: none listed)."""
    this = tree.this
    if isinstance(this, exp.Schema):
        return this.this, [c.name for c in this.expressions]
    if isinstance(this, exp.Table):
        return this, None
    raise WriteNotAdmitted(f"{_kind(tree)}: the statement names no table to write")


def _resolve(table: exp.Table, gov: "GovernanceContext") -> int:
    from provisa.compiler.stage2 import _table_id_for_node

    table_id = _table_id_for_node(table, gov)
    if table_id is None:
        raise WriteNotAdmitted(f"{table.sql(dialect='postgres')} is not a registered table")
    return table_id


def _require_columns(gov: "GovernanceContext", table_id: int, columns: list[str]) -> None:
    writable = gov.writable_columns.get(table_id, frozenset())
    for column in columns:
        if column not in writable:
            raise ColumnNotWritable(gov.role_id, column)


def _with_session_values(predicate: str, session_vars: dict[str, str]) -> str:
    """``predicate`` with each session term replaced by the request's value, NULL when unbound —
    what the pipeline does to a governed read (REQ-1682)."""

    def _value(match: re.Match) -> str:
        value = session_vars.get(match.group(1))
        return "NULL" if value is None else sql_literal(value, "postgres")

    return _SESSION_TERM.sub(_value, predicate)


def _holds(predicate: exp.Expression) -> bool | None:
    """Whether a predicate that reads no column is true (False also for NULL: a filter that is
    not true does not admit a row). None when it cannot be decided here — it still reads a
    column or another table, or evaluating it failed for any reason — which every caller treats
    as a refusal, never as true.

    The filter arrives as the governed read predicate (the stored filter text parsed as
    PostgreSQL, the dialect filters are written and governed in); it is evaluated as that tree by
    the SQL library's own executor, in this process. It is not rendered to any engine's dialect
    and nothing is sent anywhere."""
    if predicate.find(exp.Column) is not None or predicate.find(exp.Select) is not None:
        return None
    from sqlglot.executor import execute  # noqa: PLC0415

    try:
        result = execute(exp.select(exp.alias_(predicate.copy(), "admitted")))
    except Exception:  # noqa: BLE001  # allow-blind-except: an evaluator failure of ANY kind (a function it lacks, a comparison between types, a planner error) must become "not decided", which refuses the write; letting one kind through would be a 500 on a statement the caller can fix, and none may ever admit
        return None
    return bool(result.rows) and result.rows[0][0] is True


def _supplied(value: exp.Expression, params: list | None) -> exp.Expression | None:
    """The value a statement supplies, as a literal: itself when it is one, the bound value when
    it is a parameter (``$n``) and the request's values are known, else None (not decidable
    before the write)."""
    if isinstance(value, (exp.Literal, exp.Null, exp.Boolean)):
        return value
    if isinstance(value, exp.Neg) and isinstance(value.this, exp.Literal):
        return value
    if isinstance(value, exp.Parameter) and params is not None:
        position = value.this
        if isinstance(position, exp.Literal) and not position.is_string:
            index = int(position.name) - 1
            if 0 <= index < len(params):
                return exp.convert(params[index])  # pyright: ignore[reportReturnType]  # sqlglot stub types convert as Expr
    return None


def _filter_over(
    gov: "GovernanceContext",
    table_id: int,
    session_vars: dict[str, str],
    values: dict[str, exp.Expression],
) -> exp.Expression:
    """The role's row filter on ``table_id`` with every column in ``values`` replaced by the
    expression the statement supplies for it."""
    column_types = dict(gov.all_columns.get(table_id, []))
    predicate = _qualified_predicate(gov.rls_rules[table_id], None)
    cast_session_terms(predicate, column_types)
    resolved: exp.Expression = sqlglot.parse_one(  # pyright: ignore[reportAssignmentType]  # sqlglot stub types parse_one as Expr
        _with_session_values(predicate.sql(dialect="postgres"), session_vars), read="postgres"
    )
    for column in list(resolved.find_all(exp.Column)):
        if column.name in values:
            column.replace(exp.paren(values[column.name].copy()))
    return resolved


def _admit_inserted_rows(
    tree: exp.Insert,
    gov: "GovernanceContext",
    table_id: int,
    table: exp.Table,
    listed: list[str] | None,
    session_vars: dict[str, str],
    params: list | None,
) -> None:
    name = table.name
    source = tree.args.get("expression")
    if not isinstance(source, exp.Values):
        raise WriteNotAdmitted(
            f"INSERT into {name!r}: role {gov.role_id!r} has a row filter on this table, and the "
            "rows of an INSERT … SELECT cannot be checked against it before they are written. "
            "Insert the rows as VALUES."
        )
    columns = listed if listed is not None else [c for c, _ in gov.all_columns.get(table_id, [])]
    for row in source.expressions:
        supplied: dict[str, exp.Expression] = {}
        for column, value in zip(columns, row.expressions, strict=False):
            literal = _supplied(value, params)
            if literal is None:
                raise WriteNotAdmitted(
                    f"INSERT into {name!r}: role {gov.role_id!r} has a row filter on this table, "
                    "and a value that is neither a literal nor a bound parameter cannot be "
                    "checked against it before the row is written. Supply the values as "
                    "literals."
                )
            supplied[column] = literal
        _admit_row(gov, table_id, name, supplied, session_vars)


def _admit_row(
    gov: "GovernanceContext",
    table_id: int,
    name: str,
    supplied: dict[str, exp.Expression],
    session_vars: dict[str, str],
) -> None:
    """Refuse a new row that the role's row filter on the table would not let it read."""
    outcome = _holds(_filter_over(gov, table_id, session_vars, supplied))
    if outcome is None:
        raise WriteNotAdmitted(
            f"INSERT into {name!r}: role {gov.role_id!r} has a row filter on this table that "
            "cannot be decided before writing — the filter reads a column the INSERT does not "
            "supply, or uses a function or comparison that cannot be evaluated before the row "
            "exists. Supply every column the filter reads; a filter that cannot be evaluated "
            "ahead of a write admits no INSERT by this role."
        )
    if not outcome:
        raise WriteNotAdmitted(
            f"INSERT into {name!r}: the row is outside role {gov.role_id!r}'s row filter on "
            "this table. A role may only write rows it could then read."
        )


def admit_rows(
    gov: "GovernanceContext",
    table_id: int,
    name: str,
    columns: list[str],
    rows: list[list] | None = None,
    session_vars: dict[str, str] | None = None,
) -> None:
    """Admit a bulk load — rows given as values, not as a statement (pgwire ``COPY … FROM
    STDIN``) — by the rules an INSERT of the same columns is admitted by. Called once with
    ``rows`` None before any data is read (the right and the columns), and again with the rows
    before any of them is written (the row filter)."""
    require_write_op(gov, table_id, name, "INSERT")
    if not gov.can_write:
        raise WriteNotAdmitted(
            f"COPY into {name!r}: role {gov.role_id!r} does not hold the 'write' right"
        )
    _require_columns(gov, table_id, columns)
    if rows is None or table_id not in gov.rls_rules:
        return
    for row in rows:
        supplied: dict[str, exp.Expression] = {
            column: exp.convert(value)  # pyright: ignore[reportAssignmentType]  # sqlglot stub types convert as Expr
            for column, value in zip(columns, row, strict=False)
        }
        _admit_row(gov, table_id, name, supplied, session_vars or {})


def _admit_new_values(
    tree: exp.Update,
    gov: "GovernanceContext",
    table_id: int,
    table: exp.Table,
    session_vars: dict[str, str],
    params: list | None,
) -> None:
    """Refuse an UPDATE whose new values would put a row outside the role's row filter, or
    whose outcome cannot be decided before writing. An UPDATE that sets no column the filter
    reads needs nothing here: the filter in its WHERE is the whole check."""
    read_by_filter = {
        c.name for c in _qualified_predicate(gov.rls_rules[table_id], None).find_all(exp.Column)
    }
    name = table.name
    assignments: dict[str, exp.Expression] = {}
    for assignment in tree.expressions:
        if not isinstance(assignment, exp.EQ):
            continue
        column = assignment.this.name
        if column not in read_by_filter:
            continue
        literal = _supplied(assignment.expression, params)
        if literal is None:
            raise WriteNotAdmitted(
                f"UPDATE of {name!r}: role {gov.role_id!r} has a row filter on this table that "
                f"reads column {column!r}, and the new value of {column!r} is neither a literal "
                "nor a bound parameter, so whether each row stays inside the filter cannot be "
                "decided before writing. Set the column to a literal value."
            )
        assignments[column] = literal
    if not assignments:
        return
    outcome = _holds(_filter_over(gov, table_id, session_vars, assignments))
    if outcome is None:
        raise WriteNotAdmitted(
            f"UPDATE of {name!r}: role {gov.role_id!r} has a row filter on this table that "
            "cannot be decided before writing — it reads a column this UPDATE does not set, or "
            "uses a function or comparison that cannot be evaluated before the row is written. "
            "Set every column the filter reads to a literal value."
        )
    if not outcome:
        raise WriteNotAdmitted(
            f"UPDATE of {name!r}: the new values put the row outside role {gov.role_id!r}'s row "
            "filter on this table. A role may only write rows it could then read."
        )


def written_columns(tree: exp.Expression, gov: "GovernanceContext", table_id: int) -> list[str]:
    """The columns a write writes: an INSERT the columns it lists (all of the table's when it
    lists none), an UPDATE the columns it sets, a DELETE or MERGE every column (whole rows)."""
    every_column = [c for c, _ in gov.all_columns.get(table_id, [])]
    if isinstance(tree, exp.Insert):
        listed = _target(tree)[1]
        return listed if listed is not None else every_column
    if isinstance(tree, exp.Update):
        return [a.this.name for a in tree.expressions if isinstance(a, exp.EQ)]
    return every_column


def written_table_id(tree: exp.Expression, gov: "GovernanceContext") -> int:
    """The registered table a write targets — what the steps after a successful write act on
    (its cached reads dropped, its views marked stale, its change event and sinks)."""
    return _resolve(_target(tree)[0], gov)


def admit_write(
    tree: exp.Expression,
    gov: "GovernanceContext",
    session_vars: dict[str, str] | None = None,
    params: list | None = None,
) -> exp.Expression:
    """Admit ``tree`` — an INSERT, UPDATE, DELETE or MERGE — for the role ``gov`` was built for,
    and return it with the role's row filter in place. ``params`` are the request's bound
    values (``$1`` …), which the new-row check reads where the statement supplies a value as a
    parameter. Raises :class:`WriteNotAdmitted` or
    :class:`ColumnNotWritable` (both ``PermissionError``) when the write is not admitted."""
    session_vars = session_vars or {}
    kind = _kind(tree)
    table, listed = _target(tree)
    table_id = _resolve(table, gov)
    require_write_op(gov, table_id, table.name, kind)
    if not gov.can_write:
        raise WriteNotAdmitted(
            f"{kind} on {table.name!r}: role {gov.role_id!r} does not hold the 'write' right"
        )

    _require_columns(gov, table_id, written_columns(tree, gov, table_id))

    if table_id not in gov.rls_rules:
        return tree

    if isinstance(tree, exp.Merge):
        raise WriteNotAdmitted(
            f"MERGE into {table.name!r}: role {gov.role_id!r} has a row filter on this table, and "
            "the rows a MERGE would write cannot be checked against it. Use INSERT and UPDATE."
        )
    if isinstance(tree, exp.Insert):
        _admit_inserted_rows(tree, gov, table_id, table, listed, session_vars, params)
        return tree

    # UPDATE / DELETE: the rows it may touch are the rows the role can read — the read filter,
    # qualified and typed by the function reads use.
    from provisa.compiler.stage2 import _alias_for  # noqa: PLC0415
    from provisa.compiler.rls import _qualify_filter  # noqa: PLC0415

    column_types = dict(gov.all_columns.get(table_id, []))
    readable = _qualify_filter(gov.rls_rules[table_id], _alias_for(table), column_types)
    if isinstance(tree, exp.Update):
        _admit_new_values(tree, gov, table_id, table, session_vars, params)
    assert isinstance(tree, (exp.Update, exp.Delete))
    return tree.where(f"({readable})", dialect="postgres", append=True)
