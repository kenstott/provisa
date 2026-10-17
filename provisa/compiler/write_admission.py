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

An ``INSERT``, ``UPDATE``, ``DELETE``, ``MERGE`` or ``TRUNCATE`` is admitted in the governance
stage of the one pipeline — the stage a read's row filter and masks are applied in — so every
surface that lowers to SQL gets the same answer. It is admitted by the role's write grant on the
table for the operation (REQ-2074), in this order:

1. **The right.** The role holds ``write`` (REQ-868).
2. **The grant.** The role holds a grant for the operation on the table: an INSERT an insert
   grant, an UPDATE an update grant, a DELETE a delete grant, a TRUNCATE a delete grant over
   every row, a MERGE both an insert and an update grant over every row, an INSERT ... ON
   CONFLICT DO UPDATE an update grant over every row too.
3. **The columns.** Every column an INSERT lists (all of the table's when it lists none) or an
   UPDATE sets is one the grant names; whether the role can read it is not consulted. A column
   the grant presets is the grant's to set: the statement may not supply it, and the grant's
   value (a session variable, a literal, or the time of the write) is added to the statement.
4. **The rows it may touch.** The grant's rows predicate is ANDed into the WHERE of an UPDATE
   or DELETE.
5. **The rows it leaves behind.** An INSERT's rows, and the new values of an UPDATE that sets a
   column the grant's check reads, are checked against the check before anything is sent to a
   source: the supplied values are put in the check's place for those columns and the resulting
   constant predicate is evaluated in this process, with no round trip and no write. The answer
   given here decides admission; the source is not consulted, so where the source would compare
   differently (a case-insensitive collation, an implicit cast) the write is admitted or refused
   by this answer.

A grant that states no rows or check takes the role's read row filter on the table: a role
writes the rows it reads unless its grant says otherwise. ``TRUE`` is every row.

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

# REQ-1942: TRUNCATE is a mutation too, run as itself -- never rewritten to a DELETE.
WRITES = (exp.Insert, exp.Update, exp.Delete, exp.Merge, exp.TruncateTable)

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


class ReturningNotSupported(NotAvailableHere):
    """A write with a ``RETURNING`` clause on a table whose write route cannot return the rows it
    wrote (executor/write_capability.py) — refused whatever the role holds, naming the table."""

    def __init__(self, table: str) -> None:
        self.table = table
        super().__init__(
            f"{table!r} does not take RETURNING: its source is written through a server that "
            "does not return the rows it writes"
        )


class WriteFormNotSupported(NotAvailableHere):
    """A write in a statement form the table's write route does not carry
    (executor/write_capability.py) — refused whatever the role holds, naming the table and the
    form."""

    def __init__(self, table: str, form: str) -> None:
        self.table = table
        self.form = form
        super().__init__(
            f"{table!r} does not take {_FORM_NAMES[form]}: its source is written one plain "
            "INSERT, UPDATE or DELETE at a time"
        )


_FORM_NAMES = {
    "on_conflict": "INSERT ... ON CONFLICT",
    "update_from": "UPDATE ... FROM",
    "delete_using": "DELETE ... USING",
    "merge": "MERGE",
    "truncate": "TRUNCATE",
}


def write_form(tree: exp.Expression) -> str | None:
    """The form of ``tree`` beyond a plain INSERT, UPDATE or DELETE, or None when it is plain."""
    if isinstance(tree, exp.Merge):
        return "merge"
    if isinstance(tree, exp.TruncateTable):
        return "truncate"
    if isinstance(tree, exp.Insert) and tree.args.get("conflict"):
        return "on_conflict"
    if isinstance(tree, exp.Update) and tree.args.get("from_"):
        return "update_from"
    if isinstance(tree, exp.Delete) and tree.args.get("using"):
        return "delete_using"
    return None


WRITE_OPS: tuple[str, ...] = ("insert", "update", "delete")
_OPERATIONS = {
    "INSERT": ("insert",),
    "UPDATE": ("update",),
    "DELETE": ("delete",),
    "TRUNCATETABLE": ("delete",),  # it removes rows, as DELETE does
}


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


def target_table(tree: exp.Expression) -> exp.Table:
    """The table a write targets."""
    return _target(tree)[0]


def _target(tree: exp.Expression) -> tuple[exp.Table, list[str] | None]:
    """The table a write targets, and for an INSERT the columns it lists (None: none listed)."""
    if isinstance(tree, exp.TruncateTable):
        tables = [t for t in tree.expressions if isinstance(t, exp.Table)]
        if len(tables) != 1:
            raise WriteNotAdmitted("TRUNCATE empties one table at a time")
        return tables[0], None
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


def _grant(gov: "GovernanceContext", table_id: int, name: str, operation: str) -> dict:
    """The role's write grant on the table for ``operation`` (REQ-2074); refused by name when it
    holds none."""
    grant = gov.write_grants.get(table_id, {}).get(operation)
    if grant is None:
        raise WriteNotAdmitted(
            f"{operation.upper()} on {name!r}: role {gov.role_id!r} holds no {operation} grant "
            "on this table"
        )
    return grant


def _require_columns(gov: "GovernanceContext", grant: dict, name: str, columns: list[str]) -> None:
    """Every column written is one the grant names; a column the grant presets is the grant's
    to set, never the statement's."""
    preset = {p["column"] for p in grant.get("presets") or []}
    allowed = set(grant.get("columns") or [])
    for column in columns:
        if column in preset:
            raise WriteNotAdmitted(
                f"{grant['operation'].upper()} on {name!r}: {column!r} is set by role "
                f"{gov.role_id!r}'s {grant['operation']} grant; the statement may not supply it"
            )
        if column not in allowed:
            raise ColumnNotWritable(gov.role_id, column)


def _predicate(gov: "GovernanceContext", table_id: int, stated: str | None) -> str | None:
    """The predicate a grant states for its rows or its check. REQ-2074: a grant that states none
    takes the role's read row filter on the table -- a role writes the rows it reads unless its
    grant says otherwise -- and a table the role reads unfiltered gives none (every row)."""
    if stated is not None:
        return stated
    return gov.rls_rules.get(table_id)


def _label(grant: dict, stated: str) -> str:
    """What a predicate is called in a refusal: the grant's own, or the read row filter it took."""
    return (
        f"{grant['operation']} grant's {stated}" if grant.get(stated) is not None else "row filter"
    )


def _every_row(predicate: str | None) -> bool:
    """Whether ``predicate`` admits every row: none at all, or the literal TRUE."""
    if predicate is None:
        return True
    tree = sqlglot.parse_one(predicate, read="postgres")
    return isinstance(tree, exp.Boolean) and tree.this is True


def _preset_values(
    gov: "GovernanceContext",
    table_id: int,
    name: str,
    grant: dict,
    session_vars: dict[str, str],
) -> dict[str, exp.Expression]:
    """The value each of the grant's presets sets, typed as its column is."""
    types = dict(gov.all_columns.get(table_id, []))
    values: dict[str, exp.Expression] = {}
    for preset in grant.get("presets") or []:
        column, source = preset["column"], preset["source"]
        if source == "now":
            values[column] = exp.CurrentTimestamp()
            continue
        if source == "session":
            raw = session_vars.get(preset["name"])
            if raw is None:
                raise WriteNotAdmitted(
                    f"{grant['operation'].upper()} on {name!r}: role {gov.role_id!r}'s "
                    f"{grant['operation']} grant sets {column!r} from session variable "
                    f"{preset['name']!r}, which this request does not carry"
                )
        else:
            raw = preset["value"]
        literal: exp.Expression = exp.Literal.string(str(raw))
        column_type = types.get(column)
        values[column] = (
            exp.Cast(this=literal, to=exp.DataType.build(column_type)) if column_type else literal
        )
    return values


def _filter_over(
    predicate: str,
    gov: "GovernanceContext",
    table_id: int,
    session_vars: dict[str, str],
    values: dict[str, exp.Expression],
) -> exp.Expression:
    """``predicate`` with every column in ``values`` replaced by the expression the statement
    supplies for it."""
    column_types = dict(gov.all_columns.get(table_id, []))
    qualified = _qualified_predicate(predicate, None)
    cast_session_terms(qualified, column_types)
    resolved: exp.Expression = sqlglot.parse_one(  # pyright: ignore[reportAssignmentType]  # sqlglot stub types parse_one as Expr
        _with_session_values(qualified.sql(dialect="postgres"), session_vars), read="postgres"
    )
    for column in list(resolved.find_all(exp.Column)):
        if column.name in values:
            column.replace(exp.paren(values[column.name].copy()))
    return resolved


def _admit_row(
    gov: "GovernanceContext",
    table_id: int,
    name: str,
    check: str,
    supplied: dict[str, exp.Expression],
    session_vars: dict[str, str],
    *,
    kind: str = "INSERT into",
    label: str = "row filter",
) -> None:
    """Refuse a row the check would not admit, or whose admission cannot be decided here."""
    outcome = _holds(_filter_over(check, gov, table_id, session_vars, supplied))
    if outcome is None:
        raise WriteNotAdmitted(
            f"{kind} {name!r}: role {gov.role_id!r}'s {label} on this table cannot be decided "
            "before writing -- it reads a column the statement does not supply, or uses a "
            "function or comparison that cannot be evaluated before the row is written. Supply "
            "every column the check reads as a literal value."
        )
    if not outcome:
        raise WriteNotAdmitted(
            f"{kind} {name!r}: the row is outside role {gov.role_id!r}'s {label} on this table "
            f"({check}). A role may only write the rows its grant admits."
        )


def _apply_insert_presets(
    tree: exp.Insert, listed: list[str] | None, presets: dict[str, exp.Expression], name: str
) -> list[str] | None:
    """Add each preset column and its value to the INSERT's rows; the columns it lists after."""
    if not presets:
        return listed
    source = tree.args.get("expression")
    schema = tree.this
    if listed is None or not isinstance(schema, exp.Schema) or not isinstance(source, exp.Values):
        raise WriteNotAdmitted(
            f"INSERT into {name!r}: the grant sets {sorted(presets)} itself, which it can add "
            "only to an INSERT that lists its columns and gives its rows as VALUES"
        )
    for column, value in presets.items():
        schema.append("expressions", exp.to_identifier(column))
        for row in source.expressions:
            row.append("expressions", value.copy())
    return [*listed, *presets]


def _admit_inserted_rows(
    tree: exp.Insert,
    gov: "GovernanceContext",
    table_id: int,
    name: str,
    listed: list[str] | None,
    check: str,
    session_vars: dict[str, str],
    params: list | None,
    label: str,
) -> None:
    source = tree.args.get("expression")
    if not isinstance(source, exp.Values):
        raise WriteNotAdmitted(
            f"INSERT into {name!r}: role {gov.role_id!r}'s {label} on this table cannot be "
            "applied to the rows of an INSERT … SELECT before they are written. Insert the rows "
            "as VALUES."
        )
    columns = listed if listed is not None else [c for c, _ in gov.all_columns.get(table_id, [])]
    for row in source.expressions:
        supplied: dict[str, exp.Expression] = {}
        for column, value in zip(columns, row.expressions, strict=False):
            literal = (
                value
                if isinstance(value, (exp.Cast, exp.CurrentTimestamp))
                else _supplied(value, params)
            )
            if literal is None:
                raise WriteNotAdmitted(
                    f"INSERT into {name!r}: role {gov.role_id!r} has a {label} on this table, and "
                    "a value that is neither a literal nor a bound parameter cannot be checked "
                    "against it before the row is written. Supply the values as literals."
                )
            supplied[column] = literal
        _admit_row(gov, table_id, name, check, supplied, session_vars, label=label)


def admit_rows(
    gov: "GovernanceContext",
    table_id: int,
    name: str,
    columns: list[str],
    rows: list[list] | None = None,
    session_vars: dict[str, str] | None = None,
) -> None:
    """Admit a bulk load -- rows given as values, not as a statement (pgwire ``COPY ... FROM
    STDIN``) -- by the rules an INSERT of the same columns is admitted by. Called once with
    ``rows`` None before any data is read (the right, the grant and its columns), and again with
    the rows before any of them is written (the grant's check)."""
    require_write_op(gov, table_id, name, "INSERT")
    if not gov.can_write:
        raise WriteNotAdmitted(
            f"COPY into {name!r}: role {gov.role_id!r} does not hold the 'write' right"
        )
    grant = _grant(gov, table_id, name, "insert")
    if grant.get("presets"):
        raise WriteNotAdmitted(
            f"COPY into {name!r}: role {gov.role_id!r}'s insert grant sets "
            f"{sorted(p['column'] for p in grant['presets'])} itself (presets), which a COPY "
            "cannot add. Use INSERT."
        )
    from provisa.compiler.stage2 import registered_name

    columns = [registered_name(gov, table_id, c) for c in columns]
    _require_columns(gov, grant, name, columns)
    check = _predicate(gov, table_id, grant.get("check"))
    if rows is None or _every_row(check):
        return
    assert check is not None
    for row in rows:
        supplied: dict[str, exp.Expression] = {
            column: exp.convert(value)  # pyright: ignore[reportAssignmentType]  # sqlglot stub types convert as Expr
            for column, value in zip(columns, row, strict=False)
        }
        _admit_row(
            gov,
            table_id,
            name,
            check,
            supplied,
            session_vars or {},
            kind="COPY into",
            label=_label(grant, "check"),
        )


def _admit_new_values(
    tree: exp.Update,
    gov: "GovernanceContext",
    table_id: int,
    name: str,
    check: str,
    session_vars: dict[str, str],
    params: list | None,
    label: str,
) -> None:
    """Refuse an UPDATE whose new values would leave a row outside the check, or whose outcome
    cannot be decided before writing. An UPDATE that sets no column the check reads needs
    nothing here when the check is its rows predicate too: the WHERE carries it."""
    read_by_check = {c.name for c in _qualified_predicate(check, None).find_all(exp.Column)}
    assignments: dict[str, exp.Expression] = {}
    for assignment in tree.expressions:
        if not isinstance(assignment, exp.EQ):
            continue
        column = assignment.this.name
        if column not in read_by_check:
            continue
        value = assignment.expression
        literal = (
            value
            if isinstance(value, (exp.Cast, exp.CurrentTimestamp))
            else _supplied(value, params)
        )
        if literal is None:
            raise WriteNotAdmitted(
                f"UPDATE of {name!r}: role {gov.role_id!r}'s {label} on this table reads column "
                f"{column!r}, and its new value is neither a literal nor a bound parameter, so "
                "whether each row passes cannot be decided before writing. Set the column to a "
                "literal value."
            )
        assignments[column] = literal
    if not assignments:
        return
    _admit_row(gov, table_id, name, check, assignments, session_vars, kind="UPDATE of", label=label)


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
    """Admit ``tree`` -- an INSERT, UPDATE, DELETE, MERGE or TRUNCATE -- for the role ``gov`` was
    built for, by its write grant for the operation (REQ-2074), and return it with the grant's
    presets and rows in place. ``params`` are the request's bound values (``$1`` ...), which the
    check reads where the statement supplies a value as a parameter. Raises
    :class:`WriteNotAdmitted` or :class:`ColumnNotWritable` (both ``PermissionError``) when the
    write is not admitted."""
    session_vars = session_vars or {}
    kind = _kind(tree)
    table, listed = _target(tree)
    table_id = _resolve(table, gov)
    name = table.name
    if isinstance(tree, (exp.Insert, exp.Update, exp.Delete)) and gov.published.get(table_id):
        # A column the write names by its published name is its physical column, which the
        # grant states (REQ-2074): named so from here on, as lowering names it.
        from provisa.compiler.sql_rewrite import rename_write_columns

        rename_write_columns(tree, gov.published[table_id])
        table, listed = _target(tree)
    require_write_op(gov, table_id, name, kind)
    form = write_form(tree)
    if form is not None and form in gov.write_refused_forms[table_id]:
        raise WriteFormNotSupported(name, form)
    if tree.args.get("returning") is not None and not gov.write_returns_rows[table_id]:
        raise ReturningNotSupported(name)
    if not gov.can_write:
        raise WriteNotAdmitted(
            f"{kind} on {name!r}: role {gov.role_id!r} does not hold the 'write' right"
        )

    if isinstance(tree, exp.TruncateTable):
        grant = _grant(gov, table_id, name, "delete")
        rows = _predicate(gov, table_id, grant.get("rows"))
        if not _every_row(rows):
            # REQ-1942: a TRUNCATE empties the table; it cannot carry a predicate.
            raise WriteNotAdmitted(
                f"TRUNCATE of {name!r}: role {gov.role_id!r} has a {_label(grant, 'rows')} on "
                f"this table ({rows}), which a TRUNCATE cannot apply. Use DELETE, which does."
            )
        return tree

    if isinstance(tree, exp.Merge):
        every = [c for c, _ in gov.all_columns.get(table_id, [])]
        for operation in ("insert", "update"):
            grant = _grant(gov, table_id, name, operation)
            _require_columns(gov, grant, name, every)
            stated = [grant.get("check")] + ([grant.get("rows")] if operation == "update" else [])
            if not all(_every_row(_predicate(gov, table_id, p)) for p in stated):
                raise WriteNotAdmitted(
                    f"MERGE into {name!r}: role {gov.role_id!r}'s {operation} grant limits the "
                    "rows it may write, and the rows a MERGE would write cannot be checked "
                    "against it. Use INSERT and UPDATE."
                )
        return tree

    if isinstance(tree, exp.Insert):
        grant = _grant(gov, table_id, name, "insert")
        columns = (
            listed if listed is not None else [c for c, _ in gov.all_columns.get(table_id, [])]
        )
        _require_columns(gov, grant, name, columns)
        conflict = tree.args.get("conflict")
        if (
            conflict is not None
            and conflict.args.get("action") is not None
            and ("UPDATE" in conflict.sql(dialect="postgres").upper())
        ):
            update = _grant(gov, table_id, name, "update")
            set_columns = [
                e.this.name for e in conflict.find_all(exp.EQ) if isinstance(e.this, exp.Column)
            ]
            _require_columns(gov, update, name, set_columns)
            if update.get("presets") or not (
                _every_row(_predicate(gov, table_id, update.get("rows")))
                and _every_row(_predicate(gov, table_id, update.get("check")))
            ):
                raise WriteNotAdmitted(
                    f"INSERT ... ON CONFLICT DO UPDATE on {name!r}: role {gov.role_id!r}'s "
                    "update grant limits or presets the rows it may update, which the conflict "
                    "update cannot apply. Use INSERT and UPDATE."
                )
        presets = _preset_values(gov, table_id, name, grant, session_vars)
        listed = _apply_insert_presets(tree, listed, presets, name)
        check = _predicate(gov, table_id, grant.get("check"))
        if not _every_row(check):
            assert check is not None
            _admit_inserted_rows(
                tree,
                gov,
                table_id,
                name,
                listed,
                check,
                session_vars,
                params,
                _label(grant, "check"),
            )
        return tree

    from provisa.compiler.rls import _qualify_filter  # noqa: PLC0415
    from provisa.compiler.stage2 import _alias_for  # noqa: PLC0415

    assert isinstance(tree, (exp.Update, exp.Delete))
    operation = "update" if isinstance(tree, exp.Update) else "delete"
    grant = _grant(gov, table_id, name, operation)
    if isinstance(tree, exp.Update):
        _require_columns(gov, grant, name, written_columns(tree, gov, table_id))
        for column, value in _preset_values(gov, table_id, name, grant, session_vars).items():
            tree.append("expressions", exp.EQ(this=exp.column(column), expression=value))
        check = _predicate(gov, table_id, grant.get("check"))
        if not _every_row(check):
            assert check is not None
            _admit_new_values(
                tree, gov, table_id, name, check, session_vars, params, _label(grant, "check")
            )
    rows = _predicate(gov, table_id, grant.get("rows"))
    if _every_row(rows):
        return tree
    assert rows is not None
    column_types = dict(gov.all_columns.get(table_id, []))
    touched = _qualify_filter(rows, _alias_for(table), column_types)
    return tree.where(f"({touched})", dialect="postgres", append=True)
