# Copyright (c) 2026 Kenneth Stott
# Canary: d4fe42b5-d17d-40d8-84e4-ad686c67af0d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Compile GraphQL mutations → INSERT/UPDATE/DELETE SQL (REQ-031–REQ-037).

Mutations always route direct to RDBMS (never the engine).
No mutations for NoSQL sources.
"""

# Requirements: REQ-031, REQ-032, REQ-033, REQ-034, REQ-035, REQ-036, REQ-037, REQ-212, REQ-214

from __future__ import annotations

from collections.abc import Callable

from dataclasses import dataclass, field, replace

from graphql import DocumentNode, FieldNode, OperationDefinitionNode, SelectionSetNode


from provisa.compiler.hasura_dialect import compiled_field, table_hasura
from provisa.compiler.naming import (
    active_gql_convention,
    hasura_argument_name,
    hasura_pk_constraint,
    hasura_root_field,
    hasura_type_name,
)
from provisa.compiler.params import ParamCollector
from provisa.compiler.sql_gen import CompilationContext, TableMeta, _q
from provisa.compiler.sql_rewrite import context_derived, translate_epoch_temporal_columns
from provisa.compiler.sql_where import _compile_where, _extract_value


# NoSQL source types — mutations not supported


@dataclass
class MutationResult:
    """Result of compiling a mutation."""

    sql: str
    params: list
    mutation_type: str  # "insert", "update", "delete"
    table_name: str
    source_id: str
    returning_columns: list[str]
    # REQ-194: how the field answers (mutation_answer): "count", Provisa's {affected_rows};
    # "response", Hasura's mutation_response; "row", Hasura's one row (or null).
    answer: str = "count"
    # The field's key in the response: its alias, else its name.
    response_key: str = ""
    # For "response": each selected field's key and what it is (affected_rows, returning,
    # __typename). For "row" and "response"'s returning: each selected key and the RETURNING
    # column (returning_columns) it is read from, or __typename.
    response_fields: list[tuple[str, str]] = field(default_factory=list)
    row_fields: list[tuple[str, str]] = field(default_factory=list)
    # The GraphQL type names a __typename answers: the row's and the mutation response's.
    row_type: str = ""
    response_type: str = ""


#: REQ-194: a Hasura-preset table's mutation root fields: (operation, form, prefix, suffix).
_HASURA_MUTATIONS: tuple[tuple[str, str, str, str], ...] = (
    ("insert", "many", "insert", ""),
    ("insert", "one", "insert", "one"),
    ("update", "many", "update", ""),
    ("update", "by_pk", "update", "by_pk"),
    ("delete", "many", "delete", ""),
    ("delete", "by_pk", "delete", "by_pk"),
)


def _hasura_mutations(ctx: CompilationContext) -> dict[str, tuple[str, str, TableMeta]]:
    """Every Hasura-preset table's mutation root field in ``ctx``, to its operation, form
    (``many``, ``one``, ``by_pk``) and table."""

    def build(c: CompilationContext) -> dict[str, tuple[str, str, TableMeta]]:
        out: dict[str, tuple[str, str, TableMeta]] = {}
        for key, tm in c.tables.items():
            conv = table_hasura(tm) if key == tm.field_name else None
            if conv is None:
                continue
            for op, form, prefix, suffix in _HASURA_MUTATIONS:
                out[hasura_root_field(conv, tm.field_name, prefix, suffix)] = (op, form, tm)
        return out

    return context_derived(ctx, ("hasura_mutations", active_gql_convention()), build)


def _get_mutation_meta(
    field_name: str,
    ctx: CompilationContext,
) -> tuple[str, str, TableMeta]:
    """Parse mutation field name and return (operation, table_field_name, TableMeta).

    Handles both camelCase (insertOrders, sa__insertOrders) and
    snake_case (insert_orders, sa__insert_orders) mutation naming styles, and a Hasura-preset
    table's Hasura mutations (REQ-194).
    """
    hasura = _hasura_mutations(ctx).get(field_name)
    if hasura is not None:
        return hasura[0], hasura[2].field_name, hasura[2]
    domain_prefix = ""
    name = field_name
    if "__" in field_name:
        domain_prefix, name = field_name.split("__", 1)
        domain_prefix += "__"

    for op in ("upsert", "insert", "update", "delete"):
        # camelCase style: insertOrders → op=insert, rest=Orders → orders
        if name.startswith(op) and len(name) > len(op):
            rest = name[len(op) :]
            if rest[0].isupper():
                table_field = domain_prefix + rest[0].lower() + rest[1:]
                if table_field in ctx.tables:
                    return op, table_field, ctx.tables[table_field]
        # snake_case style: insert_orders → op=insert, rest=orders
        snake_prefix = op + "_"
        if name.startswith(snake_prefix) and len(name) > len(snake_prefix):
            table_field = domain_prefix + name[len(snake_prefix) :]
            if table_field in ctx.tables:
                return op, table_field, ctx.tables[table_field]
    raise ValueError(f"Unknown mutation field: {field_name!r}")


def _physical(table: TableMeta, ctx: CompilationContext) -> Callable[[str], str]:
    """A column's physical name from the published name a mutation's input, set, where or
    conflict columns name it by -- the names reads use (CompilationContext.exposed_to_physical,
    which holds an entry only where the two differ)."""

    def phys(name: str) -> str:
        return ctx.exposed_to_physical.get((table.table_id, name)) or name

    return phys


def _physical_keys(data: dict, phys: Callable[[str], str]) -> dict:
    return {phys(k): v for k, v in data.items()}


def _mutation_where(
    where: dict, table: TableMeta, ctx: CompilationContext, collector: ParamCollector
) -> str:
    """An update's or delete's ``where``, compiled as a read's is: every operator its input type
    offers, ``_and``/``_or``, and columns named by their published names."""
    return _compile_where(
        where,
        collector,
        None,
        ctx.virtual_columns.get(table.table_id),
        table.table_id,
        ctx.exposed_to_physical,
    )


def compile_upsert(  # REQ-212, REQ-036
    field_node: FieldNode,
    table: TableMeta,
    variables: dict | None,
    ctx: CompilationContext,
) -> MutationResult:
    """Compile an upsert mutation to INSERT ... ON CONFLICT ... DO UPDATE SQL."""
    collector = ParamCollector()
    args = {}
    if field_node.arguments:
        for arg in field_node.arguments:
            args[arg.name.value] = _extract_value(arg.value, variables)

    phys = _physical(table, ctx)
    input_data = _physical_keys(args.get("input", {}), phys)
    if not input_data:
        raise ValueError("upsert mutation requires 'input' argument")

    on_conflict_cols = args.get("on_conflict", [])
    if not on_conflict_cols:
        raise ValueError(
            "upsert mutation requires 'on_conflict' argument (list of conflict columns)"
        )
    if isinstance(on_conflict_cols, str):
        on_conflict_cols = [on_conflict_cols]
    on_conflict_cols = [phys(c) for c in on_conflict_cols]

    columns = list(input_data.keys())
    placeholders = [collector.add(input_data[col]) for col in columns]
    cols_sql = ", ".join(_q(c) for c in columns)
    vals_sql = ", ".join(placeholders)
    conflict_sql = ", ".join(_q(c) for c in on_conflict_cols)

    # UPDATE all non-conflict columns on conflict
    update_cols = [c for c in columns if c not in on_conflict_cols]
    if update_cols:
        set_parts = [f"{_q(c)} = EXCLUDED.{_q(c)}" for c in update_cols]
        do_clause = f"DO UPDATE SET {', '.join(set_parts)}"
    else:
        do_clause = "DO NOTHING"

    returning = ", ".join(_q(c) for c in columns)

    sql = (
        f"INSERT INTO {_q(table.schema_name)}.{_q(table.table_name)}"
        f" ({cols_sql}) VALUES ({vals_sql})"
        f" ON CONFLICT ({conflict_sql}) {do_clause}"
        f" RETURNING {returning}"
    )

    return MutationResult(
        sql=sql,
        params=collector.params,
        mutation_type="upsert",
        table_name=table.table_name,
        source_id=table.source_id,
        returning_columns=columns,
    )


def compile_insert(  # REQ-032, REQ-033, REQ-034, REQ-036
    field_node: FieldNode,
    table: TableMeta,
    variables: dict | None,
    ctx: CompilationContext,
) -> MutationResult:
    """Compile an insert mutation to INSERT SQL."""
    collector = ParamCollector()
    args = {}
    if field_node.arguments:
        for arg in field_node.arguments:
            args[arg.name.value] = _extract_value(arg.value, variables)

    input_data = _physical_keys(args.get("input", {}), _physical(table, ctx))
    if not input_data:
        raise ValueError("insert mutation requires 'input' argument")

    columns = list(input_data.keys())
    placeholders = [collector.add(input_data[col]) for col in columns]
    cols_sql = ", ".join(_q(c) for c in columns)
    vals_sql = ", ".join(placeholders)

    # RETURNING all inserted columns
    returning = ", ".join(_q(c) for c in columns)

    sql = (
        f"INSERT INTO {_q(table.schema_name)}.{_q(table.table_name)}"
        f" ({cols_sql}) VALUES ({vals_sql})"
        f" RETURNING {returning}"
    )

    return MutationResult(
        sql=sql,
        params=collector.params,
        mutation_type="insert",
        table_name=table.table_name,
        source_id=table.source_id,
        returning_columns=columns,
    )


def compile_update(  # REQ-032, REQ-033, REQ-034, REQ-035, REQ-036
    field_node: FieldNode,
    table: TableMeta,
    variables: dict | None,
    ctx: CompilationContext,
) -> MutationResult:
    """Compile an update mutation to UPDATE SQL."""
    collector = ParamCollector()
    args = {}
    if field_node.arguments:
        for arg in field_node.arguments:
            args[arg.name.value] = _extract_value(arg.value, variables)

    set_data = _physical_keys(args.get("set", {}), _physical(table, ctx))
    if not set_data:
        raise ValueError("update mutation requires 'set' argument")
    if not args.get("where"):
        raise ValueError("update mutation requires 'where' argument")

    # SET clause
    set_parts = []
    for col, val in set_data.items():
        placeholder = collector.add(val)
        set_parts.append(f"{_q(col)} = {placeholder}")
    set_sql = ", ".join(set_parts)

    where_sql = _mutation_where(args["where"], table, ctx, collector)

    updated_cols = list(set_data.keys())
    returning = ", ".join(_q(c) for c in updated_cols)

    sql = (
        f"UPDATE {_q(table.schema_name)}.{_q(table.table_name)}"
        f" SET {set_sql}"
        f" WHERE {where_sql}"
        f" RETURNING {returning}"
    )

    return MutationResult(
        sql=sql,
        params=collector.params,
        mutation_type="update",
        table_name=table.table_name,
        source_id=table.source_id,
        returning_columns=updated_cols,
    )


def compile_delete(  # REQ-032, REQ-033, REQ-035, REQ-036
    field_node: FieldNode,
    table: TableMeta,
    variables: dict | None,
    ctx: CompilationContext,
) -> MutationResult:
    """Compile a delete mutation to DELETE SQL."""
    collector = ParamCollector()
    args = {}
    if field_node.arguments:
        for arg in field_node.arguments:
            args[arg.name.value] = _extract_value(arg.value, variables)

    if not args.get("where"):
        raise ValueError("delete mutation requires 'where' argument")
    where_sql = _mutation_where(args["where"], table, ctx, collector)

    sql = (
        f"DELETE FROM {_q(table.schema_name)}.{_q(table.table_name)} WHERE {where_sql} RETURNING *"
    )

    return MutationResult(
        sql=sql,
        params=collector.params,
        mutation_type="delete",
        table_name=table.table_name,
        source_id=table.source_id,
        returning_columns=[],
    )


def _arguments(field_node: FieldNode, variables: dict | None) -> dict:
    return {
        arg.name.value: _extract_value(arg.value, variables) for arg in field_node.arguments or ()
    }


def _returned(
    selection: SelectionSetNode | None, table: TableMeta, ctx: CompilationContext
) -> list[tuple[str, str]]:
    """Each field of a returned row's selection: its response key and the physical column it
    reads (or ``__typename``). A relationship is refused by name: a mutation returns the table's
    own columns."""
    out: list[tuple[str, str]] = []
    for sel in selection.selections if selection is not None else ():
        if not isinstance(sel, FieldNode):
            raise ValueError("a mutation's returned row is selected field by field")
        name = sel.name.value
        key = sel.alias.value if sel.alias else name
        if name == "__typename":
            out.append((key, name))
            continue
        if (table.type_name, name) in ctx.joins:
            raise ValueError(
                f"{name} is a relationship: a mutation returns {table.field_name}'s own columns"
            )
        out.append((key, ctx.exposed_to_physical.get((table.table_id, name)) or name))
    return out


def _constraint_columns(constraint: str, table: TableMeta, ctx: CompilationContext) -> list[str]:
    """The physical columns of the constraint an on_conflict names: the primary key's, or a
    declared UNIQUE constraint's."""
    if constraint == hasura_pk_constraint(table.original_table_name or table.table_name):
        return ctx.pk_columns[table.table_id]
    for name, columns in ctx.unique_constraints.get(table.table_id, []):
        if name == constraint:
            return columns
    raise ValueError(f"{constraint!r} is not a constraint of {table.field_name}")


def _on_conflict_sql(
    on_conflict: dict,
    table: TableMeta,
    ctx: CompilationContext,
    convention: str,
    phys: Callable[[str], str],
) -> str:
    """``ON CONFLICT (<the constraint's columns>)``: the columns ``update_columns`` names set from
    the row offered, or, naming none, the row left as it is."""
    columns = _constraint_columns(on_conflict["constraint"], table, ctx)
    # Hasura's default for update_columns is [] (the schema declares it, REQ-194).
    updates = [
        phys(c) for c in on_conflict.get(hasura_argument_name(convention, "update_columns"), [])
    ]
    target = ", ".join(_q(c) for c in columns)
    if not updates:
        return f" ON CONFLICT ({target}) DO NOTHING"
    sets = ", ".join(f"{_q(c)} = EXCLUDED.{_q(c)}" for c in updates)
    return f" ON CONFLICT ({target}) DO UPDATE SET {sets}"


def compile_hasura(  # REQ-194
    field_node: FieldNode,
    op: str,
    form: str,
    table: TableMeta,
    variables: dict | None,
    ctx: CompilationContext,
) -> MutationResult:
    """A Hasura mutation of a Hasura-preset table, with Hasura's semantics: ``insert`` writes
    each of ``objects`` (or ``object``), a column a row leaves out taking its default, and
    ``on_conflict`` names the constraint and the columns to update; ``update`` sets ``_set`` on
    the rows ``where`` (or ``pk_columns``) names; ``delete`` deletes the rows ``where`` (or the
    key) names. A ``many`` form answers its mutation_response, ``one`` and ``by_pk`` the row."""
    conv = table_hasura(table)
    assert conv is not None
    collector = ParamCollector()
    args = _arguments(field_node, variables)
    phys = _physical(table, ctx)
    target = f"{_q(table.schema_name)}.{_q(table.table_name)}"

    if form == "many":
        response_fields: list[tuple[str, str]] = []
        row_fields: list[tuple[str, str]] = []
        for sel in field_node.selection_set.selections if field_node.selection_set else ():
            if not isinstance(sel, FieldNode):
                raise ValueError("a mutation response is selected field by field")
            name = sel.name.value
            response_fields.append((sel.alias.value if sel.alias else name, name))
            if name == "returning":
                row_fields = _returned(sel.selection_set, table, ctx)
    else:
        response_fields = []
        row_fields = _returned(field_node.selection_set, table, ctx)
    returned = list(dict.fromkeys(c for _, c in row_fields if c != "__typename"))

    if op == "insert":
        rows = [args["object"]] if form == "one" else args["objects"]
        if not rows:
            raise ValueError(f"{field_node.name.value} inserts no object")
        rows = [_physical_keys(r, phys) for r in rows]
        columns = list(dict.fromkeys(c for r in rows for c in r))
        values = ", ".join(
            "(" + ", ".join(collector.add(r[c]) if c in r else "DEFAULT" for c in columns) + ")"
            for r in rows
        )
        sql = f"INSERT INTO {target} ({', '.join(_q(c) for c in columns)}) VALUES {values}"
        if "on_conflict" in args:
            sql += _on_conflict_sql(args["on_conflict"], table, ctx, conv, phys)
        returning = returned or columns
    elif op == "update":
        set_data = _physical_keys(args.get("_set") or {}, phys)
        if not set_data:
            raise ValueError(f"{field_node.name.value} sets no column: give it _set")
        set_sql = ", ".join(f"{_q(c)} = {collector.add(v)}" for c, v in set_data.items())
        where = args["where"] if form == "many" else _key_where(args["pk_columns"])
        sql = f"UPDATE {target} SET {set_sql} WHERE {_mutation_where(where, table, ctx, collector)}"
        returning = returned or list(set_data)
    else:
        where = args["where"] if form == "many" else _key_where(args)
        sql = f"DELETE FROM {target} WHERE {_mutation_where(where, table, ctx, collector)}"
        returning = returned
    sql += f" RETURNING {', '.join(_q(c) for c in returning) if returning else '*'}"

    return MutationResult(
        sql=sql,
        params=collector.params,
        mutation_type=op,
        table_name=table.table_name,
        source_id=table.source_id,
        returning_columns=returning,
        answer="response" if form == "many" else "row",
        response_key=field_node.alias.value if field_node.alias else field_node.name.value,
        response_fields=response_fields,
        row_fields=row_fields,
        row_type=table.type_name,
        response_type=hasura_type_name(conv, table.type_name, "mutation_response"),
    )


def _key_where(key: dict) -> dict:
    """The where naming the row of ``key`` (each key column's value)."""
    return {column: {"eq": value} for column, value in key.items()}


def mutation_answer(m: MutationResult, rows: list[tuple], affected: int) -> object:  # REQ-194
    """The field's answer from the rows its statement returned (in ``m.returning_columns``
    order) and the rows it touched: Provisa's ``{affected_rows}``; Hasura's mutation_response
    (``affected_rows``, ``returning``) as selected; or Hasura's one row, null when none."""
    if m.answer == "count":
        return {"affected_rows": affected}

    def row(values: tuple) -> dict:
        return {
            key: m.row_type if column == "__typename" else values[m.returning_columns.index(column)]
            for key, column in m.row_fields
        }

    if m.answer == "row":
        if len(rows) > 1:
            raise ValueError(f"{m.response_key} touched {len(rows)} rows; it answers one")
        return row(rows[0]) if rows else None
    answer: dict = {}
    for key, name in m.response_fields:
        if name == "returning":
            answer[key] = [row(r) for r in rows]
        elif name == "__typename":
            answer[key] = m.response_type
        else:  # affected_rows, as the convention spells it
            answer[key] = affected
    return answer


def compile_mutation(  # REQ-031, REQ-032, REQ-033, REQ-036, REQ-037
    document: DocumentNode,
    ctx: CompilationContext,
    source_types: dict[str, str],
    variables: dict | None = None,
) -> list[MutationResult]:
    """Compile a GraphQL mutation document to SQL mutations.

    Raises ValueError for NoSQL sources or cross-source mutations.
    """
    results: list[MutationResult] = []

    for definition in document.definitions:
        if not isinstance(definition, OperationDefinitionNode):
            continue
        if definition.operation.value != "mutation":
            continue

        for sel in definition.selection_set.selections:
            if not isinstance(sel, FieldNode):
                continue

            hasura = _hasura_mutations(ctx).get(sel.name.value)
            if hasura is not None:
                op, form, table = hasura
                # REQ-194: read with Hasura's spellings, compiled with its semantics.
                results.append(
                    compile_hasura(compiled_field(sel, table, ctx), op, form, table, variables, ctx)
                )
                continue

            op, _, table = _get_mutation_meta(sel.name.value, ctx)

            if op == "upsert":
                compiled = compile_upsert(sel, table, variables, ctx)
            elif op == "insert":
                compiled = compile_insert(sel, table, variables, ctx)
            elif op == "update":
                compiled = compile_update(sel, table, variables, ctx)
            else:
                compiled = compile_delete(sel, table, variables, ctx)
            # A Provisa mutation answers under its field's name, as it always has.
            results.append(replace(compiled, response_key=sel.name.value))

    # Reject cross-source mutations
    source_ids = {r.source_id for r in results}
    if len(source_ids) > 1:
        raise ValueError(f"Cross-source mutations not supported. Sources involved: {source_ids}")

    # REQ-1908: a mutation's SQL is physical as built (it never passes the semantic→physical
    # rewrite), so an epoch-stored temporal column is translated here: an ISO 8601 value is written
    # as its epoch number, and RETURNING reads it back as the temporal.
    results = [
        replace(r, sql=translate_epoch_temporal_columns(r.sql, ctx.epoch_columns)) for r in results
    ]

    return results
