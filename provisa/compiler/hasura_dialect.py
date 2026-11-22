# Copyright (c) 2026 Kenneth Stott
# Canary: 5d71e0a3-92c4-4b6f-8e15-c3a9d0f7b862
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Hasura's spellings, read by the compiler (REQ-194, maintainer 2026-11-22).

A Hasura-preset table's fields take Hasura's arguments. Their names under graphql-default
(``orderBy``, ``distinctOn``, ``pkColumns``, ``onConflict``) are read here as the compiler names
them (``order_by``, ...), on the table's own fields only: a column or a parameter that happens to
be spelled ``orderBy`` elsewhere is untouched. A ``<table>_by_pk`` read is the table's read of the
row its primary key names. Operators (``_eq``) and order_by values (``DESC``) are read where the
compiler reads them (``naming.compiled_operator``, ``naming.compiled_order_direction``).
"""

# Requirements: REQ-194

from __future__ import annotations

from copy import copy

from graphql import (
    ArgumentNode,
    FieldNode,
    InlineFragmentNode,
    NameNode,
    ObjectFieldNode,
    ObjectValueNode,
    SelectionSetNode,
)

from provisa.compiler.naming import (
    GRAPHQL_DEFAULT,
    active_gql_convention,
    hasura_argument_name,
    hasura_convention_of,
)
from provisa.compiler.sql_types import CompilationContext, TableMeta

#: The arguments a Hasura table field takes, as the compiler names them.
HASURA_ARGUMENTS: tuple[str, ...] = (
    "where",
    "order_by",
    "distinct_on",
    "limit",
    "offset",
    "objects",
    "object",
    "_set",
    "pk_columns",
    "on_conflict",
)

#: graphql-default's spelling of each argument, to the compiler's.
_GRAPHQL_DEFAULT_ARGUMENTS: dict[str, str] = {
    hasura_argument_name(GRAPHQL_DEFAULT, a): a for a in HASURA_ARGUMENTS
}


def table_hasura(table: TableMeta) -> str | None:
    """The Hasura convention ``table`` is served in, None for Provisa's shapes."""
    return hasura_convention_of(table.gql_convention or active_gql_convention())


def compiled_field(field: FieldNode, table: TableMeta, ctx: CompilationContext) -> FieldNode:
    """``field``, a field of ``table`` (a root field, or a relationship's), with each of its own
    and its relationships' Hasura arguments named as the compiler names them. Unchanged where no
    table it reaches is graphql-default."""
    renamed = _renamed_arguments(field, table)
    selection = _compiled_selection(field.selection_set, table, ctx)
    if renamed is field.arguments and selection is field.selection_set:
        return field
    out = copy(field)
    out.arguments = renamed
    out.selection_set = selection
    return out


def _renamed_arguments(field: FieldNode, table: TableMeta) -> tuple[ArgumentNode, ...]:
    args = tuple(field.arguments or ())
    if table_hasura(table) != GRAPHQL_DEFAULT:
        return field.arguments  # type: ignore[return-value]
    if not any(a.name.value in _GRAPHQL_DEFAULT_ARGUMENTS for a in args):
        return field.arguments  # type: ignore[return-value]
    out = []
    for arg in args:
        name = _GRAPHQL_DEFAULT_ARGUMENTS.get(arg.name.value)
        if name is None:
            out.append(arg)
            continue
        renamed = copy(arg)
        renamed.name = NameNode(value=name)
        out.append(renamed)
    return tuple(out)


def _compiled_selection(
    selection: SelectionSetNode | None, table: TableMeta, ctx: CompilationContext
) -> SelectionSetNode | None:
    """``selection`` of ``table``'s rows: each relationship field compiled against its target;
    any other field holding a selection (an aggregate's ``nodes``, a mutation's ``returning``)
    read as more of ``table``'s rows."""
    if selection is None:
        return None
    changed = False
    out = []
    for sel in selection.selections:
        new = sel
        if isinstance(sel, FieldNode) and sel.selection_set is not None:
            join = ctx.joins.get((table.type_name, sel.name.value))
            if join is not None:
                new = compiled_field(sel, join.target, ctx)
            else:
                inner = _compiled_selection(sel.selection_set, table, ctx)
                if inner is not sel.selection_set:
                    new = copy(sel)
                    new.selection_set = inner
        elif isinstance(sel, InlineFragmentNode):
            inner = _compiled_selection(sel.selection_set, table, ctx)
            if inner is not sel.selection_set:
                new = copy(sel)
                new.selection_set = inner  # type: ignore[assignment]
        changed = changed or new is not sel
        out.append(new)
    if not changed:
        return selection
    copied = copy(selection)
    copied.selections = tuple(out)
    return copied


def by_pk_as_read(field: FieldNode, table: TableMeta) -> FieldNode:
    """``<table>_by_pk(<key>: v, ...)`` as the table's read of that row: ``<table>(where:
    {<key>: {_eq: v}, ...})``, answering under the field's own response key."""
    return FieldNode(
        alias=field.alias or NameNode(value=field.name.value),
        name=NameNode(value=table.field_name),
        arguments=(ArgumentNode(name=NameNode(value="where"), value=key_condition(field)),),
        directives=field.directives,
        selection_set=field.selection_set,
    )


def key_condition(field: FieldNode) -> ObjectValueNode:
    """The where condition naming the row of the key ``field``'s arguments give (each
    ``<key>: v`` is ``{<key>: {_eq: v}}``)."""
    args = tuple(field.arguments or ())
    return ObjectValueNode(
        fields=tuple(
            ObjectFieldNode(
                name=NameNode(value=a.name.value),
                value=ObjectValueNode(
                    fields=(ObjectFieldNode(name=NameNode(value="_eq"), value=a.value),)
                ),
            )
            for a in args
        )
    )
