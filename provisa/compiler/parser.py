# Copyright (c) 2026 Kenneth Stott
# Canary: 7af1e236-832c-4e9b-b7e1-1542c0d6e9e6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Parse and validate GraphQL operations against a generated schema.

Uses graphql-core directly (REQ-007). No third-party GraphQL framework.
"""

# Requirements: REQ-007, REQ-011, REQ-039, REQ-300

from typing import TYPE_CHECKING

from graphql import (
    DocumentNode,
    GraphQLSchema,
    parse,
    validate,
)
from graphql.language.ast import (
    BooleanValueNode,
    EnumValueNode,
    FloatValueNode,
    IntValueNode,
    ListValueNode,
    NullValueNode,
    ObjectValueNode,
    OperationDefinitionNode,
    StringValueNode,
)


if TYPE_CHECKING:
    from provisa.compiler.sql_gen import CompilationContext


class GraphQLValidationError(Exception):
    """Raised when a GraphQL query fails validation against the schema."""

    def __init__(self, errors: list):
        self.errors = errors
        messages = "; ".join(str(e) for e in errors)
        super().__init__(f"GraphQL validation failed: {messages}")


def _refuse_unoffered_writes(
    document: DocumentNode, schema: GraphQLSchema, ctx: "CompilationContext"
) -> None:  # REQ-209, REQ-1925
    """A mutation field naming a table the role reads, for an operation the table does not take,
    is refused by name (``data.write_not_supported``) — the refusal every surface gives such a
    write. The schema offers a table only the mutations its source takes, so the field is absent
    and validation alone would answer with an unnamed "Cannot query field"."""
    from graphql import OperationDefinitionNode, OperationType

    from provisa.compiler.mutation_gen import _get_mutation_meta
    from provisa.compiler.write_admission import WriteNotSupported

    mutation_type = schema.mutation_type
    offered = set(mutation_type.fields) if mutation_type is not None else set()
    for definition in document.definitions:
        if not isinstance(definition, OperationDefinitionNode):
            continue
        if definition.operation != OperationType.MUTATION:
            continue
        for selection in definition.selection_set.selections:
            name = getattr(getattr(selection, "name", None), "value", None)
            if name is None or name in offered:
                continue
            try:
                operation, _field, table = _get_mutation_meta(name, ctx)
            except ValueError:
                continue  # names no table the role reads: validation answers it
            if operation == "upsert":
                # An upsert inserts or updates: what is missing is the insert, unless the table's
                # insert field is offered, in which case it is the update.
                operation = "update" if name.replace("upsert", "insert", 1) in offered else "insert"
            raise WriteNotSupported(table.table_name, operation)


def parse_query(  # REQ-007, REQ-011, REQ-039
    schema: GraphQLSchema,
    query: str,
    variables: dict | None = None,
    *,
    ctx: "CompilationContext",
) -> DocumentNode:
    """Parse and validate a GraphQL query string against a schema.

    Args:
        schema: The graphql-core schema to validate against.
        query: GraphQL query string.
        variables: Optional variable values (validated at execution, not here).

    Returns:
        Validated DocumentNode ready for compilation.

    Raises:
        GraphQLValidationError: If the query is invalid against the schema.
        WriteNotSupported: If a mutation names a table the role reads for an operation the
            table does not take (``ctx`` resolves the field to its table).
        graphql.error.GraphQLSyntaxError: If the query has syntax errors.
    """
    document = parse(query)
    errors = validate(schema, document)
    if errors:
        _refuse_draft_fields(document, ctx)
        _refuse_unoffered_writes(document, schema, ctx)
        raise GraphQLValidationError(errors)
    return document


#: How a root field names its table beyond the table's own field name (query and mutation forms).
_FIELD_SUFFIXES = ("_aggregate", "_by_pk", "_connection", "_stream")
_FIELD_PREFIXES = ("insert_", "update_", "delete_", "upsert_")


def _refuse_draft_fields(document: DocumentNode, ctx: "CompilationContext") -> None:  # REQ-1921
    """A root field naming a draft table is refused naming it as draft (``TableIsDraft``); the
    schema offers no draft table, so validation alone would answer an unnamed "Cannot query
    field"."""
    from graphql import OperationDefinitionNode

    from provisa.compiler.definitions import TableIsDraft

    if not ctx.draft_names:
        return
    for definition in document.definitions:
        if not isinstance(definition, OperationDefinitionNode):
            continue
        for selection in definition.selection_set.selections:
            name = getattr(getattr(selection, "name", None), "value", None)
            if name is None:
                continue
            for candidate in _table_field_candidates(name):
                if candidate in ctx.draft_names:
                    raise TableIsDraft(ctx.draft_names[candidate])


def _table_field_candidates(name: str) -> list[str]:
    """The table field names a root field ``name`` could stand for."""
    out = [name]
    for prefix in _FIELD_PREFIXES:
        if name.startswith(prefix):
            out.append(name[len(prefix) :])
    for base in list(out):
        for suffix in _FIELD_SUFFIXES:
            if base.endswith(suffix):
                out.append(base[: -len(suffix)])
    return out


def _ast_default_to_python(
    node: object,  # object-ok: graphql-core AST value nodes share no common base type; return is a heterogeneous Python scalar
) -> object:  # object-ok: heterogeneous Python scalar (str, int, float, bool, None, list, dict)
    """Convert an AST default value node to a Python value."""
    if isinstance(node, StringValueNode):
        return node.value
    if isinstance(node, IntValueNode):
        return int(node.value)
    if isinstance(node, FloatValueNode):
        return float(node.value)
    if isinstance(node, BooleanValueNode):
        return node.value
    if isinstance(node, EnumValueNode):
        return node.value
    if isinstance(node, NullValueNode):
        return None
    if isinstance(node, ListValueNode):
        return [_ast_default_to_python(v) for v in node.values]
    if isinstance(node, ObjectValueNode):
        return {f.name.value: _ast_default_to_python(f.value) for f in node.fields}
    return None


def coerce_variable_defaults(  # REQ-300
    document: DocumentNode,
    variables: dict | None,
) -> dict:
    """Return a variables dict with defaults applied for any missing variables.

    GraphQL spec §6.4.1: if a variable is missing from the supplied variables
    but has a default value in the operation definition, the default is used.
    """
    result: dict = dict(variables) if variables else {}
    for defn in document.definitions:
        if not isinstance(defn, OperationDefinitionNode):
            continue
        for var_def in defn.variable_definitions:
            name = var_def.variable.name.value
            if name not in result and var_def.default_value is not None:
                result[name] = _ast_default_to_python(var_def.default_value)
    return result
