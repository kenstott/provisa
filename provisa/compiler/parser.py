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


class GraphQLValidationError(Exception):
    """Raised when a GraphQL query fails validation against the schema."""

    def __init__(self, errors: list):
        self.errors = errors
        messages = "; ".join(str(e) for e in errors)
        super().__init__(f"GraphQL validation failed: {messages}")


def parse_query(
    schema: GraphQLSchema,
    query: str,
    variables: dict | None = None,
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
        graphql.error.GraphQLSyntaxError: If the query has syntax errors.
    """
    document = parse(query)
    errors = validate(schema, document)
    if errors:
        raise GraphQLValidationError(errors)
    return document


def _ast_default_to_python(node: object) -> object:
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


def coerce_variable_defaults(
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
