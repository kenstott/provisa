# Copyright (c) 2025 Kenneth Stott
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
