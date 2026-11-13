# Copyright (c) 2026 Kenneth Stott
# Canary: a89b0f7e-a1da-4a1d-8fdf-857d4f9ce664
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The relations a request names, read before it is governed: the names a SQL statement gives
its tables, and the root fields of a GraphQL document that only reads. A draft named by either
is read through the drafts' own view (REQ-1921, admin/table_read)."""

# Requirements: REQ-1921

from __future__ import annotations


def sql_relations(sql: str) -> list[str]:
    """Each table ``sql`` names, as ``schema.table`` or ``table``, in the order named. A statement
    that does not parse is refused here as the pipeline refuses one (``SQL parse error``)."""
    import sqlglot
    import sqlglot.expressions as exp
    from sqlglot.errors import ParseError

    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except ParseError as exc:
        raise ValueError(f"SQL parse error: {exc}") from exc
    return [f"{t.db}.{t.name}" if t.db else t.name for t in tree.find_all(exp.Table)]


def query_root_fields(query: str) -> list[str]:
    """The root field names of GraphQL ``query`` when every operation in it is a query; none for
    a document holding a mutation or a subscription. A document that does not parse names none:
    it is refused where every document is parsed (compiler/parser.parse_query), with its own
    error."""
    from graphql import GraphQLSyntaxError, OperationDefinitionNode, OperationType, parse

    try:
        document = parse(query)
    except GraphQLSyntaxError:
        return []
    operations = [d for d in document.definitions if isinstance(d, OperationDefinitionNode)]
    if any(op.operation != OperationType.QUERY for op in operations):
        return []
    return [
        name
        for op in operations
        for selection in op.selection_set.selections
        if (name := getattr(getattr(selection, "name", None), "value", None)) is not None
    ]
