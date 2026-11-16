# Copyright (c) 2026 Kenneth Stott
# Canary: 8a4e2f61-3c9d-4b7a-a1e5-6f0d9c2b7e34
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a held organization's member enters into the admin API names no variable of the server's
environment (REQ-1557).

The write seam refuses such text when it is SAVED (``core.env_reference_guard``). An admin
operation may resolve what it was given before it saves it, or without saving it at all (a new
source's connection is dialled first; a connection test saves nothing), so the arguments
themselves are checked here, once, before the operation runs: every argument of the document, as
GraphQL parsed it, with its variables in place. The variable is never read.
"""

# Requirements: REQ-1557

from __future__ import annotations

from typing import Any

from graphql import ExecutionResult, GraphQLError
from graphql.language import ArgumentNode, Visitor, visit
from graphql.utilities import value_from_ast_untyped
from strawberry.extensions import SchemaExtension

from provisa.core.env_reference_guard import EnvReferenceRefused, refuse_entered


class _Arguments(Visitor):
    """Every argument value of a document, as plain values."""

    def __init__(self, variables: dict[str, Any]) -> None:
        super().__init__()
        self._variables = variables
        self.values: list[Any] = []

    def enter_argument(self, node: ArgumentNode, *_: Any) -> None:
        self.values.append({node.name.value: value_from_ast_untyped(node.value, self._variables)})


def entered_arguments(document: Any, variables: dict[str, Any] | None) -> list[Any]:
    """Each argument of ``document`` with ``variables`` in place: what the caller entered."""
    visitor = _Arguments(variables or {})
    visit(document, visitor)
    return visitor.values


class EnteredEnvReference(SchemaExtension):
    """Refuse the operation of a held organization's member whose arguments name a variable of
    the server's environment, before any of it runs."""

    def on_execute(self):
        context = self.execution_context
        try:
            refuse_entered(*entered_arguments(context.graphql_document, context.variables))
        except EnvReferenceRefused as refused:
            context.result = ExecutionResult(
                data=None,
                errors=[
                    GraphQLError(
                        str(refused),
                        extensions={"code": refused.code, "params": refused.params},
                    )
                ],
            )
        yield
