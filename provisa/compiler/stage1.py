# Copyright (c) 2026 Kenneth Stott
# Canary: 45689efa-5710-4d32-93d1-cb3a8d8cf376
# (run scripts/canary_stamp.py on this file after creating it)

"""Stage 1: GraphQL AST → SQL compiler (REQ-262).

Thin wrapper around sql_gen.compile_query that makes the two-stage
pipeline explicit. Input: validated GraphQL AST + compilation context.
Output: CompiledQuery objects (plain PG-style SQL, before governance).
"""
from __future__ import annotations

from graphql import DocumentNode

from provisa.compiler.sql_gen import CompiledQuery, CompilationContext, compile_query


def compile_graphql(
    document: DocumentNode,
    ctx: CompilationContext,
    variables: dict | None = None,
    *,
    use_catalog: bool = False,
) -> list[CompiledQuery]:
    """Stage 1: compile a validated GraphQL document to SQL.

    Returns one CompiledQuery per root selection field.
    """
    return compile_query(document, ctx, variables, use_catalog=use_catalog)
