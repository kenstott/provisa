# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One table's GraphQL schema and compiler context, for a test of gRPC's lowering.

A gRPC row read lowers to GraphQL text (``provisa.grpc.query_ir.grpc_table_to_graphql``) that the
compiler compiles against the role's schema, so a test of the lowering needs both: ``grpc_model``
builds them from a column list the way the server does, and ``compiled_read`` lowers a request and
compiles it, returning the statement and its bound values. ``serving_state`` is a state holding the
role's surface and what keeping the compiled plan reads (``api.generated_plan``)."""

from __future__ import annotations

from typing import Any, Sequence

from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.parser import parse_query
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import compile_query
from provisa.grpc.query_ir import grpc_table_to_graphql


def grpc_model(
    columns: Sequence[tuple[str, str]],
    *,
    parameters: Sequence[tuple[str, str]] = (),
    table: str = "orders",
    role: str = "admin",
    source_id: str = "pg1",
    schema_name: str = "public",
) -> tuple[Any, Any]:
    """``(schema, ctx)`` for one table of ``columns`` (name, registered type), table id 1, in
    the ``sales`` domain of ``source_id``'s ``schema_name``, every column readable by ``role``;
    ``parameters`` (name, type) are its parameters (``_nf_<name>`` columns, REQ-1986)."""
    param_columns = [
        {
            "column_name": f"_nf_{name}",
            "visible_to": [],
            "native_filter_type": "query_param",
            "native_filter_required": False,
        }
        for name, _type in parameters
    ]
    si = SchemaInput(
        tables=[
            {
                "id": 1,
                "source_id": source_id,
                "domain_id": "sales",
                "schema_name": schema_name,
                "table_name": table,
                "columns": [{"column_name": c, "visible_to": []} for c, _t in columns]
                + param_columns,
                "write_ops": [],
                "write_returns_rows": False,
                "write_refused_forms": [],
            }
        ],
        relationships=[],
        column_types={
            1: [
                ColumnMetadata(column_name=c, data_type=t, is_nullable=True)
                for c, t in [*columns, *((f"_nf_{n}", t) for n, t in parameters)]
            ]
        },
        naming_rules=[],
        role={"id": role, "domain_access": ["*"], "capabilities": []},
        domains=[{"id": "sales", "description": "Sales"}],
    )
    return generate_schema(si), build_context(si)


def compiled_read(
    model: tuple[Any, Any],
    type_name: str,
    limit: int,
    filter_msg: Any | None = None,
    read_mask: Any | None = None,
    params_msg: Any | None = None,
) -> tuple[str, list] | None:
    """The request lowered to GraphQL and compiled: ``(sql, bound values)``, or None when no
    table matches ``type_name``."""
    schema, ctx = model
    lowered = grpc_table_to_graphql(
        ctx, schema, type_name, limit, filter_msg, read_mask, params_msg
    )
    if lowered is None:
        return None
    text, variables = lowered
    (compiled,) = compile_query(parse_query(schema, text, variables, ctx=ctx), ctx, variables)
    return compiled.sql, list(compiled.params)


def serving_state(model: tuple[Any, Any], role: str = "admin", **attrs: Any) -> Any:
    """A state serving ``model`` to ``role``: its surface, and what the kept plan of a lowered
    request is keyed and anchored on (the generation, the role, its row filters and masks)."""
    from provisa.compiler.compiled_query_cache import CompiledQueryCache
    from tests.role_state import RoleState

    schema, ctx = model
    base: dict[str, Any] = {
        "org_id": "default",
        "multitenancy": False,
        "contexts": {role: ctx},
        "schemas": {role: schema},
        "rls_contexts": {},
        "roles": {role: {"id": role, "domain_access": ["*"], "capabilities": []}},
        "masking_rules": {},
        "tables": [],
        "schema_boot_id": "boot",
        "schema_version": 1,
        "compiled_query_cache": CompiledQueryCache(),
    }
    return RoleState(**{**base, **attrs})
