# Copyright (c) 2025 Kenneth Stott
# Canary: 9f27e468-893c-47c0-a67c-c2cbff9e0894
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""gRPC server that serves queries over generated proto service.

Each RPC: extract role from metadata -> look up context -> build SQL -> execute -> stream rows.
"""

from __future__ import annotations

import importlib.util
import logging
import sys
from pathlib import Path

import re

import grpc
import grpc.aio

log = logging.getLogger(__name__)


def _pascal_to_snake(name: str) -> str:
    """Convert PascalCase to snake_case: CustomerSegments -> customer_segments."""
    return re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", name).lower()


def _load_module(path: str, name: str):
    """Dynamically load a Python module from a file path."""
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load module from {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def _get_role(context: grpc.aio.ServicerContext) -> str:
    """Extract role from gRPC metadata."""
    metadata = dict(context.invocation_metadata())
    role = metadata.get("x-provisa-role")
    if not role:
        raise grpc.aio.AbortError(grpc.StatusCode.UNAUTHENTICATED, "Missing x-provisa-role metadata")
    return role


class ProvisaServicer:
    """Dynamic gRPC servicer that handles query RPCs."""

    def __init__(self, state, pb2_module, pb2_grpc_module):
        self._state = state
        self._pb2 = pb2_module
        self._pb2_grpc = pb2_grpc_module

    def __getattr__(self, name: str):
        """Dynamically resolve RPC handler methods like QueryOrders, InsertOrders."""
        if name.startswith("Query"):
            type_name = name[len("Query"):]
            # Convert PascalCase type name to snake_case field name
            field_name = _pascal_to_snake(type_name)

            async def handler(request, context):
                async for msg in self._handle_query(request, context, type_name, field_name):
                    yield msg

            return handler
        if name.startswith("Insert"):
            type_name = name[len("Insert"):]

            async def handler(request, context):
                return await self._handle_insert(request, context, type_name)

            return handler
        raise AttributeError(f"{type(self).__name__!r} has no attribute {name!r}")

    async def _handle_insert(self, request, context, type_name: str):
        """Stub handler for insert RPCs."""
        await context.abort(grpc.StatusCode.UNIMPLEMENTED, f"Insert{type_name} not yet implemented")

    async def _handle_query(self, request, context, type_name: str, field_name: str):
        """Generic query handler for any table RPC."""
        from graphql import parse as gql_parse

        from provisa.compiler.parser import parse_query
        from provisa.compiler.sql_gen import compile_query
        from provisa.compiler.rls import RLSContext, inject_rls
        from provisa.compiler.mask_inject import inject_masking
        from provisa.mv.rewriter import rewrite_if_mv_match
        from provisa.compiler.sampling import apply_sampling, get_sample_size
        from provisa.transpiler.router import Route, decide_route
        from provisa.transpiler.transpile import transpile, transpile_to_trino
        from provisa.executor.direct import execute_direct
        from provisa.executor.trino import execute_trino
        from provisa.security.rights import Capability, has_capability

        role_id = _get_role(context)
        state = self._state

        if role_id not in state.schemas:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"No schema for role {role_id!r}")

        schema = state.schemas[role_id]
        ctx = state.contexts[role_id]
        rls = state.rls_contexts.get(role_id, RLSContext.empty())
        role = state.roles.get(role_id)

        # Build a GraphQL query from the proto request
        gql_query = f"{{ {field_name} {{ "
        # Get visible fields from the message type
        msg_cls = getattr(self._pb2, type_name, None)
        if msg_cls is None:
            await context.abort(grpc.StatusCode.INTERNAL, f"Unknown message type {type_name}")
        descriptor = msg_cls.DESCRIPTOR
        field_names = [f.name for f in descriptor.fields if not f.message_type]
        gql_query += " ".join(field_names)
        gql_query += " } }"

        # Parse and compile
        document = parse_query(schema, gql_query)
        compiled_queries = compile_query(document, ctx)
        if not compiled_queries:
            await context.abort(grpc.StatusCode.INTERNAL, "No query fields compiled")
        compiled = compiled_queries[0]

        # Apply RLS, masking, MV rewrite
        compiled = inject_rls(compiled, ctx, rls)
        compiled = inject_masking(compiled, ctx, state.masking_rules, role_id)
        fresh_mvs = state.mv_registry.get_fresh()
        compiled = rewrite_if_mv_match(compiled, fresh_mvs)

        # Route
        decision = decide_route(
            sources=compiled.sources,
            source_types=state.source_types,
            source_dialects=state.source_dialects,
        )

        # Sampling
        sampling = not has_capability(role, Capability.FULL_RESULTS) if role else True
        if sampling:
            compiled = apply_sampling(compiled, get_sample_size())

        # Execute
        if decision.route == Route.DIRECT and decision.source_id:
            target_sql = transpile(compiled.sql, decision.dialect or "postgres")
            result = await execute_direct(
                state.source_pools, decision.source_id, target_sql, compiled.params,
            )
        else:
            compiled = compile_query(document, ctx, use_catalog=True)[0]
            compiled = inject_rls(compiled, ctx, rls)
            compiled = inject_masking(compiled, ctx, state.masking_rules, role_id)
            compiled = rewrite_if_mv_match(compiled, fresh_mvs)
            if sampling:
                compiled = apply_sampling(compiled, get_sample_size())
            trino_sql = transpile_to_trino(compiled.sql)
            result = execute_trino(state.trino_conn, trino_sql, compiled.params)

        # Stream rows as proto messages
        msg_cls = getattr(self._pb2, type_name)
        col_names = [c.field_name for c in compiled.columns if c.nested_in is None]
        for row in result.rows:
            kwargs = {}
            for i, col_name in enumerate(col_names):
                if i < len(row) and row[i] is not None:
                    kwargs[col_name] = row[i]
            yield msg_cls(**kwargs)


async def start_grpc_server(port: int, state, pb2_path: str, pb2_grpc_path: str) -> grpc.aio.Server:
    """Start a gRPC async server with the Provisa service.

    Args:
        port: Port to listen on.
        state: AppState with schemas, contexts, etc.
        pb2_path: Path to generated _pb2.py module.
        pb2_grpc_path: Path to generated _pb2_grpc.py module.

    Returns:
        The started grpc.aio.Server.
    """
    pb2 = _load_module(pb2_path, "provisa_service_pb2")
    pb2_grpc = _load_module(pb2_grpc_path, "provisa_service_pb2_grpc")

    servicer = ProvisaServicer(state, pb2, pb2_grpc)

    # Register handlers dynamically from the generated stub
    server = grpc.aio.server()

    # Find the add_*Servicer_to_server function
    add_fn_name = None
    for attr in dir(pb2_grpc):
        if attr.startswith("add_") and attr.endswith("Servicer_to_server"):
            add_fn_name = attr
            break

    if add_fn_name is None:
        raise RuntimeError("No servicer registration function found in generated grpc stub")

    add_fn = getattr(pb2_grpc, add_fn_name)
    add_fn(servicer, server)

    # Enable reflection
    from provisa.grpc.reflection import enable_reflection
    service_names = [pb2.DESCRIPTOR.services_by_name[s].full_name for s in pb2.DESCRIPTOR.services_by_name]
    enable_reflection(server, service_names)

    server.add_insecure_port(f"[::]:{port}")
    await server.start()
    log.info("gRPC server started on port %d", port)
    return server
