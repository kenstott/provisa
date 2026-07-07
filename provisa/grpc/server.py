# Copyright (c) 2026 Kenneth Stott
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

# Requirements: REQ-045, REQ-051, REQ-143, REQ-145, REQ-266

from __future__ import annotations

import importlib.util
import logging
import re
import sys
from typing import Any

import grpc
import grpc.aio

log = logging.getLogger(__name__)


def _pascal_to_snake(name: str) -> str:
    """Convert PascalCase to snake_case: CustomerSegments -> customer_segments."""
    return re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", name).lower()


def _load_module(path: str, name: str):
    """Dynamically load a Python module from a file path.

    Returns a cached module if the name is already in sys.modules to avoid
    re-executing the module body (which causes protobuf descriptor pool errors
    when the same .proto file is registered more than once).
    """
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load module from {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    try:
        spec.loader.exec_module(mod)
    except Exception:
        del sys.modules[name]
        raise
    return mod


def _get_role(context: grpc.aio.ServicerContext) -> str:
    """Extract role from gRPC metadata."""
    metadata = context.invocation_metadata()
    meta_dict: dict[str, Any] = dict(metadata)  # type: ignore[arg-type]
    raw = meta_dict.get("x-provisa-role")
    role = raw.decode() if isinstance(raw, bytes) else raw
    if not role:
        raise grpc.aio.AbortError(
            grpc.StatusCode.UNAUTHENTICATED, "Missing x-provisa-role metadata"
        )
    return role


class ProvisaServicer:  # REQ-045, REQ-143
    """Dynamic gRPC servicer that handles query RPCs."""

    def __init__(self, state, pb2_module, pb2_grpc_module):
        self._state = state
        self._pb2 = pb2_module
        self._pb2_grpc = pb2_grpc_module

    def __getattr__(self, name: str):
        """Dynamically resolve RPC handler methods like QueryOrders, InsertOrders."""
        if name.startswith("Query"):
            type_name = name[len("Query") :]
            # Convert PascalCase type name to snake_case field name
            field_name = _pascal_to_snake(type_name)

            async def query_handler(request, context):
                async for msg in self._handle_query(request, context, type_name, field_name):
                    yield msg

            return query_handler
        if name.startswith("Insert"):
            type_name = name[len("Insert") :]

            async def insert_handler(request, context):
                return await self._handle_insert(request, context, type_name)

            return insert_handler
        raise AttributeError(f"{type(self).__name__!r} has no attribute {name!r}")

    async def _handle_insert(self, request, context, type_name: str):
        """Stub handler for insert RPCs."""
        await context.abort(grpc.StatusCode.UNIMPLEMENTED, f"Insert{type_name} not yet implemented")

    async def _handle_query(self, request, context, type_name: str, field_name: str):
        """Lower a proto query request directly to the IR (a semantic SELECT), then run the shared
        governance → routing → physical pipeline — the same path SQL and Cypher use. gRPC never
        round-trips through GraphQL (query language → IR → governed IR → plan → physical)."""

        from provisa.grpc.query_ir import grpc_table_to_semantic_sql
        from provisa.pgwire._pipeline import _execute_plan, _govern_and_route_compiled

        # Use await context.abort() directly rather than raising AbortError, which
        # can cause "Abort error has been replaced!" in gRPC aio async generators.
        metadata = dict(context.invocation_metadata())
        role_id = metadata.get("x-provisa-role")
        if not role_id:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "Missing x-provisa-role metadata")
            return
        state = self._state

        if role_id not in state.contexts:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"No schema for role {role_id!r}")
            return
        ctx = state.contexts[role_id]

        msg_cls = getattr(self._pb2, type_name, None)
        if msg_cls is None:
            await context.abort(grpc.StatusCode.INTERNAL, f"Unknown message type {type_name}")
            return
        descriptor = msg_cls.DESCRIPTOR

        # IR: lower the request straight to a semantic SELECT (shared with the HTTP gRPC proxy), then
        # govern → route → physical exactly as the SQL/Cypher transports do.
        semantic_sql = grpc_table_to_semantic_sql(ctx, type_name, request.limit)
        if semantic_sql is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"No table for type {type_name!r}")
            return

        def _norm(s: str) -> str:
            return s.replace("_", "").lower()

        try:
            plan = await _govern_and_route_compiled(semantic_sql, role_id, state=state)
            result = await _execute_plan(plan, state)
        except PermissionError as exc:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, str(exc))
            return

        # Stream rows as proto messages, mapping result column names to proto fields by the same key
        # (governance may re-case or alias a column).
        _proto_by_norm = {_norm(f.name): f.name for f in descriptor.fields}
        out_cols = [_proto_by_norm.get(_norm(c), c) for c in result.column_names]
        for row in result.rows:
            kwargs = {}
            for i, col in enumerate(out_cols):
                if i < len(row) and row[i] is not None:
                    kwargs[col] = row[i]
            yield msg_cls(**kwargs)


async def start_grpc_server(
    port: int, state, pb2_path: str, pb2_grpc_path: str
) -> grpc.aio.Server:  # REQ-045, REQ-143
    """Start a gRPC async server with the Provisa service.

    Args:
        port: Port to listen on.
        state: AppState with schemas, contexts, etc.
        pb2_path: Path to generated _pb2.py module.
        pb2_grpc_path: Path to generated _pb2_grpc.py module.

    Returns:
        The started grpc.aio.Server.
    """
    import os

    # Derive module names from the file stems so that _pb2_grpc.py can
    # successfully import its sibling _pb2 module by the expected name.
    pb2_name = os.path.splitext(os.path.basename(pb2_path))[0]
    pb2_grpc_name = os.path.splitext(os.path.basename(pb2_grpc_path))[0]
    pb2 = _load_module(pb2_path, pb2_name)
    pb2_grpc = _load_module(pb2_grpc_path, pb2_grpc_name)

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

    service_names = [
        pb2.DESCRIPTOR.services_by_name[s].full_name for s in pb2.DESCRIPTOR.services_by_name
    ]
    enable_reflection(server, service_names)

    server.add_insecure_port(f"[::]:{port}")
    await server.start()
    log.info("gRPC server started on port %d", port)
    return server
