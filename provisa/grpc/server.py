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

import grpc
import grpc.aio

log = logging.getLogger(__name__)


def _proto_value(field, value):
    """Adapt a driver row value to the proto field's wire type.

    Protobuf assignment is exact-typed: a ``datetime`` into a ``string`` field, or into a
    ``google.protobuf.Timestamp`` field, raises ``TypeError: bad argument type for built-in
    operation`` — the whole RPC dies with StatusCode.UNKNOWN. The driver's Python type is decided by
    the PHYSICAL column (asyncpg hands back ``datetime`` for TIMESTAMP, ``Decimal`` for NUMERIC)
    while the proto field type comes from the REGISTERED column type, so the two disagree by design
    on any column whose registration widens or narrows the physical type. Every other transport
    normalizes at its own serialization boundary (serialize_rows / Arrow / the pgwire encoders);
    this is gRPC's.
    """
    from datetime import date, datetime

    from google.protobuf.descriptor import FieldDescriptor

    if field.type == FieldDescriptor.TYPE_MESSAGE:
        if field.message_type.full_name == "google.protobuf.Timestamp":
            ts = field.message_type._concrete_class()
            ts.FromDatetime(value if isinstance(value, datetime) else datetime.fromisoformat(value))
            return ts
        return value
    if field.type == FieldDescriptor.TYPE_STRING:
        return value if isinstance(value, str) else str(value)
    if field.type in (FieldDescriptor.TYPE_DOUBLE, FieldDescriptor.TYPE_FLOAT):
        return float(value)
    if field.type in (
        FieldDescriptor.TYPE_INT32,
        FieldDescriptor.TYPE_INT64,
        FieldDescriptor.TYPE_UINT32,
        FieldDescriptor.TYPE_UINT64,
    ):
        return int(value.toordinal()) if isinstance(value, (date, datetime)) else int(value)
    if field.type == FieldDescriptor.TYPE_BOOL:
        return bool(value)
    if field.type == FieldDescriptor.TYPE_BYTES:
        return value if isinstance(value, bytes) else str(value).encode()
    return value


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


def _rpc_role(metadata: dict) -> str | None:
    """The role this RPC runs as (REQ-273).

    On a secured deployment the auth interceptor has already validated the caller's credential and
    derived the role from that identity; it is published on the task and is the only value a handler
    may use, so ``x-provisa-role`` here is the client's request and has no authority. On an
    unsecured deployment there is no identity to derive from and the metadata role stands.
    """
    from provisa.grpc.auth import authorized_role

    authorized = authorized_role()
    if authorized is not None:
        return authorized
    raw = metadata.get("x-provisa-role")
    return raw.decode() if isinstance(raw, bytes) else raw


class ProvisaServicer:  # REQ-045, REQ-143
    """Dynamic gRPC servicer that handles query RPCs."""

    def __init__(self, state, pb2_module, pb2_grpc_module):
        self._state = state
        self._pb2 = pb2_module
        self._pb2_grpc = pb2_grpc_module

    async def _resolve_org(self, requested: str | None) -> str | None:
        """The org this RPC binds: the principal's membership decides, not the metadata (REQ-1337).

        With no validated principal (unsecured deployment) the requested org is all there is, and
        the caller's own check rejects a missing one."""
        from provisa.grpc.auth import authenticated_identity

        identity = authenticated_identity()
        if identity is None:
            return requested
        from provisa.api.org_resolve import resolve_session_org
        from provisa.security.rights import can_act_cross_org, capabilities_for_claims

        caps = capabilities_for_claims(identity.roles or [], getattr(self._state, "roles", {}))
        return await resolve_session_org(
            self._state,
            user_id=identity.user_id,
            can_act_any_org=can_act_cross_org(caps),
            requested_org=requested or identity.active_org_id,
        )

    async def _bind_org(self, metadata: dict):
        """Resolve+build the RPC's org and bind it (REQ-1266, REQ-1337).

        On a secured deployment the org comes from the validated principal's memberships — the same
        rule MCP, pgwire and Flight use — so ``x-provisa-org`` is a REQUEST honored only for a
        principal holding the cross-org right, and a caller cannot name someone else's org. An
        unsecured deployment has no principal to resolve, so the metadata org stands; under
        multitenancy it is REQUIRED — a missing org raises ``ValueError`` (the caller aborts) rather
        than silently binding the default. Returns the reset token, or None for single-org
        deployments (ContextVar left unset → default runtime). Handlers run on the grpc.aio loop in
        a per-RPC task, so a plain set/reset isolates the binding."""
        if not getattr(self._state, "multitenancy", False):
            return None
        raw = metadata.get("x-provisa-org")
        requested = raw.decode() if isinstance(raw, bytes) else raw
        org_id = await self._resolve_org(requested)
        if not org_id:
            raise ValueError("Missing x-provisa-org metadata")
        from provisa.api.app import ensure_org_runtime
        from provisa.api.org_runtime import set_current_org

        await ensure_org_runtime(org_id)
        return set_current_org(org_id)

    def __getattr__(self, name: str):
        """Dynamically resolve RPC handler methods like QueryOrders, InsertOrders."""
        # REQ-1359: Query{Type}Aggregate / Query{Type}GroupBy must resolve BEFORE the generic
        # Query{Type} branch below — both prefixes also start with "Query".
        if name.startswith("Query") and name.endswith("Aggregate"):
            type_name = name[len("Query") : -len("Aggregate")]

            async def query_aggregate_handler(request, context):
                return await self._handle_query_aggregate(request, context, type_name)

            return query_aggregate_handler
        if name.startswith("Query") and name.endswith("GroupBy"):
            type_name = name[len("Query") : -len("GroupBy")]

            async def query_group_by_handler(request, context):
                async for msg in self._handle_query_group_by(request, context, type_name):
                    yield msg

            return query_group_by_handler
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
        if name == "CallCommand":  # REQ-1156

            async def call_command_handler(request, context):
                return await self._handle_call_command(request, context)

            return call_command_handler
        if name.startswith("Call"):  # REQ-1156 — per-command typed RPC Call{Cmd}
            cmd_name = self._resolve_command_rpc(name[len("Call") :])

            async def typed_command_handler(request, context):
                return await self._handle_typed_command(request, context, cmd_name)

            return typed_command_handler
        raise AttributeError(f"{type(self).__name__!r} has no attribute {name!r}")

    def _emit_license_nag(self, context) -> None:
        """Attach the REQ-1137 license nag to the RPC's trailing metadata, once per peer.

        Trailing metadata is an out-of-band channel — the response messages/stream are untouched.
        Best-effort: a failure never affects the RPC."""
        try:
            from provisa.licensing import emit as _lic_emit

            text = _lic_emit.nag_for_connection(f"grpc:{context.peer()}")
            if text:
                context.set_trailing_metadata(
                    (("x-provisa-license-notice", text.replace("\n", " ")),)
                )
        except Exception:
            log.debug("gRPC license nag emission skipped", exc_info=True)

    def _resolve_command_rpc(self, cmd_pascal: str) -> str | None:
        """Reverse the Call{Cmd} RPC name to the registered command name (REQ-1156).

        proto_gen names each per-command RPC ``Call`` + command_rpc_name(name); match that same
        authority back so ``CallActiveUsers`` resolves to ``active_users``. None if no command matches
        (the handler then aborts NOT_FOUND)."""
        from provisa.grpc.proto_gen import command_rpc_name

        for fn_name in [
            *getattr(self._state, "tracked_functions", {}),
            *getattr(self._state, "tracked_webhooks", {}),
        ]:
            if command_rpc_name(fn_name) == cmd_pascal:
                return fn_name
        return None

    async def _handle_typed_command(self, request, context, cmd_name: str | None):
        """Invoke a per-command RPC's command via the one governed executor (REQ-1156).

        Reads declared arguments off the typed request message, routes through
        invoke_tracked_function (writable_by/governance enforced there), and returns the command's
        rows as CommandResponse JSON (query) or an affected-row MutationResponse (mutation)."""
        import json

        from provisa.api.data.action_exec import invoke_tracked_function

        metadata = dict(context.invocation_metadata())
        role_id = _rpc_role(metadata)
        if not role_id:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "Missing x-provisa-role metadata")
            return
        try:
            _org_token = await self._bind_org(metadata)  # REQ-1266
        except ValueError as exc:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, str(exc))
            return
        try:
            state = self._state
            fn = (
                getattr(state, "tracked_functions", {}).get(cmd_name)
                or getattr(state, "tracked_webhooks", {}).get(cmd_name)
                if cmd_name
                else None
            )
            if fn is None or cmd_name is None:
                await context.abort(grpc.StatusCode.NOT_FOUND, f"Unknown command {cmd_name!r}")
                return
            args = {
                a_name: getattr(request, a_name)
                for arg in (fn.get("arguments") or [])
                if (a_name := arg.get("name"))
            }
            try:
                rows = await invoke_tracked_function(cmd_name, args, state, role_id)
            except PermissionError as exc:
                await context.abort(grpc.StatusCode.PERMISSION_DENIED, str(exc))
                return
            self._emit_license_nag(context)  # REQ-1137
            if fn.get("kind") == "mutation":
                return self._pb2.MutationResponse(affected_rows=len(rows))
            return self._pb2.CommandResponse(rows_json=json.dumps(rows, default=str))
        finally:
            if _org_token is not None:
                from provisa.api.org_runtime import reset_current_org

                reset_current_org(_org_token)

    async def _handle_call_command(self, request, context):
        """Invoke a registered command (tracked function) via the one governed executor (REQ-1156).

        Request: {name, args_json}; response: {rows_json}. writable_by/governance is enforced inside
        invoke_tracked_function, identical to the GraphQL/SQL/Cypher surfaces."""
        import json

        from provisa.api.data.action_exec import invoke_tracked_function

        metadata = dict(context.invocation_metadata())
        role_id = _rpc_role(metadata)
        if not role_id:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "Missing x-provisa-role metadata")
            return
        try:
            _org_token = await self._bind_org(metadata)  # REQ-1266
        except ValueError as exc:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, str(exc))
            return
        try:
            state = self._state
            if request.name not in getattr(state, "tracked_functions", {}):
                await context.abort(grpc.StatusCode.NOT_FOUND, f"Unknown command {request.name!r}")
                return
            try:
                args = json.loads(request.args_json) if request.args_json else {}
            except json.JSONDecodeError as exc:
                await context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT, f"args_json not valid JSON: {exc}"
                )
                return
            try:
                rows = await invoke_tracked_function(request.name, args, state, role_id)
            except PermissionError as exc:
                await context.abort(grpc.StatusCode.PERMISSION_DENIED, str(exc))
                return
            return self._pb2.CommandResponse(rows_json=json.dumps(rows, default=str))
        finally:
            if _org_token is not None:
                from provisa.api.org_runtime import reset_current_org

                reset_current_org(_org_token)

    async def _handle_insert(self, request, context, type_name: str):
        """Stub handler for insert RPCs."""
        await context.abort(grpc.StatusCode.UNIMPLEMENTED, f"Insert{type_name} not yet implemented")

    async def _handle_query(self, request, context, type_name: str, field_name: str):
        """Lower a proto query request directly to the IR (a semantic SELECT), then run the shared
        governance → routing → physical pipeline — the same path SQL and Cypher use. gRPC never
        round-trips through GraphQL (query language → IR → governed IR → plan → physical).

        REQ-1266: resolve+bind the RPC's org (x-provisa-org) before routing; the body runs bound so
        every state.X read resolves the org's runtime. The reset is in finally around the stream."""
        # Use await context.abort() directly rather than raising AbortError, which
        # can cause "Abort error has been replaced!" in gRPC aio async generators.
        metadata = dict(context.invocation_metadata())
        role_id = _rpc_role(metadata)
        if not role_id:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "Missing x-provisa-role metadata")
            return
        try:
            _org_token = await self._bind_org(metadata)
        except ValueError as exc:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, str(exc))
            return
        try:
            async for _m in self._handle_query_bound(request, context, type_name, role_id):
                yield _m
        finally:
            if _org_token is not None:
                from provisa.api.org_runtime import reset_current_org

                reset_current_org(_org_token)

    async def _handle_query_bound(self, request, context, type_name: str, role_id: str):
        """The routing+execution body of _handle_query, run with the RPC's org already bound."""
        import asyncio

        from provisa.grpc.query_ir import grpc_table_to_semantic_sql
        from provisa.pgwire._pipeline import (
            _execute_plan,
            _govern_and_route_compiled,
            require_governed_plan,
        )
        from provisa.transpiler.router import Route

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
        except PermissionError as exc:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, str(exc))
            return

        def _kwargs_for(out_cols: list[str], row) -> dict:
            kwargs = {}
            for i, col in enumerate(out_cols):
                if i < len(row) and row[i] is not None:
                    field = descriptor.fields_by_name.get(col)
                    # No field => let msg_cls(**kwargs) raise on the unknown name rather than
                    # dropping the column silently.
                    kwargs[col] = _proto_value(field, row[i]) if field is not None else row[i]
            return kwargs

        # ENGINE route streams lazily off a worker thread — the full user result set never
        # materializes on the event loop (REQ-1215). grpc.aio runs an async generator, so each
        # batch is pulled through run_in_executor to keep the blocking cursor off the loop; peak
        # memory is bounded by one batch, not the whole result.
        if plan.route == Route.ENGINE:
            require_governed_plan(plan)  # REQ-1176: streaming terminal verifies the stamp too
            assert plan.physical_sql is not None
            loop = asyncio.get_running_loop()
            stream = await loop.run_in_executor(
                None,
                lambda: state.federation_engine.execute_engine_sync(
                    plan.physical_sql, [], session_hints=plan.session_hints
                ),
            )
            self._emit_license_nag(context)  # REQ-1137: trailing-metadata nag before the row stream
            _proto_by_norm = {_norm(f.name): f.name for f in descriptor.fields}
            out_cols = [_proto_by_norm.get(_norm(c), c) for c in stream.column_names]
            batch_iter = stream.batches()
            # REQ-074/REQ-1386: this streaming terminal never reaches _execute_plan, so the audit
            # row is written here — after the last batch, or on the way out of a failed drain.
            from provisa.pgwire._pipeline import finalize_audit

            try:
                while True:
                    batch = await loop.run_in_executor(None, next, batch_iter, None)
                    if batch is None:
                        break
                    for row in batch:
                        yield msg_cls(**_kwargs_for(out_cols, row))
            except Exception:
                await finalize_audit(plan, 500, state)
                raise
            await finalize_audit(plan, 200, state)
            return

        # Bounded routes (DIRECT / metadata / registered-function) buffer via the materializing
        # terminal — async-native, memory bounded by the route's own contract.
        result = await _execute_plan(plan, state)
        self._emit_license_nag(context)  # REQ-1137: trailing-metadata nag before the row stream
        # Stream rows as proto messages, mapping result column names to proto fields by the same key
        # (governance may re-case or alias a column).
        _proto_by_norm = {_norm(f.name): f.name for f in descriptor.fields}
        out_cols = [_proto_by_norm.get(_norm(c), c) for c in result.column_names]
        for row in result.rows:
            yield msg_cls(**_kwargs_for(out_cols, row))

    # --- REQ-1359: aggregate / group-by protocol parity -----------------------------------------
    # gRPC synthesizes GraphQL query text (query_ir.grpc_table_to_aggregate_graphql_text /
    # grpc_table_to_group_by_graphql_text) targeting the same {field}_aggregate/{field}_group_by
    # root fields JSON:API/REST synthesize, then runs it through the identical
    # parse_query/compile_query/govern/route/execute pipeline — no third, divergent aggregate
    # implementation.

    _AGG_FUNC_SUFFIX = {
        "sum": "SumFields",
        "avg": "AvgFields",
        "stddev": "StddevFields",
        "variance": "VarianceFields",
        "min": "MinFields",
        "max": "MaxFields",
    }

    def _split_agg_columns(self, columns, row) -> tuple[dict, dict]:
        from provisa.grpc.query_ir import split_agg_columns

        return split_agg_columns(columns, row)

    def _build_aggregate_result_message(self, type_name: str, top: dict, nested: dict):
        """Construct a {Type}AggregateResult message from split (top, nested) aggregate values,
        reusing the same nested-message shape proto_gen emitted (REQ-1359)."""
        agg_cls = getattr(self._pb2, f"{type_name}AggregateResult")
        kwargs = dict(top)
        for func_name, sub_kwargs in nested.items():
            suffix = self._AGG_FUNC_SUFFIX.get(func_name)
            if suffix is None:
                continue
            sub_cls = getattr(self._pb2, f"{type_name}{suffix}", None)
            if sub_cls is None:
                continue
            kwargs[func_name] = sub_cls(**sub_kwargs)
        return agg_cls(**kwargs)

    async def _handle_query_aggregate(self, request, context, type_name: str):
        """Query{Type}Aggregate (REQ-1359, unary): resolve+bind org, then run the bound aggregate
        query and map its single result row into the generated {Type}AggregateResult message."""
        metadata = dict(context.invocation_metadata())
        role_id = _rpc_role(metadata)
        if not role_id:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "Missing x-provisa-role metadata")
            return None
        try:
            _org_token = await self._bind_org(metadata)
        except ValueError as exc:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, str(exc))
            return None
        try:
            return await self._handle_query_aggregate_bound(request, context, type_name, role_id)
        finally:
            if _org_token is not None:
                from provisa.api.org_runtime import reset_current_org

                reset_current_org(_org_token)

    async def _handle_query_aggregate_bound(self, request, context, type_name: str, role_id: str):
        from provisa.compiler.parser import GraphQLValidationError, parse_query
        from provisa.compiler.sql_gen import compile_query
        from provisa.grpc.query_ir import grpc_table_to_aggregate_graphql_text
        from provisa.pgwire._pipeline import _execute_plan, _govern_and_route_compiled

        state = self._state
        if role_id not in state.contexts or role_id not in state.schemas:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"No schema for role {role_id!r}")
            return None
        ctx = state.contexts[role_id]
        schema = state.schemas[role_id]

        msg_cls = getattr(self._pb2, f"{type_name}AggregateResult", None)
        if msg_cls is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"No table for type {type_name!r}")
            return None

        funcs = list(getattr(request, "funcs", [])) or None
        gql_text = grpc_table_to_aggregate_graphql_text(ctx, type_name, funcs)
        if gql_text is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"No table for type {type_name!r}")
            return None

        try:
            document = parse_query(schema, gql_text)
        except GraphQLValidationError as exc:
            await context.abort(grpc.StatusCode.NOT_FOUND, str(exc))
            return None
        compiled_queries = compile_query(document, ctx)
        if not compiled_queries:
            await context.abort(grpc.StatusCode.INTERNAL, "Aggregate compilation failed")
            return None
        compiled = compiled_queries[0]

        try:
            plan = await _govern_and_route_compiled(
                compiled.sql, role_id, exec_params=compiled.params or None, state=state
            )
        except PermissionError as exc:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, str(exc))
            return None
        result = await _execute_plan(plan, state)
        self._emit_license_nag(context)  # REQ-1137
        row = result.rows[0] if result.rows else ()
        top, nested = self._split_agg_columns(compiled.columns, row)
        return self._build_aggregate_result_message(type_name, top, nested)

    async def _handle_query_group_by(self, request, context, type_name: str):
        """Query{Type}GroupBy (REQ-1359, streaming): resolve+bind org, then stream the bound
        group-by query's rows as {Type}GroupByRow messages, mirroring _handle_query's org lifecycle."""
        metadata = dict(context.invocation_metadata())
        role_id = _rpc_role(metadata)
        if not role_id:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "Missing x-provisa-role metadata")
            return
        try:
            _org_token = await self._bind_org(metadata)
        except ValueError as exc:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, str(exc))
            return
        try:
            async for _m in self._handle_query_group_by_bound(request, context, type_name, role_id):
                yield _m
        finally:
            if _org_token is not None:
                from provisa.api.org_runtime import reset_current_org

                reset_current_org(_org_token)

    async def _handle_query_group_by_bound(self, request, context, type_name: str, role_id: str):
        import json

        from provisa.compiler.parser import GraphQLValidationError, parse_query
        from provisa.compiler.sql_gen import compile_query
        from provisa.grpc.query_ir import grpc_table_to_group_by_graphql_text
        from provisa.pgwire._pipeline import _execute_plan, _govern_and_route_compiled

        state = self._state
        if role_id not in state.contexts or role_id not in state.schemas:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"No schema for role {role_id!r}")
            return
        ctx = state.contexts[role_id]
        schema = state.schemas[role_id]

        row_cls = getattr(self._pb2, f"{type_name}GroupByRow", None)
        if row_cls is None or getattr(self._pb2, f"{type_name}AggregateResult", None) is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"No table for type {type_name!r}")
            return

        by_columns = list(request.by)
        include_nodes = bool(getattr(request, "include_nodes", False))
        include = list(getattr(request, "include", []))
        funcs = list(getattr(request, "funcs", [])) or None
        gql_text = grpc_table_to_group_by_graphql_text(
            ctx, type_name, by_columns, funcs=funcs, include_nodes=include_nodes, include=include
        )
        if gql_text is None:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"No table for type {type_name!r} or empty 'by'"
                if by_columns
                else "GroupBy requires at least one 'by' column",
            )
            return

        try:
            document = parse_query(schema, gql_text)
        except GraphQLValidationError as exc:
            await context.abort(grpc.StatusCode.NOT_FOUND, str(exc))
            return
        compiled_queries = compile_query(document, ctx)
        if not compiled_queries:
            await context.abort(grpc.StatusCode.INTERNAL, "Group-by compilation failed")
            return
        compiled = compiled_queries[0]

        try:
            plan = await _govern_and_route_compiled(
                compiled.sql, role_id, exec_params=compiled.params or None, state=state
            )
        except PermissionError as exc:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, str(exc))
            return
        result = await _execute_plan(plan, state)
        self._emit_license_nag(context)  # REQ-1137

        from provisa.executor.serialize import _convert_value
        from provisa.grpc.query_ir import split_group_by_columns

        group_key_cols, group_key_idx, agg_cols, agg_idx = split_group_by_columns(compiled.columns)

        # REQ-1405: include_nodes routes compiled.nodes_sql through the identical
        # govern → route → execute pipeline the primary query just used above — the same one
        # pipeline every surface shares, just a second SQL string GraphQL's own
        # groupKey/aggregate/nodes shape requires (JSON:API/REST take this same seam via
        # _exec_nodes_query; gRPC just has to type it instead of dropping it into JSON).
        nodes_by_group_key: dict[tuple, list] = {}
        row_msg_cls = getattr(self._pb2, type_name, None)
        nodes_columns = compiled.nodes_columns
        if include_nodes and compiled.nodes_sql is not None and nodes_columns is not None and row_msg_cls is not None:
            nodes_plan = await _govern_and_route_compiled(
                compiled.nodes_sql, role_id, exec_params=compiled.nodes_params or None, state=state
            )
            nodes_result = await _execute_plan(nodes_plan, state)
            join_key_idx = [i for i, c in enumerate(nodes_columns) if c.nested_in == "__join_key__"]
            output_cols = [(i, c) for i, c in enumerate(nodes_columns) if c.nested_in is None]
            for node_row in nodes_result.rows:
                join_key = tuple(_convert_value(node_row[i]) for i in join_key_idx)
                node_dict = {c.field_name: _convert_value(node_row[i]) for i, c in output_cols}
                nodes_by_group_key.setdefault(join_key, []).append(
                    self._dict_row_to_message(row_msg_cls, node_dict)
                )

        for row in result.rows:
            group_key = {c.column: row[i] for c, i in zip(group_key_cols, group_key_idx)}
            agg_row = [row[i] for i in agg_idx]
            top, nested = self._split_agg_columns(agg_cols, agg_row)
            agg_msg = self._build_aggregate_result_message(type_name, top, nested)
            join_key = tuple(_convert_value(row[i]) for i in group_key_idx)
            yield row_cls(
                group_key=json.dumps(group_key, default=str),
                aggregate=agg_msg,
                nodes=nodes_by_group_key.get(join_key, []),
            )

    def _dict_row_to_message(self, msg_cls, d: dict):
        """Build a ``{Type}`` proto message from a serialize_group_by-style nested dict row
        (REQ-1405) — the same nested-dict shape ``nodes_sql`` produces for every other transport,
        just typed instead of left as JSON. Relation columns (``_build_rel_json_expr``) surface as
        a dict (many-to-one) or list-of-dicts (one-to-many) value keyed by the relation field name;
        every other value is a scalar assigned via the shared ``_proto_value`` adapter."""
        descriptor = msg_cls.DESCRIPTOR
        kwargs = {}
        nested: dict = {}
        for key, val in d.items():
            if val is None:
                continue
            field = descriptor.fields_by_name.get(key)
            if field is None:
                continue
            if field.message_type is not None and field.message_type.full_name != "google.protobuf.Timestamp":
                nested[key] = val
            else:
                kwargs[key] = _proto_value(field, val)
        msg = msg_cls(**kwargs)
        for key, val in nested.items():
            field = descriptor.fields_by_name[key]
            sub_cls = getattr(self._pb2, field.message_type.name, None)
            if sub_cls is None:
                continue
            if field.label == field.LABEL_REPEATED:
                items = val if isinstance(val, list) else [val]
                getattr(msg, key).extend(
                    self._dict_row_to_message(sub_cls, item) for item in items if isinstance(item, dict)
                )
            elif isinstance(val, dict):
                getattr(msg, key).CopyFrom(self._dict_row_to_message(sub_cls, val))
        return msg


async def start_grpc_server(
    port: int,
    state,
    pb2_path: str,
    pb2_grpc_path: str,
    tls: tuple[str, str] | None = None,
) -> grpc.aio.Server:  # REQ-045, REQ-143, REQ-1226
    """Start a gRPC async server with the Provisa service.

    Args:
        port: Port to listen on.
        state: AppState with schemas, contexts, etc.
        pb2_path: Path to generated _pb2.py module.
        pb2_grpc_path: Path to generated _pb2_grpc.py module.
        tls: Optional ``(cert_path, key_path)``. When set the server binds a TLS
            secure port (REQ-1226); otherwise it binds an insecure port.

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

    # Register handlers dynamically from the generated stub. REQ-273/REQ-1263: the auth
    # interceptor sits in front of every service on this server — the generated one and
    # reflection — so no RPC reaches a handler without a validated credential.
    from provisa.grpc.auth import AuthInterceptor

    server = grpc.aio.server(interceptors=[AuthInterceptor(state)])

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

    if tls is not None:
        # REQ-1228: client-certificate verification, when the deployment configures a CA. grpc
        # expects (private_key, certificate_chain) pairs.
        from provisa.security.mtls import grpc_server_credentials, resolve_client_auth

        _cert_path, _key_path = tls
        with open(_cert_path, "rb") as _cf, open(_key_path, "rb") as _kf:
            _creds = grpc_server_credentials(
                _cf.read(),
                _kf.read(),
                resolve_client_auth(
                    "PROVISA_GRPC_CLIENT_CA",
                    "PROVISA_GRPC_MTLS_MODE",
                    "PROVISA_GRPC_MTLS_BIND_PRINCIPAL",
                ),
            )
        server.add_secure_port(f"[::]:{port}", _creds)
    else:
        server.add_insecure_port(f"[::]:{port}")
    await server.start()
    log.info("gRPC server started on port %d (TLS=%s)", port, tls is not None)
    return server
