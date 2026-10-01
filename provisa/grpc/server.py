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

REQ-1882 (amended 2026-09-29): the synchronous ``grpc.server`` serves each RPC on its own thread
from its pool, and the whole RPC runs on that thread — the handler coroutines below run on the
RPC's own :class:`provisa.core.connection_loop.ConnectionLoop` via
:func:`provisa.grpc.rpc_scope.rpc_scope`, never on the process loop. Two RPCs govern and execute in
parallel.
"""

# Requirements: REQ-045, REQ-051, REQ-143, REQ-145, REQ-266, REQ-1882

from __future__ import annotations

import importlib.util
import logging
import re
import sys
from datetime import date, datetime, timedelta

import concurrent.futures

import grpc
from google.protobuf.descriptor import FieldDescriptor

from provisa.compiler.directives import cache_hint_from_grpc_metadata
from provisa.core.ir_types import iso8601_duration

log = logging.getLogger(__name__)

# REQ-1899: rows per {Type}Batch message for the Query{Type}Batch RPC. Sized to stay comfortably
# under gRPC's default 4MB max message size across arbitrary table widths without needing a
# matching client/server max-message-length bump: live-measured payload for a 17-column table
# averaged ~353 bytes/row (_payload_bytes' json.dumps estimate, run_benchmark.py) — protobuf is
# typically denser than JSON (varint encoding, no repeated field names), so this is a conservative
# upper bound. 5,000 rows/batch is ~1.8MB at that estimate, well under 4MB even for a wider table.
_GRPC_BATCH_ROWS = 5_000


# REQ-1899: server-wide default for grpc.max_{send,receive}_message_length — configurable via
# GRPC_MAX_MESSAGE_BYTES env var / server_cfg["grpc_max_message_bytes"] (start_grpc_server), NOT
# hardcoded, since this is a channel-wide setting (gRPC has no per-RPC-call equivalent) and
# different deployments may want a tighter or looser ceiling without a code change. 32MB comfortably
# covers a batch_rows=65,536 caller (matching _STREAM_BATCH_ROWS' granularity) even for a wider
# table than order_items' measured ~353 bytes/row.
def _max_message_bytes() -> int:
    """REQ-1913: an operator setting; its default is declared in provisa/core/settings_catalog.py."""
    from provisa.core import settings_registry

    return settings_registry.value("grpc.max_message_bytes")


def _allow_unsecured_reflection() -> bool:
    """REQ-1904: the explicit opt-in to reflection on a deployment with no auth (REQ-1913: an
    operator setting)."""
    from provisa.core import settings_registry

    return settings_registry.value("grpc.allow_unsecured_reflection")


def _max_concurrent_rpcs() -> int:
    """REQ-1904: the server-wide ceiling on in-flight RPCs, per worker process (REQ-1913: an
    operator setting)."""
    from provisa.core import settings_registry

    return settings_registry.value("concurrency.grpc_max_concurrent_rpcs")


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

    REQ-1884: date/datetime and FieldDescriptor are module-level imports, not per-call — this
    function runs once per (row, column) on the ENGINE-route streaming path (millions of times
    for a large scan), and a per-call ``import`` statement still pays a sys.modules lookup +
    attribute bind every time even when the module is already loaded.
    """
    if field.type == FieldDescriptor.TYPE_MESSAGE:
        if field.message_type.full_name == "google.protobuf.Timestamp":
            ts = field.message_type._concrete_class()
            ts.FromDatetime(value if isinstance(value, datetime) else datetime.fromisoformat(value))
            return ts
        return value
    if field.type == FieldDescriptor.TYPE_STRING:
        if isinstance(value, timedelta):
            return iso8601_duration(value)  # an interval's canonical text form, as on every surface
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


def _status_for_exception(exc: BaseException) -> grpc.StatusCode:
    """Map a mid-stream execution failure to a gRPC status code (REQ-1904).

    These catch sites used to bare-``raise`` after finalizing the audit row, which surfaces as an
    opaque ``UNKNOWN`` on the wire — the same information the validation paths elsewhere in this
    file convey via ``context.abort(StatusCode.X, ...)``. Only exception TYPES that unambiguously
    indicate a more specific condition than "the server failed" get a non-INTERNAL code; everything
    else is INTERNAL, matching this repo's fail-closed convention (never guess a more lenient code
    than the evidence supports)."""
    if isinstance(exc, TimeoutError):
        return grpc.StatusCode.DEADLINE_EXCEEDED
    if isinstance(exc, PermissionError):
        return grpc.StatusCode.PERMISSION_DENIED
    if isinstance(exc, (ConnectionError, OSError)):
        return grpc.StatusCode.UNAVAILABLE
    if isinstance(exc, ValueError):
        return grpc.StatusCode.INVALID_ARGUMENT
    return grpc.StatusCode.INTERNAL


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


class _RpcContext:
    """The handler coroutines' view of the synchronous ``grpc.ServicerContext``.

    The bodies below ``await context.abort(...)``; the synchronous server's ``abort`` raises at
    once, ending the RPC with that status. Everything else is the context itself."""

    def __init__(self, context: grpc.ServicerContext) -> None:
        self._context = context

    async def abort(self, code: grpc.StatusCode, details: str) -> None:
        self._context.abort(code, details)

    def __getattr__(self, name: str):
        return getattr(self._context, name)


def _unary(body):
    """A unary handler: ``body(request, context)`` runs to completion on the RPC's own loop."""
    from provisa.grpc.rpc_scope import rpc_scope

    def handler(request, context):
        with rpc_scope() as rpc:
            return rpc.run(body(request, _RpcContext(context)))

    return handler


def _streaming(body):
    """A response-streaming handler: the async generator ``body(request, context)`` is advanced
    one message at a time on the RPC's own loop, on the RPC's thread."""
    from provisa.grpc.rpc_scope import rpc_scope

    def handler(request, context):
        with rpc_scope() as rpc:
            yield from rpc.iterate(body(request, _RpcContext(context)))

    return handler


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
        deployments (ContextVar left unset → default runtime). Each RPC runs in its own context
        (``provisa.grpc.rpc_scope``), so a plain set/reset isolates the binding."""
        if not getattr(self._state, "multitenancy", False):
            return None
        raw = metadata.get("x-provisa-org")
        requested = raw.decode() if isinstance(raw, bytes) else raw
        org_id = await self._resolve_org(requested)
        if not org_id:
            raise ValueError("Missing x-provisa-org metadata")
        from provisa.api.app import ensure_org_runtime
        from provisa.core.request_context import set_current_org

        await ensure_org_runtime(org_id)
        return set_current_org(org_id)

    def __getattr__(self, name: str):
        """Dynamically resolve RPC handler methods like QueryOrders, InsertOrders.

        Each returned handler is synchronous (the server calls it on the RPC's pool thread) and runs
        its coroutine body on the RPC's own loop (REQ-1882)."""
        # REQ-1359: Query{Type}Aggregate / Query{Type}GroupBy must resolve BEFORE the generic
        # Query{Type} branch below — both prefixes also start with "Query".
        if name.startswith("Query") and name.endswith("Aggregate"):
            type_name = name[len("Query") : -len("Aggregate")]
            return _unary(
                lambda request, context: self._handle_query_aggregate(request, context, type_name)
            )
        if name.startswith("Query") and name.endswith("GroupBy"):
            type_name = name[len("Query") : -len("GroupBy")]
            return _streaming(
                lambda request, context: self._handle_query_group_by(request, context, type_name)
            )
        # REQ-1899: Query{Type}Batch must resolve before the generic Query{Type} branch below —
        # both prefixes also start with "Query" (same ordering concern as Aggregate/GroupBy above).
        if name.startswith("Query") and name.endswith("Batch"):
            type_name = name[len("Query") : -len("Batch")]
            return _streaming(
                lambda request, context: self._handle_query_batch(request, context, type_name)
            )
        if name.startswith("Query"):
            type_name = name[len("Query") :]
            # Convert PascalCase type name to snake_case field name
            field_name = _pascal_to_snake(type_name)
            return _streaming(
                lambda request, context: self._handle_query(request, context, type_name, field_name)
            )
        if name.startswith("Insert"):
            type_name = name[len("Insert") :]
            return _unary(lambda request, context: self._handle_insert(request, context, type_name))
        if name == "CallCommand":  # REQ-1156
            return _unary(lambda request, context: self._handle_call_command(request, context))
        if name.startswith("Call"):  # REQ-1156 — per-command typed RPC Call{Cmd}
            cmd_name = self._resolve_command_rpc(name[len("Call") :])
            return _unary(
                lambda request, context: self._handle_typed_command(request, context, cmd_name)
            )
        raise AttributeError(f"{type(self).__name__!r} has no attribute {name!r}")

    def _meter_msg(self, msg):
        """Meter one outbound proto message as egress and return it (REQ-1452).

        ``ByteSize()`` is the serialized payload, not the HTTP/2 frame: an approximation missing
        framing. Must be called inside the handler's org binding.
        """
        if msg is None:
            return msg
        from provisa.core.request_context import current_org
        from provisa.core.egress import report

        report(current_org.get(), msg.ByteSize())
        return msg

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
                return self._meter_msg(self._pb2.MutationResponse(affected_rows=len(rows)))
            return self._meter_msg(
                self._pb2.CommandResponse(rows_json=json.dumps(rows, default=str))
            )
        finally:
            if _org_token is not None:
                from provisa.core.request_context import reset_current_org

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
            return self._meter_msg(
                self._pb2.CommandResponse(rows_json=json.dumps(rows, default=str))
            )
        finally:
            if _org_token is not None:
                from provisa.core.request_context import reset_current_org

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
                yield self._meter_msg(_m)
        finally:
            if _org_token is not None:
                from provisa.core.request_context import reset_current_org

                reset_current_org(_org_token)

    async def _handle_query_batch(self, request, context, type_name: str):
        """REQ-1899: batched-rows counterpart to _handle_query — streams {Type}Batch messages
        (repeated {Type} rows) instead of one {Type} message per row. Reuses _handle_query_bound
        unchanged (same governance/routing/execution, same per-row messages) and only changes how
        those messages reach the wire. Additive: the existing per-row Query{Type} RPC and its
        handler are untouched, so no existing client (internal or external) is affected at all.

        Live-measured root cause this fixes (REQ-1898's amendment): a 2,000,000-row DIRECT scan
        via the per-row RPC took ~190s — statistically unchanged whether the row fetch itself was
        buffered or streamed — because each of 2,000,000 individual gRPC stream messages pays full
        per-message framing/serialization/flow-control cost. Batching rows into fewer, larger
        messages (mirroring Flight SQL's ~31 Arrow RecordBatches for the same data) amortizes that
        cost across many rows instead of paying it per row.

        request.batch_rows (client opt-in, REQ-1899 amendment) picks the row count per batch
        message; 0/unset falls back to _GRPC_BATCH_ROWS. The server can't safely pick one large
        default that's safe for every table's width (gRPC's default 4MB max message size; `orders`
        at 26 columns is meaningfully wider than order_items' 16), but a CLIENT querying one
        specific, known table can safely choose a bigger batch when it knows that table is narrow
        — e.g. the perf benchmark opts into a larger batch_rows for order_items specifically."""
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
            batch_cls = getattr(self._pb2, f"{type_name}Batch", None)
            if batch_cls is None:
                await context.abort(
                    grpc.StatusCode.INTERNAL, f"Unknown message type {type_name}Batch"
                )
                return
            batch_rows = getattr(request, "batch_rows", 0) or _GRPC_BATCH_ROWS
            rows_buf: list = []
            async for _m in self._handle_query_bound(request, context, type_name, role_id):
                rows_buf.append(_m)
                if len(rows_buf) >= batch_rows:
                    yield self._meter_msg(batch_cls(rows=rows_buf))
                    rows_buf = []
            if rows_buf:
                yield self._meter_msg(batch_cls(rows=rows_buf))
        finally:
            if _org_token is not None:
                from provisa.core.request_context import reset_current_org

                reset_current_org(_org_token)

    async def _handle_query_bound(self, request, context, type_name: str, role_id: str):
        """The routing+execution body of _handle_query, run with the RPC's org already bound."""
        from provisa.grpc.query_ir import (
            FilterError,
            MaskTree,
            ReadMaskError,
            grpc_table_to_semantic_sql,
            resolve_read_mask,
            restrict_json,
        )
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
        filter_msg = request.filter if request.HasField("filter") else None
        # REQ-803: the request's read_mask is part of the QUERY — only the masked columns are
        # selected, so the governed pipeline and the source see exactly what the client asked for
        # (and the statement, hence the kept-plan key, differs per mask). Fields outside the mask
        # are never set and stay at their proto defaults.
        # A mask path or a filter field that is not a column this role can read is rejected by
        # name before anything is governed or run.
        try:
            read_mask = resolve_read_mask(ctx, type_name, list(request.read_mask.paths))
            semantic = grpc_table_to_semantic_sql(
                ctx, type_name, request.limit, filter_msg, read_mask
            )
        except (ReadMaskError, FilterError) as exc:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
            return
        if semantic is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"No table for type {type_name!r}")
            return
        # REQ-1877: the statement is the request's shape; the filter values and the row limit
        # travel bound, so a request that differs only in its values reuses the kept governed plan.
        semantic_sql, bound_params = semantic

        def _norm(s: str) -> str:
            return s.replace("_", "").lower()

        try:
            # REQ-544: the call's own `x-provisa-cache` / `x-provisa-cache-ttl` metadata opt-in.
            # REQ-1897: serve_cached — an opted-in request whose entry exists comes back as a
            # Route.CACHE plan read BEFORE routing, which this handler serves below.
            plan = await _govern_and_route_compiled(
                semantic_sql,
                role_id,
                exec_params=bound_params or None,
                state=state,
                cache_hint=cache_hint_from_grpc_metadata(context.invocation_metadata()),
                serve_cached=True,
            )
        except PermissionError as exc:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, str(exc))
            return

        def _col_fields_for(out_cols: list[str]) -> list[tuple[str, object, MaskTree | None]]:
            # REQ-1884: resolved once per query, not once per (row, column) — out_cols is fixed
            # for the life of the result set, so descriptor.fields_by_name.get(col) doesn't need
            # re-running on every row on the ENGINE-route path (millions of rows for a large scan).
            # REQ-803: likewise each column's read_mask JSON sub-path selection (None = whole value).
            selections: list[MaskTree | None] = (
                read_mask.restrictions(out_cols)
                if read_mask is not None
                else [None] * len(out_cols)
            )
            return [
                (col, descriptor.fields_by_name.get(col), selection)
                for col, selection in zip(out_cols, selections)
            ]

        def _kwargs_for(col_fields: list[tuple[str, object, MaskTree | None]], row) -> dict:
            kwargs = {}
            for i, (col, field, selection) in enumerate(col_fields):
                if i < len(row) and row[i] is not None:
                    value = row[i] if selection is None else restrict_json(row[i], selection)
                    # No field => let msg_cls(**kwargs) raise on the unknown name rather than
                    # dropping the column silently.
                    kwargs[col] = _proto_value(field, value) if field is not None else value
            return kwargs

        # REQ-1897: answered from the response cache before routing — nothing was lowered,
        # optimized, routed or prepared for residency, and no source is touched. cached_result
        # egress-accounts and audits the HIT.
        if plan.cache_hit is not None:
            from provisa.pgwire._pipeline import cached_result

            hit = await cached_result(plan, state)
            self._emit_license_nag(context)  # REQ-1137
            _proto_by_norm = {_norm(f.name): f.name for f in descriptor.fields}
            out_cols = [_proto_by_norm.get(_norm(c), c) for c in hit.column_names]
            col_fields = _col_fields_for(out_cols)
            for row in hit.rows:
                yield msg_cls(**_kwargs_for(col_fields, row))
            return

        # ENGINE route streams lazily — the full user result set never materializes (REQ-1215).
        # The engine's streaming terminal is synchronous and is drained right here, on the RPC's
        # own thread (REQ-1882): each batch is pulled only when the previous one's messages have
        # been handed to gRPC, so peak memory is bounded by one batch, not the whole result.
        if plan.route == Route.ENGINE:
            require_governed_plan(plan)  # REQ-1176: streaming terminal verifies the stamp too
            assert plan.physical_sql is not None
            # REQ-1661/REQ-1865: this streaming terminal never reaches _execute_plan (see the audit
            # comment below), so its own residency calls are the ONLY place a MATERIALIZED /
            # row_materialize source this plan reads gets landed before the engine executes —
            # mirrors the identical ENGINE-route bypass fixes in pgwire/server.py,
            # api/flight/server.py, api/rest/cypher_router.py. (Confirmed live 2026-09-27: without
            # ensure_rows_resident/pushdown_row_materialize, a row_materialize table reached only
            # via JOIN — no literal predicate — read its own still-empty replica through here too,
            # same class of bug cypher_cross_engine hit over the HTTP-cypher transport.)
            from provisa.federation.query_residency import (
                ensure_resident,
                ensure_rows_resident,
                pushdown_row_materialize,
            )

            await ensure_rows_resident(state, plan.pk_bounds)
            await pushdown_row_materialize(
                state, plan.physical_sql, state.federation_engine.dialect, plan.exec_params
            )
            await ensure_resident(state, plan.sources)
            # REQ-1897: this streaming terminal bypasses _execute_plan_in_org entirely. A HIT was
            # served above from the plan itself; a plan that reaches here is a MISS (or did not
            # opt in), so the engine runs and an opted-in result is written through.
            from provisa.pgwire._pipeline import finalize_audit, response_cache_tee

            stream = state.federation_engine.execute_engine_sync(
                plan.physical_sql, plan.exec_params, session_hints=plan.session_hints
            )
            # REQ-1897: write-through to the raw-SQL response cache — batches still stream as
            # they arrive; this RPC is already on its loop, so the entry is committed below,
            # after a complete drain. None when the result is not cacheable.
            tee = response_cache_tee(plan, state, run=None)
            if tee is not None:
                stream = tee.rows(stream)
            self._emit_license_nag(context)  # REQ-1137: trailing-metadata nag before the row stream
            _proto_by_norm = {_norm(f.name): f.name for f in descriptor.fields}
            out_cols = [_proto_by_norm.get(_norm(c), c) for c in stream.column_names]
            col_fields = _col_fields_for(out_cols)
            batch_iter = stream.batches()
            # REQ-074/REQ-1386: this streaming terminal never reaches _execute_plan, so the audit
            # row is written here — after the last batch, or on the way out of a failed drain.

            try:
                _delivered = 0  # query_audit_log.row_count
                while True:
                    batch = next(batch_iter, None)
                    if batch is None:
                        break
                    _delivered += len(batch)
                    for row in batch:
                        yield msg_cls(**_kwargs_for(col_fields, row))
            except Exception as exc:
                await finalize_audit(plan, 500, state)
                await context.abort(_status_for_exception(exc), str(exc))
                return
            plan.row_count = _delivered
            await finalize_audit(plan, 200, state)
            if tee is not None:
                await tee.commit()
            return

        # REQ-1891: DIRECT route (single reachable source, live pooled driver — decide_route never
        # produces DIRECT for a MATERIALIZED/row_materialize read, see router.py's has_driver/
        # VIRTUAL_SOURCES branches) skips _execute_plan's ENGINE-oriented machinery entirely,
        # mirroring Flight SQL's proven fast path (api/flight/server.py:989-1006). Engine-wake
        # already ran in _govern_and_route_compiled above (_wake_before_governing), so nothing here
        # needs it again. _handle_query_bound is a coroutine on the RPC's own loop, so the async
        # execute_native (buffered QueryResult) is the correct terminal here — NOT
        # execute_native_stream, which drives a loop from outside it by submitting to that loop and
        # blocking on the result; called from the loop's own thread it would deadlock.
        if plan.route == Route.DIRECT and state.source_pools.has(plan.source_id):
            from provisa.executor.result import QueryResult
            from provisa.pgwire._pipeline import (
                finalize_audit,
                response_cache_tee,
                store_executed_result,
            )

            # REQ-544/REQ-1897: this terminal bypasses _execute_plan too. A HIT was served above
            # from the plan itself; a plan that reaches here is a MISS (or did not opt in), and an
            # opted-in result is written through the pipeline's own write after a complete drain.
            # None when this plan's result is not cacheable (no opt-in, no store, policy TTL 0):
            # nothing is captured.
            tee = response_cache_tee(plan, state, run=None)

            # REQ-1898: execute_native (below) fully materializes every row of the DIRECT read
            # into one Python QueryResult before a single message is sent — fine for a point
            # lookup (REQ-1891's original target), catastrophic for a large scan. Live-measured on
            # the perf-bench VM: large_scan (2,000,000 rows) via grpc took 190.7s for iteration 1
            # alone, ~9.4x flight's ~20.2s for the identical query — a real scaling defect, not the
            # intended behavior. state.source_pools.open_stream/fetch (REQ-1190) is a genuinely
            # ASYNC-NATIVE primitive (plain awaits, no run_coroutine_threadsafe/executor hop needed
            # — safe to drive directly from this loop-resident generator, unlike
            # execute_native_stream, which is documented synchronous-for-a-worker-thread and would
            # deadlock here), so a source whose driver supports streaming (postgresql today) gets
            # the same bounded-batch treatment as the ENGINE route above instead of buffering the
            # whole result. A source without a streaming driver still falls through to
            # execute_native unchanged — no regression for those.
            if state.source_pools.supports_stream(plan.source_id):
                from provisa.federation.runtime_support import _STREAM_BATCH_ROWS

                ds = await state.source_pools.open_stream(
                    plan.source_id, plan.sql, plan.exec_params or []
                )
                self._emit_license_nag(context)
                _proto_by_norm = {_norm(f.name): f.name for f in descriptor.fields}
                out_cols = [_proto_by_norm.get(_norm(c), c) for c in ds.column_names]
                col_fields = _col_fields_for(out_cols)
                _delivered = 0  # query_audit_log.row_count
                # The copy an opted-in MISS writes back, held only within the cache's own row
                # bound: a result past it is still streamed, and is not cached.
                _kept: list[tuple] | None = [] if tee is not None else None
                try:
                    while True:
                        batch = await ds.fetch(_STREAM_BATCH_ROWS)
                        if not batch:
                            break
                        _delivered += len(batch)
                        if _kept is not None and tee is not None:
                            if _delivered > tee.bound:
                                _kept = None
                            else:
                                _kept.extend(batch)
                        for row in batch:
                            yield msg_cls(**_kwargs_for(col_fields, row))
                except Exception as exc:
                    await finalize_audit(plan, 500, state)
                    await context.abort(_status_for_exception(exc), str(exc))
                    return
                finally:
                    await ds.close()
                plan.row_count = _delivered
                await finalize_audit(plan, 200, state)
                if _kept is not None:
                    await store_executed_result(
                        plan,
                        state,
                        QueryResult(
                            rows=_kept,
                            column_names=list(ds.column_names),
                            column_types=ds.column_types,
                        ),
                    )
                return

            result = await state.federation_engine.execute_native(
                state.source_pools, plan.source_id, plan.sql, plan.exec_params or []
            )
            self._emit_license_nag(context)  # REQ-1137: trailing-metadata nag before the row stream
            _proto_by_norm = {_norm(f.name): f.name for f in descriptor.fields}
            out_cols = [_proto_by_norm.get(_norm(c), c) for c in result.column_names]
            col_fields = _col_fields_for(out_cols)

            try:
                for row in result.rows:
                    yield msg_cls(**_kwargs_for(col_fields, row))
            except Exception as exc:
                await finalize_audit(plan, 500, state)
                await context.abort(_status_for_exception(exc), str(exc))
                return
            plan.row_count = len(result.rows)
            await finalize_audit(plan, 200, state)
            if tee is not None:
                await store_executed_result(plan, state, result)
            return

        # Bounded routes (CACHE / API) buffer via the materializing terminal — async-native, memory
        # bounded by the route's own contract.
        result = await _execute_plan(plan, state)
        self._emit_license_nag(context)  # REQ-1137: trailing-metadata nag before the row stream
        # Stream rows as proto messages, mapping result column names to proto fields by the same key
        # (governance may re-case or alias a column).
        _proto_by_norm = {_norm(f.name): f.name for f in descriptor.fields}
        out_cols = [_proto_by_norm.get(_norm(c), c) for c in result.column_names]
        col_fields = _col_fields_for(out_cols)
        for row in result.rows:
            yield msg_cls(**_kwargs_for(col_fields, row))

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
            return self._meter_msg(
                await self._handle_query_aggregate_bound(request, context, type_name, role_id)
            )
        finally:
            if _org_token is not None:
                from provisa.core.request_context import reset_current_org

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
        columns = list(getattr(request, "columns", [])) or None  # REQ-1882
        gql_text = grpc_table_to_aggregate_graphql_text(ctx, type_name, funcs, columns)
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
                compiled.sql,
                role_id,
                exec_params=compiled.params or None,
                state=state,
                cache_hint=cache_hint_from_grpc_metadata(context.invocation_metadata()),  # REQ-544
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
                yield self._meter_msg(_m)
        finally:
            if _org_token is not None:
                from provisa.core.request_context import reset_current_org

                reset_current_org(_org_token)

    async def _handle_query_group_by_bound(self, request, context, type_name: str, role_id: str):
        import json

        from provisa.compiler.parser import GraphQLValidationError, parse_query
        from provisa.compiler.sql_gen import compile_query
        from provisa.grpc.query_ir import FilterError, grpc_table_to_group_by_graphql_text
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
        columns = list(getattr(request, "columns", [])) or None  # REQ-1882
        filter_msg = request.filter if request.HasField("filter") else None
        # A filter on a column this role cannot read is refused by name, as on Query{Type}.
        try:
            gql_text = grpc_table_to_group_by_graphql_text(
                ctx,
                type_name,
                by_columns,
                funcs=funcs,
                include_nodes=include_nodes,
                include=include,
                filter_msg=filter_msg,
                columns=columns,
            )
        except FilterError as exc:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
            return
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
                compiled.sql,
                role_id,
                exec_params=compiled.params or None,
                state=state,
                cache_hint=cache_hint_from_grpc_metadata(context.invocation_metadata()),  # REQ-544
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
        if (
            include_nodes
            and compiled.nodes_sql is not None
            and nodes_columns is not None
            and row_msg_cls is not None
        ):
            nodes_plan = await _govern_and_route_compiled(
                compiled.nodes_sql,
                role_id,
                exec_params=compiled.nodes_params or None,
                state=state,
                cache_hint=cache_hint_from_grpc_metadata(context.invocation_metadata()),  # REQ-544
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
            if (
                field.message_type is not None
                and field.message_type.full_name != "google.protobuf.Timestamp"
            ):
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
                    self._dict_row_to_message(sub_cls, item)
                    for item in items
                    if isinstance(item, dict)
                )
            elif isinstance(val, dict):
                getattr(msg, key).CopyFrom(self._dict_row_to_message(sub_cls, val))
        return msg


def start_grpc_server(
    port: int,
    state,
    pb2_path: str,
    pb2_grpc_path: str,
    tls: tuple[str, str] | None = None,
) -> grpc.Server:  # REQ-045, REQ-143, REQ-1226, REQ-1882
    """Start a thread-per-RPC gRPC server with the Provisa service (REQ-1882).

    Args:
        port: Port to listen on.
        state: AppState with schemas, contexts, etc.
        pb2_path: Path to generated _pb2.py module.
        pb2_grpc_path: Path to generated _pb2_grpc.py module.
        tls: Optional ``(cert_path, key_path)``. When set the server binds a TLS
            secure port (REQ-1226); otherwise it binds an insecure port.

    Returns:
        The started grpc.Server.
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

    # REQ-1899: default 4MB max message size caps how large a client-chosen batch_rows can safely
    # go (see _handle_query_batch/_GRPC_BATCH_ROWS docstrings) — raised here so a caller that knows
    # its table is narrow enough can opt into a genuinely large batch (e.g. matching
    # _STREAM_BATCH_ROWS' 65,536-row granularity) without the server itself capping the response.
    # This does not change safety for callers who DON'T opt into a larger batch_rows: the
    # per-message cost of the plain per-row Query{Type} RPC and the server's own conservative
    # _GRPC_BATCH_ROWS default are both far under either limit.
    #
    # This is a channel/server-wide setting — gRPC has no per-RPC-call message-size knob, unlike
    # batch_rows which genuinely is per-query. Configurable rather than hardcoded, matching the
    # grpc_port pattern above (env var overrides server_cfg, which has the default) — a deployment
    # that wants a tighter or looser ceiling doesn't need a code change to set one.
    max_message_bytes = _max_message_bytes()
    # REQ-1904: overload had no fast-fail path — no concurrency ceiling meant a saturated server
    # just queued RPCs indefinitely instead of returning RESOURCE_EXHAUSTED. REQ-369's
    # max_flight_streams is a per-ROLE limit enforced by the rate limiter (fair-share across
    # tenants); this is the server-WIDE hard ceiling gRPC itself enforces per worker process (this
    # deployment runs `--workers N`, each its own process/server — the cap is per-process, not
    # multiplied across workers). Configurable, same env-var-over-server_cfg-default pattern as
    # grpc_max_message_bytes above. Sized well above the default DB pool (pool_size=5 per worker,
    # provisa/core/database.py) since most RPCs stream rather than hold a connection for their
    # whole lifetime, but still a real ceiling rather than "unbounded".
    max_concurrent_rpcs = _max_concurrent_rpcs()
    # REQ-1882 (amended 2026-09-29): one pool thread per in-flight RPC — each RPC runs entirely on
    # its thread (provisa.grpc.rpc_scope). The pool is sized to the concurrency ceiling, so every
    # admitted RPC has a thread and the ceiling alone decides RESOURCE_EXHAUSTED.
    server = grpc.server(
        concurrent.futures.ThreadPoolExecutor(
            max_workers=max_concurrent_rpcs, thread_name_prefix="provisa-grpc"
        ),
        interceptors=[AuthInterceptor(state)],
        maximum_concurrent_rpcs=max_concurrent_rpcs,
        options=[
            ("grpc.max_send_message_length", max_message_bytes),
            ("grpc.max_receive_message_length", max_message_bytes),
            # REQ-1904: keepalive tuning. A dead/half-open TCP peer (client crash, NAT timeout,
            # network partition) otherwise holds a server-side call slot forever — these bound
            # that: the server pings an idle connection every 60s and considers it dead if no
            # response lands within 20s.
            ("grpc.keepalive_time_ms", 60_000),
            ("grpc.keepalive_timeout_ms", 20_000),
            # A client is allowed to keepalive-ping even between calls (many gRPC client libraries
            # default to this) without being penalized as abusive.
            ("grpc.keepalive_permit_without_calls", 1),
            # Floor on how often a client may ping with no data in flight — below this the server
            # responds GOAWAY("too_many_pings") instead of a pong, the standard defense against a
            # ping-flood DoS. 10s comfortably tolerates a normally-configured client (gRPC's own
            # client default keepalive is 2 hours; anything below a few seconds is not a real
            # client) while still bounding the cost of a hostile one.
            ("grpc.http2.min_ping_interval_without_data_ms", 10_000),
            ("grpc.http2.max_ping_strikes", 2),
            # REQ-1904: prep for a future L4 load balancer — no LB is confirmed in front of this
            # deployment today, but a connection that never recycles is invisible to one when it
            # does arrive (a long-lived client keeps talking to the same backend process forever,
            # defeating rebalancing/rolling deploys). 30 minutes is long enough that this never
            # matters for a normal request/response or even a large streaming scan, short enough
            # that a future LB actually gets to redistribute load periodically.
            ("grpc.max_connection_age_ms", 30 * 60 * 1000),
            ("grpc.max_connection_age_grace_ms", 30_000),
        ],
    )

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

    # REQ-1904: grpc.health.v1 HealthServicer so an orchestrator gets a real gRPC health check
    # (SERVING/NOT_SERVING per service) instead of falling back to bare TCP reachability, which
    # says nothing about whether the process can actually serve a query. Registered ahead of
    # reflection/health-check exemption in AuthInterceptor (provisa/grpc/auth.py) so a probe never
    # needs a bearer credential — matches the HTTP surface's unauthenticated /health,/live,/ready.
    from grpc_health.v1 import health, health_pb2, health_pb2_grpc

    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    service_names = [
        pb2.DESCRIPTOR.services_by_name[s].full_name for s in pb2.DESCRIPTOR.services_by_name
    ]
    for _svc_name in ("", *service_names):
        health_servicer.set(_svc_name, health_pb2.HealthCheckResponse.SERVING)

    # REQ-1904: reflection exposes the full generated schema descriptor to anyone who can reach the
    # port. AuthInterceptor already intercepts reflection calls when auth is active (REQ-273/
    # REQ-1263), but that relied SOLELY on auth being configured — enable_reflection() ran
    # unconditionally, so an unsecured/dev-parity deployment (no auth configured, common for
    # local-dev) left the whole service surface enumerable by default with no explicit decision
    # behind it. Gate it on an explicit signal instead: auth_active(state) covers the normal
    # (secured) case, and grpc_allow_unsecured_reflection is a documented, explicit opt-in for a
    # deployment that wants reflection's discovery convenience despite having no auth. Not caught:
    # auth_active raising RuntimeError (a misconfigured, supposedly-active auth middleware) is a
    # real problem that must fail the server start loudly, not be swallowed into "reflection off".
    from provisa.grpc.auth import auth_active
    from provisa.grpc.reflection import enable_reflection

    if auth_active(state) or _allow_unsecured_reflection():
        enable_reflection(server, service_names)
    else:
        log.warning(
            "gRPC reflection disabled: no auth configured and grpc_allow_unsecured_reflection is "
            "unset. Set it explicitly (provisa.yaml server.grpc_allow_unsecured_reflection or "
            "GRPC_ALLOW_UNSECURED_REFLECTION) to enable reflection on an unsecured deployment."
        )

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
    server.start()
    log.info("gRPC server started on port %d (TLS=%s)", port, tls is not None)
    return server
