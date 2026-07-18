# Copyright (c) 2026 Kenneth Stott
# Canary: 7c2f1a94-6d38-4b0e-9a51-3e8c2d7f0b46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Extensible-function dispatcher (REQ-885) + non-bypassable invocation tracing (REQ-886).

Every registered function carries an ``impl_kind`` (source_procedure | script | http |
grpc | python). Addressing (the catalog ``name``/``function_name``) is decoupled from the
``binding`` (transport + location, swappable). :func:`dispatch_function` validates the
per-kind binding, prepares relation arguments (table_ref lazy, result_set eager-materialized,
column_value scalar), mints a scoped pgwire session, and routes to the matching executor —
all wrapped in a trace the dispatcher itself emits (REQ-886), so no kind can bypass it.

Transport seams (``_run_subprocess`` / ``_http_call`` / ``_grpc_call``) are module-level so
the boundary is mockable without external services.
"""

# Requirements: REQ-885, REQ-886

from __future__ import annotations

import importlib
import json
from typing import TYPE_CHECKING

from fastapi import HTTPException

from provisa.otel_compat import (
    TRANSPORT_BY_KIND,
    MintedSession,
    mint_udf_session,
    new_correlation_id,
    udf_invocation_trace,
)

if TYPE_CHECKING:
    from provisa.otel_compat import UdfTrace, UdfTraceSink

_HTTP_DEFAULT_TIMEOUT_S = 30.0
# A result_set relation ref is fully qualified as source.schema.table.
_QUALIFIED_REF_PARTS = 3


# --------------------------------------------------------------------------- #
# Transport seams — mockable boundary, no external service required in tests.  #
# --------------------------------------------------------------------------- #


async def _run_subprocess(argv: list[str], payload: bytes) -> bytes:
    """Run a local script (script kind), feeding *payload* on stdin, return stdout."""
    import asyncio

    proc = await asyncio.create_subprocess_exec(
        *argv,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate(payload)
    if proc.returncode != 0:
        raise HTTPException(
            status_code=502,
            detail=f"script exited {proc.returncode}: {err.decode(errors='replace')}",
        )
    return out


async def _http_call(
    method: str, url: str, payload: dict, timeout: float
) -> object:  # object-ok: decoded external JSON is truly-any
    """Invoke an http-kind function endpoint, return the decoded JSON body."""
    import httpx

    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.request(method, url, json=payload)
    resp.raise_for_status()
    return resp.json()


_GRPC_DEFAULT_TIMEOUT_S = 30.0


def _grpc_method_path(method: str) -> str:
    """Normalize a binding ``method`` into a gRPC full method path ``/Service/Method``.

    Accepts an already-slashed path (``/Pkg.Svc/Fn``), a ``Service/Method`` form, or the
    dotted ``Package.Service.Method`` form — the last dot separates method from service.
    """
    if method.startswith("/"):
        return method
    if "/" in method:
        return "/" + method.lstrip("/")
    if "." not in method:
        raise HTTPException(
            status_code=400,
            detail=f"grpc method {method!r} must be 'Service/Method' or 'pkg.Service.Method'",
        )
    service, _, name = method.rpartition(".")
    return f"/{service}/{name}"


def _grpc_identity(b: bytes) -> bytes:  # proto-less: request/response bodies are raw JSON bytes
    return b


async def _grpc_call(
    target: str,
    method: str,
    payload: dict,
    *,
    timeout: float = _GRPC_DEFAULT_TIMEOUT_S,
    tls: bool = False,
) -> object:  # object-ok: decoded external response is truly-any
    """Invoke a grpc-kind function via the generic JSON unary bridge, return the response.

    The bridge is proto-less: the JSON payload is sent as the raw unary request body and the
    unary response body is decoded as JSON. The remote service must expose a method whose
    request/response are opaque bytes carrying JSON (the Provisa hosted-function contract).
    """
    import grpc

    path = _grpc_method_path(method)
    request = json.dumps(payload).encode()
    channel = (
        grpc.aio.secure_channel(target, grpc.ssl_channel_credentials())
        if tls
        else grpc.aio.insecure_channel(target)
    )
    try:
        rpc = channel.unary_unary(
            path, request_serializer=_grpc_identity, response_deserializer=_grpc_identity
        )
        response = await rpc(request, timeout=timeout)
    except grpc.aio.AioRpcError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"grpc call {target}{path} failed: {exc.code().name}: {exc.details()}",
        ) from exc
    finally:
        await channel.close()
    return json.loads(response or b"null")


# --------------------------------------------------------------------------- #
# Argument preparation — relation-argument kinds (REQ-885).                    #
# --------------------------------------------------------------------------- #


async def _materialize_relation(ref: str, state) -> dict:
    """Eagerly materialize a referenced relation to an Arrow-compatible batch (result_set)."""
    parts = ref.split(".")
    if len(parts) < _QUALIFIED_REF_PARTS:
        raise HTTPException(
            status_code=400,
            detail=f"result_set ref {ref!r} must be source.schema.table",
        )
    src_id, schema, table = parts[0], parts[1], parts[2]
    if not state.source_pools.has(src_id):
        raise HTTPException(status_code=503, detail=f"Source '{src_id}' not connected")
    sql = f'SELECT * FROM "{schema}"."{table}"'
    result = await state.source_pools.execute(src_id, sql, [])
    from provisa.executor.serialize import _convert_value

    cols = result.column_names
    rows = [{c: _convert_value(v) for c, v in zip(cols, r)} for r in result.rows]
    return {"kind": "result_set", "columns": cols, "rows": rows}


def _arg_kinds(fn: dict) -> dict[str, str]:
    return {a["name"]: a.get("arg_kind", "column_value") for a in fn.get("arguments") or []}


async def _prepare_args(fn: dict, args: dict, state) -> tuple[dict, list[str]]:
    """Prepare a hosted-function payload by relation-argument kind; collect input refs."""
    kinds = _arg_kinds(fn)
    payload: dict = {}
    input_refs: list[str] = []
    for name, value in args.items():
        kind = kinds.get(name, "column_value")
        if kind == "column_value":
            payload[name] = value
        elif kind == "table_ref":  # lazy: pass the reference, do not materialize
            payload[name] = {"kind": "table_ref", "ref": value}
            input_refs.append(str(value))
        elif kind == "result_set":  # eager: materialize referenced relation to Arrow
            payload[name] = await _materialize_relation(str(value), state)
            input_refs.append(str(value))
        else:
            raise HTTPException(status_code=400, detail=f"Unknown arg_kind {kind!r} for {name!r}")
    return payload, input_refs


# --------------------------------------------------------------------------- #
# Per-kind executors.                                                          #
# --------------------------------------------------------------------------- #


def _require(
    binding: dict, key: str, kind: str
) -> object:  # object-ok: binding values are truly-any JSON
    """Fetch a required binding key or fail loud — no silent default (REQ-885)."""
    if key not in binding or binding[key] in (None, "", []):
        raise HTTPException(
            status_code=400,
            detail=f"function binding for kind {kind!r} is missing required key {key!r}",
        )
    return binding[key]


# --------------------------------------------------------------------------- #
# Egress control (REQ-885 security_constraint) — deny-by-default allow-list.   #
# --------------------------------------------------------------------------- #

# Loopback is the Provisa pgwire boundary — implicitly reachable, never external egress.
_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", "[::1]"})


def _endpoint_host(endpoint: str, kind: str) -> tuple[str, str]:
    """Return (host, host:port) for an http url or a grpc ``host:port`` target."""
    if kind == "http":
        from urllib.parse import urlparse

        parsed = urlparse(endpoint)
        host = parsed.hostname or ""
        hostport = f"{host}:{parsed.port}" if parsed.port else host
        return host, hostport
    host = endpoint.rsplit(":", 1)[0] if ":" in endpoint else endpoint
    return host, endpoint


def _check_egress(state, endpoint: str, kind: str) -> None:
    """Refuse an external call whose host is not on the deny-by-default egress allow-list.

    The allow-list (``state.udf_egress_allowlist``) enumerates the http/grpc endpoints a
    deployment permits hosted functions to reach; loopback (Provisa pgwire) is always
    allowed. An empty/absent list denies all external egress — no silent default (REQ-885).
    """
    host, hostport = _endpoint_host(endpoint, kind)
    if host in _LOOPBACK_HOSTS:
        return
    allowlist = getattr(state, "udf_egress_allowlist", None) or []
    if host in allowlist or hostport in allowlist:
        return
    raise HTTPException(
        status_code=403,
        detail=(
            f"egress to {endpoint!r} is denied: host not on the UDF egress allow-list "
            f"(REQ-885 deny-by-default). Add it to udf_egress_allowlist to permit."
        ),
    )


# --------------------------------------------------------------------------- #
# Row-wise-external refusal (REQ-885 performance_constraint).                  #
# --------------------------------------------------------------------------- #

_EXTERNAL_KINDS = frozenset({"http", "grpc"})
_RELATION_ARG_KINDS = frozenset({"table_ref", "result_set"})


def _reject_rowwise_external(fn: dict) -> None:
    """Refuse an external function whose declared args are all scalar (row-wise).

    Expensive/external work must be set-wise and batched: an http/grpc function that takes
    only ``column_value`` scalars would be invoked once per row, killing pushdown. It must
    declare at least one relation argument (``table_ref``/``result_set``) so the service
    receives a relation and batches outbound calls itself (REQ-885).
    """
    if fn.get("impl_kind") not in _EXTERNAL_KINDS:
        return
    declared = fn.get("arguments") or []
    if not declared:
        return
    if any(a.get("arg_kind", "column_value") in _RELATION_ARG_KINDS for a in declared):
        return
    raise HTTPException(
        status_code=400,
        detail=(
            f"function {fn.get('name', '')!r} (kind={fn.get('impl_kind')}) is row-wise external: "
            "all arguments are scalar column_value. Declare a table_ref/result_set relation "
            "argument so calls are set-wise and batched (REQ-885)."
        ),
    )


async def _exec_source_procedure(fn: dict, args: dict, state, _payload, _session) -> list[dict]:
    src_id = fn["source_id"]
    if not state.source_pools.has(src_id):
        raise HTTPException(status_code=503, detail=f"Source '{src_id}' not connected")
    params = list(args.values())
    placeholders = ", ".join(f"${i + 1}" for i in range(len(params)))
    sql = f'SELECT * FROM "{fn["schema_name"]}"."{fn["function_name"]}"({placeholders})'
    result = await state.source_pools.execute(src_id, sql, params)
    from provisa.executor.serialize import _convert_value

    cols = result.column_names
    return [{c: _convert_value(v) for c, v in zip(cols, r)} for r in result.rows]


async def _exec_script(
    fn: dict, _args, _state, payload: dict, session: MintedSession
) -> list[dict]:
    argv = list(_require(fn["binding"], "argv", "script"))  # type: ignore[arg-type]
    body = json.dumps({"args": payload, "correlation_id": session.correlation_id}).encode()
    out = await _run_subprocess(argv, body)
    return _rows_from_response(json.loads(out or b"[]"))


async def _exec_http(fn: dict, _args, state, payload: dict, session: MintedSession) -> list[dict]:
    binding = fn["binding"]
    url = str(_require(binding, "url", "http"))
    _check_egress(state, url, "http")
    method = str(binding.get("method", "POST")).upper()
    timeout = float(binding.get("timeout_s", _HTTP_DEFAULT_TIMEOUT_S))
    body = {"args": payload, "correlation_id": session.correlation_id}
    return _rows_from_response(await _http_call(method, url, body, timeout))


async def _exec_grpc(fn: dict, _args, state, payload: dict, session: MintedSession) -> list[dict]:
    binding = fn["binding"]
    target = str(_require(binding, "target", "grpc"))
    method = str(_require(binding, "method", "grpc"))
    _check_egress(state, target, "grpc")
    timeout = float(binding.get("timeout_s", _GRPC_DEFAULT_TIMEOUT_S))
    tls = bool(binding.get("tls", False))
    body = {"args": payload, "correlation_id": session.correlation_id}
    return _rows_from_response(await _grpc_call(target, method, body, timeout=timeout, tls=tls))


async def _exec_python(
    fn: dict, _args, _state, payload: dict, session: MintedSession
) -> list[dict]:
    spec = str(_require(fn["binding"], "callable", "python"))
    if ":" not in spec:
        raise HTTPException(
            status_code=400, detail=f"python callable {spec!r} must be 'module:attr'"
        )
    mod_name, attr = spec.split(":", 1)
    fn_obj = getattr(importlib.import_module(mod_name), attr)
    result = fn_obj(payload, session)
    if hasattr(result, "__await__"):
        result = await result
    return _rows_from_response(result)


def _rows_from_response(
    body: object,  # object-ok: normalizes truly-any transport payload
) -> list[dict]:
    """Normalize an executor response to a list of row dicts."""
    if isinstance(body, list):
        return list(body)
    if isinstance(body, dict):
        return [body]
    raise HTTPException(
        status_code=502, detail=f"function returned non-tabular payload: {type(body)!r}"
    )


_EXECUTORS = {
    "source_procedure": _exec_source_procedure,
    "script": _exec_script,
    "http": _exec_http,
    "grpc": _exec_grpc,
    "python": _exec_python,
}


# --------------------------------------------------------------------------- #
# Dispatcher — the single non-bypassable entry (REQ-885 + REQ-886).           #
# --------------------------------------------------------------------------- #


def _output_bytes(rows: list[dict]) -> int:
    try:
        return len(json.dumps(rows, default=str).encode())
    except (TypeError, ValueError):
        return 0


async def dispatch_function(  # REQ-885, REQ-886
    fn: dict,
    args: dict,
    state,
    role_id: str | None,
    *,
    correlation_id: str | None = None,
    trace_sink: "UdfTraceSink | None" = None,
) -> list[dict]:
    """Route one function invocation to its per-kind executor, always emitting a trace.

    The dispatcher — never the function — emits the invocation trace (REQ-886): unknown
    kinds and missing bindings fail loud before any transport is touched, and the trace is
    recorded even on failure. A scoped session carrying the correlation id is minted and
    passed to the executor so any pgwire callback's audit rows join to this invocation.
    """
    impl_kind = fn.get("impl_kind", "source_procedure")
    executor = _EXECUTORS.get(impl_kind)
    if executor is None:
        raise HTTPException(status_code=400, detail=f"Unknown function impl_kind: {impl_kind!r}")
    transport = TRANSPORT_BY_KIND[impl_kind]
    identity = "definer" if fn.get("materialize") else "invoker"
    _reject_rowwise_external(fn)

    if impl_kind == "source_procedure":
        payload: dict = args
        input_refs: list[str] = []
    else:
        payload, input_refs = await _prepare_args(fn, args, state)

    corr = correlation_id or new_correlation_id()
    session = mint_udf_session(corr, identity, role_id)
    _stamp_session(state, session)

    with udf_invocation_trace(
        udf_name=fn.get("name", fn.get("function_name", "")),
        transport=transport,
        identity=identity,
        input_refs=input_refs,
        role_id=role_id,
        correlation_id=corr,
        sink=trace_sink or getattr(state, "udf_trace_sink", None),
    ) as trace:
        rows = await executor(fn, args, state, payload, session)
        _fill_trace_output(trace, rows)
        return rows


def _fill_trace_output(trace: "UdfTrace", rows: list[dict]) -> None:
    trace.output_cardinality = len(rows)
    trace.output_bytes = _output_bytes(rows)


def _stamp_session(state, session: MintedSession) -> None:
    """Record the minted session on state so a pgwire callback can adopt its correlation id."""
    sink = getattr(state, "minted_sessions", None)
    if sink is not None:
        sink.append(session)
