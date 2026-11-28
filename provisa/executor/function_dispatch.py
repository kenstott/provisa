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
import re
from typing import TYPE_CHECKING

import grpc
from fastapi import HTTPException

from provisa.api.errors import ApiError
from provisa.core.host_code import refuse_host_code
from provisa.grpc_remote.executor import open_channel
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
# A table given as a relation argument is named schema.table, as SQL serves it (REQ-1159).
_SERVED_NAME_PARTS = 2


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
        # A tool that reports on stdout (dbt, REQ-2228) has nothing on stderr to say why.
        said = (err or out).decode(errors="replace")
        raise ApiError(
            502,
            "functions.script_failed",
            f"script exited {proc.returncode}: {said}",
            returncode=proc.returncode,
            stderr=said,
        )
    return out


async def _http_call(
    method: str,
    url: str,
    payload: dict,
    timeout: float,
    headers: dict[str, str] | None = None,
) -> object:  # object-ok: decoded external JSON is truly-any
    """Invoke an http-kind function endpoint, return the decoded JSON body."""
    import httpx

    from provisa.security import sandbox_egress

    # REQ-2006: a held org's call reaches public destinations only, each request checked.
    async with httpx.AsyncClient(
        timeout=timeout, event_hooks=sandbox_egress.endpoint_event_hooks()
    ) as client:
        resp = await client.request(method, url, json=payload, headers=headers)
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
        raise ApiError(
            400,
            "functions.grpc_method_invalid",
            f"grpc method {method!r} must be 'Service/Method' or 'pkg.Service.Method'",
            method=method,
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
    path = _grpc_method_path(method)
    request = json.dumps(payload).encode()
    from provisa.security.sandbox_egress import PrivateEndpointRefused

    # REQ-2006: a held org's command dials the address its check resolved; refused as a command.
    channel = open_channel(target, tls, refusal=PrivateEndpointRefused)
    try:
        rpc = channel.unary_unary(
            path, request_serializer=_grpc_identity, response_deserializer=_grpc_identity
        )
        response = await rpc(request, timeout=timeout)
    except grpc.aio.AioRpcError as exc:
        raise ApiError(
            502,
            "functions.grpc_call_failed",
            f"grpc call {target}{path} failed: {exc.code().name}: {exc.details()}",
            target=target,
            path=path,
            status=exc.code().name,
            details=str(exc.details()),
        ) from exc
    finally:
        await channel.close()
    return json.loads(response or b"null")


# --------------------------------------------------------------------------- #
# Argument preparation — relation-argument kinds (REQ-885).                    #
# --------------------------------------------------------------------------- #


def _served_table(fn: dict, name: str, value: object, state) -> str:  # object-ok: any JSON
    """A relation argument given as a table, named by its logical, served name: the schema-qualified
    name a statement reads it by on SQL (for example ``pet_store.orders``), as the names table
    serves it (REQ-1159, amended 2026-11-28). A physical ``source.schema.table``, or any name SQL
    serves no table by, is refused naming the form it takes. Returns the name as given."""
    from provisa.pgwire.minted import CallTokenRefused, table_key

    ref = str(value).strip()
    expected = (
        f"argument {name!r} of {fn.get('name')!r} names table {ref!r}: a table is given by its "
        "logical name as SQL serves it, schema.table (for example pet_store.orders), never by a "
        "physical source.schema.table"
    )
    try:
        parts = table_key(ref)
    except CallTokenRefused as exc:
        raise ApiError(
            400, "functions.relation_table_not_logical", expected, name=name, ref=ref
        ) from exc
    names = state.names_table
    if names is None:
        raise ApiError(
            503,
            "functions.relation_no_model",
            f"argument {name!r} of {fn.get('name')!r}: there is no model yet to name {ref!r} in",
            name=name,
            ref=ref,
        )
    served = (
        names.resolve_sql(("sql", parts[0]), parts[1]) if len(parts) == _SERVED_NAME_PARTS else None
    )
    if served is None or served.object_kind not in ("table", "view"):
        raise ApiError(400, "functions.relation_table_not_logical", expected, name=name, ref=ref)
    return ref


async def _table_relation(
    fn: dict, name: str, ref: str, columns: list[str] | None, state, role_id: str | None
) -> dict:
    """A result_set argument given as a table: the governed read of its declared columns (every
    column the role sees when it declares none) from its served name, as the calling role, so the
    role's row filters, masks and column visibility hold (REQ-1159, amended 2026-11-28). The read
    is the one a code call's table argument is read with (``minted.read_statement``)."""
    from provisa.pgwire.minted import read_statement

    return await _query_relation(fn, name, read_statement(ref, columns or []), state, role_id)


def _arg_kinds(fn: dict) -> dict[str, str]:
    return {a["name"]: a.get("arg_kind", "column_value") for a in fn.get("arguments") or []}


def _schema_from_columns(columns) -> "object | None":
    """Build a processors.contract.Schema from a declared IR-typed column list, or None if absent.

    ``columns`` is a list of {"name", "type"} dicts (REQ-1159 dataset contract); the type is an IR
    name. None/empty ⇒ no declared contract ⇒ validation skipped for that dataset."""
    if not columns:
        return None
    from provisa.processors.contract import Field, Schema

    return Schema(tuple(Field(c["name"], c.get("type", "text")) for c in columns))


def _validate_against(rows: list[dict], schema, *, where: str) -> list[dict]:
    """Validate rows against a contract Schema, surfacing a violation as a fail-loud 422 (REQ-1159).

    A contract violation is a DATA-contract error (the external service or the input relation does
    not match the declared dataset contract), never a silent coerce/drop — so it fails loud."""
    from provisa.processors.contract import SchemaViolation, validate_rows

    try:
        return list(validate_rows(rows, schema, where=where))
    except SchemaViolation as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _bind_arg_names(fn: dict, args: dict) -> dict:
    """Bind positional args (``a0``, ``a1`` …, as the SQL surfaces pass them) to the declared argument
    NAMES, so arg_kind / column-contract resolution (keyed by name) applies. A named-arg mapping (the
    GraphQL path) passes through unchanged. Without this a positional ``result_set`` ref is mistaken
    for a scalar and never materialized (REQ-1159)."""
    declared = [a["name"] for a in fn.get("arguments") or []]
    keys = list(args)
    if declared and keys and all(k == f"a{i}" for i, k in enumerate(keys)):
        return {declared[i]: v for i, v in enumerate(args.values()) if i < len(declared)}
    return args


#: A relation argument written as a query: its text begins as a query does (REQ-1159).
_QUERY_TEXT = re.compile(r"^[\s(]*(select|with|values)\b", re.IGNORECASE)


def _relation_form(value: object) -> str:  # object-ok: an argument's value is any JSON
    """How a relation argument's value gives its relation (REQ-1159, amended 2026-11-27): ``rows``
    (a JSON array of row objects, as a list or as its text), ``query`` (the text of a query), or
    ``table`` (a table reference)."""
    if isinstance(value, list):
        return "rows"
    text = str(value).strip()
    if text.startswith("["):
        return "rows"
    if _QUERY_TEXT.match(text):
        return "query"
    return "table"


def _given_rows(fn: dict, name: str, value: object) -> dict:  # object-ok: any JSON
    """A relation argument given as rows: each a JSON object, the relation's columns the names
    they carry in the order first met."""
    rows = value
    if not isinstance(rows, list):
        try:
            rows = json.loads(str(value))
        except json.JSONDecodeError as exc:
            raise ApiError(
                400,
                "functions.relation_rows_invalid",
                f"argument {name!r} of {fn.get('name')!r}: its rows are not JSON: {exc}",
                name=name,
            ) from exc
    if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
        raise ApiError(
            400,
            "functions.relation_rows_invalid",
            f"argument {name!r} of {fn.get('name')!r}: its rows must be a JSON array of objects",
            name=name,
        )
    columns: list[str] = []
    for row in rows:
        columns.extend(c for c in row if c not in columns)
    return {"kind": "result_set", "columns": columns, "rows": rows}


async def _query_relation(fn: dict, name: str, sql: str, state, role_id: str | None) -> dict:
    """A relation argument given as a query: run through the one governed pipeline as the
    caller's role, as any statement the caller writes is (REQ-1159, amended 2026-11-27).

    It is held to the role's hard row cap (REQ-1159, amended 2026-11-28): its own max_rows,
    else the default row limit unless it holds FULL_RESULTS; a read past that cap is refused by
    name, never cut."""
    from provisa.executor.serialize import result_records
    from provisa.pgwire._pipeline import _execute_plan, _govern_and_route

    if role_id is None:
        raise ApiError(
            400,
            "functions.relation_query_no_role",
            f"argument {name!r} of {fn.get('name')!r} is read as the calling role; none is bound",
            name=name,
        )
    plan = await _govern_and_route(sql, role_id, relation_argument=True)
    result = await _execute_plan(plan, state)
    if plan.limit_outcome is not None:
        _refuse_over_row_cap(fn, name, plan, state, role_id)
    return {
        "kind": "result_set",
        "columns": list(result.column_names),
        "rows": result_records(result),
    }


def _refuse_over_row_cap(fn: dict, name: str, plan, state, role_id: str) -> None:
    """A relation argument's read that the hard row cap bounding it cut (``cut``), or filled
    with whether more rows match unknown (``unchecked``), is refused naming the argument, the
    cap, whose cap it is (``role``: the role's max_rows; ``default``: the configured default
    row limit, the role setting none; ``table``: a table's), and the role: the command is never
    handed a truncated relation (REQ-1159)."""
    from provisa.compiler.row_limit import DEFAULT, ROLE, TABLE
    from provisa.compiler.stage2 import hard_row_cap
    from provisa.security.rights import effective_domain_access_role

    cap = plan.row_limit.limit
    if plan.row_limit.kind == TABLE:
        cap_kind = TABLE
    else:
        held = hard_row_cap(effective_domain_access_role(role_id, state.roles))
        if held is None:
            # The role's ceiling bounded this read, so governance gave the role a hard row cap.
            raise RuntimeError(f"role {role_id!r} has no hard row cap, yet one bounded {name!r}")
        cap_kind = held.kind
    whose = {
        TABLE: f"the row cap of a table role {role_id!r} reads",
        ROLE: f"the row cap of role {role_id!r}",
        DEFAULT: f"the default row limit, role {role_id!r} setting no row cap of its own",
    }[cap_kind]
    found = (
        "more rows match than that"
        if plan.limit_outcome == "cut"
        else "it fills that cap, and whether more rows match could not be checked"
    )
    raise ApiError(
        400,
        "functions.relation_over_row_cap",
        f"argument {name!r} of {fn.get('name')!r} is read whole, and its read is held to {cap} "
        f"rows, {whose}: {found}. Narrow the relation or raise the cap.",
        name=name,
        cap=cap,
        cap_kind=cap_kind,
        role=role_id,
        outcome=plan.limit_outcome,
    )


async def _prepare_args(fn: dict, args: dict, state, role_id: str | None) -> tuple[dict, list[str]]:
    """Prepare a hosted-function payload by relation-argument kind; collect input refs."""
    args = _bind_arg_names(fn, args)
    kinds = _arg_kinds(fn)
    columns_by_arg = {a["name"]: a.get("columns") for a in fn.get("arguments") or []}
    payload: dict = {}
    input_refs: list[str] = []
    for name, value in args.items():
        kind = kinds.get(name, "column_value")
        if kind == "column_value":
            payload[name] = value
        elif kind == "table_ref":  # lazy: pass the reference, do not materialize
            # A table_ref names a table its service reads; a query or rows names none.
            if _relation_form(value) != "table":
                raise ApiError(
                    400,
                    "functions.table_ref_not_a_table",
                    f"argument {name!r} of {fn.get('name')!r} is a table_ref: it takes a table "
                    "reference, not a query or rows",
                    name=name,
                )
            # REQ-1159 (amended 2026-11-28): named by its served name; its service reads it as the
            # caller (a code source over the minted-token listener, codesource/table_reads.py).
            value = _served_table(fn, name, value, state)
            # REQ-1159: table_ref is not materialized here, so its rows cannot be validated at prepare
            # time — the service fetches them. The declared columns still travel as the contract the
            # service must honour; input-side enforcement happens only for the eager result_set kind.
            payload[name] = {"kind": "table_ref", "ref": value}
            declared = columns_by_arg.get(name)
            if declared:
                payload[name]["columns"] = [c["name"] for c in declared]
            input_refs.append(str(value))
        elif kind == "result_set":  # eager: materialize referenced relation to Arrow
            declared = columns_by_arg.get(name)
            col_names = [c["name"] for c in declared] if declared else None
            # REQ-1159 (amended 2026-11-27): given as a table, a query or rows.
            form = _relation_form(value)
            if form == "rows":
                materialized = _given_rows(fn, name, value)
            elif form == "query":
                materialized = await _query_relation(fn, name, str(value), state, role_id)
            else:
                ref = _served_table(fn, name, value, state)
                materialized = await _table_relation(fn, name, ref, col_names, state, role_id)
                input_refs.append(ref)
            schema = _schema_from_columns(declared)
            if (
                schema is not None
            ):  # REQ-1159: enforce the declared input dataset contract, fail-loud
                materialized["rows"] = _validate_against(
                    materialized["rows"], schema, where=f"command {fn.get('name')!r} input {name!r}"
                )
            payload[name] = materialized
        else:
            raise ApiError(
                400,
                "functions.unknown_arg_kind",
                f"Unknown arg_kind {kind!r} for {name!r}",
                arg_kind=kind,
                name=name,
            )
    return payload, input_refs


# --------------------------------------------------------------------------- #
# Per-kind executors.                                                          #
# --------------------------------------------------------------------------- #


def _require(
    binding: dict, key: str, kind: str
) -> object:  # object-ok: binding values are truly-any JSON
    """Fetch a required binding key or fail loud — no silent default (REQ-885)."""
    if key not in binding or binding[key] in (None, "", []):
        raise ApiError(
            400,
            "functions.binding_missing_key",
            f"function binding for kind {kind!r} is missing required key {key!r}",
            kind=kind,
            key=key,
        )
    return binding[key]


# --------------------------------------------------------------------------- #
# Egress control (REQ-885 security_constraint) — deny-by-default allow-list.   #
# --------------------------------------------------------------------------- #

# Loopback is the Provisa pgwire boundary — implicitly reachable, never external egress, on an
# install whose org is not held (REQ-2006: a held org's call reaches public destinations only).
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


def _endpoint_port(hostport: str) -> int | None:
    """The port a ``host:port`` names, or None when it names none."""
    port = hostport.rpartition(":")[2] if ":" in hostport else ""
    return int(port) if port.isdigit() else None


def _check_egress(state, endpoint: str, kind: str) -> None:
    """Refuse an external call whose host is not on the deny-by-default egress allow-list.

    The allow-list (``state.udf_egress_allowlist``) enumerates the http/grpc endpoints a
    deployment permits hosted functions to reach; loopback (Provisa pgwire) is always
    allowed. An empty/absent list denies all external egress — no silent default (REQ-885).

    For a held org (``sandbox_egress.held``) loopback is not exempt, and a host on the list is
    reached only when every address it resolves to is public (REQ-2006, amended 2026-11-16).
    """
    from provisa.security import sandbox_egress

    host, hostport = _endpoint_host(endpoint, kind)
    held = sandbox_egress.held_here()
    if host in _LOOPBACK_HOSTS and not held:
        return
    allowlist = getattr(state, "udf_egress_allowlist", None) or []
    if host in allowlist or hostport in allowlist:
        sandbox_egress.require_allowed(
            host, _endpoint_port(hostport), refusal=sandbox_egress.PrivateEndpointRefused
        )
        return
    raise ApiError(
        403,
        "functions.egress_denied",
        (
            f"egress to {endpoint!r} is denied: host not on the UDF egress allow-list, and "
            "egress is denied by default. Add it to udf_egress_allowlist to permit."
        ),
        endpoint=endpoint,
    )


# --------------------------------------------------------------------------- #
# Row-wise-external refusal (REQ-885 performance_constraint).                  #
# --------------------------------------------------------------------------- #

_EXTERNAL_KINDS = frozenset({"http", "grpc"})
# Kinds whose arguments go to the implementation as given: a source's own procedure, and a
# source's write operation, which is passed through as is (REQ-1924).
_PASSED_THROUGH_KINDS = frozenset(
    {"source_procedure", "source_operation", "source_query", "statement"}  # REQ-2001
    | {"sql_expression", "builtin"}  # REQ-2168
)
# A code_command is not passed through: its table arguments are prepared as table_ref references
# the worker reads (REQ-2226). A command bound with fixed arguments (REQ-2228) takes the call's
# arguments as given, by name, from ``args`` rather than the prepared payload.
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
    raise ApiError(
        400,
        "functions.rowwise_external",
        (
            f"function {fn.get('name', '')!r} (kind={fn.get('impl_kind')}) is row-wise external: "
            "all arguments are scalar column_value. Declare a table_ref/result_set relation "
            "argument so calls are set-wise and batched."
        ),
        name=fn.get("name", ""),
        impl_kind=str(fn.get("impl_kind")),
    )


async def _exec_source_procedure(fn: dict, args: dict, state, _payload, _session) -> list[dict]:
    src_id = fn["source_id"]
    if not state.source_pools.has(src_id):
        raise ApiError(
            503,
            "functions.source_not_connected",
            f"Source '{src_id}' not connected",
            source_id=src_id,
        )
    params = list(args.values())
    placeholders = ", ".join(f"${i + 1}" for i in range(len(params)))
    sql = f'SELECT * FROM "{fn["schema_name"]}"."{fn["function_name"]}"({placeholders})'
    result = await state.source_pools.execute(src_id, sql, params)
    from provisa.executor.serialize import result_records

    return result_records(result)


def _json_arguments_from_sql(fn: dict, args: dict) -> dict:
    """REQ-1924: a source operation's arguments are JSON values. The SQL surfaces pass a call's
    arguments by position (``a0``, ``a1`` …) as SQL literals, so each is written there as a JSON
    literal -- ``'{"title": "x"}'``, ``'"text"'``, ``'3'`` -- and is read here as the JSON it
    spells. Arguments passed by name come from a surface that carries JSON values already."""
    keys = list(args)
    if not (keys and all(k == f"a{i}" for i, k in enumerate(keys))):
        return args
    bound = _bind_arg_names(fn, args)
    decoded: dict = {}
    for name, value in bound.items():
        if not isinstance(value, str):
            decoded[name] = value
            continue
        try:
            decoded[name] = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ApiError(
                422,
                "functions.json_argument_invalid",
                f"argument {name!r} of {fn.get('name', '')!r} is not a JSON literal: {exc}",
                name=fn.get("name", ""),
                argument=name,
            ) from exc
    return decoded


async def _exec_source_operation(fn: dict, args: dict, state, _payload, session) -> list[dict]:
    """REQ-1924: a source's write operation, called with the arguments as given. REQ-1986: an
    operation that returns the rows of its table is read as that table with the arguments
    assigned, through the one pipeline as the caller's role."""
    from provisa.compiler.operation_function import is_operation_function

    if is_operation_function(fn):
        from provisa.executor.operation_call import run

        return await run(fn, _bind_arg_names(fn, args), state, session.role_id)
    from provisa.executor.source_operation import call_operation

    return await call_operation(
        state,
        fn["source_id"],
        fn["function_name"],
        _json_arguments_from_sql(fn, args),
        fn.get("schema_name"),
    )


async def _exec_source_query(fn: dict, args: dict, state, _payload, _session) -> list[dict]:
    """REQ-1988: a command that runs its source's native query -- a MongoDB pipeline, or native
    SQL on a relational source (binding {"sql"}) -- with the call's arguments, each converted to
    its type and bound as a value."""
    from provisa.core.native_sql import BINDING_KEY

    if BINDING_KEY in (fn.get("binding") or {}):
        from provisa.executor import native_sql_command

        return await native_sql_command.run(state, fn, _bind_arg_names(fn, args))
    from provisa.mongodb.pipeline_command import run_command

    return await run_command(state, fn, _bind_arg_names(fn, args))


async def _exec_script(
    fn: dict, _args, _state, payload: dict, session: MintedSession
) -> list[dict]:
    binding = fn["binding"]
    argv = list(_require(binding, "argv", "script"))  # type: ignore[arg-type]
    # REQ-2228: a command line that takes one argument as JSON (dbt's ``run-operation --args``).
    json_argument = binding.get("json_argument")
    if json_argument is not None and payload.get(json_argument["argument"]) is not None:
        value = payload[json_argument["argument"]]
        argv += [json_argument["flag"], value if isinstance(value, str) else json.dumps(value)]
    body = json.dumps({"args": payload, "correlation_id": session.correlation_id}).encode()
    out = await _run_subprocess(argv, body)
    # REQ-2228: a tool whose report is plain text (dbt) answers one row holding it.
    if binding.get("stdout") == "text":
        return [{"output": out.decode(errors="replace")}]
    return _rows_from_response(json.loads(out or b"[]"))


async def _exec_http(fn: dict, _args, state, payload: dict, session: MintedSession) -> list[dict]:
    binding = fn["binding"]
    url = str(_require(binding, "url", "http"))
    _check_egress(state, url, "http")
    method = str(binding.get("method", "POST")).upper()
    timeout = float(binding.get("timeout_s", _HTTP_DEFAULT_TIMEOUT_S))
    if "credential_source" in binding:
        # REQ-2228: called with a body of its own and the named source's credential (dbt Cloud).
        from provisa.dbt.cloud import authorization

        headers = await authorization(state, binding["credential_source"], binding["auth_scheme"])
        return _rows_from_response(
            await _http_call(method, url, binding.get("body", {}), timeout, headers)
        )
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
        raise ApiError(
            400,
            "functions.python_callable_invalid",
            f"python callable {spec!r} must be 'module:attr'",
            spec=spec,
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
    raise ApiError(
        502,
        "functions.non_tabular_payload",
        f"function returned non-tabular payload: {type(body)!r}",
        payload_type=str(type(body)),
    )


async def _exec_code_command(
    fn: dict, args: dict, state, payload: dict, session: MintedSession
) -> list[dict]:
    """A code source's command, called in its source's worker. A binding that names a
    ``command`` is a registration bound to that ``@command`` with the arguments it fixes
    (REQ-2228); a command with no such binding is the source's ``@command`` or ``@query`` of the
    function's own name, its table arguments readable for the call (REQ-2226)."""
    from provisa.codesource.commands import call_command, run

    binding = fn.get("binding") or {}
    if "command" not in binding:
        return await run(state, fn, payload, session)
    return await call_command(
        state,
        str(_require(binding, "source_id", "code_command")),
        str(_require(binding, "command", "code_command")),
        dict(binding.get("fixed_arguments") or {}),
        _bind_arg_names(fn, args),
    )


async def _exec_statement(fn: dict, args: dict, state, _payload, session) -> list[dict]:
    """REQ-2001: a Provisa SQL statement, run through the one pipeline as the caller's role."""
    from provisa.executor.statement_command import run

    return await run(fn, _bind_arg_names(fn, args), state, session.role_id)


async def _exec_sql_expression(fn: dict, args: dict, state, _payload, session) -> list[dict]:
    """REQ-2168: a SQL expression function called on its own: SELECT its template, the call's
    arguments bound as the statement's parameters, through the one pipeline as the caller."""
    from provisa.executor.sql_expression_call import run

    return await run(fn, _bind_arg_names(fn, args), state, session.role_id)


async def _exec_builtin(fn: dict, args: dict, state, _payload, session) -> list[dict]:
    """REQ-2168: a command Provisa ships, its implementation named in the built-in registry."""
    from provisa.core.builtin_commands import implementation

    spec = str(_require(fn["binding"], "callable", "builtin"))
    return await implementation(spec)(_bind_arg_names(fn, args), state, session.role_id)


_EXECUTORS = {
    "source_procedure": _exec_source_procedure,
    "source_operation": _exec_source_operation,
    "source_query": _exec_source_query,  # REQ-1988
    "script": _exec_script,
    "http": _exec_http,
    "grpc": _exec_grpc,
    "python": _exec_python,
    "statement": _exec_statement,  # REQ-2001
    "sql_expression": _exec_sql_expression,  # REQ-2168
    "builtin": _exec_builtin,  # REQ-2168
    "code_command": _exec_code_command,  # REQ-2226, REQ-2228
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
        raise ApiError(
            400,
            "functions.unknown_impl_kind",
            f"Unknown function impl_kind: {impl_kind!r}",
            impl_kind=str(impl_kind),
        )
    refuse_host_code(fn.get("name", fn.get("function_name", "")), impl_kind)  # REQ-2134
    transport = TRANSPORT_BY_KIND[impl_kind]
    identity = "definer" if fn.get("materialize") else "invoker"
    _reject_rowwise_external(fn)

    if impl_kind in _PASSED_THROUGH_KINDS:
        payload: dict = args
        input_refs: list[str] = []
    else:
        payload, input_refs = await _prepare_args(fn, args, state, role_id)

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
        # REQ-1159: enforce the declared output dataset contract on the way out, fail-loud. An
        # off-contract external service (extra/missing/wrong-typed column) is a data-integrity fault,
        # caught here before its rows poison anything downstream (and before the content hash).
        out_schema = _schema_from_columns(fn.get("output_columns"))
        if out_schema is not None:
            rows = _validate_against(rows, out_schema, where=f"command {fn.get('name')!r} output")
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
