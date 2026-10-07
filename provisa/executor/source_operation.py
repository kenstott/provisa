# Copyright (c) 2026 Kenneth Stott
# Canary: 3e9b7c21-6d4a-4f85-a0c3-8b2d5e1f9a47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A source's operation, registered as a command and called through it (REQ-1924).

A remote source -- OpenAPI, remote GraphQL, gRPC -- offers write operations: an OpenAPI
operation that is not a GET, a field of a GraphQL schema's mutation root, a gRPC method
classified as a mutation. One the steward registers is a command of kind ``source_operation``,
named by the source and the operation. It is a mutator: it creates, changes or deletes something
in the remote system.

An OpenAPI source also offers each GET whose response declares no row schema -- a diff, a log,
an untyped document, a file. It is a command because what it answers is not rows a table could
hold; it reads, so it is registered as a query and none of what holds for a write holds for it.
An operation that answers with a file (a media type neither text nor JSON) answers one binary value.

A call is passed through as is. Provisa does not shape, type or check the input: each argument
the caller gives goes to the remote unchanged, with the source's credential, and the remote's
answer -- a refusal included -- comes back unchanged. What Provisa governs is the call's
security, and that is done before a call reaches here (who may call it, in which domain) and
around it (the invocation trace every command call records).

This module knows, per source type, what a source offers and how one operation is called.
"""

# Requirements: REQ-1924, REQ-317, REQ-326

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

import httpx

from provisa.api.errors import ApiError
from provisa.core.ir_types import bytea_hex

# The schema name a source's operations are registered under, per source type.
OPERATION_SCHEMA = {"openapi": "openapi", "graphql_remote": "graphql", "grpc_remote": "grpc_remote"}

# An OpenAPI operation's request body is passed as this one argument.
BODY_ARGUMENT = "body"

# How deep a GraphQL mutation's answer is selected: its scalar fields, and those of the objects
# it holds to this many levels.
_ANSWER_DEPTH = 2

_PATH_PARAM = re.compile(r"\{([^}]+)\}")
_HTTP_SERVER_ERROR = 500


@dataclass(frozen=True)
class Operation:
    """One operation a source offers as a command: its name, a line saying what it is, the
    arguments a call passes through to it, whether it only reads, and whether it answers with a
    file."""

    name: str
    comment: str | None
    arguments: tuple[str, ...]
    reads: bool = False
    binary: bool = False


# What a command that answers with a file returns: one row holding the file, a bytea.
BINARY_ANSWER = ({"name": "result", "type": "bytea"},)


def _source_type(state, source_id: str) -> str:
    return (getattr(state, "source_types", None) or {}).get(source_id, "")


# --- what a source offers ----------------------------------------------------------------------


def _openapi_operations(state, source_id: str) -> list[Operation] | None:
    from provisa.openapi.mapper import parse_spec

    entry = (getattr(state, "openapi_specs", None) or {}).get(source_id)
    if entry is None:
        return None
    _, mutations = parse_spec(entry["spec"])
    return [
        Operation(
            name=m.operation_id,
            comment=f"[{m.method.upper()}] {m.path}" + (f" — {m.summary}" if m.summary else ""),
            arguments=(
                *_PATH_PARAM.findall(m.path),
                *((BODY_ARGUMENT,) if m.input_schema is not None else ()),
            ),
            reads=m.reads,
            binary=m.binary,
        )
        for m in mutations
    ]


def _mutation_root(schema: dict) -> dict | None:
    root = (schema.get("mutationType") or {}).get("name")
    return next((t for t in schema.get("types") or [] if t.get("name") == root), None)


async def _graphql_schema(state, source_id: str) -> dict | None:
    from provisa.api.admin._graphql_table_registration import source_offer

    offered = await source_offer(state, source_id)
    return None if offered is None else offered[0].schema()


async def _graphql_operations(state, source_id: str) -> list[Operation] | None:
    schema = await _graphql_schema(state, source_id)
    if schema is None:
        return None
    root = _mutation_root(schema)
    return [
        Operation(
            name=f["name"],
            comment=f.get("description"),
            arguments=tuple(a["name"] for a in f.get("args") or []),
        )
        for f in (root or {}).get("fields") or []
    ]


def grpc_operation_name(mutation) -> str:
    """The name a gRPC mutation method is offered and registered under."""
    return f"{mutation.service}.{mutation.method}"


def _grpc_operations(state, source_id: str) -> list[Operation] | None:
    reg = (getattr(state, "grpc_remote_sources", None) or {}).get(source_id)
    if reg is None:
        return None
    return [
        Operation(
            name=grpc_operation_name(m),
            comment=m.full_method_path,
            arguments=tuple(f.name for f in m.input_fields),
        )
        for m in reg.get("mutations") or []
    ]


async def offered_operations(state, source_id: str) -> list[Operation] | None:
    """Every write operation ``source_id`` offers, or None when it is not a remote source this
    process holds."""
    source_type = _source_type(state, source_id)
    if source_type == "openapi":
        return _openapi_operations(state, source_id)
    if source_type == "graphql_remote":
        return await _graphql_operations(state, source_id)
    if source_type == "grpc_remote":
        return _grpc_operations(state, source_id)
    return None


async def offered_operation(state, source_id: str, name: str) -> Operation:
    """The operation ``name`` of ``source_id``; refused when the source offers none by it."""
    offered = await offered_operations(state, source_id)
    if offered is None:
        raise ApiError(
            422,
            "functions.not_a_remote_source",
            f"Source {source_id!r} offers no write operations",
            source_id=source_id,
        )
    found = next((op for op in offered if op.name == name), None)
    if found is None:
        raise ApiError(
            422,
            "functions.operation_not_offered",
            f"Source {source_id!r} offers no operation {name!r}",
            source_id=source_id,
            operation=name,
        )
    return found


# --- calling one ---------------------------------------------------------------------------------


def _refused(source_id: str, operation: str, status: int | None, answer: Any) -> ApiError:
    """The remote's refusal, passed back as the remote stated it. The remote refusing the call
    is a 422 carrying the status it answered with; the remote failing is a bad gateway."""
    said = answer if isinstance(answer, str) else json.dumps(answer, default=str)
    return ApiError(
        502 if status is not None and status >= _HTTP_SERVER_ERROR else 422,
        "functions.remote_refused",
        f"{source_id} refused {operation}: {said}",
        source_id=source_id,
        operation=operation,
        remote_status=status,
        answer=said,
    )


def _answer(resp: httpx.Response) -> Any:
    try:
        return resp.json()
    except ValueError:
        return resp.text


def _rows(answer: Any) -> list[dict]:
    """The remote's answer as the rows a command returns: an object is one row, a list of
    objects is its rows, anything else is one row holding it."""
    if isinstance(answer, dict):
        return [answer]
    if isinstance(answer, list) and all(isinstance(a, dict) for a in answer):
        return answer
    return [{"result": answer}]


async def _call_openapi(state, source_id: str, operation: str, args: dict) -> list[dict]:
    from provisa.core.secrets import resolve_secrets
    from provisa.openapi.executor import _build_auth_headers
    from provisa.openapi.mapper import parse_spec

    entry = state.openapi_specs[source_id]
    _, mutations = parse_spec(entry["spec"])
    mutation = next(m for m in mutations if m.operation_id == operation)
    given = dict(args)
    path = _PATH_PARAM.sub(lambda m: str(given.pop(m.group(1))), mutation.path)
    body = given.pop(BODY_ARGUMENT, None)
    auth = {
        k: resolve_secrets(v) if isinstance(v, str) else v
        for k, v in (entry.get("auth_config") or {}).items()
    }
    headers = {"Content-Type": "application/json", **_build_auth_headers(auth or None)}
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.request(
            mutation.method.upper(),
            entry["base_url"].rstrip("/") + path,
            params=given or None,
            json=body,
            headers=headers,
        )
    if resp.is_error:
        raise _refused(source_id, operation, resp.status_code, _answer(resp))
    if mutation.binary:
        # A command's rows hold a bytea in its canonical text form, as a source procedure's do
        # (function_dispatch); each surface carries it from there as it carries any bytea.
        return [{BINARY_ANSWER[0]["name"]: bytea_hex(resp.content)}]
    return _rows(_answer(resp))


def _type_ref(type_ref: dict) -> str:
    """A GraphQL type reference as it is written in a variable definition."""
    if type_ref["kind"] == "NON_NULL":
        return f"{_type_ref(type_ref['ofType'])}!"
    if type_ref["kind"] == "LIST":
        return f"[{_type_ref(type_ref['ofType'])}]"
    return type_ref["name"]


def _named(type_ref: dict) -> dict:
    while type_ref.get("ofType"):
        type_ref = type_ref["ofType"]
    return type_ref


def _selection(schema: dict, type_ref: dict, depth: int) -> str:
    """What is asked back of a mutation's answer: the fields that need no argument, scalars
    and enums, and the objects among them to ``depth`` levels."""
    named = _named(type_ref)
    if named["kind"] in ("SCALAR", "ENUM"):
        return ""
    found = next((t for t in schema.get("types") or [] if t.get("name") == named["name"]), {})
    parts = ["__typename"]
    for f in found.get("fields") or []:
        if any(a["type"]["kind"] == "NON_NULL" for a in f.get("args") or []):
            continue  # a field that needs an argument cannot be asked for without one
        inner = _named(f["type"])
        if inner["kind"] in ("SCALAR", "ENUM"):
            parts.append(f["name"])
        elif depth > 0:
            parts.append(f"{f['name']} {_selection(schema, f['type'], depth - 1)}")
    return "{ " + " ".join(parts) + " }"


def mutation_document(schema: dict, operation: str, given: list[str]) -> str:
    """The GraphQL document that calls the mutation ``operation`` with the arguments ``given``,
    each passed as a variable of the type the schema declares for it."""
    root = _mutation_root(schema) or {}
    field = next(f for f in root.get("fields") or [] if f["name"] == operation)
    declared = {a["name"]: a["type"] for a in field.get("args") or []}
    passed = [a for a in given if a in declared]
    head = f"({', '.join(f'${a}: {_type_ref(declared[a])}' for a in passed)})" if passed else ""
    call = f"({', '.join(f'{a}: ${a}' for a in passed)})" if passed else ""
    answer = _selection(schema, field["type"], _ANSWER_DEPTH)
    return f"mutation{head} {{ {operation}{call}{f' {answer}' if answer else ''} }}"


async def _call_graphql(state, source_id: str, operation: str, args: dict) -> list[dict]:
    from provisa.graphql_remote.introspect import _build_headers

    reg = state.graphql_remote_sources[source_id]
    schema = await _graphql_schema(state, source_id)
    assert schema is not None  # the operation was found in it
    document = mutation_document(schema, operation, list(args))
    headers = {"Content-Type": "application/json", **_build_headers(reg.get("auth"))}
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            reg["url"], json={"query": document, "variables": args}, headers=headers
        )
    answer = _answer(resp)
    if resp.is_error:
        raise _refused(source_id, operation, resp.status_code, answer)
    if isinstance(answer, dict) and answer.get("errors"):
        raise _refused(source_id, operation, resp.status_code, answer["errors"])
    return _rows((answer.get("data") or {}).get(operation) if isinstance(answer, dict) else answer)


async def _call_grpc(state, source_id: str, operation: str, args: dict) -> list[dict]:
    from provisa.grpc_remote.executor import MutationRefused, call_mutation, channel_for

    reg = state.grpc_remote_sources[source_id]
    mutation = next(m for m in reg["mutations"] if grpc_operation_name(m) == operation)
    try:
        answer = await call_mutation(
            channel_for(reg),
            mutation.full_method_path,
            reg["pb2"],
            mutation.input_message,
            mutation.output_message,
            args,
        )
    except MutationRefused as exc:
        raise _refused(source_id, operation, None, str(exc)) from exc
    return _rows(answer)


async def call_operation(state, source_id: str, operation: str, args: dict) -> list[dict]:
    """Call the write operation ``operation`` of ``source_id`` with ``args`` as given."""
    await offered_operation(state, source_id, operation)
    source_type = _source_type(state, source_id)
    if source_type == "openapi":
        return await _call_openapi(state, source_id, operation, args)
    if source_type == "graphql_remote":
        return await _call_graphql(state, source_id, operation, args)
    return await _call_grpc(state, source_id, operation, args)


def written_table(state, source_id: str, schema_table: str) -> dict | None:
    """The registered table of ``source_id`` named ``schema.table``, or None when it has none by
    that name (REQ-1924, REQ-871)."""
    schema, _, table = schema_table.partition(".")
    return next(
        (
            t
            for t in getattr(state, "tables", None) or []
            if t["source_id"] == source_id
            and t["schema_name"] == schema
            and t["table_name"] == table
        ),
        None,
    )


def _writes(command: dict) -> bool:
    """Whether ``command`` is a source's write operation (one registered as a query reads)."""
    from provisa.security.mutation_authz import MutationKind, classify_kind

    return (
        command.get("impl_kind") == "source_operation"
        and classify_kind(command.get("kind")) is MutationKind.WRITE
    )


def writes_called_in(tree, commands: dict) -> list[str]:
    """The source write operations a parsed statement calls, in any position -- a relation in
    FROM, a value in a projection, an argument -- by command name (REQ-1924)."""
    import sqlglot.expressions as exp

    return sorted(
        {
            node.name
            for node in tree.find_all(exp.Anonymous)
            if _writes(commands.get(node.name) or {})
        }
    )


def refuse_writes_in_definition(sql: str, commands: dict, what: str) -> None:
    """REQ-1924: a view or a materialized view whose definition calls a source's write operation
    would perform the write each time it is read or refreshed, so the definition is refused."""
    import sqlglot
    import sqlglot.errors

    if not any(_writes(c) for c in commands.values()):
        return  # no write operation is registered, so there is none a definition could call
    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except sqlglot.errors.ParseError as exc:
        # A definition that cannot be read cannot be shown to call no write: refused.
        raise ValueError(f"{what} does not parse: {exc}") from exc
    called = writes_called_in(tree, commands)
    if called:
        raise ValueError(
            f"{what} calls {', '.join(called)}, which writes to its source and is called on its "
            "own: a view or materialized view cannot call it (REQ-1924)"
        )
