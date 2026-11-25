# Copyright (c) 2026 Kenneth Stott
# Canary: 633f854d-ed5b-43bb-9a60-9cc1cee3b623
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Execute gRPC calls against a remote service using dynamically compiled stubs.

Requires grpcio and grpcio-tools. The .proto is compiled at source-registration time and its
message classes are built in a descriptor pool of the source's own (load_stubs).
"""

# Requirements: REQ-322, REQ-325, REQ-326, REQ-327

from __future__ import annotations

import asyncio
import ipaddress
import logging
import os
import types
import weakref
from pathlib import Path

import grpc
import grpc.aio

from provisa.security.sandbox_egress import PrivateDestinationRefused, Refusal, pinned_address

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Message classes, in a descriptor pool of the source's own
# ---------------------------------------------------------------------------


def load_stubs(pb2_path: str) -> types.ModuleType:  # REQ-617
    """The source's message classes, by message name, as its generated ``_pb2`` module names them.

    They are built from the descriptor set compiled beside ``pb2_path`` into a descriptor pool of
    their own, never the process's default pool that importing the generated module adds to. There
    a second source compiled from the same .proto, or a source refreshed with its message names
    kept, defines those names again and is refused ("duplicate symbol")."""
    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

    stem = pb2_path.removesuffix("_pb2.py")
    files = descriptor_pb2.FileDescriptorSet.FromString(Path(f"{stem}.desc").read_bytes())
    pool = descriptor_pool.DescriptorPool()
    for file in files.file:  # protoc writes a set in dependency order
        pool.Add(file)
    own = pool.FindFileByName(f"{os.path.basename(stem)}.proto")
    pb2 = types.ModuleType(os.path.basename(stem) + "_pb2")
    for name, message in own.message_types_by_name.items():
        setattr(pb2, name, message_factory.GetMessageClass(message))
    return pb2


# ---------------------------------------------------------------------------
# Proto message → dict
# ---------------------------------------------------------------------------


def _msg_to_dict(msg) -> dict:
    from google.protobuf.json_format import MessageToDict  # type: ignore[import]

    return MessageToDict(
        msg,
        preserving_proto_field_name=True,
        always_print_fields_with_no_presence=False,
    )


def _build_request(pb2, input_message_name: str, args: dict):
    msg_cls = getattr(pb2, input_message_name, None)
    if msg_cls is None:
        raise ValueError(f"Message type {input_message_name!r} not found in pb2 module")
    # Only pass args that match known field names on the message descriptor
    descriptor = msg_cls.DESCRIPTOR
    known = {f.name for f in descriptor.fields}
    filtered = {k: v for k, v in args.items() if k in known}
    return msg_cls(**filtered)


# ---------------------------------------------------------------------------
# Channel construction
# ---------------------------------------------------------------------------


def open_channel(
    server_address: str, tls: bool, *, refusal: Refusal = PrivateDestinationRefused
) -> grpc.aio.Channel:
    """Open a gRPC channel to a remote service, secure or insecure. ``refusal`` is what a held
    org's private destination is refused as: a source's unless another is given (a command's)."""
    # REQ-2006: a sandbox org's channel dials the address its check resolved, its name kept for TLS.
    target, name = pinned_address(server_address, refusal=refusal)
    if tls:
        credentials = grpc.ssl_channel_credentials()
        options = [("grpc.ssl_target_name_override", name)] if target != server_address else []
        return grpc.aio.secure_channel(target, credentials, options=options)
    return grpc.aio.insecure_channel(target)


def channel_for(reg: dict) -> grpc.aio.Channel:  # REQ-327, REQ-1882
    """The source's channel on the running event loop.

    A ``grpc.aio`` channel belongs to the loop it was opened on, and each request runs on a loop
    of its own, checked out of a bounded pool (REQ-1882). One channel opened when the source was
    added served only the request that added it; every later request on another loop failed
    with "attached to a different loop". So the source keeps one channel per loop, opened the
    first time a request on that loop calls the source; a loop that is let go takes its channel
    with it."""
    loop = asyncio.get_running_loop()
    channels: weakref.WeakKeyDictionary = reg.setdefault("channels", weakref.WeakKeyDictionary())
    channel = channels.get(loop)
    if channel is None:
        channel = open_channel(reg["server_address"], reg.get("tls", False))
        channels[loop] = channel
    return channel


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------


async def execute_query(  # REQ-325, REQ-327
    channel: grpc.aio.Channel,
    full_method_path: str,
    pb2,
    input_message_name: str,
    output_message_name: str,
    args: dict,
    server_streaming: bool = False,
) -> list[dict]:
    """Execute a query (read-only) gRPC method and return rows as list of dicts."""
    request = _build_request(pb2, input_message_name, args)

    output_cls = getattr(pb2, output_message_name, None)
    if output_cls is None:
        raise ValueError(f"Output message type {output_message_name!r} not found")

    if server_streaming:
        multi_callable = channel.unary_stream(
            full_method_path,
            request_serializer=request.__class__.SerializeToString,
            response_deserializer=output_cls.FromString,
        )
        rows: list[dict] = []
        async for response in multi_callable(request):
            rows.append(_msg_to_dict(response))
        return rows
    else:
        unary_callable = channel.unary_unary(
            full_method_path,
            request_serializer=request.__class__.SerializeToString,
            response_deserializer=output_cls.FromString,
        )
        response = await unary_callable(request)
        return [_msg_to_dict(response)]


async def execute_mutation(  # REQ-326, REQ-327
    channel: grpc.aio.Channel,
    full_method_path: str,
    pb2,
    input_message_name: str,
    output_message_name: str,
    args: dict,
) -> dict:
    """Execute a mutation (side-effecting) gRPC method. Never cached."""
    request = _build_request(pb2, input_message_name, args)

    output_cls = getattr(pb2, output_message_name, None)
    if output_cls is None:
        raise ValueError(f"Output message type {output_message_name!r} not found")

    unary_callable = channel.unary_unary(
        full_method_path,
        request_serializer=request.__class__.SerializeToString,
        response_deserializer=output_cls.FromString,
    )
    response = await unary_callable(request)
    return _msg_to_dict(response)


class MutationRefused(Exception):
    """The remote refused a mutation call: its status code and what it said (REQ-1924)."""


async def call_mutation(  # REQ-1924
    channel: grpc.aio.Channel,
    full_method_path: str,
    pb2,
    input_message_name: str,
    output_message_name: str,
    args: dict,
) -> dict:
    """:func:`execute_mutation`, with the remote's refusal raised as :class:`MutationRefused`
    stating its status code and details as the remote gave them."""
    try:
        return await execute_mutation(
            channel, full_method_path, pb2, input_message_name, output_message_name, args
        )
    except grpc.aio.AioRpcError as exc:
        raise MutationRefused(f"{exc.code().name}: {exc.details()}") from exc


# ---------------------------------------------------------------------------
# A raw unary call: request and answer as bytes the caller encodes and reads
# ---------------------------------------------------------------------------


class UnaryCallFailed(Exception):
    """A raw unary call the server did not answer: its status code name and what it said."""

    def __init__(self, code: str, details: str | None) -> None:
        super().__init__(f"{code}: {details}")
        self.code = code
        self.details = details


def _as_is(message: bytes) -> bytes:
    """The request and the answer go as the bytes the caller encoded."""
    return message


def call_unary_raw(  # REQ-2225
    target: str,
    *,
    tls: bool,
    method: str,
    request: bytes,
    timeout: float,
    org_id: str,
    refusal: Refusal,
    metadata: tuple[tuple[str, str], ...] = (),
) -> bytes:
    """Call ``method`` on the server at ``target`` (``host:port``) once, blocking, with ``request``
    as encoded bytes; return the answer's bytes. Raises :class:`UnaryCallFailed` with the server's
    status code and words when it does not answer. For a held org (REQ-2006) ``target`` is checked
    before any dial, refused as ``refusal`` when it is not public, and dialed at the address its
    check resolved, its name kept for TLS, as :func:`open_channel` does."""
    pinned, name = pinned_address(target, org_id, refusal)
    if tls:
        options = [("grpc.ssl_target_name_override", name)] if pinned != target else []
        channel = grpc.secure_channel(pinned, grpc.ssl_channel_credentials(), options=options)
    else:
        channel = grpc.insecure_channel(pinned)
    return _call_once(channel, method, request, timeout, metadata)


class NotLoopback(ValueError):
    """A loopback-only call given an address that is not a loopback IP literal."""

    def __init__(self, host: str) -> None:
        super().__init__(
            f"{host!r} is not a loopback address: a server Provisa starts itself is called on "
            "127.0.0.1 or ::1 only"
        )
        self.host = host


def call_unary_loopback(  # REQ-2225
    host: str,
    port: int,
    *,
    method: str,
    request: bytes,
    timeout: float,
) -> bytes:
    """:func:`call_unary_raw` for a server Provisa started itself on this machine, such as the
    demo's Spark Connect server: no organisation typed its address, so the sandbox egress rule
    (REQ-2006) has nothing to check. ``host`` must be a loopback IP literal; anything else, a name
    included (it would need a lookup), is refused as :class:`NotLoopback` before any dial."""
    try:
        loopback = ipaddress.ip_address(host).is_loopback
    except ValueError:
        loopback = False
    if not loopback:
        raise NotLoopback(host)
    target = f"[{host}]:{port}" if ":" in host else f"{host}:{port}"
    return _call_once(grpc.insecure_channel(target), method, request, timeout, ())


def _call_once(
    channel: grpc.Channel,
    method: str,
    request: bytes,
    timeout: float,
    metadata: tuple[tuple[str, str], ...],
) -> bytes:
    """One unary call on ``channel``, which it closes; a refusal is :class:`UnaryCallFailed`."""
    try:
        call = channel.unary_unary(method, request_serializer=_as_is, response_deserializer=_as_is)
        return call(request, timeout=timeout, metadata=metadata)
    except grpc.RpcError as failed:
        # A unary call's RpcError is also its grpc.Call, which carries the status and the words.
        if not isinstance(failed, grpc.Call):
            raise
        raise UnaryCallFailed(failed.code().name, failed.details()) from None
    finally:
        channel.close()
