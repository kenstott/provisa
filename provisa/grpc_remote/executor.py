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

Requires grpcio and grpcio-tools. Stubs are compiled at source-registration time and their
message classes built in a descriptor pool of the source's own (:func:`load_stubs`).
"""

# Requirements: REQ-322, REQ-325, REQ-326, REQ-327

from __future__ import annotations

import asyncio
import logging
import weakref
from pathlib import Path
from types import SimpleNamespace

import grpc
import grpc.aio

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Stub loader
# ---------------------------------------------------------------------------


def load_stubs(pb2_path: str) -> SimpleNamespace:  # REQ-617, REQ-329
    """A source's message classes, each source's in a descriptor pool of its own.

    Returned as the generated ``_pb2`` module would offer them: each top-level message of the
    proto by its name, and the file's ``DESCRIPTOR``. They are built from the descriptor set
    :func:`~provisa.grpc_remote.loader.compile_proto_stubs` writes beside the module, not by
    importing the module: the module adds its file to protobuf's process-wide default pool, where
    a message's full name (package.Message) is one symbol. A second source of the same proto, or
    a source added again under a new id, was refused there ("duplicate symbol"), and a module
    cached by its name in ``sys.modules`` kept serving a source's old proto after a refresh.
    """
    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

    from provisa.grpc_remote.loader import descriptor_set_path

    files = descriptor_pb2.FileDescriptorSet.FromString(
        Path(descriptor_set_path(pb2_path)).read_bytes()
    )
    pool = descriptor_pool.DescriptorPool()
    for file_proto in files.file:  # protoc lists a file's imports before the file
        pool.Add(file_proto)
    proto_file = pool.FindFileByName(files.file[-1].name)
    messages = {
        name: message_factory.GetMessageClass(descriptor)
        for name, descriptor in proto_file.message_types_by_name.items()
    }
    return SimpleNamespace(DESCRIPTOR=proto_file, **messages)


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


def open_channel(server_address: str, tls: bool) -> grpc.aio.Channel:
    """Open a gRPC channel to a remote service, secure or insecure."""
    from provisa.security.sandbox_egress import pinned_address

    # REQ-2006: a sandbox org's channel dials the address its check resolved, its name kept for TLS.
    target, name = pinned_address(server_address)
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
