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

Requires grpcio and grpcio-tools. Stubs are compiled at source-registration time
and loaded dynamically via importlib (same pattern as provisa/grpc/server.py).
"""
from __future__ import annotations

import importlib.util
import logging
import sys

import grpc
import grpc.aio

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module loader (mirrors provisa/grpc/server.py:_load_module)
# ---------------------------------------------------------------------------

def _load_module(path: str, name: str):
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load module from {path!r}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    try:
        spec.loader.exec_module(mod)
    except Exception:
        del sys.modules[name]
        raise
    return mod


def load_stubs(pb2_path: str, pb2_grpc_path: str):
    """Load compiled pb2 and pb2_grpc modules. Returns (pb2, pb2_grpc)."""
    import os
    pb2_name = os.path.splitext(os.path.basename(pb2_path))[0]
    pb2_grpc_name = os.path.splitext(os.path.basename(pb2_grpc_path))[0]
    pb2 = _load_module(pb2_path, pb2_name)
    pb2_grpc = _load_module(pb2_grpc_path, pb2_grpc_name)
    return pb2, pb2_grpc


# ---------------------------------------------------------------------------
# Proto message → dict
# ---------------------------------------------------------------------------

def _msg_to_dict(msg) -> dict:
    from google.protobuf.json_format import MessageToDict  # type: ignore[import]
    return MessageToDict(
        msg,
        preserving_proto_field_name=True,
        including_default_value_fields=False,
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
# Execution
# ---------------------------------------------------------------------------

async def execute_query(
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


async def execute_mutation(
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
