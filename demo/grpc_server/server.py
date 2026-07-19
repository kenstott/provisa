# Copyright (c) 2026 Kenneth Stott
# Canary: 9a3e7c05-6b18-42df-8c74-1e2a5d9f0b63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Demo gRPC server for a Provisa grpc-kind command (REQ-885).

Proto-less: it mirrors provisa.executor.function_dispatch._grpc_call, whose bridge sends the JSON
request as opaque unary bytes and decodes the unary response as JSON. This server registers ONE
method, ``/provisa.demo.RandomData/GetRandomSet``, that returns a JSON list of random-valued rows —
a "command that returns a set" demonstrated over the gRPC transport, with no .proto/codegen.

Run: python -m demo.grpc_server.server   (PORT env overrides the default 50071)
"""

from __future__ import annotations

import asyncio
import json
import os
import random

import grpc

_METHOD = "/provisa.demo.RandomData/GetRandomSet"
_REGIONS = ("north", "south", "east", "west")


def _identity(b: bytes) -> bytes:
    return b


async def _get_random_set(request: bytes, _context) -> bytes:
    """Decode the JSON request, return `rows` (default 5) random rows as JSON bytes."""
    payload = json.loads(request or b"{}")
    args = payload.get("args", {}) if isinstance(payload, dict) else {}
    rng = random.Random(args.get("seed")) if args.get("seed") is not None else random.Random()
    n = int(args.get("rows", 5))
    rows = [
        {
            "id": i,
            "region": rng.choice(_REGIONS),
            "amount": round(rng.uniform(0, 1000), 2),
            "active": rng.random() > 0.5,
        }
        for i in range(1, n + 1)
    ]
    return json.dumps(rows).encode()


class _RandomDataHandler(grpc.GenericRpcHandler):
    """Match the single demo method path and dispatch it as a bytes-in/bytes-out unary RPC."""

    def service(self, handler_call_details):
        if handler_call_details.method == _METHOD:
            return grpc.unary_unary_rpc_method_handler(
                _get_random_set, request_deserializer=_identity, response_serializer=_identity
            )
        return None


async def serve() -> None:
    port = os.environ.get("DEMO_GRPC_PORT", "50071")
    server = grpc.aio.server()
    server.add_generic_rpc_handlers((_RandomDataHandler(),))
    server.add_insecure_port(f"0.0.0.0:{port}")
    await server.start()
    print(f"demo gRPC RandomData server listening on 0.0.0.0:{port} ({_METHOD})", flush=True)
    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(serve())
