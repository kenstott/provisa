# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Demo gRPC server for the gRPC Remote Schema Connector e2e (REQ-1741).

Proto-BASED, unlike demo/grpc_server/server.py's proto-less bytes-in/bytes-out bridge: this
server compiles animal_catalog.proto with the SAME grpc_tools.protoc path the connector itself
uses (provisa.grpc_remote.loader.compile_proto_stubs / executor.load_stubs) so the wire format the
server emits is exactly what a real grpc_remote registration would generate stubs for — nothing
here is faked or pre-canned wire bytes.

AnimalCatalog.ListBreeds is server-streaming: it yields Breed messages one at a time, giving
provisa/grpc_remote/mapper.py real per-column typing (see animal_catalog.proto's own comment).

Run: python -m demo.grpc_remote_server.server   (DEMO_GRPC_REMOTE_PORT env overrides 50072)
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import grpc

from provisa.grpc_remote.executor import load_stubs
from provisa.grpc_remote.loader import compile_proto_stubs

_PROTO_PATH = Path(__file__).with_name("animal_catalog.proto")
_METHOD = "/provisa.demo.animal.AnimalCatalog/ListBreeds"

# Deterministic seed data — no randomness, so the e2e can assert exact rows.
_BREEDS: list[dict] = [
    {"name": "Labrador Retriever", "species": "dog", "avg_lifespan_years": 12},
    {"name": "Siamese", "species": "cat", "avg_lifespan_years": 15},
    {"name": "Holland Lop", "species": "rabbit", "avg_lifespan_years": 8},
]


def _make_handler(pb2) -> grpc.GenericRpcHandler:
    async def _list_breeds(request, _context):
        limit = request.limit or len(_BREEDS)
        for row in _BREEDS[:limit]:
            yield pb2.Breed(**row)

    class _Handler(grpc.GenericRpcHandler):
        def service(self, handler_call_details):
            if handler_call_details.method != _METHOD:
                return None
            return grpc.unary_stream_rpc_method_handler(
                _list_breeds,
                request_deserializer=pb2.ListBreedsRequest.FromString,
                response_serializer=pb2.Breed.SerializeToString,
            )

    return _Handler()


async def serve() -> None:
    port = os.environ.get("DEMO_GRPC_REMOTE_PORT", "50072")
    proto_text = _PROTO_PATH.read_text()
    pb2_path, pb2_grpc_path = compile_proto_stubs(proto_text, proto_name="animal_catalog")
    pb2, _ = load_stubs(pb2_path, pb2_grpc_path)

    server = grpc.aio.server()
    server.add_generic_rpc_handlers((_make_handler(pb2),))
    server.add_insecure_port(f"0.0.0.0:{port}")
    await server.start()
    print(f"demo grpc_remote server listening on 0.0.0.0:{port} ({_METHOD})", flush=True)
    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(serve())
