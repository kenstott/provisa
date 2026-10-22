# Copyright (c) 2026 Kenneth Stott
# Canary: 4fe10e69-0d48-467d-b774-8e8fe746df83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1911: the bench's gRPC callers each reach the server on a connection of their own.

The server runs one gRPC server per worker process on one port (SO_REUSEPORT), and the kernel
spreads connections, not calls, over them. Callers sharing a connection -- one channel per client
process, or channels gRPC folds onto one connection -- load as many workers as there are client
processes, whatever the concurrency."""

from __future__ import annotations

import sys
from concurrent import futures
from pathlib import Path
from typing import Any

import grpc
import pytest

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))

from run_benchmark import GrpcTransport  # noqa: E402

_METHOD = "/bench.Echo/Peer"


@pytest.fixture
def peers() -> Any:
    """A local gRPC server answering each call with the caller's connection (its peer address)."""

    def peer(request: bytes, context: grpc.ServicerContext) -> bytes:
        return context.peer().encode()

    handler = grpc.method_handlers_generic_handler(
        "bench.Echo", {"Peer": grpc.unary_unary_rpc_method_handler(peer)}
    )
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    server.add_generic_rpc_handlers((handler,))
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        yield f"127.0.0.1:{port}"
    finally:
        server.stop(None)


def _transport(target: str) -> GrpcTransport:
    # Only what channel() reads: the compiled proto and the server's address are not needed here.
    transport = GrpcTransport.__new__(GrpcTransport)
    transport._grpc = grpc  # noqa: SLF001
    transport._target = target  # noqa: SLF001
    return transport


def _peer(channel: Any) -> str:
    call = channel.unary_unary(_METHOD)
    return call(b"", timeout=5).decode()


def test_each_channel_is_a_connection_of_its_own(peers: str) -> None:
    transport = _transport(peers)
    channels = [transport.channel() for _ in range(4)]
    try:
        assert len({_peer(c) for c in channels}) == 4
    finally:
        for c in channels:
            c.close()
