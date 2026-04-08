# Copyright (c) 2026 Kenneth Stott
# Canary: 4ecf26ed-21b6-42fa-be37-8d70376aa96a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Source adapter for gRPC Remote sources — cache-aside execution (Phase AR)."""
from __future__ import annotations

import hashlib
import json
import logging

import grpc
import grpc.aio

from provisa.grpc_remote.executor import execute_query, execute_mutation

log = logging.getLogger(__name__)


def _args_hash(args: dict) -> str:
    return hashlib.sha256(
        json.dumps(sorted(args.items()), default=str).encode()
    ).hexdigest()[:12]


def _get_channel(grpc_remote_sources: dict, source_id: str) -> grpc.aio.Channel:
    reg = grpc_remote_sources.get(source_id)
    if reg is None:
        raise KeyError(f"gRPC remote source {source_id!r} not registered")
    channel = reg.get("channel")
    if channel is None:
        raise RuntimeError(f"No active channel for gRPC source {source_id!r}")
    return channel


async def fetch(
    source_id: str,
    full_method_path: str,
    input_message_name: str,
    output_message_name: str,
    pb2,
    args: dict,
    grpc_remote_sources: dict,
    cache_store,
    role: str = "",
    ttl: int = 300,
    server_streaming: bool = False,
) -> list[dict]:
    """Execute a gRPC query method with cache-aside. Returns rows as list of dicts."""
    cache_key = (
        f"grpc_remote:{source_id}:{full_method_path}:{_args_hash(args)}:{role}"
    )

    cached = await cache_store.get(cache_key)
    if cached is not None:
        log.debug("Cache hit %s", cache_key)
        return json.loads(cached)

    channel = _get_channel(grpc_remote_sources, source_id)
    rows = await execute_query(
        channel,
        full_method_path,
        pb2,
        input_message_name,
        output_message_name,
        args,
        server_streaming=server_streaming,
    )

    await cache_store.set(cache_key, json.dumps(rows, default=str), ttl=ttl)
    return rows


async def execute_rpc(
    source_id: str,
    full_method_path: str,
    input_message_name: str,
    output_message_name: str,
    pb2,
    args: dict,
    grpc_remote_sources: dict,
) -> dict:
    """Execute a gRPC mutation method. Never cached."""
    channel = _get_channel(grpc_remote_sources, source_id)
    return await execute_mutation(
        channel,
        full_method_path,
        pb2,
        input_message_name,
        output_message_name,
        args,
    )
