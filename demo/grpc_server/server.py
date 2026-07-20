# Copyright (c) 2026 Kenneth Stott
# Canary: 9a3e7c05-6b18-42df-8c74-1e2a5d9f0b63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Demo gRPC server for Provisa grpc-kind commands (REQ-885).

Proto-less: it mirrors provisa.executor.function_dispatch._grpc_call, whose bridge sends the JSON
request as opaque unary bytes and decodes the unary response as JSON. Two methods, no .proto/codegen:

  /provisa.demo.RandomData/GetRandomSet  — args in → a set of random-valued rows (a "command that
      RETURNS a set", driven by scalar args; it receives no input relation).

  /provisa.demo.Enrich/EnrichRows        — a result_set relation in → the SAME rows back, each with
      two derived columns. This is the enrichment perspective: Provisa materializes a referenced
      relation, sends its rows as the request body, and the service returns them transformed. The
      derivation is DETERMINISTIC (no wall-clock, no randomness) so the demo is reproducible:
        - embedding: a fixed-dim unit vector hashed from the whole row (whole-row derivation)
        - geo:       a canned lat,lon marker keyed off a 'region' field, "" otherwise (field
                     derivation — the address→geo-marker use case, canned so the demo needs no
                     external geocoder)

Run: python -m demo.grpc_server.server   (DEMO_GRPC_PORT env overrides the default 50071)
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import random

import grpc

_RANDOM_METHOD = "/provisa.demo.RandomData/GetRandomSet"
_ENRICH_METHOD = "/provisa.demo.Enrich/EnrichRows"
_REGIONS = ("north", "south", "east", "west")

# Canned region -> (lat, lon) markers. A real geocoder would resolve an address; the demo ships a
# fixed table so it needs no external service and stays deterministic.
_REGION_GEO: dict[str, tuple[float, float]] = {
    "north": (44.9778, -93.2650),
    "south": (29.7604, -95.3698),
    "east": (40.7128, -74.0060),
    "west": (37.7749, -122.4194),
}
_EMBED_DIM = 8


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


def _embedding(row: dict, dim: int = _EMBED_DIM) -> list[float]:
    """A deterministic pseudo-embedding: fold each (key, value) into a fixed-dim vector via SHA-256,
    then L2-normalize. Stands in for a real embedding model — the point is that it reads every input
    cell and is reproducible (Python's hash() is per-process salted, so hashlib is used instead)."""
    vec = [0.0] * dim
    for key, value in sorted(row.items()):
        digest = hashlib.sha256(f"{key}={value!r}".encode()).digest()
        for i in range(dim):
            vec[i] += (digest[i] / 255.0) * 2.0 - 1.0  # byte -> [-1, 1)
    norm = sum(x * x for x in vec) ** 0.5 or 1.0
    return [round(x / norm, 4) for x in vec]


def _geo_marker(row: dict) -> str:
    """A canned lat,lon for a row's 'region' (the address->geo-marker case), "" when absent."""
    region = str(row.get("region", "")).lower()
    if region in _REGION_GEO:
        lat, lon = _REGION_GEO[region]
        return f"{lat},{lon}"
    return ""


def _relation_rows(args: dict) -> list[dict]:
    """Find the sole result_set relation argument and return its materialized rows. Fail loud if the
    caller sent no relation — an enrich command with nothing to enrich is a registration error."""
    for value in args.values():
        if isinstance(value, dict) and value.get("kind") == "result_set":
            return value.get("rows") or []
    raise ValueError(
        "EnrichRows expects a result_set relation argument (arg_kind: result_set); "
        f"got scalar-only args {sorted(args)}"
    )


async def _enrich_rows(request: bytes, _context) -> bytes:
    """Decode the request, read the input relation, return each row + embedding + geo as JSON bytes.

    Schema-agnostic: the original row is echoed back as a JSON string so the response contract is
    stable regardless of the input relation's columns."""
    payload = json.loads(request or b"{}")
    args = payload.get("args", {}) if isinstance(payload, dict) else {}
    rows = _relation_rows(args)
    enriched = [
        {
            "row": json.dumps(row, sort_keys=True),
            "embedding": json.dumps(_embedding(row)),
            "geo": _geo_marker(row),
        }
        for row in rows
    ]
    return json.dumps(enriched).encode()


class _DemoHandler(grpc.GenericRpcHandler):
    """Match a demo method path and dispatch it as a bytes-in/bytes-out unary RPC."""

    _METHODS = {_RANDOM_METHOD: _get_random_set, _ENRICH_METHOD: _enrich_rows}

    def service(self, handler_call_details):
        handler = self._METHODS.get(handler_call_details.method)
        if handler is None:
            return None
        return grpc.unary_unary_rpc_method_handler(
            handler, request_deserializer=_identity, response_serializer=_identity
        )


async def serve() -> None:
    port = os.environ.get("DEMO_GRPC_PORT", "50071")
    server = grpc.aio.server()
    server.add_generic_rpc_handlers((_DemoHandler(),))
    server.add_insecure_port(f"0.0.0.0:{port}")
    await server.start()
    print(
        f"demo gRPC server listening on 0.0.0.0:{port} ({_RANDOM_METHOD}, {_ENRICH_METHOD})",
        flush=True,
    )
    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(serve())
