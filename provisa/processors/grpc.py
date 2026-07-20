# Copyright (c) 2026 Kenneth Stott
# Canary: 3a6b4c2f-7091-4e8b-8f52-1b7d6e4a92f8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""gRPC bidi-stream transport adapter (REQ-940).

Streams NDJSON-framed row messages over a bidirectional gRPC stream and reads NDJSON-framed row
messages back. The bidi stream itself is injected (``bidi``: an iterable of request chunks -> an
iterable of response chunks) so the adapter conforms to the one contract without hard-coding a stub
or a running server — a deployment supplies its hardened channel; a test supplies a fake echo stream.
Schema is validated at both ends like every other transport.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator

from provisa.processors.contract import Schema, TransportAdapter, validate_rows
from provisa.processors.framing import ndjson_decode, ndjson_encode

# A bidi stream: consume an iterable of request byte-chunks, yield response byte-chunks.
BidiStream = Callable[[Iterable[bytes]], Iterable[bytes]]


class GrpcAdapter(TransportAdapter):  # REQ-940
    """Run a processor over a gRPC bidirectional stream, NDJSON row messages both ways.

    One NDJSON-encoded row per stream message (the framing the shell/HTTP transports also use), so a
    processor author writes the same row transform regardless of transport."""

    def __init__(self, bidi: BidiStream) -> None:
        self._bidi = bidi

    def process(
        self, rows: Iterable[dict], *, schema_in: Schema, schema_out: Schema
    ) -> Iterator[dict]:
        request_chunks = ndjson_encode(validate_rows(rows, schema_in, where="grpc input"))
        response_chunks = self._bidi(request_chunks)
        out_rows = ndjson_decode(response_chunks)
        return validate_rows(out_rows, schema_out, where="grpc output")
