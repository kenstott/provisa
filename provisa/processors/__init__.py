# Copyright (c) 2026 Kenneth Stott
# Canary: e51c9d7a-2b48-4f36-8a0d-6c2f1e9b47a3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""External processor transport toolkit (REQ-940).

Extends the in-process ``preprocess(rows, ctx)`` hook (REQ-957) to EXTERNAL processors reached over
pluggable transports — shell (stdin/stdout), HTTP (streamed request/response), gRPC (bidi stream) —
all conforming to ONE streaming contract: a stream of schema-declared rows in, a stream of
schema-declared rows out, schema-validated at both ends. The toolkit owns framing (NDJSON default),
streaming, and schema validation; the processor author implements only the row transform.
"""

from __future__ import annotations

from provisa.processors.contract import (
    Field,
    Schema,
    SchemaViolation,
    TransportAdapter,
    validate_rows,
)
from provisa.processors.factory import build_adapter
from provisa.processors.framing import ndjson_decode, ndjson_encode
from provisa.processors.grpc import GrpcAdapter
from provisa.processors.http import HttpAdapter
from provisa.processors.shell import ProcessorError, ShellAdapter

__all__ = [
    "Field",
    "GrpcAdapter",
    "build_adapter",
    "HttpAdapter",
    "ProcessorError",
    "Schema",
    "SchemaViolation",
    "ShellAdapter",
    "TransportAdapter",
    "ndjson_decode",
    "ndjson_encode",
    "validate_rows",
]
