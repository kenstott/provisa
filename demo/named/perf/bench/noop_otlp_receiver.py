#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 92017e19-db9e-4733-86d8-f7150001f5d1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""A no-op OTLP/HTTP receiver for benchmark runs.

In production the trace collector and the trace database run on other machines; on the single
benchmark VM they would share the 16 vCPUs with the server under test. This stands in for the
collector: it accepts OTLP traces, metrics and logs over HTTP (the protocol Provisa exports with,
``OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf``), reads and discards each body, and answers success.
Provisa's exporters still serialize and send every batch, so the server pays what it would pay in
production; nothing downstream of the wire runs here.

Usage: noop_otlp_receiver.py [port]   (default 4319, the port --demo points Provisa at)
"""

from __future__ import annotations

import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

_PATHS = ("/v1/traces", "/v1/metrics", "/v1/logs")


class _Discard(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # keep-alive: exporters reuse one connection

    def do_POST(self) -> None:  # noqa: N802 - http.server's handler naming
        length = int(self.headers.get("Content-Length") or 0)
        if length:
            self.rfile.read(length)
        if self.path not in _PATHS:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        # An empty Export*ServiceResponse: zero bytes of protobuf means full success.
        self.send_response(200)
        self.send_header("Content-Type", "application/x-protobuf")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - base signature
        """Silent: a per-request log line would be the receiver's main cost."""


def main() -> None:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 4319
    server = ThreadingHTTPServer(("127.0.0.1", port), _Discard)
    print(f"noop OTLP receiver listening on 127.0.0.1:{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
