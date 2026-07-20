# Copyright (c) 2026 Kenneth Stott
# Canary: 295a3b1e-6f8c-4d7a-8e41-0a6c5d3f81e7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""HTTP transport adapter (REQ-940).

POSTs the NDJSON row stream as the request body and reads the NDJSON row stream from the response.
The HTTP round-trip is injectable (``poster``) so the adapter is unit-testable without a network and
so a deployment can supply its own hardened/streamed client. Schema is validated at both ends.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator

from provisa.processors.contract import Schema, TransportAdapter, validate_rows
from provisa.processors.framing import ndjson_decode, ndjson_encode
from provisa.processors.shell import ProcessorError

# A poster sends the NDJSON request body to the endpoint and returns the raw NDJSON response body.
Poster = Callable[[str, bytes], bytes]


def _default_poster(url: str, body: bytes) -> bytes:
    import urllib.request

    if not url.startswith(("http://", "https://")):
        raise ProcessorError(f"HTTP processor URL must be http(s): {url!r}")
    req = urllib.request.Request(  # noqa: S310 - scheme validated above
        url, data=body, method="POST", headers={"Content-Type": "application/x-ndjson"}
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:  # nosec B310 - scheme validated
            return resp.read()
    except OSError as exc:
        raise ProcessorError(f"HTTP processor {url!r} failed: {exc}") from exc


class HttpAdapter(TransportAdapter):  # REQ-940
    """Stream rows to an HTTP processor endpoint as NDJSON and read NDJSON rows back."""

    def __init__(self, url: str, *, poster: Poster | None = None) -> None:
        self._url = url
        self._poster = poster or _default_poster

    def process(
        self, rows: Iterable[dict], *, schema_in: Schema, schema_out: Schema
    ) -> Iterator[dict]:
        body = b"".join(ndjson_encode(validate_rows(rows, schema_in, where="http input")))
        response = self._poster(self._url, body)
        out_rows = ndjson_decode(response.splitlines())
        return validate_rows(out_rows, schema_out, where="http output")
