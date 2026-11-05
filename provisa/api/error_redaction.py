# Copyright (c) 2026 Kenneth Stott
# Canary: f891b20e-996c-4bf3-8bef-815df6fa717c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every HTTP error response's body passes central redaction (REQ-2087).

The outermost middleware: whatever answered with a status of 400 or more -- an ApiError, a
typed handler, FastAPI's own HTTPException handler, the server-error handler -- has its body
redacted before it is sent. A successful response is passed through untouched and unbuffered."""

# Requirements: REQ-2087

from __future__ import annotations

from typing import Any

#: Error bodies are small; one past this is passed through as it is, in the chunks it came in.
_MAX_BUFFERED = 1 << 20


class ErrorRedactionMiddleware:
    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        start: dict | None = None
        chunks: list[bytes] = []
        passing = False

        async def _send(message: dict) -> None:
            nonlocal start, passing
            if message["type"] == "http.response.start":
                if message.get("status", 200) < 400:
                    passing = True
                    await send(message)
                    return
                start = message
                return
            if passing or message["type"] != "http.response.body":
                await send(message)
                return
            chunks.append(message.get("body", b""))
            if message.get("more_body", False) and sum(map(len, chunks)) <= _MAX_BUFFERED:
                return
            assert start is not None
            body = b"".join(chunks)
            if not message.get("more_body", False):
                # The org the request acted in, as the auth middleware bound it on its state.
                body = _redacted(
                    body,
                    (scope.get("state") or {}).get("active_org_id"),
                    json_body=_is_json(start),
                )
            headers = [
                (k, v) for k, v in start.get("headers", []) if k.lower() != b"content-length"
            ]
            if not message.get("more_body", False):
                headers.append((b"content-length", str(len(body)).encode()))
            await send({**start, "headers": headers})
            await send(
                {
                    "type": "http.response.body",
                    "body": body,
                    "more_body": message.get("more_body", False),
                }
            )
            passing = True

        await self.app(scope, receive, _send)


def _is_json(start: dict) -> bool:
    """Whether the response ``start`` begins carries a JSON body, by its content type."""
    for key, value in start.get("headers", []):
        if key.lower() == b"content-type":
            return value.split(b";")[0].strip().lower() == b"application/json"
    return False


def _told(value: Any, org_id: str | None) -> Any:
    """Each string in a decoded JSON value as a client is told it, the value's shape kept."""
    from provisa.api.engine_error_names import client_text

    if isinstance(value, str):
        return client_text(value, org_id)
    if isinstance(value, list):
        return [_told(v, org_id) for v in value]
    if isinstance(value, dict):
        return {k: _told(v, org_id) for k, v in value.items()}
    return value


def _redacted(body: bytes, org_id: str | None, *, json_body: bool) -> bytes:
    """``body`` as a client is told it. A JSON body has each of its strings rewritten and is
    written again as JSON: rewritten as text, a name the model quotes would end the string it is
    written into, and the body would no longer be JSON."""
    import json

    from provisa.api.engine_error_names import client_text

    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        return body  # not text: an error body is text, and a binary one carries no message
    if json_body:
        # As Starlette's JSONResponse writes a body.
        return json.dumps(
            _told(json.loads(text), org_id), ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
    return client_text(text, org_id).encode("utf-8")
