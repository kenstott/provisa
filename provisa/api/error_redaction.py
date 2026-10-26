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

import re
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
                body = _redacted(body, (scope.get("state") or {}).get("active_org_id"))
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


def _redacted(body: bytes, org_id: str | None = None) -> bytes:
    from provisa.security.redaction import redact

    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        return body  # not text: an error body is text, and a binary one carries no message
    return redact(model_named(text, org_id)).encode("utf-8")


#: An engine's excerpt of the statement it ran: ``LINE n: ...`` and the caret line under it.
_EXCERPT = re.compile(r"\n+LINE \d+:[^\n]*(?:\n[ \t]*\^)?")
#: The same inside a JSON string: newlines escaped, and the string ending at an unescaped quote.
_EXCERPT_JSON = re.compile(r"(?:\\n)+LINE \d+:.*?(?:\\n[ \t]*\^|(?=(?<!\\)\"))")


def model_named(text: str, org_id: str | None) -> str:
    """``text`` with each physical address of the org's tables (its source's catalog, schema and
    table, as a statement is lowered to it and an engine's error quotes it) written as the model
    names the table (REQ-2152): a physical address is source configuration, and the caller wrote
    model names. An org with no model built names nothing, and ``text`` is answered as it is."""
    from provisa.api.app import state
    from provisa.compiler.sql_rewrite import semantic_ref

    # An engine's excerpt of the statement it ran (DuckDB's ``LINE n:`` and its caret line) is of
    # the physical SQL: truncated, it can begin inside a physical address, and its caret counts
    # characters of SQL the caller never wrote. It is dropped -- in plain text and in a JSON body,
    # where its newlines are escaped.
    text = _EXCERPT.sub("", _EXCERPT_JSON.sub("", text))
    runtime = state.org_registry.get(org_id) if org_id is not None else None
    if runtime is None:
        return text
    named: dict[str, str] = {}
    for ctx in runtime.contexts.values():
        for meta in ctx.tables.values():
            model = semantic_ref(meta)  # "domain"."table"
            parts = (meta.catalog_name, meta.schema_name, meta.table_name)
            named[".".join(f'"{p}"' for p in parts)] = model
            # As a JSON error body carries it, its quotes escaped.
            named[".".join(f'\\"{p}\\"' for p in parts)] = model.replace('"', '\\"')
            named[".".join(parts)] = model
    if not named:
        return text
    found = re.compile(
        r"(?<![\w.])(?:"
        + "|".join(re.escape(k) for k in sorted(named, key=len, reverse=True))
        + r")(?![\w])"
    )
    return found.sub(lambda m: named[m.group(0)], text)
