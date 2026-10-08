# Copyright (c) 2026 Kenneth Stott
# Canary: d094ed11-7b68-4454-b831-4ece78f13314
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a subscriber is told when its stream ends on a failure.

A stream that has opened has already answered 200, so a failure after that cannot be an HTTP
status. It is one last frame, and then the stream closes: a subscriber is never left with a
silent end.

The frame says what the HTTP error handlers (``provisa/api/app.py``) say for the same exception
on a request, no more: a refusal that carries a code says its message, code and params; a
deadline says the request timed out; anything unexpected says "Internal server error" and the
exception's type, and its message and traceback go to the server log -- under the
subscription's id, which the frame also carries, so the two can be matched.

- The table subscription (``/data/subscribe/{table}``) sends ``event: stream_error`` with that
  body as its data. Not ``error``: an ``EventSource`` raises its own ``error`` event when a
  connection drops, and a listener could not tell the two apart.
- A GraphQL subscription keeps the GraphQL response shape:
  ``{"errors": [{"message": ..., "extensions": {...}}]}``."""

from __future__ import annotations

import json
import logging
import uuid

log = logging.getLogger(__name__)

STREAM_ERROR_EVENT = "stream_error"


def new_subscription_id() -> str:
    """An id for one subscription: in its final frame and beside its failure in the log."""
    return uuid.uuid4().hex[:16]


def failure_body(exc: BaseException) -> dict:
    """What the HTTP error handlers answer for ``exc``, as a body."""
    from provisa.api.errors import ApiError

    if isinstance(exc, ApiError):
        return {"detail": exc.detail, "code": exc.code, "params": exc.params}
    code, params = getattr(exc, "code", None), getattr(exc, "params", None)
    if isinstance(code, str) and isinstance(params, dict):
        # A refusal raised below the HTTP layer that names itself (a schema registry's, a
        # region's): its own message, code and params.
        return {"detail": str(exc), "code": code, "params": params}
    if isinstance(exc, TimeoutError):
        return {"detail": "Request timed out"}
    return {"detail": "Internal server error", "type": type(exc).__name__}


def _logged(exc: BaseException, subscription_id: str, what: str) -> dict:
    body = failure_body(exc)
    if "code" in body:
        log.info("subscription %s (%s) ended: %s [%s]", subscription_id, what, exc, body["code"])
    else:
        log.error(
            "subscription %s (%s) ended on an unexpected failure",
            subscription_id,
            what,
            exc_info=(type(exc), exc, exc.__traceback__),
        )
    return {**body, "subscription_id": subscription_id}


def table_stream_error(exc: BaseException, subscription_id: str, what: str) -> str:
    """The table subscription's last frame for ``exc``."""
    body = _logged(exc, subscription_id, what)
    return f"event: {STREAM_ERROR_EVENT}\ndata: {json.dumps(body, default=str)}\n\n"


def graphql_stream_error(exc: BaseException, subscription_id: str, what: str) -> str:
    """A GraphQL subscription's frame for ``exc``: the GraphQL ``errors`` shape, the code and
    params under ``extensions``."""
    body = _logged(exc, subscription_id, what)
    message = body.pop("detail")
    return f"data: {json.dumps({'errors': [{'message': message, 'extensions': body}]}, default=str)}\n\n"


async def ending_by_name(chunks, subscription_id: str, what: str, frame=table_stream_error):
    """``chunks``, and when they end on a failure, one last frame saying why."""
    try:
        async for chunk in chunks:
            yield chunk
    except Exception as exc:
        yield frame(exc, subscription_id, what)
