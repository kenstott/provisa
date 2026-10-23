# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An error as the app's own handler answers it, for a batch answered one result per statement:
a statement's error says what the same statement sent alone would have been answered with."""

from __future__ import annotations

import inspect
import json

from fastapi import Request


async def error_answer(request: Request, exc: Exception) -> dict:
    """``{"status": ..., "body": ...}`` as the handler the app registered for ``exc``'s type (or
    the nearest base it has one for) answers it. An error the app has no handler for is raised,
    as it would have been for the statement alone."""
    handlers = request.app.exception_handlers
    for cls in type(exc).__mro__:
        if cls in handlers:
            response = handlers[cls](request, exc)
            if inspect.isawaitable(response):
                response = await response
            return {"status": response.status_code, "body": json.loads(bytes(response.body))}
    raise exc
