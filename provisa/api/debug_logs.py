# Copyright (c) 2026 Kenneth Stott
# Canary: 5b1e8d27-c6a3-4f09-9d72-e41a0b3f8c65
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``/debug/logs``: the process's recent routing and auth diagnostics, read back.

The buffer is the server process's, shared by every organization it serves, and its records carry
no organization. It is deployment infrastructure, so it is the platform plane's (REQ-1327: "a
platform route requires the deployment check (platform settings and cross-org)"): a platform
administrator reads it; an organization's members and administrators, a single-tenant org
administrator included, do not.
"""

# Requirements: REQ-1327, REQ-1913, REQ-2087

from __future__ import annotations

import logging
from collections import deque

from fastapi import APIRouter, Request

from provisa.api.admin._platform_guard import require_deployment_settings
from provisa.api.debug_log_filter import lines_matching


class DebugLogBufferHandler(logging.Handler):
    """In-memory ring buffer of routing/auth diagnostics, read back via /debug/logs."""

    def __init__(self, capacity: int = 5000) -> None:
        super().__init__()
        self.buffer: deque[str] = deque(maxlen=capacity)

    def emit(self, record: logging.LogRecord) -> None:
        # Routing/auth diagnostics only -- apscheduler/trino background chatter fires every
        # few seconds and would otherwise evict these entries long before anyone reads them.
        if not record.name.startswith("provisa.api."):
            return
        self.buffer.append(self.format(record))


router = APIRouter()


@router.get("/debug/logs")
async def debug_logs(request: Request, pattern: str | None = None, limit: int = 100):
    """Return recent logs, optionally filtered by pattern. A platform administrator's."""
    require_deployment_settings(request)
    buffer_handler = next(
        (h for h in logging.getLogger().handlers if isinstance(h, DebugLogBufferHandler)), None
    )
    if buffer_handler is None:
        return {"error": "log buffer not initialized", "logs": []}
    logs = list(buffer_handler.buffer)
    if pattern:
        logs = lines_matching(pattern, logs)  # bounded: the pattern is the client's
    return {"count": len(logs), "logs": logs[-limit:]}
