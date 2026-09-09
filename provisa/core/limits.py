# Copyright (c) 2026 Kenneth Stott
# Canary: 8d1e6b3f-2a9c-4f7d-b5e8-3c6a9d0f1e42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Resolved server limits, published by the API layer at config load (REQ-1678).

``provisa.compiler.sql_gen`` reads the default row cap from here rather than from ``api.app``
state, which is what keeps the compiler off the API import path. The API layer is the only writer.
"""

# Requirements: REQ-005, REQ-1678

from __future__ import annotations

import os

_server_limits: dict = {}


def set_server_limits(limits: dict) -> None:
    """Publish the resolved limits (app_loaders does this once per config load)."""
    global _server_limits
    _server_limits = dict(limits)


def server_limits() -> dict:
    return _server_limits


def default_row_limit() -> int:
    """The hard cap on rows returned when the caller supplies no explicit LIMIT.

    ``PROVISA_DEFAULT_ROW_LIMIT`` overrides the configured value; the configured value is
    ``server.limits.default_row_limit`` (100 when the config sets none — the config schema's own
    default, mirrored from app_loaders).
    """
    return int(
        os.environ.get(
            "PROVISA_DEFAULT_ROW_LIMIT", str(_server_limits.get("default_row_limit", 100))
        )
    )
