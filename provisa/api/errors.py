# Copyright (c) 2026 Kenneth Stott
# Canary: f3e198a1-7787-47f7-a161-6e49de339e92
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Hybrid server-side i18n for API errors (REQ-1350).

The server never translates. Every user-facing error carries a stable
machine code plus interpolation params alongside the English message; the
UI maps the code to its own i18n catalog (``serverErrors.<code>``) and
falls back to the English ``detail`` when no code is present — so
unmigrated ``HTTPException`` sites keep working unchanged.

Wire format (registered handler in ``provisa.api.app``)::

    {"detail": "<English message>", "code": "<area.snake_code>", "params": {...}}

Usage::

    raise ApiError(404, "auth.invite_not_found", "Invite not found")
    raise ApiError(400, "schema.unknown_source_type",
                   f"Unknown source type: {req.type}", type=req.type)

Param values must be JSON-serializable; param names must match the
``{{name}}`` placeholders in the catalog template for the code.
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException


class ApiError(HTTPException):
    """HTTPException with a stable i18n code and interpolation params."""

    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        headers: dict[str, str] | None = None,
        **params: Any,
    ) -> None:
        super().__init__(status_code=status_code, detail=message, headers=headers)
        self.code = code
        self.params = params
