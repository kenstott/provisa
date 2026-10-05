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


class RowLevelFilterNotPushable(ApiError):
    """REQ-1915: a row-level table was read through a filter its source cannot apply itself.

    A row-level table is never fetched whole, so a filter the source cannot evaluate natively is
    refused by name -- naming the table, the source type and why -- rather than fetching every row
    to filter them here."""

    def __init__(self, table: str, source_type: str, why: str) -> None:
        super().__init__(
            400,
            "data.row_level_filter_not_pushable",
            f"Table {table!r} is replicated row by row and its source ({source_type}) cannot "
            f"apply this filter itself: {why}. Filter on its key instead.",
            table=table,
            source_type=source_type,
            reason=why,
        )

    def __str__(self) -> str:
        return str(self.detail)


class RowLevelFilterRequired(ApiError):
    """REQ-1915: a statement read a table replicated ROW BY ROW with no filter on it.

    Such a table is replicated row by row because it is large, so it is never read whole: a read
    must filter it (any predicate on its own columns, or a join that supplies its key), and the
    rows the filter selects are fetched from the source. A read with no filter is refused at
    planning, on every surface, never answered from whatever rows happen to be replicated and
    never the trigger of a whole-table copy. ``params`` carry the table. ``str()`` is the plain
    message, so the wire protocols that report an error as text (pgwire, Bolt, Flight, gRPC) say
    the same thing HTTP does."""

    def __init__(self, table: str) -> None:
        super().__init__(
            400,
            "data.row_level_filter_required",
            f"Table {table!r} is replicated row by row because it is large, so it is not read "
            "whole: add a filter on its columns, or join it on its key to a relation that is "
            "filtered.",
            table=table,
        )

    def __str__(self) -> str:
        return str(self.detail)


def timeout_error(exc: TimeoutError) -> ApiError:
    """The 504 an HTTP surface reports for a statement that timed out (REQ-1905). One that outran
    its request timeout is ``data.query_timeout`` with the timeout, transport and setting as
    params; any other (the server is stopping, a driver's own timeout) has no timeout to name and
    is ``data.request_interrupted`` carrying its message."""
    from provisa.core.request_deadline import RequestTimedOut

    if isinstance(exc, RequestTimedOut):
        return ApiError(
            504,
            "data.query_timeout",
            str(exc),
            timeout_s=f"{exc.timeout_s:g}",
            transport=exc.transport,
            setting=exc.setting,
        )
    return ApiError(504, "data.request_interrupted", str(exc), error=str(exc))
