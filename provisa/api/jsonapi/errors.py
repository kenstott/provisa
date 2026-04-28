# Copyright (c) 2026 Kenneth Stott
# Canary: ba21c365-8832-4750-80cb-403f948bed18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""JSON:API error object formatting."""

from __future__ import annotations

from typing import Any


def jsonapi_error(
    status: int | str,
    title: str,
    detail: str | None = None,
    source_pointer: str | None = None,
    source_parameter: str | None = None,
) -> dict[str, Any]:
    """Build a single JSON:API error object."""
    err: dict[str, Any] = {
        "status": str(status),
        "title": title,
    }
    if detail is not None:
        err["detail"] = detail
    if source_pointer or source_parameter:
        source: dict[str, str] = {}
        if source_pointer:
            source["pointer"] = source_pointer
        if source_parameter:
            source["parameter"] = source_parameter
        err["source"] = source
    return err


def error_response(errors: list[dict[str, Any]]) -> dict[str, Any]:
    """Wrap error objects in a JSON:API error response envelope."""
    return {"errors": errors}
