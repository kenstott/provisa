# Copyright (c) 2026 Kenneth Stott
# Canary: 9d3f6b21-7c4e-4a85-b0d2-5e1a8c7f3b64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A forced redirect asked for over HTTP (REQ-1194, REQ-1224): the ``X-Provisa-Redirect`` headers.

``X-Provisa-Redirect: true`` asks for the result to be landed in the results store and answered
with its handle instead of its rows; ``X-Provisa-Redirect-Format`` names the file format (an
Accept-style value, parquet or orc), and ``X-Provisa-Redirect-Threshold`` may only lower the
operator's threshold (REQ-029). Every HTTP data surface without a request body of its own for the
option (JSON:API, Cypher over HTTP) reads them here; the pipeline's materialize stage does the rest.
"""

# Requirements: REQ-029, REQ-1194, REQ-1224

from __future__ import annotations

from collections.abc import Mapping

from provisa.api.errors import ApiError
from provisa.executor.redirect import (
    Delivery,
    RedirectFormatUnknown,
    delivery_from_request,
    parse_redirect_format,
)


def delivery_from_headers(headers: Mapping[str, str], role: str | None) -> Delivery | None:
    """The forced delivery the request's ``X-Provisa-Redirect*`` headers ask for, or None when it
    asks for none (the plan answers rows, or the threshold decides on a buffered transport). A
    format or threshold that cannot be read is refused (400), never replaced by a default."""
    fmt = headers.get("x-provisa-redirect-format")
    threshold = headers.get("x-provisa-redirect-threshold")
    try:
        redirect_format = parse_redirect_format(fmt) if fmt else None
    except RedirectFormatUnknown as exc:
        raise ApiError(400, "data.invalid_redirect_format", str(exc), format=exc.value) from exc
    if threshold is not None and not threshold.strip().isdigit():
        raise ApiError(
            400,
            "data.invalid_redirect_threshold",
            f"X-Provisa-Redirect-Threshold must be a whole number of rows, not {threshold!r}",
            threshold=threshold,
        )
    return delivery_from_request(
        force_redirect=headers.get("x-provisa-redirect", "").lower() == "true",
        redirect_format=redirect_format,
        threshold=int(threshold) if threshold is not None else None,
        role=role,
    )
