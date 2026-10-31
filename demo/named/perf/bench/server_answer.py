# Copyright (c) 2026 Kenneth Stott
# Canary: 4b488cd0-f8e1-4690-898e-30f76858a2f9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""An HTTP failure with the server's own answer in it. ``raise_for_status`` alone says only the
status line and a documentation link, so a report's ``first_error`` could not say why a request was
refused and the query had to be replayed by hand."""

from __future__ import annotations

from typing import Any

import httpx

BODY_LIMIT = 1000


def checked(resp: Any) -> Any:
    """``resp`` if it succeeded, else an ``HTTPStatusError`` whose message is the status and body.
    The exception still carries ``response``."""
    try:
        resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        answer = " ".join(resp.text.split())[:BODY_LIMIT]
        raise httpx.HTTPStatusError(
            f"HTTP {resp.status_code} from {exc.request.url.path}: {answer}",
            request=exc.request,
            response=resp,
        ) from None
    return resp
