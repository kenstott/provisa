# Copyright (c) 2026 Kenneth Stott
# Canary: 3e7b9d14-6a2c-4f85-b0d1-9c4e2a7f6b13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1634: OpenMetadata's answers to a data product's member-asset calls, for a test whose
fixture declares the product its tables publish through (a table publishes only as a member)."""

from __future__ import annotations

import httpx


def product_assets_response(method: str, url: str) -> httpx.Response | None:
    """The product holds no assets yet and accepts every one added; None for any other call."""
    request = httpx.Request(method, url)
    if method == "GET" and "/api/v1/dataProducts/name/" in url and url.endswith("/assets"):
        return httpx.Response(200, json={"data": []}, request=request)
    if method == "PUT" and url.endswith(("/assets/add", "/assets/remove")):
        return httpx.Response(200, json={"status": "success"}, request=request)
    return None
