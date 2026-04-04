# Copyright (c) 2025 Kenneth Stott
# Canary: c647cbda-2f37-4db6-80a7-4de973bcc716
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Webhook executor — HTTP POST with timeout and response mapping.

Sends arguments as JSON body, maps response to GraphQL return type fields.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from provisa.core.models import Webhook


@dataclass
class WebhookResult:
    """Result of a webhook execution."""

    status_code: int
    data: Any
    headers: dict[str, str]


async def execute_webhook(
    webhook: Webhook,
    arguments: dict[str, Any],
) -> WebhookResult:
    """Execute a webhook HTTP call.

    Args:
        webhook: Webhook config model
        arguments: resolved argument name→value pairs

    Returns:
        WebhookResult with status, parsed JSON data, and response headers

    Raises:
        httpx.TimeoutException: if the request exceeds timeout_ms
        httpx.HTTPStatusError: if response indicates an error (4xx/5xx)
    """
    timeout = httpx.Timeout(webhook.timeout_ms / 1000.0)

    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.request(
            method=webhook.method,
            url=webhook.url,
            json=arguments,
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()

        data = response.json()

    return WebhookResult(
        status_code=response.status_code,
        data=data,
        headers=dict(response.headers),
    )


def map_response_to_return_type(
    data: Any,
    inline_fields: list[dict[str, str]] | None = None,
) -> Any:
    """Map webhook JSON response to the expected GraphQL return shape.

    If inline_fields is provided, filters response dict to only include
    those fields. If data is a list, maps each element.
    """
    if inline_fields is None:
        return data

    field_names = {f["name"] for f in inline_fields}

    if isinstance(data, list):
        return [
            {k: v for k, v in item.items() if k in field_names}
            for item in data
            if isinstance(item, dict)
        ]

    if isinstance(data, dict):
        return {k: v for k, v in data.items() if k in field_names}

    return data
