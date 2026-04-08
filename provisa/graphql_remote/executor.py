# Copyright (c) 2026 Kenneth Stott
# Canary: f912261d-dbda-4025-83cd-f63d504ab859
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Execute queries against a remote GraphQL endpoint (REQ-309)."""
from __future__ import annotations
import httpx
from provisa.graphql_remote.introspect import _build_headers

async def execute_remote(
    url: str,
    auth: dict | None,
    field_name: str,
    columns: list[str],
    variables: dict | None = None,
) -> list[dict]:
    """Build a minimal GraphQL query and forward to the remote endpoint.

    Returns list of row dicts from data.<field_name>.
    Raises httpx.HTTPError on network failure.
    Raises ValueError if the response contains errors.
    """
    col_selection = "\n".join(columns) if columns else "__typename"
    query = f"query {{ {field_name} {{ {col_selection} }} }}"
    headers = {"Content-Type": "application/json", **_build_headers(auth)}
    payload: dict = {"query": query}
    if variables:
        payload["variables"] = variables

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, json=payload, headers=headers)
        resp.raise_for_status()
        data = resp.json()

    if "errors" in data:
        raise ValueError(f"Remote GraphQL errors: {data['errors']}")

    rows = data.get("data", {}).get(field_name, [])
    return rows if isinstance(rows, list) else [rows]
