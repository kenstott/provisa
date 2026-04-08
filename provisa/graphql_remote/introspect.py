# Copyright (c) 2026 Kenneth Stott
# Canary: d6a9b334-9335-43a2-97cd-e15b925d98ba
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GraphQL schema introspection (REQ-307)."""
from __future__ import annotations
import httpx

INTROSPECTION_QUERY = """
query IntrospectionQuery {
  __schema {
    queryType { name }
    mutationType { name }
    types {
      kind name
      fields(includeDeprecated: false) {
        name
        type { kind name ofType { kind name ofType { kind name } } }
        args {
          name
          type { kind name ofType { kind name ofType { kind name } } }
        }
      }
    }
  }
}
"""

def _build_headers(auth: dict | None) -> dict:
    if not auth:
        return {}
    auth_type = auth.get("type", "none")
    if auth_type == "bearer":
        return {"Authorization": f"Bearer {auth.get('token', '')}"}
    if auth_type == "basic":
        import base64
        creds = base64.b64encode(f"{auth.get('username','')}:{auth.get('password','')}".encode()).decode()
        return {"Authorization": f"Basic {creds}"}
    return {}

async def introspect_schema(url: str, auth: dict | None = None) -> dict:
    """POST introspection query; return parsed __schema dict.

    Raises httpx.HTTPError on network/HTTP failure.
    """
    headers = {"Content-Type": "application/json", **_build_headers(auth)}
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, json={"query": INTROSPECTION_QUERY}, headers=headers)
        resp.raise_for_status()
        data = resp.json()
    if "errors" in data:
        raise ValueError(f"Introspection errors: {data['errors']}")
    return data["data"]["__schema"]
