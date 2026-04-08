# Copyright (c) 2026 Kenneth Stott
# Canary: 5687a87d-96b7-4fdf-89ec-72a21a0ec12f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Source adapter for graphql_remote sources (REQ-309, REQ-310)."""
from __future__ import annotations
import hashlib
import json
import logging

log = logging.getLogger(__name__)

async def fetch_rows(
    source_id: str,
    field_name: str,
    url: str,
    auth: dict | None,
    columns: list[str],
    cache_store,
    ttl: int = 300,
) -> list[dict]:
    """Fetch rows from a remote GraphQL source with cache-aside pattern.

    Cache key: graphql_remote:{source_id}:{field_name}:{col_hash}
    """
    col_hash = hashlib.sha256(json.dumps(sorted(columns)).encode()).hexdigest()[:12]
    cache_key = f"graphql_remote:{source_id}:{field_name}:{col_hash}"

    cached = await cache_store.get(cache_key)
    if cached is not None:
        log.debug("Cache hit for %s", cache_key)
        return json.loads(cached)

    from provisa.graphql_remote.executor import execute_remote
    rows = await execute_remote(url=url, auth=auth, field_name=field_name, columns=columns)

    await cache_store.set(cache_key, json.dumps(rows), ttl=ttl)
    return rows
