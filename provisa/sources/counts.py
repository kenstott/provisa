# Copyright (c) 2026 Kenneth Stott
# Canary: 5937f114-da06-4000-8553-f09728344f68
"""Graph-counts helper for GQL remote sources — REQ-673."""

from __future__ import annotations


from provisa.sources.gql import GQLRemoteSource


async def graph_counts(
    source: GQLRemoteSource,
    cache_warm: bool = False,
) -> dict | None:
    """Return node counts for a GQL remote source.

    When *cache_warm* is True and a local the engine cache is available, counts
    are read from cache (not implemented here — returns None).  When False
    and a ``count_query`` is configured, the remote GraphQL API is queried.
    """
    if cache_warm:
        return None

    config = source.config
    if not config.count_query:
        return None

    response = await source.http_client.post(
        config.endpoint,
        json={"query": config.count_query},
        headers={"Content-Type": "application/json"},
    )
    body = response.json()
    data = body.get("data", {})
    # Flatten: {source_name: first integer value found in data}
    for _key, val in data.items():
        if isinstance(val, dict):
            for inner_val in val.values():
                if isinstance(inner_val, int):
                    return {config.name: inner_val}
        if isinstance(val, int):
            return {config.name: val}
    return data or None
