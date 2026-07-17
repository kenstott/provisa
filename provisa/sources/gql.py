# Copyright (c) 2026 Kenneth Stott
# Canary: 02c2cc05-41ba-49e2-97dd-8e39d9e941e2
"""GQL remote source config and client — REQ-673."""

from __future__ import annotations

from dataclasses import dataclass, field

import httpx


@dataclass
class GQLSourceConfig:
    """Configuration for a GQL remote source."""

    name: str
    endpoint: str
    count_query: str | None = None


@dataclass
class GQLRemoteSource:
    """A configured GQL remote source with an HTTP client."""

    config: GQLSourceConfig
    http_client: httpx.AsyncClient = field(default_factory=httpx.AsyncClient)
