# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Source vector-capability detection (REQ-422).

Determines whether a source can do native vector similarity search, so the compiler
can push similarity down to the source (native tier) or fall back to a pgvector cache
(fallback tier). Type-based capability + a runtime pgvector-extension probe.
"""

from __future__ import annotations

# Source type → native vector capability. None elsewhere → fallback tier.
_VECTOR_CAPABILITY: dict[str, str] = {
    "postgresql": "pgvector",  # requires the `vector` extension (probe at runtime)
    "mongodb": "atlas_vector",  # Atlas Vector Search
    "snowflake": "cortex",  # Snowflake Cortex VECTOR functions
}


def native_vector_capability(source_type: str) -> str | None:
    """Return the native vector capability name for a source type, or None."""
    return _VECTOR_CAPABILITY.get(source_type)


def supports_native_vectors(source_type: str) -> bool:
    return source_type in _VECTOR_CAPABILITY


async def has_pgvector(conn) -> bool:
    """Probe a Postgres connection for the pgvector extension (REQ-422).

    ``conn`` is an asyncpg connection. Native pgvector search is only usable when the
    extension is actually installed, regardless of the type-based capability.
    """
    row = await conn.fetchval("SELECT 1 FROM pg_extension WHERE extname = 'vector'")
    return row is not None
