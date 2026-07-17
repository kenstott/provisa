# Copyright (c) 2026 Kenneth Stott
# Canary: e3f0fdb5-31f7-42af-9f68-cde7698f96f0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Native vector support probe for source descriptors (REQ-422)."""

from __future__ import annotations

from provisa.vector.capability import supports_native_vectors

_FEATURE_FLAGS: dict[str, str] = {
    "postgresql": "pgvector",
    "mongodb": "atlas_vector_search",
    "snowflake": "cortex",
}


def check_native_vector_support(source: dict) -> bool:
    """Return True when the source descriptor supports native vector search.

    For feature-gated sources (mongodb Atlas Vector Search, Snowflake Cortex)
    the feature flag must also be present in source["features"].
    For postgresql, type-level capability (pgvector extension presence) is
    assumed when the source_type is postgresql — runtime probing is async and
    handled separately via ``capability.has_pgvector``.
    """
    source_type: str = source.get("source_type", "")
    if not supports_native_vectors(source_type):
        return False

    required_flag = _FEATURE_FLAGS.get(source_type)
    if required_flag is None:
        return True

    features: list[str] = source.get("features", [])
    # postgresql: presence of source_type alone is sufficient at type-check time.
    if source_type == "postgresql":
        return True
    # Other sources require an explicit feature flag.
    return required_flag in features or any(
        f in features for f in (required_flag, required_flag.split("_")[0])
    )
