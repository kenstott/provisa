# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Native-tier vector query logic (REQ-423/429/430).

- cosine_similarity_sql: translate cosine_similarity(column, query_vector) to a native
  source expression (REQ-423).
- vectorize_text / resolve_query_vector: turn a text query into a vector via the
  column's locked model, or accept a raw vector (REQ-430).
- validate_vector_dimensions / resolve_query_vector: lock the model per column and
  reject mismatched dimensions (REQ-429).
"""

from __future__ import annotations

from provisa.vector.registry import VectorModel, VectorModelError

# Requirements: REQ-423, REQ-429, REQ-430, REQ-431


class VectorQueryError(ValueError):
    """A vector query is malformed, or has no native translation."""


def _fmt(x: float) -> str:
    return repr(float(x))


def _vector_literal(query_vector: list[float]) -> str:
    return "[" + ",".join(_fmt(x) for x in query_vector) + "]"


def cosine_similarity_sql(
    column: str, query_vector: list[float], capability: str | None
) -> str:  # REQ-423
    """Translate ``cosine_similarity(column, query_vector)`` to a native expression (REQ-423).

    Returns a SQL scalar in [-1, 1] (1 = identical). The fallback tier (B2) handles
    sources with no native vector capability.
    """
    if not query_vector:
        raise VectorQueryError("query_vector is empty")
    if capability == "pgvector":
        # pgvector's <=> is cosine distance; similarity = 1 - distance.
        return f"(1 - ({column} <=> '{_vector_literal(query_vector)}'::vector))"
    if capability == "cortex":
        lit = _vector_literal(query_vector)
        return f"VECTOR_COSINE_SIMILARITY({column}, {lit}::VECTOR(FLOAT, {len(query_vector)}))"
    if capability == "atlas_vector":
        raise VectorQueryError(
            "Atlas vector search is a $vectorSearch aggregation stage, not a SQL scalar"
        )
    raise VectorQueryError(
        f"no native cosine_similarity for capability {capability!r}; use the fallback tier"
    )


def validate_vector_dimensions(vector: list[float], model: VectorModel) -> None:  # REQ-429
    """Reject a query vector whose dimension does not match the model (REQ-429)."""
    if len(vector) != model.dimensions:
        raise VectorModelError(
            f"query vector has {len(vector)} dimensions; model {model.id!r} expects "
            f"{model.dimensions}"
        )


async def vectorize_text(text: str, model: VectorModel, provider=None) -> list[float]:  # REQ-430
    """Embed a text query into a vector using the model's provider (REQ-430)."""
    if provider is None:
        from provisa.vector.providers import get_provider

        provider = get_provider(model.provider)
    vecs = await provider.embed([text], model)
    if not vecs:
        raise VectorQueryError("provider returned no embedding")
    return vecs[0]


async def resolve_query_vector(
    query_input, model: VectorModel, provider=None
) -> list[float]:  # REQ-429, REQ-430
    """Resolve a similarity query input to a validated vector (REQ-429/430).

    ``query_input`` is either text (embedded via the column's locked ``model``) or an
    already-computed vector (validated against the model's dimensions). A raw vector
    that does not match the locked model's dimensions is rejected (model-locking).
    """
    if isinstance(query_input, str):
        vector = await vectorize_text(query_input, model, provider)
    elif isinstance(query_input, (list, tuple)):
        vector = [float(x) for x in query_input]
    else:
        raise VectorQueryError("query input must be text or a vector")
    validate_vector_dimensions(vector, model)
    return vector
