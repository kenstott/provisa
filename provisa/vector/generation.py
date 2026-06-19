# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Generated embedding columns (REQ-427).

A ``generated_from`` virtual embedding column is produced by embedding a source text
column with a registered model. Before generation runs, a sample of rows is validated:
the source values must be non-empty text and the produced vectors must match the
model's declared dimensions.
"""

from __future__ import annotations

from dataclasses import dataclass

from provisa.vector.query import validate_vector_dimensions
from provisa.vector.registry import VectorModel


class GenerationError(ValueError):
    """A generated embedding column is misconfigured or failed validation."""


@dataclass
class GeneratedEmbeddingSpec:
    """An embedding column generated from a source text column (REQ-427)."""

    target_column: str  # the embedding column name
    source_column: str  # the text column it is generated from
    model_id: str  # registered model (REQ-419)


def spec_from_column(column) -> GeneratedEmbeddingSpec | None:
    """Build a generation spec from a Column with embedding + embedding_source_column set."""
    if not getattr(column, "embedding", False) or not getattr(
        column, "embedding_source_column", None
    ):
        return None
    if not getattr(column, "embedding_model", None):
        raise GenerationError(
            f"embedding column {column.name!r} has no embedding_model (required to generate)"
        )
    return GeneratedEmbeddingSpec(
        target_column=column.name,
        source_column=column.embedding_source_column,
        model_id=column.embedding_model,
    )


async def validate_generation(
    sample_rows: list[dict],
    spec: GeneratedEmbeddingSpec,
    model: VectorModel,
    provider=None,
) -> None:
    """Validate generation against a sample (REQ-427).

    Every sampled source value must be non-empty text, and each produced vector must
    match the model's dimensions. Raises GenerationError otherwise.
    """
    if not sample_rows:
        return
    texts: list[str] = []
    for row in sample_rows:
        val = row.get(spec.source_column)
        if val is None or (isinstance(val, str) and not val.strip()):
            raise GenerationError(
                f"source column {spec.source_column!r} has empty/null values; cannot embed"
            )
        texts.append(str(val))

    if provider is None:
        from provisa.vector.providers import get_provider

        provider = get_provider(model.provider)
    vectors = await provider.embed(texts, model)
    if len(vectors) != len(texts):
        raise GenerationError("provider returned a different number of vectors than inputs")
    for vec in vectors:
        validate_vector_dimensions(vec, model)  # raises on dimension mismatch
