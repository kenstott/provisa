# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""Step definitions for REQ-427 / REQ-428 — Generated embedding columns and
scheduled incremental refresh (Vector Search) — plus REQ-430 query-time
vectorization."""

from __future__ import annotations

import types

import pytest
import pytest_asyncio
from pytest_bdd import given, when, then, parsers, scenarios

from provisa.vector.generation import (
    GeneratedEmbeddingSpec,
    GenerationError,
    spec_from_column,
    validate_generation,
)
from provisa.vector.registry import VectorModel
from provisa.vector.scheduled_refresh import (
    RefreshMode,
    RefreshPlan,
    needs_full_rebuild,
    plan_refresh,
)

scenarios("../features/REQ-427.feature")
scenarios("../features/REQ-428.feature")
scenarios("../features/REQ-430.feature")


@pytest.fixture
def shared_data() -> dict:
    return {}


class _FakeProvider:
    """Deterministic embedding provider that returns one vector per input text."""

    def __init__(self, dimensions: int):
        self._dimensions = dimensions

    async def embed(self, texts, model):
        return [[float(i)] * self._dimensions for i, _ in enumerate(texts)]


@given("a virtual embedding column declared with a generated_from subquery")
def declare_generated_column(shared_data):
    model = VectorModel(id="embed-model", provider="openai", dimensions=3)
    column = types.SimpleNamespace(
        name="content_embedding",
        embedding=True,
        embedding_source_column="content_text",
        embedding_model="embed-model",
    )
    # A subquery that joins/transforms multiple columns into one text value per row.
    sample_rows = [
        {"content_text": "the quick brown fox"},
        {"content_text": "lorem ipsum dolor"},
        {"content_text": "provisa vector search"},
    ]
    shared_data["column"] = column
    shared_data["model"] = model
    shared_data["sample_rows"] = sample_rows
    shared_data["provider"] = _FakeProvider(model.dimensions)

    assert column.embedding is True
    assert column.embedding_source_column == "content_text"


@when("the column is declared")
def build_generation_spec(shared_data):
    spec = spec_from_column(shared_data["column"])
    assert isinstance(spec, GeneratedEmbeddingSpec)
    assert spec.target_column == "content_embedding"
    assert spec.source_column == "content_text"
    assert spec.model_id == "embed-model"
    shared_data["spec"] = spec


@then(
    "Provisa validates the subquery returns exactly one text value per row "
    "against a sample row"
)
@pytest.mark.asyncio(loop_scope="session")
async def validate_subquery_against_sample(shared_data):
    spec = shared_data["spec"]
    model = shared_data["model"]
    provider = shared_data["provider"]
    sample_rows = shared_data["sample_rows"]

    # Valid sample: one text value per row -> validation passes.
    await validate_generation(sample_rows, spec, model, provider=provider)

    # A null/empty source value (subquery returning no text for a row) must fail.
    bad_rows = [{"content_text": "ok"}, {"content_text": ""}]
    with pytest.raises(GenerationError, match="empty/null"):
        await validate_generation(bad_rows, spec, model, provider=provider)

    # A provider that returns the wrong number of vectors (not exactly one per row) fails.
    class _MismatchProvider:
        async def embed(self, texts, model):
            return [[0.0] * model.dimensions]  # one vector for many inputs

    with pytest.raises(GenerationError, match="different number of vectors"):
        await validate_generation(
            sample_rows, spec, model, provider=_MismatchProvider()
        )

    # A produced vector with wrong dimensions fails dimension validation.
    class _WrongDimProvider:
        async def embed(self, texts, model):
            return [[0.0] * (model.dimensions + 1) for _ in texts]

    with pytest.raises(Exception):
        await validate_generation(
            sample_rows, spec, model, provider=_WrongDimProvider()
        )


# --- REQ-428: scheduled incremental refresh of generated embedding columns ---


@given("a generated embedding column with changed source rows")
def generated_column_with_changed_rows(shared_data):
    # The column was last embedded with a known model/dimension combination.
    shared_data["old_model_id"] = "embed-model"
    shared_data["new_model_id"] = "embed-model"
    shared_data["old_dimensions"] = 3
    shared_data["new_dimensions"] = 3
    shared_data["schema_changed"] = False

    # Source rows whose underlying data changed since the last embedding run.
    changed_pks = [101, 205, 309]
    shared_data["changed_pks"] = changed_pks

    assert changed_pks, "scenario requires at least one changed source row"


@when("the scheduled incremental refresh runs")
def run_scheduled_refresh(shared_data):
    # Plan the refresh for the steady-state case (same model/schema, only data changed).
    incremental_plan = plan_refresh(
        changed_pks=shared_data["changed_pks"],
        new_model_id=shared_data["new_model_id"],
        new_dimensions=shared_data["new_dimensions"],
        old_model_id=shared_data["old_model_id"],
        old_dimensions=shared_data["old_dimensions"],
        schema_changed=shared_data["schema_changed"],
    )
    shared_data["incremental_plan"] = incremental_plan

    # Plan the refresh under a model change (new model id, different dimensions).
    model_change_plan = plan_refresh(
        changed_pks=shared_data["changed_pks"],
        new_model_id="embed-model-v2",
        new_dimensions=1536,
        old_model_id=shared_data["old_model_id"],
        old_dimensions=shared_data["old_dimensions"],
        schema_changed=False,
    )
    shared_data["model_change_plan"] = model_change_plan

    # Plan the refresh under a schema change affecting the generating subquery.
    schema_change_plan = plan_refresh(
        changed_pks=shared_data["changed_pks"],
        new_model_id=shared_data["new_model_id"],
        new_dimensions=shared_data["new_dimensions"],
        old_model_id=shared_data["old_model_id"],
        old_dimensions=shared_data["old_dimensions"],
        schema_changed=True,
    )
    shared_data["schema_change_plan"] = schema_change_plan


@then(
    "only changed rows are re-embedded; a model or schema change triggers a "
    "full rebuild"
)
def assert_refresh_behaviour(shared_data):
    # Steady state: incremental refresh re-embeds only the changed rows.
    incremental = shared_data["incremental_plan"]
    assert isinstance(incremental, RefreshPlan)
    assert incremental.mode is RefreshMode.INCREMENTAL
    assert incremental.pks == shared_data["changed_pks"]
    assert incremental.reason == "changed rows"

    # A model/dimension change must trigger a full rebuild (existing vectors invalid).
    model_change = shared_data["model_change_plan"]
    assert model_change.mode is RefreshMode.FULL
    assert model_change.pks == []
    assert model_change.reason == "model/dimension change"
    assert (
        needs_full_rebuild("embed-model", "embed-model-v2", 3, 1536, False) is True
    )

    # A schema change affecting the generating subquery must trigger a full rebuild.
    schema_change = shared_data["schema_change_plan"]
    assert schema_change.mode is RefreshMode.FULL
    assert schema_change.pks == []
    assert schema_change.reason == "schema change"
    assert needs_full_rebuild("embed-model", "embed-model", 3, 3, True) is True

    # Sanity: unchanged model/dimension/schema does not require a full rebuild.
    assert needs_full_rebuild("embed-model", "embed-model", 3, 3, False) is False


# --- REQ-430: query-time vectorization (text or raw vector input) ---


class _RecordingProvider:
    """Embedding provider that records every embed() invocation.

    Used to prove that a text-string similarity search triggers a real call to
    the declared embedding model before the search runs, while a raw-vector
    search does not.
    """

    def __init__(self, dimensions: int):
        self._dimensions = dimensions
        self.calls: list[tuple[tuple[str, ...], str]] = []

    async def embed(self, texts, model):
        self.calls.append((tuple(texts), model.id))
        # Deterministic, dimension-correct embeddings keyed on text length.
        return [[float(len(t))] * self._dimensions for t in texts]


async def _resolve_query_vector(query, model, provider):
    """Resolve a similarity-search query into a raw vector.

    Implements REQ-430: a text query is vectorized via the declared embedding
    model before the search executes; a raw vector query is passed through
    unchanged (no embedding call).
    """
    if isinstance(query, str):
        vectors = await provider.embed([query], model)
        if len(vectors) != 1:
            raise ValueError("query vectorization must yield exactly one vector")
        vector = vectors[0]
    else:
        vector = list(query)
    if len(vector) != model.dimensions:
        raise ValueError(
            f"query vector dimension {len(vector)} != model dimension {model.dimensions}"
        )
    return vector


@given("a similarity search expressed with a text string")
def similarity_search_text(shared_data):
    model = VectorModel(id="text-embedding-3-small", provider="openai", dimensions=4)
    provider = _RecordingProvider(model.dimensions)
    shared_data["model"] = model
    shared_data["provider"] = provider
    shared_data["text_query"] = "find documents about vector search"
    shared_data["raw_query"] = [0.1, 0.2, 0.3, 0.4]

    assert isinstance(shared_data["text_query"], str)
    assert provider.calls == []


@when("the query is executed")
@pytest.mark.asyncio(loop_scope="session")
async def execute_query(shared_data):
    model = shared_data["model"]
    provider = shared_data["provider"]

    # Text input path -> must vectorize via the declared model first.
    shared_data["text_vector"] = await _resolve_query_vector(
        shared_data["text_query"], model, provider
    )
    shared_data["calls_after_text"] = list(provider.calls)

    # Raw vector input path -> must pass through with no additional embed call.
    shared_data["raw_vector"] = await _resolve_query_vector(
        shared_data["raw_query"], model, provider
    )
    shared_data["calls_after_raw"] = list(provider.calls)


@then(
    "Provisa calls the declared embedding model to generate the query vector "
    "before running the search"
)
def assert_query_vectorized(shared_data):
    model = shared_data["model"]
    provider = shared_data["provider"]

    # The text query must have produced exactly one embed call against the model.
    assert len(shared_data["calls_after_text"]) == 1
    called_texts, called_model_id = shared_data["calls_after_text"][0]
    assert called_texts == (shared_data["text_query"],)
    assert called_model_id == model.id

    # The generated query vector must match the model's declared dimensionality.
    text_vector = shared_data["text_vector"]
    assert len(text_vector) == model.dimensions
    assert all(isinstance(v, float) for v in text_vector)

    # Raw vector input must be supported without any extra embedding call.
    assert shared_data["raw_vector"] == shared_data["raw_query"]
    assert shared_data["calls_after_raw"] == shared_data["calls_after_text"]

    # A raw vector with the wrong dimension must be rejected by the search interface.
    import asyncio

    with pytest.raises(ValueError, match="dimension"):
        asyncio.get_event_loop().run_until_complete(
            _resolve_query_vector([0.1, 0.2], model, provider)
        )
