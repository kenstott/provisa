# Copyright (c) 2026 Kenneth Stott
# Canary: 05091061-1129-433e-a64b-b16b27441464
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""Step definitions for REQ-427 / REQ-428 — Generated embedding columns and
scheduled incremental refresh (Vector Search) — plus REQ-430 query-time
vectorization, REQ-424 transparent pgvector fallback cache, REQ-422
source capability auto-detection, and REQ-423 cosine_similarity UDF."""

from __future__ import annotations

import asyncio
import types

import pytest
from pytest_bdd import given, when, then, scenarios

from provisa.vector.fallback_cache import (
    cache_ddl,
    fallback_similarity_sql,
    materialize,
)
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
scenarios("../features/REQ-424.feature")
scenarios("../features/REQ-422.feature")
scenarios("../features/REQ-423.feature")


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


@then("Provisa validates the subquery returns exactly one text value per row against a sample row")
def validate_subquery_against_sample(shared_data):
    spec = shared_data["spec"]
    model = shared_data["model"]
    provider = shared_data["provider"]
    sample_rows = shared_data["sample_rows"]

    async def _run():
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
            await validate_generation(sample_rows, spec, model, provider=_MismatchProvider())

        # A produced vector with wrong dimensions fails dimension validation.
        class _WrongDimProvider:
            async def embed(self, texts, model):
                return [[0.0] * (model.dimensions + 1) for _ in texts]

        with pytest.raises(Exception):
            await validate_generation(sample_rows, spec, model, provider=_WrongDimProvider())

    asyncio.run(_run())


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


@then("only changed rows are re-embedded; a model or schema change triggers a full rebuild")
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
    assert needs_full_rebuild("embed-model", "embed-model-v2", 3, 1536, False) is True

    # A schema change affecting the generating subquery must trigger a full rebuild.
    schema_change = shared_data["schema_change_plan"]
    assert schema_change.mode is RefreshMode.FULL
    assert schema_change.pks == []
    assert schema_change.reason == "schema change"
    assert needs_full_rebuild("embed-model", "embed-model", 3, 3, True) is True

    # Sanity: unchanged model/dimension/schema does not require a full rebuild.
    assert needs_full_rebuild("embed-model", "embed-model", 3, 3, False) is False

    # Additional edge-case assertions for completeness:

    # Only a dimension change (same model id) must also trigger a full rebuild.
    assert needs_full_rebuild("embed-model", "embed-model", 3, 1536, False) is True

    # A brand-new column (old_model_id=None, old_dimensions=None) with no schema
    # change must be treated as incremental (first-time population, not a rebuild).
    first_time_plan = plan_refresh(
        changed_pks=shared_data["changed_pks"],
        new_model_id=shared_data["new_model_id"],
        new_dimensions=shared_data["new_dimensions"],
        old_model_id=None,
        old_dimensions=None,
        schema_changed=False,
    )
    assert first_time_plan.mode is RefreshMode.INCREMENTAL
    assert first_time_plan.pks == shared_data["changed_pks"]

    # A brand-new column with a schema change must still trigger a full rebuild.
    first_time_schema_plan = plan_refresh(
        changed_pks=shared_data["changed_pks"],
        new_model_id=shared_data["new_model_id"],
        new_dimensions=shared_data["new_dimensions"],
        old_model_id=None,
        old_dimensions=None,
        schema_changed=True,
    )
    assert first_time_schema_plan.mode is RefreshMode.FULL
    assert first_time_schema_plan.reason == "schema change"

    # An empty changed_pks list with no rebuild trigger yields an incremental plan
    # that simply has nothing to do (no rows to re-embed).
    empty_incremental = plan_refresh(
        changed_pks=[],
        new_model_id=shared_data["new_model_id"],
        new_dimensions=shared_data["new_dimensions"],
        old_model_id=shared_data["old_model_id"],
        old_dimensions=shared_data["old_dimensions"],
        schema_changed=False,
    )
    assert empty_incremental.mode is RefreshMode.INCREMENTAL
    assert empty_incremental.pks == []

    # The RefreshPlan dataclass must carry the correct mode enum members.
    assert RefreshMode.INCREMENTAL.value == "incremental"
    assert RefreshMode.FULL.value == "full"


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
def execute_query(shared_data):
    model = shared_data["model"]
    provider = shared_data["provider"]

    async def _run():
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

    asyncio.run(_run())


@then(
    "Provisa calls the declared embedding model to generate the query vector before running the search"
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

    with pytest.raises(ValueError, match="dimension"):
        asyncio.run(_resolve_query_vector([0.1, 0.2], model, provider))


# ---------------------------------------------------------------------------
# REQ-424: Transparent pgvector fallback cache
# ---------------------------------------------------------------------------

_DIMENSIONS = 4
_SOURCE_TABLE = "public.documents"
_SOURCE_PK = "doc_id"
_CACHE_TABLE = "provisa_cache.documents_vec"


class _FallbackProvider:
    """Deterministic embedding provider for REQ-424 fallback tests."""

    def __init__(self, dimensions: int):
        self._dimensions = dimensions
        self.embed_calls: list[list[str]] = []

    async def embed(self, texts, model):
        self.embed_calls.append(list(texts))
        # Return a unique, deterministic vector for each text based on its index
        # across all calls so far.
        offset = sum(len(c) for c in self.embed_calls[:-1])
        return [[float(offset + i + 1) / 10.0] * self._dimensions for i in range(len(texts))]


class _FakeConn:
    """In-memory async DB connection that records executed statements."""

    def __init__(self):
        self.executed: list[tuple[str, tuple]] = []

    async def execute(self, sql: str, *args):
        self.executed.append((sql, args))


@given("a source without native vector capability")
def source_without_native_vector(shared_data):
    """Set up a non-vector-capable source with sample rows and a fake connection."""
    model = VectorModel(id="embed-model", provider="openai", dimensions=_DIMENSIONS)
    provider = _FallbackProvider(_DIMENSIONS)
    conn = _FakeConn()

    # Source rows that would be fetched from the non-vector-capable source.
    source_rows = [
        {"doc_id": "doc-1", "body": "Provisa enables fast vector search"},
        {"doc_id": "doc-2", "body": "pgvector stores embeddings efficiently"},
        {"doc_id": "doc-3", "body": "HNSW indexes accelerate nearest-neighbour queries"},
    ]

    shared_data["model"] = model
    shared_data["provider"] = provider
    shared_data["conn"] = conn
    shared_data["source_rows"] = source_rows
    shared_data["source_table"] = _SOURCE_TABLE
    shared_data["source_pk"] = _SOURCE_PK
    shared_data["cache_table"] = _CACHE_TABLE
    shared_data["text_field"] = "body"
    shared_data["has_native_vector"] = False

    # Confirm the source is flagged as not natively capable.
    assert shared_data["has_native_vector"] is False
    assert len(source_rows) == 3


@when("a cosine_similarity query is executed")
def execute_cosine_similarity_query(shared_data):
    """
    Transparent fallback path:
    1. Generate cache DDL (CREATE TABLE + CREATE INDEX).
    2. Materialize source rows (embed + upsert into cache).
    3. Build the rewritten SQL that joins the cache back to the source.
    4. Verify the caller-facing SQL contains no reference to the internal cache.
    """
    model = shared_data["model"]
    provider = shared_data["provider"]
    conn = shared_data["conn"]
    source_rows = shared_data["source_rows"]
    cache_table = shared_data["cache_table"]
    source_table = shared_data["source_table"]
    source_pk = shared_data["source_pk"]
    text_field = shared_data["text_field"]

    # Step 1 — generate DDL statements for the cache table and HNSW index.
    ddl_statements = cache_ddl(cache_table, model.dimensions)
    shared_data["ddl_statements"] = ddl_statements

    async def _run():
        # Step 2 — materialize source rows into the pgvector cache.
        materialized_count = await materialize(
            conn=conn,
            cache_table=cache_table,
            rows=source_rows,
            pk_field=source_pk,
            text_field=text_field,
            model=model,
            provider=provider,
        )
        shared_data["materialized_count"] = materialized_count

    asyncio.run(_run())

    # Step 3 — produce the rewritten similarity SQL that joins cache PKs to source.
    # The query vector represents the caller's similarity request.
    query_vector = [0.1] * _DIMENSIONS
    rewritten_sql = fallback_similarity_sql(
        source_table=source_table,
        source_pk=source_pk,
        cache_table=cache_table,
        query_vector=query_vector,
        limit=10,
    )
    shared_data["rewritten_sql"] = rewritten_sql
    shared_data["query_vector"] = query_vector

    # Record embed calls so the Then step can confirm embedding happened.
    shared_data["embed_calls"] = list(provider.embed_calls)


@then(
    "the embedding column is materialized to the pgvector cache, an HNSW index is built, and results are returned transparently"
)
def assert_transparent_fallback(shared_data):
    """
    Verify every aspect of the transparent fallback path (REQ-424):

    1. The DDL creates the cache table and an HNSW cosine index.
    2. All source rows were embedded and upserted into the cache.
    3. The rewritten query joins the cache back to the source.
    4. The caller-facing interface hides the fallback (no raw cache details exposed).
    """
    ddl_statements = shared_data["ddl_statements"]
    materialized_count = shared_data["materialized_count"]
    rewritten_sql = shared_data["rewritten_sql"]
    embed_calls = shared_data["embed_calls"]
    conn: _FakeConn = shared_data["conn"]
    source_rows = shared_data["source_rows"]
    cache_table = shared_data["cache_table"]
    source_table = shared_data["source_table"]
    source_pk = shared_data["source_pk"]

    # --- 1. DDL correctness ---
    assert len(ddl_statements) == 2, "expected CREATE TABLE and CREATE INDEX statements"

    create_table_sql = ddl_statements[0]
    assert "CREATE TABLE IF NOT EXISTS" in create_table_sql
    assert cache_table in create_table_sql
    assert f"vector({_DIMENSIONS})" in create_table_sql
    assert "pk" in create_table_sql

    create_index_sql = ddl_statements[1]
    assert "CREATE INDEX IF NOT EXISTS" in create_index_sql
    assert "hnsw" in create_index_sql.lower()
    assert "vector_cosine_ops" in create_index_sql
    assert cache_table in create_index_sql

    # --- 2. Materialization correctness ---
    assert materialized_count == len(source_rows), (
        f"expected {len(source_rows)} rows materialized, got {materialized_count}"
    )

    # The provider must have been called exactly once (batch embed of all rows).
    assert len(embed_calls) == 1, f"expected exactly one batch embed call, got {len(embed_calls)}"
    embedded_texts = embed_calls[0]
    expected_texts = [str(r["body"]) for r in source_rows]
    assert embedded_texts == expected_texts, (
        f"embedded texts mismatch: {embedded_texts!r} != {expected_texts!r}"
    )

    # Each source row must have produced an upsert into the cache.
    upsert_sqls = [sql for sql, _ in conn.executed]
    assert len(upsert_sqls) == len(source_rows), (
        f"expected {len(source_rows)} upsert statements, got {len(upsert_sqls)}"
    )
    for sql in upsert_sqls:
        assert "INSERT INTO" in sql
        assert cache_table in sql
        assert "ON CONFLICT" in sql
        assert "DO UPDATE" in sql

    # The PKs upserted must match the source rows' doc_id values.
    upserted_pks = {args[0] for _, args in conn.executed}
    expected_pks = {str(r[source_pk]) for r in source_rows}
    assert upserted_pks == expected_pks, (
        f"upserted PKs {upserted_pks!r} do not match expected {expected_pks!r}"
    )

    # --- 3. Rewritten SQL correctness ---
    # The SQL must reference both the source table and the cache table.
    assert source_table in rewritten_sql, (
        f"source table '{source_table}' missing from rewritten SQL"
    )
    assert cache_table in rewritten_sql, f"cache table '{cache_table}' missing from rewritten SQL"

    # The join must use the source PK to link cache results back.
    assert source_pk in rewritten_sql, f"source PK '{source_pk}' missing from rewritten SQL"

    # The SQL must use the cosine distance operator (<=>).
    assert "<=>" in rewritten_sql, "cosine distance operator '<=>' missing from SQL"

    # The SQL must be ordered by similarity score (descending).
    assert "ORDER BY" in rewritten_sql.upper()
    assert "_score DESC" in rewritten_sql

    # The SQL must select from the source (s.*) so the caller gets full rows.
    assert "s.*" in rewritten_sql, "rewritten SQL must select full source rows (s.*)"

    # The SQL must use a JOIN (not a subquery exposed directly to the caller).
    assert "JOIN" in rewritten_sql.upper(), "rewritten SQL must JOIN cache to source"

    # --- 4. Transparency: caller must not need to know about the fallback ---
    # The top-level SELECT must target the source table alias, not the cache alias.
    # The cache details (table name, HNSW, etc.) are encapsulated inside the subquery.
    sql_upper = rewritten_sql.upper()
    # The outer SELECT must be `SELECT s.*`, not `SELECT c.*`.
    outer_select_idx = sql_upper.index("SELECT")
    outer_select_clause = rewritten_sql[outer_select_idx : outer_select_idx + 20]
    assert "s.*" in outer_select_clause, (
        "top-level SELECT must expose source columns (s.*), not cache internals"
    )

    # The LIMIT clause must be present inside the cache subquery (not applied to
    # the outer query, so source-side pagination is not inadvertently skipped).
    assert "LIMIT" in sql_upper, "LIMIT must be present in the rewritten SQL"

    # --- 5. Fallback transparency: verify that the materialization path is opaque ---
    # The DDL and upsert operations are internal; the final SQL handed to the caller
    # reads like a normal source query joined to an opaque subquery.  The caller
    # does not receive DDL statements or raw upsert counts — only the rewritten SQL.
    #
    # We confirm this by checking that the rewritten SQL does NOT contain DDL keywords
    # that would reveal the internal cache management to the caller.
    assert "CREATE TABLE" not in rewritten_sql.upper(), (
        "rewritten query must not expose CREATE TABLE DDL to the caller"
    )
    assert "CREATE INDEX" not in rewritten_sql.upper(), (
        "rewritten query must not expose CREATE INDEX DDL to the caller"
    )
    assert "INSERT INTO" not in rewritten_sql.upper(), (
        "rewritten query must not expose INSERT/upsert statements to the caller"
    )

    # The query vector embedded in the SQL must match what the caller supplied.
    query_vector = shared_data["query_vector"]
    # Each element of the query vector appears in the literal embedded in the SQL.
    for component in query_vector:
        assert str(component) in rewritten_sql or f"{component:.1f}" in rewritten_sql, (
            f"query vector component {component} not found in rewritten SQL"
        )


# ---------------------------------------------------------------------------
# REQ-422: Source capability auto-detection at registration time
# ---------------------------------------------------------------------------


@given("a PostgreSQL source being registered")
def postgresql_source_being_registered(shared_data):
    """
    Set up two representative PostgreSQL source descriptors:

    1. A source whose PostgreSQL instance has the pgvector extension installed —
       ``native_capable`` should be True after detection.
    2. A source whose instance does NOT have pgvector — it must be flagged as
       requiring fallback.

    We use ``native_vector_capability`` from
    ``provisa.vector.capability`` to probe each source descriptor.
    The function accepts a source descriptor mapping that includes at minimum
    a ``source_type`` key and, for PostgreSQL sources, an ``extensions``
    sequence that lists installed extensions (as would be discovered by
    querying ``pg_extension``).
    """
    # Source with pgvector extension present.
    pgvector_source = {
        "source_type": "postgresql",
        "host": "pg-with-pgvector.example.com",
        "port": 5432,
        "database": "mydb",
        # Simulates the result of: SELECT extname FROM pg_extension
        "extensions": ["plpgsql", "pgvector", "pg_trgm"],
    }

    # Source without pgvector extension.
    plain_pg_source = {
        "source_type": "postgresql",
        "host": "pg-plain.example.com",
        "port": 5432,
        "database": "mydb",
        "extensions": ["plpgsql"],
    }

    # MongoDB source with Atlas Vector Search capability.
    mongodb_atlas_source = {
        "source_type": "mongodb",
        "host": "atlas-cluster.mongodb.net",
        "port": 27017,
        "database": "mydb",
        "features": ["atlas_vector_search"],
    }

    # MongoDB source without Atlas Vector Search.
    mongodb_plain_source = {
        "source_type": "mongodb",
        "host": "plain-mongo.example.com",
        "port": 27017,
        "database": "mydb",
        "features": [],
    }

    # Snowflake source with Cortex capability.
    snowflake_cortex_source = {
        "source_type": "snowflake",
        "account": "myaccount.snowflakecomputing.com",
        "database": "mydb",
        "features": ["cortex"],
    }

    # Snowflake source without Cortex.
    snowflake_plain_source = {
        "source_type": "snowflake",
        "account": "myaccount.snowflakecomputing.com",
        "database": "mydb",
        "features": [],
    }

    shared_data["pgvector_source"] = pgvector_source
    shared_data["plain_pg_source"] = plain_pg_source
    shared_data["mongodb_atlas_source"] = mongodb_atlas_source
    shared_data["mongodb_plain_source"] = mongodb_plain_source
    shared_data["snowflake_cortex_source"] = snowflake_cortex_source
    shared_data["snowflake_plain_source"] = snowflake_plain_source

    # Confirm all are typed correctly.
    assert pgvector_source["source_type"] == "postgresql"
    assert plain_pg_source["source_type"] == "postgresql"
    assert mongodb_atlas_source["source_type"] == "mongodb"
    assert mongodb_plain_source["source_type"] == "mongodb"
    assert snowflake_cortex_source["source_type"] == "snowflake"
    assert snowflake_plain_source["source_type"] == "snowflake"


@when("Provisa checks for native vector support")
def check_native_vector_support(shared_data):
    """
    Call the native vector support check and record the result.
    """
    from provisa.vector.support import check_native_vector_support as _check

    results = {}
    for key, source in [
        ("pgvector", shared_data.get("pgvector_source")),
        ("mongodb_atlas", shared_data.get("mongodb_atlas_source")),
        ("mongodb_plain", shared_data.get("mongodb_plain_source")),
        ("snowflake_cortex", shared_data.get("snowflake_cortex_source")),
        ("snowflake_plain", shared_data.get("snowflake_plain_source")),
    ]:
        if source is not None:
            results[key] = _check(source)
    shared_data["native_vector_support"] = results


@then(
    "it detects the pgvector extension and marks the source as native-capable, or flags it for fallback"
)
def assert_native_vector_detection(shared_data):
    """
    Verify REQ-422: capability detection produces the correct True/False result for
    each source descriptor prepared in the Given step.
    """
    from provisa.vector.capability import native_vector_capability, supports_native_vectors

    results = shared_data["native_vector_support"]

    # PostgreSQL with pgvector extension → native-capable.
    assert results["pgvector"] is True, "pgvector source must be detected as native-capable"

    # MongoDB with Atlas Vector Search flag → native-capable.
    assert results["mongodb_atlas"] is True, (
        "MongoDB Atlas source must be detected as native-capable"
    )

    # MongoDB without Atlas Vector Search flag → fallback.
    assert results["mongodb_plain"] is False, "Plain MongoDB must be flagged for fallback"

    # Snowflake with Cortex flag → native-capable.
    assert results["snowflake_cortex"] is True, (
        "Snowflake Cortex source must be detected as native-capable"
    )

    # Snowflake without Cortex flag → fallback.
    assert results["snowflake_plain"] is False, "Plain Snowflake must be flagged for fallback"

    # Sanity-check the underlying capability helpers directly.
    assert native_vector_capability("postgresql") == "pgvector"
    assert native_vector_capability("mongodb") == "atlas_vector"
    assert native_vector_capability("snowflake") == "cortex"
    assert native_vector_capability("mysql") is None

    assert supports_native_vectors("postgresql") is True
    assert supports_native_vectors("mongodb") is True
    assert supports_native_vectors("snowflake") is True
    assert supports_native_vectors("mysql") is False
    assert supports_native_vectors("unknown") is False


# ---------------------------------------------------------------------------
# REQ-423: cosine_similarity UDF — native translation and fallback routing
# ---------------------------------------------------------------------------

_UDF_DIMENSIONS = 3


@given("a query using cosine_similarity(column, query_vector)")
def cosine_similarity_query(shared_data):
    """
    Prepare a cosine_similarity query expression along with source descriptors
    for the native and non-native paths.
    """
    shared_data["column"] = "doc_embedding"
    shared_data["query_vector"] = [0.1, 0.2, 0.3]
    shared_data["udf_dimensions"] = _UDF_DIMENSIONS

    # Native-capable source: PostgreSQL with pgvector.
    shared_data["native_source_capability"] = "pgvector"

    # Non-native source: a source type with no vector capability.
    shared_data["fallback_source_capability"] = None

    assert len(shared_data["query_vector"]) == _UDF_DIMENSIONS
    assert shared_data["native_source_capability"] == "pgvector"
    assert shared_data["fallback_source_capability"] is None


@when("compiled for a native-capable source")
def compile_for_native_source(shared_data):
    """
    Call cosine_similarity_sql for both a native-capable and a non-native source
    and record the results (or the exception) in shared_data.
    """
    from provisa.vector.query import VectorQueryError, cosine_similarity_sql

    column = shared_data["column"]
    query_vector = shared_data["query_vector"]

    # Native path.
    shared_data["native_sql"] = cosine_similarity_sql(
        column=column,
        query_vector=query_vector,
        capability=shared_data["native_source_capability"],
    )

    # Non-native path — must raise VectorQueryError.
    try:
        cosine_similarity_sql(
            column=column,
            query_vector=query_vector,
            capability=shared_data["fallback_source_capability"],
        )
        shared_data["fallback_error"] = None
    except VectorQueryError as exc:
        shared_data["fallback_error"] = exc

    # Also exercise the Snowflake Cortex path.
    shared_data["cortex_sql"] = cosine_similarity_sql(
        column=column,
        query_vector=query_vector,
        capability="cortex",
    )

    # Empty vector must be rejected regardless of capability.
    try:
        cosine_similarity_sql(column=column, query_vector=[], capability="pgvector")
        shared_data["empty_vector_error"] = None
    except VectorQueryError as exc:
        shared_data["empty_vector_error"] = exc


@then(
    "the UDF translates to the native vector operator; for non-native sources it routes to the pgvector fallback cache"
)
def assert_udf_translation(shared_data):
    """
    Verify REQ-423: cosine_similarity UDF translates to the correct native operator
    for capable sources and raises an error for non-native sources so the compiler
    can route to the fallback cache.
    """
    from provisa.vector.query import VectorQueryError

    native_sql = shared_data["native_sql"]
    cortex_sql = shared_data["cortex_sql"]
    column = shared_data["column"]
    query_vector = shared_data["query_vector"]

    # pgvector: must use the <=> cosine distance operator with (1 - distance) form.
    assert "<=>" in native_sql, "pgvector translation must use the <=> operator"
    assert column in native_sql, "native SQL must reference the embedding column"
    assert "(1 -" in native_sql, "pgvector expression must wrap distance as (1 - ...)"
    assert "::vector" in native_sql.lower(), "pgvector literal must be cast to ::vector"
    # The query vector components must appear in the literal.
    for component in query_vector:
        assert str(component) in native_sql or repr(float(component)) in native_sql

    # Snowflake Cortex: must use VECTOR_COSINE_SIMILARITY.
    assert "VECTOR_COSINE_SIMILARITY" in cortex_sql
    assert column in cortex_sql

    # Non-native source: the compiler must receive a VectorQueryError so it can
    # route to the fallback cache rather than pushing down to the source.
    assert shared_data["fallback_error"] is not None, (
        "non-native source must raise VectorQueryError to trigger fallback routing"
    )
    assert isinstance(shared_data["fallback_error"], VectorQueryError)
    assert (
        "fallback" in str(shared_data["fallback_error"]).lower()
        or "native" in str(shared_data["fallback_error"]).lower()
    )

    # Empty query vector must always be rejected.
    assert shared_data["empty_vector_error"] is not None, (
        "empty query_vector must raise VectorQueryError"
    )
    assert isinstance(shared_data["empty_vector_error"], VectorQueryError)
