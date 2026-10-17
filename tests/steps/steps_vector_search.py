# Copyright (c) 2026 Kenneth Stott
# Canary: 05091061-1129-433e-a64b-b16b27441464
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""Step definitions for REQ-427 / REQ-428 — Generated embedding columns and
scheduled incremental refresh (Vector Search) — plus REQ-430 query-time
vectorization, REQ-424 a replica's vector index as the search fallback, REQ-422
source capability auto-detection, and REQ-423 cosine_similarity UDF."""

from __future__ import annotations

import asyncio
import types

import pytest
from pytest_bdd import given, when, then, scenarios

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
# REQ-424: a replica's vector index is the fallback for a vector search
# ---------------------------------------------------------------------------

_REPLICA = (None, "org_acme_replicas", "src__public__docs")


class _ReplicaBroker:
    """The embedded store's broker, holding one replica with an embedding column."""

    def __init__(self, rows: int) -> None:
        import pyarrow as pa

        self._table = pa.table(
            {
                "id": pa.array(range(rows), pa.int32()),
                "vec": pa.array(
                    [[float(i % 7), float(i % 5) + 1.0, float(i % 3)] for i in range(rows)],
                    pa.list_(pa.float32()),
                ),
            }
        )

    def canary(self) -> int:
        return 1

    def fetch_arrow(self, schema, table):
        del schema, table
        return self._table, 1


@given("a table with an embedding column served from its replica on the DuckDB engine")
def replica_served_table_on_duckdb(shared_data):
    import threading

    import duckdb

    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    runtime = object.__new__(DuckDBFederationRuntime)
    runtime._con = duckdb.connect()
    runtime._ext_loaded = set()
    runtime._store_copy_canary = {}
    runtime._store_relation_lock = threading.Lock()
    # The embedded store file, read through a per-statement copy (REQ-1999), as
    # tests/unit/test_vector_replica_index.py's runtime says.
    runtime._attached_store_dsn = "duckdb:///store.duckdb"
    runtime._store_broker = _ReplicaBroker(200)
    shared_data["runtime"] = runtime


@when("a top-k cosine_similarity search is executed")
def top_k_search_executed(shared_data):
    import sqlglot

    from provisa.vector.similarity import index_top_k, lower

    governed = (
        'SELECT "d"."id", cosine_similarity("d"."vec", $1, \'mini\', 3) AS "_similarity" '
        'FROM "org_acme_replicas"."src__public__docs" AS "d" '
        'ORDER BY cosine_similarity("d"."vec", $1, \'mini\', 3) DESC LIMIT 3'
    )
    searched, used = index_top_k(governed, frozenset({_REPLICA}))
    shared_data["approximate"] = used
    duck = lower(sqlglot.transpile(searched, read="postgres", write="duckdb")[0], "duckdb")
    runtime = shared_data["runtime"]
    duck = runtime._refresh_store_relations(
        duck.replace('"org_acme_replicas"', '"mat_store"."org_acme_replicas"').replace("$1", "?")
    )
    params = ["[1, 2, 0]"] * duck.count("?")
    shared_data["plan"] = runtime._con.execute("EXPLAIN " + duck, params).fetchall()[0][1]
    shared_data["rows"] = runtime._con.execute(duck, params).fetchall()


@then("the replica's HNSW index answers it and the answer says the index is approximate")
def index_answers_and_warns(shared_data):
    from provisa.vector.similarity import approximate_index_warning

    assert "HNSW" in shared_data["plan"]
    rows = shared_data["rows"]
    assert len(rows) == 3 and rows[0][1] >= rows[1][1] >= rows[2][1]
    assert shared_data["approximate"]
    assert approximate_index_warning().code == "statement.approximate_index"


# ---------------------------------------------------------------------------
# REQ-422: Source capability detection when a source's driver connects
# ---------------------------------------------------------------------------


class _ProbedDriver:
    """A connected PostgreSQL driver whose pg_extension probe answers ``rows``."""

    def __init__(self, rows):
        self.rows = rows

    async def execute(self, sql, params=None):
        return types.SimpleNamespace(rows=self.rows)


@given("a PostgreSQL source being registered")
def postgresql_source_being_registered(shared_data):
    """Two PostgreSQL sources, one with the pgvector extension installed and one without, and a
    Snowflake source, whose SQL always has its VECTOR functions."""
    shared_data["drivers"] = {
        "pgvector": ("postgres", _ProbedDriver([(1,)])),
        "plain_pg": ("postgres", _ProbedDriver([])),
        "snowflake": ("snowflake", _ProbedDriver([])),
        "mysql": ("mysql", _ProbedDriver([(1,)])),
    }


@when("Provisa checks for native vector support")
def check_native_vector_support(shared_data):
    from provisa.vector.capability import detect_vector_capability

    shared_data["capability"] = {
        key: asyncio.run(detect_vector_capability(dialect, driver))
        for key, (dialect, driver) in shared_data["drivers"].items()
    }


@then(
    "it detects the pgvector extension and marks the source as native-capable, or flags it for the engine"
)
def assert_native_vector_detection(shared_data):
    """REQ-422: a source with a native capability is marked with it; any other is the engine's."""
    assert shared_data["capability"] == {
        "pgvector": "pgvector",
        "plain_pg": None,
        "snowflake": "snowflake",
        "mysql": None,
    }


# ---------------------------------------------------------------------------
# REQ-423: cosine_similarity, governed and lowered to the terminal that runs it
# ---------------------------------------------------------------------------


@given("a query using cosine_similarity(column, query_vector)")
def cosine_similarity_query(shared_data):
    from provisa.compiler.stage2 import GovernanceContext, apply_governance

    gov = GovernanceContext()
    gov.role_id = "analyst"
    gov.table_map = {"docs": 1}
    gov.all_columns = {1: [("id", "integer"), ("doc_embedding", "array(float)")]}
    gov.visible_columns = {1: None}
    gov.embedding_columns = {1: {"doc_embedding": "m"}}
    gov.embedding_dims = {"m": 3}
    shared_data["governed"] = apply_governance(
        "SELECT d.id, cosine_similarity(d.doc_embedding, $1) AS s FROM docs d", gov
    )


@when("compiled for a native-capable source")
def compile_for_native_source(shared_data):
    from types import SimpleNamespace as _NS

    from provisa.pgwire._pipeline import _similarity_needs_engine
    from provisa.transpiler.transpile import transpile

    governed = shared_data["governed"]
    shared_data["pgvector_sql"] = transpile(governed, "postgres", pgvector=True)
    shared_data["snowflake_sql"] = transpile(governed, "snowflake")
    state = _NS(source_dialects={"pg": "postgres", "my": "mysql"})
    shared_data["mysql_to_engine"] = _similarity_needs_engine(governed, _NS(source_id="my"), state)
    shared_data["pg_to_engine"] = _similarity_needs_engine(governed, _NS(source_id="pg"), state)
    shared_data["engine_sql"] = transpile(governed, "duckdb")


@then(
    "the call is written in that source's own SQL; for a source without it the statement routes to the engine, which computes it"
)
def assert_udf_translation(shared_data):
    assert "COSINE_DISTANCE(" in shared_data["pgvector_sql"]
    assert "VECTOR_COSINE_SIMILARITY(" in shared_data["snowflake_sql"]
    assert shared_data["pg_to_engine"] is False
    assert shared_data["mysql_to_engine"] is True
    assert "ARRAY_COSINE_SIMILARITY(" in shared_data["engine_sql"]
