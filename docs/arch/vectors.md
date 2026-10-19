# Vector Search Architecture

## Overview

Vector search in Provisa is organized as three independent phases, each delivering standalone value:

1. **Native vector search** — model registry, embedding column declaration, `cosine_similarity()` UDF
2. **Non-native fallback** — the replica of a table, with an HNSW index on its embedding column
3. **Declarative embedding generation** — generate and maintain embedding columns from source text

---

## Phase 1: Native Vector Search

### Model Registry

Declared in `provisa.yaml`. All embedding and query vectorization operations must reference a registered model.

```yaml
models:
  embedding:
    - id: text-embedding-3-small
      provider: openai
      dimensions: 1536
      api_key_env: OPENAI_API_KEY
      enabled: true

    - id: nomic-embed-text
      provider: ollama
      base_url: http://localhost:11434
      dimensions: 768
      enabled: true

    - id: all-minilm-l6-v2
      provider: huggingface
      dimensions: 384
      enabled: true
```

Supported provider types:

- `openai` — OpenAI-compatible REST API
- `ollama` — local Ollama server
- `huggingface` — HuggingFace sentence-transformers model loaded locally (air-gap friendly)

### Embedding Column Declaration

A column is declared as an embedding vector by setting `embedding: true` on an existing column:

```yaml
columns:
  description_vec:
    type: array(float)
    embedding: true
    embedding_model: text-embedding-3-small  # must reference model registry
    embedding_dimensions: 1536
    embedding_source: description             # optional: originating text column
```

`embedding_source` is informational: it records what text generated the vector. Not required for externally generated embeddings.

### Source Capability Detection

At source registration time, Provisa auto-detects native vector support:

| Source | Detection | Native operator |
| --- | --- | --- |
| PostgreSQL | Check for pgvector extension | `<=>` (cosine), `<->` (L2), `<#>` (inner product) |
| MongoDB | Check Atlas tier | `$vectorSearch` |
| Snowflake | Check Cortex availability | `VECTOR_COSINE_SIMILARITY()` |
| All others | None detected | Fallback path (Phase 2) |

### cosine_similarity() UDF

Users always write:

```sql
SELECT *, cosine_similarity(description_vec, :query_vector) AS score
FROM products
ORDER BY score DESC
LIMIT 10
```

The query is a vector (a bound value or a `'[...]'` literal), a text (embedded with the column's model), or another embedding column of the same model. The call is governed with the whole statement: its column must be an embedding column the role is served (visibility and scope; a `restricted` scope is the sensitivity tier), and any other column is refused by name. Row filters and domain rules apply to the statement as to any other.

The call is written in the SQL of the terminal that runs it:

| Where the statement runs | Generated SQL |
| --- | --- |
| PostgreSQL source with pgvector | `1 - cosine_distance(col::vector, $1::vector)` |
| PostgreSQL source without pgvector | a plain array computation (`unnest` dot product) |
| Snowflake source | `VECTOR_COSINE_SIMILARITY(col::VECTOR(FLOAT, d), $1::VECTOR(FLOAT, d))` |
| DuckDB engine | `array_cosine_similarity(col::FLOAT[d], $1::FLOAT[d])` |
| Trino engine | `cosine_similarity(col::array(double), json_parse($1)::array(double))` |

A statement over any other source routes to the federation engine, which computes the call exactly. MongoDB Atlas, Qdrant and Azure Cosmos DB search natively through a query form rather than a SQL scalar: their own nearest-neighbour search is a pipeline or search table, and `cosine_similarity` over one of their collection tables is computed by the engine over the landed rows.

### Query-Time Vectorization

Both text input and raw vector input are supported. In GraphQL every table with an embedding column the role is served takes one `similar` argument, naming the column and giving a text or a vector (exactly one), and a `_similarity` score field:

```graphql
# Text input — Provisa embeds it with the column's model
products(similar: {column: description_vec, text: "red running shoes"}, limit: 10) {
  id name _similarity
}

# Raw vector input — the caller gives the vector
products(similar: {column: description_vec, vector: [0.123, -0.456, ...]}, limit: 10) {
  id _similarity
}
```

The rows most like the query come first, ahead of any `order_by`; `_similarity` without a `similar` argument is refused.

### Model Locking

Once an embedding column is generated with a model, that model is locked for that column. Provisa rejects queries using incompatible models or dimensions:

```yaml
ERROR: column description_vec was generated with text-embedding-3-small (1536d).
Query vector uses nomic-embed-text (768d). Dimension mismatch.
Re-embed the column or declare a separate embedding column.
```

---

## Phase 2: The Replica's Index

A table served from its replica is the fallback for a vector search. Where the engine reads the replica through its store's own SQL, the replica holds each embedding column as a vector with an HNSW index for cosine distance.

| Engine | Replica's embedding column | Index |
| --- | --- | --- |
| DuckDB, embedded store | `FLOAT[]` in the store; the engine's copy of the replica fixes it at `FLOAT[d]` | HNSW on the engine's copy, built each time the store changes |
| PostgreSQL | `REAL[]` in the engine's own database | HNSW (`vector_cosine_ops`) on its pgvector cast `vector(d)`, built with each replica build |
| Trino, or an engine reading a replica from a store of another kind | the vector's JSON text | none |

A top-k search reads the index: a statement with a `LIMIT`, ordered first by `cosine_similarity(...)` most similar first, whose embedding column belongs to a replica-served table. It is written nearest-first by distance (DuckDB `array_cosine_distance`, pgvector `<=>`), the order the index serves, and its answer carries the `statement.approximate_index` warning: the index may miss a row slightly more similar than the last one returned. Every other search over a replica, and every search on an engine with no index, is computed exactly and says nothing.

### Freshness

The index is as fresh as its replica. It is built with each replica build and swapped in with it, and the DuckDB engine re-indexes its copy whenever the store changes. The table's replica settings decide how fresh that is; there is no vector cache to invalidate apart from the replica.

---

## Phase 3: Declarative Embedding Generation

Provisa generates and maintains embedding columns from source text. The embedding column may not exist in the source; Provisa creates and owns it.

### Declaration

```yaml
columns:
  description_embedding:
    type: embedding
    model: text-embedding-3-small
    dimensions: 1536
    generated_from: |
      SELECT CONCAT(name, ' ', description, ' ', category)
      FROM {{table}}
      WHERE {{pk}} = :pk
    schedule: "0 2 * * *"   # nightly refresh
    incremental: true        # only re-embed changed rows
```

### generated_from Subquery

The `generated_from` value is a SQL subquery with these constraints:

- Must return exactly one text value
- Validated at declaration time against a sample row
- Template variables available: `{{table}}`, `{{pk}}`
- May reference multiple columns, apply transformations, include joins

Examples:

```sql
-- Multi-column concatenation
SELECT CONCAT(title, ' ', body, ' ', author_name)

-- With join for richer context
SELECT CONCAT(p.name, ' ', c.name, ' ', b.name)
FROM products p
JOIN categories c ON p.category_id = c.id
JOIN brands b ON p.brand_id = b.id
WHERE p.id = :pk

-- Conditional composition by row type
SELECT CASE
  WHEN type = 'article' THEN CONCAT(title, ' ', body)
  WHEN type = 'video'   THEN CONCAT(title, ' ', transcript)
END
```

### Storage

Generated embeddings are stored in the internal pgvector PostgreSQL instance. No source write access required.

### Incremental Refresh

- Track last-embedded PK + timestamp per column
- On schedule: fetch only rows where `updated_at > last_embedded_at`
- Re-embed changed rows, upsert into pgvector table
- Full rebuild triggered by: model change, schema change affecting subquery, manual admin trigger

---

## Governance

Embedding columns participate in the full Provisa governance stack:

| Governance feature | Applies to embedding columns |
| --- | --- |
| RLS | ✅ — row filters applied before similarity search |
| Column masking | ❌ — not applicable (see note below) |
| Domain boundary | ✅ — embedding columns respect domain traversal rules |
| Sensitivity tiers | ✅ — `restricted` tier blocks cosine_similarity() for unauthorized roles |
| Per-role schema visibility | ✅ — embedding columns hidden from roles without visibility |

Column masking does not apply to embedding columns, because a vector cannot be partially masked and remain semantically meaningful. Access control is enforced via visibility (hide entirely), RLS (filter rows), sensitivity tier (block search), and domain boundary rules. An embedding column is either visible and searchable for a given role, or absent from the schema entirely.

---

## Deployment Considerations

| Deployment | Embedding models available |
| --- | --- |
| Cloud / SaaS | OpenAI-compatible APIs (via API key) |
| Air-gapped | Ollama, local HuggingFace models |
| Regulated / on-prem | Ollama or local HuggingFace only — no external API calls |

The model registry makes the embedding pipeline fully portable across deployment types. An air-gapped enterprise substitutes local models; the rest of the pipeline is identical.

