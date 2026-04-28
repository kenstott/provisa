# Vector Search Architecture

## Overview

Vector search in Provisa is organized as three independent phases, each delivering standalone value:

1. **Native vector search** — model registry, embedding column declaration, `cosine_similarity()` UDF
2. **Non-native fallback** — transparent pgvector cache materialization for sources without native capability
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
      provider: local
      path: /models/all-minilm-l6-v2
      dimensions: 384
      enabled: true
```

Supported provider types:
- `openai` — OpenAI-compatible REST API
- `ollama` — local Ollama server
- `local` — HuggingFace model loaded from filesystem path (air-gap friendly)

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

`embedding_source` is informational — documents what text generated the vector. Not required for externally generated embeddings.

### Source Capability Detection

At source registration time, Provisa auto-detects native vector support:

| Source | Detection | Native operator |
|---|---|---|
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

Provisa translates per source capability at query time:

| Source capability | Generated SQL |
|---|---|
| pgvector | `description_vec <=> :query_vector` |
| Snowflake Cortex | `VECTOR_COSINE_SIMILARITY(description_vec, :query_vector)` |
| MongoDB Atlas | `$vectorSearch` operator |
| None | Route to pgvector fallback cache (Phase 2) |

### Query-Time Vectorization

Both text input and raw vector input are supported:

```graphql
# Text input — Provisa calls embedding model, generates query vector
products(similar_to: "red running shoes", limit: 10)

# Raw vector input — caller provides vector directly
products(near_vector: [0.123, -0.456, ...], limit: 10)
```

### Model Locking

Once an embedding column is generated with a model, that model is locked for that column. Provisa rejects queries using incompatible models or dimensions:

```
ERROR: column description_vec was generated with text-embedding-3-small (1536d).
Query vector uses nomic-embed-text (768d). Dimension mismatch.
Re-embed the column or declare a separate embedding column.
```

---

## Phase 2: Non-Native Fallback

For sources with no native vector capability, Provisa transparently materializes the embedding column to an internal pgvector-enabled PostgreSQL cache.

### Fallback Flow

```
cosine_similarity() against non-vector source
        ↓
Detect: source has no native vector capability
        ↓
Check pgvector cache: provisa_vector_cache.{source}_{table}_{column}_{hash}
        ↓
    ┌───────────┬──────────────┐
    ▼           ▼              ▼
  HIT         STALE          MISS
    ↓           ↓              ↓
Query cache  Rebuild      Materialize:
directly     cache        - Pull embedding col + PKs from source
                          - Write to pgvector table
                          - Build HNSW index
                          - Record cache metadata
        ↓
Query pgvector cache
        ↓
JOIN PKs back to source for remaining columns
        ↓
Return governed result — caller unaware of fallback
```

### Cache Invalidation

| Trigger | Behavior |
|---|---|
| TTL expiry | Configurable per table; stale cache not served beyond TTL |
| Mutation on source | Invalidate affected rows |
| Manual refresh | Admin API endpoint |
| Row count drift | If source row count diverges beyond threshold, full rebuild |
| Model change | Full rebuild required |
| Schema change | Full rebuild required |

---

## Phase 3: Declarative Embedding Generation

Provisa generates and maintains embedding columns from source text. The embedding column may not exist in the source — Provisa creates and owns it.

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

Generated embeddings are stored in the internal pgvector PostgreSQL instance (same infrastructure as Phase 2 fallback cache). No source write access required.

### Incremental Refresh

- Track last-embedded PK + timestamp per column
- On schedule: fetch only rows where `updated_at > last_embedded_at`
- Re-embed changed rows, upsert into pgvector table
- Full rebuild triggered by: model change, schema change affecting subquery, manual admin trigger

---

## Governance

Embedding columns participate in the full Provisa governance stack:

| Governance feature | Applies to embedding columns |
|---|---|
| RLS | ✅ — row filters applied before similarity search |
| Column masking | ❌ — not applicable (see note below) |
| Domain boundary | ✅ — embedding columns respect domain traversal rules |
| Sensitivity tiers | ✅ — `restricted` tier blocks cosine_similarity() for unauthorized roles |
| Per-role schema visibility | ✅ — embedding columns hidden from roles without visibility |
| Query approval | ✅ — similarity search queries subject to approval workflow |

Column masking does not apply to embedding columns — a vector cannot be partially masked and remain semantically meaningful. Access control is enforced via visibility (hide entirely), RLS (filter rows), sensitivity tier (block search), and domain boundary rules. An embedding column is either visible and searchable for a given role, or absent from the schema entirely.

---

## Deployment Considerations

| Deployment | Embedding models available |
|---|---|
| Cloud / SaaS | OpenAI, Cohere, Anthropic (via API key) |
| Air-gapped | Ollama, local HuggingFace models |
| Regulated / on-prem | Ollama or local only — no external API calls |

The model registry makes the embedding pipeline fully portable across deployment types. An air-gapped enterprise substitutes local models; the rest of the pipeline is identical.

---

## Requirements

REQ-419 through REQ-431. See `docs/arch/requirements.md`.
