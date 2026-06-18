# Audit — Group 4: Source Connectors

Date: 2026-06-18
Scope: **Group 4 — Source Connectors** (REQ-147–150, 229, 295–344, 372, 419–431) across `provisa/kafka/`, `provisa/transpiler/`, `provisa/neo4j/`, `provisa/sparql/`, `provisa/api_source/`, `provisa/graphql_remote/`, `provisa/openapi/`, `provisa/grpc_remote/`, `provisa/ingest/`, `provisa/subscriptions/`, `provisa/compiler/`, and the (absent) `provisa/vector/`.
Method: read implementation against requirement text with file:line evidence. Companion to [group-1.md](group-1.md), [group-2.md](group-2.md), [group-3.md](group-3.md).

## Classification key

- **To spec** — implemented and matches the requirement
- **Incomplete** — partially implemented
- **Not to spec** — implemented differently than the requirement states
- **Not added** — required but missing

## Summary

| REQ | Sub-area | Status | Finding |
| --- | --- | --- | --- |
| 147 | Kafka Sources | To spec | `kafka` in `VIRTUAL_SOURCES`; Trino Kafka properties generated (`provisa/transpiler/router.py:53`, `provisa/kafka/source.py:84`) |
| 148 | Kafka Sources | To spec | `default_window` field + `_timestamp` WHERE injection (`provisa/kafka/source.py:65`, `provisa/kafka/window.py:101`) |
| 149 | Kafka Sources | To spec | `KafkaDiscriminator` field/value → WHERE injection (`provisa/kafka/source.py:47`, `provisa/kafka/window.py:93`) |
| 150 | Kafka Sources | To spec | `SchemaSource.MANUAL` + manual `columns` list (`provisa/kafka/source.py:31`, `provisa/kafka/source.py:63`) |
| 229 | Direct-Route Dialect Expansion | To spec | All 8 new dialects in `SOURCE_TO_CONNECTOR`/`SOURCE_TO_DIALECT`; lake set TRINO_ONLY (`provisa/core/models.py:87`, `:112`, `:131`) |
| 295 | Query-API (Neo4j) | To spec | `build_api_source`/`build_endpoint` POST + `neo4j_tabular` normalizer (`provisa/neo4j/source.py:43`, `:77`) |
| 296 | Query-API (Neo4j) | To spec | `preview_query` LIMIT 5 + `validate_shape` blocks node/edge objects (`provisa/neo4j/preview.py:40`, `:80`) |
| 297 | Query-API (SPARQL) | To spec | SPARQL config + POST form + `sparql_bindings` normalizer (`provisa/sparql/source.py:46`, `:88`, `provisa/api_source/normalizers.py:193`) |
| 298 | Query-API (POST body) | To spec | Caller transmits json/form POST body (`provisa/api_source/caller.py:342`) |
| 299 | Query-API (normalizers) | Incomplete | Normalizer registry + flattener support exist, but `handle_api_query` calls `flatten_response` without the normalizer arg (`provisa/api_source/router_integration.py:100` vs `provisa/api_source/flattener.py:63`) |
| 307 | GraphQL Remote | To spec | `introspect_schema` POSTs `__schema` query with auth (`provisa/graphql_remote/introspect.py:47`, `:35`) |
| 308 | GraphQL Remote | To spec | `map_schema` → tables + functions w/ return_schema (`provisa/graphql_remote/mapper.py:467`, `:379`) |
| 309 | GraphQL Remote | Not to spec | Rows injected as VALUES CTE + in-process `response_cache_store`, not Iceberg/S3 Parquet (`provisa/source_adapters/graphql_remote_adapter.py:38`, `:47`) |
| 310 | GraphQL Remote | To spec | `apply_governance` runs on `compiled.sql` before source routing; remote rows flow through Trino Stage-2 SQL (`provisa/api/data/endpoint.py:501`) |
| 311 | GraphQL Remote | To spec | On-demand refresh re-introspects, preserves config (`provisa/api/admin/graphql_remote_router.py`, `tests/integration/test_graphql_remote_source.py:156`) |
| 312 | GraphQL Remote | To spec | `_qualify_name` prefixes `namespace__` (`provisa/graphql_remote/mapper.py:275`, `:363`) |
| 313 | GraphQL Remote | To spec | `_detect_relationships` emits relationship rows w/ cardinality (`provisa/graphql_remote/mapper.py:233`, `:255`) |
| 314 | OpenAPI | To spec | Local file or remote URL spec load (`provisa/openapi/loader.py:31`) |
| 315 | OpenAPI | To spec | Manual spec upload via admin PUT stored locally (`provisa/api/admin/openapi_router.py:261`) |
| 316 | OpenAPI | To spec | GET ops → virtual tables; params → args; 2xx schema → columns (`provisa/openapi/mapper.py:177`, `provisa/openapi/register.py:102`) |
| 317 | OpenAPI | To spec | Non-GET ops → tracked functions w/ return_schema (`provisa/openapi/mapper.py:119`, `provisa/openapi/register.py:222`) |
| 318 | OpenAPI | To spec | Iceberg/S3 Parquet cache, SHA-256 key, TTL drop (`provisa/api_source/trino_cache.py:87`, `:159`, `:290`) |
| 319 | OpenAPI | To spec | Governance applied via shared `apply_governance` on compiled SQL (`provisa/api/data/endpoint.py:501`) |
| 320 | OpenAPI | To spec | Per-source bearer/basic/api_key auth injected; Provisa token not forwarded (`provisa/source_adapters/openapi_adapter.py:28`) |
| 321 | OpenAPI | To spec | Admin refresh re-registers, preserves governance config (`provisa/api/admin/openapi_router.py:168`) |
| 322 | gRPC Remote | To spec | `grpc_remote` source registers proto + address; module distinct from `provisa/grpc/` (`provisa/api/admin/grpc_remote_router.py:36`, `provisa/grpc_remote/loader.py:159`) |
| 323 | gRPC Remote | Not to spec | Classification uses overrides + server-streaming + repeated-message-field heuristic, not the Get/List/Find/Fetch/Search/Stream name-prefix rule (`provisa/grpc_remote/mapper.py:83`, `:185`) |
| 324 | gRPC Remote | To spec | `_PROTO_TO_SQL` covers all scalar mappings; repeated/message→jsonb, enum→text (`provisa/grpc_remote/mapper.py:24`, `:98`) |
| 325 | gRPC Remote | To spec | Query method → table; streaming collects all responses into list (`provisa/grpc_remote/executor.py:86`, `:103`) |
| 326 | gRPC Remote | To spec | Mutation method → tracked function w/ return_schema (`provisa/grpc_remote/executor.py:122`, `provisa/api/admin/grpc_remote_router.py:221`) |
| 327 | gRPC Remote | Not to spec | Rows via VALUES CTE + in-process `response_cache_store`, not Iceberg/S3 Parquet; channel reuse present (`provisa/source_adapters/grpc_remote_adapter.py:57`, `:73`, `provisa/api/data/endpoint.py:1544`) |
| 328 | gRPC Remote | To spec | Governance via shared `apply_governance` on compiled SQL (`provisa/api/data/endpoint.py:501`) |
| 329 | gRPC Remote | To spec | Proto refresh re-parses, preserves config, reuses import paths (`provisa/api/admin/grpc_remote_router.py:284`, `:376`) |
| 331 | Ingest | To spec | `POST /{source_id}/{table}` 202 receiver (`provisa/ingest/router.py:29`) |
| 332 | Ingest | To spec | One `AsyncEngine` per source from SQLAlchemy URL (`provisa/ingest/engine.py:25`, `:57`) |
| 333 | Ingest | To spec | `CREATE TABLE IF NOT EXISTS` DDL + `_received_at`/`_updated_at` injected (`provisa/ingest/ddl.py:33`) |
| 334 | Ingest | To spec | Dot-notation `extract_value` w/ array index, NULL on miss (`provisa/ingest/ddl.py:60`) |
| 335 | Ingest | To spec | Single/array payload, 202 + count, 404/503 (`provisa/ingest/router.py:32`, `:68`, `:81`) |
| 336 | Ingest | Incomplete | `_updated_at` watermark polling provider present; masking applied at SSE HTTP layer not in provider (`provisa/ingest/provider.py:31`, `provisa/api/data/subscribe.py:177`) |
| 337 | Ingest | To spec | `table_columns.data_type` validated/used for DDL type (`provisa/ingest/ddl.py:25`, `provisa/api/app.py:1290`) |
| 338 | WebSocket | To spec | `WebSocketNotificationProvider` connect + subscribe + JSON stream (`provisa/subscriptions/websocket_provider.py:44`) |
| 339 | WebSocket | To spec | Reconnect loop w/ `reconnect_interval` until `close()` (`provisa/subscriptions/websocket_provider.py:76`, `:61`) |
| 340 | WebSocket | To spec | `op`/`_ts`/`event_path` mapping, ISO8601 + epoch (`provisa/subscriptions/websocket_provider.py:92`, `:98`) |
| 341 | WebSocket | To spec | URL from host/port/path/use_ssl; subscribe_payload + event_path from hints (`provisa/api/data/subscribe.py:111`) |
| 342 | RSS | To spec | RSS polling, default 300s, watermark by pub date (`provisa/subscriptions/rss_provider.py:115`, `:123`, `:150`) |
| 343 | RSS | Not to spec | Unparseable dates fall back to `datetime.now(timezone.utc)`, spec requires `datetime.min` sentinel (`provisa/subscriptions/rss_provider.py:41`, `:53`) |
| 344 | RSS | To spec | `feed_url` from hints else host/port/path/use_ssl; poll_interval override (`provisa/api/data/subscribe.py:81`) |
| 372 | File & Lake (time-travel) | To spec | `TIME_TRAVEL_SOURCES`; `as_of` → FOR TIMESTAMP/VERSION AS OF; rejects non-capable (`provisa/core/models.py:134`, `provisa/compiler/sql_gen.py:1937`, `:1940`) |
| 419 | Vector Search | Not added | No `provisa/vector/registry.py`; no model registry in codebase |
| 420 | Vector Search | Not added | No `provisa/vector/providers.py`; no multi-provider embedding support |
| 421 | Vector Search | Not added | No `embedding: true` column declaration handling |
| 422 | Vector Search | Not added | No source vector-capability auto-detection (pgvector/Atlas/Cortex) |
| 423 | Vector Search | Not added | No `cosine_similarity` UDF in SQL gen; no `cosine_similarity` anywhere in `provisa/` |
| 424 | Vector Search | Not added | No `provisa/vector/fallback_cache.py`; no pgvector cache materialization |
| 425 | Vector Search | Not added | No `provisa/vector/cache_invalidation.py`; no TTL/drift invalidation |
| 426 | Vector Search | Not added | No embedding-column governance integration |
| 427 | Vector Search | Not added | No `provisa/vector/generation.py`; no `generated_from` virtual embedding column |
| 428 | Vector Search | Not added | No `provisa/vector/scheduled_refresh.py`; no incremental re-embedding |
| 429 | Vector Search | Not added | No model-locking validation on query vectors |
| 430 | Vector Search | Not added | No `provisa/vector/query_vectorization.py`; no text-to-vector at query time |
| 431 | Vector Search | Not added | No `provisa/vector/` module; phased decomposition not built |

Counts: 45 To spec, 2 Incomplete (REQ-299, 336), 4 Not to spec (REQ-309, 323, 327, 343), 13 Not added (REQ-419–431, all Vector Search).

## Detail

### Kafka Sources (REQ-147–150) — all To spec

`kafka` is registered as a virtual (Trino-routed) source (`provisa/transpiler/router.py:53`) and `generate_trino_kafka_properties` builds the Trino Kafka connector config (`provisa/kafka/source.py:84`). `KafkaTopicConfig.default_window` defaults to `"1h"` (`provisa/kafka/source.py:65`) and `inject_kafka_filters` adds a `_timestamp` WHERE predicate (`provisa/kafka/window.py:101`). `KafkaDiscriminator` carries field/value (`provisa/kafka/source.py:47`) and the same injector adds the discriminator predicate (`provisa/kafka/window.py:93`). `SchemaSource.MANUAL` plus a `columns: list[KafkaColumn]` field supports topics without a Schema Registry (`provisa/kafka/source.py:31`, `:63`).

### Direct-Route Dialect Expansion (REQ-229) — To spec

`SOURCE_TO_CONNECTOR` (`provisa/core/models.py:87`) and `SOURCE_TO_DIALECT` (`provisa/core/models.py:112`) both contain clickhouse, mariadb (→mysql dialect), singlestore, redshift, databricks, hive, druid, exasol. `TRINO_ONLY_SOURCES = {"iceberg", "hive_s3", "delta_lake"}` (`provisa/core/models.py:131`) keeps lake/file sources out of the dialect map. `tests/unit/test_dialect_expansion.py` covers all eight.

### Query-API Sources — Neo4j & SPARQL (REQ-295–299)

REQ-295/297: Neo4j and SPARQL build `ApiSource`/`ApiEndpoint` records reusing the API-source pipeline — Neo4j POSTs to the v2 query endpoint with `neo4j_tabular` (`provisa/neo4j/source.py:43`, `:77`); SPARQL POSTs form-encoded with `sparql_bindings` (`provisa/sparql/source.py:46`, `:88`). REQ-296: `preview_query` appends `LIMIT 5` and `validate_shape` rejects node/edge objects with a corrective error (`provisa/neo4j/preview.py:40`, `:80`). REQ-298: the caller transmits JSON or form POST bodies (`provisa/api_source/caller.py:342`). REQ-299 is **Incomplete**: the normalizer registry exists with name validation (`provisa/api_source/normalizers.py:224`) and `flatten_response` accepts a `response_normalizer` argument (`provisa/api_source/flattener.py:63`), but the live execution path `handle_api_query` calls `flatten_response(page_data, endpoint.response_root, endpoint.columns)` and never passes `endpoint.response_normalizer` (`provisa/api_source/router_integration.py:100`). The named normalizer is therefore not applied during materialization, so Neo4j/SPARQL envelopes are not unwrapped on the real query path.

### GraphQL Remote Schema Connector (REQ-307–313)

REQ-307/308/312/313 are To spec: introspection POSTs `__schema` with auth (`provisa/graphql_remote/introspect.py:47`), `map_schema` produces tables, functions, and relationships (`provisa/graphql_remote/mapper.py:467`), `_qualify_name` namespace-prefixes generated names (`provisa/graphql_remote/mapper.py:275`), and `_detect_relationships` emits relationship rows with cardinality (`provisa/graphql_remote/mapper.py:233`). REQ-311 refresh re-introspects on demand (`tests/integration/test_graphql_remote_source.py:156`). REQ-310 is To spec by way of the shared pipeline: `apply_governance` rewrites `compiled.sql` with RLS/masking/visibility before any source routing (`provisa/api/data/endpoint.py:501`), and remote rows are injected as a VALUES CTE that Trino then filters with that governed SQL. REQ-309 is **Not to spec**: the requirement specifies Parquet materialization into the `results.api_cache` Iceberg table on S3 with TTL drop, but the adapter stores serialized rows in the in-process `response_cache_store` and injects them as a VALUES CTE (`provisa/source_adapters/graphql_remote_adapter.py:38`, `:47`) — no Iceberg/S3 table, no SHA-256 cache table.

### OpenAPI Auto-Registration Connector (REQ-314–321) — all To spec

`loader.py:31` reads local or remote specs (YAML/JSON). Manual upload is stored via the admin PUT route (`provisa/api/admin/openapi_router.py:261`). `mapper.py:177` maps GET ops to virtual tables and `mapper.py:119` maps non-GET ops to tracked functions with `return_schema` (`provisa/openapi/register.py:102`, `:222`). REQ-318 is the one source path that does materialize to Iceberg: `trino_cache.cache_table_name` builds a SHA-256 table name, `create_and_insert` writes Parquet to `s3a://provisa-results/api_cache/`, and `schedule_drop` honors TTL (`provisa/api_source/trino_cache.py:87`, `:159`, `:290`). Upstream auth (bearer/basic/api_key) is injected from per-source config without forwarding the caller token (`provisa/source_adapters/openapi_adapter.py:28`). Refresh re-registers while preserving governance config (`provisa/api/admin/openapi_router.py:168`). Governance (REQ-319) rides the shared `apply_governance` step (`provisa/api/data/endpoint.py:501`).

### gRPC Remote Schema Connector (REQ-322–329)

REQ-322/324/325/326/329 are To spec. The `grpc_remote` source registers proto + address and compiles stubs in a module separate from `provisa/grpc/` (`provisa/api/admin/grpc_remote_router.py:36`, `provisa/grpc_remote/loader.py:159`). `_PROTO_TO_SQL` implements the full scalar map with repeated/message→jsonb and enum→text (`provisa/grpc_remote/mapper.py:24`, `:98`). Query execution handles unary and server-streaming, collecting all streamed messages into a list (`provisa/grpc_remote/executor.py:86`, `:103`). Mutations become tracked functions with `return_schema` (`provisa/grpc_remote/executor.py:122`). Proto refresh re-parses and reuses stored import paths (`provisa/api/admin/grpc_remote_router.py:284`, `:376`). REQ-328 governance rides the shared pipeline (`provisa/api/data/endpoint.py:501`). REQ-323 is **Not to spec**: classification is driven by per-method overrides, then `server_streaming`, then `_output_has_repeated_message_field` (`provisa/grpc_remote/mapper.py:83`, `:185`) — not the documented `Get/List/Find/Fetch/Search/Stream` name-prefix rule. REQ-327 is **Not to spec**: a `grpc.aio.Channel` is reused per source and mutations are not cached, but query rows go to the in-process `response_cache_store` and a VALUES CTE (`provisa/source_adapters/grpc_remote_adapter.py:57`, `:73`, `provisa/api/data/endpoint.py:1544`), not Iceberg/S3 Parquet with a SHA-256 cache table.

### Ingest Sources — Governed HTTP Push Receiver (REQ-331–337)

REQ-331–335 and REQ-337 are To spec: the 202 push receiver (`provisa/ingest/router.py:29`), one `AsyncEngine` per source from a SQLAlchemy URL (`provisa/ingest/engine.py:25`, `:57`), startup DDL with injected `_received_at`/`_updated_at` (`provisa/ingest/ddl.py:33`), dot-notation `extract_value` with array indices and NULL-on-miss (`provisa/ingest/ddl.py:60`), single/array payload handling with 404/503 codes (`provisa/ingest/router.py:32`, `:68`), and `data_type` validation feeding DDL (`provisa/ingest/ddl.py:25`). REQ-336 is **Incomplete**: the `_updated_at` watermark polling provider exists (`provisa/ingest/provider.py:31`), but the provider yields raw ChangeEvents — RLS/masking is applied at the SSE HTTP layer (`provisa/api/data/subscribe.py:177`) rather than "identically to local table subscriptions" inside the provider, so governance parity is partial.

### WebSocket & RSS Sources (REQ-338–344)

REQ-338–341 (WebSocket) and REQ-342/344 (RSS) are To spec: the WebSocket provider connects, sends an optional subscribe payload, streams JSON, and reconnects on `reconnect_interval` until `close()` (`provisa/subscriptions/websocket_provider.py:44`, `:76`); `op`/`_ts`/`event_path` mapping accepts ISO8601 and epoch (`:92`, `:98`); URL and hints come from config (`provisa/api/data/subscribe.py:111`). RSS polls at default 300s with a pub-date watermark (`provisa/subscriptions/rss_provider.py:115`, `:150`) and resolves `feed_url`/poll_interval from hints (`provisa/api/data/subscribe.py:81`). REQ-343 is **Not to spec**: the spec requires unparseable dates to fall back to a `datetime.min` UTC sentinel so they are explicitly marked unparseable, but `_parse_date` returns `datetime.now(timezone.utc)` for both the empty and the unparseable cases (`provisa/subscriptions/rss_provider.py:41`, `:53`). `tests/unit/test_rss_provider.py:125` asserts the current-time behavior, so the test encodes the wrong contract.

### File & Lake time-travel (REQ-372) — To spec

`TIME_TRAVEL_SOURCES = {"iceberg", "delta_lake"}` (`provisa/core/models.py:134`). `sql_gen` parses an `as_of` root argument and emits `FOR TIMESTAMP AS OF`/`FOR VERSION AS OF` (`provisa/compiler/sql_gen.py:1937`), rejecting `as_of` on non-capable sources with a compile-time `ValueError` (`:1940`). `tests/unit/test_time_travel.py` covers timestamp/version forms and rejection on postgresql/hive_s3.

### Vector Search (REQ-419–431) — all Not added

There is no `provisa/vector/` directory. A repo-wide search for `cosine_similarity` matches only a vendored elasticsearch dependency (`.venv/.../vectorstore/_utils.py`), nothing in `provisa/`. None of the spec-named files exist: `registry.py`, `providers.py`, `fallback_cache.py`, `cache_invalidation.py`, `generation.py`, `scheduled_refresh.py`, `query_vectorization.py`. No model registry, embedding-column declaration, pgvector fallback cache, HNSW indexing, model locking, or query-time vectorization is implemented. All 13 requirements (REQ-419 through REQ-431) are unbuilt.

## Named tests

Present and non-trivial: `tests/unit/test_kafka_schema.py` (175 lines), `tests/integration/test_kafka_source.py` (93), `tests/unit/test_dialect_expansion.py` (242), `tests/unit/test_neo4j_normalizers.py` (310), `tests/integration/test_neo4j_exec.py` (281), `tests/unit/test_graphql_remote_introspect.py` (125), `tests/unit/test_graphql_remote_mapper.py` (414), `tests/unit/test_api_cache.py` (149), `tests/unit/test_openapi_loader.py` (72), `tests/integration/test_openapi_source.py` (205), `tests/unit/test_grpc_remote_loader.py` (147), `tests/integration/test_grpc_execution.py` (391), `tests/unit/test_ingest_ddl.py` (131), `tests/unit/test_ingest_router.py` (68), `tests/unit/test_websocket_provider.py` (279), `tests/unit/test_rss_provider.py` (277), `tests/unit/test_time_travel.py` (183).

Spec also references `tests/unit/test_kafka_schema.py` for Kafka window/discriminator behavior; those assertions live in `tests/unit/test_kafka_window.py` rather than `test_kafka_schema.py` — present, different file.

Missing: `tests/unit/test_vector.py` — referenced by all 13 Vector Search requirements (REQ-419–431), does not exist anywhere in the tree.

One existing test encodes a wrong contract: `tests/unit/test_rss_provider.py:125` asserts the current-time date fallback that REQ-343 prohibits.

## Remaining tasks

| # | REQ | Type | Effort | Task |
| --- | --- | --- | --- | --- |
| 1 | 299 | Incomplete | S | Pass `endpoint.response_normalizer` into `flatten_response` at `provisa/api_source/router_integration.py:100` so `neo4j_tabular`/`sparql_bindings` apply on the live query path; add a test that runs a Neo4j/SPARQL query end to end. |
| 2 | 343 | Not to spec | S | Change `_parse_date` to return `datetime.min` (UTC) on unparseable/empty dates (`provisa/subscriptions/rss_provider.py:41`, `:53`); fix `tests/unit/test_rss_provider.py:125` to assert the sentinel. |
| 3 | 323 | Not to spec | M | Implement the `Get/List/Find/Fetch/Search/Stream` name-prefix classification as the default in `provisa/grpc_remote/mapper.py:185`, keeping per-method override precedence; retain or document the structural heuristic if intentional. |
| 4 | 309 | Not to spec | M | Materialize GraphQL-remote query rows to the `results.api_cache` Iceberg table on S3 with a SHA-256 cache key and TTL drop via `trino_cache`, replacing the in-process `response_cache_store` path (`provisa/source_adapters/graphql_remote_adapter.py`). |
| 5 | 327 | Not to spec | M | Same Iceberg/S3 materialization with SHA-256(`source_id`+method+args) cache key for gRPC-remote query results (`provisa/source_adapters/grpc_remote_adapter.py`); keep mutations uncached and channel reuse. |
| 6 | 336 | Incomplete | M | Apply RLS/masking inside the ingest subscription provider (or document the SSE-layer enforcement as the design) so ingest subscriptions match local-table governance (`provisa/ingest/provider.py`). |
| 7 | 419 | Not added | M | Add `provisa/vector/registry.py` model registry (id, provider, dimensions, key env/base URL, enabled) with allowlist enforcement. |
| 8 | 420 | Not added | M | Add `provisa/vector/providers.py` supporting OpenAI-compatible, Ollama, and local HuggingFace providers. |
| 9 | 421 | Not added | S | Support `embedding: true` column declaration (model id, dimensions, source_column) in `provisa/compiler/introspect.py`. |
| 10 | 422 | Not added | M | Add source vector-capability auto-detection (pgvector, Atlas, Cortex) in `provisa/source_adapters/introspect.py`. |
| 11 | 423 | Not added | M | Add `cosine_similarity(column, query_vector)` UDF translation to native operators and fallback in `provisa/compiler/sql_gen.py`. |
| 12 | 424 | Not added | L | Add `provisa/vector/fallback_cache.py`: materialize to pgvector cache, HNSW index, query rewrite joining PKs back. |
| 13 | 425 | Not added | M | Add `provisa/vector/cache_invalidation.py`: TTL, mutation-triggered, manual refresh, drift detection. |
| 14 | 426 | Not added | M | Enforce RLS/masking/sensitivity tiers on embedding columns, blocking similarity search for unauthorized roles. |
| 15 | 427 | Not added | L | Add `provisa/vector/generation.py`: `generated_from` virtual embedding column with sample-row validation. |
| 16 | 428 | Not added | M | Add `provisa/vector/scheduled_refresh.py`: incremental re-embedding plus full rebuild on model/schema change. |
| 17 | 429 | Not added | S | Lock model per generated embedding column; reject mismatched model/dimension query vectors (`provisa/compiler/sql_gen.py`). |
| 18 | 430 | Not added | M | Add `provisa/vector/query_vectorization.py`: vectorize text query input via the declared model; accept text or raw vector. |
| 19 | 431 | Not added | L | Phase Vector Search into native / fallback / generation, each independently deployable; add `tests/unit/test_vector.py`. |
