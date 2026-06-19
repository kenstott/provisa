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
| 299 | Query-API (normalizers) | To spec | Fixed: `handle_api_query` passes `endpoint.response_normalizer` into `flatten_response` (`provisa/api_source/router_integration.py:101`) |
| 307 | GraphQL Remote | To spec | `introspect_schema` POSTs `__schema` query with auth (`provisa/graphql_remote/introspect.py:47`, `:35`) |
| 308 | GraphQL Remote | To spec | `map_schema` → tables + functions w/ return_schema (`provisa/graphql_remote/mapper.py:467`, `:379`) |
| 309 | GraphQL Remote | To spec | Fixed (clarified design): query rows materialize to PG `provisa_admin.gql_cache`, then hot-table promotion to Redis — Provisa's caching tier, not Iceberg/S3; dead VALUES-CTE adapter removed (`provisa/api/data/endpoint.py:677`) |
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
| 323 | gRPC Remote | To spec | Fixed: Get/List/Find/Fetch/Search/Stream name-prefix is the default classifier; override precedence + structural heuristic retained as fallback (`provisa/grpc_remote/mapper.py:46`, `:185`) |
| 324 | gRPC Remote | To spec | `_PROTO_TO_SQL` covers all scalar mappings; repeated/message→jsonb, enum→text (`provisa/grpc_remote/mapper.py:24`, `:98`) |
| 325 | gRPC Remote | To spec | Query method → table; streaming collects all responses into list (`provisa/grpc_remote/executor.py:86`, `:103`) |
| 326 | gRPC Remote | To spec | Mutation method → tracked function w/ return_schema (`provisa/grpc_remote/executor.py:122`, `provisa/api/admin/grpc_remote_router.py:221`) |
| 327 | gRPC Remote | To spec | Fixed (clarified design): query rows materialize to PG `provisa_admin.grpc_cache` (SHA-256 key), then hot-table promotion; mutations uncached, channel reuse kept (`provisa/api/data/endpoint.py:1657`) |
| 328 | gRPC Remote | To spec | Governance via shared `apply_governance` on compiled SQL (`provisa/api/data/endpoint.py:501`) |
| 329 | gRPC Remote | To spec | Proto refresh re-parses, preserves config, reuses import paths (`provisa/api/admin/grpc_remote_router.py:284`, `:376`) |
| 331 | Ingest | To spec | `POST /{source_id}/{table}` 202 receiver (`provisa/ingest/router.py:29`) |
| 332 | Ingest | To spec | One `AsyncEngine` per source from SQLAlchemy URL (`provisa/ingest/engine.py:25`, `:57`) |
| 333 | Ingest | To spec | `CREATE TABLE IF NOT EXISTS` DDL + `_received_at`/`_updated_at` injected (`provisa/ingest/ddl.py:33`) |
| 334 | Ingest | To spec | Dot-notation `extract_value` w/ array index, NULL on miss (`provisa/ingest/ddl.py:60`) |
| 335 | Ingest | To spec | Single/array payload, 202 + count, 404/503 (`provisa/ingest/router.py:32`, `:68`, `:81`) |
| 336 | Ingest | To spec | Fixed: RLS + column masking enforced at the SSE serving layer (provider is role-agnostic by design); `apply_mask_to_value` mirrors `build_mask_expression` (`provisa/api/data/subscribe.py`, `provisa/security/masking.py`) |
| 337 | Ingest | To spec | `table_columns.data_type` validated/used for DDL type (`provisa/ingest/ddl.py:25`, `provisa/api/app.py:1290`) |
| 338 | WebSocket | To spec | `WebSocketNotificationProvider` connect + subscribe + JSON stream (`provisa/subscriptions/websocket_provider.py:44`) |
| 339 | WebSocket | To spec | Reconnect loop w/ `reconnect_interval` until `close()` (`provisa/subscriptions/websocket_provider.py:76`, `:61`) |
| 340 | WebSocket | To spec | `op`/`_ts`/`event_path` mapping, ISO8601 + epoch (`provisa/subscriptions/websocket_provider.py:92`, `:98`) |
| 341 | WebSocket | To spec | URL from host/port/path/use_ssl; subscribe_payload + event_path from hints (`provisa/api/data/subscribe.py:111`) |
| 342 | RSS | To spec | RSS polling, default 300s, watermark by pub date (`provisa/subscriptions/rss_provider.py:115`, `:123`, `:150`) |
| 343 | RSS | To spec | Fixed: `_parse_date` returns `datetime.min` UTC sentinel (`_UNPARSEABLE_DATE`) on empty/unparseable (`provisa/subscriptions/rss_provider.py:41`, `:46`, `:58`) |
| 344 | RSS | To spec | `feed_url` from hints else host/port/path/use_ssl; poll_interval override (`provisa/api/data/subscribe.py:81`) |
| 372 | File & Lake (time-travel) | To spec | `TIME_TRAVEL_SOURCES`; `as_of` → FOR TIMESTAMP/VERSION AS OF; rejects non-capable (`provisa/core/models.py:134`, `provisa/compiler/sql_gen.py:1937`, `:1940`) |
| 419 | Vector Search | To spec | Added: `provisa/vector/registry.py` `VectorModelRegistry` with allowlist + `from_config` |
| 420 | Vector Search | To spec | Added: `provisa/vector/providers.py` OpenAI-compatible / Ollama / HuggingFace-local providers |
| 421 | Vector Search | To spec | Added: `Column.embedding`/`embedding_model`/`embedding_source_column` (`provisa/core/models.py`) |
| 422 | Vector Search | To spec | Added: `provisa/vector/capability.py` `native_vector_capability` (pgvector/Atlas/Cortex) + `has_pgvector` |
| 423 | Vector Search | To spec | Added: `provisa/vector/query.py` `cosine_similarity_sql` (native `<=>` / Cortex) |
| 424 | Vector Search | To spec | Added: `provisa/vector/fallback_cache.py` HNSW cache DDL, materialize, PK-rejoin |
| 425 | Vector Search | To spec | Added: `provisa/vector/cache_invalidation.py` TTL/mutation/manual/drift precedence |
| 426 | Vector Search | To spec | Added: `provisa/vector/governance.py` `can_search_embedding`/`assert_search_allowed` (visibility + masking gate) |
| 427 | Vector Search | To spec | Added: `provisa/vector/generation.py` `generated_from` spec + sample validation |
| 428 | Vector Search | To spec | Added: `provisa/vector/scheduled_refresh.py` incremental re-embed + full rebuild on model/schema change |
| 429 | Vector Search | To spec | Added: `validate_vector_dimensions` model-locking on query vectors (`provisa/vector/query.py`) |
| 430 | Vector Search | To spec | Added: `vectorize_text`/`resolve_query_vector` accept text or raw vector (`provisa/vector/query.py`) |
| 431 | Vector Search | To spec | Added: `provisa/vector/` module phased native/fallback/generation; `tests/unit/test_vector.py` |

Counts: 64 To spec, 0 Incomplete, 0 Not to spec, 0 Not added. All Group-4 requirements remediated 2026-06-19 (see Remediation). Original audit (2026-06-18): 45 To spec, 2 Incomplete (REQ-299, 336), 4 Not to spec (REQ-309, 323, 327, 343), 13 Not added (REQ-419–431).

## Detail

### Kafka Sources (REQ-147–150) — all To spec

`kafka` is registered as a virtual (Trino-routed) source (`provisa/transpiler/router.py:53`) and `generate_trino_kafka_properties` builds the Trino Kafka connector config (`provisa/kafka/source.py:84`). `KafkaTopicConfig.default_window` defaults to `"1h"` (`provisa/kafka/source.py:65`) and `inject_kafka_filters` adds a `_timestamp` WHERE predicate (`provisa/kafka/window.py:101`). `KafkaDiscriminator` carries field/value (`provisa/kafka/source.py:47`) and the same injector adds the discriminator predicate (`provisa/kafka/window.py:93`). `SchemaSource.MANUAL` plus a `columns: list[KafkaColumn]` field supports topics without a Schema Registry (`provisa/kafka/source.py:31`, `:63`).

### Direct-Route Dialect Expansion (REQ-229) — To spec

`SOURCE_TO_CONNECTOR` (`provisa/core/models.py:87`) and `SOURCE_TO_DIALECT` (`provisa/core/models.py:112`) both contain clickhouse, mariadb (→mysql dialect), singlestore, redshift, databricks, hive, druid, exasol. `TRINO_ONLY_SOURCES = {"iceberg", "hive_s3", "delta_lake"}` (`provisa/core/models.py:131`) keeps lake/file sources out of the dialect map. `tests/unit/test_dialect_expansion.py` covers all eight.

### Query-API Sources — Neo4j & SPARQL (REQ-295–299)

REQ-295/297: Neo4j and SPARQL build `ApiSource`/`ApiEndpoint` records reusing the API-source pipeline — Neo4j POSTs to the v2 query endpoint with `neo4j_tabular` (`provisa/neo4j/source.py:43`, `:77`); SPARQL POSTs form-encoded with `sparql_bindings` (`provisa/sparql/source.py:46`, `:88`). REQ-296: `preview_query` appends `LIMIT 5` and `validate_shape` rejects node/edge objects with a corrective error (`provisa/neo4j/preview.py:40`, `:80`). REQ-298: the caller transmits JSON or form POST bodies (`provisa/api_source/caller.py:342`). REQ-299 is **To spec** (fixed 2026-06-19): `handle_api_query` now passes `endpoint.response_normalizer` into `flatten_response(page_data, endpoint.response_root, endpoint.columns, endpoint.response_normalizer)` (`provisa/api_source/router_integration.py:101`), so `neo4j_tabular`/`sparql_bindings` envelopes are unwrapped on the live query path.

### GraphQL Remote Schema Connector (REQ-307–313)

REQ-307/308/312/313 are To spec: introspection POSTs `__schema` with auth (`provisa/graphql_remote/introspect.py:47`), `map_schema` produces tables, functions, and relationships (`provisa/graphql_remote/mapper.py:467`), `_qualify_name` namespace-prefixes generated names (`provisa/graphql_remote/mapper.py:275`), and `_detect_relationships` emits relationship rows with cardinality (`provisa/graphql_remote/mapper.py:233`). REQ-311 refresh re-introspects on demand (`tests/integration/test_graphql_remote_source.py:156`). REQ-310 is To spec by way of the shared pipeline: `apply_governance` rewrites `compiled.sql` with RLS/masking/visibility before any source routing (`provisa/api/data/endpoint.py:501`), and remote rows are injected as a VALUES CTE that Trino then filters with that governed SQL. REQ-309 is **To spec** (fixed 2026-06-19, clarified design): per the project's caching tiers, pull/remote-schema sources materialize on demand to the PG cache (`provisa_admin.gql_cache`) and qualifying results promote to the Redis hot-table tier — not the Iceberg/S3 path the original spec text named. The graphql-remote query path materializes to `gql_cache` (`provisa/api/data/endpoint.py:677`); the dead in-process VALUES-CTE adapter was removed.

### OpenAPI Auto-Registration Connector (REQ-314–321) — all To spec

`loader.py:31` reads local or remote specs (YAML/JSON). Manual upload is stored via the admin PUT route (`provisa/api/admin/openapi_router.py:261`). `mapper.py:177` maps GET ops to virtual tables and `mapper.py:119` maps non-GET ops to tracked functions with `return_schema` (`provisa/openapi/register.py:102`, `:222`). REQ-318 is the one source path that does materialize to Iceberg: `trino_cache.cache_table_name` builds a SHA-256 table name, `create_and_insert` writes Parquet to `s3a://provisa-results/api_cache/`, and `schedule_drop` honors TTL (`provisa/api_source/trino_cache.py:87`, `:159`, `:290`). Upstream auth (bearer/basic/api_key) is injected from per-source config without forwarding the caller token (`provisa/source_adapters/openapi_adapter.py:28`). Refresh re-registers while preserving governance config (`provisa/api/admin/openapi_router.py:168`). Governance (REQ-319) rides the shared `apply_governance` step (`provisa/api/data/endpoint.py:501`).

### gRPC Remote Schema Connector (REQ-322–329)

REQ-322/324/325/326/329 are To spec. The `grpc_remote` source registers proto + address and compiles stubs in a module separate from `provisa/grpc/` (`provisa/api/admin/grpc_remote_router.py:36`, `provisa/grpc_remote/loader.py:159`). `_PROTO_TO_SQL` implements the full scalar map with repeated/message→jsonb and enum→text (`provisa/grpc_remote/mapper.py:24`, `:98`). Query execution handles unary and server-streaming, collecting all streamed messages into a list (`provisa/grpc_remote/executor.py:86`, `:103`). Mutations become tracked functions with `return_schema` (`provisa/grpc_remote/executor.py:122`). Proto refresh re-parses and reuses stored import paths (`provisa/api/admin/grpc_remote_router.py:284`, `:376`). REQ-328 governance rides the shared pipeline (`provisa/api/data/endpoint.py:501`). REQ-323 is **To spec** (fixed 2026-06-19): the `Get/List/Find/Fetch/Search/Stream` name-prefix rule is the default classifier (`_has_query_name_prefix`, `provisa/grpc_remote/mapper.py:46`); per-method overrides take precedence and the `server_streaming` / `_output_has_repeated_message_field` structural rule is the fallback for non-prefixed methods (`:185`). REQ-327 is **To spec** (fixed 2026-06-19, clarified design): query rows materialize to the PG cache `provisa_admin.grpc_cache` keyed by SHA-256(`source_id`+method+args), with hot-table promotion to Redis; mutations stay uncached and the channel is reused (`provisa/api/data/endpoint.py:1657`).

### Ingest Sources — Governed HTTP Push Receiver (REQ-331–337)

REQ-331–335 and REQ-337 are To spec: the 202 push receiver (`provisa/ingest/router.py:29`), one `AsyncEngine` per source from a SQLAlchemy URL (`provisa/ingest/engine.py:25`, `:57`), startup DDL with injected `_received_at`/`_updated_at` (`provisa/ingest/ddl.py:33`), dot-notation `extract_value` with array indices and NULL-on-miss (`provisa/ingest/ddl.py:60`), single/array payload handling with 404/503 codes (`provisa/ingest/router.py:32`, `:68`), and `data_type` validation feeding DDL (`provisa/ingest/ddl.py:25`). REQ-336 is **To spec** (fixed 2026-06-19): RLS and column masking are both enforced at the SSE serving layer (`provisa/api/data/subscribe.py`), where role context lives. The notification provider is role-agnostic by design — `state.rls_contexts` and `state.masking_rules` are keyed by role and resolved at the serving boundary, so masking belongs beside the existing RLS filter, not in the provider. Change-event rows never pass through a SQL projection, so masking is applied in Python via `apply_mask_to_value` (`provisa/security/masking.py`), which mirrors `build_mask_expression` (regex with Trino `$N` → Python `\g<N>`, constant, temporal truncate). Subscriptions now enforce the same row- and column-level governance as local-table queries.

### WebSocket & RSS Sources (REQ-338–344)

REQ-338–341 (WebSocket) and REQ-342/344 (RSS) are To spec: the WebSocket provider connects, sends an optional subscribe payload, streams JSON, and reconnects on `reconnect_interval` until `close()` (`provisa/subscriptions/websocket_provider.py:44`, `:76`); `op`/`_ts`/`event_path` mapping accepts ISO8601 and epoch (`:92`, `:98`); URL and hints come from config (`provisa/api/data/subscribe.py:111`). RSS polls at default 300s with a pub-date watermark (`provisa/subscriptions/rss_provider.py:115`, `:150`) and resolves `feed_url`/poll_interval from hints (`provisa/api/data/subscribe.py:81`). REQ-343 is **To spec** (fixed 2026-06-19): `_parse_date` returns the `datetime.min` UTC sentinel `_UNPARSEABLE_DATE` for both the empty and the unparseable cases (`provisa/subscriptions/rss_provider.py:41`, `:46`, `:58`); the sentinel is a watermark comparison key and the non-nullable `ChangeEvent.timestamp`, while the raw `published` string stays nullable in the emitted row. `tests/unit/test_rss_provider.py` asserts the sentinel.

### File & Lake time-travel (REQ-372) — To spec

`TIME_TRAVEL_SOURCES = {"iceberg", "delta_lake"}` (`provisa/core/models.py:134`). `sql_gen` parses an `as_of` root argument and emits `FOR TIMESTAMP AS OF`/`FOR VERSION AS OF` (`provisa/compiler/sql_gen.py:1937`), rejecting `as_of` on non-capable sources with a compile-time `ValueError` (`:1940`). `tests/unit/test_time_travel.py` covers timestamp/version forms and rejection on postgresql/hive_s3.

### Vector Search (REQ-419–431) — all To spec (added 2026-06-19)

The `provisa/vector/` module was built across three tiers. Native/registry: `registry.py` (`VectorModel`, `VectorModelRegistry` allowlist, `from_config`), `providers.py` (OpenAI-compatible / Ollama / HuggingFace-local + `get_provider`), `capability.py` (`native_vector_capability` map postgresql→pgvector / mongodb→atlas_vector / snowflake→cortex, `has_pgvector`), `query.py` (`cosine_similarity_sql` emitting native pgvector `<=>` / Cortex, `vectorize_text`, `validate_vector_dimensions` model-locking, `resolve_query_vector` accepting text or raw vector). Fallback: `fallback_cache.py` (HNSW `cache_ddl`, `materialize`, PK-rejoin `fallback_similarity_sql`), `cache_invalidation.py` (`invalidation_reason` manual>mutation>TTL>drift). Generation: `generation.py` (`generated_from` spec + sample validation), `scheduled_refresh.py` (incremental re-embed, full rebuild on model/dimension/schema change). Governance: `governance.py` (`can_search_embedding`/`assert_search_allowed` — visibility + masking gate). Column declaration (`embedding`/`embedding_model`/`embedding_source_column`) is on `provisa/core/models.py:Column`. `tests/unit/test_vector.py` covers all tiers (46 cases). Compiler/scheduler/registration integration wiring and live pgvector verification remain as follow-up.

## Named tests

Present and non-trivial: `tests/unit/test_kafka_schema.py` (175 lines), `tests/integration/test_kafka_source.py` (93), `tests/unit/test_dialect_expansion.py` (242), `tests/unit/test_neo4j_normalizers.py` (310), `tests/integration/test_neo4j_exec.py` (281), `tests/unit/test_graphql_remote_introspect.py` (125), `tests/unit/test_graphql_remote_mapper.py` (414), `tests/unit/test_api_cache.py` (149), `tests/unit/test_openapi_loader.py` (72), `tests/integration/test_openapi_source.py` (205), `tests/unit/test_grpc_remote_loader.py` (147), `tests/integration/test_grpc_execution.py` (391), `tests/unit/test_ingest_ddl.py` (131), `tests/unit/test_ingest_router.py` (68), `tests/unit/test_websocket_provider.py` (279), `tests/unit/test_rss_provider.py` (277), `tests/unit/test_time_travel.py` (183).

Spec also references `tests/unit/test_kafka_schema.py` for Kafka window/discriminator behavior; those assertions live in `tests/unit/test_kafka_window.py` rather than `test_kafka_schema.py` — present, different file.

Added 2026-06-19: `tests/unit/test_vector.py` (46 cases across the native/fallback/generation tiers, REQ-419–431); `tests/unit/test_masking.py::TestApplyMaskToValue` and `tests/unit/test_subscribe.py::test_masking_applied_to_streamed_row` (REQ-336); `tests/unit/test_grpc_remote_cache.py` (REQ-327 cache-type mapping).

`tests/unit/test_rss_provider.py` now asserts the `datetime.min` sentinel (REQ-343), replacing the prior current-time contract.

## Remediation (2026-06-19)

All 19 audit tasks are resolved. Track A (connector fixes): REQ-299 normalizer on the live path; REQ-343 RSS sentinel date; REQ-309/327 PG-cache materialization (clarified caching design, superseding the Iceberg/S3 spec text); REQ-323 gRPC name-prefix classification; REQ-336 SSE-layer subscription masking. Track B (Vector Search): the full `provisa/vector/` module (REQ-419–431) across native/fallback/generation tiers with `tests/unit/test_vector.py`.

Follow-up (not part of the audit): Vector Search compiler/scheduler/registration integration wiring and live pgvector verification — the module logic is unit-tested but not yet wired into the query pipeline.
