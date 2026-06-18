# Audit — Group 9: Live Data & Events

Date: 2026-06-18
Scope: **Group 9 — Live Data & Events** (REQ-172–175, 176–181, 260–261, 282–287).
Code under `provisa/events/`, `provisa/kafka/`, `provisa/live/`, `provisa/subscriptions/`,
`provisa/api/data/subscribe.py`.
Method: read implementation against requirement text with file:line evidence.
Companion to the Group-2 audit ([group-2.md](group-2.md)).

## Classification key

- **To spec** — implemented and matches the requirement
- **Incomplete** — partially implemented
- **Not to spec** — implemented differently than the requirement states
- **Not added** — required but missing

## Summary

| REQ | Sub-area | Status | Finding |
| --- | --- | --- | --- |
| 172 | Dataset Change Events | To spec | Emits `{table, source, type, timestamp}` (no row detail) to Kafka on mutation `provisa/kafka/change_events.py:57` |
| 173 | Dataset Change Events | To spec | Change event fires in the same mutation block as cache invalidate + MV mark-stale `provisa/api/data/endpoint.py:2625` |
| 174 | Dataset Change Events | Not added | No touch/no-op mutation path for external ETL; no "touch" handler in mutation or events code (grep: no match) |
| 175 | Dataset Change Events | To spec | Topic from `PROVISA_CHANGE_EVENT_TOPIC`, default `provisa.change-events` `provisa/kafka/change_events.py:31` |
| 176 | Kafka Sinks | Not to spec | Sink publish primitive exists but `trigger_sinks_for_table` returns 0 — not wired to registered tables/views `provisa/kafka/sink_executor.py:50` |
| 177 | Kafka Sinks | Not added | No trigger-type dispatch (`change_event`/`schedule`/`manual`/`poll`); only a stub change-event path stubbed out `provisa/kafka/sink_executor.py:41` |
| 178 | Kafka Sinks | Not added | No per-table/view opt-in sink config on `Table`; only `LiveOutputConfig` for live engine `provisa/core/models.py:314` |
| 180 | Kafka Sinks | Not added | No add/remove sink lifecycle on a table/view; no sink attachment config field `provisa/core/models.py:331` |
| 181 | Kafka Sinks | To spec | JSON one-message-per-row, keyed by optional column `provisa/kafka/sink.py:95`, `provisa/kafka/sink_executor.py:105` |
| 260 | Subscriptions | To spec | `PollingNotificationProvider` watermark poll; `watermark_column` config field present `provisa/subscriptions/polling_provider.py:27`, `provisa/core/models.py:357` |
| 261 | Subscriptions | To spec | `DebeziumNotificationProvider` consumes CDC topics → `ChangeEvent`, MySQL/SQLServer/Oracle/PG `provisa/subscriptions/debezium_provider.py:49` |
| 282 | Live Query Engine | Incomplete | `LiveEngine` exists with SSE + Kafka outputs but is never fed config; SSE `/subscribe` uses providers, not the engine `provisa/live/engine.py:62`, `provisa/api/app.py:2431` |
| 283 | Live Query Engine | Not to spec | `watermark_column` required on `LiveDeliveryConfig` but no startup validation that fails when missing `provisa/core/models.py:326` |
| 285 | Live Query Engine | Not added | No `delivery: cdc|poll` field and no config validation rejecting CDC on unsupported sources `provisa/core/models.py:322` |
| 286 | Live Query Engine | Incomplete | Engine accepts both SSE + Kafka outputs per query, but a single shared watermark keyed by `query_id` only — not per `output_type` `provisa/live/engine.py:177` |
| 287 | Live Query Engine | Not to spec | `live_query_state` is `(query_id, watermark, updated_at)`; missing `source`, `output_type`, `last_polled_at`, `status`; no CREATE TABLE in repo `provisa/live/watermark.py:19` |

Status counts: To spec 6, Incomplete 2, Not to spec 3, Not added 5.

## Detail

### Dataset Change Events (REQ-172–175)

- **REQ-172** `emit_change_event` builds `{table, source, type, timestamp}` and produces to
  Kafka with key `source.table`; no row payload `provisa/kafka/change_events.py:57`. Producer
  lazy-inits and no-ops when no bootstrap server configured `provisa/kafka/change_events.py:34`.
- **REQ-173** The emit call sits in the mutation success block right after
  `response_cache_store.invalidate_by_table` and `mv_registry.mark_stale`
  `provisa/api/data/endpoint.py:2625`. Same hook, fires together.
- **REQ-174** No touch/no-op mutation. Searched mutation handler and `provisa/events/`,
  `provisa/kafka/` for "touch" — no match. External ETL cannot signal a change without a
  real DB write.
- **REQ-175** Topic resolves from `PROVISA_CHANGE_EVENT_TOPIC`, default
  `provisa.change-events` `provisa/kafka/change_events.py:31`.

### Kafka Sinks (REQ-176–181)

- **REQ-176** `trigger_sinks_for_table` is a stub that returns 0 with a comment that
  table/view-attached sinks are "forward work" `provisa/kafka/sink_executor.py:41`. The
  mutation hook calls it `provisa/api/data/endpoint.py:2634` but nothing publishes. The
  governed publish primitive `_execute_and_publish` exists `provisa/kafka/sink_executor.py:53`.
- **REQ-177** No trigger-type selection. Only the change-event path is stubbed; no
  `schedule`, `manual`, or `poll` sink trigger handling.
- **REQ-178 / REQ-180** `Table` has no sink attachment field; sinks are not opt-in per
  registered table/view and there is no add/remove lifecycle `provisa/core/models.py:331`.
  `KafkaSinkConfig` keys off a removed approved-query `stable_id`
  `provisa/kafka/sink.py:30`.
- **REQ-181** JSON one-message-per-row with optional key column is implemented in both the
  producer wrapper `provisa/kafka/sink.py:95` and the executor `provisa/kafka/sink_executor.py:105`.

### Subscriptions (REQ-260–261)

- **REQ-260** `PollingNotificationProvider` selects rows where `watermark_column > $1`,
  default `updated_at`, ordered and limited `provisa/subscriptions/polling_provider.py:41`.
  `Table.watermark_column` config field present `provisa/core/models.py:357`. Hard-delete
  blindness noted in the spec is inherent to the approach (poll sees no deleted row).
- **REQ-261** `DebeziumNotificationProvider` consumes `{prefix}.{schema|db}.{table}` topics,
  maps Debezium op codes to operations, emits `ChangeEvent`, supports JSON and Avro via
  Schema Registry `provisa/subscriptions/debezium_provider.py:49`. Registered in
  `get_provider` `provisa/subscriptions/registry.py:129`.

### Live Query Engine (REQ-282–287)

- **REQ-282** `LiveEngine` polls via APScheduler and fans to `SSEFanout` + `KafkaSinkOutput`
  `provisa/live/engine.py:62`. It starts at boot `provisa/api/app.py:2431` but no code path
  calls `live_engine.register(...)` from `Table.live` config — the engine runs empty. The
  SSE `/subscribe` route serves changes through subscription providers, and the engine's
  `query_id` path is disabled with HTTP 410 `provisa/api/data/subscribe.py:344`. CDC-vs-poll
  and SSE-vs-sink are not yet a single delivery path.
- **REQ-283** `LiveDeliveryConfig.watermark_column` is a required field
  `provisa/core/models.py:326`, but there is no startup validation that rejects poll
  delivery without it (config_loader has no live-delivery check).
- **REQ-285** No `delivery: cdc|poll` field on the live/subscription config and no
  validation rejecting `cdc` for unsupported sources `provisa/core/models.py:322`.
- **REQ-286** A live query may list SSE and Kafka outputs together, but `_poll` tracks one
  watermark keyed by `query_id` and updates it once for all outputs
  `provisa/live/engine.py:177`. A slow Kafka output is awaited inline before the next poll
  `provisa/live/engine.py:197`; outputs do not track independent watermarks.
- **REQ-287** Persisted state is `(query_id, watermark, updated_at)`
  `provisa/live/watermark.py:19`. Missing `source`, `output_type`, `last_polled_at`, and
  `status` (`active`/`paused`/`error`). No `CREATE TABLE live_query_state` exists in the
  repo (grep: no match), so resume-from-watermark depends on a table created out of band.

## Named tests

All seven named test files exist and the four unit suites pass (93 passed):

- `tests/unit/test_kafka_change_events.py` — present
- `tests/unit/test_kafka_sink.py` — present
- `tests/integration/test_kafka_sink.py` — present
- `tests/unit/test_subscribe.py` — present
- `tests/integration/test_sse_subscriptions.py` — present
- `tests/unit/test_live_engine.py` — present
- `tests/integration/test_live_sse_integration.py` — present

Tests pass against the current behavior, including the stubbed sink path, so green tests do
not establish REQ-176–180 are met.

## Remaining tasks

| # | REQ | Type | Effort | Task |
| --- | --- | --- | --- | --- |
| 1 | 174 | Not added | S | Add a touch/no-op mutation that emits a change event without a row write, for external ETL signaling |
| 2 | 176/177 | Not to spec | L | Wire `trigger_sinks_for_table` to registered table/view sinks; add `change_event`/`schedule`/`manual`/`poll` trigger dispatch |
| 3 | 178/180 | Not added | M | Add opt-in per-table/view sink config on `Table` with independent add/remove lifecycle |
| 4 | 282 | Incomplete | M | Register `Table.live` configs into `LiveEngine` at startup so the engine actually drives SSE + sink delivery |
| 5 | 283 | Not to spec | S | Fail config validation when poll delivery is configured without `watermark_column` |
| 6 | 285 | Not added | M | Add `delivery: cdc|poll` field; reject `cdc` for sources without CDC support at config load |
| 7 | 286 | Incomplete | M | Track a separate watermark per `output_type`; decouple Kafka delivery from SSE so neither blocks the other |
| 8 | 287 | Not to spec | M | Extend `live_query_state` to `(source, output_type, last_watermark, last_polled_at, status)`; add the CREATE TABLE to schema init |
