// Copyright (c) 2026 Kenneth Stott
// Canary: c4cb3534-2d2b-4a82-a75d-499bd3268645
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// Live delivery capability derived from a source type (REQ-565).
//
// A table's live delivery options are gated by its source type:
//  - delivery=cdc  → source has a real push provider (mirrors backend
//    _CDC_SUPPORTED_SOURCE_TYPES): postgresql (LISTEN/NOTIFY), debezium and
//    generic kafka (Kafka consumers), mongodb (change streams).
//  - delivery=poll → source data is watermark-polled through Trino. Any
//    federated SQL source qualifies; pure push feeds (debezium/kafka) do not.
// A source is live-capable when it supports at least one delivery mode.

export const CDC_DELIVERY_TYPES = new Set(["postgresql", "debezium", "kafka", "mongodb"]);

export const POLL_DELIVERY_TYPES = new Set([
  "postgresql",
  "mysql",
  "singlestore",
  "mariadb",
  "sqlserver",
  "oracle",
  "duckdb",
  "snowflake",
  "bigquery",
  "databricks",
  "redshift",
  "clickhouse",
  "elasticsearch",
  "pinot",
  "druid",
  "delta_lake",
  "iceberg",
  "hive",
  "mongodb",
  "cassandra",
]);

export interface LiveCapability {
  pollAvail: boolean;
  cdcAvail: boolean;
  liveCapable: boolean;
}

export function liveCapability(sourceType: string | null | undefined): LiveCapability {
  const t = (sourceType ?? "").toLowerCase();
  const pollAvail = POLL_DELIVERY_TYPES.has(t);
  const cdcAvail = CDC_DELIVERY_TYPES.has(t);
  return { pollAvail, cdcAvail, liveCapable: pollAvail || cdcAvail };
}

// REQ-824: non-PG RDBMS have no native push mechanism and reach CDC only through a
// Debezium connector. Their delta-transport (bootstrap_servers/topic_prefix/...) is
// configured once on the source. PostgreSQL uses native LISTEN/NOTIFY and needs none.
export const CDC_TRANSPORT_SOURCE_TYPES = new Set([
  "mysql",
  "mariadb",
  "sqlserver",
  "oracle",
]);

export function cdcTransportApplicable(sourceType: string | null | undefined): boolean {
  return CDC_TRANSPORT_SOURCE_TYPES.has((sourceType ?? "").toLowerCase());
}

// REQ-813/814: per-table live delivery strategy, gated by source type. Mirrors
// backend provisa/core/config_loader.py _STRATEGIES_BY_SOURCE_TYPE.
const STRATEGIES_BY_SOURCE_TYPE: Record<string, string[]> = {
  postgresql: ["poll", "native", "debezium", "kafka"],
  mongodb: ["poll", "native"],
  kafka: ["kafka"],
  mysql: ["poll", "debezium", "kafka"],
  mariadb: ["poll", "debezium", "kafka"],
  sqlserver: ["poll", "debezium", "kafka"],
  oracle: ["poll", "debezium", "kafka"],
};

export function availableStrategies(sourceType: string | null | undefined): string[] {
  const t = (sourceType ?? "").toLowerCase();
  if (!t) return [];
  return STRATEGIES_BY_SOURCE_TYPE[t] ?? ["poll"];
}

// REQ-929: source-level change_signal options, gated by source type. Only source-wide mechanisms
// belong here: ttl (timer) and the transport-driven push signals. probe/ttl_probe are TABLE-level
// only — their token comes from a per-table probe_query (or MAX(watermark_column)), neither of which
// exists on a Source — so they are offered on the table editor, not here.
export function sourceChangeSignals(sourceType: string | null | undefined): string[] {
  const strat = availableStrategies(sourceType);
  const out: string[] = ["ttl"]; // poll → timer default; every source can fall back to ttl
  if (strat.includes("native")) out.push("native");
  if (strat.includes("debezium")) out.push("debezium");
  if (strat.includes("kafka")) out.push("kafka");
  return out;
}

// REQ-982: per-table probe_type options, gated by the source's capability class. Mirrors backend
// provisa/events/probes.py probe_capabilities: file/object sources support only hash|none; HTTP APIs
// and SQL/engine-scannable sources support all four; streaming/push sources are not on the probe axis
// (empty). probe_type implies the landing shape (watermark → append, else replace).
const PROBE_STREAMING_TYPES = new Set(["kafka", "websocket", "ingest"]);
const PROBE_FILE_TYPES = new Set(["csv", "parquet", "sqlite", "files"]);
const PROBE_HTTP_API_TYPES = new Set([
  "openapi",
  "graphql_remote",
  "grpc_remote",
  "rss",
  "prometheus",
  "google_sheets",
]);

export function sourceProbeTypes(sourceType: string | null | undefined): string[] {
  const t = (sourceType ?? "").toLowerCase();
  if (PROBE_STREAMING_TYPES.has(t)) return []; // push axis — not polled, no probe
  if (PROBE_FILE_TYPES.has(t)) return ["hash", "none"];
  if (PROBE_HTTP_API_TYPES.has(t)) return ["watermark", "hash", "count", "none"];
  return ["watermark", "hash", "count", "none"]; // SQL / engine-scannable (open-ended default)
}
