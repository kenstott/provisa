// Copyright (c) 2026 Kenneth Stott
// Canary: d5201669-e816-47d1-9ee2-df3f5241e0b9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-929: human labels for the source change-signal options.
export const CHANGE_SIGNAL_LABELS: Record<string, string> = {
  ttl: "ttl (timer)",
  probe: "probe (freshness query)",
  ttl_probe: "probe + ttl",
  native: "native (source push)",
  debezium: "debezium",
  kafka: "kafka",
};

/** Source types that support schema discovery via adapter. */
// REQ-1767: kafka discovers a topic's columns from Confluent Schema Registry
// (provisa.kafka.schema_registry.discover_topic_columns) — same discover/edit/register flow as
// the other types here.
export const DISCOVERABLE_TYPES = new Set([
  "mongodb",
  "elasticsearch",
  "cassandra",
  "prometheus",
  "kafka",
]);

/** Source types that need a table mapping builder (NoSQL / non-relational). */
export const MAPPING_TYPES = new Set([
  "redis",
  "mongodb",
  "elasticsearch",
  "cassandra",
  "prometheus",
]);

export const SOURCE_TYPES = [
  // Subscriptions
  {
    value: "govdata",
    label: "AskAmerica (US Government Data)",
    category: "Subscriptions",
    defaultPort: 0,
  },
  { value: "kaggle", label: "Kaggle", category: "Subscriptions", defaultPort: 0 },
  // RDBMS
  { value: "postgresql", label: "PostgreSQL", category: "RDBMS", defaultPort: 5432 },
  { value: "mysql", label: "MySQL", category: "RDBMS", defaultPort: 3306 },
  { value: "singlestore", label: "SingleStore", category: "RDBMS", defaultPort: 3306 },
  { value: "mariadb", label: "MariaDB", category: "RDBMS", defaultPort: 3306 },
  { value: "sqlserver", label: "SQL Server", category: "RDBMS", defaultPort: 1433 },
  { value: "oracle", label: "Oracle", category: "RDBMS", defaultPort: 1521 },
  { value: "duckdb", label: "DuckDB", category: "RDBMS", defaultPort: 0 },
  // Exasol's documented default client-connection port (jdbc:exa / provisa/executor/drivers, added
  // REQ-1731) is 8563.
  { value: "exasol", label: "Exasol", category: "RDBMS", defaultPort: 8563 },
  // SAP HANA's HDB SQL port for the default tenant (instance 00): 3<instance>15 — 39015.
  { value: "saphana", label: "SAP HANA", category: "RDBMS", defaultPort: 39015 },
  // REQ-950: Postgres-wire-compatible — reuses the postgres driver/dialect/Trino connector, same
  // SIMPLE_RDBMS shape as postgresql itself.
  { value: "cockroachdb", label: "CockroachDB", category: "RDBMS", defaultPort: 26257 },
  { value: "yugabytedb", label: "YugabyteDB", category: "RDBMS", defaultPort: 5433 },
  { value: "greenplum", label: "Greenplum", category: "RDBMS", defaultPort: 5432 },
  // REQ-950: MySQL-wire-compatible — reuses the mysql driver/dialect/Trino connector.
  { value: "tidb", label: "TiDB", category: "RDBMS", defaultPort: 4000 },
  // REQ-899: reached via a DuckDB community extension, not a Trino connector or direct driver —
  // DuckDB-only. Firebird's DSN shape (firebird://user:pass@host:port/path) matches SIMPLE_RDBMS
  // exactly; `database` carries the .fdb file path in the container, same as the source-of-truth
  // integration test (tests/integration/test_firebird_source_e2e.py).
  { value: "firebird", label: "Firebird", category: "RDBMS", defaultPort: 3050 },
  // REQ-1731: a HiveServer2 endpoint reached directly over Thrift (impyla) — distinct from
  // `hive`/`hive_s3` (Trino-scanned lake storage). Read-then-land like any other RDB-shaped
  // source; SIMPLE_RDBMS's host/port/database/username/password shape matches exactly.
  { value: "hiveserver2", label: "HiveServer2", category: "RDBMS", defaultPort: 10000 },
  // Cloud DW
  { value: "snowflake", label: "Snowflake", category: "Cloud DW", defaultPort: 443 },
  { value: "bigquery", label: "BigQuery", category: "Cloud DW", defaultPort: 443 },
  { value: "databricks", label: "Databricks", category: "Cloud DW", defaultPort: 443 },
  { value: "redshift", label: "Redshift", category: "Cloud DW", defaultPort: 5439 },
  // Fabric/Synapse: T-SQL over TDS/ODBC (provisa/executor/drivers/mssql_warehouse.py); same wire
  // protocol/default port as sqlserver.
  { value: "fabric", label: "Microsoft Fabric Warehouse", category: "Cloud DW", defaultPort: 1433 },
  { value: "synapse", label: "Azure Synapse SQL", category: "Cloud DW", defaultPort: 1433 },
  // Analytics / OLAP
  { value: "clickhouse", label: "ClickHouse", category: "Analytics", defaultPort: 8123 },
  { value: "elasticsearch", label: "Elasticsearch", category: "Analytics", defaultPort: 9200 },
  { value: "pinot", label: "Apache Pinot", category: "Analytics", defaultPort: 8099 },
  { value: "druid", label: "Apache Druid", category: "Analytics", defaultPort: 8082 },
  { value: "trino", label: "Trino / Presto", category: "Analytics", defaultPort: 8080 },
  // Data Lake
  { value: "delta_lake", label: "Delta Lake", category: "Data Lake", defaultPort: 0 },
  { value: "iceberg", label: "Apache Iceberg", category: "Data Lake", defaultPort: 0 },
  { value: "hive", label: "Hive Metastore", category: "Data Lake", defaultPort: 9083 },
  // REQ-229: a Hive lake whose table data lives on S3 object storage — same Thrift metastore
  // endpoint shape as `hive` (host/port), but the type itself DECLARES S3 storage
  // (TrinoHiveS3Connector always wires Trino's native S3 filesystem; there is no hadoop/local
  // choice like plain `hive` offers).
  { value: "hive_s3", label: "Hive on S3", category: "Data Lake", defaultPort: 9083 },
  // NoSQL
  { value: "mongodb", label: "MongoDB", category: "NoSQL", defaultPort: 27017 },
  { value: "cassandra", label: "Cassandra", category: "NoSQL", defaultPort: 9042 },
  { value: "redis", label: "Redis", category: "NoSQL", defaultPort: 6379 },
  // Graph
  { value: "neo4j", label: "Neo4j", category: "Graph", defaultPort: 7474 },
  { value: "sparql", label: "SPARQL", category: "Graph", defaultPort: 443 },
  // File
  { value: "sqlite", label: "SQLite", category: "File", defaultPort: 0 },
  { value: "csv", label: "CSV File", category: "File", defaultPort: 0 },
  { value: "parquet", label: "Parquet File", category: "File", defaultPort: 0 },
  {
    value: "files",
    label: "File Directory (CSV/Parquet/XLSX/JSON, etc.)",
    category: "File",
    defaultPort: 0,
  },
  // Other
  { value: "google_sheets", label: "Google Sheets", category: "Other", defaultPort: 0 },
  { value: "prometheus", label: "Prometheus", category: "Other", defaultPort: 9090 },
  // REQ-899/1097: an Arrow Flight server reached via DuckDB's airport community extension, not a
  // Trino connector or direct driver — DuckDB-only. Host+port only (no auth/database — the
  // connector's DDL is `ATTACH '<location>' AS ... (TYPE AIRPORT)`, and the location derives from
  // host+port when no explicit override is set — connector_duckdb.py's DuckDBAirportConnector).
  { value: "airport", label: "Airport (Arrow Flight)", category: "Other", defaultPort: 50051 },
  // API
  { value: "openapi", label: "REST API (OpenAPI)", category: "API", defaultPort: 443 },
  { value: "graphql", label: "GraphQL", category: "API", defaultPort: 443 },
  { value: "grpc", label: "gRPC", category: "API", defaultPort: 50051 },
  // Streaming
  { value: "kafka", label: "Kafka", category: "Streaming", defaultPort: 9092 },
  // REQ-1739: derives ws://host:port when no explicit override is set — push_wiring.py's
  // websocket branch (base_url override, else host+port), same override-else-derive shape
  // airport/neo4j already use.
  { value: "websocket", label: "WebSocket Feed", category: "Streaming", defaultPort: 0 },
  // REQ-1739: derives http(s)://host:port+path (or federation_hints.feed_url override) —
  // provisa/api/data/subscribe.py's _build_rss_feed_url.
  { value: "rss", label: "RSS / Atom Feed", category: "Streaming", defaultPort: 0 },
  // REQ-1739: push receiver — external services POST JSON to /data/ingest/{source_id}/{table}.
  // Needs no connection fields at the source level (provisa/ingest/router.py).
  { value: "ingest", label: "Ingest (HTTP Push Receiver)", category: "Streaming", defaultPort: 0 },
  // Enterprise SaaS
  { value: "sharepoint", label: "SharePoint", category: "Enterprise", defaultPort: 0 },
  { value: "splunk", label: "Splunk", category: "Enterprise", defaultPort: 8089 },
  // Data Quality (REQ-1443) — a checker scans a governed table through Provisa's own pgwire
  // endpoint; the source itself needs no connection fields (dq/contract.py). soda is Elastic
  // License 2.0 (self-host only, config/capabilities.yaml cloud_eligible: false) — hidden on a
  // hosted plane at the point this constant is consumed for the dropdown (SourcesPage.tsx
  // typeSelectData), never omitted here, since self-hosted still needs it.
  { value: "soda", label: "Soda (Data Quality)", category: "Data Quality", defaultPort: 0 },
  {
    value: "great_expectations",
    label: "Great Expectations (Data Quality)",
    category: "Data Quality",
    defaultPort: 0,
  },
  // Public Data
];

export const API_AUTH_TYPES = [
  { value: "none", label: "No Auth" },
  { value: "bearer", label: "Bearer Token" },
  { value: "basic", label: "Basic Auth" },
  { value: "api_key", label: "API Key" },
  { value: "oauth2_client_credentials", label: "OAuth2 Client Credentials" },
  { value: "custom_headers", label: "Custom Headers" },
];

export const KAFKA_AUTH_TYPES = [
  { value: "none", label: "No Auth" },
  { value: "sasl_plain", label: "SASL/PLAIN" },
  { value: "sasl_scram_256", label: "SASL/SCRAM-SHA-256" },
  { value: "sasl_scram_512", label: "SASL/SCRAM-SHA-512" },
];

export const NAMING_CONVENTIONS = [
  { value: "", label: "Inherit (global)" },
  { value: "none", label: "none" },
  { value: "snake_case", label: "snake_case" },
  { value: "camelCase", label: "camelCase" },
  { value: "PascalCase", label: "PascalCase" },
];

export const CATEGORIES = [...new Set(SOURCE_TYPES.map((s) => s.category))];

// File-based source types (path only, no host/port/auth)
export const FILE_SOURCES = new Set(["sqlite", "csv", "parquet"]);
export const DB_DESCRIPTION_TYPES = new Set(["postgresql", "mysql", "mariadb", "sqlserver"]);

// Which source types use simple host/port/db/user/pass
export const SIMPLE_RDBMS = new Set([
  "postgresql",
  "mysql",
  "singlestore",
  "mariadb",
  "sqlserver",
  "oracle",
  "clickhouse",
  "druid",
  "mongodb",
  "cassandra",
  "redis",
  "firebird",
  "cockroachdb",
  "yugabytedb",
  "greenplum",
  "tidb",
  // REQ-994: a remote Trino/Presto coordinator read as a SOURCE (distinct from Trino as Provisa's
  // own federation engine) via the generic SQLAlchemy trino dialect — host/port/database(=catalog)/
  // username/password, same shape as any other generic RDB. Previously selectable in the type
  // dropdown but rendered zero connection fields (executor/drivers/registry.py:85-89 has a real
  // driver; registration always failed on an empty host).
  "trino",
  // Exasol: host/port/user/password/database(=schema) via pyexasol
  // (provisa/executor/drivers/exasol.py's ExasolDriver.connect). Same shape as any other generic
  // RDB; an optional federation_hints["tls_fingerprint"] pin (models.py's Source.jdbc_url) is
  // handled separately below, not part of this simple shape.
  "exasol",
  // REQ-1731: HiveServer2 over Thrift (impyla) — host/port/database/username/password, same shape
  // as any other generic RDB.
  "hiveserver2",
  // REQ-1753: saphana (hana+hdbcli via sqlalchemy-hana) — host/port/username/password/database,
  // same shape as any other generic RDB. Its SourceType enum member and dropdown entry were added
  // earlier this session, but this set was never updated — the Sources form rendered zero
  // connection fields for it, so a saphana registration could never even be filled in, let alone
  // submitted. Verified live this session against a real HANA instance once added here.
  "saphana",
]);

// Data lake types
export const DATA_LAKE = new Set(["delta_lake", "iceberg", "hive", "hive_s3"]);

// Host+port only, no database/username/password — the connector's location string derives from
// host+port alone (e.g. airport's ATTACH location, connector_duckdb.py's DuckDBAirportConnector).
// REQ-1739: websocket/rss derive their URL from host+port when no federation_hints override is
// set (push_wiring.py / subscribe.py) — same shape as airport.
// REQ-1740: Trino's pinot connector reaches the cluster through the controller's REST endpoint
// (pinot.controller-urls, TrinoPinotConnector.details() in trino_connectors.py) — host:port only;
// database/username/password rendered under SIMPLE_RDBMS were dead inputs nothing ever read.
// REQ-1766: kafka was in none of SIMPLE_RDBMS/HOST_PORT_ONLY/NO_CONNECTION_TYPES — the Sources
// form rendered ZERO connection fields for it (the same class of bug REQ-1753 fixed for saphana),
// so a kafka registration could never be filled in. push_wiring.py's kafka branch builds
// bootstrap_servers from host+port (see that file), same shape as websocket's ws://host:port.
export const HOST_PORT_ONLY = new Set(["airport", "websocket", "rss", "pinot", "kafka"]);

// Source types needing no connection fields at all beyond id/description (REQ-1739): ingest is a
// pure push receiver, soda/great_expectations are DQ checkers that scan through pgwire.
export const NO_CONNECTION_TYPES = new Set(["ingest", "soda", "great_expectations"]);

// UI source-type values → backend SourceType vocabulary where the two differ (REQ-947).
export const TYPE_ALIAS: Record<string, string> = {
  graphql: "graphql_remote",
  grpc: "grpc_remote",
};

export const GOVDATA_SUBJECTS: { value: string; label: string; schemas: string[] }[] = [
  { value: "COMMERCE", label: "Commerce", schemas: ["sec", "patents"] },
  { value: "ECONOMY", label: "Economy", schemas: ["econ"] },
  { value: "EDUCATION", label: "Education", schemas: ["census", "edu"] },
  { value: "HEALTH", label: "Health", schemas: ["health"] },
  { value: "CYBER", label: "Cyber", schemas: ["cyber_threat", "cyber_vuln"] },
  { value: "PUBLIC_SAFETY", label: "Public Safety", schemas: ["crime"] },
  { value: "ENVIRONMENT", label: "Environment", schemas: ["lands"] },
  { value: "WEATHER", label: "Weather", schemas: ["weather"] },
  { value: "GOVERNMENT", label: "Government", schemas: ["fedregister", "fec"] },
];

export const FILE_TRANSPORTS = [
  { value: "file://", label: "file:// (file mount / local disk)", needsAuth: false as const },
  { value: "ftp://", label: "ftp://", needsAuth: "userpass" as const },
  { value: "sftp://", label: "sftp://", needsAuth: "cert-or-userpass" as const },
  { value: "s3://", label: "s3://", needsAuth: "s3" as const },
  { value: "s3a://", label: "s3a://", needsAuth: "s3" as const },
  { value: "http://", label: "http://", needsAuth: "userpass" as const },
  { value: "https://", label: "https://", needsAuth: "userpass" as const },
  { value: "sharepoint://", label: "sharepoint://", needsAuth: "cert-or-userpass" as const },
];
