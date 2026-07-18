// Copyright (c) 2026 Kenneth Stott
// Canary: 55ecea78-4131-4bb4-8a93-71e1bdacea84
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
export const DISCOVERABLE_TYPES = new Set(["mongodb", "elasticsearch", "cassandra", "prometheus"]);

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
  // RDBMS
  { value: "postgresql", label: "PostgreSQL", category: "RDBMS", defaultPort: 5432 },
  { value: "mysql", label: "MySQL", category: "RDBMS", defaultPort: 3306 },
  { value: "singlestore", label: "SingleStore", category: "RDBMS", defaultPort: 3306 },
  { value: "mariadb", label: "MariaDB", category: "RDBMS", defaultPort: 3306 },
  { value: "sqlserver", label: "SQL Server", category: "RDBMS", defaultPort: 1433 },
  { value: "oracle", label: "Oracle", category: "RDBMS", defaultPort: 1521 },
  { value: "duckdb", label: "DuckDB", category: "RDBMS", defaultPort: 0 },
  // Cloud DW
  { value: "snowflake", label: "Snowflake", category: "Cloud DW", defaultPort: 443 },
  { value: "bigquery", label: "BigQuery", category: "Cloud DW", defaultPort: 443 },
  { value: "databricks", label: "Databricks", category: "Cloud DW", defaultPort: 443 },
  { value: "redshift", label: "Redshift", category: "Cloud DW", defaultPort: 5439 },
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
  { value: "files", label: "File Directory (CSV/Parquet/XLSX/JSON, etc.)", category: "File", defaultPort: 0 },
  // Other
  { value: "google_sheets", label: "Google Sheets", category: "Other", defaultPort: 0 },
  { value: "prometheus", label: "Prometheus", category: "Other", defaultPort: 9090 },
  // API
  { value: "openapi", label: "REST API (OpenAPI)", category: "API", defaultPort: 443 },
  { value: "graphql", label: "GraphQL", category: "API", defaultPort: 443 },
  { value: "grpc", label: "gRPC", category: "API", defaultPort: 50051 },
  // Streaming
  { value: "kafka", label: "Kafka", category: "Streaming", defaultPort: 9092 },
  // Enterprise SaaS
  { value: "sharepoint", label: "SharePoint", category: "Enterprise", defaultPort: 0 },
  { value: "splunk", label: "Splunk", category: "Enterprise", defaultPort: 8089 },
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
  "pinot",
  "druid",
  "mongodb",
  "cassandra",
  "redis",
]);

// Data lake types
export const DATA_LAKE = new Set(["delta_lake", "iceberg", "hive"]);

// UI source-type values → backend SourceType vocabulary where the two differ (REQ-947).
export const TYPE_ALIAS: Record<string, string> = { graphql: "graphql_remote", grpc: "grpc_remote" };

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
