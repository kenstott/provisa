// Copyright (c) 2026 Kenneth Stott
// Canary: 122d7c74-3a07-4b33-8124-eb537a1a62d6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1938: a source type's logo in the source-type picker. Brand marks come from simple-icons
// (CC0 icon data; each mark remains its owner's trademark, shown only to name the source). A type
// with no published mark — some vendors withhold theirs — is drawn as a lettered tile in its
// category's colour, so every row has the same shape.
import {
  siAirtable,
  siApachecassandra,
  siApachedruid,
  siApachehive,
  siApachekafka,
  siApacheparquet,
  siClickhouse,
  siCockroachlabs,
  siDatabricks,
  siDuckdb,
  siElasticsearch,
  siGithub,
  siGitlab,
  siGooglebigquery,
  siGooglesheets,
  siGraphql,
  siKaggle,
  siMariadb,
  siMongodb,
  siMysql,
  siNeo4j,
  siOpenapiinitiative,
  siPostgresql,
  siPrometheus,
  siRedis,
  siRss,
  siSap,
  siSinglestore,
  siSnowflake,
  siSplunk,
  siSqlite,
  siTidb,
  siTrino,
} from "simple-icons";

type Icon = { path: string; hex: string; title: string };

const MARKS: Record<string, Icon> = {
  postgresql: siPostgresql,
  greenplum: siPostgresql,
  mysql: siMysql,
  singlestore: siSinglestore,
  mariadb: siMariadb,
  duckdb: siDuckdb,
  saphana: siSap,
  cockroachdb: siCockroachlabs,
  tidb: siTidb,
  hiveserver2: siApachehive,
  hive: siApachehive,
  hive_s3: siApachehive,
  snowflake: siSnowflake,
  bigquery: siGooglebigquery,
  databricks: siDatabricks,
  clickhouse: siClickhouse,
  elasticsearch: siElasticsearch,
  druid: siApachedruid,
  trino: siTrino,
  mongodb: siMongodb,
  cassandra: siApachecassandra,
  redis: siRedis,
  neo4j: siNeo4j,
  sqlite: siSqlite,
  parquet: siApacheparquet,
  google_sheets: siGooglesheets,
  prometheus: siPrometheus,
  openapi: siOpenapiinitiative,
  graphql: siGraphql,
  github: siGithub,
  gitlab: siGitlab,
  kafka: siApachekafka,
  rss: siRss,
  splunk: siSplunk,
  kaggle: siKaggle,
  airtable: siAirtable,
};

const CATEGORY_TONES: Record<string, string> = {
  RDBMS: "#3b6ea8",
  "Cloud DW": "#2f8f83",
  Analytics: "#8a5cc2",
  "Data Lake": "#2d7d46",
  NoSQL: "#b5651d",
  Graph: "#a33d6b",
  File: "#6b7280",
  Other: "#6b7280",
  API: "#c0392b",
  Streaming: "#b7791f",
  Enterprise: "#4b5563",
  "Data Quality": "#0e7490",
  Subscriptions: "#5b4bb7",
};

function initials(label: string): string {
  const words = label
    .replace(/\(.*?\)/g, "")
    .trim()
    .split(/[\s_-]+/)
    .filter(Boolean);
  if (words.length >= 2) return (words[0][0] + words[1][0]).toUpperCase();
  return label.slice(0, 2).toUpperCase();
}

export function SourceLogo({
  type,
  label,
  category,
  size = 20,
}: {
  type: string;
  label: string;
  category: string;
  size?: number;
}) {
  const mark = MARKS[type];
  if (mark) {
    return (
      <svg
        role="img"
        aria-hidden="true"
        viewBox="0 0 24 24"
        width={size}
        height={size}
        fill={`#${mark.hex}`}
        data-testid={`source-logo-${type}`}
        style={{ flexShrink: 0 }}
      >
        <path d={mark.path} />
      </svg>
    );
  }
  const tone = CATEGORY_TONES[category] ?? "#6b7280";
  return (
    <span
      aria-hidden="true"
      data-testid={`source-logo-${type}`}
      style={{
        width: size,
        height: size,
        borderRadius: 4,
        background: tone,
        color: "#fff",
        fontSize: Math.round(size * 0.45),
        fontWeight: 700,
        display: "inline-flex",
        alignItems: "center",
        justifyContent: "center",
        flexShrink: 0,
        lineHeight: 1,
      }}
    >
      {initials(label)}
    </span>
  );
}
