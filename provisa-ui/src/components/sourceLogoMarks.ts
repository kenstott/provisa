// Copyright (c) 2026 Kenneth Stott
// Canary: 5d2e8c1a-7f40-4b9e-a316-0c8e9d2f4b71
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1938, REQ-2116: the source type logos: the type picker's and the lineage graph's. Brand marks come from simple-icons
// (CC0 icon data; each mark remains its owner's trademark, shown only to name the source). A type
// with no published mark — some vendors withhold theirs — is drawn as a lettered tile in its
// category's colour, so every row has the same shape. A mark simple-icons does not carry, and
// that its owner lets us show, is a local file under assets/source-logos (its source and terms are
// in the README there), bundled with the UI.
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
  siGoogle,
  siGooglebigquery,
  siGooglesheets,
  siGraphql,
  siKaggle,
  siMariadb,
  siMongodb,
  siQdrant,
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
  siStripe,
  siJira,
  siTidb,
  siTrino,
} from "simple-icons";
import askamerica from "../assets/source-logos/askamerica.svg";

export type Icon = { path: string; hex: string; title: string };

export const MARKS: Record<string, Icon> = {
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
  qdrant: siQdrant,
  cassandra: siApachecassandra,
  redis: siRedis,
  neo4j: siNeo4j,
  sqlite: siSqlite,
  parquet: siApacheparquet,
  google_sheets: siGooglesheets,
  google_workspace: siGoogle, // REQ-1923
  prometheus: siPrometheus,
  openapi: siOpenapiinitiative,
  graphql: siGraphql,
  github: siGithub,
  gitlab: siGitlab,
  stripe: siStripe,
  jira: siJira,
  kafka: siApachekafka,
  rss: siRss,
  splunk: siSplunk,
  kaggle: siKaggle,
  airtable: siAirtable,
};

/** Logos drawn from a bundled file, by source type. */
export const LOGO_FILES: Record<string, string> = {
  govdata: askamerica, // AskAmerica (US Government Data)
};

export const CATEGORY_TONES: Record<string, string> = {
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
  Code: "#1f6f5c", // REQ-1984, REQ-1993
};

export function initials(label: string): string {
  const words = label
    .replace(/\(.*?\)/g, "")
    .trim()
    .split(/[\s_-]+/)
    .filter(Boolean);
  if (words.length >= 2) return (words[0][0] + words[1][0]).toUpperCase();
  return label.slice(0, 2).toUpperCase();
}

/** A source type's logo as an image URL, for a canvas that draws images, not elements (the
 * lineage graph): its bundled file, its brand mark, else its lettered tile. */
export function sourceLogoImage(type: string, label: string, category: string): string {
  const file = LOGO_FILES[type];
  if (file) return file;
  const mark = MARKS[type];
  const svg = mark
    ? `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="#${mark.hex}"><path d="${mark.path}"/></svg>`
    : `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><rect width="24" height="24" rx="5" fill="${CATEGORY_TONES[category] ?? "#6b7280"}"/><text x="12" y="16" font-family="sans-serif" font-size="11" font-weight="700" fill="#fff" text-anchor="middle">${initials(label)}</text></svg>`;
  return `data:image/svg+xml;charset=utf-8,${encodeURIComponent(svg)}`;
}
