// Copyright (c) 2026 Kenneth Stott
// Canary: 3f6ff1aa-c2c5-41f0-8215-28042c85bd12
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/** Types matching provisa/api/admin/types.py */

// REQ-824: source-level CDC transport (Debezium/Kafka), entered once per source.
export interface SourceCdcConfig {
  bootstrapServers: string;
  topicPrefix: string;
  schemaRegistryUrl?: string | null;
  consumerGroupId?: string | null; // REQ-931: null/omitted = inherit Provisa-level default
}

export interface Source {
  id: string;
  type: string;
  host: string;
  port: number;
  database: string;
  username: string;
  dialect: string | null;
  cacheEnabled: boolean;
  cacheTtl: number | null;
  preferMaterialized: boolean;
  loadProtected: boolean; // REQ-1141: scheduled-refresh-only load protection
  offPeakWindow: string | null; // REQ-1141: "HH:MM-HH:MM" maintenance window
  offPeakTz: string; // REQ-1141: IANA zone for the window
  gqlNamingConvention: string | null;
  path: string | null;
  allowedDomains: string[];
  description: string;
  mappingJson?: string | null;
  changeSignal: string; // REQ-929: source default change signal, inherited by its tables
  cdc?: SourceCdcConfig | null;
}

// REQ-1143: server-derived plain-English summary of a table's effective refresh/serving policy.
export interface RefreshPolicySummary {
  text: string;
  serving: "live" | "scheduled" | "cache" | "frozen";
  warning: string | null;
}

export interface Domain {
  id: string;
  description: string;
  graphqlAlias?: string | null;
}

export function domainGqlAlias(domain: Domain): string {
  if (domain.graphqlAlias) return domain.graphqlAlias.toLowerCase();
  if (!domain.id) return "";
  const parts = domain.id.split(/[^a-zA-Z0-9]+/);
  const acronym = parts
    .filter((p) => p && /[a-zA-Z]/.test(p[0]))
    .map((p) => p[0])
    .join("")
    .toLowerCase();
  return acronym || domain.id[0]?.toLowerCase() || "";
}

export interface TableColumn {
  id: number;
  columnName: string;
  visibleTo: string[];
  writableBy: string[];
  unmaskedTo: string[];
  maskType: string | null;
  maskPattern: string | null;
  maskReplace: string | null;
  maskValue: string | null;
  maskPrecision: string | null;
  alias: string | null;
  computedSqlAlias: string;
  description: string | null;
  dataType: string | null;
  nativeFilterType: string | null;
  isPrimaryKey: boolean;
  isForeignKey: boolean;
  isAlternateKey: boolean;
  scope: string;
}

export interface ColumnPreset {
  column: string;
  source: "now" | "header" | "literal";
  name: string | null;
  value: string | null;
  dataType: string | null;
}

// REQ-1093: a declared UNIQUE constraint (single-column or composite).
export interface UniqueConstraint {
  name: string;
  columns: string[];
}

export interface LiveOutputConfig {
  type: "sse" | "kafka";
  topic: string | null;
  keyColumn: string | null;
  bootstrapServers: string | null;
}

export interface LiveKafkaConfig {
  topic: string;
  format?: string;
  keyColumn?: string | null;
}

export interface LiveDeliveryConfig {
  queryId?: string | null;
  watermarkColumn?: string | null;
  pollInterval: number;
  strategy: "poll" | "native" | "debezium" | "kafka";
  kafka?: LiveKafkaConfig | null;
  outputs: LiveOutputConfig[];
}

export interface RegisteredTable {
  id: number;
  sourceId: string;
  domainId: string;
  schemaName: string;
  tableName: string;
  alias: string | null;
  description: string | null;
  cacheTtl: number | null;
  preferMaterialized: boolean | null;
  loadProtected: boolean | null; // REQ-1141: null = inherit source
  offPeakWindow: string | null; // REQ-1141: "HH:MM-HH:MM" window override
  offPeakTz: string | null; // REQ-1141: window zone override
  refreshPolicySummary: RefreshPolicySummary | null; // REQ-1143: server-derived effective policy
  gqlNamingConvention: string | null;
  watermarkColumn: string | null;
  changeSignal: string | null;
  probeQuery: string | null;
  probeType: string | null;
  columns: TableColumn[];
  columnPresets: ColumnPreset[];
  uniqueConstraints: UniqueConstraint[]; // REQ-1093
  apiEndpoint: string | null;
  viewSql: string | null;
  materialize: boolean;
  mvRefreshInterval: number;
  mvDebounceQuiet: number; // REQ-963: seconds of quiet before firing; 0 = real-time
  mvDebounceMaxDelay: number; // REQ-963: staleness cap under continuous churn
  mvConsistency: string; // REQ-879: "shared" (fleet-coordinated) | "distributed" (per-instance)
  mvPreprocess: string | null; // REQ-957: inline preprocess(rows, ctx) hook source; null = identity
  mvBitemporalMode: string | null; // REQ-1159: null | "snapshot" | "delta" (append-only time travel)
  mvBitemporalKey: string[]; // REQ-1159: business key a version belongs to (required for delta)
  dataProduct: boolean;
  enableAggregates: boolean;
  enableGroupBy: boolean;
  canDeployToDb: boolean;
  live: LiveDeliveryConfig | null;
}

export interface Relationship {
  id: number;
  sourceTableId: number;
  targetTableId: number | null;
  sourceTableName: string;
  sourceDomainId: string;
  targetTableName: string;
  sourceColumn: string;
  targetColumn: string | null;
  cardinality: string;
  materialize: boolean;
  refreshInterval: number;
  targetFunctionName: string | null;
  functionArg: string | null;
  alias: string | null;
  graphqlAlias: string | null;
  computedCypherAlias: string | null;
  autoSuggested: boolean;
  disableCypher: boolean;
  ownerDomainId: string | null;
}

export interface RLSRule {
  id: number;
  tableId: number | null;
  domainId: string | null;
  roleId: string;
  filterExpr: string;
}

export interface MutationResult {
  success: boolean;
  message: string;
}
