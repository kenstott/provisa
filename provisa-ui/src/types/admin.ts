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
  namingConvention: string | null;
  path: string | null;
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
  const acronym = parts.filter(p => p && /[a-zA-Z]/.test(p[0])).map(p => p[0]).join("").toLowerCase();
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
  description: string | null;
  nativeFilterType: string | null;
  isPrimaryKey: boolean;
  isForeignKey: boolean;
  isAlternateKey: boolean;
}

export interface ColumnPreset {
  column: string;
  source: "now" | "header" | "literal";
  name: string | null;
  value: string | null;
  dataType: string | null;
}

export interface RegisteredTable {
  id: number;
  sourceId: string;
  domainId: string;
  schemaName: string;
  tableName: string;
  governance: string;
  alias: string | null;
  description: string | null;
  cacheTtl: number | null;
  namingConvention: string | null;
  watermarkColumn: string | null;
  columns: TableColumn[];
  columnPresets: ColumnPreset[];
}

export interface Relationship {
  id: number;
  sourceTableId: number;
  targetTableId: number | null;
  sourceTableName: string;
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
}

export interface RLSRule {
  id: number;
  tableId: number | null;
  domainId: string | null;
  roleId: string;
  filterExpr: string;
}

export interface GovernedQuery {
  id: number;
  name: string;
  queryText: string;
  status: "submitted" | "approved" | "deprecated" | "flagged";
  submittedBy: string;
  approvedBy: string | null;
  rejectionReason: string | null;
}

export interface MutationResult {
  success: boolean;
  message: string;
}
