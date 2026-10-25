// Copyright (c) 2026 Kenneth Stott
// Canary: f3cd6959-acca-4026-95ce-3c6a45008d24
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { domainToSqlName } from "../../naming";
import type { RegisteredTable, Relationship, RelationshipParameter } from "../../types/admin";

/** The name a relationship gives a table, and the one its save resolves: the table's alias when
 *  it has one, its table name otherwise (the virtual name, REQ-014). */
export function tableRef(table: Pick<RegisteredTable, "alias" | "tableName">): string {
  return table.alias ?? table.tableName;
}

export interface Candidate {
  id: number;
  source_table_id: number;
  target_table_id: number;
  source_column: string;
  target_column: string;
  cardinality: string;
  confidence: number;
  reasoning: string;
  suggested_name?: string;
}

export const EMPTY_FORM = {
  id: "",
  originalId: "",
  sourceDomain: "",
  sourceTableId: "",
  sourceColumn: "",
  targetType: "table" as "table" | "function",
  targetDomain: "",
  targetTableId: "",
  targetColumn: "",
  targetFunctionName: "",
  functionArg: "",
  cardinality: "many-to-one",
  materialize: false,
  refreshInterval: "300",
  alias: "",
  graphqlAlias: "",
  disableCypher: false,
  sameEntity: false,
  // REQ-1586: junction declaration; blank viaTable means an ordinary FK/PK-backed edge.
  // junctionDeclared is the checkbox state only — it holds the mapping panel open between checking
  // the box and picking a table, after which viaTable is what is stored and what is read back.
  junctionDeclared: false,
  viaTable: "",
  viaSourceColumn: "",
  viaTargetColumn: "",
  viaTypeColumn: "",
  viaTypeValue: "",
  viaLabelSource: "",
  // REQ-2099: the target's parameters this edge feeds, each from a source column.
  parameters: [] as RelationshipParameter[],
};

export type RelForm = typeof EMPTY_FORM;

/** What a relationship points at, as the list and the detail both show it. */
export function relationshipTargetLabel(
  r: Relationship,
  tableDomainById: Record<string, string>,
  domainsEnabled: boolean,
): string {
  if (r.targetFunctionName) return `fn:${r.targetFunctionName}(${r.functionArg ?? ""})`;
  const tDomain = domainsEnabled ? tableDomainById[r.targetTableId!] : undefined;
  const sDomain = domainsEnabled ? domainToSqlName(r.sourceDomainId ?? "") : undefined;
  return tDomain && tDomain !== sDomain
    ? `${tDomain}.${r.targetTableName}.${r.targetColumn}`
    : `${r.targetTableName}.${r.targetColumn}`;
}
