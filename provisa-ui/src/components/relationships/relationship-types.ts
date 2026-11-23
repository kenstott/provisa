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
import type {
  RegisteredTable,
  Relationship,
  RelationshipParameter,
  RelationshipShape,
} from "../../types/admin";

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
  /** Where it came from: a run's suggestions are `run:<id>`. */
  scope?: string | null;
  /** One of its tables requires a parameter, so it is never read: no values were compared. */
  reduced_information?: boolean;
}

/** The name a suggestion's relationship takes when accepted: the one suggested, else one made from
 *  the columns it joins: what a decision table's Apply sends with an accepted proposal. */
export function candidateName(c: Candidate, tableNameById: Record<string, string>): string {
  const srcTable = tableNameById[c.source_table_id] ?? String(c.source_table_id);
  const tgtTable = tableNameById[c.target_table_id] ?? String(c.target_table_id);
  return c.suggested_name || `${srcTable}-${c.source_column}-to-${tgtTable}`;
}

export const EMPTY_FORM = {
  id: "",
  originalId: "",
  sourceDomain: "",
  sourceTableId: "",
  sourceColumn: "",
  targetType: "table" as "table" | "function" | "resolver",
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
  // REQ-2249: a code resolver relating the two tables, and its columns carrying each one's key.
  resolver: "",
  sourceMatch: "",
  targetMatch: "",
  // REQ-2249: its landed matches kept by the resolver's @change function, rebuilt whole every
  // rebuildEvery seconds ("" = not given).
  incremental: false,
  rebuildEvery: "",
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

/** REQ-2249: whether a command has a code resolver's shape: a code @query taking exactly two
 * tables and returning columns. The server checks it again, with the keys, on save. */
export function isCodeResolver(f: {
  implKind?: string;
  kind: string;
  arguments: { argKind?: string }[];
  outputColumns?: unknown[] | null;
}): boolean {
  return (
    f.implKind === "code_command" &&
    f.kind === "query" &&
    f.arguments.length === 2 &&
    f.arguments.every((a) => a.argKind === "table_ref") &&
    (f.outputColumns?.length ?? 0) > 0
  );
}

/** REQ-1586 (amended 2026-11-22): the short code a relationship's model type is shown as, which
 * the Rel type columns sort and filter on. */
export const SHAPE_CODE: Record<RelationshipShape, "fk" | "jct" | "dim"> = {
  direct: "fk",
  junction: "jct",
  shared_dimension: "dim",
};
