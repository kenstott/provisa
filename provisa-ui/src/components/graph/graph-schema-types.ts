// Copyright (c) 2026 Kenneth Stott
// Canary: 9d81c8c6-28e9-4338-85b4-06b2394f1739
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

export interface SchemaNodeLabel {
  domainLabel: string | null;
  domainId: string | null;
  /** Whether the label's domain is a system domain (meta, ontology, ops), as the server lists them. */
  systemDomain: boolean;
  tableLabel: string;
  /** The registered table the label reads. */
  tableId: number;
  properties: string[];
  pkColumns: string[];
  idColumn: string | null;
  nativeFilterColumns: { name: string; type: string }[];
  propertyTypes: Record<string, string>;
  scl1: number | null;
  scl2: number | null;
  scl3: number | null;
}

export interface SchemaRel {
  type: string;
  source: string;
  target: string;
  properties: string[];
  /** What it joins, the same for a join declared from both ends (REQ-2135). */
  join: string;
  /** A link from a constant (HAS_TABLE: every row to its table's registration): read only as a hop
   * of its own, never a step of a variable-length walk (REQ-2135). */
  constantSource: boolean;
}

export interface CypherSchema {
  labels: string[];
  relationshipTypes: string[];
  propertyKeys: string[];
}

/** A label's or relationship type's count, or why the engine could not count it. */
export interface CountEntry {
  count: number | null;
  error?: string;
}

/** /data/graph-counts: each label's and type's count; a total only when every one was counted. */
export interface GraphCounts {
  node_count: number | null;
  rel_count: number | null;
  label_counts: Record<string, CountEntry>;
  rel_counts: Record<string, CountEntry>;
}
