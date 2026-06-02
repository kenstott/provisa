// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

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
};

export type RelForm = typeof EMPTY_FORM;
