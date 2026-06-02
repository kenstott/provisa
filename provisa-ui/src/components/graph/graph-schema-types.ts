// Copyright (c) 2026 Kenneth Stott
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
  tableLabel: string;
  properties: string[];
  pkColumns: string[];
  idColumn: string | null;
  nativeFilterColumns: string[];
  scl1: number | null;
  scl2: number | null;
  scl3: number | null;
}

export interface SchemaRel {
  type: string;
  source: string;
  target: string;
}

export interface CypherSchema {
  labels: string[];
  relationshipTypes: string[];
  propertyKeys: string[];
}
