// Copyright (c) 2026 Kenneth Stott
// Canary: 7d5fd023-6c3f-48cc-bbe9-5eff36a9f9c8
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

export interface JsonApiRelationshipRef {
  type: string;
  id: string;
}

export interface JsonApiRelationship {
  data?: JsonApiRelationshipRef | JsonApiRelationshipRef[] | null;
}

export interface JsonApiResource {
  type: string;
  id?: string;
  attributes?: Record<string, unknown>;
  relationships?: Record<string, JsonApiRelationship>;
}

export interface JsonApiDocument {
  data?: JsonApiResource | JsonApiResource[];
  included?: JsonApiResource[];
  meta?: Record<string, unknown>;
  links?: Record<string, string | null>;
  errors?: Array<{ detail?: string }>;
}

export interface PaginationLinks {
  first: string | null;
  prev: string | null;
  next: string | null;
  last: string | null;
}

// REQ-1417: every name JSON:API takes in a parameter — fields[], filter[], sort, groupBy and
// include, dot-paths included — is the physical column name, not the GraphQL surface's renamed
// spelling. An alias renames the GraphQL field only; the column underneath keeps its own name.
export function toApiName(col: { columnName: string; alias?: string | null }): string {
  return col.columnName;
}
