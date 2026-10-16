// Copyright (c) 2026 Kenneth Stott
// Canary: 50e004f6-2902-4621-bd25-764b2cafddf5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A table registered from a query (REQ-1670 neo4j, REQ-1683 sparql, REQ-1980 MongoDB pipeline):
// the columns its preview suggests, and a pipeline's arguments, as the form's column rows.

import { toSnakeCase } from "../../naming";
import type { ColumnForm } from "./types";

/** How a pipeline argument reaches the source (REQ-1986). */
export const QUERY_ARG = "query_arg";

/** A native-query source's two kinds of table, each mode's i18n key: its relation, and a table
 * its native query produces (REQ-1980 MongoDB, REQ-1982 Cosmos DB, REQ-1983 Qdrant, REQ-2017
 * Elasticsearch). */
export const NATIVE_QUERY_MODES = {
  mongodb: [
    ["collection", "mongoModeCollection"],
    ["pipeline", "mongoModePipeline"],
  ],
  cosmosdb: [
    ["collection", "cosmosModeContainer"],
    ["pipeline", "cosmosModeQuery"],
  ],
  qdrant: [
    ["collection", "qdrantModeCollection"],
    ["pipeline", "qdrantModeSearch"],
  ],
  elasticsearch: [
    ["collection", "esModeIndex"],
    ["pipeline", "esModeSearch"],
  ],
} as const;
const NF_PREFIX = "_nf_";

/** A column the preview suggests, granted to every role, as the steward then edits it. */
export function previewedColumn(
  c: { name: string; dataType: string; path?: string | null },
  roleIds: string[],
): ColumnForm {
  const snake = toSnakeCase(c.name);
  return {
    name: c.name,
    visibleTo: roleIds,
    writableBy: [],
    masks: [],
    alias: snake !== c.name ? snake : "",
    description: "",
    selected: true,
    nativeFilterType: null,
    dataType: c.dataType,
    isPrimaryKey: false,
    scope: "domain",
    path: c.path ?? null,
  };
}

/** A pipeline argument as a parameter column of the table: optional unless the steward makes it
 * required; a pipeline requires nothing of its own. */
export function pipelineArgument(name: string, dataType: string): ColumnForm {
  return {
    ...previewedColumn({ name: NF_PREFIX + name, dataType }, []),
    alias: "",
    nativeFilterType: QUERY_ARG,
    nativeFilterRequired: false,
    requiredBySteward: false,
  };
}

export function argumentName(c: ColumnForm): string {
  return c.name.startsWith(NF_PREFIX) ? c.name.slice(NF_PREFIX.length) : c.name;
}

export function isPipelineArgument(c: ColumnForm): boolean {
  return c.nativeFilterType === QUERY_ARG;
}

/** The types an argument may take: the IR types, and arrays of the numeric and text ones (a
 * query vector is array(double)). */
export const ARGUMENT_TYPES = [
  "text",
  "integer",
  "bigint",
  "double",
  "numeric",
  "boolean",
  "date",
  "timestamp",
  "timestamptz",
  "uuid",
  "json",
  "array(double)",
  "array(integer)",
  "array(text)",
];
