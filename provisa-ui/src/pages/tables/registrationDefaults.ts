// Copyright (c) 2026 Kenneth Stott
// Canary: 0d7b3f58-91e2-4c6a-8b45-c3e9a1f7d204
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2234: what a table is registered with when nobody changes anything -- its columns as
// discovery gives them, every role seeing each, its watermark and its source's paging. The one
// table step and the several-tables step both register from here, so the defaults are the same.

import type { ColumnMetadata, TableMetadata } from "../../api/admin";
import { toSnakeCase } from "../../naming";
import type { Paging, PagingKind, UniqueConstraint } from "../../types/admin";
import { offeredUnselected } from "../sources/qdrant";
import { CDC_TYPES } from "./constants";
import type { Registration } from "./draftTable";
import { isWatermarkEligible } from "./helpers";
import { declaredPaging } from "./paging";
import type { ColumnForm } from "./types";

/** A column as discovery describes it. */
type Discovered = Pick<ColumnMetadata, "name" | "dataType"> &
  Partial<Omit<ColumnMetadata, "name" | "dataType" | "isPrimaryKey">> & {
    isPrimaryKey?: boolean | null;
  };

/** Each discovered column as the step offers it: seen by every role, in domain scope. */
export function discoveredColumns(
  columns: Discovered[],
  roleIds: string[],
  sourceType: string,
): ColumnForm[] {
  return columns.map((c) => {
    const snake = toSnakeCase(c.name);
    return {
      name: c.name,
      visibleTo: roleIds,
      masks: [],
      alias: snake !== c.name ? snake : "",
      description: c.comment || "",
      // REQ-1983: a Qdrant collection's vectors are offered, left unselected.
      selected: !offeredUnselected(sourceType, c.path),
      nativeFilterType: c.nativeFilterType ?? null,
      nativeFilterRequired: c.nativeFilterRequired ?? null,
      dataType: c.dataType,
      subtype: c.subtype ?? null, // REQ-2171
      isPrimaryKey: c.isPrimaryKey ?? false,
      // REQ-1959: nothing is public by default. A parameter column is an argument, not
      // data, and its scope publishes nothing whatever it says.
      scope: "domain",
      // REQ-1982: a document source says where a nested field is read from; REQ-1739: an
      // ingest source's path is set below.
      path: c.path ?? null,
    };
  });
}

/** The watermark a table starts with: its updated_at (or updated) column, where the source has
 * no change data capture of its own and the column's type can be one. */
export function defaultWatermark(columns: ColumnForm[], sourceType: string): string {
  if (CDC_TYPES.has(sourceType)) return "";
  const column = columns.find(
    (c) => (c.name === "updated_at" || c.name === "updated") && isWatermarkEligible(c.dataType),
  );
  return column?.name ?? "";
}

/** Why a table cannot be registered without input from the person; null when discovery gives
 * everything its registration needs. A parameter read from a typed schema is no input. */
export type InputNeeded =
  | { kind: "declared" }
  | { kind: "noColumns" }
  | { kind: "untyped"; columns: string[] }
  // REQ-1907: not of the table but of its source: each table needs a Cache TTL of its own.
  | { kind: "cacheTtl" }
  // REQ-2000: a Qdrant collection's vectors each name the model they are an embedding of.
  | { kind: "vectors"; columns: string[] };

export function inputNeeded(
  offered: Pick<TableMetadata, "columnsDeclared">,
  columns: (Pick<ColumnMetadata, "name" | "dataType"> & { path?: string | null })[],
  sourceType = "",
): InputNeeded | null {
  if (offered.columnsDeclared) return { kind: "declared" };
  if (columns.length === 0) return { kind: "noColumns" };
  const untyped = columns.filter((c) => !c.dataType).map((c) => c.name);
  if (untyped.length > 0) return { kind: "untyped", columns: untyped };
  // A vector registered without its model is lost to the table: it is named first.
  const vectors = columns.filter((c) => offeredUnselected(sourceType, c.path)).map((c) => c.name);
  return vectors.length > 0 ? { kind: "vectors", columns: vectors } : null;
}

/** What the step chose, as a registration: what names the table, its columns and its paging. */
export interface Chosen {
  sourceId: string;
  domainId: string;
  /** Only where regions exist: `{ region }`. */
  regionInput: Record<string, unknown>;
  schemaName: string;
  tableName: string;
  alias: string;
  description: string;
  watermarkColumn: string;
  /** The registered columns (registrationColumns.registeredColumns). */
  columns: Record<string, unknown>[];
  uniqueConstraints: UniqueConstraint[];
  pagingKind: PagingKind | null;
  pagination: Paging | null;
  pagingCeilingRows: number | null;
  /** Anything else the step's input carries (a query, a file glob, an import set). */
  extra?: Record<string, unknown>;
}

export function registrationOf(chosen: Chosen): Registration {
  return {
    input: {
      sourceId: chosen.sourceId,
      domainId: chosen.domainId,
      ...chosen.regionInput,
      // REQ-1673: the PHYSICAL schema the table was picked from (or the source's fixed schema),
      // never the domain. The domain is `domainId`; registering the domain as the schema lost
      // the physical location of every source whose schema is not named after the domain --
      // the engine then attached "pet_store"."product_reviews" on a Mongo database called
      // "provisa" and every query failed with "schema does not exist".
      schemaName: chosen.schemaName,
      tableName: chosen.tableName,
      alias: chosen.alias || undefined,
      description: chosen.description || undefined,
      watermarkColumn: chosen.watermarkColumn || null,
      ...chosen.extra,
      columns: chosen.columns,
      // REQ-1093: drop empty/incomplete rows -- a constraint needs a name and >=1 column.
      uniqueConstraints: chosen.uniqueConstraints
        .filter((u) => u.name.trim() && u.columns.length > 0)
        .map((u) => ({ name: u.name.trim(), columns: u.columns })),
    },
    // REQ-318: the short step declares no paging; the longer one sends what it declared.
    pagination: chosen.pagingKind !== null ? declaredPaging(chosen.pagination) : null,
    pagingKind: chosen.pagingKind,
    pagingCeilingRows: chosen.pagingCeilingRows,
    rowRules: [],
  };
}
