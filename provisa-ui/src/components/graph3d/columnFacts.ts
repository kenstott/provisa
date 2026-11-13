// Copyright (c) 2026 Kenneth Stott
// Canary: 7a2f5d90-1c6e-4b83-9e47-d05b8c3a6f12
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (EVERY LANE NODE INFORMS AND LINKS): a column's facts as the graph reads them, for its
// hover card and detail panel -- read with the terms' refs, a product's member tables, and a
// table's columns read on asking (tableColumns.ts).

import type { MaskEntry } from "../../lib/masks";
import type { NodeInfo } from "./graphModel";

/** A column's facts: its type, description and masks (its sensitivity is read beside them). */
export const COLUMN_FACTS = ["data_type", "description", "masks"];

/** A column's facts as COLUMN_FACTS reads them. */
export interface ColumnFacts {
  dataType?: string | null;
  description?: string | null;
  // table_columns.masks: the ordered mask list (REQ-2059), as JSON.
  masks?: unknown;
}

/** ``info`` on a node, its facts not read left out; nothing when none was read. */
export function facts(info: NodeInfo): { info?: NodeInfo } {
  const kept = Object.fromEntries(
    Object.entries(info).filter(([, v]) => v !== undefined && v !== null && v !== ""),
  ) as NodeInfo;
  return Object.keys(kept).length === 0 ? {} : { info: kept };
}

/** A column's facts, as COLUMN_FACTS reads them. */
export function columnFacts(c: ColumnFacts): NodeInfo {
  // A JSON column comes back as its text or as its value, as the role's schema types it
  // (asProduct reads custom_properties the same way).
  const masks = (typeof c.masks === "string" ? JSON.parse(c.masks) : c.masks) as
    | MaskEntry[]
    | null
    | undefined;
  return {
    dataType: c.dataType ?? undefined,
    description: c.description ?? undefined,
    masked: masks == null ? undefined : masks.some((m) => m.mask !== null),
  };
}
