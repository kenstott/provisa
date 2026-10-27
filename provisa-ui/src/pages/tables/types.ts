// Copyright (c) 2026 Kenneth Stott
// Canary: 3a7f9c21-8b4e-4d12-a6f1-9e2c5b8d0f34
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { MaskEntry } from "../../lib/masks";

export interface ColumnForm {
  name: string;
  visibleTo: string[];
  masks: MaskEntry[];
  alias: string;
  description: string;
  selected: boolean;
  nativeFilterType: string | null;
  /** Whether the source requires a value for this parameter; null where it is not recorded. */
  nativeFilterRequired?: boolean | null;
  /** REQ-1986: a steward made the parameter required for the table. */
  requiredBySteward?: boolean;
  /** REQ-2000: the registered model a vector column is an embedding of. */
  embeddingModel?: string | null;
  dataType: string;
  /** REQ-2171: the subtype of dataType the column registers as; cleared when the type changes. */
  subtype?: string | null;
  isPrimaryKey: boolean;
  scope: string;
  // REQ-1739: dot-notation JSON extraction path (e.g. "payload.order_id") for an `ingest` push
  // source's per-column mapping — provisa/ingest/router.py's `_extract_row`. Only meaningful when
  // the owning table's source type is `ingest`.
  path?: string | null;
}

/**
 * A column with nothing granted beyond who may see it: the shape a column takes when the steward
 * adds it, or a preview or a sample names it. Nothing is public by default (REQ-1959).
 */
export function blankColumn(name: string, dataType: string, visibleTo: string[]): ColumnForm {
  return {
    name,
    visibleTo,
    masks: [],
    alias: "",
    description: "",
    selected: true,
    nativeFilterType: null,
    dataType,
    isPrimaryKey: false,
    scope: "domain",
    path: null,
  };
}
