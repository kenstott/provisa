// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The columns a registration sends, as the register step chose them.

import type { ColumnForm } from "./types";
import { argumentName, isPipelineArgument } from "./queryTables";

// REQ-1443: the results envelope replaces whatever columns are declared; the one declared column
// exists to carry visible_to, exactly as the YAML demo registers it.
export const DQ_PLACEHOLDER_COLUMN = { name: "scan_id", dataType: "varchar" };

/** The selected columns as TableInput takes them. ``searchQuery``: a Qdrant search's query
 * argument, the one argument Qdrant requires (REQ-1983); null for any other table. */
export function registeredColumns(columns: ColumnForm[], searchQuery: string | null) {
  return columns
    .filter((c) => c.selected)
    .map((c) => ({
      name: c.name,
      dataType: c.dataType,
      visibleTo: c.visibleTo,
      masks: c.masks,
      alias: c.alias || undefined,
      description: c.description || undefined,
      nativeFilterType: c.nativeFilterType || undefined,
      nativeFilterRequired:
        searchQuery !== null && isPipelineArgument(c) && argumentName(c) === searchQuery
          ? true
          : (c.nativeFilterRequired ?? undefined),
      requiredBySteward: c.requiredBySteward || undefined, // REQ-1986
      isPrimaryKey: c.isPrimaryKey || undefined,
      scope: c.scope || "domain",
      path: c.path?.trim() || undefined, // REQ-1739: ingest per-column JSON extraction path
      // REQ-2000: a vector column's registered model.
      embedding: c.embeddingModel ? true : undefined,
      embeddingModel: c.embeddingModel || undefined,
    }));
}
