// Copyright (c) 2026 Kenneth Stott
// Canary: e5b75dcd-3fd9-489f-beff-8fdd3265cebe
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { TFunction } from "i18next";
import type { RegisteredTable } from "../../types/admin";
import type { DetailField } from "../../components/detail/DetailFields";

// What the Query tab reads of a table's stored settings, under the edit controls' own labels.

/** REQ-1318: a view composed from metrics: what it is made of. A free-hand view has none. */
export function viewMetricsFields(t: TFunction, table: RegisteredTable): DetailField[] {
  const spec = table.viewMetrics;
  if (!spec) return [];
  return [
    { label: t("viewDefinitionForm.fieldMetrics"), value: spec.metrics.join(", ") },
    { label: t("viewDefinitionForm.fieldDimensions"), value: spec.dimensions.join(", ") },
    { label: t("viewDefinitionForm.fieldFilters"), value: spec.filters.join("; ") },
  ];
}
