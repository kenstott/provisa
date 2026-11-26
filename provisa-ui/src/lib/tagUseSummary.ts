// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { TFunction } from "i18next";
import type { ContextPlace, ContextTagUse } from "../api/contextViews";

// REQ-2208 (amended 2026-11-26): the Tags list's one line per tag, with counts.

/** The source a place's table belongs to: the first part of its ``source.schema.table`` address. */
export function placeSource(p: ContextPlace): string {
  return p.tableRef.slice(0, p.tableRef.indexOf("."));
}

/** The list cell's "6 objects in 3 sources": the places the viewer reads, then those it does not,
 * counted (their sources are not named, so not counted). */
export function whereAppliedSummary(use: ContextTagUse, t: TFunction): string {
  if (use.usages.length === 0 && use.hiddenUsages === 0) return t("contextViews.nowhere");
  const parts: string[] = [];
  if (use.usages.length > 0) {
    const sources = new Set(use.usages.map(placeSource)).size;
    parts.push(
      t("contextViews.objectsInSources", {
        objects: t("contextViews.objectCount", { count: use.usages.length }),
        sources: t("contextViews.sourceCount", { count: sources }),
      }),
    );
  }
  if (use.hiddenUsages > 0) parts.push(t("contextViews.hiddenPlaces", { count: use.hiddenUsages }));
  return parts.join(" · ");
}

/** The list cell's "24 classifications · 1 row rule". */
export function dependentsSummary(use: ContextTagUse, t: TFunction): string {
  const parts: string[] = [];
  if (use.classificationKinds.length > 0)
    parts.push(t("contextViews.classificationCount", { count: use.classificationKinds.length }));
  if (use.rowFilters > 0) parts.push(t("contextViews.rowRuleCount", { count: use.rowFilters }));
  return parts.length === 0 ? t("contextViews.noDependents") : parts.join(" · ");
}
