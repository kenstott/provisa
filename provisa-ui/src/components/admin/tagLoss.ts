// Copyright (c) 2026 Kenneth Stott
// Canary: 7a2c9e14-3f58-4b06-91d7-e4b8c0a5d621
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// What a move's tag changes say (REQ-2145), for TagLossSummary and the dialogs that hold it.

import type { TFunction } from "i18next";
import type { ColumnTagChange } from "../../api/glossary";

/** The columns whose tags change. */
export const changed = (columns: ColumnTagChange[]) =>
  columns.filter((c) => c.loses.length > 0 || c.gains.length > 0);

/** Every tag a move takes off a column: what Move confirms, and what Move and mark carries. */
export const lostTags = (columns: ColumnTagChange[]) => [
  ...new Set(columns.flatMap((c) => c.loses)),
];

/** The summary's question: one column by name, or how many; to ``into`` where it is known. */
export function tagLossTitle(t: TFunction, columns: ColumnTagChange[], into: string | null) {
  const moved = changed(columns);
  const one = moved.length === 1 ? `${moved[0].table}.${moved[0].column}` : null;
  if (into === null)
    return one
      ? t("glossaryTab.moveOneUnnamed", { column: one })
      : t("glossaryTab.moveManyUnnamed", { count: moved.length });
  return one
    ? t("glossaryTab.moveOne", { column: one, term: into })
    : t("glossaryTab.moveMany", { count: moved.length, term: into });
}
