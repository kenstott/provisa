// Copyright (c) 2026 Kenneth Stott
// Canary: 4576fb2d-f7c6-459d-99ef-7073b56b6be0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { TFunction } from "i18next";
import { EMPTY_VALUE, NO_OPERAND, type ActiveFilter } from "./columnFilter";

/** The words for a filter, as the chip above the grid reads it: "amount > 100". */
export function describeFilter(t: TFunction, f: ActiveFilter): string {
  const { col, spec } = f;
  const op = t(`columnFilter.op.${spec.op}`);
  if (spec.op === "lastDays")
    return `${col} ${t("columnFilter.chipLastDays", { count: Number(spec.a) })}`;
  if (NO_OPERAND.has(spec.op)) return `${col} ${op}`;
  if (spec.op === "between") return `${col} ${op} ${spec.a} – ${spec.b}`;
  if (spec.op === "in") {
    const shown = (spec.values ?? []).map((v) =>
      v === EMPTY_VALUE ? t("columnFilter.emptyValue") : v,
    );
    const head = shown.slice(0, 3).join(", ");
    const more = shown.length > 3 ? ` +${shown.length - 3}` : "";
    return `${col} ${op} ${head}${more}`;
  }
  return `${col} ${op} ${spec.a}`;
}
