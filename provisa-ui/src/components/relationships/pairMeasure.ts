// Copyright (c) 2026 Kenneth Stott
// Canary: 4a9c2e71-5d38-4b06-8f1e-b3d7a6c9e215
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: a proposed pair's measure as its row states it: how many of each column's distinct
// values occur in the other, which side's values are distinct, and what was not measured.

import type { TFunction } from "i18next";
import type { PairMeasure } from "../../api/admin";

/** The sentences a row's Measured cell shows, in order. */
export function measureLines(m: PairMeasure, t: TFunction): string[] {
  const lines: string[] = [];
  for (const side of ["source", "target"] as const) {
    const unmeasured = m[side].unmeasured;
    if (unmeasured === "not_read") lines.push(t(`proposalsTable.measure.notRead.${side}`));
    if (unmeasured === "not_readable") lines.push(t(`proposalsTable.measure.notReadable.${side}`));
  }
  if (m.source_in_target !== null && m.source.values !== null) {
    if (m.source.values === 0) lines.push(t("proposalsTable.measure.noValues"));
    else
      lines.push(
        t(`proposalsTable.measure.${m.source.more ? "inTargetFirst" : "inTarget"}`, {
          found: m.source_in_target,
          values: m.source.values,
        }),
      );
  }
  if (m.target_in_source !== null && m.target.values) {
    lines.push(
      t(`proposalsTable.measure.${m.target.more ? "inSourceFirst" : "inSource"}`, {
        found: m.target_in_source,
        values: m.target.values,
      }),
    );
  }
  const s = m.source.distinct;
  const d = m.target.distinct;
  if (s === true && d === true) lines.push(t("proposalsTable.measure.both"));
  else if (d === true) lines.push(t("proposalsTable.measure.target"));
  else if (s === true) lines.push(t("proposalsTable.measure.source"));
  else if (s === false && d === false) lines.push(t("proposalsTable.measure.neither"));
  return lines;
}
